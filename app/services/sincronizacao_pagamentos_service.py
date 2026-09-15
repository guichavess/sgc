"""
Serviço de Sincronização do módulo de Pagamentos (SEI + Etapas + Saldos).

Centraliza a lógica das 3 fases da rotina "Sincronizar Tudo" para que ela possa
ser executada de duas formas:

- **Manual**: pelo botão do dashboard, via endpoints SSE (que apenas reutilizam
  ``atualizar_saldos_em_lote`` e registram o log no fim).
- **Agendada**: pelo job do APScheduler, via ``executar_sincronizacao_completa``.

A "última atualização" exibida no dashboard vem da tabela ``sis_sincronizacao_log``
(model ``SincronizacaoLog``), que é a fonte única e confiável — independente de
paginação ou de existência de saldo por item.
"""
from datetime import datetime
from typing import Iterable, Optional, Tuple, List

import concurrent.futures
import re

from flask import current_app

from app.extensions import db
from app.models import Solicitacao, SaldoEmpenho, Etapa, SincronizacaoLog
from app.services.saldo_service import SaldoService


# Endpoint de download de documentos do SEI (mesmo usado na Fase 1 SSE).
SEI_DOCUMENTOS_URL = (
    "https://api.sei.pi.gov.br/v1/unidades/110006213/procedimentos/documentos"
)


# =============================================================================
# FASE 3 — SALDOS EM LOTE (corrige cascata de erros + 1 único commit)
# =============================================================================

# Sentinela de cache: (contrato, ano) cujo cálculo já falhou uma vez. Evita
# reprocessar e re-registrar o mesmo erro para outras competências do par.
_ERRO_CACHE = object()


def calcular_saldo_item(
    contrato: str,
    competencia: str,
    cache: Optional[dict] = None,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Calcula o saldo de ``(contrato, competencia)`` com cache por ``(contrato, ano)``.

    O saldo disponível depende apenas de ``(contrato, ano)`` (soma de empenhos e
    liquidações do exercício) — competências do mesmo ano compartilham o mesmo
    valor. Reaproveitar o cálculo evita repetir as agregações pesadas no SIAFE
    (correção da lentidão da Fase 3).

    Falha de cálculo é isolada (read-only, não envenena a sessão).

    Args:
        contrato: código do contrato.
        competencia: competência ``MM/YYYY``.
        cache: dict opcional ``(contrato, ano) -> saldo`` reutilizado entre chamadas.

    Returns:
        ``(saldo, erro)``:
          - ``(float, None)`` em sucesso;
          - ``(None, str)`` em erro novo (deve ser contabilizado uma vez);
          - ``(None, None)`` quando o par já falhou antes (ignorar em silêncio).
    """
    ano = None
    if competencia and '/' in competencia:
        try:
            ano = int(competencia.split('/')[-1])
        except (ValueError, IndexError):
            ano = None
    chave = (contrato, ano)

    if cache is not None and chave in cache:
        valor = cache[chave]
        if valor is _ERRO_CACHE:
            return None, None  # já contabilizado como erro anteriormente
        return valor, None

    try:
        saldo = SaldoService.calcular_saldo_disponivel(contrato, competencia)
        if cache is not None:
            cache[chave] = saldo
        return saldo, None
    except Exception as e:  # noqa: BLE001 — erro de um item não pode parar o lote
        if cache is not None:
            cache[chave] = _ERRO_CACHE
        current_app.logger.warning(
            f"[SYNC-SALDOS] Falha ao calcular {contrato}/{competencia}: {e}"
        )
        return None, f"{contrato}/{competencia}: {e}"


def gravar_saldos_calculados(
    calculados: List[Tuple[str, str, float]],
) -> Tuple[int, List[str]]:
    """
    Faz upsert em lote dos saldos já calculados com UM único ``commit()``.

    Args:
        calculados: lista de tuplas ``(contrato, competencia, saldo)``.

    Returns:
        ``(atualizados, lista_erros)``. Em falha de gravação: ``(0, [erro])``.
    """
    if not calculados:
        return 0, []

    try:
        # Pré-carrega os saldos existentes mais recentes (evita N+1)
        pares = [(c, comp) for c, comp, _ in calculados]
        existentes = {}
        for s in (
            SaldoEmpenho.query
            .filter(db.tuple_(SaldoEmpenho.cod_contrato, SaldoEmpenho.competencia).in_(pares))
            .order_by(SaldoEmpenho.data.asc())
            .all()
        ):
            # asc(): o último a sobrescrever é o mais recente
            existentes[(s.cod_contrato, s.competencia)] = s

        agora = datetime.now()
        for contrato, competencia, saldo in calculados:
            registro = existentes.get((contrato, competencia))
            if registro is not None:
                registro.saldo = saldo
                registro.data = agora
            else:
                db.session.add(SaldoEmpenho(
                    cod_contrato=contrato,
                    competencia=competencia,
                    saldo=saldo,
                    data=agora,
                ))

        db.session.commit()
        return len(calculados), []
    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        current_app.logger.error(f"[SYNC-SALDOS] Erro ao gravar lote de saldos: {e}")
        return 0, [f"Gravação em lote: {e}"]


def atualizar_saldos_em_lote(
    combinacoes: Iterable[Tuple[str, str]],
    progress_cb=None,
) -> Tuple[int, int, List[str]]:
    """
    Recalcula o saldo de cada (contrato, competência) e grava em lote.

    Estratégia em duas etapas para nunca "envenenar" a sessão:
      1. Cálculo (somente leitura, com cache por ``(contrato, ano)``): erro de
         uma combinação é isolado e apenas registrado — não afeta as demais nem
         deixa a sessão em estado inválido.
      2. Gravação: faz upsert de todos os saldos calculados e UM único
         ``commit()`` no final (atomicidade + performance).

    Args:
        combinacoes: iterável de tuplas ``(codigo_contrato, competencia)``.
        progress_cb: callback opcional ``(indice, total)`` para progresso.

    Returns:
        ``(atualizados, erros, lista_erros)``
    """
    combinacoes = list(combinacoes)
    total = len(combinacoes)
    lista_erros: List[str] = []
    calculados: List[Tuple[str, str, float]] = []
    cache: dict = {}

    # --- Etapa 1: cálculo (read-only, cache por contrato/ano, falha isolada) ---
    for i, (contrato, competencia) in enumerate(combinacoes):
        saldo, erro = calcular_saldo_item(contrato, competencia, cache)
        if erro:
            lista_erros.append(erro)
        elif saldo is not None:
            calculados.append((contrato, competencia, saldo))
        if progress_cb:
            progress_cb(i + 1, total)

    if not calculados:
        return 0, len(lista_erros), lista_erros

    # --- Etapa 2: gravação em lote (upsert) + 1 commit ---
    atualizados, erros_gravacao = gravar_saldos_calculados(calculados)
    lista_erros.extend(erros_gravacao)

    return atualizados, len(lista_erros), lista_erros


# =============================================================================
# LOG DE SINCRONIZAÇÃO — fonte da "última atualização"
# =============================================================================

def registrar_log(
    *,
    origem: str = 'manual',
    status: str = 'sucesso',
    docs: int = 0,
    etapas: int = 0,
    saldos: int = 0,
    erros: int = 0,
    usuario_id: Optional[int] = None,
    iniciado_em: Optional[datetime] = None,
) -> SincronizacaoLog:
    """Cria e persiste uma linha de log já finalizada (``finalizado_em=now``)."""
    log = SincronizacaoLog(
        iniciado_em=iniciado_em or datetime.now(),
        finalizado_em=datetime.now(),
        status=status,
        origem=origem,
        docs_atualizados=docs or 0,
        etapas_avancadas=etapas or 0,
        saldos_atualizados=saldos or 0,
        erros=erros or 0,
        usuario_id=usuario_id,
    )
    db.session.add(log)
    db.session.commit()
    return log


def obter_ultima_sincronizacao() -> Optional[SincronizacaoLog]:
    """Retorna a execução concluída mais recente (ou ``None`` se não houver)."""
    return (
        SincronizacaoLog.query
        .filter(SincronizacaoLog.finalizado_em.isnot(None))
        .order_by(SincronizacaoLog.finalizado_em.desc())
        .first()
    )


# =============================================================================
# RELATÓRIO DA SINCRONIZAÇÃO MANUAL (linguagem explicativa)
# =============================================================================

_RE_PROTOCOLO = re.compile(r'\d{5}\.\d{6}/\d{4}-\d{2}')
_RE_STATUS_HTTP = re.compile(r':\s*(\d{3})\s*$')
_RE_FALHA_REDE = re.compile(r'Timeout|ConnectionError|SSLError|ConnectionReset', re.I)

LIMITE_LINHAS_LOG = 2000

FASES_SINCRONIZACAO = [
    (1, 'Download de documentos SEI',
     'Consulta o SEI e baixa a lista atualizada de documentos de cada processo '
     'de pagamento em aberto.'),
    (2, 'Cálculo de etapas',
     'Analisa os documentos baixados (NE, NL, PD, OB, atestos...) e avança cada '
     'processo para a etapa correspondente do fluxo de pagamento.'),
    (3, 'Atualização de saldos SIAFE',
     'Recalcula o saldo de empenho de cada contrato/competência com base nos '
     'dados do SIAFE.'),
]

TIPOS_ALERTA = {
    'tempo_limite_sei': {
        'ordem': 1,
        'titulo': 'Tempo limite de resposta do SEI excedido (erro 504)',
        'explicacao': (
            'O SEI demorou mais do que o permitido para devolver a lista de documentos '
            'e encerrou a consulta. Isso costuma acontecer com processos que possuem '
            'muitos documentos ou em horários de maior carga no SEI. Nenhum dado foi '
            'perdido: as informações anteriores desses processos foram preservadas.'
        ),
        'orientacao': (
            'Tente atualizar esses processos individualmente, de preferência fora do '
            'horário de pico. Se o erro persistir, o processo pode exigir tratamento '
            'técnico específico por causa do seu volume de documentos.'
        ),
    },
    'sei_indisponivel': {
        'ordem': 2,
        'titulo': 'SEI temporariamente indisponível (erro 502/503)',
        'explicacao': (
            'O SEI não conseguiu atender a consulta naquele momento, geralmente por '
            'instabilidade ou manutenção no próprio sistema. Os dados anteriores dos '
            'processos foram preservados.'
        ),
        'orientacao': 'Aguarde alguns minutos e execute a sincronização novamente.',
    },
    'falha_rede': {
        'ordem': 3,
        'titulo': 'Falha de comunicação com o SEI',
        'explicacao': (
            'A conexão com o SEI caiu ou não respondeu mesmo após 3 tentativas '
            'automáticas com tempo de espera crescente. Os dados anteriores dos '
            'processos foram preservados.'
        ),
        'orientacao': (
            'Verifique a conectividade da rede e tente novamente. Se afetar muitos '
            'processos, acione a equipe de infraestrutura.'
        ),
    },
    'processo_inexistente': {
        'ordem': 4,
        'titulo': 'Processo não encontrado no SEI (erro 422)',
        'explicacao': (
            'O SEI informou que o número de protocolo não existe mais — normalmente '
            'porque o processo foi excluído, anulado ou digitado incorretamente.'
        ),
        'orientacao': (
            'Confirme a situação do processo no SEI. Se ele realmente não existir, a '
            'solicitação pode ser excluída do SGC pela própria tela de sincronização.'
        ),
    },
    'acesso_negado': {
        'ordem': 5,
        'titulo': 'Acesso negado pelo SEI (erro 401/403)',
        'explicacao': (
            'O SEI recusou a consulta por falta de autorização: o token de acesso '
            'expirou ou a unidade não tem permissão sobre o processo.'
        ),
        'orientacao': 'Saia e entre novamente no sistema e repita a sincronização.',
    },
    'saldo_siafe': {
        'ordem': 6,
        'titulo': 'Falha no cálculo de saldo (SIAFE)',
        'explicacao': (
            'Não foi possível recalcular o saldo de empenho deste contrato/competência, '
            'geralmente por lentidão ou indisponibilidade da API do SIAFE. O último '
            'saldo calculado continua valendo.'
        ),
        'orientacao': 'Execute novamente a sincronização mais tarde.',
    },
    'nao_classificado': {
        'ordem': 7,
        'titulo': 'Outras ocorrências',
        'explicacao': (
            'Ocorrência não identificada automaticamente. A mensagem técnica original '
            'está registrada para análise.'
        ),
        'orientacao': 'Encaminhe este relatório à equipe técnica do SGC.',
    },
}

STATUS_ROTULOS = {
    'sucesso': 'Concluída com sucesso',
    'alerta': 'Concluída com alertas',
    'erro': 'Interrompida',
}

_STATUS_FASE_VALIDOS = {'sucesso', 'alerta', 'erro', 'pendente'}


def _tipo_alerta(msg: str) -> str:
    if msg.startswith('[422]') or 'inexistente' in msg.lower():
        return 'processo_inexistente'
    if msg.startswith('[ALERTA] Saldo'):
        return 'saldo_siafe'

    status = _RE_STATUS_HTTP.search(msg)
    if status:
        codigo = status.group(1)
        if codigo == '504':
            return 'tempo_limite_sei'
        if codigo in ('502', '503'):
            return 'sei_indisponivel'
        if codigo in ('401', '403'):
            return 'acesso_negado'
        if codigo == '422':
            return 'processo_inexistente'

    if _RE_FALHA_REDE.search(msg):
        return 'falha_rede'
    return 'nao_classificado'


def classificar_alerta(mensagem) -> dict:
    """Traduz uma mensagem técnica da sincronização em explicação + orientação."""
    msg = mensagem if isinstance(mensagem, str) else ''
    tipo = _tipo_alerta(msg)
    info = TIPOS_ALERTA[tipo]
    protocolo = _RE_PROTOCOLO.search(msg)
    return {
        'tipo': tipo,
        'titulo': info['titulo'],
        'explicacao': info['explicacao'],
        'orientacao': info['orientacao'],
        'protocolo': protocolo.group(0) if protocolo else None,
        'mensagem_tecnica': msg,
    }


def _parse_datahora(valor) -> Optional[datetime]:
    if not isinstance(valor, str) or not valor:
        return None
    try:
        dt = datetime.fromisoformat(valor.replace('Z', '+00:00'))
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def _formatar_duracao(inicio: Optional[datetime], fim: Optional[datetime]) -> Optional[str]:
    if not inicio or not fim or fim < inicio:
        return None
    segundos = int((fim - inicio).total_seconds())
    horas, resto = divmod(segundos, 3600)
    minutos, seg = divmod(resto, 60)
    if horas:
        return f'{horas} h {minutos} min'
    if minutos:
        return f'{minutos} min {seg} s'
    return f'{seg} s'


def _link_seguro(link) -> Optional[str]:
    if isinstance(link, str) and link.lower().startswith(('http://', 'https://')):
        return link
    return None


def _extrair_int(padrao: str, texto: str, grupo: int = 1) -> Optional[int]:
    m = re.search(padrao, texto or '')
    return int(m.group(grupo)) if m else None


def _dados_solicitacoes_por_protocolo(protocolos) -> dict:
    """Batch load de contrato e link SEI para os protocolos citados no relatório."""
    if not protocolos:
        return {}
    try:
        linhas = (
            db.session.query(
                Solicitacao.protocolo_gerado_sei,
                Solicitacao.codigo_contrato,
                Solicitacao.link_processo_sei,
            )
            .filter(Solicitacao.protocolo_gerado_sei.in_(list(protocolos)))
            .order_by(Solicitacao.id.desc())
            .all()
        )
    except Exception as e:
        db.session.rollback()
        current_app.logger.warning(f'[SINCRONIZACAO] Relatório sem dados de contrato: {e}')
        return {}

    mapa = {}
    for protocolo, contrato, link in linhas:
        mapa.setdefault(protocolo, {'contrato': contrato, 'link_sei': link})
    return mapa


def montar_relatorio_sincronizacao(dados: dict, usuario_nome: Optional[str] = None) -> dict:
    """
    Monta o relatório explicativo da sincronização manual a partir do resumo
    coletado pelo front (fases, alertas, processos 422 e log). Tolera payload
    incompleto/malformado: ignora itens inválidos em vez de falhar.
    """
    dados = dados if isinstance(dados, dict) else {}

    # --- Fases ---
    fases_payload = dados.get('fases') if isinstance(dados.get('fases'), list) else []
    por_numero = {
        f.get('numero'): f for f in fases_payload if isinstance(f, dict)
    }

    fases = []
    for numero, nome, descricao in FASES_SINCRONIZACAO:
        bruto = por_numero.get(numero, {})
        inicio = _parse_datahora(bruto.get('inicio'))
        fim = _parse_datahora(bruto.get('fim'))
        status = bruto.get('status') if bruto.get('status') in _STATUS_FASE_VALIDOS else 'pendente'
        movimentados = bruto.get('movimentados') if isinstance(bruto.get('movimentados'), list) else []
        fases.append({
            'numero': numero,
            'nome': nome,
            'descricao': descricao,
            'status': status,
            'inicio': inicio,
            'fim': fim,
            'duracao': _formatar_duracao(inicio, fim),
            'msg_final': bruto.get('msg_final') if isinstance(bruto.get('msg_final'), str) else '',
            'movimentados': [m for m in movimentados if isinstance(m, str)],
        })

    # --- Pendências (alertas + 422) ---
    classificados = []
    alertas = dados.get('alertas') if isinstance(dados.get('alertas'), list) else []
    for alerta in alertas:
        if not isinstance(alerta, dict) or not isinstance(alerta.get('msg'), str):
            continue
        item = classificar_alerta(alerta['msg'])
        item['fase'] = alerta.get('fase') if isinstance(alerta.get('fase'), str) else ''
        classificados.append(item)

    lista_422 = dados.get('protocolos_422') if isinstance(dados.get('protocolos_422'), list) else []
    for p in lista_422:
        if not isinstance(p, dict) or not isinstance(p.get('protocolo'), str):
            continue
        item = classificar_alerta(f"[422] Processo inexistente: {p['protocolo']}")
        item['protocolo'] = p['protocolo']
        item['fase'] = 'Download SEI'
        item['link_sei'] = _link_seguro(p.get('link_sei'))
        classificados.append(item)

    vistos = set()
    unicos = []
    for item in classificados:
        chave = (item['tipo'], item['protocolo'], item['mensagem_tecnica'])
        if chave not in vistos:
            vistos.add(chave)
            unicos.append(item)

    mapa_sol = _dados_solicitacoes_por_protocolo({i['protocolo'] for i in unicos if i['protocolo']})

    grupos_por_tipo = {}
    for item in unicos:
        extra = mapa_sol.get(item['protocolo'], {})
        item['contrato'] = extra.get('contrato')
        item['link_sei'] = item.get('link_sei') or _link_seguro(extra.get('link_sei'))
        info = TIPOS_ALERTA[item['tipo']]
        grupo = grupos_por_tipo.setdefault(item['tipo'], {
            'tipo': item['tipo'],
            'ordem': info['ordem'],
            'titulo': info['titulo'],
            'explicacao': info['explicacao'],
            'orientacao': info['orientacao'],
            'itens': [],
        })
        grupo['itens'].append(item)
    grupos = sorted(grupos_por_tipo.values(), key=lambda g: g['ordem'])

    # --- Números ---
    msg_f1, msg_f2, msg_f3 = (f['msg_final'] for f in fases)
    atualizados = _extrair_int(r'(\d+)\s*/\s*(\d+)', msg_f1, 1)
    verificados = _extrair_int(r'(\d+)\s*/\s*(\d+)', msg_f1, 2)
    etapas_avancadas = _extrair_int(r'(\d+)\s+processos?\s+avançaram', msg_f2)
    if etapas_avancadas is None and fases[1]['movimentados']:
        # Fase 2 interrompida não envia o total final: conta o que foi movimentado até a queda
        etapas_avancadas = len(fases[1]['movimentados'])
    numeros = {
        'processos_verificados': verificados,
        'processos_atualizados': atualizados,
        'processos_nao_consultados': (
            verificados - atualizados if verificados is not None and atualizados is not None else None
        ),
        'etapas_avancadas': etapas_avancadas,
        'saldos_atualizados': _extrair_int(r'(\d+)\s+saldos?\s+atualizados', msg_f3),
        'total_pendencias': len(unicos),
    }

    # --- Status geral ---
    if any(f['status'] in ('erro', 'pendente') for f in fases):
        status = 'erro'
    elif unicos or any(f['status'] == 'alerta' for f in fases):
        status = 'alerta'
    else:
        status = 'sucesso'

    iniciado_em = _parse_datahora(dados.get('iniciado_em'))
    finalizado_em = _parse_datahora(dados.get('finalizado_em'))
    duracao_total = _formatar_duracao(iniciado_em, finalizado_em)

    # --- Resumo executivo ---
    paragrafos = []
    if iniciado_em:
        frase = f"A sincronização foi iniciada em {iniciado_em.strftime('%d/%m/%Y')} às {iniciado_em.strftime('%H:%M:%S')}"
        paragrafos.append(frase + (f' e durou {duracao_total}.' if duracao_total else '.'))
    if verificados is not None:
        frase = (
            f'Foram verificados {verificados} processos de pagamento em aberto no SEI: '
            f'{atualizados} tiveram seus documentos atualizados'
        )
        nao = numeros['processos_nao_consultados']
        frase += f' e {nao} não puderam ser consultados nesta execução.' if nao else '.'
        paragrafos.append(frase)
    if numeros['etapas_avancadas'] is not None:
        paragrafos.append(
            f"Com base nos documentos obtidos, {numeros['etapas_avancadas']} processo(s) avançaram de etapa no fluxo de pagamento."
        )
    if numeros['saldos_atualizados'] is not None:
        paragrafos.append(f"{numeros['saldos_atualizados']} saldo(s) de empenho foram recalculados com dados do SIAFE.")
    if status == 'erro':
        paragrafos.append(
            'A execução não chegou ao fim: ao menos uma fase foi interrompida. Os dados já '
            'gravados permanecem válidos; recomenda-se executar a sincronização novamente.'
        )
    if unicos:
        paragrafos.append(
            f'Foram registradas {len(unicos)} pendência(s), detalhadas abaixo com o significado '
            'de cada caso e a ação recomendada.'
        )
    elif status == 'sucesso':
        paragrafos.append('Nenhuma pendência foi registrada: todas as etapas foram concluídas normalmente.')

    log = dados.get('log') if isinstance(dados.get('log'), list) else []
    log = [linha for linha in log if isinstance(linha, str)][:LIMITE_LINHAS_LOG]

    return {
        'status': status,
        'status_rotulo': STATUS_ROTULOS[status],
        'iniciado_em': iniciado_em,
        'finalizado_em': finalizado_em,
        'duracao_total': duracao_total,
        'gerado_em': datetime.now(),
        'usuario': usuario_nome,
        'resumo': ' '.join(paragrafos),
        'resumo_paragrafos': paragrafos,
        'numeros': numeros,
        'fases': fases,
        'grupos': grupos,
        'log': log,
    }


# =============================================================================
# ORQUESTRAÇÃO COMPLETA (usada pelo job agendado, server-side)
# =============================================================================

def executar_sincronizacao_completa(
    usuario_id: Optional[int] = None,
    origem: str = 'agendada',
) -> dict:
    """
    Executa as 3 fases server-side (sem streaming) e registra o log.

    Reutiliza os helpers já existentes em ``app.solicitacoes.routes.api``
    (``baixar_documentos_thread`` e ``processar_item_sei``) — import tardio para
    evitar import circular (api.py importa serviços).

    Returns:
        Resumo dict com contadores e status.
    """
    # Import tardio: quebra o ciclo api -> services -> api
    from app.solicitacoes.routes.api import (
        baixar_documentos_thread, processar_item_sei
    )
    from app.services.sei_auth import gerar_token_sei_admin

    inicio = datetime.now()
    docs_ok = etapas_ok = saldos_ok = 0
    erros_total = 0

    app_real = current_app._get_current_object()
    token_sei = gerar_token_sei_admin()

    # --- FASE 1: Download de documentos do SEI ---
    if not token_sei:
        current_app.logger.error('[SYNC] Token SEI indisponível — abortando sincronização.')
        log = registrar_log(
            origem=origem, status='erro', usuario_id=usuario_id, iniciado_em=inicio,
        )
        return {'status': 'erro', 'msg': 'Token SEI indisponível', 'log_id': log.id}

    pendentes = Solicitacao.query.filter(
        Solicitacao.protocolo_gerado_sei.isnot(None),
        Solicitacao.etapa_atual_id != 6,
        Solicitacao.status_geral != 'CANCELADO',
    ).all()
    protocolos = [s.protocolo_gerado_sei for s in pendentes]

    if protocolos:
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            futures = {
                executor.submit(
                    baixar_documentos_thread, app_real, prot, token_sei, SEI_DOCUMENTOS_URL
                ): prot for prot in protocolos
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    sucesso, _msg = future.result()
                    if sucesso:
                        docs_ok += 1
                    else:
                        erros_total += 1
                except Exception as exc:  # noqa: BLE001
                    erros_total += 1
                    app_real.logger.warning(f'[SYNC] Erro download {futures[future]}: {exc}')

    # --- FASE 2: Avança etapas a partir das movimentações baixadas ---
    mapa_ordem = {e.id: e.ordem for e in Etapa.query.all()}
    ids_pendentes = [
        s.id for s in Solicitacao.query.filter(
            Solicitacao.etapa_atual_id != 6,
            Solicitacao.status_geral != 'CANCELADO',
        ).all()
    ]
    if ids_pendentes:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(
                    processar_item_sei, app_real, sid, token_sei, usuario_id, mapa_ordem
                ): sid for sid in ids_pendentes
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    if future.result():
                        etapas_ok += 1
                except Exception as exc:  # noqa: BLE001
                    erros_total += 1
                    app_real.logger.warning(f'[SYNC] Erro etapa {futures[future]}: {exc}')

    # --- FASE 3: Saldos em lote ---
    combinacoes = [
        (c, comp) for (c, comp) in db.session.query(
            Solicitacao.codigo_contrato, Solicitacao.competencia
        ).distinct().filter(Solicitacao.competencia.isnot(None)).all()
    ]
    saldos_ok, erros_saldo, _ = atualizar_saldos_em_lote(combinacoes)
    erros_total += erros_saldo

    status = 'sucesso' if erros_total == 0 else 'parcial'
    log = registrar_log(
        origem=origem, status=status, docs=docs_ok, etapas=etapas_ok,
        saldos=saldos_ok, erros=erros_total, usuario_id=usuario_id, iniciado_em=inicio,
    )

    return {
        'status': status,
        'docs': docs_ok,
        'etapas': etapas_ok,
        'saldos': saldos_ok,
        'erros': erros_total,
        'log_id': log.id,
    }
