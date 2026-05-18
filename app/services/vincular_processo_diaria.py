"""
Serviço de Vinculação/Importação de Processos SEI no módulo de Diárias.

Funções públicas:
  1. verificar_protocolo_sei(protocolo)
     — usado pelo endpoint AJAX de verificação

  2. vincular_processo_sei(itinerario_id, protocolo_sei, etapa_id, usuario_id)
     — vincula um processo SEI a uma solicitação EXISTENTE (edita itinerário)

  3. importar_processo_sei_como_novo(protocolo_sei, etapa_id, usuario_id, usuario_gerador)
     — cria uma NOVA solicitação no sistema a partir de um processo SEI existente
       (importação pelo gestor via botão "Vincular Processo" na lista de administração)

Importações usadas pelos testes (patcheadas individualmente):
    app.services.vincular_processo_diaria.gerar_token_sei_admin
    app.services.vincular_processo_diaria.consultar_procedimento_sei
    app.services.vincular_processo_diaria.listar_documentos_procedimento_sei
    app.services.vincular_processo_diaria.baixar_documento_sei
"""
from datetime import date, datetime
from decimal import Decimal

from flask import current_app
import requests

from app.extensions import db
from app.services.sei_auth import gerar_token_sei_admin
from app.services.sei_integration import (
    BASE_URL,
    UNIDADE_SEAD,
    consultar_procedimento_sei,
    listar_documentos_procedimento_sei,
)
from app.services.diarias_sei_integration import (
    baixar_documento_sei,
    SERIE_TIPO_DOCUMENTO_MAP,
    ID_SERIE_REQUISICAO_DIARIAS,
    ID_SERIE_REQUISICAO_PASSAGENS,
)
from app.services.requisicao_parser import parsear_html_requisicao_diarias
from app.constants import DiariasEtapaID, TIPOS_COM_PASSAGENS

# cod_ibge do Piauí (viagem dentro do Piauí = Estadual)
COD_IBGE_PIAUI = 22
DIARIAS_BLOCO_PROCESSOS_ID = "525701"
DIARIAS_BLOCO_UNIDADE_ID = UNIDADE_SEAD
DIARIAS_SEI_TIMEOUT = 120


# ── Função de apoio ───────────────────────────────────────────────────────────

def detectar_tipo_itinerario(estado_origem_ibge, estado_destino_ibge):
    """
    Determina o tipo de itinerário com base no estado de destino.

    Regras:
        destino = 22 (Piauí) → 1 (Estadual)
        destino = outro estado → 2 (Nacional)
        destino = None → None (não altera o tipo existente)

    Args:
        estado_origem_ibge: cod_ibge do estado de origem (pode ser None)
        estado_destino_ibge: cod_ibge do estado de destino (pode ser None)

    Returns:
        int|None — 1, 2 ou None
    """
    if estado_destino_ibge is None:
        return None
    return 1 if estado_destino_ibge == COD_IBGE_PIAUI else 2


# ── AJAX helper ──────────────────────────────────────────────────────────────

def verificar_protocolo_sei(protocolo_sei):
    """
    Verifica se um processo existe no SEI. Usado pelo endpoint AJAX.

    Args:
        protocolo_sei: string com o número do processo (formatado ou só dígitos)

    Returns:
        dict com {sucesso, protocolo_formatado, id_procedimento,
                  link_acesso, especificacao, dados_procedimento, erro}
    """
    if not protocolo_sei or not str(protocolo_sei).strip():
        return {
            'sucesso': False,
            'protocolo_formatado': '',
            'id_procedimento': '',
            'link_acesso': '',
            'especificacao': '',
            'dados_procedimento': None,
            'erro': 'Número do processo não informado.',
        }

    token = gerar_token_sei_admin()
    if not token:
        return {
            'sucesso': False,
            'protocolo_formatado': '',
            'id_procedimento': '',
            'link_acesso': '',
            'especificacao': '',
            'dados_procedimento': None,
            'erro': 'Não foi possível autenticar no SEI. Verifique as credenciais.',
        }

    return consultar_procedimento_sei(token, protocolo_sei)


# ── Serviço principal ─────────────────────────────────────────────────────────

def vincular_processo_sei(itinerario_id, protocolo_sei, etapa_id, usuario_id):
    """
    Vincula um processo SEI existente a uma solicitação de diárias.

    Passos:
        1. Autentica no SEI e consulta o procedimento
        2. Atualiza sei_protocolo, sei_id_procedimento, link_processo_sei,
           especificacao_sei e etapa_atual_id no itinerário
        3. Lista documentos do processo e cria DiariasDocumentoSei para cada
           documento mapeado em SERIE_TIPO_DOCUMENTO_MAP
        4. Localiza a Requisição de Diárias (IdSerie 532), baixa e parseia
        5. Atualiza tipo_itinerario conforme trecho (PI/PI = Estadual, etc.)
        6. Insere os integrantes em DiariasItemItinerario (idempotente por CPF)
        7. Registra histórico de movimentação
        8. flush() — commit fica a cargo do chamador (rota ou teste)

    Args:
        itinerario_id: int — PK do DiariasItinerario
        protocolo_sei:  str — número do processo SEI
        etapa_id:       int — ID da etapa para a qual o itinerário avança
        usuario_id:     int — ID do usuário responsável pela ação

    Returns:
        dict {sucesso: bool, msgs: list[str], erro: str|None}
    """
    from app.models.diaria import (
        DiariasItinerario,
        DiariasItemItinerario,
        DiariasHistoricoMovimentacao,
    )

    resultado = {'sucesso': False, 'msgs': [], 'erro': None}

    # ── 1. Autenticação SEI ──────────────────────────────────────────────────
    token = gerar_token_sei_admin()
    if not token:
        resultado['erro'] = 'Não foi possível autenticar no SEI.'
        return resultado

    # ── 2. Consultar procedimento ────────────────────────────────────────────
    proc = consultar_procedimento_sei(token, protocolo_sei)
    if not proc.get('sucesso'):
        resultado['erro'] = proc.get('erro') or 'Processo não encontrado no SEI.'
        return resultado

    # ── 3. Carregar itinerário ───────────────────────────────────────────────
    itinerario = DiariasItinerario.query.get(itinerario_id)
    if not itinerario:
        resultado['erro'] = f'Solicitação {itinerario_id} não encontrada.'
        return resultado

    # ── 4. Atualizar campos do processo SEI ──────────────────────────────────
    protocolo_formatado = proc.get('protocolo_formatado') or protocolo_sei
    itinerario.sei_protocolo = protocolo_formatado
    itinerario.sei_id_procedimento = proc.get('id_procedimento', '')
    itinerario.link_processo_sei = proc.get('link_acesso', '')
    itinerario.especificacao_sei = proc.get('especificacao', '')
    itinerario.etapa_atual_id = etapa_id

    resultado['msgs'].append(f'Processo SEI {protocolo_formatado} localizado.')

    # ── 5. Listar documentos ─────────────────────────────────────────────────
    docs_result = listar_documentos_procedimento_sei(token, protocolo_sei)
    documentos = []
    if docs_result.get('sucesso') and docs_result.get('documentos'):
        documentos = docs_result['documentos']

    # ── 6. Criar DiariasDocumentoSei para cada doc mapeado ───────────────────
    requisicao_id_doc = None  # IdDocumento da Requisição de Diárias (IdSerie 532)

    for doc in documentos:
        serie = doc.get('Serie') or {}
        id_serie = str(serie.get('IdSerie', ''))
        tipo = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie)

        if tipo:
            itinerario.set_doc(
                tipo,
                sei_id=str(doc.get('IdDocumento', '')),
                sei_formatado=str(doc.get('DocumentoFormatado', '')),
            )

        if id_serie == str(ID_SERIE_REQUISICAO_DIARIAS):
            requisicao_id_doc = doc.get('IdDocumento')

    if documentos:
        resultado['msgs'].append(f'{len(documentos)} documento(s) identificado(s) no processo.')

    # ── 6b. Detectar tipo_solicitacao_id com base nos documentos ─────────────
    _tem_req_diarias = any(
        str((d.get('Serie') or {}).get('IdSerie', '')) == str(ID_SERIE_REQUISICAO_DIARIAS)
        for d in documentos
    )
    _tem_req_passagens = any(
        str((d.get('Serie') or {}).get('IdSerie', '')) == str(ID_SERIE_REQUISICAO_PASSAGENS)
        for d in documentos
    )
    if _tem_req_passagens and not _tem_req_diarias:
        itinerario.tipo_solicitacao_id = 3
    elif _tem_req_passagens:
        itinerario.tipo_solicitacao_id = 2
    elif _tem_req_diarias:
        itinerario.tipo_solicitacao_id = 1

    # ── 7. Download e parse da Requisição de Diárias (IdSerie 532) ───────────
    if requisicao_id_doc:
        try:
            html_bytes = baixar_documento_sei(str(requisicao_id_doc))
            if html_bytes:
                parsed = parsear_html_requisicao_diarias(html_bytes)

                # Atualiza tipo_itinerario conforme trecho
                tipo_detectado = detectar_tipo_itinerario(
                    parsed.get('estado_origem'),
                    parsed.get('estado_destino'),
                )
                if tipo_detectado is not None:
                    itinerario.tipo_itinerario = tipo_detectado
                    tipo_nome = 'Estadual' if tipo_detectado == 1 else 'Nacional'
                    resultado['msgs'].append(f'Tipo de itinerário detectado: {tipo_nome}.')

                # Atualiza objetivo se disponível
                if parsed.get('objetivo') and not itinerario.objetivo:
                    itinerario.objetivo = parsed['objetivo']

                # Atualiza estados origem/destino
                if parsed.get('estado_origem'):
                    itinerario.estado_origem = parsed['estado_origem']
                if parsed.get('estado_destino'):
                    itinerario.estado_destino = parsed['estado_destino']
                if parsed.get('periodo_inicio'):
                    itinerario.data_viagem = parsed['periodo_inicio']
                if parsed.get('periodo_fim'):
                    itinerario.data_retorno = parsed['periodo_fim']
                if parsed.get('valor_total_geral'):
                    itinerario.valor_total = parsed['valor_total_geral']

                qtds = [Decimal(str(i.get('qtd_diarias') or 0)) for i in parsed.get('integrantes', [])]
                if qtds:
                    itinerario.qtd_diarias_solicitadas = max(qtds)

                # Insere integrantes (idempotente por CPF)
                integrantes = parsed.get('integrantes', [])
                inseridos = 0
                for integ in integrantes:
                    cpf = (integ.get('cpf') or '').strip()
                    if not cpf:
                        continue
                    existing = DiariasItemItinerario.query.filter_by(
                        id_itinerario=itinerario.id,
                        cpf_pessoa=cpf,
                    ).first()
                    if not existing:
                        item = DiariasItemItinerario(
                            id_itinerario=itinerario.id,
                            cpf_pessoa=cpf,
                            nome_pessoa=integ.get('nome', ''),
                            matricula_pessoa=integ.get('matricula', ''),
                            cargo_folha=integ.get('cargo', ''),
                            vinculo=integ.get('vinculo', ''),
                            banco_agencia=integ.get('banco_agencia', ''),
                            banco_conta=integ.get('banco_conta', ''),
                            valor_cargo=integ.get('valor_unitario', 0) or 0,
                        )
                        db.session.add(item)
                        inseridos += 1

                if inseridos:
                    resultado['msgs'].append(
                        f'{inseridos} integrante(s) importado(s) da Requisição.'
                    )

        except Exception as e:
            # Falha ao processar a requisição não deve abortar a vinculação
            current_app.logger.warning(
                f'[DIARIAS] Erro ao processar Requisição {requisicao_id_doc}: {e}'
            )
            resultado['msgs'].append('Aviso: não foi possível importar integrantes da Requisição.')

    # ── 8. Registrar histórico de movimentação ────────────────────────────────
    hist = DiariasHistoricoMovimentacao(
        id_itinerario=itinerario.id,
        id_etapa_anterior=None,
        id_etapa_nova=etapa_id,
        id_usuario_responsavel=usuario_id,
        data_movimentacao=datetime.now(),
        comentario=f'Processo SEI vinculado: {protocolo_formatado}',
    )
    db.session.add(hist)

    # ── 9. Flush (commit fica a cargo do chamador) ────────────────────────────
    db.session.flush()

    resultado['sucesso'] = True
    resultado['msgs'].append(f'Processo {protocolo_formatado} vinculado com sucesso.')
    return resultado


# ── Importação como novo itinerário ──────────────────────────────────────────

def importar_processo_sei_como_novo(
    protocolo_sei,
    etapa_id,
    usuario_id,
    usuario_gerador='importacao',
):
    """
    Importa um processo SEI existente como uma NOVA solicitação de diárias
    no sistema, sem partir de um itinerário pré-existente.

    Diferente de `vincular_processo_sei` (que edita um itinerário existente),
    esta função cria o DiariasItinerario do zero a partir dos dados extraídos
    do SEI — Requisição de Diárias (IdSerie 532) e documentos associados.

    Passos:
        1. Autentica no SEI e valida o protocolo
        2. Cria um novo DiariasItinerario com status Importado
        3. Delega para vincular_processo_sei() (que faz todo o trabalho de
           download, parse de integrantes, documentos e histórico)

    Args:
        protocolo_sei:   str — número do processo SEI
        etapa_id:        int — etapa em que o processo se encontra
        usuario_id:      int — ID do usuário que faz a importação
        usuario_gerador: str — login/sigla gravado no campo usuario_gerador

    Returns:
        dict {sucesso: bool, itinerario_id: int|None, msgs: list, erro: str|None}
    """
    from app.models.diaria import DiariasItinerario

    resultado = {'sucesso': False, 'itinerario_id': None, 'msgs': [], 'erro': None}

    # ── 1. Validação rápida do protocolo antes de criar qualquer registro ─────
    verif = verificar_protocolo_sei(protocolo_sei)
    if not verif.get('sucesso'):
        resultado['erro'] = verif.get('erro') or 'Processo não encontrado no SEI.'
        return resultado

    protocolo_formatado = verif.get('protocolo_formatado') or protocolo_sei

    # ── 2. Criar novo itinerário "casca" ──────────────────────────────────────
    novo = DiariasItinerario(
        usuario_gerador=usuario_gerador,
        tipo_itinerario=1,          # padrão Estadual; ajustado pelo parse da Requisição
        tipo_solicitacao_id=1,      # padrão "Apenas Diárias"; ajustável depois
        qtd_diarias_solicitadas=0,
        etapa_atual_id=etapa_id,
        status_id=1,
        data_solicitacao=datetime.now(),
        sei_protocolo=protocolo_formatado,
        sei_id_procedimento=verif.get('id_procedimento', ''),
        link_processo_sei=verif.get('link_acesso', ''),
        especificacao_sei=verif.get('especificacao', ''),
    )
    db.session.add(novo)
    db.session.flush()  # garante novo.id

    resultado['msgs'].append(f'Novo itinerário #{novo.id} criado.')

    # ── 3. Reutilizar vincular_processo_sei para documentos e integrantes ─────
    vinc = vincular_processo_sei(
        itinerario_id=novo.id,
        protocolo_sei=protocolo_sei,
        etapa_id=etapa_id,
        usuario_id=usuario_id,
    )

    resultado['msgs'].extend(vinc.get('msgs', []))

    if not vinc.get('sucesso'):
        # Problema ao buscar documentos/integrantes — não aborta, apenas avisa
        current_app.logger.warning(
            f'[DIARIAS] importar_processo: vinculação incompleta para {protocolo_sei}: '
            f'{vinc.get("erro")}'
        )
        resultado['msgs'].append(
            'Aviso: importação parcial — verifique documentos e integrantes no detalhe.'
        )

    resultado['sucesso'] = True
    resultado['itinerario_id'] = novo.id
    return resultado


# ── Sincronização em lote a partir do bloco SEI ───────────────────────────────

def consultar_bloco_diarias(
    token,
    id_bloco=DIARIAS_BLOCO_PROCESSOS_ID,
    id_unidade=DIARIAS_BLOCO_UNIDADE_ID,
    sinal_retornar_protocolos='S',
):
    """Consulta o bloco SEI de processos de diárias."""
    resultado = {'sucesso': False, 'protocolos': [], 'dados_bloco': None, 'erro': None}
    if not token:
        resultado['erro'] = 'Token SEI não fornecido.'
        return resultado

    url = f'{BASE_URL}/v1/unidades/{id_unidade}/blocos/{id_bloco}'
    headers = {'token': token, 'Accept': 'application/json'}
    params = {'sinal_retornar_protocolos': sinal_retornar_protocolos}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=DIARIAS_SEI_TIMEOUT, verify=False)
        if response.status_code != 200:
            resultado['erro'] = f'Erro ao consultar bloco SEI (HTTP {response.status_code}).'
            return resultado

        data = response.json()
        protocolos = data.get('Protocolos') if isinstance(data, dict) else []
        resultado.update({
            'sucesso': True,
            'protocolos': protocolos or [],
            'dados_bloco': data,
        })
        return resultado
    except requests.exceptions.Timeout:
        resultado['erro'] = 'Timeout ao consultar bloco SEI.'
        return resultado
    except Exception as exc:
        resultado['erro'] = f'Erro ao consultar bloco SEI: {exc}'
        return resultado


def _protocolo_ja_importado(protocolo_formatado):
    from app.models.diaria import DiariasItinerario

    return db.session.query(
        db.exists().where(
            db.or_(
                DiariasItinerario.sei_protocolo == protocolo_formatado,
                DiariasItinerario.n_processo == protocolo_formatado,
            )
        )
    ).scalar()


def _protocolos_ja_importados(protocolos_formatados):
    from app.models.diaria import DiariasItinerario

    protocolos = [p for p in protocolos_formatados if p]
    if not protocolos:
        return set()

    rows = db.session.query(
        DiariasItinerario.sei_protocolo,
        DiariasItinerario.n_processo,
    ).filter(
        db.or_(
            DiariasItinerario.sei_protocolo.in_(protocolos),
            DiariasItinerario.n_processo.in_(protocolos),
        )
    ).all()

    encontrados = set()
    for sei_protocolo, n_processo in rows:
        if sei_protocolo:
            encontrados.add(sei_protocolo)
        if n_processo:
            encontrados.add(n_processo)
    return encontrados


def _inferir_etapa_por_documentos(tipos_encontrados, tipo_solicitacao_id):
    tem_passagens = tipo_solicitacao_id in TIPOS_COM_PASSAGENS
    has = tipos_encontrados.__contains__

    if has('relatorio_viagem') or has('np'):
        return int(DiariasEtapaID.PRESTACAO_CONTAS)
    if has('ob') or has('pd') or has('nl'):
        return int(DiariasEtapaID.CONCESSAO_DIARIAS)
    if has('analise_pagamento') or has('despacho_nci') or has('autorizacao_scdp') or has('nota_empenho'):
        return int(DiariasEtapaID.ANALISE_SOLICITACAO_2)
    if has('nota_reserva') or has('quadro_orcamentario'):
        return int(DiariasEtapaID.ANALISE_SOLICITACAO)
    return int(DiariasEtapaID.SOLICITACAO_INICIAL)


def _baixar_documentos_iniciais(documentos, token):
    iniciais = {}
    for doc in documentos:
        id_serie = str((doc.get('Serie') or {}).get('IdSerie', ''))
        tipo_doc = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie)
        if tipo_doc != 'requisicao':
            continue

        protocolo_doc = str(doc.get('DocumentoFormatado') or doc.get('IdDocumento') or '')
        if not protocolo_doc:
            continue

        conteudo = baixar_documento_sei(
            protocolo_doc,
            token=token,
            timeout=DIARIAS_SEI_TIMEOUT,
        )
        if conteudo:
            iniciais[tipo_doc] = {
                'documento': doc,
                'conteudo': conteudo,
            }
    return iniciais


def _popular_itinerario_com_requisicao(itinerario, parsed):
    tipo_detectado = detectar_tipo_itinerario(
        parsed.get('estado_origem'),
        parsed.get('estado_destino'),
    )
    if tipo_detectado is not None:
        itinerario.tipo_itinerario = tipo_detectado
    if parsed.get('objetivo'):
        itinerario.objetivo = parsed['objetivo']
    if parsed.get('estado_origem'):
        itinerario.estado_origem = parsed['estado_origem']
    if parsed.get('estado_destino'):
        itinerario.estado_destino = parsed['estado_destino']
    if parsed.get('periodo_inicio'):
        itinerario.data_viagem = parsed['periodo_inicio']
    if parsed.get('periodo_fim'):
        itinerario.data_retorno = parsed['periodo_fim']
    if parsed.get('valor_total_geral'):
        itinerario.valor_total = parsed['valor_total_geral']

    qtds = [Decimal(str(i.get('qtd_diarias') or 0)) for i in parsed.get('integrantes', [])]
    if qtds:
        itinerario.qtd_diarias_solicitadas = max(qtds)


def _criar_itens_do_parse(itinerario_id, integrantes):
    from app.models.diaria import DiariasItemItinerario

    inseridos = 0
    for integrante in integrantes:
        cpf = (integrante.get('cpf') or '').strip()
        if not cpf:
            continue
        existe = db.session.query(
            db.exists().where(
                db.and_(
                    DiariasItemItinerario.id_itinerario == itinerario_id,
                    DiariasItemItinerario.cpf_pessoa == cpf,
                )
            )
        ).scalar()
        if existe:
            continue
        db.session.add(DiariasItemItinerario(
            id_itinerario=itinerario_id,
            cpf_pessoa=cpf,
            nome_pessoa=integrante.get('nome', ''),
            matricula_pessoa=integrante.get('matricula', ''),
            cargo_folha=integrante.get('cargo', ''),
            vinculo=integrante.get('vinculo', ''),
            banco_agencia=integrante.get('banco_agencia', ''),
            banco_conta=integrante.get('banco_conta', ''),
            valor_cargo=integrante.get('valor_unitario', 0) or 0,
        ))
        inseridos += 1
    return inseridos


def sincronizar_processos_bloco_diarias(
    token,
    usuario_id,
    usuario_gerador,
    id_bloco=DIARIAS_BLOCO_PROCESSOS_ID,
    id_unidade=DIARIAS_BLOCO_UNIDADE_ID,
):
    """
    Sincroniza processos do bloco SEI de diárias criando DiariasItinerario
    para processos ainda inexistentes no banco.
    """
    from app.models.diaria import DiariasHistoricoMovimentacao, DiariasItinerario

    resultado = {
        'sucesso': False,
        'total': 0,
        'criados': 0,
        'existentes': 0,
        'erros': [],
        'itinerarios_criados': [],
        'msgs': [],
        'erro': None,
    }

    bloco = consultar_bloco_diarias(token, id_bloco=id_bloco, id_unidade=id_unidade)
    if not bloco.get('sucesso'):
        resultado['erro'] = bloco.get('erro') or 'Erro ao consultar bloco SEI.'
        return resultado

    protocolos = bloco.get('protocolos') or []
    resultado['total'] = len(protocolos)
    protocolos_formatados = [
        (item.get('ProtocoloFormatado') or item.get('Protocolo') or '').strip()
        for item in protocolos
    ]
    protocolos_existentes = _protocolos_ja_importados(protocolos_formatados)

    for protocolo in protocolos_formatados:
        if not protocolo:
            continue

        if protocolo in protocolos_existentes:
            resultado['existentes'] += 1
            continue

        try:
            proc = consultar_procedimento_sei(token, protocolo, timeout=DIARIAS_SEI_TIMEOUT)
            if not proc.get('sucesso'):
                resultado['erros'].append({
                    'protocolo': protocolo,
                    'erro': proc.get('erro') or 'Processo não encontrado no SEI.',
                })
                continue

            docs_result = listar_documentos_procedimento_sei(
                token,
                protocolo,
                max_retries=1,
                timeout=DIARIAS_SEI_TIMEOUT,
            )
            if not docs_result.get('sucesso'):
                resultado['erros'].append({
                    'protocolo': protocolo,
                    'erro': docs_result.get('erro') or 'Erro ao listar documentos.',
                })
                continue

            documentos = docs_result.get('documentos') or []
            tipos_encontrados = set()
            tem_requisicao_diarias = False
            tem_requisicao_passagens = False
            for doc in documentos:
                id_serie = str((doc.get('Serie') or {}).get('IdSerie', ''))
                tipo_doc = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie)
                if tipo_doc:
                    tipos_encontrados.add(tipo_doc)
                if id_serie == str(ID_SERIE_REQUISICAO_PASSAGENS):
                    tem_requisicao_passagens = True
                if id_serie == str(ID_SERIE_REQUISICAO_DIARIAS):
                    tem_requisicao_diarias = True

            if tem_requisicao_passagens and not tem_requisicao_diarias:
                tipo_solicitacao_id = 3
            elif tem_requisicao_passagens:
                tipo_solicitacao_id = 2
            else:
                tipo_solicitacao_id = 1
            etapa_id = _inferir_etapa_por_documentos(tipos_encontrados, tipo_solicitacao_id)

            novo = DiariasItinerario(
                usuario_gerador=usuario_gerador,
                tipo_itinerario=1,
                tipo_solicitacao_id=tipo_solicitacao_id,
                qtd_diarias_solicitadas=Decimal('0'),
                etapa_atual_id=etapa_id,
                status_id=1,
                data_solicitacao=date.today(),
                data_viagem=datetime.now(),
                data_retorno=datetime.now(),
                sei_protocolo=proc.get('protocolo_formatado') or protocolo,
                sei_id_procedimento=proc.get('id_procedimento', ''),
                link_processo_sei=proc.get('link_acesso', ''),
                especificacao_sei=proc.get('especificacao', ''),
                unidade_geradora_id=str(id_unidade),
            )
            db.session.add(novo)
            db.session.flush()

            for doc in documentos:
                id_serie = str((doc.get('Serie') or {}).get('IdSerie', ''))
                tipo_doc = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie)
                if not tipo_doc:
                    continue
                novo.set_doc(
                    tipo_doc,
                    sei_id=str(doc.get('IdDocumento', '')),
                    sei_formatado=str(doc.get('DocumentoFormatado', '')),
                )

            iniciais = _baixar_documentos_iniciais(documentos, token)
            requisicao = iniciais.get('requisicao')
            if requisicao:
                parsed = parsear_html_requisicao_diarias(requisicao['conteudo'])
                _popular_itinerario_com_requisicao(novo, parsed)
                inseridos = _criar_itens_do_parse(novo.id, parsed.get('integrantes', []))
                if inseridos:
                    resultado['msgs'].append(
                        f'{protocolo}: {inseridos} integrante(s) importado(s).'
                    )

            db.session.add(DiariasHistoricoMovimentacao(
                id_itinerario=novo.id,
                id_etapa_anterior=None,
                id_etapa_nova=novo.etapa_atual_id,
                id_usuario_responsavel=usuario_id,
                data_movimentacao=datetime.now(),
                comentario=f'Processo SEI importado por sincronização do bloco {id_bloco}: {protocolo}',
            ))
            db.session.flush()

            resultado['criados'] += 1
            resultado['itinerarios_criados'].append(novo.id)
        except Exception as exc:
            current_app.logger.exception(f'[DIARIAS] Erro ao sincronizar processo {protocolo}')
            resultado['erros'].append({'protocolo': protocolo, 'erro': str(exc)})

    resultado['sucesso'] = not resultado['erros'] or resultado['criados'] > 0 or resultado['existentes'] > 0
    resultado['msgs'].append(
        f"Bloco processado: {resultado['total']} processo(s), "
        f"{resultado['criados']} criado(s), {resultado['existentes']} existente(s)."
    )
    return resultado


def prever_processos_bloco_diarias(
    token,
    id_bloco=DIARIAS_BLOCO_PROCESSOS_ID,
    id_unidade=DIARIAS_BLOCO_UNIDADE_ID,
):
    """Conta processos do bloco que ainda não existem na base local."""
    resultado = {
        'sucesso': False,
        'total': 0,
        'novos': 0,
        'existentes': 0,
        'erro': None,
    }

    bloco = consultar_bloco_diarias(token, id_bloco=id_bloco, id_unidade=id_unidade)
    if not bloco.get('sucesso'):
        resultado['erro'] = bloco.get('erro') or 'Erro ao consultar bloco SEI.'
        return resultado

    protocolos = [
        (item.get('ProtocoloFormatado') or item.get('Protocolo') or '').strip()
        for item in (bloco.get('protocolos') or [])
    ]
    protocolos = [p for p in protocolos if p]
    existentes = _protocolos_ja_importados(protocolos)

    resultado['sucesso'] = True
    resultado['total'] = len(protocolos)
    resultado['existentes'] = sum(1 for p in protocolos if p in existentes)
    resultado['novos'] = resultado['total'] - resultado['existentes']
    return resultado
