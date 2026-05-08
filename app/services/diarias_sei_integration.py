"""
Integração SEI para o módulo de Diárias.

Cria procedimento (processo), documento SEAD_MEMORANDO_SGA, requisição de diárias
e documentos externos (anexos) no SEI quando uma solicitação Nacional é criada.
"""
import io
import requests
import base64
from datetime import date, datetime
from flask import current_app

from app.services.sei_auth import gerar_token_sei_admin

# URL Base do SEI
BASE_URL = "https://api.sei.pi.gov.br"

# Constantes SEI para Diárias
UNIDADE_SEAD = "110006213"
ID_TIPO_PROCEDIMENTO_DIARIAS = "100000534"  # "Solicitacao de Diarias e/ou Passagens"
ID_SERIE_MEMORANDO_SGA = "2986"
ID_SERIE_REQUISICAO_DIARIAS = "532"   # "SEAD_REQUISIÇÃO DE DIÁRIAS"
ID_SERIE_REQUISICAO_PASSAGENS = "2975"  # "SEAD_REQUISIÇÃO_DE_PASSAGENS_AÉREAS"
ID_SERIE_COTACAO = "272"               # "Cotação" (Aplicabilidade: E - Externo)
ID_SERIE_DOCUMENTO_EXTERNO = "264"    # "Documento" (Aplicabilidade: E - Externo)
ID_SERIE_AUTORIZACAO_SECRETARIO = "574"  # "SEAD_AUTORIZAÇÃO_DO_SECRETÁRIO"
ID_SERIE_QUADRO_ORCAMENTARIO = "723"   # "SEAD_QUADRO_ORCAMENTARIO"
ID_SERIE_DESPACHO = "754"              # "SEAD_DESPACHO"
ID_SERIE_ESCOLHA_PASSAGENS = "2977"    # "SEAD_ESCOLHA_PASSAGENS"
ID_SERIE_REQUISICAO_INTERNA = "543"    # "SEAD_REQUISIÇÃO_INTERNA" (alternativa para escolha passagens)
ID_SERIE_NOTA_EMPENHO = "419"         # "NE - Nota de Empenho"
ID_SERIE_AUTORIZACAO_SCDP = "269"     # "Autorização" — Autorização SOLICITAÇÃO APROVADA SCDP
ID_SERIE_DESPACHO_SGA = "2987"        # "SEAD_DESPACHO_SGA" (Despacho do Superintendente)
ID_SERIE_ANALISE_PAGAMENTO = "461"    # "SINCIN Análise de Pagamento" (Parecer NCI)
ID_SERIE_DESPACHO_NCI = "5"           # "Despacho" (Despacho NCI)
ID_SERIE_NL = "420"               # "NL - Nota de Liquidação"
ID_SERIE_PD = "421"               # "PD - Programação de Desembolso"
ID_SERIE_OB = "422"               # "OB - Ordem Bancária"
ID_SERIE_RELATORIO_VIAGEM = "1908"  # "SEAD_RELATÓRIO DE VIAGEM (DIÁRIA)"
ID_SERIE_COMPROVANTE_VIAGEM = "35"    # "Comprovante" (upload externo pelo solicitante)
ID_SERIE_NP = "423"                   # "NP - Nota Patrimonial"
ID_SERIE_PRESTACAO_SCDP = "264"       # "Documento" (Externo - Prestação SCDP)

# Conjuntos de IdSeries para tipos de documento com múltiplas variantes.
# Diferentes processos podem usar IdSeries distintas para o mesmo tipo de documento.
ID_SERIES_RELATORIO_VIAGEM = {
    "1908",  # SEAD_RELATÓRIO DE VIAGEM (DIÁRIA) — padrão do fluxo SGC
    "261",   # Relatório de Viagem a Serviço — variante encontrada em processos
    "1014",  # GOV_RELATÓRIO_DE_VIAGEM — variante encontrada em processos importados
    "539",   # SEAD_RELATORIO_DE_VIAGEM — variante encontrada em processos importados
    "1135",  # Relatório_de_Viagem_em_Equipe — variante encontrada em processos importados
}
ID_SERIES_ANALISE_HABILITACAO = {
    "7",     # Análise — variante encontrada em processos
}
ID_SERIES_AUTORIZACAO_SCDP = {
    "269",   # Autorização — Autorização SOLICITAÇÃO APROVADA SCDP
}
ID_SERIES_PRESTACAO_SCDP = {
    "264",   # Documento (Externo) — detectado por keyword no campo Numero/Descricao
}
ID_SERIE_MEMORANDO_GENERICO = "12"    # "Memorando" (genérico, usado em processos importados)
ID_SERIE_MEMORANDO_SEAD = "534"       # "SEAD_MEMORANDO" (usado em processos antigos/importados)
ID_SERIE_MEMORANDO_DIARIA = "1907"    # "SEAD MEMORANDO DE DIÁRIA" (variante encontrada em processos)

# Conjunto completo de IdSeries que representam memorandos em processos de diárias.
# Inclui variantes de diferentes unidades/períodos do SEI.
ID_SERIES_MEMORANDO = {
    "534",   # SEAD_MEMORANDO
    "1907",  # SEAD MEMORANDO DE DIÁRIA
    "2186",  # Variante memorando
    "2187",  # Variante memorando
    "2188",  # Variante memorando
    "2189",  # Variante memorando
    "2195",  # Variante memorando
    "2986",  # SEAD_MEMORANDO_SGA (padrão atual)
    "3517",  # Variante memorando
    "3550",  # Variante memorando
    "3591",  # Variante memorando
    "3950",  # Variante memorando
    "12",    # Memorando (genérico)
}
ID_SERIE_AUTORIZACAO_GENERICA = "269" # "Autorização" (alternativa à SEAD_AUTORIZAÇÃO_DO_SECRETÁRIO)
ID_SERIE_CONVITE = "127"              # "Convite" (folder/doc do evento em processos importados)
ID_SERIE_ANEXO = "263"                # "Anexo" (folder/doc do evento em processos importados)
ID_SERIE_NOTA_RESERVA = "425"         # "SEAD_NOTA_DE_RESERVA" (usada no financeiro)
ID_HIPOTESE_LEGAL_INFO_PESSOAL = "4"  # "Informação Pessoal" - Art. 31 da Lei nº 12.527/2011

# Mapeamento: IdSerie SEI → tipo_documento na tabela diarias_itinerario_documentos
# Usado pela sincronização para detectar documentos existentes no SEI
SERIE_TIPO_DOCUMENTO_MAP = {
    **{sid: 'memorando' for sid in ID_SERIES_MEMORANDO},
    ID_SERIE_REQUISICAO_DIARIAS: 'requisicao',
    ID_SERIE_REQUISICAO_PASSAGENS: 'requisicao_passagens',
    ID_SERIE_CONVITE: 'doc_externo',
    ID_SERIE_ANEXO: 'doc_externo',
    ID_SERIE_DOCUMENTO_EXTERNO: 'doc_externo',
    ID_SERIE_AUTORIZACAO_SECRETARIO: 'autorizacao',
    ID_SERIE_QUADRO_ORCAMENTARIO: 'quadro_orcamentario',
    ID_SERIE_NOTA_RESERVA: 'nota_reserva',
    ID_SERIE_NOTA_EMPENHO: 'nota_empenho',
    ID_SERIE_COTACAO: 'memorando_cotacoes',
    ID_SERIE_ESCOLHA_PASSAGENS: 'escolha_passagens',
    ID_SERIE_REQUISICAO_INTERNA: 'escolha_passagens',
    ID_SERIE_DESPACHO_SGA: 'despacho_sga',
    ID_SERIE_ANALISE_PAGAMENTO: 'analise_pagamento',
    ID_SERIE_DESPACHO_NCI: 'despacho_nci',
    ID_SERIE_NL: 'nl',
    ID_SERIE_PD: 'pd',
    ID_SERIE_OB: 'ob',
    **{sid: 'relatorio_viagem' for sid in ID_SERIES_RELATORIO_VIAGEM},
    **{sid: 'analise_habilitacao' for sid in ID_SERIES_ANALISE_HABILITACAO},
    **{sid: 'autorizacao_scdp' for sid in ID_SERIES_AUTORIZACAO_SCDP},
    ID_SERIE_NP: 'np',
    ID_SERIE_COMPROVANTE_VIAGEM: 'comprovante_viagem',
    ID_SERIE_DESPACHO: 'despacho',
}

# Unidade destino pós-autorização (Diretoria de Planejamento e Finanças)
UNIDADE_DFIN_APOIO = "110009066"  # "SEAD-PI/GAB/SGACG/DFIN/APOIO"
UNIDADE_APOIOSGA = "110006213"    # "SEAD-PI/GAB/SGACG/APOIOSGA"
UNIDADE_NCI = "110006211"         # "SEAD-PI/GAB/NCI"
UNIDADE_CCDP = "110008607"        # "SEAD-PI/SGACG/DFIN/GEO/CCDP"
UNIDADE_GEO = "110006439"        # "SEAD-PI/GAB/SGACG/DFIN/GEO"
UNIDADE_DFIN = "110006438"       # "SEAD-PI/GAB/SGACG/DFIN"
UNIDADE_GPO = "110006440"        # "SEAD-PI/GAB/SGACG/DFIN/GPO"

def _resolver_interessados(interessados=None, itinerario=None):
    """Resolve a lista de nomes de interessados para uso nos documentos SEI."""
    if interessados:
        return ', '.join(interessados)
    if itinerario:
        try:
            from app.models.diaria import DiariasItemItinerario
            itens = DiariasItemItinerario.query.filter_by(
                id_itinerario=itinerario.id
            ).all()
            nomes = [item.nome_pessoa for item in itens if item.nome_pessoa]
            if nomes:
                return ', '.join(nomes)
        except Exception:
            pass
    return ''


def _escape_html(texto):
    """Escapa caracteres HTML para prevenir injeção em documentos SEI (HIGH-02)."""
    if not texto:
        return ''
    return (str(texto)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&#39;'))


def _bloco_assinatura(nome_assinante=None, cargo_assinante=None):
    """Gera bloco HTML de assinatura visual para documentos SEI.

    Se nome/cargo não fornecidos, tenta resolver via current_user.
    Retorna string HTML pronta para incluir no conteudo_html do documento.
    """
    _nome = nome_assinante
    _cargo = cargo_assinante or ''
    if not _nome:
        try:
            from flask_login import current_user
            _nome = current_user.nome.upper() if current_user and current_user.nome else ''
            if not _cargo:
                _cargo = getattr(current_user, 'cargo', '') or ''
        except Exception:
            _nome = ''
    return f"""
        <br>
        <p style="text-align: center;">
            <i>(assinado eletronicamente)</i><br>
            <b>{_escape_html(_nome)}</b><br>
            {_escape_html(_cargo)}
        </p>"""


def _montar_despacho_html(corpo_paragrafo_html, nome_assinante, cargo_assinante,
                          titulo=None, para_linha=None):
    """Monta o HTML padrão de um SEAD_DESPACHO (série 754).

    IMPORTANTE — o que NÃO incluir aqui (o SEI renderiza automaticamente):
      - Cabeçalho (Processo nº / Interessados / Assunto): renderizado a partir
        dos metadados Procedimento + Descricao do payload.
      - Título "DESPACHO": renderizado pelo template da série 754. Se passar
        em `titulo`, será adicionado mas FICARÁ DUPLICADO. Mantenha None.

    Args:
        corpo_paragrafo_html: HTML interno do parágrafo principal
            (ex: "Encaminho os autos à <b>CCDP</b> para ...").
        nome_assinante: nome da pessoa/titular que assinará (maiúsculo).
        cargo_assinante: cargo/função.
        titulo: título extra. Por padrão None — o SEI já adiciona "DESPACHO".
            Use apenas para títulos diferentes (raro).
        para_linha: texto opcional em "PARA:" antes do corpo
            (ex: 'PARA: NÚCLEO DE CONTROLE INTERNO - NCI').
    """
    bloco_titulo = (
        f'<p style="text-align: center;"><b>{_escape_html(titulo)}</b></p><br>'
        if titulo else ''
    )
    linha_para = f'<p>{_escape_html(para_linha)}</p><br>' if para_linha else ''
    return f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        {bloco_titulo}
        {linha_para}
        <p style="text-indent: 2em; text-align: justify;">{corpo_paragrafo_html}</p>
        {_bloco_assinatura(nome_assinante, cargo_assinante)}
    </div>
    """


# Meses por extenso para formatação de datas
MESES_EXTENSO = {
    1: 'janeiro', 2: 'fevereiro', 3: 'março', 4: 'abril',
    5: 'maio', 6: 'junho', 7: 'julho', 8: 'agosto',
    9: 'setembro', 10: 'outubro', 11: 'novembro', 12: 'dezembro',
}


def _formatar_data_extenso(dt):
    """Formata data/datetime para 'dd de mês de aaaa'."""
    if isinstance(dt, str):
        from datetime import datetime
        try:
            dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M')
        except ValueError:
            dt = datetime.strptime(dt, '%Y-%m-%d')
    return f"{dt.day} de {MESES_EXTENSO[dt.month]} de {dt.year}"


def criar_procedimento_diarias(token, dados_servidor, tipo_itinerario_nome, unidade_sei_id=None):
    """
    Etapa 1: Cria o processo de diárias no SEI.

    Args:
        token: Token de autenticação SEI
        dados_servidor: dict com {cargo, matricula} do servidor principal
        tipo_itinerario_nome: 'Nacional' ou 'Estadual'
        unidade_sei_id: ID da unidade SEI onde criar o processo (default: UNIDADE_SEAD)

    Returns:
        dict com resposta do SEI (contém IdProcedimento, ProcedimentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido.")
        return None

    unidade = unidade_sei_id or UNIDADE_SEAD
    url = f"{BASE_URL}/v1/unidades/{unidade}/procedimentos"

    cargo = dados_servidor.get('cargo', 'Servidor')
    matricula = dados_servidor.get('matricula', '')

    especificacao = f"SOLICITACAO DE DIARIAS - {cargo} - {matricula} - {tipo_itinerario_nome}"

    # Trunca se necessário (limite SEI)
    if len(especificacao) > 250:
        especificacao = especificacao[:250]

    payload = {
        "procedimento": {
            "IdTipoProcedimento": ID_TIPO_PROCEDIMENTO_DIARIAS,
            "Especificacao": especificacao,
            "Observacao": "Gerado via Sistema SGC - Módulo Diárias",
            "NivelAcesso": "Restrito",
            "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
            "Assuntos": [
                {
                    "CodigoEstruturado": "997",
                    "Descricao": "DOCUMENTO OFICIAL (Ofício, Memorando, Portaria, Edital, "
                                 "Instrução Normativa e outros)"
                },
                {
                    "CodigoEstruturado": "080.1",
                    "Descricao": "DIÁRIAS"
                }
            ]
        },
        "sinal_manter_aberto_unidade": "S",
        "sinal_enviar_email_notificacao": "N"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Criando procedimento para {matricula} ({tipo_itinerario_nome})..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao criar procedimento ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        retorno['EspecificacaoGerada'] = especificacao
        current_app.logger.info(
            f"SEI Diárias: Procedimento criado com sucesso - {retorno.get('ProcedimentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro crítico ao criar procedimento: {e}")
        return None


def gerar_memorando_diarias(token, id_procedimento, dados_memorando,
                            doc_req_diarias=None, doc_req_passagens=None,
                            unidade_sei_id=None):
    """
    Gera o documento SEAD_MEMORANDO_SGA vinculado ao processo.

    IMPORTANTE: Este documento deve ser criado APÓS as requisições de diárias
    e passagens, para que seus IDs possam ser referenciados no corpo do texto.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento criado na etapa 1
        dados_memorando: dict com {
            justificativa: texto da justificativa do usuário,
            data_viagem: date ou str 'YYYY-MM-DD',
            data_retorno: date ou str 'YYYY-MM-DD',
            tipo_solicitacao_nome: nome do tipo (ex: 'Diárias + Passagens Aéreas'),
        }
        doc_req_diarias: dict retorno do SEI da requisição de diárias (ou None)
        doc_req_passagens: dict retorno do SEI da requisição de passagens (ou None)

    Returns:
        dict com resposta do SEI (contém IdDocumento, DocumentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para memorando.")
        return None

    unidade = unidade_sei_id or UNIDADE_SEAD
    url = f"{BASE_URL}/v1/unidades/{unidade}/documentos"

    justificativa = dados_memorando.get('justificativa', '')
    justificativa_solicitante = dados_memorando.get('justificativa_solicitante', '')
    data_viagem = dados_memorando.get('data_viagem')
    data_retorno = dados_memorando.get('data_retorno')
    tipo_sol = dados_memorando.get('tipo_solicitacao_nome', 'Diárias + Passagens Aéreas')

    # Formata datas por extenso
    data_viagem_extenso = _formatar_data_extenso(data_viagem) if data_viagem else ''
    data_retorno_extenso = _formatar_data_extenso(data_retorno) if data_retorno else ''

    # Monta referências dos documentos criados anteriormente
    # Usa link SEI interno: protocolo_doc como âncora clicável no SEI
    SEI_LINK_BASE = "https://sei.pi.gov.br/sei/controlador.php"

    ref_diarias = ''
    if doc_req_diarias:
        doc_fmt = doc_req_diarias.get('DocumentoFormatado', '')
        id_doc = doc_req_diarias.get('IdDocumento', '')
        # Usa o DocumentoFormatado como texto de exibição; fallback para IdDocumento
        texto_ref = doc_fmt or id_doc or ''
        if texto_ref and id_doc:
            link = (
                f"{SEI_LINK_BASE}?acao=protocolo_visualizar"
                f"&id_protocolo={id_doc}"
                f"&infra_sistema=100000100&infra_unidade_atual={UNIDADE_SEAD}"
            )
            ref_diarias = f'(<a href="{link}" target="_blank">{_escape_html(texto_ref)}</a>)'
        elif texto_ref:
            ref_diarias = f'(<i>{_escape_html(texto_ref)}</i>)'

    ref_passagens = ''
    if doc_req_passagens:
        doc_fmt = doc_req_passagens.get('DocumentoFormatado', '')
        id_doc = doc_req_passagens.get('IdDocumento', '')
        texto_ref = doc_fmt or id_doc or ''
        if texto_ref and id_doc:
            link = (
                f"{SEI_LINK_BASE}?acao=protocolo_visualizar"
                f"&id_protocolo={id_doc}"
                f"&infra_sistema=100000100&infra_unidade_atual={UNIDADE_SEAD}"
            )
            ref_passagens = f'(<a href="{link}" target="_blank">{_escape_html(texto_ref)}</a>)'
        elif texto_ref:
            ref_passagens = f'(<i>{_escape_html(texto_ref)}</i>)'

    # Monta o texto de solicitação conforme os documentos disponíveis
    if ref_diarias and ref_passagens:
        texto_solicitacao = (
            f'Solicito autorização para a concessão de diárias {ref_diarias} '
            f'e passagens {ref_passagens}'
        )
    elif ref_diarias:
        texto_solicitacao = f'Solicito autorização para a concessão de diárias {ref_diarias}'
    elif ref_passagens:
        texto_solicitacao = f'Solicito autorização para a concessão de passagens {ref_passagens}'
    else:
        texto_solicitacao = 'Solicito autorização para a concessão de diárias e passagens'

    bloco_justificativa_solicitante = ''
    if justificativa_solicitante:
        bloco_justificativa_solicitante = (
            f'<br>\n        <p>{_escape_html(justificativa_solicitante)}</p>'
        )

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p><b>PARA:</b> GABINETE DO SECRETÁRIO DE ADMINISTRAÇÃO</p>
        <br>
        <p>Senhor Secretário,</p>
        <br>
        <p>{texto_solicitacao}, no período de
        <b>{data_viagem_extenso}</b> a <b>{data_retorno_extenso}</b>.</p>
        <br>
        <p>{justificativa}</p>{bloco_justificativa_solicitante}
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": dados_memorando.get('id_serie_memorando') or ID_SERIE_MEMORANDO_SGA,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Memorando - Solicitação de {tipo_sol}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando memorando para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar memorando ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Memorando gerado com sucesso - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro crítico ao gerar memorando: {e}")
        return None


def _formatar_data_hora(dt):
    """Formata data/datetime para 'dd/mm/aaaa HH:MM'."""
    if isinstance(dt, str):
        from datetime import datetime
        try:
            dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M')
        except ValueError:
            dt = datetime.strptime(dt, '%Y-%m-%d')
    if hasattr(dt, 'hour'):
        return f"{dt.day:02d}/{dt.month:02d}/{dt.year} {dt.hour:02d}:{dt.minute:02d}"
    return f"{dt.day:02d}/{dt.month:02d}/{dt.year}"


def _formatar_valor_brl(valor):
    """Formata valor numérico para moeda brasileira (R$ X.XXX,XX)."""
    if valor is None:
        return 'R$ 0,00'
    try:
        valor = float(valor)
    except (ValueError, TypeError):
        return 'R$ 0,00'
    # Formata com 2 casas decimais e separadores brasileiros
    inteiro = int(valor)
    decimal = int(round((valor - inteiro) * 100))
    inteiro_fmt = f"{inteiro:,}".replace(',', '.')
    return f"R$ {inteiro_fmt},{decimal:02d}"


def _resolver_titular_por_cargo(cargo_gestao):
    """Busca nome e cargo do titular (superintendente/secretario) no banco."""
    try:
        from app.models.usuario import Usuario
        u = Usuario.query.filter_by(cargo_gestao=cargo_gestao, ativo=True).first()
        if u:
            return u.nome.upper() if u.nome else '', u.cargo or ''
    except Exception:
        pass
    return '', ''


def gerar_requisicao_diarias(token, id_procedimento, dados_requisicao, unidade_sei_id=None):
    """
    Etapa 3: Gera o documento SEAD_REQUISIÇÃO DE DIÁRIAS vinculado ao processo.

    Gera uma tabela HTML no formato oficial com os dados dos servidores,
    objetivo da viagem, trecho e período.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento criado na etapa 1
        dados_requisicao: dict com {
            objetivo: texto do objetivo da viagem,
            data_viagem: date/datetime ou str,
            data_retorno: date/datetime ou str,
            servidores: list de dicts {matricula, cpf, nome, cargo, banco, agencia, conta,
                                       valor_unitario, valor_total_pessoa},
            qtd_diarias: float,
            trecho: str (ex: 'Teresina/PI - Brasília/DF'),
            tipo_solicitacao_nome: nome do tipo,
        }

    Returns:
        dict com resposta do SEI (contém IdDocumento, DocumentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para requisição.")
        return None

    unidade = unidade_sei_id or UNIDADE_SEAD
    url = f"{BASE_URL}/v1/unidades/{unidade}/documentos"

    # Resolve nomes dinâmicos dos titulares para blocos de assinatura
    _nome_sup, _cargo_sup = _resolver_titular_por_cargo('superintendente')
    _cargo_sup = _cargo_sup or 'Superintendente de Gestão Administrativa - SEAD'
    _nome_sec, _cargo_sec = _resolver_titular_por_cargo('secretario')
    _cargo_sec = _cargo_sec or 'Secretário da Administração do Estado'

    objetivo = dados_requisicao.get('objetivo', '')
    data_viagem = dados_requisicao.get('data_viagem')
    data_retorno = dados_requisicao.get('data_retorno')
    servidores = dados_requisicao.get('servidores', [])
    qtd_diarias = dados_requisicao.get('qtd_diarias', 0)
    trecho = dados_requisicao.get('trecho', '')

    # Formata datas
    periodo_viagem = _formatar_data_hora(data_viagem) if data_viagem else ''
    periodo_retorno = _formatar_data_hora(data_retorno) if data_retorno else ''

    # Monta linhas da tabela de servidores com valores reais
    linhas_servidores = ''
    valor_total_geral = 0.0
    for srv in servidores:
        banco_info = ''
        if srv.get('banco') or srv.get('agencia') or srv.get('conta'):
            partes = []
            if srv.get('banco'):
                partes.append(str(srv['banco']))
            if srv.get('agencia'):
                partes.append(str(srv['agencia']))
            if srv.get('conta'):
                partes.append(str(srv['conta']))
            banco_info = '/'.join(partes)

        valor_unit = srv.get('valor_unitario', 0)
        valor_total_pessoa = srv.get('valor_total_pessoa', 0)
        valor_total_geral += valor_total_pessoa

        # Monta coluna cargo: se assessorando, exibe cargo original + assessorado
        cargo_display = srv.get('cargo', '')
        cargo_assessorado = srv.get('cargo_assessorado')
        if cargo_assessorado:
            cargo_display += f'<br><i style="font-size:9pt;">(Assessorando: {cargo_assessorado})</i>'

        linhas_servidores += f"""
        <tr>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{srv.get('matricula', '')}</td>
            <td style="border:1px solid #000; padding:4px;">{srv.get('nome', '')}</td>
            <td style="border:1px solid #000; padding:4px;">{cargo_display}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{srv.get('vinculo', '')}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{srv.get('cpf', '')}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{banco_info}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{qtd_diarias}</td>
            <td style="border:1px solid #000; padding:4px; text-align:right;">{_formatar_valor_brl(valor_unit)}</td>
            <td style="border:1px solid #000; padding:4px; text-align:right;">{_formatar_valor_brl(valor_total_pessoa)}</td>
        </tr>"""

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 10pt;">
        <table style="width:100%; border-collapse:collapse; border:2px solid #000;">
            <thead>
                <tr style="background-color:#d9e2f3;">
                    <th style="border:1px solid #000; padding:6px; text-align:center;">MATRÍCULA</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">NOME</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">CARGO/FUNÇÃO</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">EFETIVO/COMISSIONADO/<br>TERCEIRIZADO</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">CPF</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">BANCO/AGÊNCIA/<br>CONTA</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">QUANT.</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">VALOR UNITÁRIO<br>R$</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">VALOR TOTAL<br>R$</th>
                </tr>
            </thead>
            <tbody>
                {linhas_servidores}
                <tr style="background-color:#f2f2f2; font-weight:bold;">
                    <td colspan="8" style="border:1px solid #000; padding:6px; text-align:right;">VALOR TOTAL (R$)</td>
                    <td style="border:1px solid #000; padding:6px; text-align:right;">{_formatar_valor_brl(valor_total_geral)}</td>
                </tr>
            </tbody>
        </table>

        <br>

        <table style="width:100%; border-collapse:collapse; border:2px solid #000;">
            <tr>
                <td colspan="2" style="border:1px solid #000; padding:8px; background-color:#d9e2f3; font-weight:bold;">OBJETIVO DA VIAGEM</td>
            </tr>
            <tr>
                <td colspan="2" style="border:1px solid #000; padding:8px; min-height:60px;">{objetivo}</td>
            </tr>
            <tr>
                <td style="border:1px solid #000; padding:8px; width:50%;"><b>TRECHO:</b> {trecho}</td>
                <td style="border:1px solid #000; padding:8px; width:50%;"><b>PERÍODO:</b> {periodo_viagem} a {periodo_retorno}</td>
            </tr>
        </table>

        <br><br>

        <p style="text-align: center; font-size: 10pt;">
            (assinado eletronicamente)<br>
            <b>{_escape_html(_nome_sup)}</b><br>
            {_escape_html(_cargo_sup)}
        </p>

        <br>

        <table style="width:100%; border-collapse:collapse; border:2px solid #000;">
            <tr>
                <td style="border:1px solid #000; padding:8px; background-color:#d9e2f3; font-weight:bold;">DESPACHO DO SECRETÁRIO:</td>
            </tr>
            <tr>
                <td style="border:1px solid #000; padding:12px;">
                    <p style="text-indent: 2em; text-align: justify;">
                        Autorizo o pagamento das diárias, na forma legal.
                    </p>
                    <p style="text-indent: 2em; text-align: justify;">
                        À DFIN, para conhecimento e demais providências necessárias.
                    </p>
                    <br>
                    <p style="text-align: center; font-size: 10pt;">
                        (assinado eletronicamente)<br>
                        <b>{_escape_html(_nome_sec)}</b><br>
                        {_escape_html(_cargo_sec)}
                    </p>
                </td>
            </tr>
        </table>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_REQUISICAO_DIARIAS,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Requisição de Diárias - {trecho}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando requisição de diárias para procedimento {id_procedimento}..."
        )

        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar requisição ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Requisição gerada com sucesso - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro crítico ao gerar requisição: {e}")
        return None


def gerar_requisicao_passagens(token, id_procedimento, dados_requisicao, unidade_sei_id=None):
    """
    Gera o documento SEAD_REQUISIÇÃO_DE_PASSAGENS_AÉREAS vinculado ao processo.

    Gera uma tabela HTML no formato oficial com os dados dos servidores,
    trecho e período da viagem para solicitação de passagens aéreas.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento criado na etapa 1
        dados_requisicao: dict com {
            objetivo: texto do objetivo da viagem,
            data_viagem: date/datetime ou str,
            data_retorno: date/datetime ou str,
            servidores: list de dicts {matricula, cpf, nome, cargo},
            trecho: str (ex: 'Teresina/PI - Brasília/DF'),
        }

    Returns:
        dict com resposta do SEI (contém IdDocumento, DocumentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para requisição de passagens.")
        return None

    unidade = unidade_sei_id or UNIDADE_SEAD
    url = f"{BASE_URL}/v1/unidades/{unidade}/documentos"

    objetivo = dados_requisicao.get('objetivo', '')
    data_viagem = dados_requisicao.get('data_viagem')
    data_retorno = dados_requisicao.get('data_retorno')
    servidores = dados_requisicao.get('servidores', [])
    trecho = dados_requisicao.get('trecho', '')

    # Formata datas
    periodo_viagem = _formatar_data_hora(data_viagem) if data_viagem else ''
    periodo_retorno = _formatar_data_hora(data_retorno) if data_retorno else ''

    # Monta linhas da tabela de servidores (sem colunas financeiras - passagens)
    linhas_servidores = ''
    for idx, srv in enumerate(servidores, 1):
        # Monta coluna cargo: se assessorando, exibe cargo original + assessorado
        cargo_display = srv.get('cargo', '')
        cargo_assessorado = srv.get('cargo_assessorado')
        if cargo_assessorado:
            cargo_display += f'<br><i style="font-size:9pt;">(Assessorando: {cargo_assessorado})</i>'

        linhas_servidores += f"""
        <tr>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{idx}</td>
            <td style="border:1px solid #000; padding:4px;">{srv.get('nome', '')}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{srv.get('matricula', '')}</td>
            <td style="border:1px solid #000; padding:4px; text-align:center;">{srv.get('cpf', '')}</td>
            <td style="border:1px solid #000; padding:4px;">{cargo_display}</td>
        </tr>"""

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 10pt;">
        <table style="width:100%; border-collapse:collapse; border:2px solid #000;">
            <thead>
                <tr style="background-color:#d9e2f3;">
                    <th style="border:1px solid #000; padding:6px; text-align:center; width:5%;">Nº</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">NOME</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">MATRÍCULA</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">CPF</th>
                    <th style="border:1px solid #000; padding:6px; text-align:center;">CARGO/FUNÇÃO</th>
                </tr>
            </thead>
            <tbody>
                {linhas_servidores}
            </tbody>
        </table>

        <br>

        <table style="width:100%; border-collapse:collapse; border:2px solid #000;">
            <tr>
                <td colspan="2" style="border:1px solid #000; padding:8px; background-color:#d9e2f3; font-weight:bold;">OBJETIVO DA VIAGEM</td>
            </tr>
            <tr>
                <td colspan="2" style="border:1px solid #000; padding:8px; min-height:60px;">{objetivo}</td>
            </tr>
            <tr>
                <td style="border:1px solid #000; padding:8px; width:50%;"><b>TRECHO:</b> {trecho}</td>
                <td style="border:1px solid #000; padding:8px; width:50%;"><b>PERÍODO:</b> {periodo_viagem} a {periodo_retorno}</td>
            </tr>
        </table>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_REQUISICAO_PASSAGENS,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Requisição de Passagens Aéreas - {trecho}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando requisição de passagens para procedimento {id_procedimento}..."
        )

        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar req. passagens ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Requisição de passagens gerada - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro crítico ao gerar req. passagens: {e}")
        return None


def adicionar_documento_externo(token, protocolo_formatado, arquivo_bytes, nome_arquivo,
                                descricao='Documento anexo', id_serie=None, numero=None,
                                unidade_id=None):
    """
    Etapa 4: Adiciona um documento externo (PDF, DOCX, imagem) ao processo SEI.

    Usa o endpoint específico /documentos/externo que aceita arquivos em base64.

    Args:
        token: Token de autenticação SEI
        protocolo_formatado: Número do procedimento formatado (ex: '00206.000123/2026-01')
        arquivo_bytes: bytes do arquivo a ser enviado
        nome_arquivo: nome do arquivo com extensão (ex: 'documento.pdf')
        descricao: descrição do documento
        id_serie: ID da série (default: ID_SERIE_DOCUMENTO_EXTERNO "264")
                  Usar "272" para Cotação, "425" para Nota de Reserva, etc.
        numero: Número/nome do documento no SEI (ex: 'passagens Pedro')

    Returns:
        dict com resposta do SEI (contém IdDocumento, DocumentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para documento externo.")
        return None

    unidade = unidade_id or UNIDADE_SEAD
    url = f"{BASE_URL}/v1/unidades/{unidade}/documentos/externo"

    # Codifica o arquivo em base64
    conteudo_base64 = base64.b64encode(arquivo_bytes).decode('utf-8')

    # Data atual formatada dd/mm/yyyy
    data_hoje = datetime.now().strftime('%d/%m/%Y')

    payload = {
        "Procedimento": protocolo_formatado,
        "IdSerie": id_serie or ID_SERIE_DOCUMENTO_EXTERNO,
        "Data": data_hoje,
        "Observacao": descricao,
        "NomeArquivo": nome_arquivo,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "Conteudo": conteudo_base64,
        "SinBloqueado": "N"
    }
    if numero:
        payload["Numero"] = numero

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Adicionando documento externo '{nome_arquivo}' "
            f"ao procedimento {protocolo_formatado}..."
        )

        response = requests.post(url, json=payload, headers=headers, timeout=60, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao adicionar doc externo ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Documento externo adicionado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro crítico ao adicionar doc externo: {e}")
        return None


def criar_processo_diarias_completo(dados_itinerario, dados_servidor, justificativa_texto,
                                    dados_requisicao=None, arquivo_externo=None,
                                    tipo_solicitacao_id=None, unidade_sei_id=None,
                                    justificativa_solicitante=None):
    """
    Fluxo completo: autentica, cria procedimento, gera memorando, requisições e documento externo.

    Usado no crud.py/store() para viagens Nacionais com Diárias + Passagens Aéreas.

    Documentos gerados conforme tipo_solicitacao_id:
    - 1 (Apenas Diárias): memorando + requisição de diárias
    - 2 (Diárias + Passagens): memorando + requisição de diárias + requisição de passagens
    - 3 (Apenas Passagens): memorando + requisição de passagens

    Args:
        dados_itinerario: dict com {
            tipo_solicitacao_nome, tipo_itinerario_nome,
            data_viagem, data_retorno
        }
        dados_servidor: dict com {cargo, matricula} do primeiro servidor
        justificativa_texto: texto da justificativa digitado pelo usuário
        dados_requisicao: dict com {
            objetivo, servidores (list), qtd_diarias, trecho
        }
        arquivo_externo: dict com {bytes, nome_arquivo, descricao} ou None
        tipo_solicitacao_id: int (1=Apenas Diárias, 2=Diárias+Passagens, 3=Apenas Passagens)

    Returns:
        dict com {
            procedimento, memorando, requisicao, requisicao_passagens,
            doc_externo, protocolo, sucesso, erro
        }
    """
    # IDs dos tipos de solicitação
    TIPO_SOL_APENAS_DIARIAS = 1
    TIPO_SOL_DIARIAS_PASSAGENS = 2
    TIPO_SOL_APENAS_PASSAGENS = 3

    # Define quais documentos criar com base no tipo
    gerar_req_diarias = tipo_solicitacao_id in (TIPO_SOL_APENAS_DIARIAS, TIPO_SOL_DIARIAS_PASSAGENS)
    gerar_req_passagens = tipo_solicitacao_id in (TIPO_SOL_DIARIAS_PASSAGENS, TIPO_SOL_APENAS_PASSAGENS)

    # Fallback: se não informou tipo, cria requisição de diárias (compatibilidade)
    if tipo_solicitacao_id is None:
        gerar_req_diarias = True
        gerar_req_passagens = False

    resultado = {
        'procedimento': None,
        'memorando': None,
        'requisicao': None,
        'requisicao_passagens': None,
        'doc_externo': None,
        'protocolo': None,
        'sucesso': False,
        'erro': None,
    }

    # CRIT-01: Prevenção de duplicatas — verifica se já existe processo SEI
    if dados_itinerario.get('sei_protocolo'):
        resultado['erro'] = (
            f"Itinerário já possui processo SEI: {dados_itinerario['sei_protocolo']}. "
            f"Criação duplicada bloqueada."
        )
        current_app.logger.warning(f"SEI Diárias: {resultado['erro']}")
        return resultado

    try:
        # 1. Autenticação
        token = gerar_token_sei_admin()
        if not token:
            resultado['erro'] = 'Falha na autenticação com o SEI.'
            current_app.logger.error("SEI Diárias: Falha na autenticação.")
            return resultado

        # 2. Criar procedimento
        tipo_itinerario_nome = dados_itinerario.get('tipo_itinerario_nome', 'Nacional')
        proc = criar_procedimento_diarias(token, dados_servidor, tipo_itinerario_nome,
                                                 unidade_sei_id=unidade_sei_id)
        if not proc:
            resultado['erro'] = 'Falha ao criar procedimento no SEI.'
            return resultado

        resultado['procedimento'] = proc
        id_procedimento = proc.get('IdProcedimento') or proc.get('id')
        protocolo_formatado = proc.get('ProcedimentoFormatado', '')
        resultado['protocolo'] = protocolo_formatado

        # 3. Gerar requisição de diárias (se aplicável)
        #    Criada ANTES do memorando para que o ID possa ser referenciado no texto.
        doc_req_diarias = None
        if dados_requisicao and gerar_req_diarias:
            dados_requisicao['data_viagem'] = dados_itinerario.get('data_viagem')
            dados_requisicao['data_retorno'] = dados_itinerario.get('data_retorno')

            req = gerar_requisicao_diarias(token, id_procedimento, dados_requisicao,
                                                  unidade_sei_id=unidade_sei_id)
            if not req:
                current_app.logger.warning("SEI Diárias: Requisição de diárias falhou.")
            else:
                resultado['requisicao'] = req
                doc_req_diarias = req

        # 4. Gerar requisição de passagens aéreas (se aplicável)
        #    Criada ANTES do memorando para que o ID possa ser referenciado no texto.
        doc_req_passagens = None
        if dados_requisicao and gerar_req_passagens:
            dados_requisicao['data_viagem'] = dados_itinerario.get('data_viagem')
            dados_requisicao['data_retorno'] = dados_itinerario.get('data_retorno')

            req_pass = gerar_requisicao_passagens(token, id_procedimento, dados_requisicao,
                                                       unidade_sei_id=unidade_sei_id)
            if not req_pass:
                current_app.logger.warning("SEI Diárias: Requisição de passagens falhou.")
            else:
                resultado['requisicao_passagens'] = req_pass
                doc_req_passagens = req_pass

        # 5. Gerar memorando (POR ÚLTIMO dos 3 documentos internos)
        #    Agora pode referenciar os IDs das requisições criadas acima.
        # Tipo 1 (Apenas Diárias) usa SEAD_MEMORANDO (534); tipos 2,3 usam SEAD_MEMORANDO_SGA (2986)
        serie_memorando = ID_SERIE_MEMORANDO_SEAD if tipo_solicitacao_id == TIPO_SOL_APENAS_DIARIAS else None

        dados_memorando = {
            'justificativa': justificativa_texto or '',
            'data_viagem': dados_itinerario.get('data_viagem'),
            'data_retorno': dados_itinerario.get('data_retorno'),
            'tipo_solicitacao_nome': dados_itinerario.get('tipo_solicitacao_nome', 'Diárias + Passagens Aéreas'),
            'id_serie_memorando': serie_memorando,
            'justificativa_solicitante': justificativa_solicitante or '',
        }

        memo = gerar_memorando_diarias(
            token, id_procedimento, dados_memorando,
            doc_req_diarias=doc_req_diarias,
            doc_req_passagens=doc_req_passagens,
            unidade_sei_id=unidade_sei_id,
        )
        if not memo:
            resultado['erro'] = 'Procedimento criado, mas falha ao gerar memorando no SEI.'
            return resultado

        resultado['memorando'] = memo

        # 6. Adicionar documento externo (se houver arquivo)
        if arquivo_externo and arquivo_externo.get('bytes'):
            doc_ext = adicionar_documento_externo(
                token,
                protocolo_formatado,
                arquivo_externo['bytes'],
                arquivo_externo['nome_arquivo'],
                arquivo_externo.get('descricao', 'Documento anexo - Solicitacao de Diarias'),
                unidade_id=unidade_sei_id,
            )
            if not doc_ext:
                current_app.logger.warning(
                    "SEI Diárias: Documento externo falhou, mas processo e documentos internos OK."
                )
            else:
                resultado['doc_externo'] = doc_ext

        resultado['sucesso'] = True
        return resultado

    except Exception as e:
        resultado['erro'] = f'Erro inesperado na integração SEI: {str(e)}'
        current_app.logger.error(f"SEI Diárias: Erro inesperado: {e}")
        return resultado


def enviar_procedimento(token, protocolo_procedimento, unidades_destino,
                        manter_aberto=True, unidade_origem=None):
    """
    Envia (encaminha) um procedimento para uma ou mais unidades no SEI.

    Usa PATCH /v1/unidades/{id}/procedimentos/enviar.

    Args:
        token: Token de autenticacao SEI
        protocolo_procedimento: Protocolo formatado (ex: '00002.009305/2025-23')
        unidades_destino: list de IDs de unidades destino (ex: ['110009066'])
        manter_aberto: se True, mantém o processo aberto na unidade atual
        unidade_origem: ID da unidade remetente (default: UNIDADE_SEAD)

    Returns:
        dict com {sucesso: bool, erro: str ou None}
    """
    resultado = {'sucesso': False, 'erro': None}

    if not token:
        resultado['erro'] = 'Token nao fornecido.'
        return resultado

    origem = unidade_origem or UNIDADE_SEAD
    url = f"{BASE_URL}/v1/unidades/{origem}/procedimentos/enviar"

    payload = {
        'protocolo': protocolo_procedimento,
        'unidades_envio': unidades_destino,
        'sinal_manter_aberto_unidade': 'S' if manter_aberto else 'N',
        'sinal_enviar_email_notificacao': 'N',
        'sinal_remover_anotacao': 'N',
        'sinal_reabrir': 'N',
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    try:
        response = requests.patch(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code in [200, 204]:
            resultado['sucesso'] = True
            current_app.logger.info(
                f"SEI Diarias: Procedimento {protocolo_procedimento} enviado "
                f"para {unidades_destino}."
            )
        else:
            resultado['erro'] = f'Erro HTTP {response.status_code}: {response.text[:300]}'
            current_app.logger.error(
                f"SEI Diarias: Erro ao enviar procedimento: {resultado['erro']}"
            )
            # MED-03: raise para consistência com demais funções SEI
            response.raise_for_status()

        return resultado

    except Exception as e:
        resultado['erro'] = f'Erro ao enviar procedimento: {str(e)}'
        current_app.logger.error(f"SEI Diarias: {resultado['erro']}")
        return resultado


def consultar_documentos_procedimento(protocolo_procedimento):
    """
    Lista todos os documentos de um procedimento (processo) no SEI.

    Usa GET /v1/unidades/{id}/documentos?protocolo_procedimento={protocolo}.

    Args:
        protocolo_procedimento: Numero formatado do processo (ex: '00002.009305/2025-23')

    Returns:
        dict com {
            sucesso: bool,
            documentos: list de dicts (cada doc com Serie, Assinaturas, etc.),
            total: int,
            erro: str ou None,
        }
    """
    resultado = {
        'sucesso': False,
        'documentos': [],
        'total': 0,
        'erro': None,
    }

    try:
        token = gerar_token_sei_admin()
        if not token:
            resultado['erro'] = 'Falha na autenticacao SEI.'
            return resultado

        url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/documentos"
        params = {
            'protocolo_procedimento': protocolo_procedimento,
            'quantidade': 500,  # MED-13: ampliado de 100 para 500 para processos grandes
            'sinal_assinaturas': 'S',
        }
        headers = {
            'token': token,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        response = requests.get(url, params=params, headers=headers, timeout=60, verify=False)

        if response.status_code != 200:
            resultado['erro'] = f'Erro HTTP {response.status_code}: {response.text[:200]}'
            current_app.logger.error(
                f"SEI Diarias: Erro ao listar documentos do procedimento: {resultado['erro']}"
            )
            return resultado

        data = response.json()

        # Resposta pode ter formato paginado {Info: {}, Documentos: []} ou ser lista direta
        if isinstance(data, dict) and 'Documentos' in data:
            documentos = data['Documentos']
        elif isinstance(data, list):
            documentos = data
        else:
            # Resposta de documento unico (sem array)
            documentos = [data] if data else []

        resultado['sucesso'] = True
        resultado['documentos'] = documentos
        resultado['total'] = len(documentos)

        current_app.logger.info(
            f"SEI Diarias: Procedimento {protocolo_procedimento} tem {resultado['total']} documento(s)."
        )
        return resultado

    except Exception as e:
        resultado['erro'] = f'Erro ao listar documentos: {str(e)}'
        current_app.logger.error(f"SEI Diarias: {resultado['erro']}")
        return resultado


def _texto_limpo_documento_sei(conteudo):
    """Extrai texto simples de HTML/PDF-texto retornado pelo SEI."""
    import html as _html
    import re
    import unicodedata

    if not conteudo:
        return ''
    if isinstance(conteudo, bytes):
        texto = ''
        for encoding in ('utf-8', 'iso-8859-1', 'cp1252'):
            try:
                texto = conteudo.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        if not texto:
            texto = conteudo.decode('utf-8', errors='ignore')
    else:
        texto = str(conteudo)

    texto = re.sub(r'<(script|style)\b[^>]*>.*?</\1>', ' ', texto, flags=re.I | re.S)
    texto = re.sub(r'<[^>]+>', ' ', texto)
    texto = _html.unescape(texto)
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return re.sub(r'\s+', ' ', texto).strip().lower()


def detectar_despacho_sga_negacao(documentos_sei):
    """
    Detecta despacho SGA (serie 2987) com conteudo de negacao/indeferimento.

    O SEI usa a mesma serie para despacho SGA normal e despacho de negacao,
    entao a distincao precisa olhar o texto do documento.
    """
    docs = documentos_sei or []
    termos_fortes = (
        'nego o prosseguimento',
        'negacao',
        'solicitacao negada',
        'indefer',
        'nao autoriz',
        'nao sera possivel',
    )
    termos_contexto = ('diaria', 'diarias', 'passagem', 'passagens', 'concessao')

    for doc in docs:
        serie = doc.get('Serie') or {}
        if str(serie.get('IdSerie', '')) != str(ID_SERIE_DESPACHO_SGA):
            continue

        texto_base = _texto_limpo_documento_sei(' '.join(
            str(doc.get(campo) or '')
            for campo in ('Descricao', 'Numero', 'Observacao')
        ))
        texto = texto_base

        protocolo_doc = doc.get('DocumentoFormatado') or doc.get('IdDocumento')
        if protocolo_doc:
            try:
                conteudo = baixar_documento_sei(protocolo_doc)
                texto_baixado = _texto_limpo_documento_sei(conteudo)
                if texto_baixado:
                    texto = f'{texto_base} {texto_baixado}'.strip()
            except Exception as exc:
                current_app.logger.warning(
                    f'[DIARIAS] Falha ao baixar despacho SGA para detectar negacao: {exc}'
                )

        if not texto:
            continue

        tem_negacao = any(t in texto for t in termos_fortes)
        tem_contexto = any(t in texto for t in termos_contexto)
        tem_restricao = 'restric' in texto and 'orcament' in texto and tem_contexto

        if (tem_negacao and tem_contexto) or tem_restricao:
            return {
                'negado': True,
                'doc_sei_id': str(doc.get('IdDocumento') or ''),
                'doc_sei_formatado': str(doc.get('DocumentoFormatado') or ''),
                'justificativa': 'Negacao detectada automaticamente em despacho SGA no SEI.',
            }

    return {'negado': False}


def aplicar_negacao_detectada_sei(itinerario, info_negacao):
    """Marca o itinerario como negado e limpa flags de autorizacao incompativeis."""
    from datetime import datetime as _dt

    if not info_negacao or not info_negacao.get('negado'):
        return False

    itinerario.processo_negado = True
    itinerario.processo_negado_data = itinerario.processo_negado_data or _dt.now()
    itinerario.processo_negado_por_nome = (
        itinerario.processo_negado_por_nome or 'Detectado automaticamente via SEI'
    )
    itinerario.processo_negado_justificativa = (
        itinerario.processo_negado_justificativa
        or info_negacao.get('justificativa')
        or 'Negacao detectada automaticamente em despacho SGA no SEI.'
    )
    itinerario.processo_negado_doc_sei_id = (
        info_negacao.get('doc_sei_id') or itinerario.processo_negado_doc_sei_id
    )
    itinerario.processo_negado_doc_sei_formatado = (
        info_negacao.get('doc_sei_formatado') or itinerario.processo_negado_doc_sei_formatado
    )

    itinerario.superintendente_assinou = False
    itinerario.superintendente_assinou_data = None
    itinerario.superintendente_assinou_nome = None
    itinerario.secretario_assinou = False
    itinerario.secretario_assinou_data = None
    itinerario.secretario_assinou_nome = None

    doc_req = itinerario.get_doc('requisicao')
    if doc_req:
        doc_req.assinado = False

    return True


# Função antiga _verificar_dupla_assinatura foi substituída por
# app/services/diarias_assinaturas.py → verificar_assinaturas_requeridas(doc, itinerario).
# A nova função verifica 3 requisitos (super da área, super da SGA, secretário)
# com matching preciso via banco de dados (sigla_login) + fallback textual.


def verificar_autorizacao_diaria(itinerario, documentos_sei=None):
    """
    Verifica autorizacao no processo SEI da solicitacao de diaria.

    Para TODOS os tipos de solicitacao, o documento de autorizacao
    (Requisicao de Diarias ou Autorizacao do Secretario) deve possuir
    DUAS assinaturas: Superintendente + Secretario de Estado.

    Fluxo de assinatura:
    1. Superintendente assina primeiro
    2. Secretario assina depois

    Somente apos ambas as assinaturas a solicitacao e considerada autorizada
    e avanca para a etapa Financeiro.

    Args:
        itinerario: objeto DiariasItinerario (deve ter sei_protocolo)
        documentos_sei: lista de documentos já buscados do SEI (opcional).
            Se fornecida, evita uma segunda chamada à API do SEI.

    Returns:
        dict com {
            autorizada: bool,
            documento_autorizacao: dict ou None (dados do documento encontrado),
            avancou_etapa: bool,
            erro: str ou None,
        }
    """
    from app.services.diaria_service import DiariaService
    from app.constants import DiariasEtapaID

    resultado = {
        'autorizada': False,
        'documento_autorizacao': None,
        'avancou_etapa': False,
        'erro': None,
    }

    # Verifica se tem protocolo do processo SEI
    protocolo_proc = itinerario.sei_protocolo
    if not protocolo_proc:
        resultado['erro'] = 'Itinerario nao possui processo SEI.'
        return resultado

    # Usa docs já buscados ou consulta o SEI (evita chamada duplicada quando chamado
    # a partir de sincronizar_documentos_diaria que já buscou os docs)
    if documentos_sei is None:
        resp_docs = consultar_documentos_procedimento(protocolo_proc)
        if not resp_docs['sucesso']:
            resultado['erro'] = resp_docs['erro']
            return resultado
        documentos_sei = resp_docs['documentos']

    if itinerario.processo_negado:
        resultado['erro'] = 'Itinerario negado; verificacao de autorizacao ignorada.'
        return resultado

    info_negacao = detectar_despacho_sga_negacao(documentos_sei)
    if aplicar_negacao_detectada_sei(itinerario, info_negacao):
        from app.extensions import db as _db_neg
        _db_neg.session.commit()
        resultado['negacao_detectada'] = True
        resultado['erro'] = 'Negacao detectada em despacho SGA no SEI.'
        return resultado

    # Determina logica conforme tipo de solicitacao
    tipo_sol = getattr(itinerario, 'tipo_solicitacao_id', None)
    apenas_diarias = (tipo_sol == 1)

    from app.services.diarias_assinaturas import verificar_assinaturas_requeridas

    doc_encontrado = None
    info_assinaturas = None

    # Sincronização progressiva: se a Requisição de Diárias (532) já tiver
    # sido assinada pelo Superintendente cadastrado no SEI (mesmo sem o
    # Secretário), atualiza o flag local para liberar o sub-passo "Autorização
    # do Secretário" no fluxo. Esta verificação é o fallback para quando a
    # assinatura é feita diretamente no SEI (fora do sistema).
    from app.services.diarias_autorizacao import verificar_assinatura_superintendente_sei
    try:
        check_super = verificar_assinatura_superintendente_sei(itinerario, documentos_sei=documentos_sei)
        if check_super.get('assinada') and not itinerario.superintendente_assinou:
            from datetime import datetime as _dt
            itinerario.superintendente_assinou = True
            sei_ts = check_super.get('data_hora_assinatura')
            if not sei_ts:
                current_app.logger.warning(
                    f'[DIARIAS] verificar_autorizacao_diaria: data_hora_assinatura is None '
                    f'para itinerario={itinerario.id}, usando datetime.now()'
                )
            itinerario.superintendente_assinou_data = sei_ts or _dt.now()
            itinerario.superintendente_assinou_nome = check_super.get('assinante_nome')
            doc_local = itinerario.get_doc('requisicao')
            if doc_local:
                doc_local.assinado = True
            else:
                itinerario.set_doc(
                    'requisicao',
                    sei_id=check_super.get('doc_sei_id'),
                    sei_formatado=check_super.get('doc_sei_formatado'),
                    assinado=True,
                )
            from app.extensions import db as _db_sync
            _db_sync.session.commit()
            current_app.logger.info(
                f"[DIARIAS] verificar_autorizacao_diaria: superintendente_assinou=True "
                f"sincronizado do SEI (itinerario={itinerario.id}, "
                f"assinante={check_super.get('assinante_nome')!r})"
            )
            resultado['superintendente_sincronizado'] = True
    except Exception as exc:
        current_app.logger.warning(
            f"[DIARIAS] Falha na sincronização do Superintendente "
            f"(itinerario={itinerario.id}): {exc}"
        )

    if apenas_diarias:
        # Tipo 1 (Apenas Diarias):
        # 1) Busca Requisicao de Diarias com assinaturas requeridas
        # 2) Fallback: Autorizacao do Secretario com assinaturas requeridas
        for doc in documentos_sei:
            serie = doc.get('Serie', {})
            if str(serie.get('IdSerie', '')) == ID_SERIE_REQUISICAO_DIARIAS:
                info_assinaturas = verificar_assinaturas_requeridas(doc, itinerario)
                if info_assinaturas['completa']:
                    doc_encontrado = doc
                break

        # Fallback: Autorizacao do Secretario
        if not doc_encontrado:
            for doc in documentos_sei:
                serie = doc.get('Serie', {})
                if str(serie.get('IdSerie', '')) == ID_SERIE_AUTORIZACAO_SECRETARIO:
                    info_ass = verificar_assinaturas_requeridas(doc, itinerario)
                    if info_ass['completa']:
                        doc_encontrado = doc
                        info_assinaturas = info_ass
                    else:
                        info_assinaturas = info_ass
                    break
    else:
        # Tipos 2 e 3 (com passagens): SOMENTE o Autorizo do Secretário (574)
        # autoriza a solicitação. Assinaturas na Requisição de Diárias (532)
        # são administrativas e NÃO devem causar avanço de etapa.
        for doc in documentos_sei:
            serie = doc.get('Serie', {})
            if str(serie.get('IdSerie', '')) == ID_SERIE_AUTORIZACAO_SECRETARIO:
                info_ass = verificar_assinaturas_requeridas(doc, itinerario)
                if info_ass['completa']:
                    doc_encontrado = doc
                    info_assinaturas = info_ass
                else:
                    info_assinaturas = info_ass
                break

        # Fallback informativo: se Autorizo não existe, coleta info parcial
        # da Requisição para exibir status de assinaturas (sem avançar etapa).
        if not doc_encontrado and not info_assinaturas:
            for doc in documentos_sei:
                serie = doc.get('Serie', {})
                if str(serie.get('IdSerie', '')) == ID_SERIE_REQUISICAO_DIARIAS:
                    info_assinaturas = verificar_assinaturas_requeridas(doc, itinerario)
                    break

    if doc_encontrado and info_assinaturas and info_assinaturas['completa']:
        resultado['autorizada'] = True
        resultado['documento_autorizacao'] = {
            'id_documento': doc_encontrado.get('IdDocumento', ''),
            'documento_formatado': doc_encontrado.get('DocumentoFormatado', ''),
            'serie_nome': doc_encontrado.get('Serie', {}).get('Nome', ''),
            'data': doc_encontrado.get('Data', ''),
            'assinaturas': info_assinaturas['assinaturas'],
        }

        # Sincroniza campos do secretário quando detectado via SEI
        if not itinerario.secretario_assinou:
            itinerario.secretario_assinou = True
            sec = next(
                (a for a in info_assinaturas.get('assinantes', []) if a.get('eh_secretario')),
                None,
            )
            if sec:
                itinerario.secretario_assinou_nome = sec.get('nome')
            itinerario.secretario_assinou_data = (
                info_assinaturas.get('data_hora_secretario')
                or info_assinaturas.get('data_hora_ultima_assinatura')
            )

        # Avanca etapa se ainda estiver na etapa 1 (Solicitação Inicial)
        # CRIT-05: Lock otimista — re-lê do banco para evitar race condition
        from app.extensions import db as _db
        _db.session.refresh(itinerario)
        if itinerario.etapa_atual_id == DiariasEtapaID.SOLICITACAO_INICIAL:
            doc_fmt = doc_encontrado.get('DocumentoFormatado', '?')
            nomes = info_assinaturas['nomes']

            comentario = (
                f"Documento {doc_fmt} assinado pelo Superintendente e Secretario: "
                f"{', '.join(nomes)}"
            )

            # Sempre avança para Análise (etapa 3) — NR + Quadro primeiro, cotações depois
            proxima_etapa = DiariasEtapaID.ANALISE_SOLICITACAO

            DiariaService.registrar_movimentacao(
                itinerario.id,
                proxima_etapa,
                usuario_id=None,
                comentario=comentario,
                data_movimentacao=info_assinaturas.get('data_hora_ultima_assinatura'),
            )
            resultado['avancou_etapa'] = True
            current_app.logger.info(
                f"SEI Diarias: Itinerario {itinerario.id} avancou para etapa {int(proxima_etapa)} "
                f"- Documento {doc_fmt}."
            )

            # Encaminha o processo para DFIN/APOIO (Diretoria de Planejamento e Financas)
            token = gerar_token_sei_admin()
            if token:
                envio = enviar_procedimento(
                    token,
                    protocolo_proc,
                    [UNIDADE_DFIN_APOIO],
                    manter_aberto=True,
                )
                resultado['envio_procedimento'] = envio
                if envio['sucesso']:
                    current_app.logger.info(
                        f"SEI Diarias: Procedimento {protocolo_proc} encaminhado "
                        f"para DFIN/APOIO ({UNIDADE_DFIN_APOIO})."
                    )

                    # Gera despacho DFIN automaticamente após encaminhamento
                    try:
                        from app.models.diaria import DiariasItemItinerario
                        itens = DiariasItemItinerario.query.filter_by(
                            id_itinerario=itinerario.id
                        ).all()
                        nomes_interessados = [
                            item.nome_pessoa for item in itens
                            if item.nome_pessoa
                        ]

                        despacho_ret = gerar_despacho_dfin(
                            token=token,
                            id_procedimento=itinerario.sei_id_procedimento,
                            sei_protocolo=protocolo_proc,
                            interessados=nomes_interessados,
                        )
                        if despacho_ret:
                            itinerario.set_doc('despacho_dfin',
                                               sei_id=str(despacho_ret.get('IdDocumento', '')),
                                               sei_formatado=despacho_ret.get('DocumentoFormatado', ''))
                            from app.extensions import db
                            db.session.commit()
                            resultado['despacho_dfin'] = despacho_ret
                            current_app.logger.info(
                                f"SEI Diarias: Despacho DFIN gerado - "
                                f"{despacho_ret.get('DocumentoFormatado', '')}"
                            )
                        else:
                            current_app.logger.warning(
                                "SEI Diarias: Falha ao gerar despacho DFIN."
                            )
                    except Exception as e:
                        current_app.logger.error(
                            f"SEI Diarias: Erro ao gerar despacho DFIN: {e}"
                        )
                else:
                    current_app.logger.warning(
                        f"SEI Diarias: Falha ao encaminhar procedimento: {envio['erro']}"
                    )
            else:
                resultado['envio_procedimento'] = {'sucesso': False, 'erro': 'Falha ao obter token'}
                current_app.logger.warning(
                    "SEI Diarias: Nao foi possivel obter token para encaminhar procedimento."
                )
    elif info_assinaturas:
        # Documento encontrado mas sem as 2 assinaturas requeridas (Super + Secretário)
        faltam = []
        if not info_assinaturas.get('tem_superintendente'):
            faltam.append('Superintendente')
        if not info_assinaturas['tem_secretario']:
            faltam.append('Secretário')

        if not info_assinaturas['assinaturas']:
            resultado['erro'] = 'Documento encontrado, mas ainda nao possui nenhuma assinatura.'
        else:
            nomes_parcial = info_assinaturas['nomes']
            resultado['erro'] = (
                f"Documento assinado por {', '.join(nomes_parcial)}, "
                f"mas falta assinatura de: {', '.join(faltam)}."
            )

        # Retorna dados parciais do documento para referencia
        for doc in documentos_sei:
            serie = doc.get('Serie', {})
            if str(serie.get('IdSerie', '')) in (ID_SERIE_REQUISICAO_DIARIAS,
                                                  ID_SERIE_AUTORIZACAO_SECRETARIO):
                resultado['documento_autorizacao'] = {
                    'id_documento': doc.get('IdDocumento', ''),
                    'documento_formatado': doc.get('DocumentoFormatado', ''),
                    'serie_nome': doc.get('Serie', {}).get('Nome', ''),
                    'data': doc.get('Data', ''),
                    'assinaturas': info_assinaturas['assinaturas'],
                }
                break

    return resultado


# ── Despacho DFIN ────────────────────────────────────────────────────────────


def gerar_despacho_dfin(token, id_procedimento, sei_protocolo, interessados,
                       nome_assinante=None, cargo_assinante=None):
    """
    Gera o documento SEAD_DESPACHO (série 754) vinculado ao processo SEI.

    Despacho padrão do DFIN/APOIO encaminhando o processo para análise
    orçamentária, emissão de NR e quadro orçamentário.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo (ex: 00002.009305/2025-23)
        interessados: lista de nomes das pessoas do itinerário

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho DFIN.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_DFIN_APOIO}/documentos"

    # Titular DFIN — IGNORA args do chamador. Despacho sempre assinado pelo
    # Diretor de Planejamento e Finanças (titular cadastrado em sis_usuarios
    # com cargo_gestao='diretor_dfin'). Caso não cadastrado, fallback textual.
    nome_dfin, cargo_dfin = _resolver_titular_por_cargo('diretor_dfin')
    nome_final = nome_dfin or 'DIRETORIA DE PLANEJAMENTO E FINANÇAS - SEAD-PI'
    cargo_final = cargo_dfin or 'Diretor de Planejamento e Finanças - SEAD-PI'

    corpo = (
        'Encaminho o processo à <b>Gerência de Execução Orçamentária</b> para '
        'conhecimento e envio para a <b>Coordenação de Controle de Diárias e Passagens</b> '
        'para verificação do quantitativo de diárias recebidas, assim como a emissão de '
        'relatório de análise quanto a aprovação/reprovação da prestação de contas anterior '
        'e à <b>Gerência de Planejamento e Orçamento</b> para análise da disponibilidade '
        'orçamentária, emissão de nota de reserva e quadro de informação orçamentária, '
        'devendo ser observados os procedimentos legais.</p>'
        '<br>'
        '<p style="text-indent: 2em; text-align: justify;">'
        'Após, remetam-se os autos à <b>SGA</b> para deliberação.'
    )
    conteudo_html = _montar_despacho_html(corpo, nome_final, cargo_final)

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho DFIN - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho DFIN para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho DFIN ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho DFIN gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho DFIN: {e}")
        return None


# ── Quadro Orçamentário ─────────────────────────────────────────────────────


def gerar_quadro_orcamentario(token, id_procedimento, dados_quadro, sei_protocolo):
    """
    Gera o documento SEAD_QUADRO_ORCAMENTARIO (série 723) vinculado ao processo SEI.

    Cria uma tabela HTML no formato oficial com os dados orçamentários da diária.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        dados_quadro: dict com {
            ug, funcao, subfuncao, programa, plano_interno,
            fonte_recursos, natureza_despesa,
            valor_inicial_nr, saldo_nr, valor_despesa, saldo_atual_nr
        }
        sei_protocolo: Protocolo formatado do processo (ex: 00002.009305/2025-23)

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para quadro orçamentário.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_GPO}/documentos"

    hoje = date.today()
    data_formatada = f"{hoje.day} de {MESES_EXTENSO[hoje.month]} de {hoje.year}"

    ug = dados_quadro.get('ug', '')
    funcao = dados_quadro.get('funcao', '')
    subfuncao = dados_quadro.get('subfuncao', '')
    programa = dados_quadro.get('programa', '')
    plano_interno = dados_quadro.get('plano_interno', '')
    fonte_recursos = dados_quadro.get('fonte_recursos', '')
    natureza_despesa = dados_quadro.get('natureza_despesa', '')
    valor_inicial_nr = _formatar_valor_brl(dados_quadro.get('valor_inicial_nr'))
    saldo_nr = _formatar_valor_brl(dados_quadro.get('saldo_nr'))
    valor_despesa = _formatar_valor_brl(dados_quadro.get('valor_despesa'))
    saldo_atual_nr = _formatar_valor_brl(dados_quadro.get('saldo_atual_nr'))

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p style="text-align: center;"><b>GERÊNCIA DE PLANEJAMENTO E ORÇAMENTO - GPO</b></p>
        <br>
        <p>Processo SEI nº <b>{sei_protocolo}</b></p>
        <br>
        <table border="1" cellpadding="8" cellspacing="0" style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="width: 50%;"><b>UG:</b></td>
                <td style="text-align: right;">{ug}</td>
            </tr>
            <tr>
                <td><b>FUNÇÃO:</b></td>
                <td style="text-align: right;">{funcao}</td>
            </tr>
            <tr>
                <td><b>SUBFUNÇÃO:</b></td>
                <td style="text-align: right;">{subfuncao}</td>
            </tr>
            <tr>
                <td><b>PROGRAMA:</b></td>
                <td style="text-align: right;">{programa}</td>
            </tr>
            <tr>
                <td><b>PLANO INTERNO:</b></td>
                <td style="text-align: right;">{plano_interno}</td>
            </tr>
            <tr>
                <td><b>FONTE DE RECURSOS:</b></td>
                <td style="text-align: right;">{fonte_recursos}</td>
            </tr>
            <tr>
                <td><b>NATUREZA DA DESPESA:</b></td>
                <td style="text-align: right;">{natureza_despesa}</td>
            </tr>
            <tr>
                <td><b>VALOR INICIAL DA NOTA DE RESERVA:</b></td>
                <td style="text-align: right;">{valor_inicial_nr}</td>
            </tr>
            <tr>
                <td><b>SALDO DA NOTA DE RESERVA:</b></td>
                <td style="text-align: right;">{saldo_nr}</td>
            </tr>
            <tr>
                <td><b>VALOR DA DESPESA:</b></td>
                <td style="text-align: right;"><b>{valor_despesa}</b></td>
            </tr>
        </table>
        <br>
        <table border="1" cellpadding="8" cellspacing="0" style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="width: 50%;"><b>UG:</b></td>
                <td style="text-align: right;">{ug}</td>
            </tr>
            <tr>
                <td><b>SALDO ATUAL DA NOTA DE RESERVA:</b></td>
                <td style="text-align: right;"><b>{saldo_atual_nr}</b></td>
            </tr>
        </table>
        <br>
        <p style="text-align: center;">Gerência de Planejamento e Orçamento da SEAD-PI</p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_QUADRO_ORCAMENTARIO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Quadro Orçamentário - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Financeiro"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando quadro orçamentário para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar quadro orçamentário ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Quadro orçamentário gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar quadro orçamentário: {e}")
        return None


# ── Escolha de Passagens ─────────────────────────────────────────────────────

JUSTIFICATIVAS_ESCOLHA = {
    'J1': 'o valor de diárias previstas ultrapassa o benefício econômico proporcionado pela escolha da passagem de menor valor',
    'J2': 'recomendação médica devidamente atestada, com a indicação do respectivo Código Internacional de Doenças (CID)',
    'J3': 'para atender as disposições das alíneas do inciso III do art. 6° do Decreto nº 14.891, de 11 de julho de 2012',
    'J4': 'autorização do Governador ou do dirigente máximo do órgão ou entidade',
    'J5': 'Outros:',
}


def gerar_escolha_passagens(token, id_procedimento, dados_escolha, sei_protocolo):
    """
    Gera o documento SEAD_ESCOLHA_PASSAGENS (série 2977) vinculado ao processo SEI.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        dados_escolha: dict com {
            voos_ida: list de DiariasCotacaoVoo (todas as opções IDA),
            voos_volta: list de DiariasCotacaoVoo (todas as opções VOLTA),
            escolha_ida_id: int (ID do voo IDA selecionado),
            escolha_volta_id: int (ID do voo VOLTA selecionado),
            menor_valor: bool,
            justificativa_codigos: list de str (ex: ['J1', 'J4']),
            justificativa_outros_texto: str ou None,
            declaracao: bool,
        }
        sei_protocolo: Protocolo formatado do processo

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para escolha de passagens.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    voos_ida = dados_escolha.get('voos_ida', [])
    voos_volta = dados_escolha.get('voos_volta', [])
    escolha_ida_id = dados_escolha.get('escolha_ida_id')
    escolha_volta_id = dados_escolha.get('escolha_volta_id')
    menor_valor = dados_escolha.get('menor_valor', False)
    justificativa_codigos = dados_escolha.get('justificativa_codigos', [])
    justificativa_outros_texto = dados_escolha.get('justificativa_outros_texto', '')
    declaracao = dados_escolha.get('declaracao', False)

    # Monta linhas de opções IDA
    linhas_ida = []
    for i, voo in enumerate(voos_ida, 1):
        marcador = '(x)' if voo.id == escolha_ida_id else '( )'
        saida_fmt = voo.saida.strftime('%d/%m/%Y - %H:%M') if voo.saida else ''
        rota = voo.resumo_trecho if hasattr(voo, 'resumo_trecho') else f'{voo.origem} > {voo.destino}'
        if voo.tem_conexao:
            rota = f'{rota} (por {voo.destino_conexao})'
        detalhes = f'voo {voo.cia} {voo.voo}, {saida_fmt}'
        if voo.tem_conexao:
            detalhes += f' (por {voo.destino_conexao})'
        linhas_ida.append(f'{marcador} OPÇÃO {i} - {detalhes}')

    # Monta linhas de opções VOLTA
    linhas_volta = []
    for i, voo in enumerate(voos_volta, 1):
        marcador = '(x)' if voo.id == escolha_volta_id else '( )'
        saida_fmt = voo.saida.strftime('%d/%m/%Y - %H:%M') if voo.saida else ''
        detalhes = f'voo {voo.cia} {voo.voo}, {saida_fmt}'
        if voo.tem_conexao:
            detalhes += f' (por {voo.destino_conexao})'
        linhas_volta.append(f'{marcador} OPÇÃO {i} - {detalhes}')

    texto_ida = '<br>'.join(linhas_ida) if linhas_ida else '(sem opções)'
    texto_volta = '<br>'.join(linhas_volta) if linhas_volta else '(sem opções)'

    # Menor valor
    x_sim = 'x' if menor_valor else ' '
    x_nao = ' ' if menor_valor else 'x'

    # Justificativas
    html_justificativas = ''
    if not menor_valor:
        html_justificativas = """
        <p><b>JUSTIFICATIVA POR NÃO OPTAR PELO BILHETE DE MENOR VALOR</b>
        <i>(Conforme art.9, § 2º da Instrução Normativa Conjunta SEADPREV/CGE nº 01/2021):</i></p>
        """
        for code, texto in JUSTIFICATIVAS_ESCOLHA.items():
            marcador = '(x)' if code in justificativa_codigos else '( )'
            if code == 'J5' and justificativa_outros_texto:
                html_justificativas += f'<p>{marcador} {texto} {justificativa_outros_texto}</p>\n'
            else:
                html_justificativas += f'<p>{marcador} {texto}</p>\n'

    # Declaração
    x_decl = 'X' if declaracao else ' '

    # Nota: o título "JUSTIFICATIVA DE ESCOLHA DE PASSAGENS" já vem do template SEI da série 2977
    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 11pt;">
        <table border="1" cellpadding="8" cellspacing="0" style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="width: 30%; vertical-align: top;"><b>VOO ESCOLHIDO:</b></td>
                <td>
                    <p><b>IDA:</b> {texto_ida}</p>
                    <p><b>VOLTA:</b> {texto_volta}</p>
                </td>
            </tr>
            <tr>
                <td><b>FOI ESCOLHIDO O BILHETE DE MENOR VALOR?</b></td>
                <td>({x_sim}) SIM &nbsp;&nbsp;&nbsp;&nbsp; ({x_nao}) NÃO - <i>JUSTIFICAR ABAIXO</i></td>
            </tr>
        </table>
        <br>
        {html_justificativas}
        <br>
        <table border="1" cellpadding="8" cellspacing="0" style="width: 100%; border-collapse: collapse;">
            <tr>
                <td>
                    <b>( {x_decl} )</b> <i>Declaro ter ciência de que alterações de percurso, data ou horário de
                    deslocamentos serão de minha inteira responsabilidade, caso não sejam formalmente
                    autorizadas ou determinadas pela Secretaria de Administração.
                    (Conforme art. 6º, § único, do Decreto nº 14.891/2012).</i>
                </td>
            </tr>
        </table>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_ESCOLHA_PASSAGENS,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Escolha de Passagens - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando escolha de passagens para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar escolha de passagens ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Escolha de passagens gerada - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar escolha de passagens: {e}")
        return None


# ── 2º MEMORANDO SGA — Encaminhamento de Cotações ─────────────────────────

def gerar_memorando_cotacoes(token, id_procedimento, sei_protocolo,
                              ref_cotacoes_fmt, ref_requisicao_passagens_fmt):
    """
    Gera o 2º SEAD_MEMORANDO_SGA (IdSerie 2986) encaminhando as cotações
    de passagens para escolha do bilhete.

    Este documento é criado SOMENTE quando o admin registra a escolha de
    passagens. Referencia os PDFs de cotações enviados ao SEI e a
    requisição de passagens aéreas.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        ref_cotacoes_fmt: str com ID(s) das cotações (ex: "0020145484")
                          — pode conter múltiplos separados por vírgula
        ref_requisicao_passagens_fmt: str com ID da requisição de passagens
                                       (ex: "SEAD-0020116597")
    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para memorando cotações.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p><b>PARA:</b> SUPERINTENDÊNCIA DE GESTÃO ADMINISTRATIVA</p>
        <br>
        <p>Encaminhamos cotações de passagens aéreas (id. {ref_cotacoes_fmt}) de acordo com os
        voos disponíveis para as datas de ida e volta indicados na requisição (id.{ref_requisicao_passagens_fmt}), para
        determinação do bilhete que deverá ser emitido, <b>por meio de preenchimento do formulário
        SEAD_ESCOLHA_PASSAGENS</b>.</p>
        <br>
        <p>Ressaltamos que a ordem de prioridade para escolha do voo deve seguir as
        disposições do Decreto Estadual Nº 14.891/2012, conforme segue:</p>
        <br>
        <p style="margin-left: 40px;"><i>Art. 6º Para aquisição de passagens aéreas, a Secretaria de Administração observará
        condições de aquisição semelhantes ao setor privado, devendo:</i></p>
        <br>
        <p style="margin-left: 40px;"><i>I - <b>solicitar a passagem pelo menor preço</b> dentre aqueles oferecidos pelas companhias
        aéreas, inclusive os decorrentes da aplicação de tarifas promocionais ou reduzidas para
        horários compatíveis com a programação da viagem;</i></p>
        <br>
        <p style="margin-left: 40px;"><i>[...]</i></p>
        <br>
        <p style="margin-left: 40px;"><i>III - a autorização da emissão do bilhete deverá ser realizada considerando o horário e o
        período da participação da autoridade, servidor ou particular no evento, o tempo de
        translado, e a otimização do trabalho, visando garantir condição laborativa produtiva,
        preferencialmente utilizando os seguintes parâmetros:</i></p>
        <br>
        <p style="margin-left: 60px;"><i><b>a) a escolha do vôo deve recair prioritariamente em percursos de menor duração,
        evitando-se, sempre que possível, trechos com escalas e conexões;</b></i></p>
        <br>
        <p style="margin-left: 60px;"><i><b>b) o embarque e o desembarque devem estar compreendidos no período entre sete
        e vinte e uma horas, salvo a inexistência de vôos que atendam a estes horários;</b></i></p>
        <br>
        <p style="margin-left: 60px;"><i><b>em viagens nacionais, deve-se priorizar o horário do desembarque que anteceda
        em no mínimo três horas o início previsto dos trabalhos, evento ou missão;</b> e</i></p>
        <br>
        <p style="margin-left: 60px;"><i><b>d) em viagens internacionais, em que a soma dos trechos da origem até o destino
        ultrapasse oito horas, e que sejam realizadas no período noturno, o embarque,
        prioritariamente, deverá ocorrer com um dia de antecedência.</b></i></p>
        <br>
        <p style="margin-left: 40px;"><i>IV - a emissão do bilhete de passagem aérea deve ser ao menor preço, prevalecendo,
        sempre que possível, a tarifa em classe econômica, observado o disposto no inciso
        anterior e alíneas, e no art. 8º deste Decreto</i></p>
        <br>
        <p style="margin-left: 40px;"><i>[...]</i></p>
        <br>
        <p style="margin-left: 40px;"><i>Art. 8. As passagens aéreas serão adquiridas observando-se as seguintes categorias:</i></p>
        <br>
        <p style="margin-left: 40px;"><i>I - primeira classe, para o Governador e vice-Governador do Estado;</i></p>
        <br>
        <p style="margin-left: 40px;"><i>II - classe executiva, para Secretários e dirigentes máximos de entidades da
        administração indireta;</i></p>
        <br>
        <p style="margin-left: 40px;"><i>III - classe econômica, para os demais casos</i></p>
        <br>
        <p style="margin-left: 40px;"><i>Parágrafo único. Quando não houver primeira classe ou classe executiva, conforme o
        caso, para o trecho desejado, será adquirida passagem, respectivamente, de classe
        executiva e de classe econômica.</i></p>
        <br>
        <p>Após a viagem, solicita-se a juntada nos autos do <i><b>cartão de embarque ou
        congênere</b></i> para a comprovação do embarque.</p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_MEMORANDO_SGA,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Memorando - Encaminhamento de Cotações - {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando memorando de cotações para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar memorando cotações ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Memorando cotações gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar memorando cotações: {e}")
        return None


# ── Documento de Cotações (interno, série 272) ────────────────────────────


def gerar_documento_cotacoes(token, id_procedimento, sei_protocolo, cotacoes_voos):
    """
    Gera documento interno de cotações de passagens (IdSerie 272 — Cotação)
    com tabela HTML formatada de todos os DiariasCotacaoVoo do itinerário.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        cotacoes_voos: list de DiariasCotacaoVoo (ida + volta)

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para documento de cotações.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    voos_ida = [v for v in cotacoes_voos if v.tipo_trecho == 'ida']
    voos_volta = [v for v in cotacoes_voos if v.tipo_trecho == 'volta']

    def _montar_linhas(voos):
        linhas = ""
        for voo in voos:
            saida_fmt = _formatar_data_hora(voo.saida) if voo.saida else ''
            chegada_fmt = _formatar_data_hora(voo.chegada) if voo.chegada else ''
            origem = voo.origem or ''
            destino = voo.destino or ''
            if voo.tem_conexao and voo.destino_conexao:
                destino = f"{voo.destino} → {voo.destino_conexao}"
            bagagem = voo.bagagem or '—'
            valor = _formatar_valor_brl(voo.valor)
            linhas += f"""
                <tr>
                    <td style="padding:4px 6px; border:1px solid #999;">{voo.cia or ''}</td>
                    <td style="padding:4px 6px; border:1px solid #999;">{voo.voo or ''}</td>
                    <td style="padding:4px 6px; border:1px solid #999;">{saida_fmt}</td>
                    <td style="padding:4px 6px; border:1px solid #999;">{chegada_fmt}</td>
                    <td style="padding:4px 6px; border:1px solid #999;">{origem}</td>
                    <td style="padding:4px 6px; border:1px solid #999;">{destino}</td>
                    <td style="padding:4px 6px; border:1px solid #999;">{bagagem}</td>
                    <td style="padding:4px 6px; border:1px solid #999; text-align:right;">{valor}</td>
                </tr>"""
        return linhas

    cabecalho = """
                <tr style="background:#f0f0f0; font-weight:bold;">
                    <td style="padding:4px 6px; border:1px solid #999;">CIA</td>
                    <td style="padding:4px 6px; border:1px solid #999;">Voo</td>
                    <td style="padding:4px 6px; border:1px solid #999;">Saída</td>
                    <td style="padding:4px 6px; border:1px solid #999;">Chegada</td>
                    <td style="padding:4px 6px; border:1px solid #999;">Origem</td>
                    <td style="padding:4px 6px; border:1px solid #999;">Destino</td>
                    <td style="padding:4px 6px; border:1px solid #999;">Bagagem</td>
                    <td style="padding:4px 6px; border:1px solid #999; text-align:right;">Valor</td>
                </tr>"""

    html_ida = ""
    if voos_ida:
        html_ida = f"""
        <p><b>TRECHO IDA</b> ({len(voos_ida)} opção(ões))</p>
        <table style="width:100%; border-collapse:collapse; font-size:10pt; margin-bottom:12px;">
            {cabecalho}
            {_montar_linhas(voos_ida)}
        </table>"""

    html_volta = ""
    if voos_volta:
        html_volta = f"""
        <p><b>TRECHO VOLTA</b> ({len(voos_volta)} opção(ões))</p>
        <table style="width:100%; border-collapse:collapse; font-size:10pt; margin-bottom:12px;">
            {cabecalho}
            {_montar_linhas(voos_volta)}
        </table>"""

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 11pt;">
        <p style="text-align: center;"><b>COTAÇÕES DE PASSAGENS AÉREAS</b></p>
        <p style="text-align: center;">Processo {sei_protocolo}</p>
        <br>
        {html_ida}
        {html_volta}
        <br>
        <p><i>Documento gerado automaticamente pelo SGC — Módulo Diárias.</i></p>
    </div>
    """

    # Série 272 (Cotação) é de Aplicabilidade Externa — enviar como doc externo
    # Converte HTML para PDF em memória e envia via adicionar_documento_externo
    try:
        html_completo = f"""<html><head><meta charset="utf-8"><style>
            body {{ font-family: Arial, sans-serif; font-size: 11pt; margin: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            td, th {{ padding: 4px 6px; border: 1px solid #999; }}
        </style></head><body>{conteudo_html}</body></html>"""

        # Tenta converter para PDF via weasyprint ou pdfkit (se disponível)
        pdf_bytes = None
        try:
            from weasyprint import HTML as WeasyprintHTML
            pdf_bytes = WeasyprintHTML(string=html_completo).write_pdf()
        except ImportError:
            pass

        if not pdf_bytes:
            try:
                import pdfkit
                pdf_bytes = pdfkit.from_string(html_completo, False)
            except (ImportError, Exception):
                pass

        if pdf_bytes:
            retorno = adicionar_documento_externo(
                token=token,
                protocolo_formatado=sei_protocolo,
                arquivo_bytes=pdf_bytes,
                nome_arquivo='cotacoes_passagens.pdf',
                descricao=f'Cotações de Passagens - {sei_protocolo}',
                id_serie=ID_SERIE_COTACAO,
            )
            return retorno
        else:
            # Fallback: envia o HTML como arquivo .html
            html_bytes = html_completo.encode('utf-8')
            retorno = adicionar_documento_externo(
                token=token,
                protocolo_formatado=sei_protocolo,
                arquivo_bytes=html_bytes,
                nome_arquivo='cotacoes_passagens.html',
                descricao=f'Cotações de Passagens - {sei_protocolo}',
                id_serie=ID_SERIE_COTACAO,
            )
            return retorno
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao preparar cotações como doc externo: {e}")
        return None

    # Código legado (mantido como referência — série 272 não aceita doc interno)
    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_COTACAO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Cotações de Passagens - {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando documento de cotações para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar documento cotações ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Documento cotações gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar documento cotações: {e}")
        return None


# ── Autorização do Secretário ─────────────────────────────────────────────


def gerar_autorizacao_secretario(token, id_procedimento, tipo_solicitacao_id, sei_protocolo,
                                 nome_assinante=None, cargo_assinante=None):
    """
    Cria documento SEAD_AUTORIZACAO_DO_SECRETARIO (IdSerie 574) no processo SEI.

    O texto do documento varia conforme o tipo da solicitação:
      - Tipo 1 (Apenas Diárias): "Autorizo o pagamento de diárias..."
      - Tipo 2 (Diárias + Passagens): "Autorizo a compra das passagens e o pagamento de diárias..."
      - Tipo 3 (Apenas Passagens): "Autorizo a compra das passagens..."

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        tipo_solicitacao_id: 1, 2 ou 3
        sei_protocolo: Protocolo formatado do processo
        nome_assinante: Nome do assinante (dinâmico via current_user)
        cargo_assinante: Cargo do assinante (dinâmico via auth SEI)

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para autorização do secretário.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    # Resolve nome e cargo do assinante dinamicamente
    _nome = nome_assinante
    _cargo = cargo_assinante or 'Secretário de Administração do Estado do Piauí'
    if not _nome:
        try:
            from flask_login import current_user
            _nome = current_user.nome.upper() if current_user and current_user.nome else ''
        except Exception:
            _nome = ''

    # Determina texto conforme tipo de solicitação
    tipo_id = int(tipo_solicitacao_id) if tipo_solicitacao_id else 2
    if tipo_id == 1:
        texto_autorizo = "Autorizo o pagamento de diárias"
    elif tipo_id == 3:
        texto_autorizo = "Autorizo a compra das passagens"
    else:
        # Tipo 2 (padrão) — Diárias + Passagens
        texto_autorizo = "Autorizo a compra das passagens e o pagamento de diárias"

    # O conteúdo deve incluir texto + bloco de assinatura visual.
    # Usamos Tipo="D" (documento em branco) para evitar que o template
    # da série 574 insira o bloco "AUTORIZO NA FORMA DA LEI" duplicado.
    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <br><br>
        <p style="text-indent: 2em; text-align: justify;">
            <b>{_escape_html(texto_autorizo)}</b> e encaminho os
            autos a Superintendência de Gestão Administrativa - SGA,
            <u>para conhecimento e providências
            necessárias, devendo ser observados os procedimentos legais.</u>
        </p>
        <br><br>
        <p style="text-align: center;">
            <i>(assinado eletronicamente)</i><br>
            <b>{_escape_html(_nome)}</b><br>
            {_escape_html(_cargo)}
        </p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_AUTORIZACAO_SECRETARIO,
        "Tipo": "D",
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Autorização do Secretário - {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando autorização do secretário para procedimento {id_procedimento} "
            f"(tipo={tipo_id})..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar autorização ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Autorização do secretário gerada - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar autorização do secretário: {e}")
        return None


# ── Análise de Diárias (idSerie 7) ───────────────────────────────────────

ID_SERIE_ANALISE = "7"  # "SEAD_ANALISE"
ID_SERIE_PRESTACAO_CONTAS = "1908"  # Documento de prestação de contas
LIMITE_DIARIAS_ANUAL = 180  # Decreto Estadual nº 14.910/2012, art. 7º


def verificar_elegibilidade_servidor(cpf, ano=None):
    """
    Verifica se um servidor está apto a receber novas diárias.

    Regras (Decreto Estadual nº 14.910/2012):
    1. Total acumulado de diárias no ano < 180 (art. 7º)
    2. Prestação de contas da última viagem aprovada (art. 12, §2º)

    A verificação da prestação de contas ocorre em duas etapas:
    a) Primeiro verifica na tabela local (diarias_controle_prestacao)
    b) Se pendente, busca documento idSerie 1908 no último processo SEI

    Args:
        cpf: CPF do servidor (com ou sem formatação)
        ano: Ano de referência (default: ano corrente)

    Returns:
        dict com {
            apto: bool,
            acumulado: float (total diárias no ano),
            limite: int (180),
            prestacao_status: str ('APROVADO', 'PENDENTE', 'N/A'),
            ultima_viagem: dict ou None,
            motivo_bloqueio: str ou None,
        }
    """
    from app.extensions import db
    from app.models.diaria import (
        DiariasControleServidor, DiariasControleViagem, DiariasControlePrestacao,
    )

    if not ano:
        ano = date.today().year

    resultado = {
        'apto': True,
        'acumulado': 0.0,
        'limite': LIMITE_DIARIAS_ANUAL,
        'prestacao_status': 'N/A',
        'ultima_viagem': None,
        'motivo_bloqueio': None,
    }

    cpf_limpo = cpf.strip().replace('.', '').replace('-', '')

    # 1. Calcular acumulado anual
    acumulado_query = db.session.query(
        db.func.coalesce(db.func.sum(DiariasControleServidor.qtd_diarias), 0)
    ).join(
        DiariasControleViagem,
        DiariasControleServidor.viagem_id == DiariasControleViagem.id
    ).filter(
        DiariasControleServidor.cpf == cpf_limpo,
        DiariasControleViagem.status_viagem == DiariasControleViagem.STATUS_REALIZADA,
        db.extract('year', DiariasControleViagem.data_inicio) == ano,
    )
    acumulado = float(acumulado_query.scalar() or 0)
    resultado['acumulado'] = acumulado

    if acumulado >= LIMITE_DIARIAS_ANUAL:
        resultado['apto'] = False
        resultado['motivo_bloqueio'] = (
            f'Limite anual atingido: {acumulado:.1f} de {LIMITE_DIARIAS_ANUAL} diárias '
            f'(Decreto 14.910/2012, art. 7º)'
        )
        return resultado

    # 2. Buscar última viagem realizada do servidor
    ultima = db.session.query(
        DiariasControleServidor
    ).join(
        DiariasControleViagem,
        DiariasControleServidor.viagem_id == DiariasControleViagem.id
    ).filter(
        DiariasControleServidor.cpf == cpf_limpo,
        DiariasControleViagem.status_viagem == DiariasControleViagem.STATUS_REALIZADA,
    ).order_by(
        DiariasControleViagem.data_termino.desc()
    ).first()

    if not ultima:
        # Sem viagem anterior — apto (primeira viagem)
        resultado['prestacao_status'] = 'N/A'
        return resultado

    resultado['ultima_viagem'] = {
        'processo': ultima.viagem.processo if ultima.viagem else None,
        'data_inicio': str(ultima.viagem.data_inicio) if ultima.viagem else None,
        'data_termino': str(ultima.viagem.data_termino) if ultima.viagem else None,
    }

    # 3. Verificar prestação de contas na tabela local
    if ultima.prestacao:
        if ultima.prestacao.relatorio == DiariasControlePrestacao.RELATORIO_APROVADO:
            resultado['prestacao_status'] = 'APROVADO'
            return resultado
        elif ultima.prestacao.status == DiariasControlePrestacao.STATUS_ENTREGUE:
            # Entregue mas ainda não aprovada — verificamos via SEI também
            resultado['prestacao_status'] = 'ENTREGUE'
        else:
            resultado['prestacao_status'] = 'PENDENTE'

    # 4. Verificação complementar via SEI (busca idSerie 1908)
    if ultima.viagem and ultima.viagem.processo:
        try:
            resp_docs = consultar_documentos_procedimento(ultima.viagem.processo)
            if resp_docs.get('sucesso'):
                for doc in resp_docs['documentos']:
                    serie = doc.get('Serie', {})
                    if str(serie.get('IdSerie', '')) == ID_SERIE_PRESTACAO_CONTAS:
                        resultado['prestacao_status'] = 'APROVADO'
                        current_app.logger.info(
                            f"SEI Diárias: Prestação de contas encontrada via SEI "
                            f"para CPF {cpf_limpo} no processo {ultima.viagem.processo}"
                        )
                        return resultado
        except Exception as e:
            current_app.logger.warning(
                f"SEI Diárias: Erro ao verificar prestação via SEI: {e}"
            )

    # Se chegou aqui e prestação não é APROVADO, bloqueia
    if resultado['prestacao_status'] != 'APROVADO':
        resultado['apto'] = False
        resultado['motivo_bloqueio'] = (
            f'Prestação de contas pendente para a última viagem '
            f'(processo {ultima.viagem.processo if ultima.viagem else "?"}). '
            f'Decreto 14.910/2012, art. 12, §2º'
        )

    return resultado


def gerar_analise_diarias(token, id_procedimento, sei_protocolo, servidores_analise):
    """
    Gera documento SEAD_ANALISE (IdSerie 7) no processo SEI.

    Conteúdo: tabela com servidor, quantidade acumulada e status da prestação.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        servidores_analise: lista de dicts com {
            nome: str,
            acumulado: float,
            prestacao_status: str ('APROVADO', 'PENDENTE', 'N/A'),
            apto: bool,
        }

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para análise.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_CCDP}/documentos"

    ano_atual = date.today().year

    # Monta linhas da tabela
    linhas_html = ''
    for s in servidores_analise:
        nome = s.get('nome', '?')
        acumulado = s.get('acumulado', 0)
        prestacao = s.get('prestacao_status', 'N/A')
        linhas_html += f"""
            <tr>
                <td style="border: 1px solid #000; padding: 6px;">{nome}</td>
                <td style="border: 1px solid #000; padding: 6px; text-align: center;">
                    {acumulado:.1f}
                </td>
                <td style="border: 1px solid #000; padding: 6px; text-align: center;">
                    {prestacao}
                </td>
            </tr>
        """

    # Determina conclusão
    todos_aptos = all(s.get('apto', False) for s in servidores_analise)
    if todos_aptos:
        conclusao = 'Do exposto, o(a) servidor(a) está apto(a) a receber novas diárias.'
    else:
        nomes_bloqueados = [s['nome'] for s in servidores_analise if not s.get('apto')]
        conclusao = (
            'Do exposto, o(s) seguinte(s) servidor(es) <b>não está(ão) apto(s)</b> '
            f'a receber novas diárias: {", ".join(nomes_bloqueados)}.'
        )

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p><b>PROCESSO Nº {sei_protocolo}</b></p>
        <p><b>INTERESSADO:</b> @INTERESSADOS_VIRGULA_ESPACO_MAIUSCULAS@</p>

        <p style="text-align: center;"><b>RELATÓRIO DE ANÁLISE</b></p>

        <p style="text-indent: 2em; text-align: justify;">
            Trata-se de análise preliminar sobre o quantitativo de diárias recebidas pelo(a)
            servidor(a) e o resultado de suas prestações de contas no exercício anterior.
        </p>

        <p style="text-indent: 2em; text-align: justify;">
            Conforme o art. 7º do Decreto Estadual nº 14.910/2012, "<i>o total das diárias
            atribuídas a militar, servidor ou empregado público não poderá exceder de 180 (cento e oitenta)
            por ano, salvo em casos especiais, previamente autorizados pelo Governador do Estado</i>".
        </p>

        <p style="text-indent: 2em; text-align: justify;">
            Também, o §2º do art. 12, do referido Decreto Estadual, diz que "<i>a falta de
            comprovação do deslocamento no prazo previsto, inabilita o servidor a receber novas diárias,
            salvo em casos excepcionais, de comprovado interesse público e devidamente justificado pelo
            chefe imediato</i>".
        </p>

        <p style="text-indent: 2em; text-align: justify;">
            Portanto, diante do processo de diárias e após a verificação do cumprimento legal
            da legislação vigente, apresento o(a) servidor(a) com a quantidade de diárias acumuladas
            recebidas durante o <b>ano {ano_atual}</b>, bem como a sua habilitação para receber novas diárias:
        </p>

        <br>
        <table style="border-collapse: collapse; width: 100%; margin: 10px 0;">
            <thead>
                <tr style="background-color: #f0f0f0;">
                    <th style="border: 1px solid #000; padding: 8px;">Servidor/Terceirizado</th>
                    <th style="border: 1px solid #000; padding: 8px;">Quant. Acumulada</th>
                    <th style="border: 1px solid #000; padding: 8px;">Prestação de Contas Anterior</th>
                </tr>
            </thead>
            <tbody>
                {linhas_html}
            </tbody>
        </table>
        <br>

        <p style="text-indent: 2em; text-align: justify;">{conclusao}</p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_ANALISE,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Análise de Diárias - {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando análise para procedimento {id_procedimento} "
            f"({len(servidores_analise)} servidores)..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar análise ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Análise gerada - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar análise: {e}")
        return None


def gerar_nota_empenho(token, id_procedimento, sei_protocolo, codigo_ne, dados_empenho=None):
    """
    Gera documento Nota de Empenho (idSerie 419) como documento EXTERNO no SEI.

    No processo modelo, a NE é registrada como documento externo na unidade CCDP.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID interno do procedimento SEI
        sei_protocolo: Número formatado do processo
        codigo_ne: Código da NE (ex: '2026NE00456')
        dados_empenho: dict opcional (não usado para doc externo)

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para NE.")
        return None

    current_app.logger.info(
        f"SEI Diarias: Gerando NE {codigo_ne} no procedimento {id_procedimento}..."
    )

    # NE é documento EXTERNO (série 419) — cria na unidade CCDP
    # Gera um HTML simples como conteúdo do documento
    html = f"""<html><body style="font-family:Arial;font-size:12pt;">
    <h3 style="text-align:center;">NOTA DE EMPENHO</h3>
    <p><b>NE:</b> {_escape_html(codigo_ne)}</p>
    <p><b>Processo:</b> {_escape_html(sei_protocolo)}</p>
    </body></html>"""

    html_bytes = html.encode('utf-8')

    try:
        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=sei_protocolo,
            arquivo_bytes=html_bytes,
            nome_arquivo=f'NE_{codigo_ne}.html',
            descricao=f'Nota de Empenho {codigo_ne}',
            id_serie=ID_SERIE_NOTA_EMPENHO,
            numero=codigo_ne,
            unidade_id=UNIDADE_CCDP,
        )

        if retorno:
            current_app.logger.info(
                f"SEI Diarias: NE gerada - {retorno.get('DocumentoFormatado', retorno)}"
            )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diarias: Erro ao gerar NE: {e}")
        return None


# ── Despacho CCDP → SGA (idSerie 754, pós Nota de Empenho) ────────────────


def gerar_despacho_ccdp(token, id_procedimento, sei_protocolo, interessados=None, itinerario=None,
                       nome_assinante=None, cargo_assinante=None):
    """
    Gera o Despacho CCDP (série 754) após emissão da Nota de Empenho.

    Retorna os autos à Superintendência de Gestão Administrativa para
    conhecimento e providências referente à concessão de diárias.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        interessados: lista de nomes dos interessados (opcional)
        itinerario: objeto DiariasItinerario para resolver interessados (opcional)

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho CCDP.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_CCDP}/documentos"

    # Titular CCDP — IGNORA args do chamador. Despacho sempre assinado pelo
    # Coordenador da CCDP (cargo_gestao='coordenador_ccdp').
    nome_ccdp, cargo_ccdp = _resolver_titular_por_cargo('coordenador_ccdp')
    nome_final = nome_ccdp or 'COORDENAÇÃO DE CONTROLE DE DIÁRIAS E PASSAGENS'
    cargo_final = cargo_ccdp or 'Coordenação de Controle de Diárias e Passagens - SEAD-PI'

    corpo = (
        'Após a realização de análise técnica e emissão da nota de empenho, retorno os '
        'autos à <b>Superintendência de Gestão Administrativa</b>, para conhecimento e '
        'providências referente à concessão de diárias.'
    )
    conteudo_html = _montar_despacho_html(corpo, nome_final, cargo_final)

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho CCDP - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho CCDP para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho CCDP ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho CCDP gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho CCDP: {e}")
        return None


# ── Despacho SGA → NCI (idSerie 2987) ─────────────────────────────────────


def gerar_despacho_sga(token, id_procedimento, sei_protocolo, ref_despacho_ccdp_id,
                       ref_despacho_ccdp_formatado, nome_assinante=None, cargo_assinante=None,
                       interessados=None, itinerario=None):
    """
    Gera o Despacho SGA (série 2987) assinado pelo Superintendente.

    Encaminha os autos ao NCI para análise, referenciando o despacho CCDP anterior.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        ref_despacho_ccdp_id: ID do despacho CCDP anterior (para referência)
        ref_despacho_ccdp_formatado: Número formatado do despacho CCDP
        nome_assinante: Nome do assinante (default: busca do current_user ou fallback)
        cargo_assinante: Cargo do assinante (default: 'Superintendente de Gestão Administrativa – SEAD')

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho SGA.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_APOIOSGA}/documentos"

    # Texto do link = apenas o número formatado (legível). Fallback p/ sei_id.
    ref_texto = ref_despacho_ccdp_formatado or ref_despacho_ccdp_id or ''

    # Titular SGA — IGNORA args do chamador. Despacho SGA sempre assinado pelo
    # Superintendente de Gestão Administrativa (cargo_gestao='superintendente').
    nome_sga, cargo_sga = _resolver_titular_por_cargo('superintendente')
    nome_final = nome_sga or 'SUPERINTENDÊNCIA DE GESTÃO ADMINISTRATIVA'
    cargo_final = cargo_sga or 'Superintendente de Gestão Administrativa – SEAD'

    # Monta a referência ao despacho CCDP no formato "despacho (<link>)".
    # O link aponta para o documento individual no SEI via protocolo_visualizar.
    if ref_despacho_ccdp_id and ref_texto:
        link_ref = (
            'https://sei.pi.gov.br/sei/controlador.php'
            '?acao=protocolo_visualizar'
            f'&id_protocolo={ref_despacho_ccdp_id}'
            f'&infra_sistema=100000100&infra_unidade_atual={UNIDADE_SEAD}'
        )
        ref_html = f'despacho (<a href="{link_ref}" target="_blank">{_escape_html(ref_texto)}</a>)'
    elif ref_texto:
        ref_html = f'despacho ({_escape_html(ref_texto)})'
    else:
        ref_html = 'despacho'

    corpo = (
        f'Em atenção ao {ref_html}, '
        'da COORDENAÇÃO DE CONTROLE DE DIÁRIAS E PASSAGENS, encaminhamos os autos '
        'para análise e demais providências necessárias.'
    )
    conteudo_html = _montar_despacho_html(
        corpo, nome_final, cargo_final,
        para_linha='PARA: NÚCLEO DE CONTROLE INTERNO - NCI',
    )

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO_SGA,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho SGA → NCI - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho SGA para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho SGA ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho SGA gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho SGA: {e}")
        return None


def gerar_despacho_sga_negacao(token, id_procedimento, sei_protocolo,
                                justificativa, unidade_geradora_descricao,
                                nome_assinante=None, cargo_assinante=None):
    """
    Gera o Despacho SGA de Negação (série 2987) assinado pelo Superintendente.

    O despacho contém a justificativa da negação e é direcionado à unidade
    solicitante (unidade_geradora_descricao).

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        justificativa: Texto da justificativa de negação
        unidade_geradora_descricao: Descrição da caixa SEI do solicitante
        nome_assinante: Nome do assinante (default: busca do titular)
        cargo_assinante: Cargo do assinante

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho negação.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_APOIOSGA}/documentos"

    nome_sga, cargo_sga = _resolver_titular_por_cargo('superintendente')
    nome_final = nome_assinante or nome_sga or 'SUPERINTENDÊNCIA DE GESTÃO ADMINISTRATIVA'
    cargo_final = cargo_assinante or cargo_sga or 'Superintendente de Gestão Administrativa – SEAD'

    corpo = (
        'Considerando a análise da solicitação, nego o prosseguimento '
        'pelos motivos abaixo:<br><br>'
        f'{_escape_html(justificativa)}'
    )
    conteudo_html = _montar_despacho_html(
        corpo, nome_final, cargo_final,
        para_linha=f'PARA: {unidade_geradora_descricao}',
    )

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO_SGA,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho SGA - Negação - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho negação para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho negação ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho negação gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho negação: {e}")
        return None


# ── Análise de Pagamento NCI (idSerie 461) ────────────────────────────────

# Perguntas da Análise de Pagamento (roteiro de diárias)
PERGUNTAS_ANALISE_PAGAMENTO = [
    {"id": "q1", "texto": "A(s) diária(s) foi(ram) solicitada(s) para militares, servidores públicos, empregados públicos, ou agentes políticos que, em caráter eventual ou transitório, e no interesse do serviço, deslocarem-se da localidade onde têm exercício para outro ponto do território estadual, nacional ou estrangeiro?",
     "campos_extras": ["vinculo", "nome_agente"]},
    {"id": "q2", "texto": "O servidor é o gestor máximo do órgão ou entidade?"},
    {"id": "q3", "texto": "Foi avaliado a oportunidade e a necessidade da viagem e aptidão do servidor antes da autorização?",
     "campos_extras": ["quem_analisou"]},
    {"id": "q4", "texto": "O valor da diária corresponde ao valor do cargo de autoridade superior?",
     "campos_extras": ["classe_correspondente"]},
    {"id": "q5", "texto": "O agente é autoridade superior no órgão ou entidade?",
     "campos_extras": ["cargo_agente"]},
    {"id": "q6", "texto": "No caso de servidor não ser autoridade superior, a diária é equivalente ao cargo do servidor?"},
    {"id": "q7", "texto": "O servidor indicado para acompanhar a autoridade possui cargo igual ou inferior ao da autoridade que acompanha?"},
    {"id": "q8", "texto": "A diária foi majorada em até 30% quando a despesa com hospedagem for coberta pelo servidor?"},
    {"id": "q9", "texto": "O deslocamento da sede para o aeroporto ou rodoviária, a diária é de 50%?"},
    {"id": "q10", "texto": "O valor das diárias está com base nos anexos I e II do decreto 20.890/2022 e alterações posteriores?",
     "campos_extras": ["valor_diaria", "destino_viagem"]},
    {"id": "q11", "texto": "As diárias a serem concedidas estão dentro do período da viagem?"},
    {"id": "q12", "texto": "Em caso de prorrogação, a viagem foi comunicada ao concedente?"},
    {"id": "q13", "texto": "Foi demonstrada a disponibilidade orçamentário-financeira para a execução da despesa?",
     "campos_extras": ["projeto_natureza", "fonte_recursos"]},
    {"id": "q14", "texto": "O servidor realizou viagem anterior a serviço do Estado com direito a recebimento de diárias?"},
    {"id": "q15", "texto": "O servidor prestou contas das diárias recebidas anteriormente?"},
    {"id": "q16", "texto": "Foi aprovada a prestação de contas da viagem anterior feita pelo servidor?",
     "campos_extras": ["responsavel_analise"]},
    {"id": "q17", "texto": "A viagem foi autorizada pelo ordenador de despesa?",
     "campos_extras": ["quem_autorizou"]},
    {"id": "q18", "texto": "A publicação da portaria de diárias foi realizada no DOE antes da viagem?"},
    {"id": "q19", "texto": "Trata-se de despesas de exercícios anteriores?"},
    {"id": "q20", "texto": "Em caso de DEA, foi apresentada a justificativa para o não pagamento no exercício?"},
    {"id": "q21", "texto": "Em caso de DEA, a despesa foi reconhecida pela autoridade competente?"},
]


def gerar_analise_pagamento(token, id_procedimento, sei_protocolo, respostas,
                            observacoes=None):
    """
    Gera documento SINCIN Análise de Pagamento (série 461) no processo SEI.

    Cria o parecer do NCI com as 21 perguntas S/N e a conclusão.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        respostas: dict com respostas {
            'q1': {'resposta': 'S'/'N', 'observacao': '...', 'vinculo': '...', ...},
            'q2': {'resposta': 'S'/'N'},
            ...
        }
        observacoes: texto geral de observações

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para análise de pagamento.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_NCI}/documentos"

    hoje = date.today()
    referencia = f"{MESES_EXTENSO[hoje.month].capitalize()}/{hoje.year}"

    # Monta as linhas da tabela de perguntas
    linhas_perguntas = ''
    questoes_na = []  # questões que não se aplicam

    for pergunta in PERGUNTAS_ANALISE_PAGAMENTO:
        pid = pergunta['id']
        resp_data = respostas.get(pid, {})
        resp = resp_data.get('resposta', '')
        obs = resp_data.get('observacao', '')

        if resp == 'NA':
            questoes_na.append(pid.replace('q', ''))
            continue

        sim_x = '<b><i>X</i></b>' if resp == 'S' else ''
        nao_x = '<b><i>X</i></b>' if resp == 'N' else ''

        # Campos extras (vinculo, nome, etc.)
        extras_html = ''
        for campo in pergunta.get('campos_extras', []):
            valor = resp_data.get(campo, '')
            if valor:
                label = campo.replace('_', ' ').title()
                extras_html += f'<br><i style="margin-left:20px;">{label}: <b>{valor}</b></i>'

        if obs:
            extras_html += f'<br><i style="margin-left:20px;">Observação: {obs}</i>'

        num = pid.replace('q', '')
        linhas_perguntas += f"""
            <tr>
                <td style="border:1px solid #000; padding:6px; text-align:left;">
                    {num}. {pergunta['texto']}{extras_html}
                </td>
                <td style="border:1px solid #000; padding:6px; text-align:center; width:30px;">{sim_x}</td>
                <td style="border:1px solid #000; padding:6px; text-align:center; width:30px;">{nao_x}</td>
            </tr>"""

    # Observação das questões não aplicáveis
    na_texto = ''
    if questoes_na:
        na_texto = f'<p><i>Observação: Conforme estabelecido no roteiro, as seguintes questões não se aplicaram à análise: {", ".join(questoes_na)}.</i></p>'

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 11pt;">
        <h3 style="text-align: center;">GOVERNO DO ESTADO DO PIAUÍ<br>CONTROLADORIA-GERAL DO ESTADO</h3>
        <br>
        <table style="width:100%; border-collapse:collapse; margin-bottom:15px;">
            <tr>
                <td style="border:1px solid #000; padding:6px; font-weight:bold; width:30%;">PROCESSO</td>
                <td style="border:1px solid #000; padding:6px;">{sei_protocolo}</td>
            </tr>
            <tr>
                <td style="border:1px solid #000; padding:6px; font-weight:bold;">REFERÊNCIA</td>
                <td style="border:1px solid #000; padding:6px;">{referencia}</td>
            </tr>
        </table>

        <h4>I. Introdução</h4>
        <p style="text-align: justify;">
            Com amparo no Decreto Estadual n. 17.526, de 04/12/2017, analisei o processo nº <b>{sei_protocolo}</b>,
            referente a <b>PAGAMENTO DE DIÁRIAS</b>, conforme roteiro de Pagamento diárias, previamente definido
            pela Superintendência de Controladoria Geral do Estado (SUPCGE), com respaldo no art. 21, § 2° da Lei
            7.884/2022, conforme demonstrado a seguir.
        </p>

        <h4>II. Análise</h4>
        <p style="text-align:right;"><small>Legenda: S = Sim &nbsp; N = Não</small></p>
        <table style="width:100%; border-collapse:collapse; border:2px solid #000;">
            <thead>
                <tr style="background-color:#d9e2f3;">
                    <th style="border:1px solid #000; padding:6px;">Pergunta</th>
                    <th style="border:1px solid #000; padding:6px; width:30px;">S</th>
                    <th style="border:1px solid #000; padding:6px; width:30px;">N</th>
                </tr>
            </thead>
            <tbody>
                {linhas_perguntas}
            </tbody>
        </table>
        {na_texto}

        <h4>III. Conclusão</h4>
        <p style="text-align: justify;">
            Em face das constatações apresentadas acima, conclui-se que os requisitos técnico-econômicos da
            operação foram cumpridos em seus aspectos relevantes, competindo ao gestor decidir sobre a conveniência e
            oportunidade da autorização.
        </p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_ANALISE_PAGAMENTO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Análise de Pagamento - {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando análise de pagamento para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar análise ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Análise de pagamento gerada - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar análise de pagamento: {e}")
        return None


# ── Despacho NCI (idSerie 5) ──────────────────────────────────────────────


def gerar_despacho_nci(token, id_procedimento, sei_protocolo,
                       ref_analise_formatado=None,
                       nome_assinante=None, cargo_assinante=None):
    """
    Gera o Despacho do NCI (série 5) encaminhando para pagamento.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo
        ref_analise_formatado: Número formatado da análise de pagamento (para referência)

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho NCI.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_NCI}/documentos"

    hoje = date.today()
    data_extenso = f"{hoje.day} DE {MESES_EXTENSO[hoje.month].upper()} DE {hoje.year}"

    ref_analise = ''
    if ref_analise_formatado:
        ref_analise = f"""
        <p style="text-indent: 2em; text-align: justify;">
            Após exame da documentação inserida no presente processo, foi
            incluída a Análise do Núcleo de Controle Interno nº {ref_analise_formatado} - Conclusão:
            Regular.
        </p>"""

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p><b>PROCESSO Nº: {sei_protocolo}</b></p>
        <p><b>PARA:</b> SUPERINTENDÊNCIA DE GESTÃO ADMINISTRATIVA E CONTROLE DOS GASTOS - SEAD-PI</p>
        <br>
        <p style="text-indent: 2em; text-align: justify;">
            Trata-se de processo nº {sei_protocolo} relativo ao pagamento de diárias e passagens.
        </p>
        {ref_analise}
        <br>
        <p style="text-indent: 2em;">Sem mais,</p>
        <p style="text-indent: 2em;">Encaminha-se para pagamento.</p>
        <p style="text-indent: 2em;">Atenciosamente,</p>
            {_bloco_assinatura(nome_assinante, cargo_assinante)}
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO_NCI,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho NCI - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho NCI para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho NCI ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho NCI gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho NCI: {e}")
        return None


# ── Despacho APOIO/DFIN (idSerie 754) ───────────────────────────────────────

def gerar_despacho_apoio(token, id_procedimento, sei_protocolo, ref_analise_nci_id,
                         interessados=None, itinerario=None,
                       nome_assinante=None, cargo_assinante=None):
    """
    Gera Despacho APOIO/DFIN (série 754) assinado pelo Superintendente.

    Conteúdo: "Considerando a ausência de irregularidades na análise do Núcleo de
    Controle Interno - NCI ({ref_analise_nci_id}) e a inexistência de óbices para o
    pagamento, encaminho o processo à Diretoria de Planejamento e Finanças-DFIN
    para pagamento e demais providências pertinentes, devendo ser observados os
    procedimentos legais."

    Criado na unidade UNIDADE_DFIN_APOIO.
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho APOIO.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_DFIN_APOIO}/documentos"

    # Titular APOIOSGA — IGNORA args do chamador. Despacho de apoio assinado
    # pelo Superintendente de Gestão Administrativa.
    nome_sup, cargo_sup = _resolver_titular_por_cargo('superintendente')
    nome_final = nome_sup or 'SUPERINTENDÊNCIA DE GESTÃO ADMINISTRATIVA - SEAD-PI'
    cargo_final = cargo_sup or 'Superintendente de Gestão Administrativa - SEAD-PI'

    corpo = (
        f'Considerando a ausência de irregularidades na análise do Núcleo de Controle '
        f'Interno - NCI ({_escape_html(str(ref_analise_nci_id or ""))}) e a inexistência de '
        'óbices para o pagamento, encaminho o processo à <b>Diretoria de Planejamento e '
        'Finanças-DFIN</b> para pagamento e demais providências pertinentes, '
        '<i>devendo ser observados os procedimentos legais.</i>'
    )
    conteudo_html = _montar_despacho_html(corpo, nome_final, cargo_final)

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho APOIO/DFIN - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho APOIO para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho APOIO ({response.status_code}): {response.text}"
            )

        response.raise_for_status()
        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho APOIO gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho APOIO: {e}")
        return None


# ── Despacho Diretor DFIN (idSerie 754) ─────────────────────────────────────

def gerar_despacho_diretor(token, id_procedimento, sei_protocolo, ref_despacho_apoio_id,
                           interessados=None, itinerario=None,
                       nome_assinante=None, cargo_assinante=None):
    """
    Gera Despacho do Diretor de Planejamento e Finanças (série 754).

    Conteúdo: "Considerando o despacho da SGACG ({ref_despacho_apoio_id}), encaminho o processo à
    GERÊNCIA DE EXECUÇÃO ORÇAMENTÁRIA para liquidação, pagamento e demais
    providências pertinentes, devendo ser observados os procedimentos legais."

    Criado na unidade UNIDADE_DFIN_APOIO.
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho Diretor.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_DFIN_APOIO}/documentos"

    # Titular DFIN — IGNORA args do chamador. Sempre o Diretor de Planejamento e Finanças.
    nome_dfin, cargo_dfin = _resolver_titular_por_cargo('diretor_dfin')
    nome_final = nome_dfin or 'DIRETORIA DE PLANEJAMENTO E FINANÇAS - SEAD-PI'
    cargo_final = cargo_dfin or 'Diretor de Planejamento e Finanças - SEAD-PI'

    corpo = (
        f'Considerando o despacho da <b>SGACG</b> ({_escape_html(str(ref_despacho_apoio_id or ""))}), '
        'encaminho o processo à <b>GERÊNCIA DE EXECUÇÃO ORÇAMENTÁRIA</b> para '
        'liquidação, pagamento e demais providências pertinentes, devendo ser '
        'observados os procedimentos legais.'
    )
    conteudo_html = _montar_despacho_html(corpo, nome_final, cargo_final)

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho Diretor DFIN - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho Diretor para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho Diretor ({response.status_code}): {response.text}"
            )

        response.raise_for_status()
        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho Diretor gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho Diretor: {e}")
        return None


# ── Despacho GEO (idSerie 754) ──────────────────────────────────────────────

def gerar_despacho_geo(token, id_procedimento, sei_protocolo,
                       interessados=None, itinerario=None,
                       nome_assinante=None, cargo_assinante=None):
    """
    Gera Despacho GEO (série 754) assinado pelo Gerente de Execução Orçamentária.

    Conteúdo: "Encaminho os autos à Coordenação de Controle de Diárias e Passagens -
    CCDP para verificação do quantitativo de diárias recebidas, assim como a emissão
    de relatório de análise quanto a aprovação/reprovação da prestação de contas anterior."

    Criado na unidade UNIDADE_GEO.

    Nota: o assinante é SEMPRE o titular de `cargo_gestao='gerente_geo'`.
    Argumentos `nome_assinante`/`cargo_assinante` são ignorados para evitar
    que o cargo do usuário logado (ex: Superintendente operando o fluxo de teste)
    vaze para o PDF final.
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho GEO.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_GEO}/documentos"

    # Ignora args externos — assinatura SEMPRE é do titular da GEO
    nome_geo, cargo_geo = _resolver_titular_por_cargo('gerente_geo')
    nome_final = nome_geo or 'GERÊNCIA DE EXECUÇÃO ORÇAMENTÁRIA - SEAD-PI'
    cargo_final = cargo_geo or 'Gerência de Execução Orçamentária - SEAD-PI'

    corpo = (
        'Encaminho os autos à <b>Coordenação de Controle de Diárias e Passagens - CCDP</b> '
        'para verificação do quantitativo de diárias recebidas, assim como a emissão '
        'de relatório de análise quanto a aprovação/reprovação da prestação de contas anterior.'
    )
    conteudo_html = _montar_despacho_html(corpo, nome_final, cargo_final)

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho GEO - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho GEO para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho GEO ({response.status_code}): {response.text}"
            )

        response.raise_for_status()
        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho GEO gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho GEO: {e}")
        return None


# ── NL - Nota de Liquidação (idSerie 420) ───────────────────────────────────

def gerar_nl(token, id_procedimento, sei_protocolo, codigo_nl):
    """Gera documento NL - Nota de Liquidação (série 420) como doc externo no SEI."""
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para NL.")
        return None
    current_app.logger.info(f"SEI Diárias: Gerando NL {codigo_nl}...")
    html = f'<html><body><p>Nota de Liquidação: {_escape_html(codigo_nl)}</p><p>Processo: {_escape_html(sei_protocolo)}</p></body></html>'
    try:
        return adicionar_documento_externo(
            token=token, protocolo_formatado=sei_protocolo,
            arquivo_bytes=html.encode('utf-8'), nome_arquivo=f'NL_{codigo_nl}.html',
            descricao=f'NL {codigo_nl}', id_serie=ID_SERIE_NL, numero=codigo_nl,
            unidade_id=UNIDADE_CCDP)
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar NL: {e}")
        return None


def gerar_pd(token, id_procedimento, sei_protocolo, codigo_pd):
    """Gera documento PD - Programação de Desembolso (série 421) como doc externo no SEI."""
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para PD.")
        return None
    current_app.logger.info(f"SEI Diárias: Gerando PD {codigo_pd}...")
    html = f'<html><body><p>Programação de Desembolso: {_escape_html(codigo_pd)}</p><p>Processo: {_escape_html(sei_protocolo)}</p></body></html>'
    try:
        return adicionar_documento_externo(
            token=token, protocolo_formatado=sei_protocolo,
            arquivo_bytes=html.encode('utf-8'), nome_arquivo=f'PD_{codigo_pd}.html',
            descricao=f'PD {codigo_pd}', id_serie=ID_SERIE_PD, numero=codigo_pd,
            unidade_id=UNIDADE_CCDP)
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar PD: {e}")
        return None


def gerar_ob(token, id_procedimento, sei_protocolo, codigo_ob):
    """Gera documento OB - Ordem Bancária (série 422) como doc externo no SEI."""
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para OB.")
        return None
    current_app.logger.info(f"SEI Diárias: Gerando OB {codigo_ob}...")
    html = f'<html><body><p>Ordem Bancária: {_escape_html(codigo_ob)}</p><p>Processo: {_escape_html(sei_protocolo)}</p></body></html>'
    try:
        return adicionar_documento_externo(
            token=token, protocolo_formatado=sei_protocolo,
            arquivo_bytes=html.encode('utf-8'), nome_arquivo=f'OB_{codigo_ob}.html',
            descricao=f'OB {codigo_ob}', id_serie=ID_SERIE_OB, numero=codigo_ob,
            unidade_id=UNIDADE_CCDP)
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar OB: {e}")
        return None


# ── Relatório de Viagem (idSerie 1908) ────────────────────────────────────

def gerar_relatorio_viagem(token, id_procedimento, sei_protocolo, dados_relatorio):
    """
    Gera documento SEAD_RELATÓRIO DE VIAGEM (DIÁRIA) (série 1908) no processo SEI.

    O relatório é preenchido pelo solicitante (servidor/viajante) após a conclusão
    da viagem, contendo dados pessoais, dados da viagem e o relato.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID do procedimento SEI
        sei_protocolo: Protocolo formatado do processo (ex: 00002.009305/2025-23)
        dados_relatorio: dict com {
            nome: str (nome do servidor),
            matricula: str,
            cpf: str,
            lotacao: str (setor/orgão),
            cargo_funcao: str,
            periodo_inicio: str (dd/mm/yyyy),
            periodo_fim: str (dd/mm/yyyy),
            qtd_diarias: float,
            valor_diaria: str (formatado R$),
            valor_total: str (formatado R$),
            trajeto: str (ex: TERESINA-PI/SÃO PAULO-SP/TERESINA-PI),
            relato: str (texto livre do relato da viagem),
        }

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para relatório de viagem.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    nome = dados_relatorio.get('nome', '')
    matricula = dados_relatorio.get('matricula', '')
    cpf = dados_relatorio.get('cpf', '')
    lotacao = dados_relatorio.get('lotacao', '')
    cargo_funcao = dados_relatorio.get('cargo_funcao', '')
    periodo_inicio = dados_relatorio.get('periodo_inicio', '')
    periodo_fim = dados_relatorio.get('periodo_fim', '')
    qtd_diarias = dados_relatorio.get('qtd_diarias', '')
    valor_diaria = dados_relatorio.get('valor_diaria', '')
    valor_total = dados_relatorio.get('valor_total', '')
    trajeto = dados_relatorio.get('trajeto', '')
    relato = dados_relatorio.get('relato', '')

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">

        <table border="1" cellpadding="8" cellspacing="0" style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
            <tr>
                <td colspan="2" style="background-color: #f0f0f0; font-weight: bold;">1 - Dados do Servidor</td>
            </tr>
            <tr>
                <td colspan="2"><b>Nome:</b> {nome}</td>
            </tr>
            <tr>
                <td colspan="2"><b>Matrícula:</b> {matricula}</td>
            </tr>
            <tr>
                <td colspan="2"><b>CPF:</b> {cpf}</td>
            </tr>
            <tr>
                <td colspan="2"><b>Lotação:</b> {lotacao}</td>
            </tr>
            <tr>
                <td colspan="2"><b>Cargo/Função:</b> {cargo_funcao}</td>
            </tr>
        </table>

        <table border="1" cellpadding="8" cellspacing="0" style="width: 100%; border-collapse: collapse;">
            <tr>
                <td colspan="2" style="background-color: #f0f0f0; font-weight: bold;">2 - Dados da Viagem</td>
            </tr>
            <tr>
                <td style="width: 50%;"><b>Período:</b> {periodo_inicio} A {periodo_fim}</td>
                <td><b>Quantidade de Diárias:</b> {qtd_diarias}</td>
            </tr>
            <tr>
                <td><b>Valor da Diária (R$):</b> {valor_diaria}</td>
                <td><b>Valor Total (R$):</b> {valor_total}</td>
            </tr>
            <tr>
                <td colspan="2"><b>Trajeto:</b> {trajeto}</td>
            </tr>
            <tr>
                <td colspan="2"><b>Relato da Viagem:</b> {relato}</td>
            </tr>
        </table>

    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_RELATORIO_VIAGEM,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Relatório de Viagem - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando relatório de viagem para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar relatório de viagem ({response.status_code}): {response.text}"
            )

        response.raise_for_status()
        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Relatório de viagem gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar relatório de viagem: {e}")
        return None


# ── NP - Nota Patrimonial (idSerie 423) ──────────────────────────────────

def gerar_np(token, id_procedimento, sei_protocolo, codigo_np):
    """Gera documento NP - Nota Patrimonial (série 423) no processo SEI."""
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para NP.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_CCDP}/documentos"

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p style="text-align: center;"><b>NOTA PATRIMONIAL</b></p>
        <br>
        <p>Processo nº <b>{sei_protocolo}</b></p>
        <p>Código NP: <b>{codigo_np}</b></p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_NP,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Numero": codigo_np,
        "Descricao": f"NP {codigo_np} - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(f"SEI Diárias: Gerando NP {codigo_np}...")
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)
        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar NP ({response.status_code}): {response.text}"
            )
        response.raise_for_status()
        retorno = response.json()
        current_app.logger.info(f"SEI Diárias: NP gerada - {retorno.get('DocumentoFormatado', retorno)}")
        return retorno
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar NP: {e}")
        return None


# ── Despacho Final CCDP (idSerie 754) ────────────────────────────────────

def gerar_despacho_final_ccdp(token, id_procedimento, sei_protocolo,
                              interessados=None, itinerario=None,
                       nome_assinante=None, cargo_assinante=None):
    """
    Gera Despacho Final CCDP (série 754) - "Processo pago e concluído nesta unidade."

    Criado na unidade UNIDADE_CCDP.
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para despacho final CCDP.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_CCDP}/documentos"

    # Titular CCDP — IGNORA args do chamador. Despacho final assinado pelo
    # Coordenador da CCDP (cargo_gestao='coordenador_ccdp').
    nome_ccdp, cargo_ccdp = _resolver_titular_por_cargo('coordenador_ccdp')
    nome_final = nome_ccdp or 'COORDENAÇÃO DE CONTROLE DE DIÁRIAS E PASSAGENS'
    cargo_final = cargo_ccdp or 'Coordenação de Controle de Diárias e Passagens - SEAD-PI'

    corpo = 'Processo pago e concluído nesta unidade.'
    conteudo_html = _montar_despacho_html(corpo, nome_final, cargo_final)

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_DESPACHO,
        "Conteudo": conteudo_html,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
        "Descricao": f"Despacho Final CCDP - Processo {sei_protocolo}",
        "Observacao": "Gerado automaticamente pelo SGC - Módulo Diárias"
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diárias: Gerando despacho final CCDP para procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)
        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar despacho final CCDP ({response.status_code}): {response.text}"
            )
        response.raise_for_status()
        retorno = response.json()
        current_app.logger.info(
            f"SEI Diárias: Despacho final CCDP gerado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho final CCDP: {e}")
        return None


def _vincular_escolha_a_voos(itinerario, dados_escolha):
    """
    Tenta vincular as opcoes escolhidas (extraidas do PDF) aos DiariasCotacaoVoo
    ja importados. Best-effort: se falhar, os dados textuais ficam disponiveis.

    Estrategia:
    - Ordena cotacoes por tipo_trecho (ida/volta) e por id (ordem de insercao = ordem no PDF)
    - Se ha 2 opcoes escolhidas: assume 1a = IDA, 2a = VOLTA
    - Se ha 1 opcao: tenta vincular como IDA
    """
    from app.models.diaria import DiariasCotacaoVoo

    opcoes = dados_escolha.get('opcoes_escolhidas', [])
    if not opcoes:
        return

    voos_ida = DiariasCotacaoVoo.query.filter_by(
        itinerario_id=itinerario.id, tipo_trecho='ida'
    ).order_by(DiariasCotacaoVoo.id).all()

    voos_volta = DiariasCotacaoVoo.query.filter_by(
        itinerario_id=itinerario.id, tipo_trecho='volta'
    ).order_by(DiariasCotacaoVoo.id).all()

    if not voos_ida and not voos_volta:
        return

    try:
        if len(opcoes) >= 2 and voos_ida and voos_volta:
            # 2 opcoes: 1a = IDA, 2a = VOLTA (opcoes sao 1-indexed)
            idx_ida = opcoes[0] - 1
            idx_volta = opcoes[1] - 1
            if 0 <= idx_ida < len(voos_ida):
                itinerario.escolha_voo_ida_id = voos_ida[idx_ida].id
            if 0 <= idx_volta < len(voos_volta):
                itinerario.escolha_voo_volta_id = voos_volta[idx_volta].id
        elif len(opcoes) == 1 and voos_ida:
            idx = opcoes[0] - 1
            if 0 <= idx < len(voos_ida):
                itinerario.escolha_voo_ida_id = voos_ida[idx].id
    except (IndexError, ValueError):
        pass  # Falha silenciosa — dados textuais permanecem


# =============================================================================
# SINCRONIZAÇÃO INDIVIDUAL — Atualizar documentos e etapa a partir do SEI
# =============================================================================

def sincronizar_documentos_diaria(itinerario, force_cotacoes=False):
    """
    Consulta documentos do processo no SEI e atualiza:
    1. Tabela diarias_itinerario_documentos (upsert por tipo_documento)
    2. etapa_atual_id baseada nos documentos encontrados

    Lógica de determinação de etapa (da mais avançada para a mais recente):
        - Tem OB/PD/NL → Concessão (4) ou Prestação (5) se tem relatório
        - Tem NR/análise_pagamento → Análise (3)
        - Tem cotações/escolha_passagens → Escolha do Voo (2)
        - Tem autorização assinada → Escolha do Voo (2) ou Análise (3) conforme tipo
        - Default: Solicitação Inicial (1)

    Args:
        itinerario: objeto DiariasItinerario (com sei_protocolo preenchido)

    Returns:
        dict com {sucesso, docs_atualizados, etapa_anterior, etapa_nova, msgs, erro}
    """
    from app.extensions import db
    from app.constants import DiariasEtapaID, TIPOS_COM_PASSAGENS

    resultado = {
        'sucesso': False,
        'docs_atualizados': 0,
        'docs_encontrados': [],
        'etapa_anterior': itinerario.etapa_atual_id,
        'etapa_nova': itinerario.etapa_atual_id,
        'msgs': [],
        'erro': None,
    }

    if not itinerario.sei_protocolo:
        resultado['erro'] = 'Itinerário sem protocolo SEI.'
        return resultado

    # 1. Buscar documentos do SEI
    res_docs = consultar_documentos_procedimento(itinerario.sei_protocolo)
    if not res_docs['sucesso']:
        resultado['erro'] = res_docs.get('erro', 'Erro ao consultar SEI.')
        return resultado

    documentos_sei = res_docs['documentos']
    resultado['msgs'].append(f'{len(documentos_sei)} documento(s) encontrado(s) no SEI.')

    # 2. Mapear documentos SEI → tipo_documento local e fazer upsert
    tipos_encontrados = set()
    docs_count = 0

    for doc in documentos_sei:
        id_serie = doc.get('Serie', {}).get('IdSerie', '')
        serie_nome = doc.get('Serie', {}).get('Nome', '')
        tipo_doc = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie)

        # Refinamento: IdSerie 264 (Documento Externo) pode ser prestação SCDP
        # Detecta pela descrição ou número quando o doc externo é um comprovante SCDP
        if id_serie == ID_SERIE_DOCUMENTO_EXTERNO:
            texto_ref = ((doc.get('Descricao', '') or '') + ' ' + (doc.get('Numero', '') or '')).lower()
            if any(kw in texto_ref for kw in ('scdp', 'prestação', 'prestacao')):
                tipo_doc = 'prestacao_scdp'

        if not tipo_doc:
            continue

        sei_id = str(doc.get('IdDocumento', ''))
        sei_formatado = str(doc.get('DocumentoFormatado', ''))

        if not sei_id:
            continue

        tipos_encontrados.add(tipo_doc)

        # Upsert: só atualiza se não existe ou se o sei_id é diferente/vazio
        doc_local = itinerario.get_doc(tipo_doc)
        novo = False
        if not doc_local:
            itinerario.set_doc(tipo_doc, sei_id=sei_id, sei_formatado=sei_formatado)
            docs_count += 1
            novo = True
        elif not doc_local.sei_id or doc_local.sei_id != sei_id:
            itinerario.set_doc(tipo_doc, sei_id=sei_id, sei_formatado=sei_formatado)
            docs_count += 1
            novo = True

        resultado['docs_encontrados'].append({
            'tipo': tipo_doc,
            'serie_nome': serie_nome,
            'sei_formatado': sei_formatado,
            'novo': novo,
        })

    resultado['docs_atualizados'] = docs_count
    if docs_count:
        resultado['msgs'].append(f'{docs_count} documento(s) atualizado(s) no banco.')

    info_negacao = detectar_despacho_sga_negacao(documentos_sei)
    negacao_detectada = aplicar_negacao_detectada_sei(itinerario, info_negacao)
    if negacao_detectada:
        resultado['negacao_detectada'] = True
        resultado['msgs'].append('Negacao detectada em Despacho SGA no SEI.')

    # 2b. Se tem cotação, tenta importar opções de voo via OCR
    # Detecta docs de cotação: IdSerie 272 (Cotação) OU secundários (264, 263) com keywords
    _kw_cotacao = ('cotaç', 'cotac', 'passag', 'voo', 'vôo')
    _tem_doc_cotacao = 'memorando_cotacoes' in tipos_encontrados or any(
        str(d.get('Serie', {}).get('IdSerie', '')) == ID_SERIE_COTACAO
        or (
            str(d.get('Serie', {}).get('IdSerie', '')) in (ID_SERIE_DOCUMENTO_EXTERNO, ID_SERIE_ANEXO)
            and any(kw in (d.get('Serie', {}).get('Nome', '') + ' ' + d.get('Descricao', '')).lower()
                    for kw in _kw_cotacao)
        )
        for d in documentos_sei
    )
    if _tem_doc_cotacao:
        try:
            res_cot = importar_cotacoes_do_sei(itinerario, documentos_sei, force=force_cotacoes)
            resultado['msgs'].extend(res_cot.get('msgs', []))
            resultado['cotacoes_importadas'] = res_cot.get('cotacoes_importadas', 0)
        except Exception as e:
            resultado['msgs'].append(f'Erro ao importar cotações: {str(e)[:100]}')

    # 2c. Extrair dados de escolha de passagens do PDF no SEI (doc 2977 ou 543)
    if 'escolha_passagens' in tipos_encontrados and not itinerario.escolha_voo_ida_id and not itinerario.escolha_sei_opcoes:
        doc_escolha = itinerario.get_doc('escolha_passagens')
        if doc_escolha and doc_escolha.sei_formatado:
            try:
                pdf_bytes = baixar_documento_sei(doc_escolha.sei_formatado)
                if pdf_bytes:
                    from app.services.escolha_passagens_parser import extrair_escolha_passagens
                    dados_escolha = extrair_escolha_passagens(pdf_bytes)

                    itinerario.escolha_via_sei = True
                    itinerario.escolha_sei_opcoes = ','.join(
                        str(n) for n in dados_escolha.get('opcoes_escolhidas', [])
                    ) or None
                    if dados_escolha.get('menor_valor') is not None:
                        itinerario.escolha_menor_valor = dados_escolha['menor_valor']
                    itinerario.escolha_justificativa_codigos = ','.join(
                        dados_escolha.get('justificativa_codigos', [])
                    ) or None
                    itinerario.escolha_justificativa_outros = dados_escolha.get('justificativa_outros_texto') or None
                    itinerario.escolha_declaracao_responsabilidade = dados_escolha.get('declaracao', False)

                    # Cruzamento best-effort: vincular opcoes aos DiariasCotacaoVoo
                    _vincular_escolha_a_voos(itinerario, dados_escolha)

                    opcoes = dados_escolha.get('opcoes_escolhidas', [])
                    resultado['msgs'].append(
                        f'Escolha de passagens extraída do PDF (opções: {opcoes}, '
                        f'menor_valor={dados_escolha.get("menor_valor")}).'
                    )
                    if dados_escolha.get('erros'):
                        resultado['msgs'].extend(dados_escolha['erros'])
                else:
                    itinerario.escolha_via_sei = True
                    resultado['msgs'].append('Escolha de passagens: falha no download do PDF.')
            except Exception as e:
                itinerario.escolha_via_sei = True
                resultado['msgs'].append(f'Escolha de passagens: erro na extração: {str(e)[:100]}')
        else:
            itinerario.escolha_via_sei = True

    # 3. Determinar etapa correta com base nos documentos encontrados
    tem_passagens = itinerario.tipo_solicitacao_id in TIPOS_COM_PASSAGENS

    # Checa presença de documentos-chave por etapa (do mais avançado ao inicial)
    has = lambda t: t in tipos_encontrados  # noqa: E731

    etapa_nova = DiariasEtapaID.SOLICITACAO_INICIAL  # default

    if has('relatorio_viagem') or has('np'):
        etapa_nova = DiariasEtapaID.PRESTACAO_CONTAS
    elif has('ob') or has('pd') or has('nl'):
        etapa_nova = DiariasEtapaID.CONCESSAO_DIARIAS
    elif has('analise_pagamento') or has('despacho_nci') or has('autorizacao_scdp') or has('nota_empenho'):
        etapa_nova = DiariasEtapaID.ANALISE_SOLICITACAO_2
    elif tem_passagens and (has('memorando_cotacoes') or has('escolha_passagens')):
        etapa_nova = DiariasEtapaID.ESCOLHA_VOO
    elif has('nota_reserva') or has('quadro_orcamentario'):
        etapa_nova = DiariasEtapaID.ANALISE_SOLICITACAO
    # Nota: 'autorizacao' sozinho NÃO avança mais a etapa aqui — avançar sem
    # verificar as assinaturas requeridas levava a transições incorretas.
    # A verificação de assinaturas ocorre abaixo (passo 4).

    resultado['etapa_nova'] = int(etapa_nova)

    if itinerario.etapa_atual_id != int(etapa_nova):
        resultado['msgs'].append(
            f'Etapa atualizada: {itinerario.etapa_atual_id} → {int(etapa_nova)}'
        )
        itinerario.etapa_atual_id = int(etapa_nova)
    else:
        resultado['msgs'].append(f'Etapa mantida: {int(etapa_nova)}')

    db.session.commit()

    # 4. Se a etapa permaneceu em Solicitação Inicial, verifica assinaturas de autorização.
    # Reutiliza documentos_sei já buscados — sem segunda chamada ao SEI.
    autorizacao_resultado = None
    if int(etapa_nova) == DiariasEtapaID.SOLICITACAO_INICIAL and not itinerario.processo_negado:
        try:
            autorizacao_resultado = verificar_autorizacao_diaria(
                itinerario, documentos_sei=documentos_sei
            )
            if autorizacao_resultado.get('avancou_etapa'):
                resultado['etapa_nova'] = int(DiariasEtapaID.ANALISE_SOLICITACAO)
                resultado['msgs'].append(
                    f'Autorização detectada — etapa avançada para '
                    f'{int(DiariasEtapaID.ANALISE_SOLICITACAO)} (Análise da Solicitação).'
                )
            elif autorizacao_resultado.get('superintendente_sincronizado'):
                resultado['msgs'].append(
                    'Assinatura do Superintendente registrada. Aguardando Secretário.'
                )
        except Exception as e:
            autorizacao_resultado = {
                'autorizada': False,
                'avancou_etapa': False,
                'erro': str(e),
            }
            resultado['msgs'].append(f'Verificação de assinatura: {str(e)[:80]}')

    resultado['autorizacao'] = autorizacao_resultado
    resultado['sucesso'] = True
    return resultado


def baixar_documento_sei(protocolo_documento, token=None, id_unidade=UNIDADE_SEAD, timeout=120):
    """
    Baixa o conteúdo binário de um documento do SEI.

    Usa GET /v1/unidades/{id}/documentos/baixar?protocolo_documento={id}.

    Args:
        protocolo_documento: IdDocumento do documento SEI

    Returns:
        bytes do documento (PDF) ou None em caso de erro
    """
    try:
        token_uso = token or gerar_token_sei_admin()
        if not token_uso:
            current_app.logger.error("SEI Diarias: falha na autenticação para baixar documento.")
            return None

        url = f"{BASE_URL}/v1/unidades/{id_unidade}/documentos/baixar"
        params = {'protocolo_documento': str(protocolo_documento)}
        headers = {
            'token': token_uso,
            'Accept': 'application/octet-stream',
        }

        response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)

        if response.status_code != 200:
            body_preview = response.text[:300] if response.text else '(vazio)'
            current_app.logger.error(
                f"SEI Diarias: erro ao baixar documento {protocolo_documento}: "
                f"HTTP {response.status_code} - {body_preview}"
            )
            return None

        content = response.content
        if not content or len(content) < 100:
            current_app.logger.warning(
                f"SEI Diarias: documento {protocolo_documento} retornou conteúdo vazio/pequeno ({len(content)} bytes)."
            )
            return None

        current_app.logger.info(
            f"SEI Diarias: documento {protocolo_documento} baixado ({len(content)} bytes)."
        )
        return content

    except Exception as e:
        current_app.logger.error(f"SEI Diarias: erro ao baixar documento: {str(e)}")
        return None


def extrair_nr_de_pdf(pdf_bytes):
    """
    Extrai o código da Nota de Reserva de um PDF textual do SEI.

    Usa pypdfium2 para extrair texto e regex para capturar o padrão
    de NR (ex: 2026NR00223).

    Args:
        pdf_bytes: bytes do PDF

    Returns:
        str com o código NR ou None se não encontrado
    """
    import re
    import pypdfium2 as pdfium

    _RE_NR = re.compile(r'\d{4}NR\d{5}')

    try:
        pdf = pdfium.PdfDocument(pdf_bytes)
        for page_idx in range(len(pdf)):
            page = pdf[page_idx]
            text = page.get_textpage().get_text_bounded()
            match = _RE_NR.search(text)
            if match:
                pdf.close()
                return match.group(0)
        pdf.close()
        return None
    except Exception as e:
        current_app.logger.error(f"[DIARIAS NR] Erro ao extrair NR do PDF: {e}")
        return None


def varrer_nota_reserva(itinerario, token=None):
    """
    Busca a Nota de Reserva (IdSerie=425) no SEI para um itinerário,
    baixa o PDF e extrai o código NR.

    Args:
        itinerario: DiariasItinerario com n_processo preenchido
        token: token SEI pré-gerado (opcional, para reutilização em batch)

    Returns:
        dict com {sucesso: bool, nr_codigo: str, doc_formatado: str, erro: str}
    """
    from app.extensions import db
    from app.models.diaria import DiariasMovimentacao

    resultado = {'sucesso': False, 'nr_codigo': None, 'doc_formatado': None, 'erro': None}

    protocolo = itinerario.sei_protocolo
    if not protocolo:
        resultado['erro'] = 'Itinerário sem protocolo SEI'
        return resultado

    # Busca documento de NR na tabela diarias_movimentacao
    mov_nr = DiariasMovimentacao.query.filter_by(
        protocolo_procedimento=protocolo,
        id_serie=425
    ).first()

    if not mov_nr:
        resultado['erro'] = 'Sem NR no SEI'
        return resultado

    doc_formatado = mov_nr.documento_formatado
    resultado['doc_formatado'] = doc_formatado

    # Baixa o PDF
    pdf_bytes = _baixar_documento_com_token(doc_formatado, token)
    if not pdf_bytes:
        resultado['erro'] = f'Falha ao baixar PDF {doc_formatado}'
        return resultado

    # Extrai código NR
    nr_codigo = extrair_nr_de_pdf(pdf_bytes)
    if not nr_codigo:
        resultado['erro'] = f'NR não encontrada no PDF {doc_formatado}'
        return resultado

    resultado['sucesso'] = True
    resultado['nr_codigo'] = nr_codigo

    # Salva no itinerário
    itinerario.set_doc('nota_reserva', sei_formatado=doc_formatado, codigo=nr_codigo)

    return resultado


def _baixar_documento_com_token(protocolo_documento, token=None):
    """
    Baixa documento do SEI, reutilizando token se fornecido.
    Versão interna para uso em batch (evita gerar token por documento).
    """
    try:
        if not token:
            token = gerar_token_sei_admin()
        if not token:
            return None

        url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos/baixar"
        params = {'protocolo_documento': str(protocolo_documento)}
        headers = {
            'token': token,
            'Accept': 'application/octet-stream',
        }

        response = requests.get(url, params=params, headers=headers, timeout=60, verify=False)

        if response.status_code != 200:
            return None

        content = response.content
        if not content or len(content) < 100:
            return None

        return content

    except Exception:
        return None


def importar_cotacoes_do_sei(itinerario, documentos_sei, force=False):
    """
    Identifica documentos de cotação no processo SEI, baixa os PDFs,
    extrai opções de voo via OCR e salva como DiariasCotacaoVoo.

    Args:
        itinerario: DiariasItinerario
        documentos_sei: lista de documentos retornados pelo SEI
        force: se True, remove cotações existentes antes de reimportar

    Returns:
        dict com {cotacoes_importadas: int, msgs: list de str}
    """
    from app.extensions import db
    from app.models.diaria import DiariasCotacaoVoo
    from app.services.cotacao_pdf_parser import extrair_cotacoes_pdf
    from decimal import Decimal

    resultado = {'cotacoes_importadas': 0, 'msgs': []}

    # Verifica se já existem cotações de voo para este itinerário
    existentes = DiariasCotacaoVoo.query.filter_by(itinerario_id=itinerario.id).count()
    if existentes > 0:
        if force:
            DiariasCotacaoVoo.query.filter_by(itinerario_id=itinerario.id).delete()
            resultado['msgs'].append(f'{existentes} cotações existentes removidas (reimportação forçada).')
        else:
            resultado['msgs'].append(f'Cotações de voo já existem ({existentes} opções). Ignorando importação.')
            return resultado

    # Filtra documentos de cotação — aceita IdSerie 272 ("Cotação") como primário,
    # mas também tenta IdSerie 264 ("Documento") se o nome contiver "cotação/cotacao"
    ID_SERIES_COTACAO_PRIMARIAS = {ID_SERIE_COTACAO}  # 272
    ID_SERIES_COTACAO_SECUNDARIAS = {ID_SERIE_DOCUMENTO_EXTERNO, ID_SERIE_ANEXO}  # 264, 263

    docs_cotacao = []
    for d in documentos_sei:
        id_serie = str(d.get('Serie', {}).get('IdSerie', ''))
        nome_serie = d.get('Serie', {}).get('Nome', '')
        descricao = d.get('Descricao', '')
        # Primários: sempre incluir
        if id_serie in ID_SERIES_COTACAO_PRIMARIAS:
            docs_cotacao.append(d)
        # Secundários: incluir se nome/descrição sugere cotação
        elif id_serie in ID_SERIES_COTACAO_SECUNDARIAS:
            texto_ref = (nome_serie + ' ' + descricao).lower()
            if any(kw in texto_ref for kw in ['cotaç', 'cotac', 'passag', 'voo', 'vôo']):
                docs_cotacao.append(d)
                current_app.logger.info(
                    f"[DIARIAS] Doc secundário incluído como cotação: {d.get('DocumentoFormatado', '')} "
                    f"(IdSerie={id_serie}, Nome='{nome_serie}', Desc='{descricao}')"
                )

    if not docs_cotacao:
        resultado['msgs'].append('Nenhum documento de cotação encontrado no processo.')
        return resultado

    resultado['msgs'].append(f'{len(docs_cotacao)} documento(s) de cotação encontrado(s).')
    total_importados = 0

    for doc in docs_cotacao:
        doc_fmt = doc.get('DocumentoFormatado', '')

        if not doc_fmt:
            continue

        # Baixa o PDF (usa DocumentoFormatado como protocolo_documento)
        pdf_bytes = baixar_documento_sei(doc_fmt)
        if not pdf_bytes:
            resultado['msgs'].append(f'Cotação {doc_fmt}: falha no download.')
            continue

        # Extrai voos via OCR
        try:
            dados = extrair_cotacoes_pdf(pdf_bytes)
        except Exception as e:
            resultado['msgs'].append(f'Cotação {doc_fmt}: erro no OCR: {str(e)[:80]}')
            continue

        if dados.get('erros'):
            resultado['msgs'].append(f'Cotação {doc_fmt}: {"; ".join(dados["erros"])}')

        total_extraido = len(dados.get('ida', [])) + len(dados.get('volta', []))
        if total_extraido == 0:
            # Log diagnóstico: tenta extrair texto para entender o conteúdo do PDF
            try:
                import PyPDF2
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
                sample = ''
                for page in reader.pages[:2]:
                    sample += (page.extract_text() or '')
                sample = sample[:500].replace('\n', ' | ')
                current_app.logger.warning(
                    f"[DIARIAS] Cotação {doc_fmt}: OCR não extraiu voos. "
                    f"PDF={len(pdf_bytes)} bytes, texto amostra: {sample}"
                )
            except Exception:
                current_app.logger.warning(
                    f"[DIARIAS] Cotação {doc_fmt}: OCR não extraiu voos. PDF={len(pdf_bytes)} bytes."
                )

        # Salva opções de voo extraídas
        count = 0
        for tipo_trecho, voos in [('ida', dados.get('ida', [])), ('volta', dados.get('volta', []))]:
            for voo in voos:
                if not voo.get('voo') or not voo.get('saida'):
                    current_app.logger.info(
                        f"[DIARIAS] Cotação {doc_fmt}: voo descartado (sem data de saída) — "
                        f"cia={voo.get('cia')}, voo={voo.get('voo')}, origem={voo.get('origem')}, destino={voo.get('destino')}"
                    )
                    continue  # Pula opções incompletas (saida é NOT NULL no modelo)

                cotacao_voo = DiariasCotacaoVoo(
                    itinerario_id=itinerario.id,
                    contrato_codigo=None,  # Será preenchido depois se necessário
                    tipo_trecho=tipo_trecho,
                    cia=voo['cia'],
                    voo=voo['voo'],
                    saida=voo['saida'],
                    chegada=voo['chegada'],
                    origem=voo.get('origem', ''),
                    destino=voo.get('destino', ''),
                    bagagem=voo.get('bagagem', '1'),
                    valor=Decimal(str(voo['valor'])) if voo.get('valor') else Decimal('0'),
                    fonte='ocr_sei',
                    cia_conexao=voo.get('cia_conexao'),
                    voo_conexao=voo.get('voo_conexao'),
                    saida_conexao=voo.get('saida_conexao'),
                    chegada_conexao=voo.get('chegada_conexao'),
                    origem_conexao=voo.get('origem_conexao') or None,
                    destino_conexao=voo.get('destino_conexao') or None,
                )
                db.session.add(cotacao_voo)
                count += 1

        if count:
            db.session.flush()
            total_importados += count
            resultado['msgs'].append(f'Cotação {doc_fmt}: {count} opções de voo extraídas (IDA: {len(dados.get("ida", []))}, VOLTA: {len(dados.get("volta", []))}).')
        else:
            resultado['msgs'].append(f'Cotação {doc_fmt}: nenhuma opção de voo válida extraída.')

    resultado['cotacoes_importadas'] = total_importados
    return resultado
