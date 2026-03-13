"""
Integração SEI para o módulo de Diárias.

Cria procedimento (processo), documento SEAD_MEMORANDO_SGA, requisição de diárias
e documentos externos (anexos) no SEI quando uma solicitação Nacional é criada.
"""
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
ID_SERIE_NOTA_EMPENHO = "419"         # "NE - Nota de Empenho"
ID_SERIE_AUTORIZACAO_SCDP = "264"     # Doc externo para Autorização SCDP
ID_HIPOTESE_LEGAL_INFO_PESSOAL = "4"  # "Informação Pessoal" - Art. 31 da Lei nº 12.527/2011

# Unidade destino pós-autorização (Diretoria de Planejamento e Finanças)
UNIDADE_DFIN_APOIO = "110009066"  # "SEAD-PI/GAB/SGACG/DFIN/APOIO"

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


def criar_procedimento_diarias(token, dados_servidor, tipo_itinerario_nome):
    """
    Etapa 1: Cria o processo de diárias no SEI.

    Args:
        token: Token de autenticação SEI
        dados_servidor: dict com {cargo, matricula} do servidor principal
        tipo_itinerario_nome: 'Nacional' ou 'Estadual'

    Returns:
        dict com resposta do SEI (contém IdProcedimento, ProcedimentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos"

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
        response = requests.post(url, json=payload, headers=headers, timeout=30)

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
                            doc_req_diarias=None, doc_req_passagens=None):
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

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    justificativa = dados_memorando.get('justificativa', '')
    data_viagem = dados_memorando.get('data_viagem')
    data_retorno = dados_memorando.get('data_retorno')
    tipo_sol = dados_memorando.get('tipo_solicitacao_nome', 'Diárias + Passagens Aéreas')

    # Formata datas por extenso
    data_viagem_extenso = _formatar_data_extenso(data_viagem) if data_viagem else ''
    data_retorno_extenso = _formatar_data_extenso(data_retorno) if data_retorno else ''

    # Monta referências dos documentos criados anteriormente
    ref_diarias = ''
    if doc_req_diarias:
        doc_fmt = doc_req_diarias.get('DocumentoFormatado', '')
        ref_diarias = f'<i>({doc_fmt})</i>' if doc_fmt else ''

    ref_passagens = ''
    if doc_req_passagens:
        doc_fmt = doc_req_passagens.get('DocumentoFormatado', '')
        ref_passagens = f'<i>({doc_fmt})</i>' if doc_fmt else ''

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

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p><b>PARA:</b> GABINETE DO SECRETÁRIO DE ADMINISTRAÇÃO</p>
        <br>
        <p>Senhor Secretário,</p>
        <br>
        <p>{texto_solicitacao}, no período de
        <b>{data_viagem_extenso}</b> a <b>{data_retorno_extenso}</b>.</p>
        <br>
        <p>{justificativa}</p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_MEMORANDO_SGA,
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
        response = requests.post(url, json=payload, headers=headers, timeout=30)

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


def gerar_requisicao_diarias(token, id_procedimento, dados_requisicao):
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

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

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
        print(f"[DEBUG SEI] Requisição - Procedimento: {id_procedimento}, Série: {ID_SERIE_REQUISICAO_DIARIAS}")
        print(f"[DEBUG SEI] Requisição - Servidores: {len(servidores)}, Trecho: {trecho}")

        response = requests.post(url, json=payload, headers=headers, timeout=30)

        print(f"[DEBUG SEI] Requisição - Status: {response.status_code}")
        if response.status_code not in [200, 201]:
            print(f"[DEBUG SEI] Requisição - ERRO: {response.text[:500]}")
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar requisição ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        print(f"[DEBUG SEI] Requisição - SUCESSO: {retorno.get('DocumentoFormatado')}")
        current_app.logger.info(
            f"SEI Diárias: Requisição gerada com sucesso - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        print(f"[DEBUG SEI] Requisição - EXCEÇÃO: {type(e).__name__}: {e}")
        current_app.logger.error(f"SEI Diárias: Erro crítico ao gerar requisição: {e}")
        return None


def gerar_requisicao_passagens(token, id_procedimento, dados_requisicao):
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

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

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
        <h3 style="text-align:center; margin-bottom:20px;">REQUISIÇÃO DE PASSAGENS AÉREAS</h3>

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
        print(f"[DEBUG SEI] Req. Passagens - Procedimento: {id_procedimento}, Série: {ID_SERIE_REQUISICAO_PASSAGENS}")
        print(f"[DEBUG SEI] Req. Passagens - Servidores: {len(servidores)}, Trecho: {trecho}")

        response = requests.post(url, json=payload, headers=headers, timeout=30)

        print(f"[DEBUG SEI] Req. Passagens - Status: {response.status_code}")
        if response.status_code not in [200, 201]:
            print(f"[DEBUG SEI] Req. Passagens - ERRO: {response.text[:500]}")
            current_app.logger.error(
                f"SEI Diárias: Erro ao gerar req. passagens ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        print(f"[DEBUG SEI] Req. Passagens - SUCESSO: {retorno.get('DocumentoFormatado')}")
        current_app.logger.info(
            f"SEI Diárias: Requisição de passagens gerada - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        print(f"[DEBUG SEI] Req. Passagens - EXCEÇÃO: {type(e).__name__}: {e}")
        current_app.logger.error(f"SEI Diárias: Erro crítico ao gerar req. passagens: {e}")
        return None


def adicionar_documento_externo(token, protocolo_formatado, arquivo_bytes, nome_arquivo,
                                descricao='Documento anexo', id_serie=None, numero=None):
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

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos/externo"

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
        print(f"[DEBUG SEI] Doc Externo - Procedimento: {protocolo_formatado}, "
              f"Arquivo: {nome_arquivo}, Tamanho base64: {len(conteudo_base64)}")

        response = requests.post(url, json=payload, headers=headers, timeout=60)

        print(f"[DEBUG SEI] Doc Externo - Status: {response.status_code}")
        if response.status_code not in [200, 201]:
            print(f"[DEBUG SEI] Doc Externo - ERRO: {response.text[:500]}")
            current_app.logger.error(
                f"SEI Diárias: Erro ao adicionar doc externo ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        print(f"[DEBUG SEI] Doc Externo - SUCESSO: {retorno.get('DocumentoFormatado')}")
        current_app.logger.info(
            f"SEI Diárias: Documento externo adicionado - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        print(f"[DEBUG SEI] Doc Externo - EXCEÇÃO: {type(e).__name__}: {e}")
        current_app.logger.error(f"SEI Diárias: Erro crítico ao adicionar doc externo: {e}")
        return None


def criar_processo_diarias_completo(dados_itinerario, dados_servidor, justificativa_texto,
                                    dados_requisicao=None, arquivo_externo=None,
                                    tipo_solicitacao_id=None):
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

    try:
        # 1. Autenticação
        token = gerar_token_sei_admin()
        if not token:
            resultado['erro'] = 'Falha na autenticação com o SEI.'
            current_app.logger.error("SEI Diárias: Falha na autenticação.")
            return resultado

        # 2. Criar procedimento
        tipo_itinerario_nome = dados_itinerario.get('tipo_itinerario_nome', 'Nacional')
        proc = criar_procedimento_diarias(token, dados_servidor, tipo_itinerario_nome)
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
            print(f"[DEBUG SEI] Chamando gerar_requisicao_diarias com id_proc={id_procedimento}")

            req = gerar_requisicao_diarias(token, id_procedimento, dados_requisicao)
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
            print(f"[DEBUG SEI] Chamando gerar_requisicao_passagens com id_proc={id_procedimento}")

            req_pass = gerar_requisicao_passagens(token, id_procedimento, dados_requisicao)
            if not req_pass:
                current_app.logger.warning("SEI Diárias: Requisição de passagens falhou.")
            else:
                resultado['requisicao_passagens'] = req_pass
                doc_req_passagens = req_pass

        # 5. Gerar memorando (POR ÚLTIMO dos 3 documentos internos)
        #    Agora pode referenciar os IDs das requisições criadas acima.
        dados_memorando = {
            'justificativa': justificativa_texto or '',
            'data_viagem': dados_itinerario.get('data_viagem'),
            'data_retorno': dados_itinerario.get('data_retorno'),
            'tipo_solicitacao_nome': dados_itinerario.get('tipo_solicitacao_nome', 'Diárias + Passagens Aéreas'),
        }

        memo = gerar_memorando_diarias(
            token, id_procedimento, dados_memorando,
            doc_req_diarias=doc_req_diarias,
            doc_req_passagens=doc_req_passagens,
        )
        if not memo:
            resultado['erro'] = 'Procedimento criado, mas falha ao gerar memorando no SEI.'
            return resultado

        resultado['memorando'] = memo

        # 6. Adicionar documento externo (se houver arquivo)
        if arquivo_externo and arquivo_externo.get('bytes'):
            print(f"[DEBUG SEI] arquivo_externo recebido: {arquivo_externo.get('nome_arquivo')}, "
                  f"tamanho: {len(arquivo_externo['bytes'])} bytes")

            doc_ext = adicionar_documento_externo(
                token,
                protocolo_formatado,
                arquivo_externo['bytes'],
                arquivo_externo['nome_arquivo'],
                arquivo_externo.get('descricao', 'Documento anexo - Solicitacao de Diarias'),
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
                        manter_aberto=True):
    """
    Envia (encaminha) um procedimento para uma ou mais unidades no SEI.

    Usa PATCH /v1/unidades/{id}/procedimentos/enviar.

    Args:
        token: Token de autenticacao SEI
        protocolo_procedimento: Protocolo formatado (ex: '00002.009305/2025-23')
        unidades_destino: list de IDs de unidades destino (ex: ['110009066'])
        manter_aberto: se True, mantém o processo aberto na unidade atual

    Returns:
        dict com {sucesso: bool, erro: str ou None}
    """
    resultado = {'sucesso': False, 'erro': None}

    if not token:
        resultado['erro'] = 'Token nao fornecido.'
        return resultado

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/enviar"

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
        print(f"[DEBUG SEI] Enviando procedimento {protocolo_procedimento} "
              f"para unidades: {unidades_destino}")

        response = requests.patch(url, json=payload, headers=headers, timeout=30, verify=False)

        print(f"[DEBUG SEI] Enviar procedimento - Status: {response.status_code}")

        if response.status_code in [200, 204]:
            resultado['sucesso'] = True
            current_app.logger.info(
                f"SEI Diarias: Procedimento {protocolo_procedimento} enviado "
                f"para {unidades_destino}."
            )
        else:
            resultado['erro'] = f'Erro HTTP {response.status_code}: {response.text[:300]}'
            print(f"[DEBUG SEI] Enviar procedimento - ERRO: {response.text[:500]}")
            current_app.logger.error(
                f"SEI Diarias: Erro ao enviar procedimento: {resultado['erro']}"
            )

        return resultado

    except Exception as e:
        resultado['erro'] = f'Erro ao enviar procedimento: {str(e)}'
        print(f"[DEBUG SEI] Enviar procedimento - EXCEÇÃO: {type(e).__name__}: {e}")
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
            'quantidade': 100,  # Busca ate 100 documentos
        }
        headers = {
            'token': token,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        print(f"[DEBUG SEI] Listando documentos: {url}?protocolo_procedimento={protocolo_procedimento}")
        response = requests.get(url, params=params, headers=headers, timeout=30, verify=False)
        print(f"[DEBUG SEI] Listar documentos - Status: {response.status_code}")

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


def verificar_autorizacao_diaria(itinerario):
    """
    Verifica autorizacao no processo SEI da solicitacao de diaria.

    A logica varia conforme o tipo da solicitacao:

    - **Apenas Diarias (tipo 1):** procura o documento SEAD_REQUISICAO_DE_DIARIAS
      (IdSerie 532) e verifica se ele possui assinaturas. Se assinado, considera
      autorizado.

    - **Diarias + Passagens (tipo 2) e Apenas Passagens (tipo 3):** procura o
      documento SEAD_AUTORIZACAO_DO_SECRETARIO (IdSerie 574). Se existir, considera
      autorizado (logica original).

    Se autorizado, avanca automaticamente para etapa 2 (Solicitacao Autorizada).

    Args:
        itinerario: objeto DiariasItinerario (deve ter sei_protocolo)

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

    # Lista documentos do processo
    resp_docs = consultar_documentos_procedimento(protocolo_proc)
    if not resp_docs['sucesso']:
        resultado['erro'] = resp_docs['erro']
        return resultado

    # Determina logica conforme tipo de solicitacao
    tipo_sol = getattr(itinerario, 'tipo_solicitacao_id', None)
    apenas_diarias = (tipo_sol == 1)

    doc_encontrado = None

    if apenas_diarias:
        # Tipo 1 (Apenas Diarias):
        # 1º Busca Requisicao de Diarias assinada (fluxo original via SEI)
        # 2º Busca Autorizacao do Secretario (fluxo via sistema ou SEI direto)
        for doc in resp_docs['documentos']:
            serie = doc.get('Serie', {})
            if str(serie.get('IdSerie', '')) == ID_SERIE_REQUISICAO_DIARIAS:
                assinaturas = doc.get('Assinaturas', [])
                if assinaturas:
                    doc_encontrado = doc
                break  # so existe uma requisicao, nao precisa continuar

        # Se nao encontrou requisicao assinada, verifica autorizacao do secretario
        if not doc_encontrado:
            for doc in resp_docs['documentos']:
                serie = doc.get('Serie', {})
                if str(serie.get('IdSerie', '')) == ID_SERIE_AUTORIZACAO_SECRETARIO:
                    doc_encontrado = doc
                    break
    else:
        # Tipos 2 e 3: busca Autorizacao do Secretario (logica original)
        for doc in resp_docs['documentos']:
            serie = doc.get('Serie', {})
            if str(serie.get('IdSerie', '')) == ID_SERIE_AUTORIZACAO_SECRETARIO:
                doc_encontrado = doc
                break

    if doc_encontrado:
        resultado['autorizada'] = True
        resultado['documento_autorizacao'] = {
            'id_documento': doc_encontrado.get('IdDocumento', ''),
            'documento_formatado': doc_encontrado.get('DocumentoFormatado', ''),
            'serie_nome': doc_encontrado.get('Serie', {}).get('Nome', ''),
            'data': doc_encontrado.get('Data', ''),
            'assinaturas': doc_encontrado.get('Assinaturas', []),
        }

        # Avanca para etapa 2 se ainda estiver na etapa 1
        if itinerario.etapa_atual_id == DiariasEtapaID.SOLICITACAO_INICIADA:
            doc_fmt = doc_encontrado.get('DocumentoFormatado', '?')
            assinaturas = doc_encontrado.get('Assinaturas', [])
            nomes = [a.get('Nome', '?') for a in assinaturas]

            if apenas_diarias:
                # Comentario para Apenas Diarias
                comentario = f"Requisicao de Diarias ({doc_fmt}) assinada por: {', '.join(nomes)}"
            else:
                # Comentario para tipos com passagens
                if nomes:
                    comentario = f"Autorizacao do Secretario ({doc_fmt}) assinada por: {', '.join(nomes)}"
                else:
                    comentario = f"Documento de Autorizacao do Secretario encontrado ({doc_fmt})"

            DiariaService.registrar_movimentacao(
                itinerario.id,
                DiariasEtapaID.FINANCEIRO,
                usuario_id=None,
                comentario=comentario,
            )
            resultado['avancou_etapa'] = True
            current_app.logger.info(
                f"SEI Diarias: Itinerario {itinerario.id} avancou para etapa 2 "
                f"(Financeiro) - Documento {doc_fmt}."
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
                            itinerario.sei_id_despacho_dfin = str(
                                despacho_ret.get('IdDocumento', '')
                            )
                            itinerario.sei_despacho_dfin_formatado = despacho_ret.get(
                                'DocumentoFormatado', ''
                            )
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
    elif apenas_diarias:
        # Tipo 1: documento existe mas nao tem assinaturas — informa ao usuario
        for doc in resp_docs['documentos']:
            serie = doc.get('Serie', {})
            if str(serie.get('IdSerie', '')) == ID_SERIE_REQUISICAO_DIARIAS:
                resultado['documento_autorizacao'] = {
                    'id_documento': doc.get('IdDocumento', ''),
                    'documento_formatado': doc.get('DocumentoFormatado', ''),
                    'serie_nome': doc.get('Serie', {}).get('Nome', ''),
                    'data': doc.get('Data', ''),
                    'assinaturas': [],
                }
                resultado['erro'] = 'Requisicao de Diarias encontrada, mas ainda nao possui assinaturas.'
                break

    return resultado


# ── Despacho DFIN ────────────────────────────────────────────────────────────


def gerar_despacho_dfin(token, id_procedimento, sei_protocolo, interessados):
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

    interessados_texto = ', '.join(interessados) if interessados else '@interessados_virgula_espaco@'

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p>Processo nº <b>{sei_protocolo}</b></p>
        <p>Interessados: {interessados_texto}</p>
        <p>Assunto: Documento Oficial: Ofício, Memorando, Portaria, Edital, Instrução Normativa e outros</p>
        <br>
        <p style="text-align: center;"><b>DESPACHO</b></p>
        <br>
        <p style="text-indent: 2em; text-align: justify;">
            Encaminho o processo à <b>Gerência de Execução Orçamentária</b> para
            conhecimento e envio para a <b>Coordenação de Controle de Diárias e Passagens</b> para
            verificação do quantitativo de diárias recebidas, assim como a emissão de relatório de análise
            quanto a aprovação/reprovação da prestação de contas anterior e à <b>Gerência de
            Planejamento e Orçamento</b> para análise da disponibilidade orçamentária, emissão de nota de
            reserva e quadro de informação orçamentária, devendo ser observados os procedimentos
            legais.
        </p>
        <br>
        <p style="text-indent: 2em; text-align: justify;">
            Após, remetam-se os autos à <b>SGA</b> para deliberação.
        </p>
    </div>
    """

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
        response = requests.post(url, json=payload, headers=headers, timeout=30)

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

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

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
        response = requests.post(url, json=payload, headers=headers, timeout=30)

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

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 11pt;">
        <p style="text-align: center;"><b>JUSTIFICATIVA DE ESCOLHA DE PASSAGENS</b></p>
        <br>
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
        response = requests.post(url, json=payload, headers=headers, timeout=30)

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
        response = requests.post(url, json=payload, headers=headers, timeout=30)

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


# ── Autorização do Secretário ─────────────────────────────────────────────


def gerar_autorizacao_secretario(token, id_procedimento, tipo_solicitacao_id, sei_protocolo):
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

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado) ou None
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para autorização do secretário.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    # Determina texto conforme tipo de solicitação
    tipo_id = int(tipo_solicitacao_id) if tipo_solicitacao_id else 2
    if tipo_id == 1:
        texto_autorizo = "Autorizo o pagamento de diárias"
    elif tipo_id == 3:
        texto_autorizo = "Autorizo a compra das passagens"
    else:
        # Tipo 2 (padrão) — Diárias + Passagens
        texto_autorizo = "Autorizo a compra das passagens e o pagamento de diárias"

    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p style="text-indent: 2em; text-align: justify;">
            <b>{texto_autorizo}</b> e encaminho os autos à Superintendência
            de Gestão Administrativa - SGA, para conhecimento e providências
            necessárias, devendo ser observados os procedimentos legais.
        </p>
    </div>
    """

    payload = {
        "Procedimento": str(id_procedimento),
        "IdSerie": ID_SERIE_AUTORIZACAO_SECRETARIO,
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

    cpf_limpo = cpf.strip()

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

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

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
    Gera documento Nota de Empenho (idSerie 419) no processo SEI.

    Args:
        token: Token de autenticação SEI
        id_procedimento: ID interno do procedimento SEI
        sei_protocolo: Número formatado do processo
        codigo_ne: Código da NE (ex: '2026NE00456')
        dados_empenho: dict opcional com dados adicionais {
            'valor', 'natureza_despesa', 'fonte_recursos', 'favorecido', 'objeto'
        }

    Returns:
        dict com resposta do SEI (IdDocumento, DocumentoFormatado, etc.)
    """
    if not token:
        current_app.logger.error("SEI Diárias: Token não fornecido para NE.")
        return None

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos"

    # Monta HTML do conteúdo da NE
    dados = dados_empenho or {}
    hoje = datetime.now()
    data_formatada = f"{hoje.day} de {MESES_EXTENSO[hoje.month]} de {hoje.year}"

    linhas_extra = ""
    if dados.get('valor'):
        linhas_extra += f"""
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold;">Valor</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">R$ {dados['valor']}</td>
            </tr>"""
    if dados.get('natureza_despesa'):
        linhas_extra += f"""
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold;">Natureza da Despesa</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">{dados['natureza_despesa']}</td>
            </tr>"""
    if dados.get('fonte_recursos'):
        linhas_extra += f"""
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold;">Fonte de Recursos</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">{dados['fonte_recursos']}</td>
            </tr>"""
    if dados.get('favorecido'):
        linhas_extra += f"""
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold;">Favorecido</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">{dados['favorecido']}</td>
            </tr>"""
    if dados.get('objeto'):
        linhas_extra += f"""
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold;">Objeto</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">{dados['objeto']}</td>
            </tr>"""

    html_conteudo = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <h3 style="text-align: center; margin-bottom: 20px;">NOTA DE EMPENHO</h3>
        <table style="width: 100%; border-collapse: collapse; margin-bottom: 15px;">
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold; width:35%;">Nota de Empenho</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">{codigo_ne}</td>
            </tr>
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold;">Processo</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">{sei_protocolo}</td>
            </tr>
            <tr>
                <td style="padding:6px 10px; border:1px solid #ccc; font-weight:bold;">Data</td>
                <td style="padding:6px 10px; border:1px solid #ccc;">{data_formatada}</td>
            </tr>{linhas_extra}
        </table>
    </div>
    """

    conteudo_b64 = base64.b64encode(html_conteudo.encode('utf-8')).decode('utf-8')

    payload = {
        "Procedimento": id_procedimento,
        "IdSerie": ID_SERIE_NOTA_EMPENHO,
        "Numero": codigo_ne,
        "Descricao": f"Nota de Empenho {codigo_ne}",
        "Conteudo": conteudo_b64,
        "NivelAcesso": "Restrito",
        "IdHipoteseLegal": ID_HIPOTESE_LEGAL_INFO_PESSOAL,
        "SinBloqueado": "N",
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        current_app.logger.info(
            f"SEI Diarias: Gerando NE {codigo_ne} no procedimento {id_procedimento}..."
        )
        response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code not in [200, 201]:
            current_app.logger.error(
                f"SEI Diarias: Erro ao gerar NE ({response.status_code}): {response.text}"
            )

        response.raise_for_status()

        retorno = response.json()
        current_app.logger.info(
            f"SEI Diarias: NE gerada - {retorno.get('DocumentoFormatado', retorno)}"
        )
        return retorno

    except Exception as e:
        current_app.logger.error(f"SEI Diarias: Erro ao gerar NE: {e}")
        return None
