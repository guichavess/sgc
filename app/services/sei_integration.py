import re
import requests
import json
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# URL Base
BASE_URL = "https://api.sei.pi.gov.br"
UNIDADE_SEAD = "110006213"

def formatar_mes_competencia(competencia_mm_aaaa):
    """Converte '01/2026' para 'Janeiro de 2026'"""
    meses = {
        '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
        '05': 'Maio', '06': 'Junho', '07': 'Julho', '08': 'Agosto',
        '09': 'Setembro', '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
    }
    try:
        mes, ano = competencia_mm_aaaa.split('/')
        nome_mes = meses.get(mes, mes)
        return f"{nome_mes} de {ano}"
    except:
        return competencia_mm_aaaa

ESPECIFICACAO_MAX_CHARS = 250


def _nome_contratado(dados_contrato):
    """Nome do contratado para a especificacao, sem o prefixo do CNPJ.

    Prefere 'nomeContratadoResumido'. Sem ele, usa 'nomeContratado', que vem
    do SIAFE no formato "CNPJ - RAZAO SOCIAL" — so a razao social interessa.
    """
    resumido = str(dados_contrato.get('nomeContratadoResumido') or '').strip()
    if resumido:
        return resumido

    completo = str(dados_contrato.get('nomeContratado') or '').strip()
    return completo.split(' - ', 1)[-1].strip()


def montar_especificacao_pagamento(dados_contrato, competencia):
    """Monta a especificacao do processo de pagamento no SEI.

    Formato: "PAGAMENTO DE CONTRATO <num> - <contratado> - <codigo> - <comp>".
    Campos vazios sao omitidos (nada de separadores orfaos) e o resultado e
    truncado no limite do campo no SEI.
    """
    partes = [
        str(dados_contrato.get('numeroOriginal') or '').strip(),
        _nome_contratado(dados_contrato),
        str(dados_contrato.get('codigo') or '').strip(),
        str(competencia or '').strip(),
    ]
    corpo = ' - '.join(p for p in partes if p)
    especificacao = f"PAGAMENTO DE CONTRATO {corpo}".strip()
    return especificacao[:ESPECIFICACAO_MAX_CHARS]


MENSAGEM_ERRO_SEI_MAX_CHARS = 300


def _log_sei():
    """Logger da aplicação quando há contexto Flask; senão, o logger do módulo."""
    from flask import current_app, has_app_context
    import logging
    return current_app.logger if has_app_context() else logging.getLogger(__name__)


def mensagem_erro_sei(response):
    """Extrai a mensagem legível de uma resposta de erro do SEI (JSON ou texto/HTML), limitada."""
    mensagem = ''
    try:
        corpo = response.json()
    except ValueError:
        corpo = None

    if isinstance(corpo, dict):
        for chave in ('mensagem', 'Mensagem', 'message', 'erro', 'error', 'detail', 'descricao', 'erros', 'errors'):
            valor = corpo.get(chave)
            if isinstance(valor, (list, tuple)):
                valor = '; '.join(str(v).strip() for v in valor if str(v).strip())
            elif isinstance(valor, dict):
                valor = '; '.join(str(v).strip() for v in valor.values() if str(v).strip())
            if valor:
                mensagem = str(valor)
                break
    elif isinstance(corpo, str):
        mensagem = corpo

    if not mensagem:
        texto = getattr(response, 'text', '') or ''
        titulo = re.search(r'<h1[^>]*>(.*?)</h1>|<title[^>]*>(.*?)</title>', texto, re.S | re.I)
        if titulo:
            texto = titulo.group(1) or titulo.group(2)
        mensagem = re.sub(r'<[^>]+>', ' ', texto)

    mensagem = ' '.join(mensagem.split())
    return mensagem[:MENSAGEM_ERRO_SEI_MAX_CHARS]


def descrever_falha_criacao(detalhe):
    """Traduz o detalhe de falha de criar_procedimento_pagamento para o alerta da tela.

    Retorna {titulo, mensagem, dica, codigo}; a mensagem do SEI, quando houver, entra na mensagem.
    """
    detalhe = detalhe or {}
    tem_status = 'status' in detalhe
    status = detalhe.get('status')
    do_sei = (detalhe.get('mensagem') or '').strip()

    if tem_status and status is None:
        falha = {
            'titulo': 'O SEI não respondeu',
            'mensagem': 'A abertura do processo não foi confirmada: o SEI demorou demais ou está fora do ar.',
            'dica': 'Confira no SEI se o processo não foi aberto e tente novamente em alguns minutos.',
            'codigo': 'SEI sem resposta',
        }
    elif status == 401:
        falha = {
            'titulo': 'Sua sessão no SEI expirou',
            'mensagem': 'O SEI não aceitou mais o seu acesso, então o processo não foi aberto.',
            'dica': 'Saia do SGC e entre novamente para renovar o acesso ao SEI.',
        }
    elif status == 403:
        falha = {
            'titulo': 'Sem permissão na unidade escolhida',
            'mensagem': 'Seu usuário do SEI não pode abrir processos nessa unidade.',
            'dica': 'Escolha outra unidade SEI ou peça acesso a essa unidade no SEI.',
        }
    elif status in (400, 404, 409, 422):
        falha = {
            'titulo': 'O SEI recusou a abertura do processo',
            'mensagem': 'O SEI não aceitou os dados enviados.',
            'dica': 'Confira contrato, competência e unidade e tente novamente.',
        }
    elif isinstance(status, int) and status >= 500:
        falha = {
            'titulo': 'O SEI está instável',
            'mensagem': 'O SEI apresentou um erro interno e o processo não foi aberto.',
            'dica': 'Seus dados continuam aqui. Tente novamente em alguns minutos.',
        }
    else:
        falha = {
            'titulo': 'Não foi possível criar o processo no SEI',
            'mensagem': 'O processo não foi aberto.',
            'dica': 'Tente novamente. Se continuar, informe o código abaixo ao suporte.',
        }

    if do_sei:
        falha['mensagem'] = f'{falha["mensagem"]} Resposta do SEI: “{do_sei}”'
    falha.setdefault('codigo', f'SEI {status}' if status is not None else 'SEI')
    return falha


def criar_procedimento_pagamento(token, unidade_id, dados_contrato, competencia, detalhe_erro=None):
    """
    Etapa 1: Cria o processo de pagamento no SEI.

    Retorna o dict do SEI ou None. Com `detalhe_erro` (dict), a falha é descrita nele:
    {'status': código HTTP ou None (sem resposta), 'mensagem': texto do SEI}.
    """
    log = _log_sei()
    if detalhe_erro is None:
        detalhe_erro = {}

    if not token:
        log.warning('[SEI] Criar procedimento sem token (unidade %s)', unidade_id)
        detalhe_erro.update(status=401, mensagem='')
        return None

    url = f"{BASE_URL}/v1/unidades/{unidade_id}/procedimentos"

    especificacao_formatada = montar_especificacao_pagamento(
        dados_contrato, competencia
    )

    payload = {
        "procedimento": {
            "IdTipoProcedimento": "100000312", 
            "Especificacao": especificacao_formatada,
            "Observacao": "Gerado via Sistema SGC",
            "NivelAcesso": "Público",
            "Assuntos": [
                {
                    "CodigoEstruturado": "092",
                    "Descricao": "CONTRATAÇÃO E EXECUÇÃO DE SERVIÇO (Incluem-se documentos referentes a todas as fases da prestação de serviço por pessoa jurídica: Licitação, Contratação, Execução, Acompanhamento e Pagamento)"
                },
                {
                    "CodigoEstruturado": "997",
                    "Descricao": "DOCUMENTO OFICIAL (Ofício, Memorando, Portaria, Edital, Instrução Normativa e outros)"
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
        log.info('[SEI] Criando procedimento de pagamento na unidade %s', unidade_id)
        response = requests.post(url, json=payload, headers=headers, verify=False, timeout=60)
    except requests.RequestException as e:
        log.warning('[SEI] Sem resposta ao criar procedimento na unidade %s: %s', unidade_id, e)
        detalhe_erro.update(status=None, mensagem='')
        return None

    if response.status_code not in (200, 201):
        mensagem = mensagem_erro_sei(response)
        log.warning('[SEI] Erro %s ao criar procedimento na unidade %s: %s',
                    response.status_code, unidade_id, mensagem or '(sem mensagem)')
        detalhe_erro.update(status=response.status_code, mensagem=mensagem)
        return None

    try:
        retorno = response.json()
    except ValueError:
        log.warning('[SEI] Resposta inválida ao criar procedimento na unidade %s', unidade_id)
        detalhe_erro.update(status=response.status_code, mensagem='Resposta inválida do SEI')
        return None

    log.info('[SEI] Procedimento criado: %s (id %s, unidade %s, contrato %s, competência %s)',
             retorno.get('ProcedimentoFormatado'), retorno.get('IdProcedimento'), unidade_id,
             dados_contrato.get('codigo'), competencia)
    retorno['EspecificacaoGerada'] = especificacao_formatada
    return retorno

def gerar_documento_pagamento(token, unidade_id, id_procedimento, dados_ctx, detalhe_erro=None):
    """
    Etapa 2: Gera o documento (Requerimento) vinculado ao processo.
    Ajuste: Payload simplificado (Procedimento direto e sem data explícita).

    Retorna o dict do SEI ou None. Com `detalhe_erro` (dict), a falha é descrita nele:
    {'status': código HTTP ou None (sem resposta), 'mensagem': texto do SEI}.
    """
    log = _log_sei()
    if detalhe_erro is None:
        detalhe_erro = {}
    url = f"{BASE_URL}/v1/unidades/{unidade_id}/documentos"
    
    competencia_texto = formatar_mes_competencia(dados_ctx['competencia'])
    
    conteudo_html = f"""
    <div style="font-family: Arial, sans-serif; font-size: 12pt;">
        <p>Sr. Superintendente,</p>
        <br>
        <p>Trata-se da solicitação de pagamento de serviço referente ao Contrato <b>{dados_ctx['num_contrato']}</b>, 
        firmado com a empresa <b>{dados_ctx['empresa']}</b>, cujo objeto é {dados_ctx['objeto']}, 
        na competência de <b>{competencia_texto}</b>.</p>
        <br>
        <p>Com a devida ciência encaminho os autos para análise e providências que o caso requer 
        nos termos previstos na Lei nº 4.320/64.</p>
        <br>
        <p>Atenciosamente,</p>
        <br>
        <br>
        <p><b>{dados_ctx['usuario_nome']}</b><br>
        {dados_ctx.get('usuario_cargo', 'Colaborador Administrativo')}</p>
    </div>
    """

    # --- CORREÇÃO APLICADA ---
    # 1. 'Procedimento' recebe o ID direto (string), sem objeto aninhado.
    # 2. Campo 'Data' removido (o SEI assume a data atual).
    payload = {
        "Procedimento": id_procedimento,  # Passando o valor direto
        "IdSerie": "2614",
        "Conteudo": conteudo_html,
        "NivelAcesso": "Público",
        "SinBloqueado": "N",
        "Descricao": f"Solicitação de Pagamento - {dados_ctx['competencia']}",
        "Observacao": "Gerado automaticamente pelo SGC"
    }
    
    headers = {
        'token': token, 
        'Content-Type': 'application/json',
        'Accept': 'application/json' # Boa prática manter o Accept
    }

    try:
        log.info('[SEI] Gerando requisição de pagamento no procedimento %s (unidade %s)', id_procedimento, unidade_id)
        response = requests.post(url, json=payload, headers=headers, verify=False, timeout=60)
    except requests.RequestException as e:
        log.warning('[SEI] Sem resposta ao gerar documento no procedimento %s: %s', id_procedimento, e)
        detalhe_erro.update(status=None, mensagem='')
        return None

    if response.status_code not in (200, 201):
        mensagem = mensagem_erro_sei(response)
        log.warning('[SEI] Erro %s ao gerar documento no procedimento %s (unidade %s): %s',
                    response.status_code, id_procedimento, unidade_id, mensagem or '(sem mensagem)')
        detalhe_erro.update(status=response.status_code, mensagem=mensagem)
        return None

    try:
        return response.json()
    except ValueError:
        log.warning('[SEI] Resposta inválida ao gerar documento no procedimento %s', id_procedimento)
        detalhe_erro.update(status=response.status_code, mensagem='Resposta inválida do SEI')
        return None


def assinar_documento(token, unidade_id, dados_assinatura, protocolo_proc=None):
    """
    Etapa 3: Assina o documento gerado via API SEI.

    dados_assinatura espera:
    - protocolo_doc: O número visual do documento (ex: 0001234)
    - orgao: Sigla do órgão (ex: SEAD-PI)
    - cargo: Cargo do usuário (ex: Assessora Técnica)
    - id_login: ID da sessão de login do SEI
    - id_usuario: ID do usuário no SEI
    - senha: A senha digitada no popup

    Args:
        protocolo_proc: protocolo do processo SEI (ex: '00002.003853/2026-21').
            Quando fornecido e está em DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS,
            a assinatura é simulada sem chamar o SEI (útil para processos teste).

    Bypass:
    - Se DIARIAS_BYPASS_ASSINATURAS=True (global), sempre simula.
    - Se protocolo_proc está em DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS, simula
      apenas para esse processo específico.
    """
    # ── Bypass de assinatura (global OU por protocolo específico) ──
    from app.constants import protocolo_tem_bypass_assinatura
    if protocolo_tem_bypass_assinatura(protocolo_proc):
        from flask import current_app
        current_app.logger.info(
            f"[BYPASS] Assinatura simulada para documento "
            f"{dados_assinatura.get('protocolo_doc', '?')} "
            f"(processo={protocolo_proc!r})"
        )
        return {"sucesso": True, "aviso": "Assinatura bypassed (modo teste)"}

    if not token:
        return {"sucesso": False, "erro": "Token inválido"}

    url = f"{BASE_URL}/v1/unidades/{unidade_id}/documentos/assinar"

    payload = {
        "ProtocoloDocumento": dados_assinatura['protocolo_doc'],
        "Orgao": dados_assinatura['orgao'],
        "Cargo": dados_assinatura['cargo'],
        "IdLogin": dados_assinatura['id_login'],
        "IdUsuario": dados_assinatura['id_usuario'],
        "Senha": dados_assinatura['senha']
    }

    headers = {
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        from flask import current_app
        current_app.logger.info(f"SEI: Assinando documento {dados_assinatura['protocolo_doc']}...")
        response = requests.patch(url, json=payload, headers=headers, verify=False, timeout=30)

        if response.status_code == 204:
            return {"sucesso": True}
        else:
            erro_msg = response.text
            # Documento já assinado fora da aplicação — trata como sucesso
            if "já foi assinado" in erro_msg:
                current_app.logger.info(
                    f"SEI: Documento {dados_assinatura['protocolo_doc']} já estava assinado (ignorado)."
                )
                return {"sucesso": True, "aviso": "Documento já estava assinado"}
            current_app.logger.error(f"SEI: Erro assinatura: {erro_msg}")
            return {"sucesso": False, "erro": f"SEI recusou: {erro_msg}"}

    except Exception as e:
        from flask import current_app
        current_app.logger.error(f"SEI: Erro de conexão na assinatura: {e}")
        return {"sucesso": False, "erro": str(e)}


def consultar_procedimento_sei(token, protocolo, timeout=120):
    """
    Consulta um processo existente no SEI via endpoint de Consulta de Procedimento.

    Usa GET /v1/unidades/{id_unidade}/procedimentos/consulta
    Se retornar 200, o processo existe e os dados são extraídos da resposta.

    Args:
        token: Token de autenticação SEI
        protocolo: Número do processo (ex: '00002.009305/2025-23')

    Returns:
        dict com {sucesso, protocolo_formatado, id_procedimento,
                   link_acesso, especificacao, dados_procedimento, erro}
    """
    resultado = {
        'sucesso': False,
        'protocolo_formatado': '',
        'id_procedimento': '',
        'link_acesso': '',
        'especificacao': '',
        'dados_procedimento': None,
        'erro': None
    }

    if not token:
        resultado['erro'] = 'Token SEI não fornecido.'
        return resultado

    protocolo_limpo = "".join(filter(str.isdigit, protocolo))
    if not protocolo_limpo:
        resultado['erro'] = 'Protocolo inválido.'
        return resultado

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/consulta"
    params = {
        'protocolo_procedimento': protocolo_limpo
    }
    headers = {
        'token': token,
        'Accept': 'application/json'
    }

    try:
        _log_sei().info('[SEI] Consultando procedimento %s', protocolo_limpo)
        response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)

        if response.status_code != 200:
            resultado['erro'] = f'Processo não encontrado no SEI (HTTP {response.status_code}).'
            return resultado

        data = response.json()

        # Extrai dados do procedimento
        resultado['sucesso'] = True
        resultado['dados_procedimento'] = data
        resultado['protocolo_formatado'] = str(data.get('ProcedimentoFormatado', protocolo))
        resultado['id_procedimento'] = str(data.get('IdProcedimento', ''))
        resultado['link_acesso'] = data.get('LinkAcesso', '')
        resultado['especificacao'] = data.get('Especificacao', '')

        _log_sei().info('[SEI] Processo encontrado: %s', resultado['protocolo_formatado'])
        return resultado

    except requests.exceptions.Timeout:
        resultado['erro'] = 'Timeout ao consultar SEI. Tente novamente.'
        return resultado
    except Exception as e:
        resultado['erro'] = f'Erro ao consultar SEI: {str(e)}'
        return resultado


def listar_documentos_procedimento_sei(token, protocolo, max_retries=3, timeout=120):
    """
    Lista os documentos de um processo existente no SEI.

    Usa GET /v1/unidades/{id_unidade}/procedimentos/documentos
    Compartilhada entre modulos Pagamentos e CGFR.

    Args:
        token: Token de autenticacao SEI
        protocolo: Numero do processo (formatado ou so digitos)
        max_retries: Numero de tentativas (default 3, com backoff progressivo)
        timeout: Timeout em segundos por tentativa (default 60)

    Returns:
        dict com {sucesso: bool, documentos: list, erro: str|None}
    """
    import logging
    import time
    logger = logging.getLogger(__name__)

    protocolo_limpo = "".join(filter(str.isdigit, protocolo))
    if not token or not protocolo_limpo:
        return {'sucesso': False, 'documentos': [], 'erro': 'Token ou protocolo invalido'}

    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/documentos"
    params = {
        'protocolo_procedimento': protocolo_limpo,
        'pagina': 1,
        'quantidade': 1000,
        'sinal_completo': 'N'
    }
    headers = {
        'token': token,
        'Accept': 'application/json'
    }

    # Retry com backoff progressivo (5s, 10s, 15s)
    resp = None
    last_error = None
    for tentativa in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers,
                                timeout=timeout, verify=False)
            if resp.status_code == 200:
                break
            last_error = f'HTTP {resp.status_code}'
            logger.warning(f"[{protocolo}] API retornou {resp.status_code} "
                           f"(tentativa {tentativa}/{max_retries})")
        except requests.exceptions.ReadTimeout:
            last_error = 'Timeout'
            logger.warning(f"[{protocolo}] Timeout (tentativa {tentativa}/{max_retries})")
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError) as e:
            last_error = str(e)
            logger.warning(f"[{protocolo}] Conexao: {e} (tentativa {tentativa}/{max_retries})")

        if tentativa < max_retries:
            time.sleep(tentativa * 5)

    if not resp or resp.status_code != 200:
        return {
            'sucesso': False,
            'documentos': [],
            'erro': f'API SEI nao respondeu apos {max_retries} tentativas ({last_error})',
        }

    try:
        data = resp.json()
    except Exception:
        return {'sucesso': False, 'documentos': [], 'erro': 'JSON invalido na resposta da API SEI'}

    # Parsing: resposta pode ser dict com 'Documentos', 'resultados', ou lista direta
    documentos = []
    if isinstance(data, dict):
        documentos = data.get('Documentos', [])
        if not documentos and 'resultados' in data:
            documentos = data['resultados']
    elif isinstance(data, list):
        documentos = data

    return {'sucesso': True, 'documentos': documentos, 'erro': None}
