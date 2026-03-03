import requests
import json
from datetime import datetime

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

def criar_procedimento_pagamento(token, unidade_id, dados_contrato, competencia):
    """
    Etapa 1: Cria o processo de pagamento no SEI.
    """
    if not token:
        print("❌ Erro: Token não fornecido.")
        return None

    url = f"{BASE_URL}/v1/unidades/{unidade_id}/procedimentos"
    
    # Sanitização
    num_orig = str(dados_contrato.get('numeroOriginal', '')).strip()
    nome_contr = str(dados_contrato.get('nomeContratadoResumido', '')).strip()
    cod_contr = str(dados_contrato.get('codigo', '')).strip()
    
    especificacao_formatada = f"PAGAMENTO DE CONTRATO {num_orig}-{nome_contr[18:]}-{cod_contr}-{competencia}"
    
    if len(especificacao_formatada) > 250:
        especificacao_formatada = especificacao_formatada[:250]

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
        print(f"📡 Enviando requisição SEI (Criar Procedimento) para unidade {unidade_id}...")
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code not in [200, 201]:
            print(f"❌ Erro SEI ao criar procedimento ({response.status_code}): {response.text}")
            
        response.raise_for_status()
        
        retorno = response.json()
        retorno['EspecificacaoGerada'] = especificacao_formatada
        return retorno

    except Exception as e:
        print(f"Erro crítico na integração SEI (Procedimento): {e}")
        return None

def gerar_documento_pagamento(token, unidade_id, id_procedimento, dados_ctx):
    """
    Etapa 2: Gera o documento (Requerimento) vinculado ao processo.
    Ajuste: Payload simplificado (Procedimento direto e sem data explícita).
    """
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
        print(f"📡 Enviando requisição SEI (Gerar Documento) para unidade {unidade_id}...")
        
        # Debug opcional: verifique o que está sendo enviado
        # print(json.dumps(payload, indent=2)) 

        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code not in [200, 201]:
            print(f"❌ Erro SEI ao gerar documento ({response.status_code}): {response.text}")

        response.raise_for_status()
        return response.json()
        
    except Exception as e:
        print(f"Erro ao gerar documento: {e}")
        return None


def assinar_documento(token, unidade_id, dados_assinatura):
    """
    Etapa 3: Assina o documento gerado via API SEI.
    
    dados_assinatura espera:
    - protocolo_doc: O número visual do documento (ex: 0001234)
    - orgao: Sigla do órgão (ex: SEAD-PI)
    - cargo: Cargo do usuário (ex: Assessora Técnica)
    - id_login: ID da sessão de login do SEI
    - id_usuario: ID do usuário no SEI
    - senha: A senha digitada no popup
    """
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
        print(f"✍️ Tentando assinar documento {dados_assinatura['protocolo_doc']}...")
        response = requests.patch(url, json=payload, headers=headers)

        if response.status_code == 204:
            return {"sucesso": True}
        else:
            erro_msg = response.text
            print(f"❌ Erro assinatura SEI: {erro_msg}")
            return {"sucesso": False, "erro": f"SEI recusou: {erro_msg}"}

    except Exception as e:
        print(f"Erro de conexão na assinatura: {e}")
        return {"sucesso": False, "erro": str(e)}


def consultar_procedimento_sei(token, protocolo):
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
        print(f"📡 Consultando procedimento SEI: {protocolo_limpo}...")
        response = requests.get(url, params=params, headers=headers, timeout=60)

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

        print(f"✅ Processo encontrado: {resultado['protocolo_formatado']}")
        return resultado

    except requests.exceptions.Timeout:
        resultado['erro'] = 'Timeout ao consultar SEI. Tente novamente.'
        return resultado
    except Exception as e:
        resultado['erro'] = f'Erro ao consultar SEI: {str(e)}'
        return resultado


def listar_documentos_procedimento_sei(token, protocolo):
    """
    Lista os documentos de um processo existente no SEI.

    Usa GET /v1/unidades/{id_unidade}/procedimentos/documentos
    Separada da consulta para permitir buscar documentos após validar o processo.

    Args:
        token: Token de autenticação SEI
        protocolo: Número do processo (formatado ou só dígitos)

    Returns:
        list de documentos ou lista vazia se falhar
    """
    protocolo_limpo = "".join(filter(str.isdigit, protocolo))
    if not token or not protocolo_limpo:
        return []

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

    try:
        response = requests.get(url, params=params, headers=headers, timeout=60)

        if response.status_code != 200:
            return []

        data = response.json()

        # Parsing: resposta pode ser dict com 'Documentos', 'resultados', ou lista direta
        documentos = []
        if isinstance(data, dict):
            documentos = data.get('Documentos', [])
            if not documentos and 'resultados' in data:
                documentos = data['resultados']
        elif isinstance(data, list):
            documentos = data

        return documentos

    except Exception as e:
        print(f"Erro ao listar documentos SEI: {e}")
        return []