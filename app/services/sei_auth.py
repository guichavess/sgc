import requests
import json
import os
import urllib3
from flask import current_app

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def gerar_token_sei_admin():
    """
    Gera um token de acesso ao SEI utilizando credenciais administrativas.
    Tenta ler do ambiente (OS) ou da configuração do Flask.
    """
    url_auth = "https://api.sei.pi.gov.br/v1/orgaos/usuarios/login"
    
    # 1. Tenta pegar do Sistema Operacional OU da Configuração do Flask
    usuario = os.getenv("SEI_USER") or current_app.config.get("SEI_USER")
    senha = os.getenv("SEI_PASSWORD") or current_app.config.get("SEI_PASSWORD")
    orgao = os.getenv("SEI_ORGAO", "SEAD-PI") # Default para SEAD-PI se não definido

    if not usuario or not senha:
        current_app.logger.error("❌ ERRO SEI AUTH: Credenciais (SEI_USER/SEI_PASSWORD) não encontradas no .env ou config.")
        return None

    payload = {
        "Usuario": usuario,
        "Senha": senha,
        "Orgao": orgao
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        response = requests.post(url_auth, json=payload, headers=headers, timeout=120, verify=False)
        
        if response.status_code == 200:
            dados = response.json()
            
            # 2. Tenta extrair token de várias chaves possíveis do JSON
            token = dados.get('IdSession') or dados.get('token') or dados.get('Token')
            
            # 3. Se não veio no JSON, tenta pegar dos Headers
            if not token:
                token = response.headers.get('token') or response.headers.get('Token')
                
            if token:
                return token
            else:
                current_app.logger.error(f"❌ ERRO SEI AUTH: Token não encontrado na resposta. Dados: {dados}")
                return None
        else:
            current_app.logger.error(f"❌ ERRO SEI AUTH: Status {response.status_code}. Resposta: {response.text}")
            return None
            
    except Exception as e:
        current_app.logger.error(f"❌ EXCEÇÃO SEI AUTH: {str(e)}")
        return None


def autenticar_usuario_sei(usuario, senha, protocolo_bypass=None):
    """
    Autentica um usuario especifico no SEI e retorna dados completos.

    Diferente de gerar_token_sei_admin() que so retorna o token,
    esta funcao retorna o dict completo com token, IdUsuario, IdLogin, etc.
    Usado para operacoes que precisam identificar o usuario (ex: assinatura).

    Bypass:
    - Se DIARIAS_BYPASS_ASSINATURAS=True (global), simula.
    - Se `protocolo_bypass` está em DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS,
      simula apenas para esse processo.

    Args:
        usuario: Login do usuario no SEI
        senha: Senha do usuario no SEI
        protocolo_bypass: protocolo SEI do processo (ex: '00002.003853/2026-21').
            Quando fornecido e na lista de bypass, retorna token fake
            sem chamar a API real do SEI.

    Returns:
        dict com {token, id_usuario, id_login, dados_completos} ou None em caso de erro
    """
    # ── Bypass para testes (global OU por protocolo específico) ──
    from app.constants import protocolo_tem_bypass_assinatura
    if protocolo_tem_bypass_assinatura(protocolo_bypass):
        current_app.logger.info(
            f"[BYPASS] Autenticação SEI simulada para usuario={usuario} "
            f"(processo={protocolo_bypass!r})"
        )
        return {
            'token': 'bypass-token',
            'id_usuario': 'bypass-user',
            'id_login': 'bypass-login',
            'nome': usuario,
            'cargo': '',
            'unidades': [],
            'dados_completos': {},
        }
    url_auth = "https://api.sei.pi.gov.br/v1/orgaos/usuarios/login"
    orgao = os.getenv("SEI_ORGAO", "SEAD-PI")

    payload = {
        "Usuario": usuario,
        "Senha": senha,
        "Orgao": orgao,
    }

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    try:
        response = requests.post(url_auth, json=payload, headers=headers, timeout=120, verify=False)

        if response.status_code == 200:
            dados = response.json()

            # Estrutura SEI: {"Token": "...", "Login": {"IdUsuario": ..., "IdLogin": ...}, ...}
            # Token fica na RAIZ do JSON (igual ao que o modulo pagamentos usa em auth/routes.py)
            login_data = dados.get('Login', dados)

            token = dados.get('Token') or dados.get('token') or dados.get('IdSession')
            if not token:
                token = login_data.get('IdSession') or login_data.get('token') or login_data.get('Token')
            if not token:
                token = response.headers.get('token') or response.headers.get('Token')

            if not token:
                current_app.logger.error(
                    f"SEI AUTH Usuario: Token nao encontrado. Dados: {dados}"
                )
                return None

            return {
                'token': token,
                'id_usuario': str(login_data.get('IdUsuario', '')),
                'id_login': str(login_data.get('IdLogin', '') or token),
                'nome': login_data.get('Nome', ''),
                'cargo': login_data.get('UltimoCargoAssinatura', ''),
                'id_unidade_atual': str(login_data.get('IdUnidadeAtual', '')),
                'unidades': dados.get('Unidades', []),
                'dados_completos': dados,
            }
        else:
            current_app.logger.error(
                f"SEI AUTH Usuario: Status {response.status_code}. Resposta: {response.text[:300]}"
            )
            return None

    except Exception as e:
        current_app.logger.error(f"SEI AUTH Usuario: Excecao: {str(e)}")
        return None