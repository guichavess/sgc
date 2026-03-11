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
        response = requests.post(url_auth, json=payload, headers=headers, timeout=15, verify=False)
        
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


def autenticar_usuario_sei(usuario, senha):
    """
    Autentica um usuario especifico no SEI e retorna dados completos.

    Diferente de gerar_token_sei_admin() que so retorna o token,
    esta funcao retorna o dict completo com token, IdUsuario, IdLogin, etc.
    Usado para operacoes que precisam identificar o usuario (ex: assinatura).

    Args:
        usuario: Login do usuario no SEI
        senha: Senha do usuario no SEI

    Returns:
        dict com {token, id_usuario, id_login, dados_completos} ou None em caso de erro
    """
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
        response = requests.post(url_auth, json=payload, headers=headers, timeout=15, verify=False)

        if response.status_code == 200:
            dados = response.json()

            token = dados.get('IdSession') or dados.get('token') or dados.get('Token')
            if not token:
                token = response.headers.get('token') or response.headers.get('Token')

            if not token:
                current_app.logger.error(
                    f"SEI AUTH Usuario: Token nao encontrado. Dados: {dados}"
                )
                return None

            return {
                'token': token,
                'id_usuario': str(dados.get('IdUsuario', '')),
                'id_login': str(dados.get('IdLogin', '') or token),
                'nome': dados.get('Nome', ''),
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