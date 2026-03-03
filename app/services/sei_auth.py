import requests
import json
import os
from flask import current_app

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
        response = requests.post(url_auth, json=payload, headers=headers, timeout=15)
        
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