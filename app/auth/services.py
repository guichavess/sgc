import requests
import json

def realizar_login_sei(usuario, senha, orgao):
    url = "https://api.sei.pi.gov.br/v1/orgaos/usuarios/login"
    payload = {
        "Usuario": usuario,
        "Senha": senha,
        "Orgao": orgao
    }
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status() # Levanta erro para status 4xx ou 5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro na conexão com SEI: {e}")
        return None