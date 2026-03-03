"""
Script de teste: Consulta assinaturas de documentos SEI via API.
"""
import requests
import os
import json
import urllib3
urllib3.disable_warnings()
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

BASE_URL = 'https://api.sei.pi.gov.br'
UNIDADE = '110006213'

# 1. Autenticar
print("=== Autenticando no SEI ===")
resp = requests.post(f'{BASE_URL}/v1/orgaos/usuarios/login', json={
    'Usuario': os.getenv('SEI_USER'),
    'Senha': os.getenv('SEI_PASSWORD'),
    'Orgao': 'SEAD-PI'
}, headers={'Content-Type': 'application/json', 'Accept': 'application/json'}, timeout=15, verify=False)
token = resp.json().get('Token')
print(f"Token obtido: {bool(token)}")

# 2. Testar GET /v1/unidades/{id}/documentos com protocolo_documento
print("\n=== GET /documentos (documento individual) ===")
for param_ass in ['sinal_retornar_assinaturas', 'sinRetornarAssinaturas', 'SinRetornarAssinaturas']:
    r = requests.get(
        f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos',
        params={
            'protocolo_documento': '0020123947',
            param_ass: 'S',
        },
        headers={'token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'},
        timeout=30, verify=False,
    )
    doc = r.json()
    keys = list(doc.keys()) if isinstance(doc, dict) else '?'
    ass = doc.get('Assinaturas', 'NAO_RETORNADO')
    print(f"  {param_ass}: keys={keys}, Assinaturas={ass}")

# 3. Testar com TODOS os sinais habilitados
print("\n=== Teste com todos os sinais habilitados ===")
protocolo_proc = '00002009305202523'

# Combinações de nomes possiveis para assinatura
signal_combos = [
    # Combo 1: snake_case
    {
        'sinal_retornar_andamento_geracao': 'S',
        'sinal_retornar_assinaturas': 'S',
        'sinal_retornar_publicacao': 'S',
        'sinal_retornar_campos': 'S',
    },
    # Combo 2: camelCase
    {
        'sinRetornarAndamentoGeracao': 'S',
        'sinRetornarAssinaturas': 'S',
        'sinRetornarPublicacao': 'S',
        'sinRetornarCampos': 'S',
    },
    # Combo 3: PascalCase
    {
        'SinRetornarAndamentoGeracao': 'S',
        'SinRetornarAssinaturas': 'S',
        'SinRetornarPublicacao': 'S',
        'SinRetornarCampos': 'S',
    },
]

for i, combo in enumerate(signal_combos):
    params = {
        'protocolo_procedimento': protocolo_proc,
        'pagina': 1,
        'quantidade': 2,
        'sinal_retornar_conteudo': 'N',
        **combo,
    }
    style = ['snake_case', 'camelCase', 'PascalCase'][i]
    r = requests.get(
        f'{BASE_URL}/v1/unidades/{UNIDADE}/procedimentos/documentos',
        params=params,
        headers={'token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'},
        timeout=30, verify=False,
    )
    print(f"\n  Combo {style} -> status={r.status_code}")
    if r.status_code == 200:
        data = r.json()
        docs = data.get('Documentos', [])
        if docs:
            d = docs[0]
            keys_doc = list(d.keys())
            print(f"    Keys do 1o doc: {keys_doc}")
            ass = d.get('Assinaturas', 'NAO_RETORNADO')
            andamento = d.get('AndamentoGeracao', 'NAO_RETORNADO')
            publicacao = d.get('Publicacao', 'NAO_RETORNADO')
            campos = d.get('Campos', 'NAO_RETORNADO')
            print(f"    Assinaturas: {type(ass).__name__} len={len(ass) if isinstance(ass, list) else '?'}")
            print(f"    AndamentoGeracao: {type(andamento).__name__}")
            print(f"    Publicacao: {type(publicacao).__name__}")
            print(f"    Campos: {type(campos).__name__}")
            if isinstance(ass, list) and len(ass) > 0:
                print(f"\n  >>> SUCESSO! Combo '{style}' retorna assinaturas!")
                for a in ass:
                    print(f"      - {a.get('Nome')} | {a.get('CargoFuncao')}")
                break
    else:
        print(f"    Erro: {r.text[:300]}")

# 4. Teste direto: GET /v1/unidades/{id}/documentos (singular, individual) com variações
print("\n\n=== Teste GET /documentos (individual) com todos os sinais ===")
for combo in signal_combos:
    params = {
        'protocolo_documento': '0020123947',
        **combo,
    }
    style = ['snake_case', 'camelCase', 'PascalCase'][signal_combos.index(combo)]
    r = requests.get(
        f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos',
        params=params,
        headers={'token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'},
        timeout=30, verify=False,
    )
    print(f"\n  Combo {style} -> status={r.status_code}")
    if r.status_code == 200:
        doc = r.json()
        print(f"    Keys: {list(doc.keys())}")
        ass = doc.get('Assinaturas', 'NAO_RETORNADO')
        if isinstance(ass, list) and len(ass) > 0:
            print(f"    >>> SUCESSO! {len(ass)} assinaturas encontradas!")
            for a in ass:
                print(f"      - {a.get('Nome')} | {a.get('CargoFuncao')}")
            break
