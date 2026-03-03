"""
Teste 3: Explorar se os sinais sao passados via headers ou com valores diferentes.
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
PROTOCOLO_PROC = '00002009305202523'

# Autenticar
resp = requests.post(f'{BASE_URL}/v1/orgaos/usuarios/login', json={
    'Usuario': os.getenv('SEI_USER'),
    'Senha': os.getenv('SEI_PASSWORD'),
    'Orgao': 'SEAD-PI'
}, headers={'Content-Type': 'application/json', 'Accept': 'application/json'}, timeout=15, verify=False)
token = resp.json().get('Token')
print(f"Token: {'OK' if token else 'FALHOU'}")

# Teste 1: sinais como headers
print("\n=== 1. Sinais como HEADERS ===")
r = requests.get(
    f'{BASE_URL}/v1/unidades/{UNIDADE}/procedimentos/documentos',
    params={
        'protocolo_procedimento': PROTOCOLO_PROC,
        'pagina': 1,
        'quantidade': 1,
    },
    headers={
        'token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'sinal_retornar_assinaturas': 'S',
        'sinal_retornar_andamento_geracao': 'S',
        'sinal_retornar_publicacao': 'S',
        'sinal_retornar_campos': 'S',
        'sinal_retornar_conteudo': 'N',
    },
    timeout=30, verify=False,
)
if r.status_code == 200:
    data = r.json()
    docs = data.get('Documentos', [])
    if docs:
        d = docs[0]
        print(f"  Keys: {list(d.keys())}")
        if 'Assinaturas' in d:
            print(f"  >>> HEADERS FUNCIONA! Assinaturas: {len(d['Assinaturas'])}")

# Teste 2: sinais com valores true/false
print("\n=== 2. Sinais com true/false ===")
for val in ['true', 'True', '1', 'sim']:
    r = requests.get(
        f'{BASE_URL}/v1/unidades/{UNIDADE}/procedimentos/documentos',
        params={
            'protocolo_procedimento': PROTOCOLO_PROC,
            'pagina': 1,
            'quantidade': 1,
            'sinal_retornar_assinaturas': val,
        },
        headers={'token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'},
        timeout=30, verify=False,
    )
    if r.status_code == 200:
        data = r.json()
        docs = data.get('Documentos', [])
        if docs and 'Assinaturas' in docs[0]:
            print(f"  valor='{val}' -> FUNCIONA! Assinaturas: {len(docs[0]['Assinaturas'])}")
            break
        else:
            keys = list(docs[0].keys()) if docs else '?'
            print(f"  valor='{val}' -> keys={keys}")

# Teste 3: sinais com nome diferente (sem prefixo sinal_)
print("\n=== 3. Nomes alternativos sem prefixo ===")
alt_names = [
    'retornar_assinaturas',
    'retornarAssinaturas',
    'RetornarAssinaturas',
    'assinaturas',
    'Assinaturas',
    'incluir_assinaturas',
]
for name in alt_names:
    r = requests.get(
        f'{BASE_URL}/v1/unidades/{UNIDADE}/procedimentos/documentos',
        params={
            'protocolo_procedimento': PROTOCOLO_PROC,
            'pagina': 1,
            'quantidade': 1,
            name: 'S',
        },
        headers={'token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'},
        timeout=30, verify=False,
    )
    if r.status_code == 200:
        data = r.json()
        docs = data.get('Documentos', [])
        if docs and 'Assinaturas' in docs[0]:
            print(f"  '{name}'=S -> FUNCIONA!")
            break
    print(f"  '{name}'=S -> nao")

# Teste 4: GET individual /documentos com protocolo_documento e sinais no body (POST?)
print("\n=== 4. GET /documentos individual - resposta raw completa ===")
r = requests.get(
    f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos',
    params={
        'protocolo_documento': '0020123947',
        'sinal_retornar_andamento_geracao': 'S',
        'sinal_retornar_assinaturas': 'S',
        'sinal_retornar_publicacao': 'S',
        'sinal_retornar_campos': 'S',
    },
    headers={'token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'},
    timeout=30, verify=False,
)
print(f"  Status: {r.status_code}")
print(f"  Response headers: {dict(r.headers)}")
print(f"  Body: {r.text[:500]}")

# Teste 5: Checar se existem mais rotas disponiveis
print("\n=== 5. Testar endpoints de assinaturas diretamente ===")
alt_endpoints = [
    f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos/22361284/assinaturas',
    f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos/0020123947/assinaturas',
    f'{BASE_URL}/v1/documentos/22361284/assinaturas',
]
for url in alt_endpoints:
    try:
        r = requests.get(url, headers={'token': token, 'Accept': 'application/json'}, timeout=10, verify=False)
        print(f"  {url.split('.br')[-1]}: status={r.status_code}")
        if r.status_code == 200:
            print(f"    >>> {r.text[:300]}")
    except Exception as e:
        print(f"  {url.split('.br')[-1]}: ERRO")
