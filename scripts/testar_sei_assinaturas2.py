"""
Teste 2: Explorando todas as possibilidades de endpoints SEI para obter assinaturas.
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
ID_DOCUMENTO = '22361284'
DOC_FORMATADO = '0020123947'
PROTOCOLO_PROC = '00002009305202523'
PROTOCOLO_PROC_FORMATADO = '00002.009305/2025-23'

# Autenticar
resp = requests.post(f'{BASE_URL}/v1/orgaos/usuarios/login', json={
    'Usuario': os.getenv('SEI_USER'),
    'Senha': os.getenv('SEI_PASSWORD'),
    'Orgao': 'SEAD-PI'
}, headers={'Content-Type': 'application/json', 'Accept': 'application/json'}, timeout=15, verify=False)
token = resp.json().get('Token')
headers = {'token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'}
print(f"Token: {'OK' if token else 'FALHOU'}")

# Teste 1: GET /v1/unidades/{id}/documentos/{id_documento} (path param)
print("\n=== 1. GET /documentos/{id_documento} (path param) ===")
urls_test = [
    f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos/{ID_DOCUMENTO}',
    f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos/{DOC_FORMATADO}',
]
for url in urls_test:
    r = requests.get(url, headers=headers, timeout=30, verify=False)
    print(f"  {url.split('documentos/')[-1]}: status={r.status_code}")
    if r.status_code == 200:
        doc = r.json()
        print(f"    Keys: {list(doc.keys()) if isinstance(doc, dict) else '?'}")
        ass = doc.get('Assinaturas', 'NAO')
        if isinstance(ass, list):
            print(f"    >>> Assinaturas: {len(ass)}")
            for a in ass:
                print(f"      - {a.get('Nome')} | {a.get('CargoFuncao')}")
    elif r.status_code != 404:
        print(f"    Resp: {r.text[:200]}")

# Teste 2: Com protocolo formatado (com pontos)
print("\n=== 2. GET /procedimentos/documentos com protocolo formatado ===")
r = requests.get(
    f'{BASE_URL}/v1/unidades/{UNIDADE}/procedimentos/documentos',
    params={
        'protocolo_procedimento': PROTOCOLO_PROC_FORMATADO,
        'pagina': 1,
        'quantidade': 2,
        'sinal_retornar_conteudo': 'N',
        'sinal_retornar_assinaturas': 'S',
        'sinal_retornar_andamento_geracao': 'S',
    },
    headers=headers, timeout=30, verify=False,
)
print(f"  status={r.status_code}")
if r.status_code == 200:
    data = r.json()
    docs = data.get('Documentos', data if isinstance(data, list) else [])
    if docs:
        d = docs[0] if isinstance(docs, list) else docs
        print(f"  Keys 1o doc: {list(d.keys()) if isinstance(d, dict) else '?'}")

# Teste 3: GET /v1/procedimentos/{protocolo}/documentos (sem unidade)
print("\n=== 3. Endpoints alternativos ===")
alt_urls = [
    f'{BASE_URL}/v1/procedimentos/{ID_DOCUMENTO}/documentos',
    f'{BASE_URL}/v1/documentos/{ID_DOCUMENTO}',
    f'{BASE_URL}/v2/unidades/{UNIDADE}/documentos',
]
for url in alt_urls:
    try:
        r = requests.get(url, params={'protocolo_documento': DOC_FORMATADO},
                        headers=headers, timeout=10, verify=False)
        print(f"  {url}: status={r.status_code}")
    except Exception as e:
        print(f"  {url}: ERRO {e}")

# Teste 4: Dump completo da resposta para ver TODOS os campos retornados
print("\n=== 4. Dump completo do GET /documentos ===")
r = requests.get(
    f'{BASE_URL}/v1/unidades/{UNIDADE}/documentos',
    params={'protocolo_documento': DOC_FORMATADO},
    headers=headers, timeout=30, verify=False,
)
if r.status_code == 200:
    print(json.dumps(r.json(), indent=2, ensure_ascii=False))

# Teste 5: Listar procedimentos/documentos sem limites de quantidade
print("\n=== 5. GET /procedimentos/documentos - resposta COMPLETA primeiro doc ===")
r = requests.get(
    f'{BASE_URL}/v1/unidades/{UNIDADE}/procedimentos/documentos',
    params={
        'protocolo_procedimento': PROTOCOLO_PROC,
        'pagina': 1,
        'quantidade': 1,
    },
    headers=headers, timeout=30, verify=False,
)
if r.status_code == 200:
    data = r.json()
    print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])
