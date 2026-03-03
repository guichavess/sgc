"""
Script para consultar hipóteses legais do SEI API.
Busca opções de restrição de acesso para documentos.
"""
import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://api.sei.pi.gov.br"

# ── 1. Autenticação ──────────────────────────────────────────────────────────
print("=" * 80)
print("ETAPA 1: Autenticação")
print("=" * 80)

login_payload = {
    "Usuario": "pedro.alexandre@sead.pi.gov.br",
    "Senha": "Ppedro1920.",
    "Orgao": "SEAD-PI"
}

resp = requests.post(
    f"{BASE_URL}/v1/orgaos/usuarios/login",
    json=login_payload,
    verify=False,
    timeout=30
)

print(f"Status: {resp.status_code}")
print(f"Response: {resp.text[:500]}")

if resp.status_code != 200:
    print("ERRO: Falha na autenticação. Abortando.")
    exit(1)

token = resp.json()["Token"]
print(f"Token obtido: {token[:20]}...")

headers = {
    "token": token,
    "Accept": "application/json"
}

# ── 2. Tentar endpoints de hipóteses legais ───────────────────────────────────
print("\n" + "=" * 80)
print("ETAPA 2: Buscar hipóteses legais")
print("=" * 80)

endpoints = [
    "/v1/hipoteses-legais",
    "/v1/unidades/110006213/hipoteses-legais",
    "/v1/hipoteses_legais",
    "/v1/unidades/110006213/hipoteses_legais",
    "/v1/hipoteses",
    "/v1/unidades/110006213/restricoes",
    "/v1/niveis-acesso",
    "/v1/unidades/110006213/niveis-acesso",
    "/v1/tipos-restricao",
]

for ep in endpoints:
    print(f"\n{'─' * 70}")
    print(f"GET {ep}")
    print(f"{'─' * 70}")
    try:
        r = requests.get(
            f"{BASE_URL}{ep}",
            headers=headers,
            verify=False,
            timeout=30
        )
        print(f"Status: {r.status_code}")
        # Pretty print JSON if possible
        try:
            data = r.json()
            text = json.dumps(data, indent=2, ensure_ascii=False)
            if len(text) > 3000:
                print(text[:3000])
                print(f"\n... (truncado, total {len(text)} chars)")
            else:
                print(text)
        except Exception:
            print(f"Response (text): {r.text[:1000]}")
    except Exception as e:
        print(f"ERRO: {e}")

# ── 3. Consultar OpenAPI spec ─────────────────────────────────────────────────
print("\n" + "=" * 80)
print("ETAPA 3: Verificar OpenAPI spec")
print("=" * 80)

openapi_paths = ["/openapi.json", "/swagger.json", "/v1/openapi.json", "/v1/swagger.json", "/api-docs"]

for path in openapi_paths:
    print(f"\nGET {path}")
    try:
        r = requests.get(f"{BASE_URL}{path}", verify=False, timeout=30)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            try:
                spec = r.json()
                # Search for hipotese-related paths
                if "paths" in spec:
                    print(f"\nTotal paths found: {len(spec['paths'])}")
                    for p, methods in spec["paths"].items():
                        p_lower = p.lower()
                        if any(kw in p_lower for kw in ["hipotese", "hipótese", "hipoteses", "hipóteses", "restricao", "restricão", "acesso", "nivel", "nível"]):
                            print(f"\n  MATCH: {p}")
                            for method, detail in methods.items():
                                if isinstance(detail, dict):
                                    summary = detail.get("summary", detail.get("description", ""))
                                    print(f"    {method.upper()}: {summary}")
                else:
                    text = json.dumps(spec, indent=2, ensure_ascii=False)
                    print(text[:2000])
            except Exception:
                print(r.text[:2000])
        else:
            print(r.text[:300])
    except Exception as e:
        print(f"ERRO: {e}")

# ── 4. Se encontrou hipóteses, filtrar por dados pessoais ─────────────────────
print("\n" + "=" * 80)
print("ETAPA 4: Resumo - buscando 'informação pessoal' / 'dados pessoais'")
print("=" * 80)

# Re-try the most likely successful endpoint and filter
for ep in ["/v1/hipoteses-legais", "/v1/unidades/110006213/hipoteses-legais", "/v1/hipoteses_legais"]:
    try:
        r = requests.get(f"{BASE_URL}{ep}", headers=headers, verify=False, timeout=30)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                print(f"\nEndpoint {ep} retornou {len(data)} hipóteses.")
                print("\nTodas as hipóteses:")
                for i, h in enumerate(data):
                    print(f"  [{i+1}] {json.dumps(h, ensure_ascii=False)}")
                
                print("\nFiltradas (pessoal/pessoais/informação/dados):")
                for h in data:
                    h_str = json.dumps(h, ensure_ascii=False).lower()
                    if any(kw in h_str for kw in ["pessoal", "pessoais", "informação", "informacao", "dados", "sigilo", "restrit"]):
                        print(f"  >>> {json.dumps(h, indent=4, ensure_ascii=False)}")
                break
    except Exception:
        continue

print("\n" + "=" * 80)
print("Fim da consulta.")
print("=" * 80)
