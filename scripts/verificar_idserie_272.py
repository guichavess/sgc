"""
Script para verificar quais processos de diárias possuem documento com IdSerie 272.
Uso: python scripts/verificar_idserie_272.py
Usa ThreadPoolExecutor para consultas paralelas.
"""
import requests
import os
import sys
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

BASE_URL = "https://api.sei.pi.gov.br"
UNIDADE_SEAD = "110006213"
MAX_WORKERS = 8

PROCESSOS = [
    "00002.003166/2026-13", "00002.009305/2025-23", "00002.012970/2025-02",
    "00002.013017/2025-73", "00002.000065/2026-82", "00002.000129/2026-45",
    "00002.000140/2026-13", "00002.000461/2026-18", "00002.012771/2025-96",
    "00002.000084/2026-17", "00002.000088/2026-97", "00002.000091/2026-19",
    "00002.000141/2026-50", "00002.000303/2026-50", "00002.000325/2026-10",
    "00002.000304/2026-02", "00002.000596/2026-75", "00002.000610/2026-31",
    "00002.000633/2026-45", "00002.000634/2026-90", "00002.000655/2026-13",
    "00002.000657/2026-02", "00002.000727/2026-14", "00002.000747/2026-95",
    "00002.000786/2026-92", "00002.000837/2026-86", "00002.000845/2026-22",
    "00002.000853/2026-79", "00002.000914/2026-06", "00002.001021/2026-70",
    "00002.001037/2026-82", "00002.001074/2026-91", "00002.001169/2026-12",
    "00002.001280/2026-09", "00002.001330/2026-40", "00002.001350/2026-11",
    "00002.001352/2026-18", "00002.001353/2026-54", "00002.001370/2026-91",
    "00002.001476/2026-95", "00002.001502/2026-85", "00002.001571/2026-99",
    "00002.001588/2026-46", "00002.001618/2026-14", "00002.001645/2026-97",
    "00002.001653/2026-33", "00002.001657/2026-11", "00002.001683/2026-40",
    "00002.001691/2026-96", "00002.001693/2026-85", "00002.001697/2026-63",
    "00002.001710/2026-84", "00002.001759/2026-37", "00002.001781/2026-87",
    "00002.001783/2026-76", "00002.001788/2026-07", "00002.001790/2026-78",
    "00002.001793/2026-10", "00002.001863/2026-21", "00002.001869/2026-07",
    "00002.001879/2026-34", "00002.001901/2026-46", "00002.002092/2026-90",
    "00002.002118/2026-08", "00002.002270/2026-82", "00002.002391/2026-24",
    "00002.002427/2026-70", "00002.002433/2026-27", "00002.002475/2026-68",
    "00002.002489/2026-81", "00002.002509/2026-14", "00002.002539/2026-21",
    "00002.002569/2026-37", "00002.002620/2026-19", "00002.002637/2026-68",
    "00002.002657/2026-39", "00002.002702/2026-55", "00002.002708/2026-22",
    "00002.002711/2026-46", "00002.002714/2026-80", "00002.002715/2026-24",
    "00002.002717/2026-13", "00002.002719/2026-11", "00002.002724/2026-15",
    "00002.002725/2026-60", "00002.002731/2026-17", "00002.002792/2026-84",
    "00002.002795/2026-18", "00002.002800/2026-92", "00002.002831/2026-43",
    "00002.002836/2026-76", "00002.002838/2026-65", "00002.002842/2026-23",
    "00002.002844/2026-12", "00002.002846/2026-10", "00002.002853/2026-11",
    "00002.002855/2026-01", "00002.002858/2026-36", "00002.002860/2026-13",
    "00002.002861/2026-50", "00002.002865/2026-38", "00002.002964/2026-10",
    "00002.002967/2026-53", "00002.002990/2026-48", "00002.003003/2026-22",
    "00002.003013/2026-68", "00002.003023/2026-01", "00002.003034/2026-83",
    "00002.003036/2026-72", "00002.003041/2026-85", "00002.003092/2026-15",
    "00002.003101/2026-60", "00002.003258/2026-95", "00002.002437/2026-13",
    "00002.000189/2026-68", "00002.000663/2026-51", "00002.000751/2026-53",
    "00002.001091/2026-28", "00002.001113/2026-50", "00002.001188/2026-31",
    "00002.001240/2026-59", "00002.001488/2026-10", "00002.001627/2026-13",
    "00002.001775/2026-20", "00002.001950/2026-89", "00002.002031/2026-22",
    "00002.001134/2026-75", "00002.000665/2026-41",
]

ID_SERIE_ALVO = "272"


def autenticar():
    url = f"{BASE_URL}/v1/orgaos/usuarios/login"
    usuario = os.getenv("SEI_USER")
    senha = os.getenv("SEI_PASSWORD")
    orgao = os.getenv("SEI_ORGAO", "SEAD-PI")

    if not usuario or not senha:
        print("ERRO: SEI_USER/SEI_PASSWORD nao definidos no .env", flush=True)
        return None

    payload = {"Usuario": usuario, "Senha": senha, "Orgao": orgao}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    resp = requests.post(url, json=payload, headers=headers, timeout=15, verify=False)
    if resp.status_code == 200:
        dados = resp.json()
        token = dados.get("Token") or dados.get("token") or dados.get("IdSession")
        if token:
            print(f"Autenticado com sucesso.\n", flush=True)
            return token
    print(f"Falha na autenticacao: {resp.status_code} {resp.text[:200]}", flush=True)
    return None


def verificar_processo(token, protocolo):
    """Consulta um processo e retorna (protocolo, 'COM'|'SEM'|'ERRO')."""
    protocolo_limpo = "".join(filter(str.isdigit, protocolo))
    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/documentos"
    params = {
        "protocolo_procedimento": protocolo_limpo,
        "pagina": 1,
        "quantidade": 1000,
        "sinal_completo": "N",
    }
    headers = {"token": token, "Accept": "application/json"}

    for tentativa in range(1, 4):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=60, verify=False)
            if resp.status_code == 200:
                data = resp.json()
                docs = data.get("Documentos", [])
                tem = any(
                    str(d.get("Serie", {}).get("IdSerie", "")) == ID_SERIE_ALVO
                    for d in docs
                )
                return (protocolo, "COM" if tem else "SEM")
        except Exception:
            pass
        if tentativa < 3:
            time.sleep(tentativa * 3)

    return (protocolo, "ERRO")


def main():
    token = autenticar()
    if not token:
        return

    com_272 = []
    sem_272 = []
    erros = []
    concluidos = 0
    total = len(PROCESSOS)

    print(f"Iniciando consulta de {total} processos com {MAX_WORKERS} workers...\n", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(verificar_processo, token, proc): proc
            for proc in PROCESSOS
        }

        for future in as_completed(futures):
            concluidos += 1
            protocolo, resultado = future.result()

            if resultado == "COM":
                com_272.append(protocolo)
                status = f"TEM IdSerie {ID_SERIE_ALVO}"
            elif resultado == "SEM":
                sem_272.append(protocolo)
                status = f"NAO TEM IdSerie {ID_SERIE_ALVO}"
            else:
                erros.append(protocolo)
                status = "ERRO"

            print(f"[{concluidos}/{total}] {protocolo} -> {status}", flush=True)

    # === RELATORIO ===
    print("\n" + "=" * 70, flush=True)
    print(f"RELATORIO - IdSerie {ID_SERIE_ALVO}", flush=True)
    print("=" * 70, flush=True)
    print(f"\nTotal de processos: {total}", flush=True)
    print(f"COM IdSerie {ID_SERIE_ALVO}: {len(com_272)}", flush=True)
    print(f"SEM IdSerie {ID_SERIE_ALVO}: {len(sem_272)}", flush=True)
    print(f"ERROS: {len(erros)}", flush=True)

    if com_272:
        print(f"\n--- PROCESSOS COM IdSerie {ID_SERIE_ALVO} ({len(com_272)}) ---", flush=True)
        for p in sorted(com_272):
            print(f"  {p}", flush=True)

    if sem_272:
        print(f"\n--- PROCESSOS SEM IdSerie {ID_SERIE_ALVO} ({len(sem_272)}) ---", flush=True)
        for p in sorted(sem_272):
            print(f"  {p}", flush=True)

    if erros:
        print(f"\n--- PROCESSOS COM ERRO ({len(erros)}) ---", flush=True)
        for p in sorted(erros):
            print(f"  {p}", flush=True)


if __name__ == "__main__":
    main()
