"""
Script para atualizar dados de Programacao de Desembolso (PD) do SIAFE.

Busca dados da API SIAFE e grava nas tabelas:
  - pd (tabela principal)
  - pd_itens (classificadores expandidos)

Uso:
    python scripts/atualizar_pd.py
"""
import time
import warnings
import requests
import pandas as pd
import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text, create_engine

# Suprime warnings de SSL (verify=False)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# 1. CARREGAR VARIÁVEIS DE AMBIENTE
# =========================
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')

from dotenv import load_dotenv
load_dotenv(dotenv_path)

if not os.getenv('DB_USER'):
    print(f"AVISO: Arquivo .env nao encontrado ou variaveis vazias. Buscado em: {dotenv_path}")

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO CRITICO: Variaveis de banco de dados ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
ENGINE = create_engine(DATABASE_URI, echo=False)

# =========================
# 2. AUTENTICAÇÃO SIAFE
# =========================
def get_token():
    siafe_user = os.getenv('SIAFE_USUARIO')
    siafe_pass = os.getenv('SIAFE_SENHA')

    if not siafe_user or not siafe_pass:
        print("ERRO: Variaveis SIAFE_USUARIO e SIAFE_SENHA nao encontradas no .env")
        sys.exit(1)

    url = "https://tesouro.sefaz.pi.gov.br/siafe-api/auth"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"usuario": siafe_user, "senha": siafe_pass})

    max_tentativas = 3
    for tentativa in range(1, max_tentativas + 1):
        try:
            print(f"  Tentativa {tentativa}/{max_tentativas}...")
            response = requests.post(url, headers=headers, data=payload, timeout=60, verify=False)
            if response.status_code == 200:
                return response.json()['token']
            else:
                raise Exception(f"Erro Auth SIAFE: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"  Falha na tentativa {tentativa}: {e}")
            if tentativa < max_tentativas:
                print(f"  Aguardando 5s antes de tentar novamente...")
                time.sleep(5)
            else:
                print("ERRO: Nao foi possivel autenticar apos todas as tentativas.")
                sys.exit(1)


print("Autenticando no SIAFE...")
TOKEN = get_token()

# =========================
# 3. CONFIGURAÇÕES
# =========================

# Anos a processar
YEARS = [2023,2024, 2025, 2026]

# UG
UG_MODE = "single"      # "single" ou "all"
UG_SINGLE = "210101"
UG_PAD_SIZE = 6

# Tabelas destino
TABELA_PD = "pd"
TABELA_PD_ITENS = "pd_itens"

# DELETE por ano
DELETE_DATE_FIELD = "dataEmissao"

# Performance
MAX_WORKERS_CAP = 16
CHUNKSIZE_SQL = 100

# =========================
# 4. COLUNAS
# =========================
COLUMNS_PD = [
    "id", "codigo", "codProcesso", "statusDocumento", "codigoUG",
    "codigoGestao", "codFonte", "codNatureza", "codigoCredor", "nomeCredor",
    "dataEmissao", "valor", "itens", "observacao", "codClassificacao",
    "statusExecucao", "codUgPagadora", "codigoOB", "codigoNE", "codigoNL",
    "valorTotalPD",
]

INT_COLUMNS = [
    "codigoGestao", "codFonte", "codNatureza", "codigoCredor",
    "codUgPagadora", "codContrato",
]

DATE_COLUMNS = ["dataEmissao"]

# =========================
# 5. FUNÇÕES AUXILIARES
# =========================

def make_session():
    session = requests.Session()
    session.verify = False
    retry = Retry(
        total=5, connect=5, read=5, status=5, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def normalize_ug(value, size):
    if value is None:
        return None
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = [c for c in s if c.isdigit()]
    s = "".join(digits)
    return s.zfill(size) if s else None


def load_ugs_from_db():
    q = text("SELECT codigo FROM ug")
    with ENGINE.connect() as conn:
        rows = conn.execute(q).fetchall()
    ugs = []
    seen = set()
    for r in rows:
        if not r:
            continue
        ug = normalize_ug(r[0], UG_PAD_SIZE)
        if ug and ug not in seen:
            seen.add(ug)
            ugs.append(ug)
    return ugs


def resolve_ugs():
    mode = (UG_MODE or "").lower().strip()
    if mode == "single":
        ug = normalize_ug(UG_SINGLE, UG_PAD_SIZE)
        if not ug:
            raise ValueError("UG_SINGLE esta vazio/invalido, mas UG_MODE='single'.")
        print(f"Modo SINGLE selecionado. UG alvo: {ug}")
        return [ug]
    if mode == "all":
        ugs = load_ugs_from_db()
        if not ugs:
            raise ValueError("Nenhuma UG encontrada na tabela 'ug'.")
        return ugs
    raise ValueError("UG_MODE invalido. Use 'single' ou 'all'.")


def table_exists_mysql(conn, table_name):
    q = text("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t LIMIT 1")
    return conn.execute(q, {"t": table_name}).fetchone() is not None


def parse_cod_contrato(series):
    """Extrai codContrato a partir do codClassificacao.
    O contrato eh o bloco de 8 digitos antes do sufixo '. 0'."""
    s = series.astype("string").fillna("").str.strip()
    contrato = s.str.extract(r"(\d{8})\s*\.\s*0\s*$", expand=False)
    contrato = contrato.fillna(s.str.extract(r"(\d{8})(?!.*\d{8})", expand=False))
    return contrato.fillna("0")


# =========================
# 6. DELETE POR ANO + UG
# =========================

def delete_year_data(conn, year):
    """Deleta registros do ano nas tabelas pd e pd_itens, filtrando por UG no modo single."""
    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"
    where_ug = f"AND codigoUG = '{UG_SINGLE}'" if UG_MODE == "single" else ""

    print(f"  Limpando dados de {year}...")
    deleted_total = 0

    # Deletar itens
    if table_exists_mysql(conn, TABELA_PD_ITENS):
        del_stmt = text(
            f"DELETE FROM {TABELA_PD_ITENS} "
            f"WHERE {DELETE_DATE_FIELD} >= :start AND {DELETE_DATE_FIELD} < :end {where_ug}"
        )
        r = conn.execute(del_stmt, {"start": start_date, "end": end_date})
        deleted_total += (r.rowcount or 0)

    # Deletar tabela principal
    if table_exists_mysql(conn, TABELA_PD):
        del_stmt = text(
            f"DELETE FROM {TABELA_PD} "
            f"WHERE {DELETE_DATE_FIELD} >= :start AND {DELETE_DATE_FIELD} < :end {where_ug}"
        )
        r = conn.execute(del_stmt, {"start": start_date, "end": end_date})
        deleted_total += (r.rowcount or 0)

    print(f"  Limpeza {year} concluida. {deleted_total} registros removidos.")
    return deleted_total


# =========================
# 7. EXPANSÃO ITENS (pd_itens)
# =========================

def expand_items(df_raw):
    """Expande classificadores dos itens para tabela pd_itens."""
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    rows = []
    for _, row in df_raw.iterrows():
        itens = row.get("itens", None)
        if not isinstance(itens, list) or not itens:
            continue

        pd_id = row.get("id", None)
        codigo_pd = row.get("codigo", None)
        codigo_ug = row.get("codigoUG", None)
        data_emissao = row.get("dataEmissao", None)

        for item in itens:
            if not isinstance(item, dict):
                continue
            classificadores = item.get("classificadores", None)
            if not isinstance(classificadores, list) or not classificadores:
                continue

            for c in classificadores:
                if not isinstance(c, dict):
                    continue
                rows.append({
                    "id": pd_id,
                    "codigo": codigo_pd,
                    "codigoUG": codigo_ug,
                    "dataEmissao": data_emissao,
                    "codigoTipoClassificador": c.get("codigoTipoClassificador", None),
                    "nomeTipoClassificador": c.get("nomeTipoClassificador", None),
                    "nomeClassificador": c.get("nomeClassificador", None),
                })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# =========================
# 8. FETCH DA API
# =========================

def fetch_data(session, ug, token, year):
    start_time = time.time()
    url = f"https://tesouro.sefaz.pi.gov.br/siafe-api/programacao-desembolso-orcamentaria/{year}/{ug}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = session.get(url, headers=headers, timeout=(5, 60), verify=False)
        elapsed = time.time() - start_time
        status = resp.status_code

        if status != 200:
            return ug, pd.DataFrame(), pd.DataFrame(), 0, elapsed, status

        try:
            data = resp.json()
        except ValueError:
            return ug, pd.DataFrame(), pd.DataFrame(), 0, elapsed, "json_error"

        if not data:
            return ug, pd.DataFrame(), pd.DataFrame(), 0, elapsed, "no_data"

        df_raw = pd.json_normalize(data)
        df_main = df_raw.reindex(columns=COLUMNS_PD)

        # Extrair codContrato do codClassificacao
        if "codClassificacao" in df_main.columns:
            df_main["codContrato"] = parse_cod_contrato(df_main["codClassificacao"])
        else:
            df_main["codContrato"] = "0"

        if "codigo" in df_main.columns:
            df_main = df_main.drop_duplicates(subset="codigo", keep="first")

        df_items = expand_items(df_raw)

        if "itens" in df_main.columns:
            df_main = df_main.drop(columns=["itens"], errors="ignore")

        qtd = len(df_main)
        return ug, df_main, df_items, qtd, elapsed, 200

    except requests.RequestException:
        elapsed = time.time() - start_time
        return ug, pd.DataFrame(), pd.DataFrame(), 0, elapsed, "request_error"


# =========================
# 9. MAIN
# =========================

def main():
    t0 = time.time()
    ugs = resolve_ugs()

    print("=" * 70)
    print(f"Atualizacao de PD (Programacao de Desembolso)")
    print(f"Anos: {YEARS}")
    print(f"UGs: {len(ugs)} ({'single' if UG_MODE == 'single' else 'all'})")
    print("=" * 70)

    session = make_session()

    for year in YEARS:
        print(f"\n{'=' * 70}")
        print(f"Processando ano {year}...")
        print(f"{'=' * 70}")

        dfs_main = []
        dfs_items = []
        total_docs = 0

        max_workers = min(MAX_WORKERS_CAP, max(1, len(ugs)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_data, session, ug, TOKEN, year) for ug in ugs]

            for fut in as_completed(futures):
                ug, df_main, df_items, qtd, elapsed, status = fut.result()
                total_docs += qtd

                if df_main is not None and not df_main.empty:
                    dfs_main.append(df_main)
                if df_items is not None and not df_items.empty:
                    dfs_items.append(df_items)

        if not dfs_main:
            print(f"  Nenhum dado retornado para {year}.")
            continue

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            final_main = pd.concat(dfs_main, ignore_index=True)
            final_items = pd.concat(dfs_items, ignore_index=True) if dfs_items else pd.DataFrame()

        # Conversoes tabela principal
        for col in INT_COLUMNS:
            if col in final_main.columns:
                final_main[col] = (
                    pd.to_numeric(final_main[col], errors="coerce")
                    .fillna(0)
                    .astype("int64")
                )

        if "valorTotalPD" in final_main.columns:
            final_main["valorTotalPD"] = pd.to_numeric(
                final_main["valorTotalPD"], errors="coerce"
            ).astype("float64")

        if "valor" in final_main.columns:
            final_main["valor"] = pd.to_numeric(
                final_main["valor"], errors="coerce"
            ).astype("float64")

        for col in DATE_COLUMNS:
            if col in final_main.columns:
                final_main[col] = pd.to_datetime(final_main[col], errors="coerce")

        # Conversoes tabela itens
        if final_items is not None and not final_items.empty:
            final_items["id"] = (
                pd.to_numeric(final_items["id"], errors="coerce")
                .fillna(0)
                .astype("int64")
            )
            dt = pd.to_datetime(final_items["dataEmissao"], errors="coerce")
            final_items["dataEmissao"] = dt.dt.strftime("%Y-%m-%d").fillna("")
            final_items = final_items.reindex(columns=[
                "id", "codigo", "codigoUG", "dataEmissao",
                "codigoTipoClassificador", "nomeTipoClassificador", "nomeClassificador",
            ])

        # Insert no banco
        try:
            with ENGINE.begin() as conn:
                delete_year_data(conn, year)

                final_main.to_sql(
                    name=TABELA_PD, con=conn, if_exists="append",
                    index=False, chunksize=CHUNKSIZE_SQL, method="multi",
                )

                if final_items is not None and not final_items.empty:
                    final_items.to_sql(
                        name=TABELA_PD_ITENS, con=conn, if_exists="append",
                        index=False, chunksize=CHUNKSIZE_SQL, method="multi",
                    )

            print(f"  Ano {year}: {len(final_main)} PDs inseridas")
            if final_items is not None and not final_items.empty:
                print(f"  Ano {year}: {len(final_items)} itens classificadores inseridos")

        except Exception as e:
            print(f"  ERRO ao inserir no banco (ano {year}): {e}")

    elapsed_total = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"Concluido em {elapsed_total:.2f}s ({elapsed_total / 60:.2f} min)")
    print(f"Tabelas '{TABELA_PD}' e '{TABELA_PD_ITENS}' atualizadas!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
