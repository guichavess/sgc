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
    print(f"AVISO: Arquivo .env não encontrado ou variáveis vazias. Buscado em: {dotenv_path}")

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO CRÍTICO: Variáveis de banco de dados ausentes no .env")
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
        print("ERRO: Variáveis SIAFE_USUARIO e SIAFE_SENHA não encontradas no .env")
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
                print("ERRO: Não foi possível autenticar após todas as tentativas.")
                sys.exit(1)


print("Autenticando no SIAFE...")
TOKEN = get_token()

# =========================
# 3. CONFIGURAÇÕES
# =========================

tabela_destino = "reserva"
YEAR = 2026


# >>> controle fácil do DELETE por ano em campo de data <<<
DELETE_YEAR = YEAR
DELETE_DATE_FIELD = "dataEmissao"

# Escolha do modo:
# - "single"  -> usa só a UG definida em UG_SINGLE
# - "all"     -> busca todas as UGs do banco (SELECT codigo FROM ug)
UG_MODE = "single"
UG_SINGLE = "210101"

UG_PAD_SIZE = 6

MAX_WORKERS_CAP = 16
CHUNKSIZE_SQL = 2000

# Diagnóstico
DEBUG_LOG = True
DEBUG_MAX_EXAMPLES = 20

COLUMNS = [
    "id",
    "codigo",
    "codigoDocAlterado",
    "codProcesso",
    "dataProcesso",
    "assuntoProcesso",
    "resumoProcesso",
    "anoProcesso",
    "statusDocumento",
    "codigoUG",
    "ordenadoresDespesa",
    "codFonte",
    "codNatureza",
    "codClassificacao",
    "codigoCredor",
    "nomeCredor",
    "valor",
    "observacao",
    "tipoAlteracao",
    "dataEmissao",
    "codigoEmpenhoVinculado",
    "tipoReserva",
    # >>> NOVO CAMPO POPULADO VIA CLASSIFICADORES
    "codContrato",
]

INT_COLUMNS = ["anoProcesso", "codFonte", "codNatureza", "codigoEmpenhoVinculado"]
DATE_COLUMNS = ["dataProcesso", "dataEmissao"]

# =========================
# HTTP SESSION ROBUSTA
# =========================

def make_session():
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=20,
        pool_maxsize=20
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# =========================
# BANCO DE DADOS
# =========================

def table_exists_mysql(conn, table_name):
    query = text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = :table
        LIMIT 1
    """)
    return conn.execute(query, {"table": table_name}).fetchone() is not None


def delete_year_data(conn):
    if not table_exists_mysql(conn, tabela_destino):
        print("Tabela não existe ainda, será criada.")
        return 0

    start_date = "{}-01-01".format(DELETE_YEAR)
    end_date = "{}-01-01".format(DELETE_YEAR + 1)

    delete_query = text("""
        DELETE FROM {table}
        WHERE {date_field} >= :start_date
          AND {date_field} <  :end_date
    """.format(table=tabela_destino, date_field=DELETE_DATE_FIELD))

    result = conn.execute(delete_query, {"start_date": start_date, "end_date": end_date})
    rows_deleted = result.rowcount or 0
    print("{} registros de {} deletados com sucesso!".format(rows_deleted, DELETE_YEAR))
    return rows_deleted


def normalize_ug(value, size):
    if value is None:
        return None

    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]

    digits = []
    for c in s:
        if c.isdigit():
            digits.append(c)
    s = "".join(digits)

    if not s:
        return None

    return s.zfill(size)


def load_ugs_from_db():
    query = text("SELECT codigo FROM ug")
    with ENGINE.connect() as conn:
        rows = conn.execute(query).fetchall()

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
            raise ValueError("UG_SINGLE está vazio/inválido, mas UG_MODE='single'.")
        return [ug]

    if mode == "all":
        ugs = load_ugs_from_db()
        if not ugs:
            raise ValueError("Nenhuma UG encontrada na tabela 'ug'.")
        return ugs

    raise ValueError("UG_MODE inválido. Use 'single' ou 'all'.")

# =========================
# UTILITÁRIOS (CLASSIFICADORES)
# =========================

def _get_classificador_nome(classificadores, codigo_tipo):
    if not isinstance(classificadores, list):
        return ""
    for item in classificadores:
        if isinstance(item, dict) and item.get("codigoTipoClassificador") == codigo_tipo:
            val = item.get("nomeClassificador")
            return "" if val is None else str(val).strip()
    return ""


def build_cod_contrato_from_classificadores(classificadores):
    return _get_classificador_nome(classificadores, 54)

# =========================
# DIAGNÓSTICO (LOG)
# =========================

def log_diagnostico_reserva(df, max_examples=20):
    if df is None or df.empty:
        print("[DIAG] DataFrame final está vazio.")
        return

    cod = df["codigo"].astype("string") if "codigo" in df.columns else pd.Series([], dtype="string")
    contrato = df["codContrato"].astype("string") if "codContrato" in df.columns else pd.Series([], dtype="string")

    contrato = contrato.fillna("").str.strip()
    empty_contrato = contrato.eq("").sum()

    print("=" * 70)
    print("[DIAG] Qualidade do campo codContrato (RESERVA)")
    print("=" * 70)
    print(f"[DIAG] Total de linhas: {len(df)}")
    print(f"[DIAG] Linhas com codContrato vazio: {empty_contrato}")

    if empty_contrato > 0:
        exemplos = cod[contrato.eq("")].dropna().head(max_examples).tolist()
        print(f"[DIAG] Exemplos de 'codigo' com codContrato vazio (até {max_examples}): {exemplos}")

    print("=" * 70)

# =========================
# API
# =========================

def fetch_data(session, ug, token, year):
    start_time = time.time()
    url = "https://tesouro.sefaz.pi.gov.br/siafe-api/nota-reserva/{}/{}".format(year, ug)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = session.get(url, headers=headers, timeout=(5, 30), verify=False)
        elapsed = time.time() - start_time
        status = response.status_code

        if status != 200:
            return ug, pd.DataFrame(), 0, elapsed, status

        try:
            data = response.json()
        except ValueError:
            return ug, pd.DataFrame(), 0, elapsed, "json_error"

        if not data:
            return ug, pd.DataFrame(), 0, elapsed, "no_data"

        df_full = pd.json_normalize(data)

        if "classificadores" not in df_full.columns:
            df_full["classificadores"] = None

        df_full["codContrato"] = df_full["classificadores"].apply(build_cod_contrato_from_classificadores)

        for col in COLUMNS:
            if col not in df_full.columns:
                df_full[col] = None

        df = df_full[COLUMNS].copy()

        df = df.drop_duplicates(subset="codigo", keep="first")

        return ug, df, len(df), elapsed, 200

    except requests.RequestException:
        elapsed = time.time() - start_time
        return ug, pd.DataFrame(), 0, elapsed, "request_error"

# =========================
# MAIN
# =========================

def main_reserva():
    t0 = time.time()

    ugs = resolve_ugs()
    is_all_mode = (UG_MODE or "").lower().strip() == "all"

    print("=" * 70)
    print("Iniciando atualização de dados de RESERVA - Ano {}".format(YEAR))
    if is_all_mode:
        print("UGs: {} UGs (modo all)".format(len(ugs)))
    else:
        print("UG: {}".format(", ".join(ugs)))
    print("=" * 70)

    print("Buscando dados da API...")

    session = make_session()

    dfs = []
    total_docs = 0

    total_api_time_sum = 0.0
    per_ug_summary = []
    status_counts = {}

    max_workers = min(MAX_WORKERS_CAP, max(1, len(ugs)))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_data, session, ug, TOKEN, YEAR) for ug in ugs]

        for future in as_completed(futures):
            ug, df, qtd, elapsed, status = future.result()

            total_docs += qtd
            total_api_time_sum += elapsed

            status_counts[status] = status_counts.get(status, 0) + 1
            per_ug_summary.append((ug, qtd, elapsed))

            if df is not None and not df.empty:
                dfs.append(df)

    if not dfs:
        print("Nenhum dado foi retornado da API.")
        print("Resumo de status/erros da API: {}".format(status_counts))
        print("Exemplo de UGs usadas: {}".format(ugs[:10]))
        return

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        final_df = pd.concat(dfs, ignore_index=True)

    if DEBUG_LOG:
        log_diagnostico_reserva(final_df, max_examples=DEBUG_MAX_EXAMPLES)

    for col in INT_COLUMNS:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce").fillna(0).astype("int64")

    for col in DATE_COLUMNS:
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors="coerce")

    try:
        with ENGINE.begin() as conn:
            delete_year_data(conn)

            final_df.to_sql(
                name=tabela_destino,
                con=conn,
                if_exists="append",
                index=False,
                chunksize=CHUNKSIZE_SQL,
                method="multi",
            )

        elapsed_total = time.time() - t0

        print("=" * 70)
        print("RESUMO DA OPERAÇÃO")
        print("=" * 70)

        if is_all_mode:
            per_ug_summary.sort(key=lambda x: x[0])
            print("UG | Quantidade de NR | Tempo de busca (s)")
            for ug, qtd, elapsed in per_ug_summary:
                print("{} | {} | {:.2f}".format(ug, qtd, elapsed))

            print("-" * 70)
            print("UGs buscadas: {}".format(len(per_ug_summary)))
            print("Total de NR: {}".format(total_docs))
            print("Tempo total do procedimento: {:.2f} min".format(elapsed_total / 60.0))
        else:
            print("Total de documentos processados: {}".format(total_docs))
            print("Tempo total do procedimento: {:.2f}s".format(elapsed_total))

        print("Registros inseridos no banco: {}".format(len(final_df)))
        print("Tabela '{}' atualizada com sucesso!".format(tabela_destino))
        print("=" * 70)

    except Exception as e:
        print("Erro ao inserir no banco (rollback automático): {}".format(e))


if __name__ == "__main__":
    main_reserva()
