import time
import warnings
import requests
import pandas as pd
import os
import json
import sys
import math

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

# Suprime warnings de SSL (verify=False)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# 1. CARREGAR VARIÁVEIS DE AMBIENTE E CONEXÃO
# =============================================================================

# Tenta localizar o .env na raiz do projeto
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO CRÍTICO: Variáveis de banco de dados ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
ENGINE = create_engine(DATABASE_URI, echo=False)

# =============================================================================
# 2. CONFIGURAÇÕES ESPECÍFICAS
# =============================================================================

YEAR = datetime.now().year
tabela_destino = "liquidacao"  # Alterado conforme solicitado

# >>> MODO SINGLE TRAVADO <<<
UG_MODE = "single"
UG_SINGLE = "210101"
UG_PAD_SIZE = 6

MAX_WORKERS_CAP = 16
UPSERT_BATCH_SIZE = 500

# Colunas que serão gravadas na tabela
COLUMNS = [
    "id", "codigo", "codProcesso", "dataProcesso", "assuntoProcesso", "resumoProcesso",
    "anoProcesso", "statusDocumento", "codigoUG", "codFonte", "codNatureza",
    "codigoCredor", "nomeCredor", "dataEmissao", "dataCancelamento",
    "dataContabilizacao", "valor", "observacao", "codigoEmpenhoVinculado",
    "exercicioNE", "codigoEL", "exercicioEL", "codClassificacao", "tipoAlteracao",
    "codContrato",
]

INT_COLUMNS = ["anoProcesso", "codFonte", "codNatureza", "exercicioNE", "exercicioEL"]
DATE_COLUMNS = ["dataProcesso", "dataEmissao", "dataCancelamento", "dataContabilizacao"]

# =============================================================================
# 3. AUTENTICAÇÃO (SIAFE)
# =============================================================================
def get_token():
    siafe_user = os.getenv('SIAFE_USUARIO')
    siafe_pass = os.getenv('SIAFE_SENHA')

    if not siafe_user or not siafe_pass:
        print("ERRO: Variáveis SIAFE_USUARIO e SIAFE_SENHA não encontradas no .env")
        sys.exit(1)

    url = "https://tesouro.sefaz.pi.gov.br/siafe-api/auth"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"usuario": siafe_user, "senha": siafe_pass})
    
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=30, verify=False)
        if response.status_code == 200:
            return response.json()['token']
        else:
            raise Exception(f"Erro Auth SIAFE: {response.status_code}")
    except Exception as e:
        print(f"Erro ao obter token: {e}")
        sys.exit(1)

print("Autenticando no SIAFE...")
TOKEN = get_token()

# =============================================================================
# 4. HELPERS E SESSÃO
# =============================================================================

def make_session():
    session = requests.Session()
    session.verify = False  # SIAFE API pode ter certificado self-signed
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
    if value is None: return None
    s = str(value).strip()
    if s.endswith(".0"): s = s[:-2]
    digits = [c for c in s if c.isdigit()]
    s = "".join(digits)
    return s.zfill(size) if s else None

def resolve_ugs():
    if UG_MODE == "single":
        ug = normalize_ug(UG_SINGLE, UG_PAD_SIZE)
        return [ug] if ug else []
    
    # Fallback
    with ENGINE.connect() as conn:
        rows = conn.execute(text("SELECT codigo FROM ug")).fetchall()
    return [normalize_ug(r[0], UG_PAD_SIZE) for r in rows if r[0]]

def _to_py(value):
    """Converte tipos do pandas/numpy para tipos nativos do Python aceitos pelo DBAPI."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def upsert_dataframe(conn, df, table_name, columns, key_columns):
    """INSERT ... ON DUPLICATE KEY UPDATE para idempotência via PK composta."""
    if df is None or df.empty:
        print("Nenhum registro para upsert.")
        return 0

    cols_sql = ", ".join("`{}`".format(c) for c in columns)
    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
    update_cols = [c for c in columns if c not in key_columns]
    update_clause = ", ".join(
        "`{c}` = VALUES(`{c}`)".format(c=c) for c in update_cols
    )

    total_rows = len(df)
    processed = 0
    raw = conn.connection
    cursor = raw.cursor()

    try:
        for start in range(0, total_rows, UPSERT_BATCH_SIZE):
            batch = df.iloc[start:start + UPSERT_BATCH_SIZE]
            rows = [
                tuple(_to_py(v) for v in row)
                for row in batch[columns].itertuples(index=False, name=None)
            ]
            values_sql = ", ".join([placeholders] * len(rows))
            flat_params = [v for row in rows for v in row]

            sql = (
                "INSERT INTO `{table}` ({cols}) VALUES {vals} "
                "ON DUPLICATE KEY UPDATE {upd}"
            ).format(
                table=table_name,
                cols=cols_sql,
                vals=values_sql,
                upd=update_clause,
            )
            cursor.execute(sql, flat_params)
            processed += len(rows)
    finally:
        cursor.close()

    print("UPSERT concluído: {} linhas processadas em '{}'.".format(processed, table_name))
    return processed

# =============================================================================
# 5. PARSE DE CLASSIFICADORES (Extração de Contrato e Competência)
# =============================================================================

def _get_classificador_nome(classificadores, codigo_tipo):
    if not isinstance(classificadores, list): return ""
    for item in classificadores:
        if isinstance(item, dict) and item.get("codigoTipoClassificador") == codigo_tipo:
            val = item.get("nomeClassificador")
            return "" if val is None else str(val).strip()
    return ""

def build_cod_contrato_from_classificadores(classificadores):
    # Contrato = 54
    return _get_classificador_nome(classificadores, 54)

def build_competencia_from_classificadores(classificadores):
    # Ano = 81, Mês = 502
    ano = _get_classificador_nome(classificadores, 81)
    mes = _get_classificador_nome(classificadores, 502)
    if not mes or not ano: return ""
    # Mês com 2 dígitos
    return f"{mes.zfill(2)}/{ano}"

# =============================================================================
# 7. API FETCH
# =============================================================================

def fetch_data(session, ug, token, year):
    start_time = time.time()
    url = f"https://tesouro.sefaz.pi.gov.br/siafe-api/nota-liquidacao/{year}/{ug}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = session.get(url, headers=headers, timeout=(5, 30))
        elapsed = time.time() - start_time
        
        if response.status_code != 200:
            return ug, pd.DataFrame(), 0, elapsed, response.status_code

        data = response.json()
        if not data:
            return ug, pd.DataFrame(), 0, elapsed, "no_data"

        # Normaliza sem reindex para não perder a coluna 'classificadores'
        df_full = pd.json_normalize(data)

        if "classificadores" not in df_full.columns:
            df_full["classificadores"] = None

        # Lógica de extração de colunas personalizadas
        df_full["codContrato"] = df_full["classificadores"].apply(build_cod_contrato_from_classificadores)
        df_full["competencia"] = df_full["classificadores"].apply(build_competencia_from_classificadores)

        # Seleciona apenas colunas finais
        for col in COLUMNS:
            if col not in df_full.columns: df_full[col] = None

        df = df_full[COLUMNS].copy()
        df = df.drop_duplicates(subset="codigo", keep="first")

        return ug, df, len(df), elapsed, 200

    except Exception as e:
        elapsed = time.time() - start_time
        return ug, pd.DataFrame(), 0, elapsed, "error"

# =============================================================================
# 8. MAIN
# =============================================================================

def main():
    t0 = time.time()
    ugs = resolve_ugs()
    
    print(f"Iniciando LIQUIDAÇÃO - Ano {YEAR} - Modo {UG_MODE}")

    session = make_session()
    dfs = []
    
    # Execução (com ThreadPool mesmo sendo 1 UG, mantém padrão)
    with ThreadPoolExecutor(max_workers=min(16, len(ugs))) as executor:
        futures = [executor.submit(fetch_data, session, ug, TOKEN, YEAR) for ug in ugs]
        for fut in as_completed(futures):
            ug, df, qtd, _, status = fut.result()
            print(f"UG {ug}: Status {status}, Registros {qtd}")
            if not df.empty:
                dfs.append(df)

    if not dfs:
        print("Nenhum dado retornado.")
        return

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        final_df = pd.concat(dfs, ignore_index=True)

    # Conversão de Tipos
    for col in INT_COLUMNS:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce").fillna(0).astype("int64")

    for col in DATE_COLUMNS:
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors="coerce")

    # Filtro defensivo: garante que só registros do ano-alvo sejam gravados.
    # Protege contra a API retornar NLs com dataEmissao fora de YEAR. Anos
    # anteriores ficam intactos no banco.
    if "dataEmissao" in final_df.columns:
        n_total = len(final_df)
        final_df = final_df[final_df["dataEmissao"].dt.year == YEAR].copy()
        n_fora = n_total - len(final_df)
        if n_fora > 0:
            print(f"[FILTRO ANO] {n_fora} linha(s) com dataEmissao fora de {YEAR} ignorada(s).")

    # Inserção no Banco (UPSERT via PK composta codigoUG+codigo — idempotente)
    try:
        with ENGINE.begin() as conn:
            print(f"Upsert de {len(final_df)} registros na tabela {tabela_destino}...")
            upsert_dataframe(
                conn,
                final_df,
                tabela_destino,
                COLUMNS,
                key_columns=("codigoUG", "codigo"),
            )

        elapsed_total = time.time() - t0
        print(f"SUCESSO TOTAL! Tempo: {elapsed_total:.2f}s")

    except Exception as e:
        print(f"ERRO FATAL: {e}")

if __name__ == "__main__":
    main()