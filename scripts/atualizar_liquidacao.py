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

YEAR = 2026
tabela_destino = "liquidacao"  # Alterado conforme solicitado

DELETE_YEAR = YEAR
DELETE_DATE_FIELD = "dataEmissao"

# >>> MODO SINGLE TRAVADO <<<
UG_MODE = "single"
UG_SINGLE = "210101"
UG_PAD_SIZE = 6

MAX_WORKERS_CAP = 16
CHUNKSIZE_SQL = 100

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

def table_exists_mysql(conn, table_name):
    q = text("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t LIMIT 1")
    return conn.execute(q, {"t": table_name}).fetchone() is not None

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
# 6. FUNÇÕES DE BANCO (DELETE)
# =============================================================================

def delete_year_data(conn):
    if not table_exists_mysql(conn, tabela_destino):
        print(f"Tabela {tabela_destino} não existe ainda, será criada automaticamente.")
        return 0

    start_date = f"{DELETE_YEAR}-01-01"
    end_date = f"{DELETE_YEAR + 1}-01-01"

    print(f"Limpando dados de {DELETE_YEAR}...")

    # Se estiver em modo single, deleta APENAS dados dessa UG para esse ano
    if UG_MODE == "single":
        delete_query = text(f"""
            DELETE FROM {tabela_destino}
            WHERE {DELETE_DATE_FIELD} >= :start_date
              AND {DELETE_DATE_FIELD} <  :end_date
              AND codigoUG = :ug
        """)
        result = conn.execute(delete_query, {
            "start_date": start_date, 
            "end_date": end_date,
            "ug": UG_SINGLE
        })
    else:
        # Modo legacy (all) - Cuidado: apaga tudo do ano
        delete_query = text(f"""
            DELETE FROM {tabela_destino}
            WHERE {DELETE_DATE_FIELD} >= :start_date
              AND {DELETE_DATE_FIELD} <  :end_date
        """)
        result = conn.execute(delete_query, {"start_date": start_date, "end_date": end_date})

    rows_deleted = result.rowcount or 0
    print(f"{rows_deleted} registros removidos.")
    return rows_deleted

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

    # Inserção no Banco
    try:
        with ENGINE.begin() as conn:
            delete_year_data(conn)
            
            print(f"Inserindo {len(final_df)} registros na tabela {tabela_destino}...")
            final_df.to_sql(
                name=tabela_destino,
                con=conn,
                if_exists="append",
                index=False,
                chunksize=CHUNKSIZE_SQL,
                method="multi",
            )

        print(f"SUCESSO TOTAL! Tempo: {time.time() - t0:.2f}s")

    except Exception as e:
        print(f"ERRO FATAL: {e}")

if __name__ == "__main__":
    main()