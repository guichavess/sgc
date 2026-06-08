"""
Script de importação de Ordens Bancárias (OB/Pagamentos) do SIAFE.
Busca dados da API de ordem-bancaria-orcamentaria e grava na tabela 'ob'.
"""
import time
import warnings
import requests
import pandas as pd
import os
import json
import sys

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

# Permite importar app.utils.competencia quando rodado como script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.utils.competencia import normalizar_competencia  # noqa: E402
from app.utils.siafe import normalizar_codigo_fonte  # noqa: E402

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

# Anos a processar — inclui ano anterior para refrescar status de documentos
# que mudaram após a virada de ano (ex.: OB de 2025 contabilizada em 2026).
YEARS = [datetime.now().year - 1, datetime.now().year]
tabela_destino = "ob"

DELETE_DATE_FIELD = "dataEmissao"

UG_MODE = "list"        # "single", "list" ou "all"
UG_SINGLE = "210101"    # usado apenas se UG_MODE="single"
UG_LIST = ["210101", "210102"]  # usado se UG_MODE="list"
UG_PAD_SIZE = 6

MAX_WORKERS_CAP = 16
CHUNKSIZE_SQL = 100

# Colunas que serão gravadas na tabela
COLUMNS = [
    "id",
    "codigo",
    "codProcesso",
    "assuntoProcesso",
    "anoProcesso",
    "statusDocumento",
    "codigoUG",
    "nomeUG",
    "codigoGestao",
    "codFonte",
    "codNatureza",
    "codigoCredor",
    "nomeCredor",
    "dataEmissao",
    "dataContabilizacao",
    "valor",
    "observacao",
    "competencia",
    "objectType",
    "envioStatus",
    "codigoRegistroEnvio",
    "ugEmitente",
    "dataPagamento",
    "codigoUGPagadora",
    "codigoUGEmpenho",
    "tipoOB",
    "idNE",
    "codigoNE",
    "exercicioNE",
    "idNL",
    "codigoNL",
    "exercicioNL",
    "codigoPDO",
    "codClassificacao",
    "codContrato",
]

INT_COLUMNS = [
    "anoProcesso",
    "codigoGestao",
    "codFonte",
    "codNatureza",
    "ugEmitente",
    "codigoUGPagadora",
    "codigoUGEmpenho",
    "idNE",
    "exercicioNE",
    "idNL",
    "exercicioNL",
]

DATE_COLUMNS = ["dataEmissao", "dataContabilizacao", "dataPagamento"]

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
    mode = (UG_MODE or "").lower().strip()
    if mode == "single":
        ug = normalize_ug(UG_SINGLE, UG_PAD_SIZE)
        return [ug] if ug else []
    if mode == "list":
        ugs = [normalize_ug(u, UG_PAD_SIZE) for u in UG_LIST]
        return [u for u in ugs if u]
    # Fallback: all — busca todas as UGs do banco
    with ENGINE.connect() as conn:
        rows = conn.execute(text("SELECT codigo FROM ug")).fetchall()
    ugs = []
    seen = set()
    for r in rows:
        if not r: continue
        ug = normalize_ug(r[0], UG_PAD_SIZE)
        if ug and ug not in seen:
            seen.add(ug)
            ugs.append(ug)
    return ugs

def table_exists_mysql(conn, table_name):
    q = text("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t LIMIT 1")
    return conn.execute(q, {"t": table_name}).fetchone() is not None

def year_date_range(year):
    """Retorna faixa de datas para a URL da API."""
    return f"{year}-01-01/{year}-12-31"

# =============================================================================
# 5. PARSE DE CLASSIFICADORES (Extração de Contrato)
# =============================================================================

def parse_cod_contrato_from_codClassificacao(series: pd.Series) -> pd.Series:
    """
    Extrai o código do contrato do campo codClassificacao.

    Padrão observado:
      ...<muitos espaços> 25014874.         0
      ...<muitos espaços> 00000000.         0

    Regra: capturar o último grupo de 8 dígitos imediatamente antes de um '.' e do final "0".
    """
    s = series.astype("string").fillna("")
    # captura: 8 dígitos + '.' + espaços + (um ou mais dígitos) até o fim
    out = s.str.extract(r"(\d{8})\.\s*\d+\s*$", expand=False)
    return out.fillna("").astype("string")


# =============================================================================
# 6. FUNÇÕES DE BANCO (DELETE)
# =============================================================================

def delete_year_data(conn, year):
    if not table_exists_mysql(conn, tabela_destino):
        print(f"Tabela {tabela_destino} não existe ainda, será criada automaticamente.")
        return 0

    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"

    print(f"Limpando dados de {year}...")

    mode = (UG_MODE or "").lower().strip()
    if mode == "single":
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
    elif mode == "list":
        placeholders = ", ".join(f"'{u}'" for u in UG_LIST)
        delete_query = text(f"""
            DELETE FROM {tabela_destino}
            WHERE {DELETE_DATE_FIELD} >= :start_date
              AND {DELETE_DATE_FIELD} <  :end_date
              AND codigoUG IN ({placeholders})
        """)
        result = conn.execute(delete_query, {"start_date": start_date, "end_date": end_date})
    else:
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

    data_range = year_date_range(year)
    url = f"https://tesouro.sefaz.pi.gov.br/siafe-api/ordem-bancaria-orcamentaria/{year}/{ug}/{data_range}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = session.get(url, headers=headers, timeout=(5, 60))
        elapsed = time.time() - start_time

        if response.status_code != 200:
            return ug, pd.DataFrame(), 0, elapsed, response.status_code

        try:
            data = response.json()
        except ValueError:
            return ug, pd.DataFrame(), 0, elapsed, "json_error"

        if not data:
            return ug, pd.DataFrame(), 0, elapsed, "no_data"

        df_raw = pd.json_normalize(data)

        # Ajusta colunas principais
        df = df_raw.reindex(columns=COLUMNS)

        if "codFonte" in df.columns:
            df["codFonte"] = df["codFonte"].apply(normalizar_codigo_fonte)

        # codContrato extraído de codClassificacao
        if "codClassificacao" in df.columns:
            df["codContrato"] = parse_cod_contrato_from_codClassificacao(df["codClassificacao"])
        else:
            df["codContrato"] = ""

        # Competência: a API SIAFE de OB devolve direto (texto descritivo
        # como '12 - Dezembro/2024'). Normaliza pra MM/YYYY pra ficar
        # consistente com PD/liquidação/empenho.
        if "competencia" in df.columns:
            df["competencia"] = df["competencia"].apply(normalizar_competencia)

        # Dedup
        if "codigo" in df.columns:
            df = df.drop_duplicates(subset="codigo", keep="first")

        return ug, df, len(df), elapsed, 200

    except requests.RequestException:
        elapsed = time.time() - start_time
        return ug, pd.DataFrame(), 0, elapsed, "request_error"

# =============================================================================
# 8. MAIN
# =============================================================================

def main():
    t0 = time.time()
    ugs = resolve_ugs()
    is_all_mode = (UG_MODE or "").lower().strip() == "all"

    print("Autenticando no SIAFE...")
    token = get_token()

    print("=" * 70)
    print(f"Iniciando atualização de dados de OB (Pagamento) - Anos {YEARS}")
    if is_all_mode:
        print(f"UGs da SEAD: {len(ugs)} UGs (modo all)")
    else:
        print(f"UGs da SEAD: {', '.join(ugs)}")
    print("=" * 70)

    session = make_session()
    max_workers = min(MAX_WORKERS_CAP, max(1, len(ugs)))

    for year in YEARS:
        print(f"\n{'=' * 70}")
        print(f"Processando ano {year}...")
        print(f"{'=' * 70}")

        dfs = []
        total_docs = 0
        total_api_time_sum = 0.0
        per_ug_summary = []
        status_counts = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_data, session, ug, token, year) for ug in ugs]
            for fut in as_completed(futures):
                ug, df, qtd, elapsed, status = fut.result()
                total_docs += qtd
                total_api_time_sum += elapsed
                status_counts[status] = status_counts.get(status, 0) + 1
                per_ug_summary.append((ug, qtd, elapsed))
                print(f"UG {ug}: Status {status}, Registros {qtd}")
                if df is not None and not df.empty:
                    dfs.append(df)

        if not dfs:
            print(f"Nenhum dado retornado da API para {year}.")
            print(f"Resumo de status/erros: {status_counts}")
            continue

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

        if "competencia" in final_df.columns:
            final_df["competencia"] = final_df["competencia"].astype("string")

        # codContrato permanece como string para não perder zeros à esquerda

        # Filtro defensivo: garante que só OBs do ano-alvo sejam gravadas.
        # Anos NÃO incluídos em YEARS ficam intactos no banco (DELETE escopado por year).
        if "dataEmissao" in final_df.columns:
            n_total = len(final_df)
            final_df = final_df[final_df["dataEmissao"].dt.year == year].copy()
            n_fora = n_total - len(final_df)
            if n_fora > 0:
                print(f"[FILTRO ANO] {n_fora} linha(s) com dataEmissao fora de {year} ignorada(s).")

        # Inserção no Banco
        try:
            with ENGINE.begin() as conn:
                delete_year_data(conn, year)

                print(f"Inserindo {len(final_df)} registros na tabela {tabela_destino}...")
                final_df.to_sql(
                    name=tabela_destino,
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=CHUNKSIZE_SQL,
                    method="multi",
                )

            print(f"Ano {year}: {len(final_df)} OBs inseridas.")

        except Exception as e:
            print(f"ERRO ao inserir no banco (ano {year}): {e}")

    elapsed_total = time.time() - t0
    print("=" * 70)
    print(f"Concluído em {elapsed_total:.2f}s ({elapsed_total / 60:.2f} min)")
    print(f"Tabela '{tabela_destino}' atualizada para anos {YEARS}.")
    print("=" * 70)

if __name__ == "__main__":
    main()
