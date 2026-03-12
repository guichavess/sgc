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
from datetime import datetime

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

# >>> nome fixo conforme solicitado <<<
tabela_destino = "loa"

# Anos a buscar (default ou via CLI: --years 2026)
YEARS = [2023, 2024, 2025, 2026]
if '--years' in sys.argv:
    idx = sys.argv.index('--years')
    YEARS = [int(y) for y in sys.argv[idx+1:] if y.isdigit()]

# Escolha do modo:
# - "single"  -> usa só a UG definida em UG_SINGLE
# - "all"     -> busca todas as UGs do banco (SELECT codigo FROM ug)
UG_MODE = "single"
UG_SINGLE = "210101"
UG_PAD_SIZE = 6

# Performance
MAX_WORKERS_CAP = 16
CHUNKSIZE_SQL = 2000

# Log
DEBUG_LOG = True

# Contas LOA
IDS = [
    "522110101", "522120101", "522120201", "522120202", "522120203",
    "522120301", "522120302", "522120303", "522190401", "622120106", "622110101"
]

ID_DESCRIPTIONS = {
    "522110101": "CRÉDITO INICIAL (Congelado)",
    "522120101": "CREDITO SUPLEMENTAR",
    "522120201": "CRÉDITOS ESPECIAIS ABERTOS",
    "522120202": "CRÉDITOS ESPECIAIS REABERTOS",
    "522120203": "CRÉDITOS ESPECIAIS REABERTOS - SUPLEMENTAÇÃO",
    "522120301": "CRÉDITOS EXTRAORDINÁRIOS ABERTOS",
    "522120302": "CRÉDITOS EXTRAORDINÁRIOS REABERTOS",
    "522120303": "CRÉDITOS EXTRAORDINÁRIOS REABERTOS - SUPLEMENTAÇÃO",
    "522190401": "CANCELAMENTO DE DOTACOES",
    "622120106": "BLOQUEIO SOLICITAÇÃO DE CREDITO",
    "622110101": "Credito Disponivel",
}

# Colunas alvo (V2: inclui codAcao, codNatureza, codFonte, codPrograma)
COLUMNS = [
    "codigoUG",
    "saldo",
    "classificacaoStr",
    "contaCorrente",
    "saldoAnterior",
    "valorCredito",
    "valorDebito",
    "mes",
    "ano",
    "id",
    "descricao",
    "codAcao",
    "codNatureza",
    "codFonte",
    "codPrograma",
]

# =========================
# HELPERS DE CLASSIFICAÇÃO
# =========================

def _only_digits(value):
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits or None


def _extract_mid_digits(source_value, start, length):
    """
    Equivalente ao Mid do SQL/VB, usando base 1.
    Depois remove qualquer caractere que não seja dígito.
    """
    if source_value is None:
        return None
    s = str(source_value)
    begin = max(start - 1, 0)
    end = begin + length
    return _only_digits(s[begin:end])


def extract_classificacao_campos(classificacao_str, conta_corrente=None):
    """
    Extrai codAcao, codNatureza, codFonte e codPrograma a partir de classificacaoStr.

    Referências (posições Mid base 1):
    - Mid(contacorrente, 11, 16) -> codPrograma
    - Mid(contacorrente, 23, 4)  -> codAcao
    - Mid(contacorrente, 30, 4)  -> codFonte
    - Mid(contacorrente, 35, 9)  -> codNatureza
    """
    source = classificacao_str if classificacao_str not in (None, "") else conta_corrente

    result = {
        "codAcao": _extract_mid_digits(source, 23, 4),
        "codNatureza": _extract_mid_digits(source, 35, 9),
        "codFonte": _extract_mid_digits(source, 30, 4),
        "codPrograma": _extract_mid_digits(source, 11, 16),
    }

    # fallback: quando o layout vier com espaços irregulares, tenta usar a estrutura
    # lógica observada no JSON da API.
    if not all(result.values()):
        fallback_source = classificacao_str or conta_corrente
        if fallback_source:
            parts = [p.strip() for p in str(fallback_source).split(".") if p.strip()]

            # Exemplo observado:
            # 21.101. 1.04.122.0109.2000.1.5.00.3.3.90.33. 0.0000. E0000. TD0.000001
            # índices úteis:
            # [0]=21 [1]=101 [2]=1 [3]=04 [4]=122 [5]=0109 [6]=2000 [7]=1 [8]=5 [9]=00
            # [10]=3 [11]=3 [12]=90 [13]=33
            try:
                if not result["codPrograma"] and len(parts) >= 7:
                    result["codPrograma"] = _only_digits("".join(parts[2:7]))

                if not result["codAcao"] and len(parts) >= 7:
                    result["codAcao"] = _only_digits(parts[6])

                if not result["codFonte"] and len(parts) >= 10:
                    result["codFonte"] = _only_digits("".join(parts[8:10]))

                if not result["codNatureza"] and len(parts) >= 14:
                    result["codNatureza"] = _only_digits("".join(parts[10:14]))
            except Exception:
                pass

    return result


def enrich_classificacao_columns(df):
    """
    Povoa codAcao, codNatureza, codFonte e codPrograma no DataFrame.
    """
    if df.empty:
        for col in ["codAcao", "codNatureza", "codFonte", "codPrograma"]:
            if col not in df.columns:
                df[col] = None
        return df

    def _parse_row(row):
        classificacao_str = row.get("classificacaoStr")
        conta_corrente = row.get("contaCorrente")
        return extract_classificacao_campos(classificacao_str, conta_corrente)

    extracted = df.apply(_parse_row, axis=1, result_type="expand")

    for col in ["codAcao", "codNatureza", "codFonte", "codPrograma"]:
        df[col] = extracted.get(col)

    return df


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

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
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


def drop_table_if_exists(conn, table_name):
    """Exclui a tabela se existir (DROP TABLE)."""
    if table_exists_mysql(conn, table_name):
        conn.execute(text(f"DROP TABLE `{table_name}`"))
        print(f"Tabela '{table_name}' excluída com sucesso (DROP TABLE).")
    else:
        print(f"Tabela '{table_name}' não existe, será criada do zero.")


def delete_year_data(conn, year):
    if not table_exists_mysql(conn, tabela_destino):
        print("  Tabela não existe ainda, será criada.")
        return 0

    delete_query = text(f"DELETE FROM {tabela_destino} WHERE ano = :year")
    result = conn.execute(delete_query, {"year": int(year)})
    rows_deleted = result.rowcount or 0
    print(f"  {rows_deleted} registros de {year} deletados com sucesso!")
    return rows_deleted


def normalize_ug(value, size):
    if value is None:
        return None

    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]

    digits = [c for c in s if c.isdigit()]
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
# API
# =========================

def should_skip_month(year, month):
    now = datetime.now()
    if int(year) == int(now.year) and int(month) > int(now.month):
        return True
    return False


def fetch_data(session, cod_conta, ug, year, month, token):
    t0 = time.time()

    url = f"https://tesouro.sefaz.pi.gov.br/siafe-api/saldo-contabil/{year}/{month}/{cod_conta}/{ug}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = session.get(url, headers=headers, timeout=(5, 30), verify=False)
        elapsed = time.time() - t0
        status = resp.status_code

        if status != 200:
            return (ug, cod_conta, month), pd.DataFrame(columns=COLUMNS), 0, elapsed, status

        try:
            data = resp.json()
        except ValueError:
            return (ug, cod_conta, month), pd.DataFrame(columns=COLUMNS), 0, elapsed, "json_error"

        if not data:
            return (ug, cod_conta, month), pd.DataFrame(columns=COLUMNS), 0, elapsed, "no_data"

        df_full = pd.json_normalize(data)

        # garante colunas mínimas
        for col in ["mes", "ano"]:
            if col not in df_full.columns:
                df_full[col] = int(month) if col == "mes" else int(year)

        df_full["id"] = cod_conta
        df_full["descricao"] = ID_DESCRIPTIONS.get(cod_conta, "")

        # garante colunas existentes antes do enriquecimento
        for col in [
            "codigoUG", "saldo", "classificacaoStr", "contaCorrente",
            "saldoAnterior", "valorCredito", "valorDebito",
            "mes", "ano", "id", "descricao",
        ]:
            if col not in df_full.columns:
                df_full[col] = None

        # V2: enriquece com codAcao, codNatureza, codFonte, codPrograma
        df_full = enrich_classificacao_columns(df_full)

        for col in COLUMNS:
            if col not in df_full.columns:
                df_full[col] = None

        df = df_full[COLUMNS].copy()
        return (ug, cod_conta, month), df, len(df), elapsed, 200

    except requests.RequestException:
        elapsed = time.time() - t0
        return (ug, cod_conta, month), pd.DataFrame(columns=COLUMNS), 0, elapsed, "request_error"

# =========================
# MAIN
# =========================

def main_loa():
    wall_t0 = time.time()

    ugs = resolve_ugs()
    is_all_mode = (UG_MODE or "").lower().strip() == "all"

    months = list(range(1, 13))

    print("=" * 70)
    print(f"Iniciando atualização de dados de LOA (V2) - Anos {YEARS}")
    print(f"  Novas colunas: codAcao, codNatureza, codFonte, codPrograma")
    if is_all_mode:
        print(f"  UGs: {len(ugs)} UGs (modo all)")
    else:
        print(f"  UG: {', '.join(ugs)}")
    print(f"  Tabela destino: {tabela_destino}")
    print("=" * 70)

    # === PASSO 0: DROP TABLE para recriar com novas colunas ===
    print("\n[PASSO 0] Excluindo tabela antiga (DROP TABLE)...")
    with ENGINE.begin() as conn:
        drop_table_if_exists(conn, tabela_destino)

    session = make_session()

    for YEAR in YEARS:
        print(f"\n{'─'*50}")
        print(f"  Processando ano {YEAR}...")
        print(f"{'─'*50}")

        dfs = []
        total_rows = 0
        total_api_time_sum = 0.0

        status_counts = {}
        per_ug_summary = {}

        max_workers = min(MAX_WORKERS_CAP, max(1, len(ugs)))

        tasks = []
        for ug in ugs:
            for month in months:
                if should_skip_month(YEAR, month):
                    continue
                for cod in IDS:
                    if cod == "522110101" and month != 1:
                        continue
                    tasks.append((cod, ug, YEAR, month))

        if not tasks:
            print(f"  [SKIP] Nenhuma tarefa para o ano {YEAR} (meses futuros)")
            continue

        if DEBUG_LOG:
            print(f"  [INFO] Total de chamadas planejadas para {YEAR}: {len(tasks)}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_data, session, cod, ug, year, month, TOKEN)
                       for (cod, ug, year, month) in tasks]

            for future in as_completed(futures):
                key, df, qtd, elapsed, status = future.result()

                total_rows += qtd
                total_api_time_sum += elapsed
                status_counts[status] = status_counts.get(status, 0) + 1

                ug = key[0]
                prev_rows, prev_time = per_ug_summary.get(ug, (0, 0.0))
                per_ug_summary[ug] = (prev_rows + qtd, prev_time + elapsed)

                if df is not None and not df.empty:
                    dfs.append(df)

        if not dfs:
            print(f"  Nenhum dado retornado para {YEAR}. Status: {status_counts}")
            continue

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            final_df = pd.concat(dfs, ignore_index=True)

        final_df = final_df.dropna(how="all")

        if "mes" in final_df.columns:
            final_df["mes"] = pd.to_numeric(final_df["mes"], errors="coerce").fillna(0).astype("int64")
        if "ano" in final_df.columns:
            final_df["ano"] = pd.to_numeric(final_df["ano"], errors="coerce").fillna(0).astype("int64")

        try:
            with ENGINE.begin() as conn:
                # No primeiro ano a tabela já foi dropada; nos seguintes, append direto
                final_df.to_sql(
                    name=tabela_destino,
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=CHUNKSIZE_SQL,
                    method="multi",
                )

            print(f"  [OK] {YEAR}: {len(final_df)} linhas inseridas (total API: {total_rows})")

        except Exception as e:
            print(f"  [ERRO] {YEAR}: {e}")

    # === PÓS-PROCESSAMENTO: Otimizar colunas e criar índices ===
    print("\n[PÓS] Otimizando colunas e criando índices...")
    try:
        with ENGINE.begin() as conn:
            conn.execute(text(f"ALTER TABLE {tabela_destino} MODIFY COLUMN codAcao VARCHAR(20)"))
            conn.execute(text(f"ALTER TABLE {tabela_destino} MODIFY COLUMN codNatureza VARCHAR(20)"))
            conn.execute(text(f"ALTER TABLE {tabela_destino} MODIFY COLUMN codFonte VARCHAR(20)"))
            conn.execute(text(f"ALTER TABLE {tabela_destino} MODIFY COLUMN codPrograma VARCHAR(30)"))
            conn.execute(text(f"ALTER TABLE {tabela_destino} MODIFY COLUMN id VARCHAR(20)"))
            conn.execute(text(f"ALTER TABLE {tabela_destino} MODIFY COLUMN codigoUG VARCHAR(15)"))
            print("  Colunas convertidas para VARCHAR")

            conn.execute(text(f"CREATE INDEX idx_loa_ano_acao_nat_fonte ON {tabela_destino} (ano, codAcao, codNatureza, codFonte)"))
            conn.execute(text(f"CREATE INDEX idx_loa_ano_id ON {tabela_destino} (ano, id)"))
            print("  Índices criados: idx_loa_ano_acao_nat_fonte, idx_loa_ano_id")
    except Exception as e:
        print(f"  [AVISO] Erro ao otimizar: {e}")

    wall_elapsed = time.time() - wall_t0
    print(f"\n{'='*70}")
    print(f"CONCLUÍDO — Todos os anos processados em {wall_elapsed:.1f}s ({wall_elapsed/60:.1f} min)")
    print(f"Tabela '{tabela_destino}' recriada com colunas de classificação (V2)")
    print(f"{'='*70}")


if __name__ == "__main__":
    main_loa()
