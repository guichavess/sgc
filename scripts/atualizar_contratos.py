import time
import warnings
import hashlib
import requests
import pandas as pd
import os
import json
import sys

from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text, bindparam, create_engine
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
# 2. CONFIGURAÇÕES ESPECÍFICAS (SINGLE MODE)
# =============================================================================

TBL_CONTRATOS = "contratos"
TBL_FISCAIS = "fiscais_contrato"
TBL_ADITIVOS = "contratos_aditivo"

# Ano corrente ou fixo
YEAR_OVERRIDE = None
YEAR = YEAR_OVERRIDE if YEAR_OVERRIDE else date.today().year

# >>> CONFIGURAÇÃO FIXA PARA O SEU CENÁRIO <<<
UG_MODE = "single"
UG_SINGLE = "210101"
UG_PAD_SIZE = 6

# Performance
MAX_WORKERS_CAP = 24
CHUNKSIZE_SQL = 2000
HTTP_TIMEOUT = (5, 40)

# =============================================================================
# 3. FUNÇÃO DE AUTENTICAÇÃO (SIAFE)
# =============================================================================
def get_token(max_tentativas=3):
    siafe_user = os.getenv('SIAFE_USUARIO')
    siafe_pass = os.getenv('SIAFE_SENHA')

    if not siafe_user or not siafe_pass:
        print("ERRO: Variáveis SIAFE_USUARIO e SIAFE_SENHA não encontradas no .env")
        sys.exit(1)

    url = "https://tesouro.sefaz.pi.gov.br/siafe-api/auth"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"usuario": siafe_user, "senha": siafe_pass})

    for tentativa in range(1, max_tentativas + 1):
        try:
            print(f"  Tentativa {tentativa}/{max_tentativas}...")
            response = requests.post(url, headers=headers, data=payload, timeout=30, verify=False)
            if response.status_code == 200:
                return response.json()['token']
            else:
                print(f"  Erro Auth SIAFE: HTTP {response.status_code}")
        except Exception as e:
            print(f"  Falha na tentativa {tentativa}: {e}")

        if tentativa < max_tentativas:
            espera = tentativa * 5
            print(f"  Aguardando {espera}s antes de tentar novamente...")
            import time
            time.sleep(espera)

    print("ERRO CRÍTICO: Não foi possível autenticar no SIAFE após todas as tentativas.")
    sys.exit(1)

print("Autenticando no SIAFE...")
TOKEN = get_token()

# =============================================================================
# 4. COLUNAS E ESTRUTURAS
# =============================================================================

COLUMNS_CONTRATO = [
    "codigo", "situacao", "numeroOriginal", "numProcesso", "objeto", "natureza",
    "tipoContratante", "codigoContratante", "nomeContratante", "tipoContratado",
    "codigoContratado", "nomeContratado", "valor", "valorTotal", "dataProposta",
    "dataCelebracao", "dataPublicacao", "dataInicioVigencia", "dataFimVigencia",
    "codigoModalidadeLicitacao", "nomeModalidadeLicitacao", "regimeExecucao",
    "modalidade", "objetivo", "status", "dataFimVigenciaTotal",
]

DATE_COLUMNS_CONTRATO = [
    "dataProposta", "dataCelebracao", "dataPublicacao", "dataInicioVigencia",
    "dataFimVigencia", "dataFimVigenciaTotal",
]

FISCAL_COLS = ["codigo_contrato", "tipo", "nome", "cpf", "telefone", "email", "registroProfissional"]

ADITIVO_COLS = [
    "codigo_contrato", "codAditivo", "numOriginal", "numProcesso", "dtVigenciaIni",
    "dtVigenciaFim", "dtPublicacao", "valor", "dataCelebracao", "objeto",
]
ADITIVO_DATE_COLS = ["dtVigenciaIni", "dtVigenciaFim", "dtPublicacao", "dataCelebracao"]

# =============================================================================
# 5. HELPERS E CONEXÃO
# =============================================================================

def make_session():
    session = requests.Session()
    session.verify = False  # SIAFE API pode ter certificado self-signed
    retry = Retry(
        total=5, connect=5, read=5, status=5, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
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

def chunked(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf: yield buf

def stable_hash(obj) -> str:
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()

def sanitize_records(records):
    out = []
    for r in records:
        clean = {}
        for k, v in r.items():
            if v is None:
                clean[k] = None
            else:
                try:
                    if pd.isna(v): clean[k] = None
                    else: clean[k] = v
                except Exception:
                    clean[k] = v
        out.append(clean)
    return out

def resolve_ugs():
    if UG_MODE == "single":
        ug = normalize_ug(UG_SINGLE, UG_PAD_SIZE)
        return [ug] if ug else []
    
    # Fallback
    with ENGINE.connect() as conn:
        rows = conn.execute(text("SELECT codigo FROM ug")).fetchall()
    return [normalize_ug(r[0], UG_PAD_SIZE) for r in rows if r[0]]

def _column_exists(conn, table_name, column_name):
    """Verifica se uma coluna existe na tabela."""
    q = text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = :tbl
          AND column_name = :col
        LIMIT 1
    """)
    return conn.execute(q, {"tbl": table_name, "col": column_name}).fetchone() is not None

def ensure_tables(conn):
    # Cria tabela se não existir
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_CONTRATOS} (
            codigo VARCHAR(32) NOT NULL,
            situacao VARCHAR(50) NULL,
            numeroOriginal VARCHAR(80) NULL,
            numProcesso VARCHAR(80) NULL,
            objeto TEXT NULL,
            natureza VARCHAR(50) NULL,
            tipoContratante VARCHAR(80) NULL,
            codigoContratante VARCHAR(32) NULL,
            nomeContratante VARCHAR(255) NULL,
            tipoContratado VARCHAR(80) NULL,
            codigoContratado VARCHAR(32) NULL,
            nomeContratado VARCHAR(255) NULL,
            valor DECIMAL(18,2) NULL,
            valorTotal DECIMAL(18,2) NULL,
            dataProposta DATE NULL,
            dataCelebracao DATE NULL,
            dataPublicacao DATE NULL,
            dataInicioVigencia DATE NULL,
            dataFimVigencia DATE NULL,
            codigoModalidadeLicitacao VARCHAR(32) NULL,
            nomeModalidadeLicitacao VARCHAR(120) NULL,
            regimeExecucao VARCHAR(80) NULL,
            modalidade VARCHAR(80) NULL,
            objetivo TEXT NULL,
            status VARCHAR(80) NULL,
            dataFimVigenciaTotal DATE NULL,
            hash_contrato CHAR(40) NULL,
            hash_fiscais  CHAR(40) NULL,
            hash_aditivos CHAR(40) NULL,
            last_checked  TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (codigo),
            KEY idx_numeroOriginal (numeroOriginal)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    # Se a tabela já existia, garante que as colunas novas existam
    new_columns = {
        "hash_contrato": "CHAR(40) NULL",
        "hash_fiscais": "CHAR(40) NULL",
        "hash_aditivos": "CHAR(40) NULL",
        "last_checked": "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    }
    for col_name, col_def in new_columns.items():
        if not _column_exists(conn, TBL_CONTRATOS, col_name):
            print(f"  Adicionando coluna '{col_name}' na tabela {TBL_CONTRATOS}...")
            conn.execute(text(f"ALTER TABLE {TBL_CONTRATOS} ADD COLUMN {col_name} {col_def}"))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_FISCAIS} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo_contrato VARCHAR(32) NULL,
            tipo VARCHAR(40) NULL,
            nome VARCHAR(255) NULL,
            cpf VARCHAR(20) NULL,
            telefone VARCHAR(60) NULL,
            email VARCHAR(180) NULL,
            registroProfissional VARCHAR(80) NULL,
            data_atualizacao TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_fiscal_contrato (codigo_contrato),
            KEY idx_fiscal_nome (nome)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_ADITIVOS} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo_contrato VARCHAR(32) NULL,
            codAditivo VARCHAR(20) NULL,
            numOriginal VARCHAR(80) NULL,
            numProcesso VARCHAR(80) NULL,
            dtVigenciaIni DATE NULL,
            dtVigenciaFim DATE NULL,
            dtPublicacao DATE NULL,
            valor DECIMAL(18,2) NULL,
            dataCelebracao DATE NULL,
            objeto TEXT NULL,
            updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_aditivo_contrato (codigo_contrato),
            KEY idx_aditivo_numOriginal (numOriginal)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

# =============================================================================
# 6. LÓGICA DE NEGÓCIO
# =============================================================================

def fetch_cod_contratos_from_empenho(conn, ug: str):
    # Busca códigos de contratos presentes na tabela EMPENHO (dependência lógica)
    q = text("""
        SELECT DISTINCT l.codContrato AS codContrato
        FROM empenho l
        WHERE l.codigoUG = :ug AND l.codContrato <> 0
    """)
    rows = conn.execute(q, {"ug": ug}).fetchall()
    return [str(r[0]) for r in rows if r and r[0] not in (None, 0, "0", 0.0)]

def load_existing_hashes(conn, cods):
    if not cods: return {}
    out = {}
    q = text(f"SELECT codigo, hash_contrato, hash_fiscais, hash_aditivos FROM {TBL_CONTRATOS} WHERE codigo IN :cods")
    q = q.bindparams(bindparam("cods", expanding=True))
    
    for chunk in chunked(cods, 5000):
        rows = conn.execute(q, {"cods": chunk}).fetchall()
        for codigo, hc, hf, ha in rows:
            out[str(codigo)] = (hc, hf, ha)
    return out

def fetch_contrato(session, cod_contrato: str, token: str, year: int):
    url = f"https://tesouro.sefaz.pi.gov.br/siafe-api/contrato/{year}/{cod_contrato}"
    headers = {"Authorization": f"Bearer {token}"}
    t0 = time.time()
    try:
        resp = session.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        elapsed = time.time() - t0
        if resp.status_code != 200: return cod_contrato, None, elapsed, resp.status_code
        
        data = resp.json()
        if not data: return cod_contrato, None, elapsed, "no_data"

        fiscais = data.get("responsaveisContrato") or []
        aditivos = data.get("aditivos") or []
        
        # Gera Hashes para comparar mudanças
        h_contrato = stable_hash({k: data.get(k) for k in COLUMNS_CONTRATO})
        h_fiscais = stable_hash(fiscais)
        h_aditivos = stable_hash(aditivos)
        
        return cod_contrato, (data, h_contrato, h_fiscais, h_aditivos), elapsed, 200
    except Exception:
        return cod_contrato, None, time.time() - t0, "error"

# =============================================================================
# 7. FUNÇÕES DE UPSERT (GRAVAÇÃO)
# =============================================================================

def upsert_contratos_changed(conn, rows):
    if not rows: return 0
    df = pd.DataFrame(rows)
    for col in COLUMNS_CONTRATO:
        if col not in df.columns: df[col] = None
    for col in DATE_COLUMNS_CONTRATO:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    
    cols = COLUMNS_CONTRATO + ["hash_contrato", "hash_fiscais", "hash_aditivos", "last_checked"]
    placeholders = ", ".join([f":{c}" for c in cols])
    # Monta query dinâmica de UPDATE
    updates = ", ".join([f"{c}=VALUES({c})" for c in cols if c != "codigo"])
    
    sql = text(f"INSERT INTO {TBL_CONTRATOS} ({', '.join(cols)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}")
    
    records = sanitize_records(df[cols].to_dict(orient="records"))
    for chunk in chunked(records, CHUNKSIZE_SQL):
        conn.execute(sql, chunk)
    return len(records)

def sync_fiscais_changed(conn, codContrato, fiscais):
    conn.execute(text(f"DELETE FROM {TBL_FISCAIS} WHERE codigo_contrato = :c"), {"c": codContrato})
    if not fiscais: return 0

    df = pd.json_normalize(fiscais)
    for col in ["tipo", "nome", "cpf", "telefone", "email", "registroProfissional"]:
        if col not in df.columns: df[col] = None

    df.insert(0, "codigo_contrato", str(codContrato))
    # Limpeza básica de CPF
    df["cpf"] = df["cpf"].astype("string").fillna("").str.replace(r"\D+", "", regex=True)
    df = df[df["cpf"].str.len() > 0]

    # Remove duplicatas (API pode retornar mesmo CPF+tipo duplicado)
    df = df.drop_duplicates(subset=["codigo_contrato", "tipo", "cpf"], keep="first")

    if df.empty: return 0

    placeholders = ", ".join([f":{c}" for c in FISCAL_COLS])
    sql = text(f"""
        INSERT IGNORE INTO {TBL_FISCAIS} ({", ".join(FISCAL_COLS)}) VALUES ({placeholders})
    """)

    records = sanitize_records(df[FISCAL_COLS].to_dict(orient="records"))
    for chunk in chunked(records, CHUNKSIZE_SQL):
        conn.execute(sql, chunk)
    return len(records)

def sync_aditivos_changed(conn, codContrato, aditivos):
    conn.execute(text(f"DELETE FROM {TBL_ADITIVOS} WHERE codigo_contrato = :c"), {"c": codContrato})
    if not aditivos: return 0

    df = pd.json_normalize(aditivos)
    df.insert(0, "codigo_contrato", str(codContrato))

    # Garante colunas
    needed_cols = ["codigo_contrato"] + [c for c in ADITIVO_COLS if c != "codigo_contrato"]
    for col in needed_cols:
        if col not in df.columns: df[col] = None
        
    for col in ADITIVO_DATE_COLS:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    placeholders = ", ".join([f":{c}" for c in ADITIVO_COLS])
    sql = text(f"""
        INSERT INTO {TBL_ADITIVOS} ({", ".join(ADITIVO_COLS)}) VALUES ({placeholders})
    """)
    
    records = sanitize_records(df[ADITIVO_COLS].to_dict(orient="records"))
    for chunk in chunked(records, CHUNKSIZE_SQL):
        conn.execute(sql, chunk)
    return len(records)

# =============================================================================
# 8. EXECUÇÃO PRINCIPAL
# =============================================================================

def main():
    t0 = time.time()
    ugs = resolve_ugs()
    
    print(f"Iniciando atualização de CONTRATOS (Modo: {UG_MODE})")

    # Garante estrutura das tabelas
    with ENGINE.begin() as conn:
        ensure_tables(conn)

    # 1. Busca quais contratos a UG tem (olhando na tabela Empenho)
    # Isso é muito mais rápido que buscar "todos os contratos da API"
    with ENGINE.connect() as conn:
        cods = []
        for ug in ugs:
            print(f"Buscando códigos de contrato para UG {ug} na tabela empenho...")
            cods.extend(fetch_cod_contratos_from_empenho(conn, ug))
    
    cods = sorted(set(cods))
    print(f"Total de contratos únicos encontrados: {len(cods)}")
    
    if not cods:
        print("Nenhum contrato vinculado a empenhos desta UG.")
        return

    # 2. Carrega hashes atuais (para não atualizar o que não mudou)
    print("Carregando hashes atuais do banco...")
    with ENGINE.connect() as conn:
        existing = load_existing_hashes(conn, cods)

    # 3. Consulta API em paralelo
    session = make_session()
    changed_rows, changed_fiscais, changed_aditivos = [], [], []
    now_ts = datetime.now()

    print(f"Consultando detalhes de {len(cods)} contratos na API...")
    with ThreadPoolExecutor(max_workers=min(24, len(cods))) as ex:
        futures = [ex.submit(fetch_contrato, session, c, TOKEN, YEAR) for c in cods]
        
        for fut in as_completed(futures):
            cod, payload, _, _ = fut.result()
            if not payload: continue
            
            data, h_contrato, h_fiscais, h_aditivos = payload
            old = existing.get(str(cod))
            old_hc, old_hf, old_ha = old if old else (None, None, None)

            # Só atualiza se o hash mudou (dados mudaram)
            if (h_contrato != old_hc) or (h_fiscais != old_hf) or (h_aditivos != old_ha):
                row = {k: data.get(k) for k in COLUMNS_CONTRATO}
                row["codigo"] = str(row.get("codigo") or cod)
                row["hash_contrato"] = h_contrato
                row["hash_fiscais"] = h_fiscais
                row["hash_aditivos"] = h_aditivos
                row["last_checked"] = now_ts
                changed_rows.append(row)

                if (h_fiscais != old_hf):
                    changed_fiscais.append((str(cod), data.get("responsaveisContrato") or []))
                if (h_aditivos != old_ha):
                    changed_aditivos.append((str(cod), data.get("aditivos") or []))

    # 4. Gravação (apenas do que mudou)
    if changed_rows:
        print(f"Detectadas mudanças em {len(changed_rows)} contratos. Gravando...")
        with ENGINE.begin() as conn:
            upsert_contratos_changed(conn, changed_rows)
            for cod, fisc in changed_fiscais: sync_fiscais_changed(conn, cod, fisc)
            for cod, adit in changed_aditivos: sync_aditivos_changed(conn, cod, adit)
        print("Gravação concluída.")
    else:
        print("Nenhuma alteração detectada nos contratos existentes.")

    print(f"Tempo total: {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()