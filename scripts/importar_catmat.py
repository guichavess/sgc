"""
Script de sincronização do Catálogo de Materiais (CATMAT) a partir de banco MySQL remoto.
Fonte: Banco remoto (tabelas sol_grupos, sol_classes, sol_pdms, sol_itens)
Destino: Banco local MySQL (sgc) - tabelas catmat_*

Estratégia: Sync diferencial com hash (só atualiza o que mudou).

Uso: python scripts/importar_catmat.py

Configurar no .env:
    CATMAT_DB_HOST=<host>
    CATMAT_DB_USER=<user>
    CATMAT_DB_PASS=<pass>
    CATMAT_DB_NAME=<dbname>
    CATMAT_DB_PORT=3306  (opcional, default 3306)

Estrutura das tabelas remotas:
    sol_grupos:  id, data_hora_atualizacao, nome, status
    sol_classes: id, data_hora_atualizacao, nome, status, id_grupo
    sol_pdms:    id, data_hora_atualizacao, nome, status, id_classe
    sol_itens:   id, aplica_margem_preferencia, codigo_ncm, data_hora_atualizacao,
                 descricao, descricao_ncm, item_sustentavel, status, id_pdm
"""
import os
import sys
import time
import json
import hashlib

import pandas as pd
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

# =============================================================================
# 1. CARREGAR VARIÁVEIS DE AMBIENTE E CONEXÕES
# =============================================================================

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

# Banco local (sgc)
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO CRÍTICO: Variáveis de banco local (DB_*) ausentes no .env")
    sys.exit(1)

LOCAL_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
LOCAL_ENGINE = create_engine(LOCAL_URI, echo=False)

# Banco remoto (catmat)
CATMAT_HOST = os.getenv('CATMAT_DB_HOST')
CATMAT_USER = os.getenv('CATMAT_DB_USER')
CATMAT_PASS = os.getenv('CATMAT_DB_PASS')
CATMAT_DB = os.getenv('CATMAT_DB_NAME')
CATMAT_PORT = os.getenv('CATMAT_DB_PORT', '3306')

if not all([CATMAT_HOST, CATMAT_USER, CATMAT_DB]):
    print("ERRO CRÍTICO: Variáveis do banco remoto CATMAT (CATMAT_DB_*) ausentes no .env")
    print("  Necessárias: CATMAT_DB_HOST, CATMAT_DB_USER, CATMAT_DB_PASS, CATMAT_DB_NAME")
    sys.exit(1)

REMOTE_URI = f"mysql+pymysql://{CATMAT_USER}:{CATMAT_PASS}@{CATMAT_HOST}:{CATMAT_PORT}/{CATMAT_DB}"
REMOTE_ENGINE = create_engine(REMOTE_URI, echo=False)

# =============================================================================
# 2. CONFIGURAÇÕES
# =============================================================================

CHUNKSIZE_SQL = 2000

# Configuração de sync por tabela.
# remote_cols: colunas a ler do remoto
# column_map: remoto -> local (mapeamento de nomes)
# Tabelas processadas em ordem hierárquica (top-down)
SYNC_CONFIG = [
    {
        'remote_table': 'sol_grupos',
        'local_table': 'catmat_grupos',
        'remote_cols': ['id', 'nome', 'status', 'data_hora_atualizacao'],
        'column_map': {
            'id': 'codigo',
            'nome': 'nome',
            'status': 'status',
            'data_hora_atualizacao': 'data_atualizacao',
        },
    },
    {
        'remote_table': 'sol_classes',
        'local_table': 'catmat_classes',
        'remote_cols': ['id', 'nome', 'status', 'data_hora_atualizacao', 'id_grupo'],
        'column_map': {
            'id': 'codigo',
            'nome': 'nome',
            'status': 'status',
            'data_hora_atualizacao': 'data_atualizacao',
            'id_grupo': 'codigo_grupo',
        },
    },
    {
        'remote_table': 'sol_pdms',
        'local_table': 'catmat_pdms',
        'remote_cols': ['id', 'nome', 'status', 'data_hora_atualizacao', 'id_classe'],
        'column_map': {
            'id': 'codigo',
            'nome': 'nome',
            'status': 'status',
            'data_hora_atualizacao': 'data_atualizacao',
            'id_classe': 'codigo_classe',
        },
    },
    {
        'remote_table': 'sol_itens',
        'local_table': 'catmat_itens',
        'remote_cols': [
            'id', 'descricao', 'status', 'data_hora_atualizacao', 'id_pdm',
            'codigo_ncm', 'descricao_ncm', 'item_sustentavel', 'aplica_margem_preferencia',
        ],
        'column_map': {
            'id': 'codigo',
            'descricao': 'descricao',
            'status': 'status',
            'data_hora_atualizacao': 'data_atualizacao',
            'id_pdm': 'codigo_pdm',
            'codigo_ncm': 'codigo_ncm',
            'descricao_ncm': 'descricao_ncm',
            'item_sustentavel': 'item_sustentavel',
            'aplica_margem_preferencia': 'aplica_margem_preferencia',
        },
    },
]

# =============================================================================
# 3. HELPERS
# =============================================================================

def stable_hash(obj) -> str:
    """Gera SHA1 hash estável de um dicionário."""
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha1(blob.encode('utf-8')).hexdigest()


def sanitize_records(records):
    """Converte NaN/NaT para None em lista de dicts. Converte numpy types para nativos."""
    out = []
    for r in records:
        clean = {}
        for k, v in r.items():
            if v is None:
                clean[k] = None
            else:
                try:
                    if pd.isna(v):
                        clean[k] = None
                    else:
                        if hasattr(v, 'item'):
                            clean[k] = v.item()
                        else:
                            clean[k] = v
                except Exception:
                    clean[k] = v
        out.append(clean)
    return out


def chunked(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


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


# =============================================================================
# 4. CRIAÇÃO DAS TABELAS LOCAIS
# =============================================================================

def ensure_tables(conn):
    """Cria tabelas catmat_* se não existirem. Adiciona colunas novas se necessário."""

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS catmat_grupos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo INT NOT NULL UNIQUE,
            nome VARCHAR(255) NOT NULL,
            status TINYINT(1) DEFAULT 1,
            data_atualizacao DATETIME NULL,
            hash_row CHAR(40) NULL,
            last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS catmat_classes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo INT NOT NULL UNIQUE,
            codigo_grupo INT NOT NULL,
            nome VARCHAR(255) NOT NULL,
            status TINYINT(1) DEFAULT 1,
            data_atualizacao DATETIME NULL,
            hash_row CHAR(40) NULL,
            last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_catmat_classe_grupo (codigo_grupo)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS catmat_pdms (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo INT NOT NULL UNIQUE,
            codigo_classe INT NOT NULL,
            nome VARCHAR(500) NOT NULL,
            status TINYINT(1) DEFAULT 1,
            data_atualizacao DATETIME NULL,
            hash_row CHAR(40) NULL,
            last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_catmat_pdm_classe (codigo_classe)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS catmat_itens (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo INT NOT NULL UNIQUE,
            codigo_pdm INT NOT NULL,
            descricao TEXT NOT NULL,
            status TINYINT(1) DEFAULT 1,
            codigo_ncm VARCHAR(20) NULL,
            descricao_ncm VARCHAR(500) NULL,
            item_sustentavel TINYINT(1) DEFAULT 0,
            aplica_margem_preferencia TINYINT(1) DEFAULT 0,
            data_atualizacao DATETIME NULL,
            hash_row CHAR(40) NULL,
            last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_catmat_item_pdm (codigo_pdm)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    # Adicionar colunas novas para tabelas que já existiam (migração incremental)
    new_columns = {
        'catmat_grupos': {
            'status': 'TINYINT(1) DEFAULT 1',
            'data_atualizacao': 'DATETIME NULL',
        },
        'catmat_classes': {
            'status': 'TINYINT(1) DEFAULT 1',
            'data_atualizacao': 'DATETIME NULL',
        },
        'catmat_pdms': {
            'status': 'TINYINT(1) DEFAULT 1',
            'data_atualizacao': 'DATETIME NULL',
        },
        'catmat_itens': {
            'descricao': 'VARCHAR(500) NOT NULL DEFAULT ""',
            'codigo_ncm': 'VARCHAR(20) NULL',
            'descricao_ncm': 'VARCHAR(500) NULL',
            'item_sustentavel': 'TINYINT(1) DEFAULT 0',
            'aplica_margem_preferencia': 'TINYINT(1) DEFAULT 0',
            'data_atualizacao': 'DATETIME NULL',
        },
    }
    for table, cols in new_columns.items():
        for col_name, col_def in cols.items():
            if not _column_exists(conn, table, col_name):
                print(f"    Adicionando coluna '{col_name}' em {table}...")
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}"))

    # Se catmat_itens tinha coluna 'nome' (versão anterior), renomear para 'descricao'
    if _column_exists(conn, 'catmat_itens', 'nome') and not _column_exists(conn, 'catmat_itens', 'descricao'):
        print("    Renomeando coluna 'nome' para 'descricao' em catmat_itens...")
        conn.execute(text("ALTER TABLE catmat_itens CHANGE COLUMN nome descricao VARCHAR(500) NOT NULL"))

    print("  Tabelas locais garantidas.")


# =============================================================================
# 5. LÓGICA DE SYNC DIFERENCIAL
# =============================================================================

def fetch_remote_data(remote_conn, config):
    """Lê todos os registros da tabela remota."""
    table = config['remote_table']
    cols = config['remote_cols']
    sql = f"SELECT {', '.join(cols)} FROM {table}"
    df = pd.read_sql(text(sql), remote_conn)
    return df


def convert_value(val):
    """Converte tipos especiais do banco remoto para tipos Python nativos."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    # BIT/bytes do MySQL -> int (b'\x01' -> 1, b'\x00' -> 0)
    if isinstance(val, (bytes, bytearray)):
        return int.from_bytes(val, byteorder='big')
    # Timestamp do pandas -> datetime nativo
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    # numpy int/float -> Python nativo
    if hasattr(val, 'item'):
        return val.item()
    return val


def map_remote_to_local(df_remote, column_map):
    """Aplica mapeamento de colunas remoto -> local e retorna lista de dicts."""
    records = []
    for _, row in df_remote.iterrows():
        rec = {}
        for remote_col, local_col in column_map.items():
            val = row.get(remote_col)
            rec[local_col] = convert_value(val)
        records.append(rec)
    return records


def load_local_hashes(local_conn, table_name):
    """Carrega {codigo: hash_row} da tabela local."""
    q = text(f"SELECT codigo, hash_row FROM {table_name}")
    rows = local_conn.execute(q).fetchall()
    return {int(r[0]): r[1] for r in rows}


def sync_table(local_conn, config, df_remote):
    """Sincroniza uma tabela: compara hashes, aplica INSERT/UPDATE/DELETE."""
    local_table = config['local_table']
    column_map = config['column_map']

    # Mapear dados remotos para formato local
    mapped_records = map_remote_to_local(df_remote, column_map)

    # Carrega hashes locais
    local_hashes = load_local_hashes(local_conn, local_table)

    to_insert = []
    to_update = []
    remote_codes = set()

    for rec in mapped_records:
        code = rec.get('codigo')
        if code is None:
            continue
        code = int(code)
        remote_codes.add(code)

        # Calcula hash dos dados (excluindo hash_row)
        row_hash = stable_hash(rec)
        rec['hash_row'] = row_hash

        if code not in local_hashes:
            to_insert.append(rec)
        elif local_hashes[code] != row_hash:
            to_update.append(rec)

    # Detecta deleções
    to_delete = set(local_hashes.keys()) - remote_codes

    # Aplica inserções em chunks
    if to_insert:
        cols = list(to_insert[0].keys())
        placeholders = ", ".join([f":{c}" for c in cols])
        sql = text(f"INSERT INTO {local_table} ({', '.join(cols)}) VALUES ({placeholders})")
        records = sanitize_records(to_insert)
        for chunk in chunked(records, CHUNKSIZE_SQL):
            local_conn.execute(sql, chunk)

    # Aplica atualizações
    if to_update:
        for rec in to_update:
            set_parts = []
            params = {'_codigo': rec['codigo']}
            for k, v in rec.items():
                if k != 'codigo':
                    set_parts.append(f"{k} = :{k}")
                    params[k] = v
            sql = text(f"UPDATE {local_table} SET {', '.join(set_parts)} WHERE codigo = :_codigo")
            local_conn.execute(sql, params)

    # Aplica deleções (ordem inversa à hierarquia)
    if to_delete:
        for chunk in chunked(list(to_delete), CHUNKSIZE_SQL):
            placeholders_str = ', '.join([str(c) for c in chunk])
            local_conn.execute(text(f"DELETE FROM {local_table} WHERE codigo IN ({placeholders_str})"))

    return len(to_insert), len(to_update), len(to_delete)


# =============================================================================
# 6. EXECUÇÃO PRINCIPAL
# =============================================================================

def main():
    t0 = time.time()

    print("=== Sincronização CATMAT (Catálogo de Materiais) ===")
    print()

    # Testar conexão remota
    print("Testando conexão com banco remoto...")
    try:
        with REMOTE_ENGINE.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  Conexão remota OK.")
    except Exception as e:
        print(f"  ERRO ao conectar no banco remoto: {e}")
        sys.exit(1)

    # Contar registros remotos
    print("Contando registros remotos...")
    with REMOTE_ENGINE.connect() as conn:
        for cfg in SYNC_CONFIG:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {cfg['remote_table']}")).scalar()
            print(f"  {cfg['remote_table']}: {count} registros")
    print()

    # Garantir tabelas locais
    print("Verificando estrutura das tabelas locais...")
    with LOCAL_ENGINE.begin() as conn:
        ensure_tables(conn)
    print()

    # Sync em ordem hierárquica (top-down)
    print("Iniciando sincronização...")
    total_insert = 0
    total_update = 0
    total_delete = 0

    with REMOTE_ENGINE.connect() as remote_conn:
        with LOCAL_ENGINE.begin() as local_conn:
            for config in SYNC_CONFIG:
                print(f"  {config['remote_table']} -> {config['local_table']}...")

                try:
                    df_remote = fetch_remote_data(remote_conn, config)

                    ins, upd, dlt = sync_table(local_conn, config, df_remote)
                    print(f"    +{ins} inseridos, ~{upd} atualizados, -{dlt} removidos")

                    total_insert += ins
                    total_update += upd
                    total_delete += dlt
                except Exception as e:
                    print(f"    ERRO: {e}")
                    raise

    print()
    print(f"Totais: +{total_insert} inseridos, ~{total_update} atualizados, -{total_delete} removidos")
    print(f"Sincronização concluída em {time.time() - t0:.2f}s")


if __name__ == '__main__':
    main()
