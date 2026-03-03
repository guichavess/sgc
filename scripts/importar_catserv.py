"""
Script de importação do Catálogo de Serviços (CATSERV) a partir do Excel.
Fonte: BASE CATSERV.xlsx (5 sheets: SECAO, DIVISAO, GRUPO, CLASSE, ITEM)
Destino: Banco local MySQL (sgc) - tabelas catserv_*

Uso: python scripts/importar_catserv.py
"""
import os
import sys
import time

import pandas as pd
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

# =============================================================================
# 1. CARREGAR VARIÁVEIS DE AMBIENTE E CONEXÃO
# =============================================================================

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
# 2. CONFIGURAÇÕES
# =============================================================================

EXCEL_FILE = os.path.join(base_dir, 'BASE CATSERV.xlsx')

SHEET_SECAO = 'SECAO'
SHEET_DIVISAO = 'DIVISAO'
SHEET_GRUPO = 'GRUPO'
SHEET_CLASSE = 'CLASSE'
SHEET_ITEM = 'ITEM'

TBL_SECOES = 'catserv_secoes'
TBL_DIVISOES = 'catserv_divisoes'
TBL_GRUPOS = 'catserv_grupos'
TBL_CLASSES = 'catserv_classes'
TBL_SERVICOS = 'catserv_servicos'

CHUNKSIZE_SQL = 2000

# =============================================================================
# 3. HELPERS
# =============================================================================

def fix_encoding(val):
    """Tenta corrigir strings com encoding CP1252 corrompido."""
    if not isinstance(val, str):
        return val
    try:
        return val.encode('latin1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return val


def sanitize_records(records):
    """Converte NaN/NaT/pd.NA para None em lista de dicts. Converte numpy int para int nativo."""
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
                        # Converte numpy int/float para tipo Python nativo
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


# =============================================================================
# 4. CRIAÇÃO DAS TABELAS
# =============================================================================

def ensure_tables(conn):
    """Cria tabelas catserv_* se não existirem."""

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_SECOES} (
            codigo_secao INT NOT NULL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            status TINYINT(1) DEFAULT 1,
            data_atualizacao DATETIME NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_DIVISOES} (
            codigo_divisao INT NOT NULL PRIMARY KEY,
            codigo_secao INT NOT NULL,
            nome VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY idx_divisao_secao (codigo_secao),
            CONSTRAINT fk_divisao_secao FOREIGN KEY (codigo_secao)
                REFERENCES {TBL_SECOES} (codigo_secao) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_GRUPOS} (
            codigo_grupo INT NOT NULL PRIMARY KEY,
            codigo_divisao INT NOT NULL,
            nome VARCHAR(255) NOT NULL,
            status TINYINT(1) DEFAULT 1,
            data_atualizacao DATETIME NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY idx_grupo_divisao (codigo_divisao),
            CONSTRAINT fk_grupo_divisao FOREIGN KEY (codigo_divisao)
                REFERENCES {TBL_DIVISOES} (codigo_divisao) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_CLASSES} (
            codigo_classe INT NOT NULL PRIMARY KEY,
            codigo_grupo INT NOT NULL,
            nome VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY idx_classe_grupo (codigo_grupo),
            CONSTRAINT fk_classe_grupo FOREIGN KEY (codigo_grupo)
                REFERENCES {TBL_GRUPOS} (codigo_grupo) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TBL_SERVICOS} (
            codigo_servico INT NOT NULL PRIMARY KEY,
            codigo_classe INT NULL,
            codigo_grupo INT NOT NULL,
            nome VARCHAR(500) NOT NULL,
            status TINYINT(1) DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY idx_servico_classe (codigo_classe),
            KEY idx_servico_grupo (codigo_grupo),
            CONSTRAINT fk_servico_classe FOREIGN KEY (codigo_classe)
                REFERENCES {TBL_CLASSES} (codigo_classe) ON DELETE SET NULL,
            CONSTRAINT fk_servico_grupo FOREIGN KEY (codigo_grupo)
                REFERENCES {TBL_GRUPOS} (codigo_grupo) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

    print("  Tabelas garantidas (CREATE IF NOT EXISTS).")


# =============================================================================
# 5. FUNÇÕES DE CARGA POR SHEET
# =============================================================================

def load_secoes(excel_path):
    """Carrega sheet SECAO e retorna DataFrame normalizado."""
    df = pd.read_excel(excel_path, sheet_name=SHEET_SECAO)
    df = df.rename(columns={
        'resultado.codigoSecao': 'codigo_secao',
        'resultado.nomeSecao': 'nome',
        'resultado.statusSecao': 'status',
        'resultado.dataHoraAtualizacao': 'data_atualizacao',
    })
    df['nome'] = df['nome'].apply(fix_encoding)
    df['status'] = df['status'].astype(int)
    df['data_atualizacao'] = pd.to_datetime(df['data_atualizacao'], errors='coerce')
    df['codigo_secao'] = df['codigo_secao'].astype(int)
    return df[['codigo_secao', 'nome', 'status', 'data_atualizacao']]


def load_divisoes(excel_path):
    """Carrega sheet DIVISAO e retorna DataFrame normalizado."""
    df = pd.read_excel(excel_path, sheet_name=SHEET_DIVISAO)
    # Coluna 'resultado.nomeSecao' na verdade contém codigo_secao (int)
    df = df.rename(columns={
        'resultado.nomeSecao': 'codigo_secao',
        'resultado.codigoDivisao': 'codigo_divisao',
        'resultado.nomeDivisao': 'nome',
    })
    df['nome'] = df['nome'].apply(fix_encoding)
    df['codigo_secao'] = df['codigo_secao'].astype(int)
    df['codigo_divisao'] = df['codigo_divisao'].astype(int)
    return df[['codigo_divisao', 'codigo_secao', 'nome']]


def load_grupos(excel_path):
    """Carrega sheet GRUPO e retorna DataFrame normalizado."""
    df = pd.read_excel(excel_path, sheet_name=SHEET_GRUPO)
    df = df.rename(columns={
        'resultado.codigoDivisao': 'codigo_divisao',
        'resultado.codigoGrupo': 'codigo_grupo',
        'resultado.nomeGrupo': 'nome',
        'resultado.statusGrupo': 'status',
        'resultado.dataHoraAtualizacao': 'data_atualizacao',
    })
    df['nome'] = df['nome'].apply(fix_encoding)
    df['status'] = df['status'].astype(int)
    df['data_atualizacao'] = pd.to_datetime(df['data_atualizacao'], errors='coerce')
    df['codigo_divisao'] = df['codigo_divisao'].astype(int)
    df['codigo_grupo'] = df['codigo_grupo'].astype(int)
    return df[['codigo_grupo', 'codigo_divisao', 'nome', 'status', 'data_atualizacao']]


def load_classes(excel_path):
    """Extrai classes únicas da sheet ITEM (a sheet CLASSE tem códigos incompatíveis)."""
    df = pd.read_excel(excel_path, sheet_name=SHEET_ITEM)
    df = df.dropna(subset=['resultado.codigoServico'])

    # Filtrar apenas itens que têm classe
    mask = df['resultado.codigoClasse'].notna()
    classes = df[mask][['resultado.codigoClasse', 'resultado.nomeClasse', 'resultado.codigoGrupo']].copy()
    classes = classes.drop_duplicates(subset='resultado.codigoClasse')

    classes = classes.rename(columns={
        'resultado.codigoClasse': 'codigo_classe',
        'resultado.nomeClasse': 'nome',
        'resultado.codigoGrupo': 'codigo_grupo',
    })
    classes['nome'] = classes['nome'].apply(fix_encoding)
    classes['codigo_classe'] = classes['codigo_classe'].astype(int)
    classes['codigo_grupo'] = classes['codigo_grupo'].astype(int)
    return classes[['codigo_classe', 'codigo_grupo', 'nome']]


def load_servicos(excel_path):
    """Carrega sheet ITEM e retorna DataFrame normalizado (apenas colunas relevantes)."""
    df = pd.read_excel(excel_path, sheet_name=SHEET_ITEM)

    # Drop rows com codigoServico nulo (linhas totalmente vazias)
    df = df.dropna(subset=['resultado.codigoServico'])

    df = df.rename(columns={
        'resultado.codigoServico': 'codigo_servico',
        'resultado.codigoClasse': 'codigo_classe',
        'resultado.codigoGrupo': 'codigo_grupo',
        'resultado.nomeServico': 'nome',
        'resultado.statusServico': 'status',
    })

    df['nome'] = df['nome'].apply(fix_encoding)
    df['codigo_servico'] = df['codigo_servico'].astype(int)
    df['codigo_grupo'] = df['codigo_grupo'].astype(int)
    df['status'] = df['status'].astype(int)

    # codigo_classe é nullable (710 serviços não têm classe)
    # Converte float -> Int64 (nullable integer type do pandas)
    df['codigo_classe'] = pd.array(
        [int(v) if pd.notna(v) else pd.NA for v in df['codigo_classe']],
        dtype=pd.Int64Dtype()
    )

    return df[['codigo_servico', 'codigo_classe', 'codigo_grupo', 'nome', 'status']]


# =============================================================================
# 6. INSERÇÃO COM TRUNCATE + INSERT
# =============================================================================

def truncate_and_insert(conn, table_name, df, columns):
    """Limpa a tabela e insere os registros."""
    # Desabilita FK checks para permitir TRUNCATE com FKs
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    if df.empty:
        print(f"  {table_name}: 0 registros (tabela vazia)")
        return 0

    placeholders = ", ".join([f":{c}" for c in columns])
    sql = text(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})")

    records = sanitize_records(df[columns].to_dict(orient='records'))
    count = 0
    for chunk in chunked(records, CHUNKSIZE_SQL):
        conn.execute(sql, chunk)
        count += len(chunk)

    print(f"  {table_name}: {count} registros inseridos")
    return count


# =============================================================================
# 7. EXECUÇÃO PRINCIPAL
# =============================================================================

def main():
    t0 = time.time()

    # Valida existência do arquivo Excel
    if not os.path.isfile(EXCEL_FILE):
        print(f"ERRO: Arquivo Excel não encontrado: {EXCEL_FILE}")
        sys.exit(1)

    print(f"Importando Catálogo de Serviços de: {EXCEL_FILE}")
    print()

    # Valida sheets do Excel
    xl = pd.ExcelFile(EXCEL_FILE)
    expected_sheets = [SHEET_SECAO, SHEET_DIVISAO, SHEET_GRUPO, SHEET_CLASSE, SHEET_ITEM]
    missing = [s for s in expected_sheets if s not in xl.sheet_names]
    if missing:
        print(f"ERRO: Sheets ausentes no Excel: {missing}")
        print(f"  Sheets encontradas: {xl.sheet_names}")
        sys.exit(1)
    xl.close()

    # Garantir tabelas
    print("Verificando estrutura das tabelas...")
    with ENGINE.begin() as conn:
        ensure_tables(conn)

    # Carregar dados de cada sheet
    print("Carregando dados do Excel...")
    df_secoes = load_secoes(EXCEL_FILE)
    df_divisoes = load_divisoes(EXCEL_FILE)
    df_grupos = load_grupos(EXCEL_FILE)
    df_classes = load_classes(EXCEL_FILE)
    df_servicos = load_servicos(EXCEL_FILE)

    print(f"  SECAO:   {len(df_secoes)} registros")
    print(f"  DIVISAO: {len(df_divisoes)} registros")
    print(f"  GRUPO:   {len(df_grupos)} registros")
    print(f"  CLASSE:  {len(df_classes)} registros")
    print(f"  ITEM:    {len(df_servicos)} registros")
    print()

    # Inserir em ordem hierárquica (top-down)
    print("Inserindo dados no banco...")
    with ENGINE.begin() as conn:
        truncate_and_insert(conn, TBL_SECOES, df_secoes,
                            ['codigo_secao', 'nome', 'status', 'data_atualizacao'])

        truncate_and_insert(conn, TBL_DIVISOES, df_divisoes,
                            ['codigo_divisao', 'codigo_secao', 'nome'])

        truncate_and_insert(conn, TBL_GRUPOS, df_grupos,
                            ['codigo_grupo', 'codigo_divisao', 'nome', 'status', 'data_atualizacao'])

        truncate_and_insert(conn, TBL_CLASSES, df_classes,
                            ['codigo_classe', 'codigo_grupo', 'nome'])

        truncate_and_insert(conn, TBL_SERVICOS, df_servicos,
                            ['codigo_servico', 'codigo_classe', 'codigo_grupo', 'nome', 'status'])

    print()
    print(f"Importação concluída em {time.time() - t0:.2f}s")


if __name__ == '__main__':
    main()
