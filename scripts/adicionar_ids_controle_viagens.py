"""
Script de migração: Adiciona colunas de ID (cod_ibge) à tabela diarias_controle_viagens.

Novas colunas:
  - origem_id INT NULL      → cod_ibge do município (estadual) ou estado (nacional)
  - destino_id INT NULL     → cod_ibge do município (estadual) ou estado (nacional)
  - tipo_viagem SMALLINT NULL → 1=Estadual (IDs = municípios), 2=Nacional (IDs = estados)

Códigos IBGE de estados (2 dígitos: 11-53) e municípios (7 dígitos: 1100015+)
NÃO conflitam, mas o tipo_viagem permite distinguir a referência correta.

Uso:
  python scripts/adicionar_ids_controle_viagens.py               # DRY-RUN
  python scripts/adicionar_ids_controle_viagens.py --executar     # APLICA
"""
import os
import sys
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = os.getenv('DB_NAME', 'sgc')

DRY_RUN = '--executar' not in sys.argv


def get_connection():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor,
    )


def column_exists(cursor, table, column):
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (DB_NAME, table, column))
    return cursor.fetchone()['cnt'] > 0


def run():
    conn = get_connection()
    cursor = conn.cursor()

    modo = "DRY-RUN (use --executar para aplicar)" if DRY_RUN else "EXECUTANDO"
    print("=" * 65)
    print(f"Migração: IDs cod_ibge em diarias_controle_viagens — {modo}")
    print("=" * 65)

    table = 'diarias_controle_viagens'
    alterations = [
        ('origem_id', 'INT NULL COMMENT \'cod_ibge municipio (estadual) ou estado (nacional)\''),
        ('destino_id', 'INT NULL COMMENT \'cod_ibge municipio (estadual) ou estado (nacional)\''),
        ('tipo_viagem', 'SMALLINT NULL COMMENT \'1=Estadual, 2=Nacional\''),
    ]

    for col_name, col_def in alterations:
        if column_exists(cursor, table, col_name):
            print(f"  SKIP - {col_name} já existe.")
        else:
            sql = f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}"
            print(f"  {'(DRY-RUN) ' if DRY_RUN else 'OK   - '}{sql}")
            if not DRY_RUN:
                cursor.execute(sql)

    if not DRY_RUN:
        conn.commit()
        # Verificação
        cursor.execute(f"DESCRIBE {table}")
        cols = [r['Field'] for r in cursor.fetchall()]
        print(f"\n  Colunas atuais: {', '.join(cols)}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 65)
    print("Concluído!" + (" (DRY-RUN)" if DRY_RUN else ""))
    print("=" * 65)


if __name__ == '__main__':
    run()
