"""
Script de migração: Adiciona colunas para Autorização SCDP e Nota de Empenho
na tabela diarias_itinerario.

Novas colunas:
  - sei_id_autorizacao_scdp VARCHAR(50)       → ID do doc externo SCDP no SEI
  - sei_autorizacao_scdp_formatado VARCHAR(50) → Número formatado do doc SCDP
  - nota_empenho_codigo VARCHAR(50)            → Código da Nota de Empenho
  - sei_id_nota_empenho VARCHAR(50)            → ID do doc NE no SEI
  - sei_nota_empenho_formatado VARCHAR(50)     → Número formatado do doc NE

Uso:
  python scripts/adicionar_scdp_nota_empenho.py               # DRY-RUN
  python scripts/adicionar_scdp_nota_empenho.py --executar     # APLICA
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
    print(f"Migração: SCDP + Nota de Empenho — {modo}")
    print("=" * 65)

    table = 'diarias_itinerario'
    alterations = [
        ('sei_id_autorizacao_scdp', "VARCHAR(50) NULL COMMENT 'ID doc externo SCDP no SEI'"),
        ('sei_autorizacao_scdp_formatado', "VARCHAR(50) NULL COMMENT 'Número formatado doc SCDP'"),
        ('nota_empenho_codigo', "VARCHAR(50) NULL COMMENT 'Código da Nota de Empenho'"),
        ('sei_id_nota_empenho', "VARCHAR(50) NULL COMMENT 'ID doc NE no SEI'"),
        ('sei_nota_empenho_formatado', "VARCHAR(50) NULL COMMENT 'Número formatado doc NE'"),
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
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
        total = cursor.fetchone()['cnt']
        print(f"\n  Total registros na tabela: {total}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 65)
    print("Concluído!" + (" (DRY-RUN)" if DRY_RUN else ""))
    print("=" * 65)


if __name__ == '__main__':
    run()
