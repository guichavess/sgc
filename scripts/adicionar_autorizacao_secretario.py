"""
Script de migração: Adiciona colunas de Autorização do Secretário ao diarias_itinerario.

Colunas:
  - sei_id_autorizacao VARCHAR(50)     — ID do documento SEAD_AUTORIZACAO_DO_SECRETARIO
  - sei_autorizacao_formatado VARCHAR(50) — Número formatado do documento

Uso:
  python scripts/adicionar_autorizacao_secretario.py
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


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )


def column_exists(cursor, table, column):
    cursor.execute("""
        SELECT COUNT(*) as cnt
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
    """, (DB_NAME, table, column))
    return cursor.fetchone()['cnt'] > 0


def run_migration():
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 60)
    print("Migração: Autorização do Secretário")
    print("=" * 60)

    colunas = [
        ('sei_id_autorizacao', 'VARCHAR(50) NULL'),
        ('sei_autorizacao_formatado', 'VARCHAR(50) NULL'),
    ]

    for col_name, col_def in colunas:
        if column_exists(cursor, 'diarias_itinerario', col_name):
            print(f"  SKIP - coluna {col_name} já existe.")
        else:
            sql = f"ALTER TABLE diarias_itinerario ADD COLUMN {col_name} {col_def}"
            cursor.execute(sql)
            print(f"  OK - coluna {col_name} adicionada.")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("Migração concluída com sucesso!")
    print("=" * 60)


if __name__ == '__main__':
    run_migration()
