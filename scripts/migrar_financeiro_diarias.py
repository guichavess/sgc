"""
Script de migração: Adiciona colunas de Nota de Reserva ao diarias_itinerario.

Colunas adicionadas:
  - nota_reserva: código da NR inserida pelo financeiro
  - sei_id_nota_reserva: ID do documento no SEI
  - sei_nota_reserva_formatado: número formatado do doc SEI

Uso:
  python scripts/migrar_financeiro_diarias.py
"""
import os
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
    print("Migração: Colunas de Nota de Reserva (Financeiro/Diárias)")
    print("=" * 60)

    colunas = [
        ('nota_reserva', 'VARCHAR(50) NULL'),
        ('sei_id_nota_reserva', 'VARCHAR(50) NULL'),
        ('sei_nota_reserva_formatado', 'VARCHAR(50) NULL'),
    ]

    for i, (col_name, col_def) in enumerate(colunas, 1):
        print(f"\n[{i}/{len(colunas)}] Verificando coluna {col_name}...")
        if column_exists(cursor, 'diarias_itinerario', col_name):
            print(f"   SKIP - coluna {col_name} já existe.")
        else:
            cursor.execute(f"ALTER TABLE diarias_itinerario ADD COLUMN {col_name} {col_def}")
            print(f"   OK - coluna {col_name} adicionada.")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("Migração concluída com sucesso!")
    print("=" * 60)


if __name__ == '__main__':
    run_migration()
