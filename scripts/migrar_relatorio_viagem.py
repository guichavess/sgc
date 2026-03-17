"""
Script de migracao: Adiciona colunas do Relatorio de Viagem ao modulo de Diarias.

Colunas adicionadas (diarias_itinerario):
  - sei_id_relatorio_viagem       VARCHAR(50) NULL
  - sei_relatorio_viagem_formatado VARCHAR(50) NULL

Uso:
  python scripts/migrar_relatorio_viagem.py             (DRY-RUN)
  python scripts/migrar_relatorio_viagem.py --executar   (aplica de verdade)
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

    modo = "DRY-RUN (use --executar para aplicar)" if DRY_RUN else "EXECUTANDO"
    print("=" * 60)
    print(f"Migracao: Relatorio de Viagem (Diarias) - {modo}")
    print("=" * 60)

    colunas = [
        ('sei_id_relatorio_viagem',        'VARCHAR(50) NULL'),
        ('sei_relatorio_viagem_formatado', 'VARCHAR(50) NULL'),
    ]

    print(f"\n-- Colunas ({len(colunas)}) --")
    for col_name, col_def in colunas:
        if column_exists(cursor, 'diarias_itinerario', col_name):
            print(f"  SKIP - coluna {col_name} ja existe.")
        else:
            sql = f"ALTER TABLE diarias_itinerario ADD COLUMN {col_name} {col_def}"
            if DRY_RUN:
                print(f"  [DRY-RUN] ADD COLUMN {col_name} {col_def}")
            else:
                cursor.execute(sql)
                print(f"  OK - coluna {col_name} adicionada.")

    if DRY_RUN:
        print("\n[!] DRY-RUN: nenhuma alteracao foi aplicada.")
        print("  Execute com --executar para aplicar.")
        conn.rollback()
    else:
        conn.commit()
        print("\n[OK] Migracao aplicada com sucesso!")

    cursor.close()
    conn.close()
    print("\n" + "=" * 60)


if __name__ == '__main__':
    run_migration()
