"""
Script de migracao: Adiciona colunas pos-OB ao modulo de Diarias.

Colunas adicionadas (diarias_itinerario):
  - sei_id_relatorio_viagem           VARCHAR(50) NULL  (IdSerie 1908)
  - sei_relatorio_viagem_formatado    VARCHAR(50) NULL
  - sei_id_comprovante_viagem         VARCHAR(50) NULL  (IdSerie 35)
  - sei_comprovante_viagem_formatado  VARCHAR(50) NULL
  - np_codigo                         VARCHAR(50) NULL  (IdSerie 423)
  - sei_id_np                         VARCHAR(50) NULL
  - sei_np_formatado                  VARCHAR(50) NULL
  - sei_id_prestacao_scdp             VARCHAR(50) NULL  (IdSerie 264)
  - sei_prestacao_scdp_formatado      VARCHAR(50) NULL
  - sei_id_despacho_final             VARCHAR(50) NULL  (IdSerie 754)
  - sei_despacho_final_formatado      VARCHAR(50) NULL

Etapas adicionadas (diarias_etapas):
  - 10: Comprovante de Viagem
  - 11: Prestacao de Contas CCDP

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


def etapa_exists(cursor, etapa_id):
    cursor.execute(
        "SELECT COUNT(*) as cnt FROM diarias_etapas WHERE id = %s", (etapa_id,)
    )
    return cursor.fetchone()['cnt'] > 0


def run_migration():
    conn = get_connection()
    cursor = conn.cursor()

    modo = "DRY-RUN (use --executar para aplicar)" if DRY_RUN else "EXECUTANDO"
    print("=" * 70)
    print(f"Migracao: Relatorio + Comprovante + Prestacao Contas - {modo}")
    print("=" * 70)

    # ── 1. Colunas ──
    colunas = [
        ('sei_id_relatorio_viagem',        'VARCHAR(50) NULL'),
        ('sei_relatorio_viagem_formatado', 'VARCHAR(50) NULL'),
        ('sei_id_comprovante_viagem',      'VARCHAR(50) NULL'),
        ('sei_comprovante_viagem_formatado', 'VARCHAR(50) NULL'),
        ('np_codigo',                      'VARCHAR(50) NULL'),
        ('sei_id_np',                      'VARCHAR(50) NULL'),
        ('sei_np_formatado',               'VARCHAR(50) NULL'),
        ('sei_id_prestacao_scdp',          'VARCHAR(50) NULL'),
        ('sei_prestacao_scdp_formatado',   'VARCHAR(50) NULL'),
        ('sei_id_despacho_final',          'VARCHAR(50) NULL'),
        ('sei_despacho_final_formatado',   'VARCHAR(50) NULL'),
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

    # ── 2. Etapas ──
    etapas = [
        (10, 'Comprovante de Viagem', 'comprovante_viagem', 10, '#198754', 'bi-file-earmark-pdf'),
        (11, 'Prestacao de Contas CCDP', 'prestacao_contas_ccdp', 11, '#dc3545', 'bi-clipboard-check'),
    ]

    print(f"\n-- Etapas ({len(etapas)}) --")
    for eid, nome, alias, ordem, cor, icone in etapas:
        if etapa_exists(cursor, eid):
            print(f"  SKIP - etapa {eid} ({nome}) ja existe.")
        else:
            sql = ("INSERT INTO diarias_etapas (id, nome, alias, ordem, cor_hex, icone) "
                   "VALUES (%s, %s, %s, %s, %s, %s)")
            if DRY_RUN:
                print(f"  [DRY-RUN] INSERT etapa {eid}: {nome}")
            else:
                cursor.execute(sql, (eid, nome, alias, ordem, cor, icone))
                print(f"  OK - etapa {eid} ({nome}) inserida.")

    if DRY_RUN:
        print("\n[!] DRY-RUN: nenhuma alteracao foi aplicada.")
        print("  Execute com --executar para aplicar.")
        conn.rollback()
    else:
        conn.commit()
        print("\n[OK] Migracao aplicada com sucesso!")

    cursor.close()
    conn.close()
    print("\n" + "=" * 70)


if __name__ == '__main__':
    run_migration()
