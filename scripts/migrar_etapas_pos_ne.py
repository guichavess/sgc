"""
Script de migração: Adiciona etapas e colunas pós-NE ao módulo de Diárias.

Etapas adicionadas (diarias_etapas):
  - id=4: Despacho CCDP (pós NE, envio à SGA)
  - id=5: Ciência SGA (Superintendente + Despacho SGA → NCI)
  - id=6: Análise NCI (Análise de Pagamento + Despacho NCI)

Colunas adicionadas (diarias_itinerario):
  - sei_id_despacho_ccdp, sei_despacho_ccdp_formatado
  - ciencia_superintendente, ciencia_superintendente_data
  - sei_id_despacho_sga, sei_despacho_sga_formatado
  - ciencia_nci, ciencia_nci_data
  - analise_pagamento_respostas, analise_pagamento_observacoes
  - sei_id_analise_pagamento, sei_analise_pagamento_formatado
  - sei_id_despacho_nci, sei_despacho_nci_formatado

Uso:
  python scripts/migrar_etapas_pos_ne.py             (DRY-RUN)
  python scripts/migrar_etapas_pos_ne.py --executar   (aplica de verdade)
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
    cursor.execute("SELECT COUNT(*) as cnt FROM diarias_etapas WHERE id = %s", (etapa_id,))
    return cursor.fetchone()['cnt'] > 0


def run_migration():
    conn = get_connection()
    cursor = conn.cursor()

    modo = "DRY-RUN (use --executar para aplicar)" if DRY_RUN else "EXECUTANDO"
    print("=" * 60)
    print(f"Migracao: Etapas e colunas pos-NE (Diarias) - {modo}")
    print("=" * 60)

    # =========================================================================
    # PARTE 1: Novas etapas em diarias_etapas
    # =========================================================================
    novas_etapas = [
        (4, 'Despacho CCDP', 'despacho_ccdp', 4, '#17a2b8', 'fas fa-file-signature'),
        (5, 'Ciência SGA', 'ciencia_sga', 5, '#20c997', 'fas fa-check-double'),
        (6, 'Análise NCI', 'analise_nci', 6, '#e83e8c', 'fas fa-clipboard-check'),
    ]

    print("\n-- PARTE 1: Etapas --")
    for etapa_id, nome, alias, ordem, cor, icone in novas_etapas:
        if etapa_exists(cursor, etapa_id):
            print(f"  SKIP - etapa id={etapa_id} ({nome}) ja existe.")
        else:
            sql = """
                INSERT INTO diarias_etapas (id, nome, alias, ordem, cor_hex, icone)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            if DRY_RUN:
                print(f"  [DRY-RUN] INSERT id={etapa_id}: {nome} (alias={alias}, cor={cor})")
            else:
                cursor.execute(sql, (etapa_id, nome, alias, ordem, cor, icone))
                print(f"  OK - INSERT id={etapa_id}: {nome}")

    # =========================================================================
    # PARTE 2: Novas colunas em diarias_itinerario
    # =========================================================================
    colunas = [
        # Despacho CCDP → SGA
        ('sei_id_despacho_ccdp',          'VARCHAR(50) NULL'),
        ('sei_despacho_ccdp_formatado',   'VARCHAR(50) NULL'),
        # Ciência Superintendente + Despacho SGA → NCI
        ('ciencia_superintendente',       'TINYINT(1) NOT NULL DEFAULT 0'),
        ('ciencia_superintendente_data',  'DATETIME NULL'),
        ('sei_id_despacho_sga',           'VARCHAR(50) NULL'),
        ('sei_despacho_sga_formatado',    'VARCHAR(50) NULL'),
        # Análise NCI + Despacho NCI
        ('ciencia_nci',                   'TINYINT(1) NOT NULL DEFAULT 0'),
        ('ciencia_nci_data',              'DATETIME NULL'),
        ('analise_pagamento_respostas',   'TEXT NULL'),
        ('analise_pagamento_observacoes', 'TEXT NULL'),
        ('sei_id_analise_pagamento',      'VARCHAR(50) NULL'),
        ('sei_analise_pagamento_formatado', 'VARCHAR(50) NULL'),
        ('sei_id_despacho_nci',           'VARCHAR(50) NULL'),
        ('sei_despacho_nci_formatado',    'VARCHAR(50) NULL'),
    ]

    print(f"\n-- PARTE 2: Colunas ({len(colunas)}) --")
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

    # =========================================================================
    # COMMIT
    # =========================================================================
    if DRY_RUN:
        print("\n[!] DRY-RUN: nenhuma alteracao foi aplicada.")
        print("  Execute com --executar para aplicar.")
        conn.rollback()
    else:
        conn.commit()
        print("\n[OK] Migracao aplicada com sucesso!")

    # Verificação final
    print("\n-- Estado final das etapas --")
    cursor.execute("SELECT id, nome, alias, ordem, cor_hex, icone FROM diarias_etapas ORDER BY id")
    for e in cursor.fetchall():
        print(f"  id={e['id']}: {e['nome']} (alias={e['alias']}, ordem={e['ordem']})")

    cursor.close()
    conn.close()
    print("\n" + "=" * 60)


if __name__ == '__main__':
    run_migration()
