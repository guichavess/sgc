"""
Script de migracao: Adiciona etapas 7/8/9 e colunas pos-Analise NCI ao modulo de Diarias.

Etapas adicionadas (diarias_etapas):
  - id=7: Despacho APOIO (Superintendente -> DFIN)
  - id=8: Despacho Diretor (Diretor DFIN -> GEO)
  - id=9: Despacho GEO (GEO -> CCDP + NL/PD/OB)

Colunas adicionadas (diarias_itinerario): 18 colunas
  - ciencia_apoio, ciencia_apoio_data, sei_id_despacho_apoio, sei_despacho_apoio_formatado
  - ciencia_diretor, ciencia_diretor_data, sei_id_despacho_diretor, sei_despacho_diretor_formatado
  - ciencia_geo, ciencia_geo_data, sei_id_despacho_geo, sei_despacho_geo_formatado
  - nl_codigo, sei_id_nl, sei_nl_formatado
  - pd_codigo, sei_id_pd, sei_pd_formatado
  - ob_codigo, sei_id_ob, sei_ob_formatado

Uso:
  python scripts/migrar_etapas_pos_ne_2.py             (DRY-RUN)
  python scripts/migrar_etapas_pos_ne_2.py --executar   (aplica de verdade)
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
    print(f"Migracao 2: Etapas pos-Analise NCI (Diarias) - {modo}")
    print("=" * 60)

    # =========================================================================
    # PARTE 1: Novas etapas em diarias_etapas
    # =========================================================================
    novas_etapas = [
        (7, 'Despacho APOIO', 'despacho_apoio', 7, '#1565C0', 'fas fa-user-tie'),
        (8, 'Despacho Diretor', 'despacho_diretor', 8, '#0D47A1', 'fas fa-briefcase'),
        (9, 'Despacho GEO', 'despacho_geo', 9, '#004D40', 'fas fa-building'),
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
        # Despacho APOIO/DFIN (Superintendente)
        ('ciencia_apoio',                'TINYINT(1) NOT NULL DEFAULT 0'),
        ('ciencia_apoio_data',           'DATETIME NULL'),
        ('sei_id_despacho_apoio',        'VARCHAR(50) NULL'),
        ('sei_despacho_apoio_formatado', 'VARCHAR(50) NULL'),
        # Despacho Diretor DFIN
        ('ciencia_diretor',              'TINYINT(1) NOT NULL DEFAULT 0'),
        ('ciencia_diretor_data',         'DATETIME NULL'),
        ('sei_id_despacho_diretor',      'VARCHAR(50) NULL'),
        ('sei_despacho_diretor_formatado', 'VARCHAR(50) NULL'),
        # Despacho GEO
        ('ciencia_geo',                  'TINYINT(1) NOT NULL DEFAULT 0'),
        ('ciencia_geo_data',             'DATETIME NULL'),
        ('sei_id_despacho_geo',          'VARCHAR(50) NULL'),
        ('sei_despacho_geo_formatado',   'VARCHAR(50) NULL'),
        # NL
        ('nl_codigo',                    'VARCHAR(50) NULL'),
        ('sei_id_nl',                    'VARCHAR(50) NULL'),
        ('sei_nl_formatado',             'VARCHAR(50) NULL'),
        # PD
        ('pd_codigo',                    'VARCHAR(50) NULL'),
        ('sei_id_pd',                    'VARCHAR(50) NULL'),
        ('sei_pd_formatado',             'VARCHAR(50) NULL'),
        # OB
        ('ob_codigo',                    'VARCHAR(50) NULL'),
        ('sei_id_ob',                    'VARCHAR(50) NULL'),
        ('sei_ob_formatado',             'VARCHAR(50) NULL'),
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

    # Verificacao final
    print("\n-- Estado final das etapas --")
    cursor.execute("SELECT id, nome, alias, ordem, cor_hex, icone FROM diarias_etapas ORDER BY id")
    for e in cursor.fetchall():
        print(f"  id={e['id']}: {e['nome']} (alias={e['alias']}, ordem={e['ordem']})")

    cursor.close()
    conn.close()
    print("\n" + "=" * 60)


if __name__ == '__main__':
    run_migration()
