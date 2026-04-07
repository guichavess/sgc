"""
Script de migração: Normalizar diarias_itinerario_sei → documentos + quadro orçamentário.

Fase A (default): Cria diarias_itinerario_documentos + diarias_itinerario_quadro_orcamentario
                   e migra dados de diarias_itinerario_sei (ou diarias_itinerario se sei não existe).
Fase B (--drop-sei-table): Remove tabela diarias_itinerario_sei após verificação.
Fase C (--drop-columns): Remove colunas sei_* antigas de diarias_itinerario.

Uso:
  python scripts/migrar_diarias_sei.py                   # cria tabelas + migra dados
  python scripts/migrar_diarias_sei.py --dry-run         # mostra SQL sem executar
  python scripts/migrar_diarias_sei.py --drop-sei-table  # remove diarias_itinerario_sei
  python scripts/migrar_diarias_sei.py --drop-columns    # remove colunas antigas de diarias_itinerario
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db

# Mapeamento: tipo_documento → (coluna_sei_id, coluna_formatado, coluna_codigo)
MAPA_TIPO_COLUNAS = {
    'memorando':            ('sei_id_memorando', 'sei_memorando_formatado', None),
    'requisicao':           ('sei_id_requisicao', 'sei_requisicao_formatado', None),
    'requisicao_passagens': ('sei_id_requisicao_passagens', 'sei_requisicao_passagens_formatado', None),
    'doc_externo':          ('sei_id_doc_externo', 'sei_doc_externo_formatado', None),
    'autorizacao':          ('sei_id_autorizacao', 'sei_autorizacao_formatado', None),
    'despacho_dfin':        ('sei_id_despacho_dfin', 'sei_despacho_dfin_formatado', None),
    'nota_reserva':         ('sei_id_nota_reserva', 'sei_nota_reserva_formatado', 'nota_reserva'),
    'quadro_orcamentario':  ('sei_id_quadro_orcamentario', 'sei_quadro_orcamentario_formatado', None),
    'escolha_passagens':    ('sei_id_escolha_passagens', 'sei_escolha_passagens_formatado', None),
    'memorando_cotacoes':   ('sei_id_memorando_cotacoes', 'sei_memorando_cotacoes_formatado', None),
    'autorizacao_scdp':     ('sei_id_autorizacao_scdp', 'sei_autorizacao_scdp_formatado', None),
    'nota_empenho':         ('sei_id_nota_empenho', 'sei_nota_empenho_formatado', 'nota_empenho_codigo'),
    'despacho_ccdp':        ('sei_id_despacho_ccdp', 'sei_despacho_ccdp_formatado', None),
    'despacho_sga':         ('sei_id_despacho_sga', 'sei_despacho_sga_formatado', None),
    'analise_pagamento':    ('sei_id_analise_pagamento', 'sei_analise_pagamento_formatado', None),
    'despacho_nci':         ('sei_id_despacho_nci', 'sei_despacho_nci_formatado', None),
    'despacho_apoio':       ('sei_id_despacho_apoio', 'sei_despacho_apoio_formatado', None),
    'despacho_diretor':     ('sei_id_despacho_diretor', 'sei_despacho_diretor_formatado', None),
    'despacho_geo':         ('sei_id_despacho_geo', 'sei_despacho_geo_formatado', None),
    'nl':                   ('sei_id_nl', 'sei_nl_formatado', 'nl_codigo'),
    'pd':                   ('sei_id_pd', 'sei_pd_formatado', 'pd_codigo'),
    'ob':                   ('sei_id_ob', 'sei_ob_formatado', 'ob_codigo'),
    'relatorio_viagem':     ('sei_id_relatorio_viagem', 'sei_relatorio_viagem_formatado', None),
    'comprovante_viagem':   ('sei_id_comprovante_viagem', 'sei_comprovante_viagem_formatado', None),
    'np':                   ('sei_id_np', 'sei_np_formatado', 'np_codigo'),
    'prestacao_scdp':       ('sei_id_prestacao_scdp', 'sei_prestacao_scdp_formatado', None),
    'despacho_final':       ('sei_id_despacho_final', 'sei_despacho_final_formatado', None),
}

# Colunas do quadro orçamentário (nome antigo → nome novo)
QUADRO_COLUNAS = {
    'quadro_ug': 'ug',
    'quadro_funcao': 'funcao',
    'quadro_subfuncao': 'subfuncao',
    'quadro_programa': 'programa',
    'quadro_plano_interno': 'plano_interno',
    'quadro_fonte_recursos': 'fonte_recursos',
    'quadro_natureza_despesa': 'natureza_despesa',
    'quadro_valor_inicial_nr': 'valor_inicial_nr',
    'quadro_saldo_nr': 'saldo_nr',
    'quadro_valor_despesa': 'valor_despesa',
    'quadro_saldo_atual_nr': 'saldo_atual_nr',
}

# Todas as colunas antigas que devem ser removidas de diarias_itinerario
COLUNAS_ANTIGAS = []
for col_id, col_fmt, col_cod in MAPA_TIPO_COLUNAS.values():
    COLUNAS_ANTIGAS.append(col_id)
    COLUNAS_ANTIGAS.append(col_fmt)
    if col_cod:
        COLUNAS_ANTIGAS.append(col_cod)
COLUNAS_ANTIGAS.extend(QUADRO_COLUNAS.keys())


def tabela_existe(cursor, db_name, tabela):
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """, (db_name, tabela))
    return cursor.fetchone()[0] > 0


def coluna_existe(cursor, db_name, tabela, coluna):
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (db_name, tabela, coluna))
    return cursor.fetchone()[0] > 0


def normalizar(dry_run=False):
    """Cria diarias_itinerario_documentos + diarias_itinerario_quadro_orcamentario e migra dados."""
    conn = db.engine.raw_connection()
    cursor = conn.cursor()
    db_name = db.engine.url.database

    try:
        # ── 1. Determinar tabela fonte ──────────────────────────────
        if tabela_existe(cursor, db_name, 'diarias_itinerario_sei'):
            tabela_fonte = 'diarias_itinerario_sei'
            col_id_fonte = 'itinerario_id'
        else:
            tabela_fonte = 'diarias_itinerario'
            col_id_fonte = 'id'
        print(f"Tabela fonte: {tabela_fonte}")

        # ── 2. Criar diarias_itinerario_documentos ──────────────────
        if tabela_existe(cursor, db_name, 'diarias_itinerario_documentos'):
            cursor.execute("SELECT COUNT(*) FROM diarias_itinerario_documentos")
            count = cursor.fetchone()[0]
            print(f"Tabela diarias_itinerario_documentos já existe ({count} registros). Pulando.")
            docs_existe = True
        else:
            docs_existe = False
            create_docs_sql = """CREATE TABLE `diarias_itinerario_documentos` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `itinerario_id` INT NOT NULL,
    `tipo_documento` VARCHAR(50) NOT NULL,
    `sei_id` VARCHAR(50) NULL,
    `sei_formatado` VARCHAR(50) NULL,
    `codigo` VARCHAR(50) NULL,
    UNIQUE KEY `uq_itin_tipo_doc` (`itinerario_id`, `tipo_documento`),
    INDEX `ix_itin_doc_itinerario_id` (`itinerario_id`),
    CONSTRAINT `fk_itin_doc_itinerario` FOREIGN KEY (`itinerario_id`)
        REFERENCES `diarias_itinerario` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""

            print("\n" + "=" * 70)
            print("CREATE TABLE diarias_itinerario_documentos")
            print("=" * 70)
            if dry_run:
                print(create_docs_sql)
            else:
                cursor.execute(create_docs_sql)
                conn.commit()
                print("Tabela criada.")

        # ── 3. Criar diarias_itinerario_quadro_orcamentario ──────────
        if tabela_existe(cursor, db_name, 'diarias_itinerario_quadro_orcamentario'):
            cursor.execute("SELECT COUNT(*) FROM diarias_itinerario_quadro_orcamentario")
            count = cursor.fetchone()[0]
            print(f"Tabela diarias_itinerario_quadro_orcamentario já existe ({count} registros). Pulando.")
            quadro_existe = True
        else:
            quadro_existe = False
            create_quadro_sql = """CREATE TABLE `diarias_itinerario_quadro_orcamentario` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `itinerario_id` INT NOT NULL,
    `ug` VARCHAR(20) NULL,
    `funcao` VARCHAR(10) NULL,
    `subfuncao` VARCHAR(10) NULL,
    `programa` VARCHAR(10) NULL,
    `plano_interno` VARCHAR(10) NULL,
    `fonte_recursos` VARCHAR(20) NULL,
    `natureza_despesa` VARCHAR(20) NULL,
    `valor_inicial_nr` DECIMAL(14,2) NULL,
    `saldo_nr` DECIMAL(14,2) NULL,
    `valor_despesa` DECIMAL(14,2) NULL,
    `saldo_atual_nr` DECIMAL(14,2) NULL,
    UNIQUE KEY `uq_quadro_itinerario_id` (`itinerario_id`),
    CONSTRAINT `fk_quadro_itinerario` FOREIGN KEY (`itinerario_id`)
        REFERENCES `diarias_itinerario` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""

            print("\n" + "=" * 70)
            print("CREATE TABLE diarias_itinerario_quadro_orcamentario")
            print("=" * 70)
            if dry_run:
                print(create_quadro_sql)
            else:
                cursor.execute(create_quadro_sql)
                conn.commit()
                print("Tabela criada.")

        if dry_run and (docs_existe and quadro_existe):
            print("\n[DRY-RUN] Ambas as tabelas já existem.")
            return

        # ── 4. Migrar dados: documentos ─────────────────────────────
        if not docs_existe:
            print("\n" + "=" * 70)
            print("MIGRANDO DOCUMENTOS SEI")
            print("=" * 70)

            # Verificar quais colunas existem na fonte
            total_inseridos = 0
            for tipo, (col_id, col_fmt, col_cod) in MAPA_TIPO_COLUNAS.items():
                cols_select = []
                cols_names = []

                if coluna_existe(cursor, db_name, tabela_fonte, col_id):
                    cols_select.append(f'`{col_id}`')
                    cols_names.append('sei_id')
                if coluna_existe(cursor, db_name, tabela_fonte, col_fmt):
                    cols_select.append(f'`{col_fmt}`')
                    cols_names.append('sei_formatado')
                if col_cod and coluna_existe(cursor, db_name, tabela_fonte, col_cod):
                    cols_select.append(f'`{col_cod}`')
                    cols_names.append('codigo')

                if not cols_select:
                    continue

                # Monta condição WHERE: pelo menos um campo não-NULL
                where_parts = [f'`{c.strip("`")}` IS NOT NULL' for c in cols_select]
                where_clause = ' OR '.join(where_parts)

                insert_sql = f"""INSERT INTO `diarias_itinerario_documentos`
    (`itinerario_id`, `tipo_documento`, {', '.join(f'`{n}`' for n in cols_names)})
SELECT `{col_id_fonte}`, '{tipo}', {', '.join(cols_select)}
FROM `{tabela_fonte}`
WHERE {where_clause}"""

                if dry_run:
                    print(f"\n-- {tipo}:")
                    print(insert_sql)
                else:
                    cursor.execute(insert_sql)
                    rows = cursor.rowcount
                    if rows > 0:
                        total_inseridos += rows
                        print(f"  {tipo}: {rows} registros")

            if not dry_run:
                conn.commit()
                print(f"\nTotal documentos inseridos: {total_inseridos}")

        # ── 5. Migrar dados: quadro orçamentário ────────────────────
        if not quadro_existe:
            print("\n" + "=" * 70)
            print("MIGRANDO QUADRO ORÇAMENTÁRIO")
            print("=" * 70)

            cols_fonte = []
            cols_destino = []
            for col_antiga, col_nova in QUADRO_COLUNAS.items():
                if coluna_existe(cursor, db_name, tabela_fonte, col_antiga):
                    cols_fonte.append(f'`{col_antiga}`')
                    cols_destino.append(f'`{col_nova}`')

            if cols_fonte:
                where_parts = [f'{c} IS NOT NULL' for c in cols_fonte]
                where_clause = ' OR '.join(where_parts)

                insert_quadro_sql = f"""INSERT INTO `diarias_itinerario_quadro_orcamentario`
    (`itinerario_id`, {', '.join(cols_destino)})
SELECT `{col_id_fonte}`, {', '.join(cols_fonte)}
FROM `{tabela_fonte}`
WHERE {where_clause}"""

                if dry_run:
                    print(insert_quadro_sql)
                else:
                    cursor.execute(insert_quadro_sql)
                    rows = cursor.rowcount
                    conn.commit()
                    print(f"Quadros orçamentários inseridos: {rows}")
            else:
                print("Nenhuma coluna de quadro encontrada na fonte.")

        # ── 6. Verificação ──────────────────────────────────────────
        if not dry_run:
            print("\n" + "=" * 70)
            print("VERIFICAÇÃO")
            print("=" * 70)

            cursor.execute("SELECT COUNT(*) FROM diarias_itinerario_documentos")
            count_docs = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT itinerario_id) FROM diarias_itinerario_documentos")
            distinct_docs = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM diarias_itinerario_quadro_orcamentario")
            count_quadro = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM `{tabela_fonte}`")
            count_fonte = cursor.fetchone()[0]

            print(f"  Registros na fonte ({tabela_fonte}): {count_fonte}")
            print(f"  Documentos SEI: {count_docs} registros, {distinct_docs} itinerários distintos")
            print(f"  Quadros orçamentários: {count_quadro} registros")

            # Spot-check: requisicao
            col_check = 'sei_id_requisicao'
            if coluna_existe(cursor, db_name, tabela_fonte, col_check):
                cursor.execute(f"""
                    SELECT COUNT(*) FROM `{tabela_fonte}` s
                    LEFT JOIN diarias_itinerario_documentos d
                        ON d.itinerario_id = s.`{col_id_fonte}` AND d.tipo_documento = 'requisicao'
                    WHERE s.`{col_check}` IS NOT NULL
                      AND (d.sei_id IS NULL
                           OR COALESCE(s.`{col_check}`, '') COLLATE utf8mb4_unicode_ci
                              != COALESCE(d.sei_id, '') COLLATE utf8mb4_unicode_ci)
                """)
                diffs = cursor.fetchone()[0]
                if diffs == 0:
                    print("  OK: Spot-check requisicao — dados conferem.")
                else:
                    print(f"  AVISO: {diffs} registros com requisicao divergente!")

            print("\nNormalização concluída com sucesso!")
        else:
            print("\n[DRY-RUN] Nenhuma alteração feita.")

    finally:
        cursor.close()
        conn.close()


def drop_sei_table(dry_run=False):
    """Remove tabela diarias_itinerario_sei após verificação."""
    conn = db.engine.raw_connection()
    cursor = conn.cursor()
    db_name = db.engine.url.database

    try:
        if not tabela_existe(cursor, db_name, 'diarias_itinerario_sei'):
            print("Tabela diarias_itinerario_sei não existe (já removida?).")
            return

        if not tabela_existe(cursor, db_name, 'diarias_itinerario_documentos'):
            print("ERRO: diarias_itinerario_documentos não existe. Rode a normalização primeiro.")
            return

        # Verificação: todos os registros da sei foram migrados?
        cursor.execute("SELECT COUNT(*) FROM diarias_itinerario_sei")
        count_sei = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT itinerario_id) FROM diarias_itinerario_documentos")
        distinct_docs = cursor.fetchone()[0]

        print(f"diarias_itinerario_sei: {count_sei} registros")
        print(f"diarias_itinerario_documentos: {distinct_docs} itinerários distintos")

        if distinct_docs < count_sei:
            print(f"AVISO: Menos itinerários em documentos ({distinct_docs}) do que em sei ({count_sei}).")
            print("Verifique se a migração foi completa antes de continuar.")

        sql = "DROP TABLE `diarias_itinerario_sei`"
        if dry_run:
            print(f"\n[DRY-RUN] {sql}")
        else:
            print(f"\nExecutando: {sql}")
            cursor.execute(sql)
            conn.commit()
            print("Tabela diarias_itinerario_sei removida com sucesso.")

    finally:
        cursor.close()
        conn.close()


def drop_columns(dry_run=False):
    """Remove colunas antigas de diarias_itinerario."""
    conn = db.engine.raw_connection()
    cursor = conn.cursor()
    db_name = db.engine.url.database

    try:
        if not tabela_existe(cursor, db_name, 'diarias_itinerario_documentos'):
            print("ERRO: diarias_itinerario_documentos não existe. Rode a normalização primeiro.")
            return

        colunas_para_dropar = []
        for col in COLUNAS_ANTIGAS:
            if coluna_existe(cursor, db_name, 'diarias_itinerario', col):
                colunas_para_dropar.append(col)

        if not colunas_para_dropar:
            print("Nenhuma coluna antiga para remover (já foram removidas?).")
            return

        print(f"Removendo {len(colunas_para_dropar)} colunas de diarias_itinerario...")

        for col in colunas_para_dropar:
            sql = f"ALTER TABLE `diarias_itinerario` DROP COLUMN `{col}`"
            if dry_run:
                print(f"  [DRY-RUN] {sql}")
            else:
                print(f"  DROP COLUMN {col}...")
                cursor.execute(sql)

        if not dry_run:
            conn.commit()
            print(f"\n{len(colunas_para_dropar)} colunas removidas com sucesso.")
        else:
            print(f"\n[DRY-RUN] Nenhuma alteração feita.")

    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Normalizar documentos SEI para tabelas separadas')
    parser.add_argument('--drop-sei-table', action='store_true',
                        help='Remove tabela diarias_itinerario_sei')
    parser.add_argument('--drop-columns', action='store_true',
                        help='Remove colunas antigas de diarias_itinerario')
    parser.add_argument('--dry-run', action='store_true',
                        help='Mostra SQL sem executar')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        if args.drop_sei_table:
            drop_sei_table(dry_run=args.dry_run)
        elif args.drop_columns:
            drop_columns(dry_run=args.dry_run)
        else:
            normalizar(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
