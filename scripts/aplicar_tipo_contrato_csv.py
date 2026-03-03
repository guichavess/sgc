'''
Aplicar Tipo de Contrato via CSV
=================================
DRY-RUN por padrao. Use --executar para aplicar.

Fases:
  1. ALTER TABLE: adiciona coluna tipo_contrato em contratos
  2. UPDATE tipo_contrato a partir do CSV (S, M, SM)
  3. Limpar tipificacoes inconsistentes:
     - Contrato tipo S com catmat_* preenchido -> zerar catmat
     - Contrato tipo M com catserv_* preenchido -> zerar catserv
  4. Relatorio

Uso:
  python scripts/aplicar_tipo_contrato_csv.py              # dry-run
  python scripts/aplicar_tipo_contrato_csv.py --executar   # aplica
'''
import os
import sys
import csv
from datetime import datetime

import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DB = dict(
    host=os.getenv('DB_HOST', 'localhost'),
    user=os.getenv('DB_USER', 'root'),
    password=os.getenv('DB_PASS', ''),
    database=os.getenv('DB_NAME', 'sgc'),
    charset='utf8mb4'
)

EXECUTAR = '--executar' in sys.argv
CSV_PATH = os.path.join(BASE_DIR, 'tipificacao contratos.csv')

MAPA_TIPO = {
    's': 'S',
    'm': 'M',
    'sm': 'SM',
}


def main():
    modo = 'EXECUTAR' if EXECUTAR else 'DRY-RUN'
    print('Aplicar Tipo de Contrato via CSV [%s]' % modo)
    print('=' * 70)

    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    # ── Fase 0: Ler CSV ─────────────────────────────────────────────
    print('\n[0/4] Lendo CSV...')
    csv_data = {}
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            cod = row['Contrato'].strip()
            tipo = row['Tipo'].strip().lower()
            if cod and tipo and cod != 'Total Geral':
                if tipo in MAPA_TIPO:
                    csv_data[cod] = MAPA_TIPO[tipo]
                else:
                    print('  AVISO: tipo desconhecido "%s" para contrato %s' % (tipo, cod))

    print('  %d contratos no CSV' % len(csv_data))
    print('    S:  %d' % sum(1 for v in csv_data.values() if v == 'S'))
    print('    M:  %d' % sum(1 for v in csv_data.values() if v == 'M'))
    print('    SM: %d' % sum(1 for v in csv_data.values() if v == 'SM'))

    # Verificar quais existem no DB
    existentes = set()
    for cod in csv_data:
        cur.execute('SELECT codigo FROM contratos WHERE codigo = %s', (cod,))
        if cur.fetchone():
            existentes.add(cod)

    nao_encontrados = sorted(set(csv_data.keys()) - existentes)
    print('  %d existem no DB, %d nao encontrados' % (len(existentes), len(nao_encontrados)))
    if nao_encontrados:
        print('  Nao encontrados: %s' % nao_encontrados)

    # ── Fase 1: Schema ──────────────────────────────────────────────
    print('\n[1/4] Schema Migration...')
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'contratos' AND COLUMN_NAME = 'tipo_contrato'
    """, (DB['database'],))
    col_existe = cur.fetchone()[0] > 0

    if col_existe:
        print('  Coluna tipo_contrato ja existe')
    else:
        print('  Coluna tipo_contrato NAO existe - precisa adicionar')
        if EXECUTAR:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN tipo_contrato CHAR(2) NULL AFTER modalidade,
                ADD INDEX idx_contrato_tipo (tipo_contrato)
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')

    # ── Fase 2: Aplicar tipo_contrato do CSV ────────────────────────
    print('\n[2/4] Aplicando tipo_contrato...')

    # Verificar se coluna existe para poder consultar
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'contratos' AND COLUMN_NAME = 'tipo_contrato'
    """, (DB['database'],))
    col_disponivel = cur.fetchone()[0] > 0

    updates_tipo = []
    for cod in sorted(existentes):
        tipo_csv = csv_data[cod]
        tipo_atual = None
        if col_disponivel:
            cur.execute('SELECT tipo_contrato FROM contratos WHERE codigo = %s', (cod,))
            row = cur.fetchone()
            tipo_atual = row[0] if row else None

        if tipo_atual != tipo_csv:
            updates_tipo.append((cod, tipo_atual, tipo_csv))

    print('  %d contratos precisam de UPDATE no tipo_contrato' % len(updates_tipo))
    for cod, antigo, novo in updates_tipo:
        print('    [%s] %s -> %s' % (cod, antigo or 'NULL', novo))

    if EXECUTAR and updates_tipo:
        for cod, _, novo in updates_tipo:
            cur.execute('UPDATE contratos SET tipo_contrato = %s WHERE codigo = %s', (novo, cod))
        conn.commit()
        print('  -> %d UPDATEs aplicados' % len(updates_tipo))

    # ── Fase 3: Limpar tipificacoes inconsistentes ──────────────────
    print('\n[3/4] Limpando tipificacoes inconsistentes...')

    # 3a. Contratos tipo S com catmat_* preenchido
    limpeza_catmat = []
    for cod in sorted(existentes):
        tipo = csv_data[cod]
        if tipo == 'S':
            cur.execute("""
                SELECT catmat_pdm_id, catmat_classe_id
                FROM contratos WHERE codigo = %s
            """, (cod,))
            r = cur.fetchone()
            if r and (r[0] is not None or r[1] is not None):
                limpeza_catmat.append((cod, r[0], r[1]))

    if limpeza_catmat:
        print('  Tipo S com catmat preenchido (zerar): %d' % len(limpeza_catmat))
        for cod, pdm, cls in limpeza_catmat:
            print('    [%s] catmat_pdm=%s, catmat_classe=%s -> NULL' % (cod, pdm, cls))
        if EXECUTAR:
            for cod, _, _ in limpeza_catmat:
                cur.execute("""
                    UPDATE contratos
                    SET catmat_pdm_id = NULL, catmat_classe_id = NULL
                    WHERE codigo = %s
                """, (cod,))
            conn.commit()
            print('  -> %d contratos limpos (catmat zerado)' % len(limpeza_catmat))
    else:
        print('  Nenhum contrato tipo S com catmat inconsistente')

    # 3b. Contratos tipo M com catserv_* preenchido
    limpeza_catserv = []
    for cod in sorted(existentes):
        tipo = csv_data[cod]
        if tipo == 'M':
            cur.execute("""
                SELECT catserv_classe_id, catserv_grupo_id
                FROM contratos WHERE codigo = %s
            """, (cod,))
            r = cur.fetchone()
            if r and (r[0] is not None or r[1] is not None):
                limpeza_catserv.append((cod, r[0], r[1]))

    if limpeza_catserv:
        print('  Tipo M com catserv preenchido (zerar): %d' % len(limpeza_catserv))
        for cod, cls, grp in limpeza_catserv:
            print('    [%s] catserv_classe=%s, catserv_grupo=%s -> NULL' % (cod, cls, grp))
        if EXECUTAR:
            for cod, _, _ in limpeza_catserv:
                cur.execute("""
                    UPDATE contratos
                    SET catserv_classe_id = NULL, catserv_grupo_id = NULL
                    WHERE codigo = %s
                """, (cod,))
            conn.commit()
            print('  -> %d contratos limpos (catserv zerado)' % len(limpeza_catserv))
    else:
        print('  Nenhum contrato tipo M com catserv inconsistente')

    # ── Fase 4: Relatorio Final ─────────────────────────────────────
    print('\n[4/4] Relatorio Final...')
    print('-' * 70)

    # Contagem por tipo
    if col_existe or EXECUTAR:
        cur.execute("""
            SELECT tipo_contrato, COUNT(*)
            FROM contratos
            WHERE tipo_contrato IS NOT NULL
            GROUP BY tipo_contrato
            ORDER BY tipo_contrato
        """)
        print('  Contratos com tipo_contrato definido:')
        total_tipados = 0
        for r in cur.fetchall():
            label = {'S': 'Servico', 'M': 'Material', 'SM': 'Misto'}.get(r[0], r[0])
            print('    %s (%s): %d' % (r[0], label, r[1]))
            total_tipados += r[1]
        print('    TOTAL: %d' % total_tipados)

        cur.execute('SELECT COUNT(*) FROM contratos WHERE tipo_contrato IS NULL')
        sem_tipo = cur.fetchone()[0]
        print('  Contratos SEM tipo_contrato: %d' % sem_tipo)

    # Verificar tipificacao catalogo
    cur.execute('SELECT COUNT(*) FROM contratos WHERE catserv_classe_id IS NOT NULL OR catserv_grupo_id IS NOT NULL')
    print('  Contratos com catserv (classe ou grupo): %d' % cur.fetchone()[0])
    cur.execute('SELECT COUNT(*) FROM contratos WHERE catmat_pdm_id IS NOT NULL')
    print('  Contratos com catmat (pdm): %d' % cur.fetchone()[0])

    print('\n' + '=' * 70)
    if not EXECUTAR:
        print('  [DRY-RUN] Nenhuma alteracao foi feita. Use --executar para aplicar.')
    print('=' * 70)

    conn.close()


if __name__ == '__main__':
    main()
