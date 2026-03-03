"""
Script para vincular itens de contratos a partir de planilha Excel.

Lê o Excel com colunas (codigo contrato, ITEM) e vincula cada item
ao contrato na tabela itens_vinculados, fazendo match pela descrição
com a tabela itens_contrato.

Uso:
    python scripts/vincular_itens_excel.py                  # DRY-RUN (só mostra o que faria)
    python scripts/vincular_itens_excel.py --executar       # Executa de fato

Gera relatório ao final com:
- Itens vinculados com sucesso
- Contratos não encontrados no BD
- Itens não encontrados (sem match de descrição)
- Contratos SEM nenhum item vinculado
"""
import argparse
import os
import sys
import unicodedata
from datetime import datetime

import openpyxl
import pymysql
from dotenv import load_dotenv

# ── Configuração ──
EXCEL_PATH = r'C:\Users\guilh\OneDrive\Documentos\itens mat e serv vinculado a contrato.xlsx'
SHEET_NAME = 'Planilha1'

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASS', 'root'),
    'database': os.getenv('DB_NAME', 'sgc'),
    'charset': 'utf8mb4',
}


def normalizar(texto):
    """Normaliza texto para comparação: remove acentos, uppercase, strip."""
    if not texto:
        return ''
    return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode().upper().strip()


def main():
    parser = argparse.ArgumentParser(description='Vincula itens do Excel aos contratos.')
    parser.add_argument('--executar', action='store_true',
                        help='Executa de fato (sem flag = dry-run)')
    args = parser.parse_args()
    dry_run = not args.executar

    if dry_run:
        print('=' * 60)
        print('  MODO DRY-RUN — nenhuma alteração será feita no banco')
        print('=' * 60)
    else:
        print('=' * 60)
        print('  MODO EXECUÇÃO — alterações serão aplicadas!')
        print('=' * 60)

    # ── 1. Conectar ao banco ──
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── 2. Carregar itens_contrato (descrição normalizada → id) ──
    cur.execute('SELECT id, descricao FROM itens_contrato')
    itens_db = {}
    for row in cur.fetchall():
        desc_norm = normalizar(row[1])
        if desc_norm:
            itens_db[desc_norm] = row[0]
    print(f'\n[BD] itens_contrato carregados: {len(itens_db)}')

    # ── 3. Carregar contratos existentes ──
    cur.execute('SELECT codigo FROM contratos')
    contratos_db = set(str(r[0]) for r in cur.fetchall())
    print(f'[BD] Contratos existentes: {len(contratos_db)}')

    # ── 4. Carregar vinculações existentes ──
    cur.execute('SELECT codigo_contrato, item_contrato_id FROM itens_vinculados')
    vinculos_existentes = set((str(r[0]), r[1]) for r in cur.fetchall())
    print(f'[BD] Vinculações existentes: {len(vinculos_existentes)}')

    # ── 5. Carregar Excel ──
    wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True)
    ws = wb[SHEET_NAME]
    linhas = list(ws.iter_rows(values_only=True))[1:]  # skip header
    wb.close()
    print(f'[Excel] Linhas de dados: {len(linhas)}')

    # ── 6. Processar ──
    stats = {
        'vinculados': [],
        'ja_vinculados': [],
        'contrato_nao_encontrado': [],
        'item_nao_encontrado': [],
        'codigo_invalido': [],
    }

    for codigo_raw, item_desc in linhas:
        codigo_str = str(codigo_raw).strip() if codigo_raw else ''

        # Skip S/N ou vazio
        if not codigo_str or codigo_str.upper() == 'S/N':
            stats['codigo_invalido'].append((codigo_str, item_desc))
            continue

        # Contrato existe?
        if codigo_str not in contratos_db:
            stats['contrato_nao_encontrado'].append((codigo_str, item_desc))
            continue

        # Match item por descrição
        desc_norm = normalizar(item_desc)
        item_id = itens_db.get(desc_norm)

        if not item_id:
            stats['item_nao_encontrado'].append((codigo_str, item_desc))
            continue

        # Já vinculado?
        if (codigo_str, item_id) in vinculos_existentes:
            stats['ja_vinculados'].append((codigo_str, item_desc, item_id))
            continue

        # Vincular!
        stats['vinculados'].append((codigo_str, item_desc, item_id))
        vinculos_existentes.add((codigo_str, item_id))  # evitar duplicata na mesma execução

        if not dry_run:
            cur.execute(
                """INSERT INTO itens_vinculados
                   (codigo_contrato, tipo, item_contrato_id, data_vinculacao)
                   VALUES (%s, 'I', %s, %s)""",
                (codigo_str, item_id, datetime.now())
            )

    if not dry_run:
        conn.commit()

    # ── 7. Relatório de contratos sem itens vinculados ──
    cur.execute("""
        SELECT c.codigo, c.objeto
        FROM contratos c
        LEFT JOIN itens_vinculados iv ON iv.codigo_contrato = c.codigo
        WHERE iv.id IS NULL
        ORDER BY c.codigo
    """)
    contratos_sem_itens = cur.fetchall()

    conn.close()

    # ── 8. Exibir relatório ──
    print('\n' + '=' * 60)
    print('  RELATÓRIO')
    print('=' * 60)

    print(f'\n[OK] Vinculados com sucesso: {len(stats["vinculados"])}')
    if stats['vinculados']:
        for c, desc, iid in stats['vinculados'][:10]:
            print(f'   Contrato {c} <- item_id={iid}: {desc[:70]}')
        if len(stats['vinculados']) > 10:
            print(f'   ... e mais {len(stats["vinculados"]) - 10}')

    print(f'\n[SKIP] Ja vinculados (ignorados): {len(stats["ja_vinculados"])}')

    print(f'\n[WARN] Codigos invalidos (S/N): {len(stats["codigo_invalido"])}')
    for c, desc in stats['codigo_invalido']:
        print(f'   [{c}] {desc[:70]}')

    print(f'\n[ERRO] Contratos NAO encontrados no BD: {len(stats["contrato_nao_encontrado"])}')
    codigos_nao_encontrados = set()
    for c, desc in stats['contrato_nao_encontrado']:
        if c not in codigos_nao_encontrados:
            codigos_nao_encontrados.add(c)
            print(f'   Contrato {c}')

    print(f'\n[ERRO] Itens NAO encontrados (sem match de descricao): {len(stats["item_nao_encontrado"])}')
    for c, desc in stats['item_nao_encontrado']:
        print(f'   Contrato {c}: "{desc[:80]}"')

    print(f'\n{"=" * 60}')
    print(f'  CONTRATOS SEM NENHUM ITEM VINCULADO: {len(contratos_sem_itens)}')
    print(f'{"=" * 60}')
    for codigo, objeto in contratos_sem_itens:
        obj_safe = (objeto or '--')[:70]
        print(f'   {codigo}: {obj_safe}')

    print(f'\n--- Resumo ---')
    print(f'Total linhas Excel:          {len(linhas)}')
    print(f'Vinculados:                  {len(stats["vinculados"])}')
    print(f'Ja existiam:                 {len(stats["ja_vinculados"])}')
    print(f'Codigo invalido:             {len(stats["codigo_invalido"])}')
    print(f'Contrato nao encontrado:     {len(stats["contrato_nao_encontrado"])}')
    print(f'Item nao encontrado:         {len(stats["item_nao_encontrado"])}')
    print(f'Contratos sem itens no BD:   {len(contratos_sem_itens)}')

    if dry_run:
        print(f'\n>> Para aplicar, execute: python scripts/vincular_itens_excel.py --executar')


if __name__ == '__main__':
    main()
