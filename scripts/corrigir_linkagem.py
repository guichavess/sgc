'''
Corrigir Linkagem
=================
1. itens_vinculados.catmat_item_id: converte codigo -> id (app busca por PK)
2. execucoes.itens_contrato_id: linka via descricao do Excel -> itens_contrato.descricao

DRY-RUN por padrao. Use --executar para aplicar.
'''
import os, sys, unicodedata
from datetime import datetime
import pandas as pd
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))
EXCEL = os.path.join(BASE_DIR, 'itens correção.xlsx')
DB = dict(host=os.getenv('DB_HOST','localhost'), user=os.getenv('DB_USER','root'),
          password=os.getenv('DB_PASS',''), database=os.getenv('DB_NAME','sgc'), charset='utf8mb4')
EXECUTAR = '--executar' in sys.argv

def norm(t):
    if not t: return ''
    return unicodedata.normalize('NFKD', str(t)).encode('ascii','ignore').decode().upper().strip()

def main():
    print('Corrigir Linkagem [%s]' % ('EXECUTAR' if EXECUTAR else 'DRY-RUN'))
    print('='*60)
    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    # ==============================================================
    # FASE 1: Corrigir itens_vinculados.catmat_item_id (codigo -> id)
    # ==============================================================
    print('\n[FASE 1] Corrigir itens_vinculados.catmat_item_id')
    print('-'*60)

    # Mapa codigo -> id
    cur.execute('SELECT id, codigo FROM catmat_itens')
    mapa_codigo_id = {r[1]: r[0] for r in cur.fetchall()}

    cur.execute('''
        SELECT id, catmat_item_id FROM itens_vinculados
        WHERE tipo = 'M' AND catmat_item_id IS NOT NULL
    ''')
    iv_materiais = cur.fetchall()

    updates_iv = []
    sem_match_iv = []
    ja_correto = 0

    for iv_id, catmat_val in iv_materiais:
        # Se o valor atual corresponde a um codigo, converter para id
        if catmat_val in mapa_codigo_id:
            novo_id = mapa_codigo_id[catmat_val]
            if novo_id != catmat_val:
                updates_iv.append((novo_id, iv_id))
            else:
                ja_correto += 1
        else:
            sem_match_iv.append((iv_id, catmat_val))

    print('  Total materiais: %d' % len(iv_materiais))
    print('  A corrigir (codigo->id): %d' % len(updates_iv))
    print('  Ja corretos (id=codigo): %d' % ja_correto)
    print('  Sem match em catmat_itens: %d' % len(sem_match_iv))
    if sem_match_iv:
        for iv_id, val in sem_match_iv[:5]:
            print('    iv=%d catmat_item_id=%d (nao encontrado)' % (iv_id, val))

    if EXECUTAR and updates_iv:
        for novo_id, iv_id in updates_iv:
            cur.execute('UPDATE itens_vinculados SET catmat_item_id = %s WHERE id = %s', (novo_id, iv_id))
        conn.commit()
        print('  -> %d atualizados' % len(updates_iv))

    # ==============================================================
    # FASE 2: Corrigir execucoes.itens_contrato_id via descricao
    # ==============================================================
    print('\n[FASE 2] Corrigir execucoes.itens_contrato_id')
    print('-'*60)

    # Ler Excel
    df = pd.read_excel(EXCEL)
    print('  Linhas Excel: %d' % len(df))

    # Carregar itens_contrato indexado por descricao normalizada
    cur.execute('SELECT id, descricao FROM itens_contrato')
    ic_by_desc = {}
    for r in cur.fetchall():
        d = norm(r[1])
        ic_by_desc.setdefault(d, r[0])  # primeiro encontrado

    # Carregar execucoes sem itens_contrato_id
    cur.execute('''
        SELECT id, codigo_contrato, mes, ano, valor
        FROM execucoes WHERE itens_contrato_id IS NULL
    ''')
    exec_by_key = {}
    for r in cur.fetchall():
        key = (str(r[1]), r[2], r[3])
        exec_by_key.setdefault(key, []).append({'id': r[0], 'valor': float(r[4]) if r[4] else None})

    updates_exec = []
    desc_nao_encontrada = set()
    exec_nao_encontrada = 0

    for _, row in df.iterrows():
        siafe, item_desc, data_valor, valor = row.iloc[0], row.iloc[1], row.iloc[2], row.iloc[3]
        if pd.isna(siafe) or pd.isna(item_desc): continue

        cod = str(int(float(str(siafe))))
        desc_norm = norm(item_desc)

        # Buscar itens_contrato pela descricao
        ic_id = ic_by_desc.get(desc_norm)
        if ic_id is None:
            desc_nao_encontrada.add(desc_norm[:80])
            continue

        # Extrair mes/ano
        if isinstance(data_valor, datetime):
            mes, ano = data_valor.month, data_valor.year
        else:
            continue

        try:
            val = float(valor) if not pd.isna(valor) else None
        except: val = None

        # Encontrar execucao no BD
        candidates = exec_by_key.get((cod, mes, ano), [])
        if not candidates:
            exec_nao_encontrada += 1
            continue

        # Match por valor
        matched = None
        for c in candidates:
            if val is None or c['valor'] is None or abs(c['valor'] - val) < 0.01:
                matched = c
                break
        if not matched:
            matched = candidates[0]

        updates_exec.append((ic_id, matched['id']))
        candidates.remove(matched)

    print('  Execucoes a linkar: %d' % len(updates_exec))
    print('  Descricoes nao encontradas: %d' % len(desc_nao_encontrada))
    if desc_nao_encontrada:
        for d in sorted(desc_nao_encontrada)[:10]:
            print('    %s' % d)
        if len(desc_nao_encontrada) > 10:
            print('    ... e mais %d' % (len(desc_nao_encontrada) - 10))
    print('  Excel sem exec no BD: %d (ja linkadas ou sem valor)' % exec_nao_encontrada)

    if EXECUTAR and updates_exec:
        for ic_id, exec_id in updates_exec:
            cur.execute('UPDATE execucoes SET itens_contrato_id = %s WHERE id = %s', (ic_id, exec_id))
        conn.commit()
        print('  -> %d atualizados' % len(updates_exec))

    # RESUMO
    print('\n' + '='*60)
    print('RESUMO')
    print('  Fase 1 - catmat_item_id corrigidos: %d' % len(updates_iv))
    print('  Fase 2 - execucoes linkadas: %d' % len(updates_exec))
    if not EXECUTAR:
        print('\n  [DRY-RUN] Use --executar para aplicar.')
    conn.close()

if __name__ == '__main__':
    main()
