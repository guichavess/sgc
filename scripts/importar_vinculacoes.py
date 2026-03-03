'''
Importar Vinculacoes - itens vinculacao.xlsx -> itens_vinculados
================================================================
DRY-RUN por padrao. Use --executar para aplicar.

Uso:
  python scripts/importar_vinculacoes.py            # dry-run
  python scripts/importar_vinculacoes.py --executar  # aplica no banco
'''
import os
import sys
import unicodedata
from datetime import datetime

import pandas as pd
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

VINC_PATH = os.path.join(BASE_DIR, 'itens vinculação.xlsx')
DB = dict(host=os.getenv('DB_HOST','localhost'), user=os.getenv('DB_USER','root'),
          password=os.getenv('DB_PASS',''), database=os.getenv('DB_NAME','sgc'), charset='utf8mb4')

EXECUTAR = '--executar' in sys.argv


def norm(t):
    if not t: return ''
    return unicodedata.normalize('NFKD', str(t)).encode('ascii','ignore').decode().upper().strip()


def norm_tipo(t):
    n = norm(t)
    return 'S' if 'SERVIC' in n else 'M'


def main():
    modo = 'EXECUTAR' if EXECUTAR else 'DRY-RUN'
    print(f'Importar Vinculacoes [{modo}]')
    print('='*50)

    # Ler Excel
    print('[1/4] Lendo arquivo Excel...')
    df = pd.read_excel(VINC_PATH)
    registros = []
    for _, row in df.iterrows():
        siafe = row.iloc[0]
        item_id = row.iloc[1]
        tipo = row.iloc[2]
        if pd.isna(siafe) or str(siafe).strip() in ('', '-'):
            continue
        if pd.isna(item_id):
            continue
        registros.append({
            'siafe': str(int(float(str(siafe)))),
            'id': int(float(str(item_id))),
            'tipo': norm_tipo(tipo),
        })

    # Dedup
    dedup = {}
    for r in registros:
        k = (r['siafe'], r['tipo'], r['id'])
        dedup[k] = r
    print(f'  Registros unicos: {len(dedup)}')

    # Consultar BD
    print('[2/4] Consultando banco de dados...')
    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    cur.execute("SELECT codigo FROM contratos")
    contratos = {str(r[0]) for r in cur.fetchall()}

    cur.execute("SELECT codigo_servico FROM catserv_servicos")
    catserv = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT codigo FROM catmat_itens")
    catmat_itens = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT codigo FROM catmat_pdms")
    catmat_pdms = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT id, codigo_contrato, tipo, catserv_servico_id, catmat_item_id FROM itens_vinculados")
    db_vinc = cur.fetchall()

    # Indexar
    db_idx = {}
    db_by_contrato = {}
    for r in db_vinc:
        cod = str(r[1])
        cat_id = r[3] if r[2] == 'S' else r[4]
        db_idx[(cod, r[2], cat_id)] = r[0]  # id
        db_by_contrato.setdefault((cod, r[2]), []).append(r)

    # Classificar
    print('[3/4] Analisando...')
    inserts = []
    updates = []
    ignorados = 0

    for k, v in dedup.items():
        siafe, tipo, item_id = k
        if siafe not in contratos:
            ignorados += 1
            continue
        if tipo == 'S' and item_id not in catserv:
            ignorados += 1
            continue
        if tipo == 'M' and item_id not in catmat_itens and item_id not in catmat_pdms:
            ignorados += 1
            continue

        if (siafe, tipo, item_id) in db_idx:
            continue  # ja existe

        existing = db_by_contrato.get((siafe, tipo), [])
        encontrou = False
        for ex in existing:
            old_id = ex[3] if tipo == 'S' else ex[4]
            if old_id != item_id:
                updates.append({
                    'db_id': ex[0],
                    'siafe': siafe,
                    'tipo': tipo,
                    'new_id': item_id,
                })
                encontrou = True
                break
        if not encontrou:
            inserts.append(v)

    print(f'  INSERTs: {len(inserts)}')
    print(f'  UPDATEs: {len(updates)}')
    print(f'  Ignorados: {ignorados}')

    # Executar
    print(f'[4/4] {"Aplicando" if EXECUTAR else "Simulando"}...')

    if EXECUTAR:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # INSERTs
        for ins in inserts:
            catserv_id = ins['id'] if ins['tipo'] == 'S' else None
            catmat_id = ins['id'] if ins['tipo'] == 'M' else None
            cur.execute("""
                INSERT INTO itens_vinculados
                    (codigo_contrato, tipo, catserv_servico_id, catmat_item_id, data_vinculacao)
                VALUES (%s, %s, %s, %s, %s)
            """, (ins['siafe'], ins['tipo'], catserv_id, catmat_id, now))

        # UPDATEs
        for upd in updates:
            if upd['tipo'] == 'S':
                cur.execute("UPDATE itens_vinculados SET catserv_servico_id = %s WHERE id = %s",
                            (upd['new_id'], upd['db_id']))
            else:
                cur.execute("UPDATE itens_vinculados SET catmat_item_id = %s WHERE id = %s",
                            (upd['new_id'], upd['db_id']))

        conn.commit()
        print(f'  {len(inserts)} inseridos, {len(updates)} atualizados')
    else:
        print(f'  [DRY-RUN] Nenhuma alteracao aplicada')
        print(f'  Use --executar para aplicar')

    conn.close()
    print('='*50)


if __name__ == '__main__':
    main()
