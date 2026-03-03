'''
Importar Execucoes - itens correcao.xlsx -> execucoes
=====================================================
DRY-RUN por padrao. Use --executar para aplicar.

Uso:
  python scripts/importar_execucoes.py            # dry-run
  python scripts/importar_execucoes.py --executar  # aplica no banco
'''
import os
import sys
import unicodedata
from datetime import datetime, date

import pandas as pd
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

CORR_PATH = os.path.join(BASE_DIR, 'itens correção.xlsx')
DB = dict(host=os.getenv('DB_HOST','localhost'), user=os.getenv('DB_USER','root'),
          password=os.getenv('DB_PASS',''), database=os.getenv('DB_NAME','sgc'), charset='utf8mb4')

EXECUTAR = '--executar' in sys.argv


def norm(t):
    if not t: return ''
    return unicodedata.normalize('NFKD', str(t)).encode('ascii','ignore').decode().upper().strip()


def main():
    modo = 'EXECUTAR' if EXECUTAR else 'DRY-RUN'
    print(f'Importar Execucoes [{modo}]')
    print('='*50)

    # Ler Excel
    print('[1/4] Lendo arquivo Excel...')
    df = pd.read_excel(CORR_PATH)
    registros = []
    for _, row in df.iterrows():
        siafe = row.iloc[0]
        item = row.iloc[1]
        data_valor = row.iloc[2]
        valor = row.iloc[3]
        qtde = row.iloc[4]
        contratado = row.iloc[5]

        if pd.isna(siafe):
            continue

        dt = data_valor if isinstance(data_valor, datetime) else None
        try:
            v = float(valor) if not pd.isna(valor) else None
        except (ValueError, TypeError):
            v = None
        try:
            q = int(float(str(qtde))) if not pd.isna(qtde) and str(qtde).strip() != '-' else None
        except (ValueError, TypeError):
            q = None

        registros.append({
            'siafe': str(int(float(str(siafe)))),
            'item': str(item).strip() if not pd.isna(item) else '',
            'data': dt,
            'mes': dt.month if dt else None,
            'ano': dt.year if dt else None,
            'valor': v,
            'qtde': q,
            'contratado': str(contratado).strip() if not pd.isna(contratado) else '',
        })
    print(f'  Registros: {len(registros)}')

    # Consultar BD
    print('[2/4] Consultando banco de dados...')
    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    cur.execute("SELECT codigo FROM contratos")
    contratos = {str(r[0]) for r in cur.fetchall()}

    # itens_contrato com link ao contrato via contratos.categoria_contrato_id
    cur.execute("""
        SELECT ic.id, ic.descricao, c.codigo
        FROM itens_contrato ic
        JOIN contratos c ON c.categoria_contrato_id = ic.categoria_id
    """)
    ic_by_contrato_desc = {}
    for r in cur.fetchall():
        if r[2]:
            k = (str(r[2]), norm(r[1]))
            ic_by_contrato_desc[k] = r[0]  # itens_contrato.id

    # Execucoes existentes
    cur.execute("""
        SELECT e.id, e.codigo_contrato, e.mes, e.ano, e.valor, e.quantidade,
               ic.descricao AS desc_item
        FROM execucoes e
        LEFT JOIN itens_contrato ic ON e.itens_contrato_id = ic.id
    """)
    db_exec = cur.fetchall()

    db_idx = {}
    for r in db_exec:
        desc = norm(r[6]) if r[6] else ''
        key = (str(r[1]), desc, r[2], r[3])
        db_idx[key] = {
            'id': r[0],
            'valor': float(r[4]) if r[4] is not None else None,
            'qtde': int(r[5]) if r[5] is not None else None,
        }

    # Classificar
    print('[3/4] Analisando...')
    inserts = []
    updates = []
    sem_valor = 0
    ignorados = 0
    iguais = 0

    for c in registros:
        if c['siafe'] not in contratos:
            ignorados += 1
            continue
        if c['valor'] is None and c['qtde'] is None:
            sem_valor += 1
            continue

        key = (c['siafe'], norm(c['item']), c['mes'], c['ano'])
        if key in db_idx:
            db = db_idx[key]
            val_diff = abs((db['valor'] or 0) - (c['valor'] or 0)) > 0.005
            qtd_diff = (db['qtde'] or 0) != (c['qtde'] or 0)
            if val_diff or qtd_diff:
                updates.append({**c, 'db_id': db['id']})
            else:
                iguais += 1
        else:
            inserts.append(c)

    print(f'  INSERTs: {len(inserts)}')
    print(f'  UPDATEs: {len(updates)}')
    print(f'  Iguais: {iguais}')
    print(f'  Sem valor: {sem_valor}')
    print(f'  Ignorados: {ignorados}')

    # Executar
    print(f'[4/4] {"Aplicando" if EXECUTAR else "Simulando"}...')

    if EXECUTAR:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        inserted = 0
        updated = 0

        for ins in inserts:
            # Tentar achar itens_contrato_id
            ic_id = ic_by_contrato_desc.get((ins['siafe'], norm(ins['item'])))

            dt = date(ins['ano'], ins['mes'], 1) if ins['ano'] and ins['mes'] else None

            cur.execute("""
                INSERT INTO execucoes
                    (codigo_contrato, itens_contrato_id, data, valor, quantidade, mes, ano, tipo, data_criacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                ins['siafe'], ic_id, dt,
                ins['valor'], ins['qtde'] or 1,
                ins['mes'], ins['ano'], 'S', now,
            ))
            inserted += 1

        for upd in updates:
            cur.execute("""
                UPDATE execucoes
                SET valor = %s, quantidade = %s
                WHERE id = %s
            """, (upd['valor'], upd['qtde'] or 1, upd['db_id']))
            updated += 1

        conn.commit()
        print(f'  {inserted} inseridos, {updated} atualizados')
    else:
        print(f'  [DRY-RUN] Nenhuma alteracao aplicada')
        print(f'  Use --executar para aplicar')

    conn.close()
    print('='*50)


if __name__ == '__main__':
    main()
