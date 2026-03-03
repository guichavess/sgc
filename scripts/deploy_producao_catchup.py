'''
Deploy Producao Catch-up - Aplicar todas as migracoes pendentes
================================================================
Verifica e aplica TUDO que foi feito localmente mas pode estar
faltando em producao:

  1. Schema: tipo_contrato, catserv_grupo_id, item_vinculado_id
  2. tipo_contrato via CSV (S, M, SM)
  3. Tipificacao reversa: catserv_classe_id, catserv_grupo_id,
     catmat_pdm_id, catmat_classe_id
  4. Correcao tipo_contrato SM->S para contratos sem material
  5. Reimport vinculacoes + execucoes para contratos especificos
     (25014859, 25014928, 25014931, 25017768)

DRY-RUN por padrao. Use --executar para aplicar.

Uso:
  python scripts/deploy_producao_catchup.py             # dry-run (so verifica)
  python scripts/deploy_producao_catchup.py --executar   # aplica tudo
'''
import os
import sys
import unicodedata
import csv
from datetime import datetime, date
from collections import defaultdict

import pandas as pd
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DB = dict(host=os.getenv('DB_HOST', 'localhost'), user=os.getenv('DB_USER', 'root'),
          password=os.getenv('DB_PASS', ''), database=os.getenv('DB_NAME', 'sgc'), charset='utf8mb4')

EXECUTAR = '--executar' in sys.argv

CSV_PATH = os.path.join(BASE_DIR, 'tipificacao contratos.csv')
VINC_PATH = os.path.join(BASE_DIR, 'itens vinculação.xlsx')
CORR_PATH = os.path.join(BASE_DIR, 'itens correção.xlsx')

# Contratos que precisam de reimport especifico
CONTRATOS_ESPECIFICOS = ['25014859', '25014928', '25014931', '25017768']

# Contratos com tipo_contrato errado (SM sem material) -> S
CONTRATOS_FIX_SM_PARA_S = ['24012221', '25018283']

MAPA_TIPO = {'s': 'S', 'm': 'M', 'sm': 'SM'}


# ─── Utilidades ──────────────────────────────────────────────────────────────

def norm(t):
    if not t:
        return ''
    return unicodedata.normalize('NFKD', str(t)).encode('ascii', 'ignore').decode().upper().strip()


def norm_tipo(t):
    n = norm(t)
    return 'S' if 'SERVIC' in n else 'M'


def tokenize(t):
    n = norm(t)
    return set(n.split()) if n else set()


def jaccard(s1, s2):
    if not s1 or not s2:
        return 0.0
    inter = s1 & s2
    union = s1 | s2
    return len(inter) / len(union) if union else 0.0


def col_existe(cur, tabela, coluna):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (DB['database'], tabela, coluna))
    return cur.fetchone()[0] > 0


# ─── PASSO 1: Schema Migrations ─────────────────────────────────────────────

def passo1_schema(conn, executar):
    print('\n' + '=' * 70)
    print('PASSO 1: Schema Migrations')
    print('=' * 70)
    cur = conn.cursor()
    alteracoes = 0

    # 1a. tipo_contrato em contratos
    if col_existe(cur, 'contratos', 'tipo_contrato'):
        print('  [OK] contratos.tipo_contrato ja existe')
    else:
        print('  [FALTA] contratos.tipo_contrato')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN tipo_contrato CHAR(2) NULL AFTER modalidade,
                ADD INDEX idx_contrato_tipo (tipo_contrato)
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    # 1b. catserv_grupo_id em contratos
    if col_existe(cur, 'contratos', 'catserv_grupo_id'):
        print('  [OK] contratos.catserv_grupo_id ja existe')
    else:
        print('  [FALTA] contratos.catserv_grupo_id')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN catserv_grupo_id INT NULL AFTER catserv_classe_id,
                ADD INDEX idx_contrato_catserv_grupo (catserv_grupo_id)
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    # 1c. item_vinculado_id em execucoes
    if col_existe(cur, 'execucoes', 'item_vinculado_id'):
        print('  [OK] execucoes.item_vinculado_id ja existe')
    else:
        print('  [FALTA] execucoes.item_vinculado_id')
        if executar:
            cur.execute("""
                ALTER TABLE execucoes
                ADD COLUMN item_vinculado_id INT NULL,
                ADD INDEX idx_exec_vinculado (item_vinculado_id),
                ADD CONSTRAINT fk_exec_vinculado FOREIGN KEY (item_vinculado_id)
                    REFERENCES itens_vinculados(id) ON DELETE SET NULL
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    # 1d. tipificado_por em contratos
    if col_existe(cur, 'contratos', 'tipificado_por'):
        print('  [OK] contratos.tipificado_por ja existe')
    else:
        print('  [FALTA] contratos.tipificado_por')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN tipificado_por BIGINT NULL
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    # 1e. data_tipificacao em contratos
    if col_existe(cur, 'contratos', 'data_tipificacao'):
        print('  [OK] contratos.data_tipificacao ja existe')
    else:
        print('  [FALTA] contratos.data_tipificacao')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN data_tipificacao DATETIME NULL
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    # 1f. catserv_classe_id em contratos
    if col_existe(cur, 'contratos', 'catserv_classe_id'):
        print('  [OK] contratos.catserv_classe_id ja existe')
    else:
        print('  [FALTA] contratos.catserv_classe_id')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN catserv_classe_id INT NULL
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    # 1g. catmat_pdm_id em contratos
    if col_existe(cur, 'contratos', 'catmat_pdm_id'):
        print('  [OK] contratos.catmat_pdm_id ja existe')
    else:
        print('  [FALTA] contratos.catmat_pdm_id')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN catmat_pdm_id INT NULL
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    # 1h. catmat_classe_id em contratos
    if col_existe(cur, 'contratos', 'catmat_classe_id'):
        print('  [OK] contratos.catmat_classe_id ja existe')
    else:
        print('  [FALTA] contratos.catmat_classe_id')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN catmat_classe_id INT NULL
            """)
            conn.commit()
            print('  -> Coluna adicionada')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')
        alteracoes += 1

    if alteracoes == 0:
        print('\n  Nenhuma alteracao de schema necessaria!')
    else:
        print(f'\n  {alteracoes} alteracoes de schema {"aplicadas" if executar else "pendentes"}')

    return alteracoes


# ─── PASSO 2: Aplicar tipo_contrato do CSV ──────────────────────────────────

def passo2_tipo_contrato_csv(conn, executar):
    print('\n' + '=' * 70)
    print('PASSO 2: Aplicar tipo_contrato do CSV')
    print('=' * 70)
    cur = conn.cursor()

    if not os.path.exists(CSV_PATH):
        print(f'  AVISO: CSV nao encontrado: {CSV_PATH}')
        print(f'  Pulando passo 2. Copie "tipificacao contratos.csv" para a raiz do projeto.')
        return 0

    # Ler CSV
    csv_data = {}
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            cod = row['Contrato'].strip()
            tipo = row['Tipo'].strip().lower()
            if cod and tipo and cod != 'Total Geral':
                if tipo in MAPA_TIPO:
                    csv_data[cod] = MAPA_TIPO[tipo]
    print(f'  {len(csv_data)} contratos no CSV (S:{sum(1 for v in csv_data.values() if v=="S")}, '
          f'M:{sum(1 for v in csv_data.values() if v=="M")}, '
          f'SM:{sum(1 for v in csv_data.values() if v=="SM")})')

    # Verificar quais ja tem tipo_contrato correto
    updates = []
    for cod, tipo_csv in csv_data.items():
        cur.execute('SELECT tipo_contrato FROM contratos WHERE codigo = %s', (cod,))
        row = cur.fetchone()
        if not row:
            continue
        if row[0] != tipo_csv:
            updates.append((cod, row[0], tipo_csv))

    if not updates:
        print('  Todos os contratos ja tem tipo_contrato correto. Nada a fazer.')
        return 0

    print(f'  {len(updates)} contratos precisam de UPDATE:')
    for cod, antigo, novo in updates[:20]:
        print(f'    [{cod}] {antigo or "NULL"} -> {novo}')
    if len(updates) > 20:
        print(f'    ... +{len(updates)-20} mais')

    if executar:
        for cod, _, novo in updates:
            cur.execute('UPDATE contratos SET tipo_contrato = %s WHERE codigo = %s', (novo, cod))
        conn.commit()
        print(f'  -> {len(updates)} UPDATEs aplicados')

    return len(updates)


# ─── PASSO 3: Tipificacao Reversa (classe/grupo/PDM) ────────────────────────

def passo3_tipificacao(conn, executar):
    print('\n' + '=' * 70)
    print('PASSO 3: Tipificacao Reversa (catserv/catmat)')
    print('=' * 70)
    cur = conn.cursor()

    # Carregar catalogos
    cur.execute("SELECT codigo_servico, codigo_classe, codigo_grupo FROM catserv_servicos")
    catserv = {}
    for r in cur.fetchall():
        catserv[r[0]] = {'codigo_classe': r[1], 'codigo_grupo': r[2]}

    cur.execute("SELECT codigo_classe, codigo_grupo FROM catserv_classes")
    catserv_classes = {}
    for r in cur.fetchall():
        catserv_classes[r[0]] = {'codigo_grupo': r[1]}

    cur.execute("SELECT id, codigo, codigo_pdm FROM catmat_itens")
    catmat_by_id = {}
    catmat_by_codigo = {}
    for r in cur.fetchall():
        catmat_by_id[r[0]] = {'codigo': r[1], 'codigo_pdm': r[2]}
        catmat_by_codigo[r[1]] = {'id': r[0], 'codigo_pdm': r[2]}

    cur.execute("SELECT id, codigo, codigo_classe FROM catmat_pdms")
    pdm_by_codigo = {}
    for r in cur.fetchall():
        pdm_by_codigo[r[1]] = {'id': r[0], 'codigo_classe': r[2]}

    cur.execute("SELECT id, codigo FROM catmat_classes")
    classe_mat_by_codigo = {r[1]: r[0] for r in cur.fetchall()}

    # Carregar vinculacoes
    cur.execute("""
        SELECT iv.codigo_contrato, iv.tipo, iv.catserv_servico_id, iv.catmat_item_id
        FROM itens_vinculados iv
        JOIN contratos c ON c.codigo = iv.codigo_contrato
    """)
    vinc_por_contrato = defaultdict(list)
    for cod, tipo, catserv_id, catmat_id in cur.fetchall():
        vinc_por_contrato[str(cod)].append({
            'tipo': tipo, 'catserv_servico_id': catserv_id, 'catmat_item_id': catmat_id,
        })
    print(f'  {sum(len(v) for v in vinc_por_contrato.values())} vinculacoes em {len(vinc_por_contrato)} contratos')

    # Estado atual
    cur.execute("""
        SELECT codigo, catserv_classe_id, catserv_grupo_id, catmat_pdm_id, catmat_classe_id
        FROM contratos
    """)
    estado = {}
    for r in cur.fetchall():
        estado[str(r[0])] = {
            'catserv_classe_id': r[1], 'catserv_grupo_id': r[2],
            'catmat_pdm_id': r[3], 'catmat_classe_id': r[4],
        }

    # Calcular
    updates = []
    grupo_fallback = 0

    for siafe, vincs in vinc_por_contrato.items():
        e = estado.get(siafe, {})
        servicos = [v for v in vincs if v['tipo'] == 'S']
        materiais = [v for v in vincs if v['tipo'] == 'M']
        upd = {'siafe': siafe}
        mudou = False

        # CATSERV
        if servicos:
            freq_cls = defaultdict(int)
            freq_grp = defaultdict(int)
            for v in servicos:
                sid = v['catserv_servico_id']
                if sid and sid in catserv:
                    cls = catserv[sid]['codigo_classe']
                    grp = catserv[sid]['codigo_grupo']
                    if cls:
                        freq_cls[cls] += 1
                    if grp:
                        freq_grp[grp] += 1

            if freq_cls:
                best_cls = max(freq_cls, key=freq_cls.get)
                grp_da_cls = catserv_classes.get(best_cls, {}).get('codigo_grupo')
                if e.get('catserv_classe_id') is None or e.get('catserv_grupo_id') is None:
                    upd['catserv_classe_id'] = best_cls
                    upd['catserv_grupo_id'] = grp_da_cls
                    if best_cls != e.get('catserv_classe_id') or grp_da_cls != e.get('catserv_grupo_id'):
                        mudou = True
            elif freq_grp and e.get('catserv_classe_id') is None and e.get('catserv_grupo_id') is None:
                best_grp = max(freq_grp, key=freq_grp.get)
                upd['catserv_grupo_id'] = best_grp
                if best_grp != e.get('catserv_grupo_id'):
                    mudou = True
                    grupo_fallback += 1

        # CATMAT
        if materiais:
            pdm_freq = defaultdict(int)
            for v in materiais:
                mid = v['catmat_item_id']
                cod_pdm = None
                if mid and mid in catmat_by_id:
                    cod_pdm = catmat_by_id[mid]['codigo_pdm']
                elif mid and mid in catmat_by_codigo:
                    cod_pdm = catmat_by_codigo[mid]['codigo_pdm']
                if cod_pdm and cod_pdm in pdm_by_codigo:
                    info = pdm_by_codigo[cod_pdm]
                    pdm_freq[(info['id'], info['codigo_classe'])] += 1

            if pdm_freq and e.get('catmat_pdm_id') is None:
                (pdm_id, pdm_cod_cls) = max(pdm_freq, key=pdm_freq.get)
                classe_id = classe_mat_by_codigo.get(pdm_cod_cls)
                upd['catmat_pdm_id'] = pdm_id
                upd['catmat_classe_id'] = classe_id
                if pdm_id != e.get('catmat_pdm_id') or classe_id != e.get('catmat_classe_id'):
                    mudou = True

        if mudou:
            updates.append(upd)

    upd_cls = [u for u in updates if 'catserv_classe_id' in u]
    upd_grp = [u for u in updates if 'catserv_grupo_id' in u and 'catserv_classe_id' not in u]
    upd_mat = [u for u in updates if 'catmat_pdm_id' in u]

    print(f'\n  Updates necessarios: {len(updates)}')
    print(f'    CATSERV classe: {len(upd_cls)}')
    print(f'    CATSERV grupo (fallback): {len(upd_grp)}')
    print(f'    CATMAT PDM+classe: {len(upd_mat)}')

    if not updates:
        print('  Tipificacao ja esta completa. Nada a fazer.')
        return 0

    if executar:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for u in updates:
            sets = ['data_tipificacao = %s']
            params = [now]
            if 'catserv_classe_id' in u:
                sets.append('catserv_classe_id = %s')
                params.append(u['catserv_classe_id'])
                sets.append('catserv_grupo_id = %s')
                params.append(u.get('catserv_grupo_id'))
            elif 'catserv_grupo_id' in u:
                sets.append('catserv_grupo_id = %s')
                params.append(u['catserv_grupo_id'])
            if 'catmat_pdm_id' in u:
                sets.append('catmat_pdm_id = %s')
                params.append(u['catmat_pdm_id'])
                sets.append('catmat_classe_id = %s')
                params.append(u.get('catmat_classe_id'))
            params.append(u['siafe'])
            cur.execute(f"UPDATE contratos SET {', '.join(sets)} WHERE codigo = %s", params)
        conn.commit()
        print(f'  -> {len(updates)} contratos atualizados')
    else:
        for u in updates[:20]:
            partes = []
            if 'catserv_classe_id' in u:
                partes.append(f"catserv_classe={u['catserv_classe_id']}")
            if 'catserv_grupo_id' in u and 'catserv_classe_id' not in u:
                partes.append(f"catserv_grupo={u['catserv_grupo_id']}")
            if 'catmat_pdm_id' in u:
                partes.append(f"catmat_pdm={u['catmat_pdm_id']}")
            print(f'    [{u["siafe"]}] {", ".join(partes)}')
        if len(updates) > 20:
            print(f'    ... +{len(updates)-20} mais')

    return len(updates)


# ─── PASSO 4: Correcao tipo_contrato SM -> S ────────────────────────────────

def passo4_fix_tipo_contrato(conn, executar):
    print('\n' + '=' * 70)
    print('PASSO 4: Correcao tipo_contrato SM -> S (contratos sem material)')
    print('=' * 70)
    cur = conn.cursor()

    fixes = []
    for cod in CONTRATOS_FIX_SM_PARA_S:
        cur.execute('SELECT tipo_contrato FROM contratos WHERE codigo = %s', (cod,))
        row = cur.fetchone()
        if not row:
            print(f'  [{cod}] Contrato nao encontrado no BD')
            continue
        if row[0] == 'SM':
            fixes.append(cod)
            print(f'  [{cod}] tipo_contrato = SM -> precisa corrigir para S')
        elif row[0] == 'S':
            print(f'  [{cod}] ja esta como S. OK.')
        else:
            print(f'  [{cod}] tipo_contrato = {row[0]}. Verificar manualmente.')

    if not fixes:
        print('  Nenhuma correcao necessaria.')
        return 0

    if executar:
        for cod in fixes:
            cur.execute("""
                UPDATE contratos
                SET tipo_contrato = 'S',
                    catmat_pdm_id = NULL,
                    catmat_classe_id = NULL
                WHERE codigo = %s
            """, (cod,))
        conn.commit()
        print(f'  -> {len(fixes)} contratos corrigidos (SM -> S, catmat zerado)')
    else:
        print(f'  [DRY-RUN] {len(fixes)} contratos seriam corrigidos')

    return len(fixes)


# ─── PASSO 5: Reimport contratos especificos ────────────────────────────────

def passo5_reimport_especificos(conn, executar):
    print('\n' + '=' * 70)
    print('PASSO 5: Reimport Vinculacoes + Execucoes (contratos especificos)')
    print('=' * 70)
    cur = conn.cursor()

    # Verificar se contratos existem
    placeholders = ','.join(['%s'] * len(CONTRATOS_ESPECIFICOS))
    cur.execute(f"SELECT codigo FROM contratos WHERE codigo IN ({placeholders})", CONTRATOS_ESPECIFICOS)
    existentes = {str(r[0]) for r in cur.fetchall()}
    nao_encontrados = set(CONTRATOS_ESPECIFICOS) - existentes

    if nao_encontrados:
        print(f'  AVISO: Contratos NAO encontrados: {", ".join(sorted(nao_encontrados))}')
    if existentes:
        print(f'  Contratos encontrados: {", ".join(sorted(existentes))}')

    contratos = [c for c in CONTRATOS_ESPECIFICOS if c in existentes]
    if not contratos:
        print('  Nenhum contrato valido. Pulando.')
        return 0

    # --- 5a. Verificar se arquivos Excel existem ---
    vinc_ok = os.path.exists(VINC_PATH)
    corr_ok = os.path.exists(CORR_PATH)
    print(f'\n  Arquivo vinculacoes: {"OK" if vinc_ok else "NAO ENCONTRADO"} ({VINC_PATH})')
    print(f'  Arquivo execucoes:   {"OK" if corr_ok else "NAO ENCONTRADO"} ({CORR_PATH})')

    if not vinc_ok or not corr_ok:
        print('  ERRO: Copie os arquivos Excel para a raiz do projeto antes de continuar.')
        return 0

    alvo_set = set(contratos)

    # --- 5b. Ler e importar vinculacoes ---
    print('\n  [5a] Lendo vinculacoes do Excel...')
    df_vinc = pd.read_excel(VINC_PATH)
    registros_vinc = []
    for _, row in df_vinc.iterrows():
        siafe = row.iloc[0]
        item_id = row.iloc[1]
        tipo = row.iloc[2]
        if pd.isna(siafe) or str(siafe).strip() in ('', '-'):
            continue
        if pd.isna(item_id):
            continue
        siafe_str = str(int(float(str(siafe))))
        if siafe_str not in alvo_set:
            continue
        registros_vinc.append({
            'siafe': siafe_str,
            'id': int(float(str(item_id))),
            'tipo': norm_tipo(tipo),
        })

    # Dedup
    dedup_vinc = {}
    for r in registros_vinc:
        k = (r['siafe'], r['tipo'], r['id'])
        dedup_vinc[k] = r
    print(f'  {len(dedup_vinc)} registros unicos de vinculacao')

    # Validar contra catalogos
    cur.execute("SELECT codigo_servico FROM catserv_servicos")
    catserv_ids = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT codigo FROM catmat_itens")
    catmat_itens_ids = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT codigo FROM catmat_pdms")
    catmat_pdms_ids = {r[0] for r in cur.fetchall()}

    validos = {}
    ignorados = 0
    for k, v in dedup_vinc.items():
        _, tipo, item_id = k
        if tipo == 'S' and item_id not in catserv_ids:
            ignorados += 1
            continue
        if tipo == 'M' and item_id not in catmat_itens_ids and item_id not in catmat_pdms_ids:
            ignorados += 1
            continue
        validos[k] = v
    print(f'  {len(validos)} validos, {ignorados} ignorados (ID nao no catalogo)')

    # Resumo por contrato
    por_contrato = defaultdict(lambda: {'S': 0, 'M': 0})
    for k, v in validos.items():
        por_contrato[v['siafe']][v['tipo']] += 1
    for c in contratos:
        s = por_contrato.get(c, {'S': 0, 'M': 0})
        print(f'    {c}: {s["S"]} servicos, {s["M"]} materiais')

    if executar:
        # Limpar vinculacoes existentes
        cur.execute(f"UPDATE execucoes SET item_vinculado_id = NULL WHERE codigo_contrato IN ({placeholders})",
                    contratos)
        cur.execute(f"DELETE FROM itens_vinculados WHERE codigo_contrato IN ({placeholders})", contratos)

        # Inserir novas
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        inserted_vinc = 0
        for k, v in validos.items():
            catserv_id = v['id'] if v['tipo'] == 'S' else None
            catmat_id = v['id'] if v['tipo'] == 'M' else None
            cur.execute("""
                INSERT INTO itens_vinculados
                    (codigo_contrato, tipo, catserv_servico_id, catmat_item_id, data_vinculacao)
                VALUES (%s, %s, %s, %s, %s)
            """, (v['siafe'], v['tipo'], catserv_id, catmat_id, now))
            inserted_vinc += 1
        conn.commit()
        print(f'  -> {inserted_vinc} vinculacoes inseridas')
    else:
        print(f'  [DRY-RUN] {len(validos)} vinculacoes seriam inseridas')

    # --- 5c. Ler e importar execucoes ---
    print('\n  [5b] Lendo execucoes do Excel...')
    df_corr = pd.read_excel(CORR_PATH)
    registros_exec = []
    for _, row in df_corr.iterrows():
        siafe = row.iloc[0]
        item = row.iloc[1]
        data_valor = row.iloc[2]
        valor = row.iloc[3]
        qtde = row.iloc[4]
        if pd.isna(siafe):
            continue
        siafe_str = str(int(float(str(siafe))))
        if siafe_str not in alvo_set:
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
        registros_exec.append({
            'siafe': siafe_str,
            'item': str(item).strip() if not pd.isna(item) else '',
            'data': dt,
            'mes': dt.month if dt else None,
            'ano': dt.year if dt else None,
            'valor': v,
            'qtde': q,
        })
    print(f'  {len(registros_exec)} registros de execucoes nos contratos-alvo')

    # Carregar vinculacoes atualizadas (recem-importadas ou existentes)
    cur.execute(f"""
        SELECT iv.id, iv.codigo_contrato, iv.tipo, iv.catserv_servico_id, iv.catmat_item_id
        FROM itens_vinculados iv WHERE iv.codigo_contrato IN ({placeholders})
    """, contratos)
    vinc_raw = cur.fetchall()
    print(f'  {len(vinc_raw)} vinculacoes para matching')

    # Catalogo CATSERV
    cur.execute("SELECT codigo_servico, nome FROM catserv_servicos")
    catserv_nomes = {r[0]: r[1] for r in cur.fetchall()}

    # Catalogo CATMAT
    cur.execute("SELECT id, codigo, descricao FROM catmat_itens")
    catmat_by_id = {}
    catmat_by_codigo = {}
    for r in cur.fetchall():
        catmat_by_id[r[0]] = {'codigo': r[1], 'descricao': r[2]}
        catmat_by_codigo[r[1]] = {'id': r[0], 'descricao': r[2]}

    # Index vinculacoes
    vinc_por_contrato = defaultdict(list)
    for vid, cod, tipo, catserv_id, catmat_id in vinc_raw:
        desc = ''
        if tipo == 'S' and catserv_id and catserv_id in catserv_nomes:
            desc = catserv_nomes[catserv_id]
        elif tipo == 'M' and catmat_id:
            if catmat_id in catmat_by_id:
                desc = catmat_by_id[catmat_id]['descricao']
            elif catmat_id in catmat_by_codigo:
                desc = catmat_by_codigo[catmat_id]['descricao']
        vinc_por_contrato[str(cod)].append({
            'id': vid, 'tipo': tipo,
            'catserv_servico_id': catserv_id, 'catmat_item_id': catmat_id,
            'descricao': desc, 'tokens': tokenize(desc),
        })

    # Match
    matched = []
    sem_vinculacao = []
    grupos_exec = defaultdict(list)
    for r in registros_exec:
        if r['valor'] is None and r['qtde'] is None:
            continue
        key = (r['siafe'], norm(r['item']))
        grupos_exec[key].append(r)

    for (siafe, item_norm), rows in grupos_exec.items():
        vincs = vinc_por_contrato.get(siafe, [])
        if not vincs:
            sem_vinculacao.extend(rows)
            continue
        if len(vincs) == 1:
            best_vinc = vincs[0]
        else:
            item_tokens = tokenize(item_norm)
            best_score = -1
            best_vinc = None
            for v in vincs:
                score = jaccard(item_tokens, v['tokens'])
                if score > best_score:
                    best_score = score
                    best_vinc = v
        if best_vinc:
            for r in rows:
                matched.append({
                    **r,
                    'item_vinculado_id': best_vinc['id'],
                    'tipo': best_vinc['tipo'],
                    'catserv_servico_id': best_vinc['catserv_servico_id'] if best_vinc['tipo'] == 'S' else None,
                    'catmat_item_id': best_vinc['catmat_item_id'] if best_vinc['tipo'] == 'M' else None,
                })

    print(f'  Matched: {len(matched)}, Sem vinculacao: {len(sem_vinculacao)}')

    # Resumo
    por_contrato_exec = defaultdict(lambda: {'n': 0, 'val': 0.0})
    for r in matched:
        por_contrato_exec[r['siafe']]['n'] += 1
        por_contrato_exec[r['siafe']]['val'] += r['valor'] or 0
    for c in contratos:
        info = por_contrato_exec.get(c, {'n': 0, 'val': 0.0})
        print(f'    {c}: {info["n"]} execucoes, R$ {info["val"]:,.2f}')

    if sem_vinculacao:
        print(f'\n  Execucoes sem vinculacao ({len(sem_vinculacao)}):')
        for r in sem_vinculacao:
            print(f'    [{r["siafe"]}] {r["item"][:60]}')

    if executar:
        # Deletar execucoes existentes
        cur.execute(f"DELETE FROM execucoes WHERE codigo_contrato IN ({placeholders})", contratos)
        # Inserir
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for r in matched:
            dt = date(r['ano'], r['mes'], 1) if r['ano'] and r['mes'] else None
            cur.execute("""
                INSERT INTO execucoes
                    (codigo_contrato, item_vinculado_id, data, valor, quantidade,
                     mes, ano, tipo, catserv_servico_id, catmat_item_id, data_criacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['siafe'], r['item_vinculado_id'], dt, r['valor'], r['qtde'] or 1,
                r['mes'], r['ano'], r['tipo'], r['catserv_servico_id'], r['catmat_item_id'], now,
            ))
        conn.commit()
        print(f'  -> {len(matched)} execucoes inseridas')
    else:
        print(f'  [DRY-RUN] {len(matched)} execucoes seriam inseridas')

    # --- 5d. Tipificar os contratos especificos ---
    print('\n  [5c] Tipificando contratos especificos...')
    cur.execute("SELECT codigo_servico, codigo_classe, codigo_grupo FROM catserv_servicos")
    catserv_full = {}
    for r in cur.fetchall():
        catserv_full[r[0]] = {'codigo_classe': r[1], 'codigo_grupo': r[2]}

    cur.execute("SELECT id, codigo, codigo_pdm FROM catmat_itens")
    catmat_itens_full = {}
    catmat_cod_full = {}
    for r in cur.fetchall():
        catmat_itens_full[r[0]] = {'codigo': r[1], 'codigo_pdm': r[2]}
        catmat_cod_full[r[1]] = {'id': r[0], 'codigo_pdm': r[2]}

    cur.execute("SELECT id, codigo, codigo_classe FROM catmat_pdms")
    pdm_by_cod = {}
    for r in cur.fetchall():
        pdm_by_cod[r[1]] = {'id': r[0], 'codigo_classe': r[2]}

    cur.execute("SELECT id, codigo FROM catmat_classes")
    cls_mat = {r[1]: r[0] for r in cur.fetchall()}

    # Recarregar vinculacoes
    cur.execute(f"""
        SELECT iv.codigo_contrato, iv.tipo, iv.catserv_servico_id, iv.catmat_item_id
        FROM itens_vinculados iv WHERE iv.codigo_contrato IN ({placeholders})
    """, contratos)

    vinc_tip = defaultdict(list)
    for cod, tipo, catserv_id, catmat_id in cur.fetchall():
        vinc_tip[str(cod)].append({'tipo': tipo, 'catserv_servico_id': catserv_id, 'catmat_item_id': catmat_id})

    tip_updates = []
    for siafe in contratos:
        vincs = vinc_tip.get(siafe, [])
        if not vincs:
            print(f'    [{siafe}] Sem vinculacoes, pulando')
            continue

        servicos = [v for v in vincs if v['tipo'] == 'S']
        materiais = [v for v in vincs if v['tipo'] == 'M']
        has_s = len(servicos) > 0
        has_m = len(materiais) > 0

        upd = {
            'siafe': siafe,
            'tipo_contrato': 'SM' if has_s and has_m else ('S' if has_s else 'M'),
            'catserv_classe_id': None,
            'catserv_grupo_id': None,
            'catmat_pdm_id': None,
            'catmat_classe_id': None,
        }

        if servicos:
            freq_cls = defaultdict(int)
            freq_grp = defaultdict(int)
            for v in servicos:
                sid = v['catserv_servico_id']
                if sid and sid in catserv_full:
                    cls = catserv_full[sid]['codigo_classe']
                    grp = catserv_full[sid]['codigo_grupo']
                    if cls:
                        freq_cls[cls] += 1
                    if grp:
                        freq_grp[grp] += 1
            if freq_cls:
                upd['catserv_classe_id'] = max(freq_cls, key=freq_cls.get)
                for v in servicos:
                    sid = v['catserv_servico_id']
                    if sid and sid in catserv_full and catserv_full[sid]['codigo_classe'] == upd['catserv_classe_id']:
                        upd['catserv_grupo_id'] = catserv_full[sid]['codigo_grupo']
                        break
            elif freq_grp:
                upd['catserv_grupo_id'] = max(freq_grp, key=freq_grp.get)

        if materiais:
            pdm_freq = defaultdict(int)
            for v in materiais:
                mid = v['catmat_item_id']
                cod_pdm = None
                if mid and mid in catmat_itens_full:
                    cod_pdm = catmat_itens_full[mid]['codigo_pdm']
                elif mid and mid in catmat_cod_full:
                    cod_pdm = catmat_cod_full[mid]['codigo_pdm']
                if cod_pdm and cod_pdm in pdm_by_cod:
                    info = pdm_by_cod[cod_pdm]
                    pdm_freq[(info['id'], info['codigo_classe'])] += 1
            if pdm_freq:
                (pdm_id, pdm_cod_cls) = max(pdm_freq, key=pdm_freq.get)
                upd['catmat_pdm_id'] = pdm_id
                upd['catmat_classe_id'] = cls_mat.get(pdm_cod_cls)

        tip_updates.append(upd)
        print(f'    [{siafe}] tipo={upd["tipo_contrato"]}, '
              f'catserv_classe={upd["catserv_classe_id"]}, grupo={upd["catserv_grupo_id"]}, '
              f'catmat_pdm={upd["catmat_pdm_id"]}, classe={upd["catmat_classe_id"]}')

    if executar and tip_updates:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for u in tip_updates:
            cur.execute("""
                UPDATE contratos SET
                    tipo_contrato = %s, catserv_classe_id = %s, catserv_grupo_id = %s,
                    catmat_pdm_id = %s, catmat_classe_id = %s, data_tipificacao = %s
                WHERE codigo = %s
            """, (u['tipo_contrato'], u['catserv_classe_id'], u['catserv_grupo_id'],
                  u['catmat_pdm_id'], u['catmat_classe_id'], now, u['siafe']))
        conn.commit()
        print(f'  -> {len(tip_updates)} contratos re-tipificados')

    return len(matched)


# ─── PASSO 6: Verificacao Final ─────────────────────────────────────────────

def passo6_verificacao(conn):
    print('\n' + '=' * 70)
    print('PASSO 6: Verificacao Final')
    print('=' * 70)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM contratos")
    print(f'  Total contratos: {cur.fetchone()[0]}')

    cur.execute("SELECT COUNT(*) FROM itens_vinculados")
    print(f'  Total vinculacoes: {cur.fetchone()[0]}')

    cur.execute("SELECT COUNT(*) FROM execucoes")
    print(f'  Total execucoes: {cur.fetchone()[0]}')

    # tipo_contrato
    try:
        cur.execute("""
            SELECT tipo_contrato, COUNT(*) FROM contratos
            WHERE tipo_contrato IS NOT NULL GROUP BY tipo_contrato ORDER BY tipo_contrato
        """)
        rows = cur.fetchall()
        print(f'\n  tipo_contrato:')
        for r in rows:
            label = {'S': 'Servico', 'M': 'Material', 'SM': 'Misto'}.get(r[0], r[0])
            print(f'    {r[0]} ({label}): {r[1]}')
        cur.execute("SELECT COUNT(*) FROM contratos WHERE tipo_contrato IS NULL")
        print(f'    NULL: {cur.fetchone()[0]}')
    except Exception as e:
        print(f'  Erro ao verificar tipo_contrato: {e}')

    # Tipificacao
    try:
        cur.execute("SELECT COUNT(*) FROM contratos WHERE catserv_classe_id IS NOT NULL")
        print(f'\n  Contratos com catserv_classe_id: {cur.fetchone()[0]}')
        cur.execute("SELECT COUNT(*) FROM contratos WHERE catserv_grupo_id IS NOT NULL")
        print(f'  Contratos com catserv_grupo_id: {cur.fetchone()[0]}')
        cur.execute("SELECT COUNT(*) FROM contratos WHERE catmat_pdm_id IS NOT NULL")
        print(f'  Contratos com catmat_pdm_id: {cur.fetchone()[0]}')
        cur.execute("SELECT COUNT(*) FROM contratos WHERE data_tipificacao IS NOT NULL")
        print(f'  Contratos com data_tipificacao: {cur.fetchone()[0]}')
    except Exception as e:
        print(f'  Erro ao verificar tipificacao: {e}')

    # Contratos especificos
    print(f'\n  Contratos especificos:')
    for cod in CONTRATOS_ESPECIFICOS:
        cur.execute("SELECT tipo_contrato, catserv_classe_id, catserv_grupo_id, catmat_pdm_id FROM contratos WHERE codigo = %s", (cod,))
        r = cur.fetchone()
        if r:
            cur.execute("SELECT COUNT(*) FROM itens_vinculados WHERE codigo_contrato = %s", (cod,))
            nv = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM execucoes WHERE codigo_contrato = %s", (cod,))
            ne = cur.fetchone()[0]
            print(f'    [{cod}] tipo={r[0]}, catserv_cls={r[1]}, catserv_grp={r[2]}, catmat_pdm={r[3]}, vinc={nv}, exec={ne}')
        else:
            print(f'    [{cod}] NAO ENCONTRADO')

    # Contratos com fix SM->S
    for cod in CONTRATOS_FIX_SM_PARA_S:
        cur.execute("SELECT tipo_contrato FROM contratos WHERE codigo = %s", (cod,))
        r = cur.fetchone()
        if r:
            print(f'    [{cod}] tipo_contrato = {r[0]} {"(OK)" if r[0] == "S" else "(VERIFICAR!)"}')


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    modo = 'EXECUTAR' if EXECUTAR else 'DRY-RUN'
    print(f'Deploy Producao Catch-up [{modo}]')
    print('=' * 70)
    print(f'Banco: {DB["database"]}@{DB["host"]}')
    print(f'CSV:   {CSV_PATH} {"(existe)" if os.path.exists(CSV_PATH) else "(NAO ENCONTRADO)"}')
    print(f'Excel vinc: {VINC_PATH} {"(existe)" if os.path.exists(VINC_PATH) else "(NAO ENCONTRADO)"}')
    print(f'Excel exec: {CORR_PATH} {"(existe)" if os.path.exists(CORR_PATH) else "(NAO ENCONTRADO)"}')
    print('=' * 70)

    conn = pymysql.connect(**DB)
    try:
        passo1_schema(conn, EXECUTAR)
        passo2_tipo_contrato_csv(conn, EXECUTAR)
        passo3_tipificacao(conn, EXECUTAR)
        passo4_fix_tipo_contrato(conn, EXECUTAR)
        passo5_reimport_especificos(conn, EXECUTAR)
        passo6_verificacao(conn)
    finally:
        conn.close()

    print('\n' + '=' * 70)
    print(f'Concluido [{modo}]')
    if not EXECUTAR:
        print('Use --executar para aplicar as alteracoes')
    print('=' * 70)


if __name__ == '__main__':
    main()
