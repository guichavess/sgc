'''
Reimportar Vinculacoes + Execucoes para contratos especificos
==============================================================
DRY-RUN por padrao. Use --executar para aplicar.

Uso:
  python scripts/reimportar_contratos_especificos.py                      # dry-run
  python scripts/reimportar_contratos_especificos.py --executar           # aplica
  python scripts/reimportar_contratos_especificos.py --contratos 25014859,25014928  # custom

Contratos padrao: 25014859, 25014928, 25014931, 25017768
'''
import os
import sys
import unicodedata
from datetime import datetime, date
from collections import defaultdict

import pandas as pd
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

VINC_PATH = os.path.join(BASE_DIR, 'itens vinculação.xlsx')
CORR_PATH = os.path.join(BASE_DIR, 'itens correção.xlsx')
DB = dict(host=os.getenv('DB_HOST', 'localhost'), user=os.getenv('DB_USER', 'root'),
          password=os.getenv('DB_PASS', ''), database=os.getenv('DB_NAME', 'sgc'), charset='utf8mb4')

EXECUTAR = '--executar' in sys.argv

# Contratos-alvo
CONTRATOS_PADRAO = ['25014859', '25014928', '25014931', '25017768']


def parse_contratos():
    """Pega contratos do argumento --contratos ou usa padrao."""
    for arg in sys.argv:
        if arg.startswith('--contratos='):
            return [c.strip() for c in arg.split('=', 1)[1].split(',')]
    for i, arg in enumerate(sys.argv):
        if arg == '--contratos' and i + 1 < len(sys.argv):
            return [c.strip() for c in sys.argv[i + 1].split(',')]
    return CONTRATOS_PADRAO


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


# ─── FASE 1: Reimportar Vinculacoes ─────────────────────────────────────────

def fase1_vinculacoes(conn, contratos_alvo, executar):
    """Remove vinculacoes existentes e reimporta do Excel para os contratos-alvo."""
    print('\n' + '=' * 60)
    print('FASE 1: Reimportar Vinculacoes')
    print('=' * 60)

    cur = conn.cursor()
    alvo_set = set(contratos_alvo)

    # 1a: Ler Excel
    print(f'[1a] Lendo {VINC_PATH}...')
    if not os.path.exists(VINC_PATH):
        print(f'  ERRO: Arquivo nao encontrado: {VINC_PATH}')
        return
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
        siafe_str = str(int(float(str(siafe))))
        if siafe_str not in alvo_set:
            continue
        registros.append({
            'siafe': siafe_str,
            'id': int(float(str(item_id))),
            'tipo': norm_tipo(tipo),
        })

    # Dedup
    dedup = {}
    for r in registros:
        k = (r['siafe'], r['tipo'], r['id'])
        dedup[k] = r
    print(f'  {len(dedup)} registros unicos nos contratos-alvo')

    # 1b: Validar IDs contra catalogos
    print('[1b] Validando contra catalogos...')
    cur.execute("SELECT codigo_servico FROM catserv_servicos")
    catserv = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT codigo FROM catmat_itens")
    catmat_itens = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT codigo FROM catmat_pdms")
    catmat_pdms = {r[0] for r in cur.fetchall()}

    validos = {}
    ignorados = 0
    for k, v in dedup.items():
        siafe, tipo, item_id = k
        if tipo == 'S' and item_id not in catserv:
            ignorados += 1
            continue
        if tipo == 'M' and item_id not in catmat_itens and item_id not in catmat_pdms:
            ignorados += 1
            continue
        validos[k] = v
    print(f'  {len(validos)} validos, {ignorados} ignorados (ID nao encontrado no catalogo)')

    # 1c: Contar e deletar existentes
    print('[1c] Removendo vinculacoes existentes...')
    placeholders = ','.join(['%s'] * len(contratos_alvo))
    cur.execute(f"SELECT COUNT(*) FROM itens_vinculados WHERE codigo_contrato IN ({placeholders})",
                contratos_alvo)
    count_antes = cur.fetchone()[0]
    print(f'  Vinculacoes existentes: {count_antes}')

    if executar:
        # Primeiro limpar referências em execuções
        cur.execute(f"""
            UPDATE execucoes SET item_vinculado_id = NULL
            WHERE codigo_contrato IN ({placeholders})
        """, contratos_alvo)
        # Depois deletar vinculações
        cur.execute(f"DELETE FROM itens_vinculados WHERE codigo_contrato IN ({placeholders})",
                    contratos_alvo)
        print(f'  -> {count_antes} vinculacoes removidas')
    else:
        print(f'  [DRY-RUN] {count_antes} vinculacoes seriam removidas')

    # 1d: Inserir novas
    print('[1d] Inserindo novas vinculacoes...')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    inserted = 0

    if executar:
        for k, v in validos.items():
            catserv_id = v['id'] if v['tipo'] == 'S' else None
            catmat_id = v['id'] if v['tipo'] == 'M' else None
            cur.execute("""
                INSERT INTO itens_vinculados
                    (codigo_contrato, tipo, catserv_servico_id, catmat_item_id, data_vinculacao)
                VALUES (%s, %s, %s, %s, %s)
            """, (v['siafe'], v['tipo'], catserv_id, catmat_id, now))
            inserted += 1
        conn.commit()
        print(f'  -> {inserted} vinculacoes inseridas')
    else:
        print(f'  [DRY-RUN] {len(validos)} vinculacoes seriam inseridas')

    # Resumo por contrato
    por_contrato = defaultdict(lambda: {'S': 0, 'M': 0})
    for k, v in validos.items():
        por_contrato[v['siafe']][v['tipo']] += 1
    print(f'\n  Resumo por contrato:')
    for c in contratos_alvo:
        s = por_contrato.get(c, {'S': 0, 'M': 0})
        print(f'    {c}: {s["S"]} servicos, {s["M"]} materiais')


# ─── FASE 2: Reimportar Execucoes ───────────────────────────────────────────

def fase2_execucoes(conn, contratos_alvo, executar):
    """Remove execucoes existentes e reimporta do Excel para os contratos-alvo."""
    print('\n' + '=' * 60)
    print('FASE 2: Reimportar Execucoes')
    print('=' * 60)

    cur = conn.cursor()
    alvo_set = set(contratos_alvo)

    # 2a: Ler Excel
    print(f'[2a] Lendo {CORR_PATH}...')
    if not os.path.exists(CORR_PATH):
        print(f'  ERRO: Arquivo nao encontrado: {CORR_PATH}')
        return
    df = pd.read_excel(CORR_PATH)
    registros = []
    for _, row in df.iterrows():
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
        registros.append({
            'siafe': siafe_str,
            'item': str(item).strip() if not pd.isna(item) else '',
            'data': dt,
            'mes': dt.month if dt else None,
            'ano': dt.year if dt else None,
            'valor': v,
            'qtde': q,
        })
    print(f'  {len(registros)} registros nos contratos-alvo')

    # 2b: Carregar vinculacoes atualizadas (recem-importadas)
    print('[2b] Carregando vinculacoes atualizadas...')
    placeholders = ','.join(['%s'] * len(contratos_alvo))
    cur.execute(f"""
        SELECT iv.id, iv.codigo_contrato, iv.tipo, iv.catserv_servico_id, iv.catmat_item_id
        FROM itens_vinculados iv
        WHERE iv.codigo_contrato IN ({placeholders})
    """, contratos_alvo)
    vinc_raw = cur.fetchall()
    print(f'  {len(vinc_raw)} vinculacoes carregadas')

    # Catalogo CATSERV: codigo_servico -> nome
    cur.execute("SELECT codigo_servico, nome FROM catserv_servicos")
    catserv = {r[0]: r[1] for r in cur.fetchall()}

    # Catalogo CATMAT
    cur.execute("SELECT id, codigo, descricao FROM catmat_itens")
    catmat_by_id = {}
    catmat_by_codigo = {}
    for r in cur.fetchall():
        catmat_by_id[r[0]] = {'codigo': r[1], 'descricao': r[2]}
        catmat_by_codigo[r[1]] = {'id': r[0], 'descricao': r[2]}

    # Montar vinculacoes indexadas
    vinc_por_contrato = defaultdict(list)
    for vid, cod, tipo, catserv_id, catmat_id in vinc_raw:
        desc = ''
        if tipo == 'S' and catserv_id and catserv_id in catserv:
            desc = catserv[catserv_id]
        elif tipo == 'M' and catmat_id:
            if catmat_id in catmat_by_id:
                desc = catmat_by_id[catmat_id]['descricao']
            elif catmat_id in catmat_by_codigo:
                desc = catmat_by_codigo[catmat_id]['descricao']
        vinc_por_contrato[str(cod)].append({
            'id': vid,
            'tipo': tipo,
            'catserv_servico_id': catserv_id,
            'catmat_item_id': catmat_id,
            'descricao': desc,
            'tokens': tokenize(desc),
        })

    # 2c: Match execucoes -> vinculacoes
    print('[2c] Fazendo match execucoes -> vinculacoes...')
    matched = []
    sem_vinculacao = []
    sem_valor = 0
    match_direto = 0
    match_fuzzy = 0
    match_falhou = []

    grupos_exec = defaultdict(list)
    for r in registros:
        if r['valor'] is None and r['qtde'] is None:
            sem_valor += 1
            continue
        key = (r['siafe'], norm(r['item']))
        grupos_exec[key].append(r)

    for (siafe, item_norm), rows in grupos_exec.items():
        vincs = vinc_por_contrato.get(siafe, [])
        if not vincs:
            sem_vinculacao.extend(rows)
            continue

        best_vinc = None
        if len(vincs) == 1:
            best_vinc = vincs[0]
            match_direto += len(rows)
        else:
            item_tokens = tokenize(item_norm)
            best_score = -1
            for v in vincs:
                score = jaccard(item_tokens, v['tokens'])
                if score > best_score:
                    best_score = score
                    best_vinc = v
            if best_score >= 0.15:
                match_fuzzy += len(rows)
            else:
                match_falhou.append({
                    'siafe': siafe,
                    'item': rows[0]['item'],
                    'score': best_score,
                    'vinc_desc': best_vinc['descricao'] if best_vinc else '',
                    'n_rows': len(rows),
                })
                match_fuzzy += len(rows)

        if best_vinc:
            for r in rows:
                matched.append({
                    **r,
                    'item_vinculado_id': best_vinc['id'],
                    'tipo': best_vinc['tipo'],
                    'catserv_servico_id': best_vinc['catserv_servico_id'] if best_vinc['tipo'] == 'S' else None,
                    'catmat_item_id': best_vinc['catmat_item_id'] if best_vinc['tipo'] == 'M' else None,
                })

    print(f'  Match direto: {match_direto}')
    print(f'  Match fuzzy:  {match_fuzzy}')
    print(f'  Sem vinculacao: {len(sem_vinculacao)}')
    print(f'  Sem valor/qtde: {sem_valor}')
    print(f'  Total para INSERT: {len(matched)}')

    if match_falhou:
        print(f'\n  Matches baixa confianca ({len(match_falhou)}):')
        for m in match_falhou[:10]:
            print(f'    [{m["siafe"]}] "{m["item"][:50]}" <-> "{m["vinc_desc"][:50]}" (score={m["score"]:.2f})')

    # 2d: Deletar execucoes existentes
    print(f'\n[2d] Removendo execucoes existentes...')
    cur.execute(f"SELECT COUNT(*) FROM execucoes WHERE codigo_contrato IN ({placeholders})",
                contratos_alvo)
    count_antes = cur.fetchone()[0]
    print(f'  Execucoes existentes: {count_antes}')

    if executar:
        cur.execute(f"DELETE FROM execucoes WHERE codigo_contrato IN ({placeholders})",
                    contratos_alvo)
        print(f'  -> {count_antes} execucoes removidas')
    else:
        print(f'  [DRY-RUN] {count_antes} execucoes seriam removidas')

    # 2e: Inserir novas
    print('[2e] Inserindo novas execucoes...')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if executar:
        inserted = 0
        for r in matched:
            dt = date(r['ano'], r['mes'], 1) if r['ano'] and r['mes'] else None
            cur.execute("""
                INSERT INTO execucoes
                    (codigo_contrato, item_vinculado_id, data, valor, quantidade,
                     mes, ano, tipo, catserv_servico_id, catmat_item_id, data_criacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['siafe'],
                r['item_vinculado_id'],
                dt,
                r['valor'],
                r['qtde'] or 1,
                r['mes'],
                r['ano'],
                r['tipo'],
                r['catserv_servico_id'],
                r['catmat_item_id'],
                now,
            ))
            inserted += 1
        conn.commit()
        print(f'  -> {inserted} execucoes inseridas')
    else:
        print(f'  [DRY-RUN] {len(matched)} execucoes seriam inseridas')

    # Resumo por contrato
    por_contrato = defaultdict(int)
    por_contrato_val = defaultdict(float)
    for r in matched:
        por_contrato[r['siafe']] += 1
        por_contrato_val[r['siafe']] += r['valor'] or 0

    print(f'\n  Resumo por contrato:')
    for c in contratos_alvo:
        n = por_contrato.get(c, 0)
        v = por_contrato_val.get(c, 0)
        print(f'    {c}: {n} execucoes, R$ {v:,.2f}')

    if sem_vinculacao:
        print(f'\n  Execucoes sem vinculacao ({len(sem_vinculacao)}):')
        por_c = defaultdict(list)
        for r in sem_vinculacao:
            por_c[r['siafe']].append(r)
        for siafe in sorted(por_c.keys()):
            rows = por_c[siafe]
            itens = set(r['item'] for r in rows)
            print(f'    [{siafe}] {len(rows)} exec:')
            for it in sorted(itens):
                print(f'      - {it[:80]}')


# ─── FASE 3: Re-tipificar contratos ─────────────────────────────────────────

def fase3_tipificacao(conn, contratos_alvo, executar):
    """Re-tipifica os contratos-alvo a partir das vinculacoes."""
    print('\n' + '=' * 60)
    print('FASE 3: Re-tipificacao')
    print('=' * 60)

    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(contratos_alvo))

    # Carregar vinculacoes
    cur.execute(f"""
        SELECT iv.codigo_contrato, iv.tipo, iv.catserv_servico_id, iv.catmat_item_id
        FROM itens_vinculados iv
        WHERE iv.codigo_contrato IN ({placeholders})
    """, contratos_alvo)
    vinc_raw = cur.fetchall()

    # Catalogos
    cur.execute("SELECT codigo_servico, codigo_classe, codigo_grupo FROM catserv_servicos")
    catserv = {}
    for r in cur.fetchall():
        catserv[r[0]] = {'codigo_classe': r[1], 'codigo_grupo': r[2]}

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

    # Agrupar por contrato
    vinc_por_contrato = defaultdict(list)
    for cod, tipo, catserv_id, catmat_id in vinc_raw:
        vinc_por_contrato[str(cod)].append({
            'tipo': tipo,
            'catserv_servico_id': catserv_id,
            'catmat_item_id': catmat_id,
        })

    # Tipificar
    updates = []
    for siafe in contratos_alvo:
        vincs = vinc_por_contrato.get(siafe, [])
        if not vincs:
            print(f'  [{siafe}] Sem vinculacoes, pulando')
            continue

        servicos = [v for v in vincs if v['tipo'] == 'S']
        materiais = [v for v in vincs if v['tipo'] == 'M']

        upd = {
            'siafe': siafe,
            'catserv_classe_id': None,
            'catserv_grupo_id': None,
            'catmat_pdm_id': None,
            'catmat_classe_id': None,
            'tipo_contrato': None,
        }

        # Determinar tipo_contrato
        has_s = len(servicos) > 0
        has_m = len(materiais) > 0
        if has_s and has_m:
            upd['tipo_contrato'] = 'SM'
        elif has_s:
            upd['tipo_contrato'] = 'S'
        elif has_m:
            upd['tipo_contrato'] = 'M'

        # SERVICO -> classe
        if servicos:
            classes_freq = defaultdict(int)
            grupos_freq = defaultdict(int)
            for v in servicos:
                sid = v['catserv_servico_id']
                if sid and sid in catserv:
                    cls = catserv[sid].get('codigo_classe')
                    grp = catserv[sid].get('codigo_grupo')
                    if cls:
                        classes_freq[cls] += 1
                    if grp:
                        grupos_freq[grp] += 1
            if classes_freq:
                upd['catserv_classe_id'] = max(classes_freq, key=classes_freq.get)
                # Derivar grupo da classe
                for sid_v in servicos:
                    sid = sid_v['catserv_servico_id']
                    if sid and sid in catserv and catserv[sid].get('codigo_classe') == upd['catserv_classe_id']:
                        upd['catserv_grupo_id'] = catserv[sid].get('codigo_grupo')
                        break
            elif grupos_freq:
                upd['catserv_grupo_id'] = max(grupos_freq, key=grupos_freq.get)

        # MATERIAL -> PDM -> classe
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
                    pdm_info = pdm_by_codigo[cod_pdm]
                    pdm_freq[(pdm_info['id'], pdm_info['codigo_classe'])] += 1

            if pdm_freq:
                (pdm_id, pdm_cod_classe) = max(pdm_freq, key=pdm_freq.get)
                upd['catmat_pdm_id'] = pdm_id
                upd['catmat_classe_id'] = classe_mat_by_codigo.get(pdm_cod_classe)

        updates.append(upd)
        print(f'  [{siafe}] tipo={upd["tipo_contrato"]}, '
              f'catserv_classe={upd["catserv_classe_id"]}, catserv_grupo={upd["catserv_grupo_id"]}, '
              f'catmat_pdm={upd["catmat_pdm_id"]}, catmat_classe={upd["catmat_classe_id"]}')

    # Aplicar
    if executar:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for upd in updates:
            cur.execute("""
                UPDATE contratos SET
                    tipo_contrato = %s,
                    catserv_classe_id = %s,
                    catserv_grupo_id = %s,
                    catmat_pdm_id = %s,
                    catmat_classe_id = %s,
                    data_tipificacao = %s
                WHERE codigo = %s
            """, (
                upd['tipo_contrato'],
                upd['catserv_classe_id'],
                upd['catserv_grupo_id'],
                upd['catmat_pdm_id'],
                upd['catmat_classe_id'],
                now,
                upd['siafe'],
            ))
        conn.commit()
        print(f'\n  -> {len(updates)} contratos re-tipificados')
    else:
        print(f'\n  [DRY-RUN] {len(updates)} contratos seriam re-tipificados')


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    modo = 'EXECUTAR' if EXECUTAR else 'DRY-RUN'
    contratos_alvo = parse_contratos()

    print(f'Reimportar Contratos Especificos [{modo}]')
    print('=' * 60)
    print(f'Contratos-alvo: {", ".join(contratos_alvo)}')
    print('=' * 60)

    conn = pymysql.connect(**DB)
    try:
        # Verificar se os contratos existem
        cur = conn.cursor()
        placeholders = ','.join(['%s'] * len(contratos_alvo))
        cur.execute(f"SELECT codigo FROM contratos WHERE codigo IN ({placeholders})", contratos_alvo)
        existentes = {str(r[0]) for r in cur.fetchall()}
        nao_encontrados = set(contratos_alvo) - existentes
        if nao_encontrados:
            print(f'\n  AVISO: Contratos nao encontrados no banco: {", ".join(nao_encontrados)}')
        contratos_validos = [c for c in contratos_alvo if c in existentes]
        if not contratos_validos:
            print('  ERRO: Nenhum contrato valido encontrado. Abortando.')
            return

        # Fase 1: Vinculacoes
        fase1_vinculacoes(conn, contratos_validos, EXECUTAR)

        # Fase 2: Execucoes
        fase2_execucoes(conn, contratos_validos, EXECUTAR)

        # Fase 3: Tipificacao
        fase3_tipificacao(conn, contratos_validos, EXECUTAR)

    finally:
        conn.close()

    print('\n' + '=' * 60)
    print(f'Concluido [{modo}]')
    if not EXECUTAR:
        print('Use --executar para aplicar as alteracoes')
    print('=' * 60)


if __name__ == '__main__':
    main()
