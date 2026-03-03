'''
Reimportar Execucoes com Vinculacao Correta + Engenharia Reversa Tipificacao
=============================================================================
DRY-RUN por padrao. Use --executar para aplicar.

Fases:
  1) ALTER TABLE: adicionar coluna item_vinculado_id em execucoes (se nao existir)
  2) TRUNCATE execucoes
  3) Reimportar de 'itens correcao.xlsx' vinculando com itens_vinculados
     - Match por contrato + descricao do catalogo (catserv/catmat) vs descricao do Excel
  4) Engenharia reversa: tipificar contratos a partir das vinculacoes
     - CATSERV: servico -> classe -> contratos.catserv_classe_id
     - CATMAT:  item -> PDM -> classe -> contratos.catmat_pdm_id + catmat_classe_id

Uso:
  python scripts/reimportar_execucoes.py            # dry-run
  python scripts/reimportar_execucoes.py --executar  # aplica no banco
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

CORR_PATH = os.path.join(BASE_DIR, 'itens correção.xlsx')
DB = dict(host=os.getenv('DB_HOST', 'localhost'), user=os.getenv('DB_USER', 'root'),
          password=os.getenv('DB_PASS', ''), database=os.getenv('DB_NAME', 'sgc'), charset='utf8mb4')

EXECUTAR = '--executar' in sys.argv


# ─── Utilidades ───────────────────────────────────────────────────────────────

def norm(t):
    """Normaliza texto: remove acentos, uppercase, strip."""
    if not t:
        return ''
    return unicodedata.normalize('NFKD', str(t)).encode('ascii', 'ignore').decode().upper().strip()


def tokenize(t):
    """Tokeniza texto normalizado para matching."""
    n = norm(t)
    return set(n.split()) if n else set()


def jaccard(s1, s2):
    """Similaridade de Jaccard entre dois conjuntos de tokens."""
    if not s1 or not s2:
        return 0.0
    inter = s1 & s2
    union = s1 | s2
    return len(inter) / len(union) if union else 0.0


# ─── Fase 0: Leitura de dados ────────────────────────────────────────────────

def ler_excel_execucoes():
    """Le o Excel de execucoes e retorna lista de dicts."""
    print('[0a] Lendo itens correcao.xlsx...')
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
        })
    print(f'  {len(registros)} registros lidos')
    return registros


def carregar_dados_db(conn):
    """Carrega dados necessarios do banco."""
    cur = conn.cursor()

    # Contratos existentes
    print('[0b] Carregando contratos...')
    cur.execute("SELECT codigo FROM contratos")
    contratos = {str(r[0]) for r in cur.fetchall()}
    print(f'  {len(contratos)} contratos')

    # Vinculacoes existentes (itens_vinculados)
    print('[0c] Carregando vinculacoes (itens_vinculados)...')
    cur.execute("""
        SELECT id, codigo_contrato, tipo, catserv_servico_id, catmat_item_id
        FROM itens_vinculados
    """)
    vinculacoes_raw = cur.fetchall()
    print(f'  {len(vinculacoes_raw)} vinculacoes')

    # Catalogo CATSERV: codigo_servico -> nome, codigo_classe, codigo_grupo
    print('[0d] Carregando catalogo CATSERV...')
    cur.execute("SELECT codigo_servico, nome, codigo_classe, codigo_grupo FROM catserv_servicos")
    catserv = {}
    for r in cur.fetchall():
        catserv[r[0]] = {'nome': r[1], 'codigo_classe': r[2], 'codigo_grupo': r[3]}
    print(f'  {len(catserv)} servicos')

    # Catalogo CATMAT: id -> codigo, descricao, codigo_pdm
    print('[0e] Carregando catalogo CATMAT (itens)...')
    cur.execute("SELECT id, codigo, descricao, codigo_pdm FROM catmat_itens")
    catmat_by_id = {}
    catmat_by_codigo = {}
    for r in cur.fetchall():
        catmat_by_id[r[0]] = {'codigo': r[1], 'descricao': r[2], 'codigo_pdm': r[3]}
        catmat_by_codigo[r[1]] = {'id': r[0], 'descricao': r[2], 'codigo_pdm': r[3]}
    print(f'  {len(catmat_by_id)} itens')

    # CATMAT PDMs: codigo -> id, codigo_classe
    print('[0f] Carregando CATMAT PDMs...')
    cur.execute("SELECT id, codigo, codigo_classe FROM catmat_pdms")
    pdm_by_codigo = {}
    for r in cur.fetchall():
        pdm_by_codigo[r[1]] = {'id': r[0], 'codigo_classe': r[2]}
    print(f'  {len(pdm_by_codigo)} PDMs')

    # CATMAT Classes: codigo -> id
    print('[0g] Carregando CATMAT Classes...')
    cur.execute("SELECT id, codigo FROM catmat_classes")
    classe_mat_by_codigo = {}
    for r in cur.fetchall():
        classe_mat_by_codigo[r[1]] = r[0]  # codigo -> id
    print(f'  {len(classe_mat_by_codigo)} classes')

    # Montar vinculacoes com descricao do catalogo
    vinc_por_contrato = defaultdict(list)  # siafe -> lista de vinculacoes
    for vid, cod_contrato, tipo, catserv_id, catmat_id in vinculacoes_raw:
        descricao_cat = ''
        if tipo == 'S' and catserv_id and catserv_id in catserv:
            descricao_cat = catserv[catserv_id]['nome']
        elif tipo == 'M' and catmat_id:
            if catmat_id in catmat_by_id:
                descricao_cat = catmat_by_id[catmat_id]['descricao']
            # Fallback: tentar como codigo
            elif catmat_id in catmat_by_codigo:
                descricao_cat = catmat_by_codigo[catmat_id]['descricao']

        vinc_por_contrato[str(cod_contrato)].append({
            'id': vid,
            'tipo': tipo,
            'catserv_servico_id': catserv_id,
            'catmat_item_id': catmat_id,
            'descricao': descricao_cat,
            'tokens': tokenize(descricao_cat),
        })

    return contratos, vinc_por_contrato, catserv, catmat_by_id, catmat_by_codigo, pdm_by_codigo, classe_mat_by_codigo


# ─── Fase 1: Schema migration ────────────────────────────────────────────────

def fase1_schema(conn, executar):
    """Adiciona coluna item_vinculado_id em execucoes se nao existir."""
    print('\n' + '=' * 60)
    print('FASE 1: Schema Migration')
    print('=' * 60)

    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'execucoes' AND COLUMN_NAME = 'item_vinculado_id'
    """, (DB['database'],))
    existe = cur.fetchone()[0] > 0

    if existe:
        print('  Coluna item_vinculado_id ja existe em execucoes')
    else:
        print('  Coluna item_vinculado_id NAO existe - precisa adicionar')
        if executar:
            cur.execute("""
                ALTER TABLE execucoes
                ADD COLUMN item_vinculado_id INT NULL,
                ADD INDEX idx_exec_vinculado (item_vinculado_id),
                ADD CONSTRAINT fk_exec_vinculado FOREIGN KEY (item_vinculado_id)
                    REFERENCES itens_vinculados(id) ON DELETE SET NULL
            """)
            conn.commit()
            print('  -> Coluna adicionada com sucesso')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')


# ─── Fase 2: Match execucoes -> vinculacoes ──────────────────────────────────

def fase2_match(registros, contratos, vinc_por_contrato):
    """Faz o match de cada execucao com a melhor vinculacao."""
    print('\n' + '=' * 60)
    print('FASE 2: Match Execucoes -> Vinculacoes')
    print('=' * 60)

    matched = []
    sem_contrato = 0
    sem_vinculacao = []
    sem_valor = 0
    match_direto = 0   # contrato com 1 vinculacao
    match_fuzzy = 0    # matched por similaridade
    match_falhou = []  # tem vinculacoes mas nao conseguiu matchear

    # Agrupar execucoes por (siafe, item_normalizado) para otimizar matching
    grupos_exec = defaultdict(list)
    for r in registros:
        if r['siafe'] not in contratos:
            sem_contrato += 1
            continue
        if r['valor'] is None and r['qtde'] is None:
            sem_valor += 1
            continue
        key = (r['siafe'], norm(r['item']))
        grupos_exec[key].append(r)

    print(f'  {len(grupos_exec)} grupos unicos (contrato+item)')
    print(f'  {sem_contrato} registros sem contrato no BD')
    print(f'  {sem_valor} registros sem valor/qtde')

    # Para cada grupo, encontrar a melhor vinculacao
    vinc_cache = {}  # (siafe, item_norm) -> vinculacao match
    contratos_sem_vinc = set()

    for (siafe, item_norm), rows in grupos_exec.items():
        vincs = vinc_por_contrato.get(siafe, [])

        if not vincs:
            contratos_sem_vinc.add(siafe)
            sem_vinculacao.extend(rows)
            continue

        best_vinc = None

        if len(vincs) == 1:
            # Unica vinculacao -> match direto
            best_vinc = vincs[0]
            match_direto += len(rows)
        else:
            # Multiplas vinculacoes -> fuzzy match
            item_tokens = tokenize(item_norm)
            best_score = -1

            for v in vincs:
                score = jaccard(item_tokens, v['tokens'])
                if score > best_score:
                    best_score = score
                    best_vinc = v

            if best_score >= 0.15:  # threshold minimo
                match_fuzzy += len(rows)
            else:
                # Score muito baixo, reportar mas usar mesmo assim (melhor match)
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

    print(f'\n  Resultado do matching:')
    print(f'    Match direto (1 vinculacao):  {match_direto}')
    print(f'    Match fuzzy  (N vinculacoes): {match_fuzzy}')
    print(f'    Sem vinculacao no contrato:   {len(sem_vinculacao)}')
    print(f'    Total matched para INSERT:    {len(matched)}')

    if contratos_sem_vinc:
        print(f'\n  Contratos sem nenhuma vinculacao ({len(contratos_sem_vinc)}):')
        for c in sorted(contratos_sem_vinc):
            print(f'    - {c}')

    if match_falhou:
        print(f'\n  Matches com baixa confianca ({len(match_falhou)}):')
        for m in match_falhou[:20]:
            print(f'    - [{m["siafe"]}] "{m["item"][:50]}" <-> "{m["vinc_desc"][:50]}" (score={m["score"]:.2f}, {m["n_rows"]} rows)')

    return matched, sem_vinculacao


# ─── Fase 3: TRUNCATE + INSERT ───────────────────────────────────────────────

def fase3_importar(conn, matched, executar):
    """Trunca execucoes e insere os registros matched."""
    print('\n' + '=' * 60)
    print('FASE 3: TRUNCATE + INSERT Execucoes')
    print('=' * 60)

    cur = conn.cursor()

    if executar:
        # Contar registros atuais
        cur.execute("SELECT COUNT(*) FROM execucoes")
        antes = cur.fetchone()[0]
        print(f'  Registros antes: {antes}')

        # TRUNCATE
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        cur.execute("TRUNCATE TABLE execucoes")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        print(f'  TRUNCATE executado')

        # INSERT
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
        print(f'  {inserted} registros inseridos')
    else:
        print(f'  [DRY-RUN] TRUNCATE + {len(matched)} INSERTs seriam executados')


# ─── Fase 4: Engenharia reversa tipificacao ──────────────────────────────────

def fase4_tipificacao(conn, vinc_por_contrato, catserv, catmat_by_id, catmat_by_codigo,
                       pdm_by_codigo, classe_mat_by_codigo, executar):
    """Tipifica contratos a partir das vinculacoes."""
    print('\n' + '=' * 60)
    print('FASE 4: Engenharia Reversa - Tipificacao')
    print('=' * 60)

    cur = conn.cursor()

    updates_serv = []   # (codigo_contrato, catserv_classe_id)
    updates_mat = []    # (codigo_contrato, catmat_pdm_id, catmat_classe_id)
    sem_classe_serv = []
    sem_pdm_mat = []
    conflitos = []

    for siafe, vincs in vinc_por_contrato.items():
        # Separar por tipo
        servicos = [v for v in vincs if v['tipo'] == 'S']
        materiais = [v for v in vincs if v['tipo'] == 'M']

        # --- SERVICOS: servico -> classe ---
        if servicos:
            classes_serv = set()
            for v in servicos:
                sid = v['catserv_servico_id']
                if sid and sid in catserv:
                    cls = catserv[sid].get('codigo_classe')
                    if cls:
                        classes_serv.add(cls)

            if len(classes_serv) == 1:
                updates_serv.append((siafe, classes_serv.pop()))
            elif len(classes_serv) > 1:
                # Multiplas classes - usar a mais frequente
                freq = defaultdict(int)
                for v in servicos:
                    sid = v['catserv_servico_id']
                    if sid and sid in catserv:
                        cls = catserv[sid].get('codigo_classe')
                        if cls:
                            freq[cls] += 1
                best = max(freq, key=freq.get)
                updates_serv.append((siafe, best))
                conflitos.append({
                    'siafe': siafe,
                    'tipo': 'CATSERV',
                    'classes': list(classes_serv),
                    'escolhida': best,
                })
            elif not classes_serv:
                # Servicos sem classe (pertencem direto ao grupo)
                grupos = set()
                for v in servicos:
                    sid = v['catserv_servico_id']
                    if sid and sid in catserv:
                        grp = catserv[sid].get('codigo_grupo')
                        if grp:
                            grupos.add(grp)
                sem_classe_serv.append({
                    'siafe': siafe,
                    'servicos': [v['catserv_servico_id'] for v in servicos],
                    'grupos': list(grupos),
                })

        # --- MATERIAIS: item -> PDM -> classe ---
        if materiais:
            pdms_found = set()      # (pdm_id, pdm_codigo_classe)
            for v in materiais:
                mid = v['catmat_item_id']
                if mid and mid in catmat_by_id:
                    codigo_pdm = catmat_by_id[mid]['codigo_pdm']
                    if codigo_pdm in pdm_by_codigo:
                        pdm_info = pdm_by_codigo[codigo_pdm]
                        pdms_found.add((pdm_info['id'], pdm_info['codigo_classe']))
                elif mid and mid in catmat_by_codigo:
                    # Fallback: catmat_item_id pode ainda ser um codigo
                    codigo_pdm = catmat_by_codigo[mid]['codigo_pdm']
                    if codigo_pdm in pdm_by_codigo:
                        pdm_info = pdm_by_codigo[codigo_pdm]
                        pdms_found.add((pdm_info['id'], pdm_info['codigo_classe']))

            if pdms_found:
                # Se multiplos PDMs, pegar o mais frequente
                if len(pdms_found) == 1:
                    pdm_id, pdm_cod_classe = pdms_found.pop()
                else:
                    freq_pdm = defaultdict(int)
                    for v in materiais:
                        mid = v['catmat_item_id']
                        if mid and mid in catmat_by_id:
                            codigo_pdm = catmat_by_id[mid]['codigo_pdm']
                            if codigo_pdm in pdm_by_codigo:
                                key = (pdm_by_codigo[codigo_pdm]['id'], pdm_by_codigo[codigo_pdm]['codigo_classe'])
                                freq_pdm[key] += 1
                    if not freq_pdm:
                        # Fallback: usar o primeiro PDM encontrado
                        pdm_id, pdm_cod_classe = pdms_found.pop()
                    else:
                        best_pdm = max(freq_pdm, key=freq_pdm.get)
                        pdm_id, pdm_cod_classe = best_pdm
                    conflitos.append({
                        'siafe': siafe,
                        'tipo': 'CATMAT',
                        'pdms': list(pdms_found),
                        'escolhido': (pdm_id, pdm_cod_classe),
                    })

                # Resolver classe
                classe_id = classe_mat_by_codigo.get(pdm_cod_classe)
                updates_mat.append((siafe, pdm_id, classe_id))

                if not classe_id:
                    sem_pdm_mat.append({
                        'siafe': siafe,
                        'pdm_id': pdm_id,
                        'pdm_cod_classe': pdm_cod_classe,
                    })
            else:
                sem_pdm_mat.append({
                    'siafe': siafe,
                    'materiais': [v['catmat_item_id'] for v in materiais],
                })

    print(f'\n  Tipificacao SERVICO: {len(updates_serv)} contratos')
    print(f'  Tipificacao MATERIAL: {len(updates_mat)} contratos')
    print(f'  Servicos sem classe (direto no grupo): {len(sem_classe_serv)}')
    print(f'  Materiais sem PDM resolvido: {len(sem_pdm_mat)}')
    print(f'  Conflitos (multiplas classes/PDMs): {len(conflitos)}')

    if sem_classe_serv:
        print(f'\n  Servicos sem classe (codigo_classe=NULL):')
        for s in sem_classe_serv[:10]:
            print(f'    - [{s["siafe"]}] servicos={s["servicos"]} grupos={s["grupos"]}')

    if sem_pdm_mat:
        print(f'\n  Materiais sem PDM:')
        for s in sem_pdm_mat[:10]:
            print(f'    - [{s["siafe"]}] {s}')

    if conflitos:
        print(f'\n  Conflitos (usou mais frequente):')
        for c in conflitos[:10]:
            print(f'    - [{c["siafe"]}] {c["tipo"]}: opcoes={c.get("classes", c.get("pdms"))} -> {c.get("escolhida", c.get("escolhido"))}')

    # Aplicar tipificacao
    if executar:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for siafe, classe_id in updates_serv:
            cur.execute("""
                UPDATE contratos
                SET catserv_classe_id = %s, data_tipificacao = %s
                WHERE codigo = %s AND catserv_classe_id IS NULL
            """, (classe_id, now, siafe))

        for siafe, pdm_id, classe_id in updates_mat:
            cur.execute("""
                UPDATE contratos
                SET catmat_pdm_id = %s, catmat_classe_id = %s, data_tipificacao = %s
                WHERE codigo = %s AND catmat_pdm_id IS NULL
            """, (pdm_id, classe_id, now, siafe))

        conn.commit()
        print(f'\n  -> {len(updates_serv)} contratos tipificados (SERV)')
        print(f'  -> {len(updates_mat)} contratos tipificados (MAT)')
    else:
        print(f'\n  [DRY-RUN] {len(updates_serv)} UPDATEs SERV + {len(updates_mat)} UPDATEs MAT seriam executados')

    return updates_serv, updates_mat


# ─── Fase 5: Relatorio de execucoes sem vinculacao ───────────────────────────

def fase5_relatorio_sem_vinculacao(sem_vinculacao):
    """Mostra execucoes que nao puderam ser vinculadas."""
    if not sem_vinculacao:
        return

    print('\n' + '=' * 60)
    print('FASE 5: Execucoes sem vinculacao (nao importadas)')
    print('=' * 60)

    # Agrupar por contrato
    por_contrato = defaultdict(list)
    for r in sem_vinculacao:
        por_contrato[r['siafe']].append(r)

    print(f'  {len(sem_vinculacao)} execucoes em {len(por_contrato)} contratos sem vinculacao')
    print(f'\n  Detalhamento:')
    for siafe in sorted(por_contrato.keys()):
        rows = por_contrato[siafe]
        itens = set(r['item'] for r in rows)
        total_val = sum(r['valor'] or 0 for r in rows)
        print(f'    [{siafe}] {len(rows)} exec, R$ {total_val:,.2f}')
        for it in sorted(itens):
            print(f'      - {it[:80]}')


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    modo = 'EXECUTAR' if EXECUTAR else 'DRY-RUN'
    print(f'Reimportar Execucoes com Vinculacao [{modo}]')
    print('=' * 60)

    # Fase 0: Leitura
    registros = ler_excel_execucoes()

    conn = pymysql.connect(**DB)
    try:
        (contratos, vinc_por_contrato, catserv,
         catmat_by_id, catmat_by_codigo,
         pdm_by_codigo, classe_mat_by_codigo) = carregar_dados_db(conn)

        # Fase 1: Schema
        fase1_schema(conn, EXECUTAR)

        # Fase 2: Match
        matched, sem_vinculacao = fase2_match(registros, contratos, vinc_por_contrato)

        # Fase 3: TRUNCATE + INSERT
        fase3_importar(conn, matched, EXECUTAR)

        # Fase 4: Tipificacao
        fase4_tipificacao(conn, vinc_por_contrato, catserv, catmat_by_id, catmat_by_codigo,
                          pdm_by_codigo, classe_mat_by_codigo, EXECUTAR)

        # Fase 5: Relatorio
        fase5_relatorio_sem_vinculacao(sem_vinculacao)

    finally:
        conn.close()

    print('\n' + '=' * 60)
    print(f'Concluido [{modo}]')
    if not EXECUTAR:
        print('Use --executar para aplicar as alteracoes')
    print('=' * 60)


if __name__ == '__main__':
    main()
