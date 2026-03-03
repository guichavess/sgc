'''
Tipificar Contratos via Engenharia Reversa
===========================================
DRY-RUN por padrao. Use --executar para aplicar.
Use --force para sobrescrever tipificacoes existentes.

Logica:
  Para cada contrato com vinculacoes (itens_vinculados):
    CATSERV (servicos):
      servico.codigo_classe != NULL -> contratos.catserv_classe_id  (nivel classe)
      servico.codigo_classe == NULL -> contratos.catserv_grupo_id   (fallback grupo)
    CATMAT (materiais):
      item.id -> item.codigo_pdm -> pdm.id        -> contratos.catmat_pdm_id
      item.id -> item.codigo_pdm -> pdm.codigo_classe -> classe.id -> contratos.catmat_classe_id

Uso:
  python scripts/tipificar_contratos.py                   # dry-run
  python scripts/tipificar_contratos.py --executar        # aplica so onde NULL
  python scripts/tipificar_contratos.py --executar --force # sobrescreve tudo
'''
import os
import sys
from datetime import datetime
from collections import defaultdict

import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DB = dict(host=os.getenv('DB_HOST', 'localhost'), user=os.getenv('DB_USER', 'root'),
          password=os.getenv('DB_PASS', ''), database=os.getenv('DB_NAME', 'sgc'), charset='utf8mb4')

EXECUTAR = '--executar' in sys.argv
FORCE = '--force' in sys.argv


# ─── Fase 0: Schema migration ────────────────────────────────────────────────

def fase0_schema(conn, executar):
    """Adiciona coluna catserv_grupo_id em contratos se nao existir."""
    print('\n[0/5] Schema Migration...')
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'contratos' AND COLUMN_NAME = 'catserv_grupo_id'
    """, (DB['database'],))
    existe = cur.fetchone()[0] > 0

    if existe:
        print('  Coluna catserv_grupo_id ja existe em contratos')
    else:
        print('  Coluna catserv_grupo_id NAO existe - precisa adicionar')
        if executar:
            cur.execute("""
                ALTER TABLE contratos
                ADD COLUMN catserv_grupo_id INT NULL AFTER catserv_classe_id,
                ADD INDEX idx_contrato_catserv_grupo (catserv_grupo_id)
            """)
            conn.commit()
            print('  -> Coluna adicionada com sucesso')
        else:
            print('  -> [DRY-RUN] ALTER TABLE seria executado')


def main():
    modo = 'EXECUTAR' + (' + FORCE' if FORCE else '') if EXECUTAR else 'DRY-RUN'
    print(f'Tipificar Contratos [{modo}]')
    print('=' * 70)

    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    # ─── Fase 0: Schema ──────────────────────────────────────────────────

    fase0_schema(conn, EXECUTAR)

    # ─── 1. Carregar dados de referencia ──────────────────────────────────

    print('\n[1/5] Carregando dados de referencia...')

    # CATSERV: servicos
    cur.execute("SELECT codigo_servico, codigo_classe, codigo_grupo, nome FROM catserv_servicos")
    catserv_servicos = {}
    for r in cur.fetchall():
        catserv_servicos[r[0]] = {'codigo_classe': r[1], 'codigo_grupo': r[2], 'nome': r[3]}
    print(f'  {len(catserv_servicos)} servicos CATSERV')

    # CATSERV: classes
    cur.execute("SELECT codigo_classe, codigo_grupo, nome FROM catserv_classes")
    catserv_classes = {}
    for r in cur.fetchall():
        catserv_classes[r[0]] = {'codigo_grupo': r[1], 'nome': r[2]}
    print(f'  {len(catserv_classes)} classes CATSERV')

    # CATSERV: grupos
    cur.execute("SELECT codigo_grupo, codigo_divisao, nome FROM catserv_grupos")
    catserv_grupos = {}
    for r in cur.fetchall():
        catserv_grupos[r[0]] = {'codigo_divisao': r[1], 'nome': r[2]}
    print(f'  {len(catserv_grupos)} grupos CATSERV')

    # CATMAT: itens (id -> codigo_pdm)
    cur.execute("SELECT id, codigo, codigo_pdm, descricao FROM catmat_itens")
    catmat_itens = {}
    for r in cur.fetchall():
        catmat_itens[r[0]] = {'codigo': r[1], 'codigo_pdm': r[2], 'descricao': r[3]}
    print(f'  {len(catmat_itens)} itens CATMAT')

    # CATMAT: PDMs (codigo -> id, codigo_classe)
    cur.execute("SELECT id, codigo, codigo_classe, nome FROM catmat_pdms")
    catmat_pdms_by_codigo = {}
    for r in cur.fetchall():
        catmat_pdms_by_codigo[r[1]] = {'id': r[0], 'codigo_classe': r[2], 'nome': r[3]}
    print(f'  {len(catmat_pdms_by_codigo)} PDMs CATMAT')

    # CATMAT: Classes (codigo -> id)
    cur.execute("SELECT id, codigo, codigo_grupo, nome FROM catmat_classes")
    catmat_classes_by_codigo = {}
    for r in cur.fetchall():
        catmat_classes_by_codigo[r[1]] = {'id': r[0], 'codigo_grupo': r[2], 'nome': r[3]}
    print(f'  {len(catmat_classes_by_codigo)} classes CATMAT')

    # ─── 2. Carregar vinculacoes ──────────────────────────────────────────

    print('\n[2/5] Carregando vinculacoes...')
    cur.execute("""
        SELECT iv.id, iv.codigo_contrato, iv.tipo, iv.catserv_servico_id, iv.catmat_item_id
        FROM itens_vinculados iv
        JOIN contratos c ON c.codigo = iv.codigo_contrato
        ORDER BY iv.codigo_contrato, iv.tipo
    """)
    vincs_raw = cur.fetchall()

    vinc_por_contrato = defaultdict(list)
    for vid, cod, tipo, catserv_id, catmat_id in vincs_raw:
        vinc_por_contrato[cod].append({
            'id': vid, 'tipo': tipo,
            'catserv_servico_id': catserv_id,
            'catmat_item_id': catmat_id,
        })
    print(f'  {len(vincs_raw)} vinculacoes em {len(vinc_por_contrato)} contratos')

    # ─── 3. Carregar estado atual dos contratos ───────────────────────────

    print('\n[3/5] Carregando estado atual dos contratos...')

    # Verificar se coluna catserv_grupo_id existe no SELECT
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'contratos' AND COLUMN_NAME = 'catserv_grupo_id'
    """, (DB['database'],))
    has_grupo_col = cur.fetchone()[0] > 0

    if has_grupo_col:
        cur.execute("""
            SELECT codigo, catserv_classe_id, catserv_grupo_id, catmat_pdm_id, catmat_classe_id, data_tipificacao
            FROM contratos
            WHERE codigo IN ({})
        """.format(','.join(f"'{c}'" for c in vinc_por_contrato.keys())))
        contratos_estado = {}
        for r in cur.fetchall():
            contratos_estado[r[0]] = {
                'catserv_classe_id': r[1],
                'catserv_grupo_id': r[2],
                'catmat_pdm_id': r[3],
                'catmat_classe_id': r[4],
                'data_tipificacao': r[5],
            }
    else:
        cur.execute("""
            SELECT codigo, catserv_classe_id, catmat_pdm_id, catmat_classe_id, data_tipificacao
            FROM contratos
            WHERE codigo IN ({})
        """.format(','.join(f"'{c}'" for c in vinc_por_contrato.keys())))
        contratos_estado = {}
        for r in cur.fetchall():
            contratos_estado[r[0]] = {
                'catserv_classe_id': r[1],
                'catserv_grupo_id': None,
                'catmat_pdm_id': r[2],
                'catmat_classe_id': r[3],
                'data_tipificacao': r[4],
            }
    print(f'  {len(contratos_estado)} contratos carregados')

    # ─── 4. Calcular tipificacao ──────────────────────────────────────────

    print('\n[4/5] Calculando tipificacao...')
    print('-' * 70)

    updates = []
    conflitos = []
    grupo_fallback = []     # contratos tipificados no nivel grupo (sem classe)

    for siafe, vincs in sorted(vinc_por_contrato.items()):
        estado = contratos_estado.get(siafe, {})
        servicos = [v for v in vincs if v['tipo'] == 'S']
        materiais = [v for v in vincs if v['tipo'] == 'M']

        upd = {
            'siafe': siafe,
            'catserv_classe_id': None,
            'catserv_grupo_id': None,
            'catmat_pdm_id': None,
            'catmat_classe_id': None,
        }
        mudou = False

        # ── CATSERV ──────────────────────────────────────────────────────
        if servicos:
            freq_classes = defaultdict(int)
            freq_grupos = defaultdict(int)
            servicos_info = []

            for v in servicos:
                sid = v['catserv_servico_id']
                if sid and sid in catserv_servicos:
                    srv = catserv_servicos[sid]
                    cls = srv['codigo_classe']
                    grp = srv['codigo_grupo']
                    if cls:
                        freq_classes[cls] += 1
                    else:
                        freq_grupos[grp] += 1
                    servicos_info.append({'sid': sid, 'classe': cls, 'grupo': grp, 'nome': srv['nome']})

            if freq_classes:
                # Tem pelo menos 1 servico com classe -> tipificar por classe
                if len(freq_classes) == 1:
                    classe_escolhida = list(freq_classes.keys())[0]
                else:
                    classe_escolhida = max(freq_classes, key=freq_classes.get)
                    conflitos.append({
                        'siafe': siafe, 'tipo': 'CATSERV-CLASSE',
                        'opcoes': dict(freq_classes),
                        'escolhida': classe_escolhida,
                        'nomes': {c: catserv_classes[c]['nome'] for c in freq_classes if c in catserv_classes},
                    })

                # Derivar grupo da classe
                grupo_da_classe = catserv_classes.get(classe_escolhida, {}).get('codigo_grupo')

                atual_cls = estado.get('catserv_classe_id')
                atual_grp = estado.get('catserv_grupo_id')
                if FORCE or atual_cls is None:
                    upd['catserv_classe_id'] = classe_escolhida
                    upd['catserv_grupo_id'] = grupo_da_classe
                    if classe_escolhida != atual_cls or grupo_da_classe != atual_grp:
                        mudou = True

            elif freq_grupos:
                # NENHUM servico tem classe -> fallback para grupo
                if len(freq_grupos) == 1:
                    grupo_escolhido = list(freq_grupos.keys())[0]
                else:
                    grupo_escolhido = max(freq_grupos, key=freq_grupos.get)
                    conflitos.append({
                        'siafe': siafe, 'tipo': 'CATSERV-GRUPO',
                        'opcoes': dict(freq_grupos),
                        'escolhida': grupo_escolhido,
                        'nomes': {g: catserv_grupos[g]['nome'] for g in freq_grupos if g in catserv_grupos},
                    })

                atual_grp = estado.get('catserv_grupo_id')
                if FORCE or (estado.get('catserv_classe_id') is None and atual_grp is None):
                    upd['catserv_grupo_id'] = grupo_escolhido
                    # catserv_classe_id permanece NULL
                    if grupo_escolhido != atual_grp:
                        mudou = True

                grp_info = catserv_grupos.get(grupo_escolhido, {})
                grupo_fallback.append({
                    'siafe': siafe,
                    'grupo_id': grupo_escolhido,
                    'grupo_nome': grp_info.get('nome', '?'),
                    'servicos': [s['sid'] for s in servicos_info],
                })

        # ── CATMAT ───────────────────────────────────────────────────────
        if materiais:
            freq_pdms = defaultdict(int)

            for v in materiais:
                mid = v['catmat_item_id']
                if mid and mid in catmat_itens:
                    item = catmat_itens[mid]
                    cod_pdm = item['codigo_pdm']
                    if cod_pdm in catmat_pdms_by_codigo:
                        pdm = catmat_pdms_by_codigo[cod_pdm]
                        freq_pdms[(pdm['id'], pdm['codigo_classe'])] += 1

            if freq_pdms:
                if len(freq_pdms) == 1:
                    (pdm_id, pdm_cod_classe) = list(freq_pdms.keys())[0]
                else:
                    (pdm_id, pdm_cod_classe) = max(freq_pdms, key=freq_pdms.get)
                    conflitos.append({
                        'siafe': siafe, 'tipo': 'CATMAT',
                        'opcoes': {str(k): v for k, v in freq_pdms.items()},
                        'escolhido': (pdm_id, pdm_cod_classe),
                    })

                classe_id = catmat_classes_by_codigo.get(pdm_cod_classe, {}).get('id')

                atual_pdm = estado.get('catmat_pdm_id')
                atual_classe = estado.get('catmat_classe_id')
                if FORCE or atual_pdm is None:
                    upd['catmat_pdm_id'] = pdm_id
                    upd['catmat_classe_id'] = classe_id
                    if pdm_id != atual_pdm or classe_id != atual_classe:
                        mudou = True

        if mudou:
            updates.append(upd)

    # ─── Relatorio ────────────────────────────────────────────────────────

    upd_classe = [u for u in updates if u['catserv_classe_id'] is not None]
    upd_grupo = [u for u in updates if u['catserv_classe_id'] is None and u['catserv_grupo_id'] is not None]
    upd_mat = [u for u in updates if u['catmat_pdm_id'] is not None]

    print(f'\n  RESULTADO:')
    print(f'    Total contratos com vinculacoes:  {len(vinc_por_contrato)}')
    print(f'    Updates a aplicar:                {len(updates)}')
    print(f'      - CATSERV nivel classe:         {len(upd_classe)}')
    print(f'      - CATSERV nivel grupo (fallback):{len(upd_grupo)}')
    print(f'      - CATMAT (PDM+classe):          {len(upd_mat)}')
    print(f'    Conflitos resolvidos:              {len(conflitos)}')

    if updates:
        print(f'\n  DETALHAMENTO DOS UPDATES:')
        for u in updates:
            partes = []
            if u['catserv_classe_id']:
                cls = catserv_classes.get(u['catserv_classe_id'], {})
                partes.append(f"SERV classe={u['catserv_classe_id']} ({cls.get('nome', '?')[:40]})")
            elif u['catserv_grupo_id']:
                grp = catserv_grupos.get(u['catserv_grupo_id'], {})
                partes.append(f"SERV *grupo*={u['catserv_grupo_id']} ({grp.get('nome', '?')[:40]})")
            if u['catmat_pdm_id']:
                partes.append(f"MAT pdm_id={u['catmat_pdm_id']} classe_id={u['catmat_classe_id']}")
            print(f"    [{u['siafe']}] {' | '.join(partes)}")

    if grupo_fallback:
        print(f'\n  GRUPO FALLBACK ({len(grupo_fallback)} contratos - classe NULL no servico):')
        for g in grupo_fallback:
            print(f"    [{g['siafe']}] grupo={g['grupo_id']} ({g['grupo_nome'][:50]}) servicos={g['servicos']}")

    if conflitos:
        print(f'\n  CONFLITOS ({len(conflitos)}):')
        for c in conflitos[:15]:
            tipo = c['tipo']
            if 'CATSERV' in tipo:
                opcoes_str = ', '.join(f"{k}({v}x): {c['nomes'].get(k, '?')[:30]}" for k, v in c['opcoes'].items())
                print(f"    [{c['siafe']}] {tipo}: {opcoes_str}")
                print(f"      -> Escolhida: {c['escolhida']} ({c['nomes'].get(c['escolhida'], '?')})")
            else:
                print(f"    [{c['siafe']}] {tipo}: opcoes={c['opcoes']} -> {c['escolhido']}")
        if len(conflitos) > 15:
            print(f'    ... +{len(conflitos) - 15} conflitos')

    # ─── 5. Aplicar ──────────────────────────────────────────────────────

    print(f'\n[5/5] {"Aplicando" if EXECUTAR else "Simulando"}...')
    print('-' * 70)

    if EXECUTAR and updates:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        aplicados = 0

        for u in updates:
            sets = ['data_tipificacao = %s']
            params = [now]

            if u['catserv_classe_id'] is not None:
                sets.append('catserv_classe_id = %s')
                params.append(u['catserv_classe_id'])
                # Tambem setar o grupo (derivado da classe)
                sets.append('catserv_grupo_id = %s')
                params.append(u['catserv_grupo_id'])
            elif u['catserv_grupo_id'] is not None:
                # Fallback: so grupo, sem classe
                sets.append('catserv_grupo_id = %s')
                params.append(u['catserv_grupo_id'])

            if u['catmat_pdm_id'] is not None:
                sets.append('catmat_pdm_id = %s')
                params.append(u['catmat_pdm_id'])

            if u['catmat_classe_id'] is not None:
                sets.append('catmat_classe_id = %s')
                params.append(u['catmat_classe_id'])

            params.append(u['siafe'])
            sql = f"UPDATE contratos SET {', '.join(sets)} WHERE codigo = %s"
            cur.execute(sql, params)
            aplicados += 1

        conn.commit()
        print(f'  {aplicados} contratos atualizados')
    elif EXECUTAR:
        print(f'  Nenhum update necessario')
    else:
        print(f'  [DRY-RUN] {len(updates)} UPDATEs seriam executados')
        if not FORCE:
            print(f'  Nota: Use --force para sobrescrever tipificacoes existentes')

    # ─── Resumo final ────────────────────────────────────────────────────

    print('\n' + '=' * 70)
    cur.execute("SELECT COUNT(*) FROM contratos WHERE catserv_classe_id IS NOT NULL")
    print(f'  Contratos com catserv_classe_id:  {cur.fetchone()[0]}')

    if has_grupo_col or EXECUTAR:
        try:
            cur.execute("SELECT COUNT(*) FROM contratos WHERE catserv_grupo_id IS NOT NULL")
            print(f'  Contratos com catserv_grupo_id:   {cur.fetchone()[0]}')
        except Exception:
            pass

    cur.execute("SELECT COUNT(*) FROM contratos WHERE catmat_pdm_id IS NOT NULL")
    print(f'  Contratos com catmat_pdm_id:      {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(*) FROM contratos WHERE data_tipificacao IS NOT NULL")
    print(f'  Contratos com data_tipificacao:   {cur.fetchone()[0]}')

    # Vinculados sem nenhuma tipificacao
    if has_grupo_col or EXECUTAR:
        try:
            cur.execute("""
                SELECT COUNT(DISTINCT iv.codigo_contrato)
                FROM itens_vinculados iv
                JOIN contratos c ON c.codigo = iv.codigo_contrato
                WHERE c.catserv_classe_id IS NULL
                  AND c.catserv_grupo_id IS NULL
                  AND c.catmat_pdm_id IS NULL
            """)
            print(f'  Vinculados sem tipificacao:       {cur.fetchone()[0]}')
        except Exception:
            pass
    else:
        cur.execute("""
            SELECT COUNT(DISTINCT iv.codigo_contrato)
            FROM itens_vinculados iv
            JOIN contratos c ON c.codigo = iv.codigo_contrato
            WHERE c.catserv_classe_id IS NULL AND c.catmat_pdm_id IS NULL
        """)
        print(f'  Vinculados sem tipificacao:       {cur.fetchone()[0]}')

    print('=' * 70)
    conn.close()


if __name__ == '__main__':
    main()
