"""
Tipificacao reversa: preenche catserv_classe_id / catmat_classe_id / catmat_pdm_id
nos contratos nao tipificados, subindo a hierarquia a partir dos itens vinculados.

Logica:
  1. Busca contratos sem tipificacao que tenham itens vinculados com de-para
  2. Para cada contrato, resolve hierarquia:
     - CATSERV: servico -> classe (ou grupo -> primeira classe se servico sem classe)
     - CATMAT: item -> pdm -> classe
  3. Se classe unica, tipifica automaticamente
  4. Se multiplas classes, pula (manual)

Modo DRY-RUN por padrao. Use --executar para aplicar.

Uso:
    python scripts/tipificar_reverso.py
    python scripts/tipificar_reverso.py --executar
"""
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -- Config ---------------------------------------------------------------
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, '.env'))

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
ENGINE = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log_tipificar_reverso.txt')
EXECUTAR = '--executar' in sys.argv

# -- Logger ----------------------------------------------------------------
log_lines = []


def log(msg=''):
    print(msg)
    log_lines.append(msg)


def salvar_log():
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(log_lines))
    print(f"\nLog salvo em: {LOG_PATH}")


# ==========================================================================
log("=" * 90)
log("TIPIFICACAO REVERSA: itens vinculados -> hierarquia -> tipificacao do contrato")
log(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
log(f"Modo: {'EXECUCAO REAL' if EXECUTAR else 'DRY-RUN (use --executar para aplicar)'}")
log("=" * 90)

with ENGINE.connect() as conn:
    # 1. Buscar contratos nao tipificados com itens vinculados que tenham de-para
    candidatos = conn.execute(text('''
        SELECT iv.codigo_contrato, iv.tipo,
               COALESCE(iv.catserv_servico_id, ic.catserv_servico_id) as catserv_id,
               COALESCE(iv.catmat_item_id, ic.catmat_item_id) as catmat_id
        FROM itens_vinculados iv
        JOIN contratos c ON c.codigo = iv.codigo_contrato
        LEFT JOIN itens_contrato ic ON ic.id = iv.item_contrato_id
        WHERE c.catserv_classe_id IS NULL AND c.catmat_classe_id IS NULL
          AND (
              iv.catserv_servico_id IS NOT NULL
              OR iv.catmat_item_id IS NOT NULL
              OR ic.catserv_servico_id IS NOT NULL
              OR ic.catmat_item_id IS NOT NULL
          )
        ORDER BY iv.codigo_contrato
    ''')).fetchall()

    # Agrupar por contrato
    contratos_map = {}
    for r in candidatos:
        cod = r[0]
        if cod not in contratos_map:
            contratos_map[cod] = {'catserv_ids': set(), 'catmat_ids': set()}
        if r[2]:
            contratos_map[cod]['catserv_ids'].add(r[2])
        if r[3]:
            contratos_map[cod]['catmat_ids'].add(r[3])

    log(f"\nContratos nao tipificados com de-para: {len(contratos_map)}")

    # 2. Pre-carregar mapas para resolver hierarquias em batch
    # CATSERV: servico -> (codigo_classe, codigo_grupo)
    servicos_all = conn.execute(text(
        'SELECT codigo_servico, codigo_classe, codigo_grupo FROM catserv_servicos'
    )).fetchall()
    mapa_servico = {r[0]: {'classe': r[1], 'grupo': r[2]} for r in servicos_all}

    # CATSERV: grupo -> primeira classe (para servicos sem classe)
    grupo_classes = conn.execute(text(
        'SELECT codigo_grupo, MIN(codigo_classe) as primeira_classe FROM catserv_classes GROUP BY codigo_grupo'
    )).fetchall()
    mapa_grupo_classe = {r[0]: r[1] for r in grupo_classes}

    # CATMAT: item.id -> (pdm.id, pdm.codigo_classe, classe.id)
    catmat_join = conn.execute(text('''
        SELECT i.id, p.id as pdm_id, p.codigo as pdm_codigo, p.codigo_classe, cl.id as classe_id
        FROM catmat_itens i
        JOIN catmat_pdms p ON p.codigo = i.codigo_pdm
        JOIN catmat_classes cl ON cl.codigo = p.codigo_classe
    ''')).fetchall()
    mapa_catmat = {r[0]: {'pdm_id': r[1], 'pdm_codigo': r[2], 'classe_id': r[4]} for r in catmat_join}

    # 3. Resolver hierarquia para cada contrato
    tipificaveis = []       # (codigo, catserv_classe_id, catmat_classe_id, catmat_pdm_id, detalhes)
    multiplas_classes = []   # (codigo, classes, detalhes)
    sem_classe = []          # (codigo, detalhes)

    for cod in sorted(contratos_map.keys()):
        dados = contratos_map[cod]
        catserv_ids = list(dados['catserv_ids'])
        catmat_ids = list(dados['catmat_ids'])

        result_catserv_classe = None
        result_catmat_classe = None
        result_catmat_pdm = None

        # --- CATSERV ---
        if catserv_ids:
            classes = set()
            grupos = set()
            for sid in catserv_ids:
                srv = mapa_servico.get(sid)
                if srv:
                    if srv['classe']:
                        classes.add(srv['classe'])
                    if srv['grupo']:
                        grupos.add(srv['grupo'])

            if len(classes) == 1:
                result_catserv_classe = list(classes)[0]
            elif len(classes) == 0 and len(grupos) >= 1:
                # Servicos sem classe - tentar pegar do grupo
                # Se todos os grupos apontam para a mesma classe, usar essa
                classes_de_grupo = set()
                for g in grupos:
                    cl = mapa_grupo_classe.get(g)
                    if cl:
                        classes_de_grupo.add(cl)
                if len(classes_de_grupo) == 1:
                    result_catserv_classe = list(classes_de_grupo)[0]
                elif len(classes_de_grupo) > 1:
                    multiplas_classes.append((cod, list(classes_de_grupo), f'via grupo(s) {list(grupos)}'))
                    continue
                else:
                    sem_classe.append((cod, f'grupos {list(grupos)} sem classes cadastradas'))
                    continue
            elif len(classes) > 1:
                multiplas_classes.append((cod, list(classes), f'servicos {catserv_ids}'))
                continue

        # --- CATMAT ---
        if catmat_ids:
            pdm_ids = set()
            classe_ids = set()
            for mid in catmat_ids:
                mat = mapa_catmat.get(mid)
                if mat:
                    pdm_ids.add(mat['pdm_id'])
                    classe_ids.add(mat['classe_id'])

            if len(pdm_ids) == 1 and len(classe_ids) == 1:
                result_catmat_pdm = list(pdm_ids)[0]
                result_catmat_classe = list(classe_ids)[0]
            elif len(classe_ids) > 1:
                multiplas_classes.append((cod, list(classe_ids), f'catmat items {catmat_ids}'))
                continue
            elif len(pdm_ids) > 1:
                multiplas_classes.append((cod, list(pdm_ids), f'multiplos PDMs'))
                continue

        if result_catserv_classe or result_catmat_classe:
            tipificaveis.append((
                cod, result_catserv_classe, result_catmat_classe, result_catmat_pdm
            ))

    # ==========================================================================
    # RELATORIO
    # ==========================================================================
    log("\n" + "=" * 90)
    log("RELATORIO")
    log("=" * 90)

    log(f"  Contratos analisados:          {len(contratos_map)}")
    log(f"  [OK] Tipificaveis:             {len(tipificaveis)}")
    log(f"  [!]  Multiplas classes (manual):{len(multiplas_classes)}")
    log(f"  [X]  Sem classe no grupo:      {len(sem_classe)}")

    # Tipificaveis
    log("\n" + "=" * 90)
    log(f"TIPIFICAVEIS AUTOMATICAMENTE ({len(tipificaveis)})")
    log("=" * 90)
    for cod, catserv_cl, catmat_cl, catmat_pdm in tipificaveis:
        parts = [f'Contrato {cod}']
        if catserv_cl:
            parts.append(f'catserv_classe_id={catserv_cl}')
        if catmat_cl:
            parts.append(f'catmat_classe_id={catmat_cl}')
        if catmat_pdm:
            parts.append(f'catmat_pdm_id={catmat_pdm}')
        log(f"  {' | '.join(parts)}")

    # Multiplas classes
    if multiplas_classes:
        log("\n" + "=" * 90)
        log(f"MULTIPLAS CLASSES - DECISAO MANUAL ({len(multiplas_classes)})")
        log("=" * 90)
        for cod, classes, detalhe in multiplas_classes:
            log(f"  Contrato {cod}: classes={classes} ({detalhe})")

    # Sem classe
    if sem_classe:
        log("\n" + "=" * 90)
        log(f"SEM CLASSE NO GRUPO ({len(sem_classe)})")
        log("=" * 90)
        for cod, detalhe in sem_classe:
            log(f"  Contrato {cod}: {detalhe}")

    # ==========================================================================
    # EXECUTAR
    # ==========================================================================
    if EXECUTAR and tipificaveis:
        log("\n" + "=" * 90)
        log("APLICANDO TIPIFICACOES...")
        log("=" * 90)

        with ENGINE.begin() as conn_w:
            ok = 0
            for cod, catserv_cl, catmat_cl, catmat_pdm in tipificaveis:
                sets = []
                params = {'cod': cod, 'agora': datetime.now()}

                if catserv_cl:
                    sets.append('catserv_classe_id = :catserv')
                    params['catserv'] = catserv_cl
                if catmat_cl:
                    sets.append('catmat_classe_id = :catmat_cl')
                    params['catmat_cl'] = catmat_cl
                if catmat_pdm:
                    sets.append('catmat_pdm_id = :catmat_pdm')
                    params['catmat_pdm'] = catmat_pdm

                sets.append('data_tipificacao = :agora')

                sql = f"UPDATE contratos SET {', '.join(sets)} WHERE codigo = :cod"
                conn_w.execute(text(sql), params)
                ok += 1

        log(f"\n  [OK] {ok} contratos tipificados com sucesso!")

    elif EXECUTAR:
        log("\n  Nenhuma tipificacao a aplicar.")
    else:
        log(f"\n  Modo DRY-RUN. Para aplicar, rode:")
        log(f"     python scripts/tipificar_reverso.py --executar")

log("\n" + "=" * 90)
log("Concluido.")
log("=" * 90)

salvar_log()
