"""
Script para importar itens restantes do Excel:
  1. Faz o DE-PARA (atualiza catserv_servico_id / catmat_item_id em itens_contrato)
  2. Vincula os itens ao contrato (insere em itens_vinculados)

Planilha: 'itens restantes.xlsx'
Colunas: codigo do contrato | ITEM | ID | TIPO | item cat_mat_serv

Modo DRY-RUN por padrao. Use --executar para aplicar.

Uso:
    python scripts/importar_itens_restantes.py
    python scripts/importar_itens_restantes.py --executar
"""
import os
import sys
import re
import unicodedata
from datetime import datetime

import pandas as pd
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

EXCEL_PATH = r"C:\Users\guilh\OneDrive\Documentos\itens restantes.xlsx"
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log_itens_restantes.txt')

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


# -- Funcoes auxiliares ----------------------------------------------------
def normalizar(texto):
    """Remove acentos, converte para upper, colapsa espacos."""
    if not texto or not isinstance(texto, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', texto)
    sem_acento = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r'\s+', ' ', sem_acento.upper().strip())


# ==========================================================================
# INICIO
# ==========================================================================
log("=" * 90)
log("IMPORTACAO ITENS RESTANTES: Excel -> de-para + vinculacao")
log(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
log(f"Modo: {'EXECUCAO REAL' if EXECUTAR else 'DRY-RUN (use --executar para aplicar)'}")
log("=" * 90)

# -- Ler Excel -------------------------------------------------------------
df = pd.read_excel(EXCEL_PATH, dtype=str)
log(f"\nExcel carregado: {len(df)} linhas")
log(f"Colunas: {list(df.columns)}")

# Colunas do Excel
COL_CONTRATO = 'código do contrato'
COL_ITEM = 'ITEM'
COL_ID = 'ID'
COL_TIPO = 'TIPO'
COL_CAT_DESC = 'item cat_mat_serv'

# -- Carregar itens_contrato do banco -------------------------------------
with ENGINE.connect() as conn:
    db_itens = conn.execute(text(
        "SELECT id, descricao, catserv_servico_id, catmat_item_id FROM itens_contrato ORDER BY id"
    )).fetchall()

log(f"Itens no banco (itens_contrato): {len(db_itens)}")

# Criar dicionario: descricao_normalizada -> lista de {id, catserv_servico_id, catmat_item_id}
db_map = {}
for row in db_itens:
    key = normalizar(row[1])
    if key not in db_map:
        db_map[key] = []
    db_map[key].append({
        'id': row[0],
        'descricao': row[1],
        'catserv_servico_id': row[2],
        'catmat_item_id': row[3],
    })

# -- Carregar contratos validos --------------------------------------------
with ENGINE.connect() as conn:
    contratos_rows = conn.execute(text("SELECT codigo FROM contratos")).fetchall()
contratos_validos = {str(r[0]).strip() for r in contratos_rows}
log(f"Contratos validos no banco: {len(contratos_validos)}")

# -- Carregar vinculacoes existentes ---------------------------------------
with ENGINE.connect() as conn:
    vinc_rows = conn.execute(text(
        "SELECT codigo_contrato, item_contrato_id FROM itens_vinculados"
    )).fetchall()
vinculacoes_existentes = {(str(r[0]).strip(), r[1]) for r in vinc_rows}
log(f"Vinculacoes existentes: {len(vinculacoes_existentes)}")

# -- Carregar CATSERV validos ----------------------------------------------
with ENGINE.connect() as conn:
    catserv_rows = conn.execute(text("SELECT codigo_servico FROM catserv_servicos")).fetchall()
catserv_validos = {str(r[0]) for r in catserv_rows}
log(f"CATSERV servicos validos: {len(catserv_validos)}")

# -- Carregar CATMAT: codigo -> id -----------------------------------------
with ENGINE.connect() as conn:
    catmat_item_rows = conn.execute(text("SELECT id, codigo FROM catmat_itens")).fetchall()
    catmat_pdm_rows = conn.execute(text("SELECT id, codigo FROM catmat_pdms")).fetchall()

catmat_item_codigos = {str(r[1]): r[0] for r in catmat_item_rows}
catmat_pdm_codigos = {str(r[1]): r[0] for r in catmat_pdm_rows}
log(f"CATMAT Itens: {len(catmat_item_codigos)} | PDMs: {len(catmat_pdm_codigos)}")

# ==========================================================================
# PROCESSAR
# ==========================================================================
log("\n" + "=" * 90)
log("PROCESSAMENTO")
log("=" * 90)

db_ids_usados = set()

# Resultados
lista_sucesso = []         # (linha, contrato, db_id, tipo, desc_item, cat_id, cat_desc, acoes)
lista_contrato_invalido = []
lista_sem_match = []
lista_ids_esgotados = []
lista_catalogo_invalido = []

# Acoes a executar
updates_depara = []        # (db_id, campo, valor)
inserts_vinculacao = []    # (codigo_contrato, tipo, catserv_id, catmat_id, item_contrato_id)

for idx, row in df.iterrows():
    linha = idx + 2
    contrato_raw = str(row[COL_CONTRATO]).strip() if pd.notna(row[COL_CONTRATO]) else ''
    item_desc_raw = str(row[COL_ITEM]).strip() if pd.notna(row[COL_ITEM]) else ''
    cat_id_raw = str(row[COL_ID]).strip() if pd.notna(row[COL_ID]) else ''
    tipo_raw = str(row[COL_TIPO]).strip() if pd.notna(row[COL_TIPO]) else ''
    cat_desc_raw = str(row[COL_CAT_DESC]).strip() if pd.notna(row[COL_CAT_DESC]) else ''

    if not item_desc_raw or item_desc_raw == 'nan':
        continue

    # Limpar contrato (remover .0 se veio como float)
    if contrato_raw.endswith('.0'):
        contrato_raw = contrato_raw[:-2]

    tipo_norm = normalizar(tipo_raw)  # SERVICO ou MATERIAL
    item_norm = normalizar(item_desc_raw)

    # Validar contrato
    if contrato_raw not in contratos_validos:
        lista_contrato_invalido.append((linha, contrato_raw, item_desc_raw))
        continue

    # Match no banco (itens_contrato)
    if item_norm not in db_map:
        lista_sem_match.append((linha, contrato_raw, item_desc_raw))
        continue

    # Pegar primeiro ID ainda nao usado
    candidatos = db_map[item_norm]
    db_match = None
    for c in candidatos:
        if c['id'] not in db_ids_usados:
            db_match = c
            db_ids_usados.add(c['id'])
            break

    if not db_match:
        lista_ids_esgotados.append((linha, contrato_raw, item_desc_raw))
        continue

    db_id = db_match['id']

    # Validar catalogo
    if not cat_id_raw or not cat_id_raw.replace('.0', '').isdigit():
        lista_catalogo_invalido.append((linha, item_desc_raw, f"ID catalogo invalido: '{cat_id_raw}'"))
        continue

    # Limpar ID (remover .0 se veio como float)
    if cat_id_raw.endswith('.0'):
        cat_id_raw = cat_id_raw[:-2]

    acoes = []

    # ---- 1. DE-PARA (UPDATE itens_contrato) ----
    if tipo_norm == 'SERVICO':
        if cat_id_raw not in catserv_validos:
            lista_catalogo_invalido.append((linha, item_desc_raw, f"CATSERV codigo_servico={cat_id_raw} nao existe"))
            continue
        catserv_id = int(cat_id_raw)
        catmat_id = None
        tipo_letra = 'S'

        if db_match['catserv_servico_id'] is None:
            updates_depara.append((db_id, 'catserv_servico_id', catserv_id))
            acoes.append('DE-PARA')
        else:
            acoes.append('DE-PARA(ja preenchido)')

    elif tipo_norm == 'MATERIAL':
        catserv_id = None
        tipo_letra = 'M'

        if cat_id_raw in catmat_item_codigos:
            catmat_id = catmat_item_codigos[cat_id_raw]
        elif cat_id_raw in catmat_pdm_codigos:
            catmat_id = catmat_pdm_codigos[cat_id_raw]
        else:
            lista_catalogo_invalido.append((linha, item_desc_raw, f"CATMAT codigo={cat_id_raw} nao existe"))
            continue

        if db_match['catmat_item_id'] is None:
            updates_depara.append((db_id, 'catmat_item_id', catmat_id))
            acoes.append('DE-PARA')
        else:
            acoes.append('DE-PARA(ja preenchido)')

    else:
        lista_catalogo_invalido.append((linha, item_desc_raw, f"Tipo desconhecido: '{tipo_raw}'"))
        continue

    # ---- 2. VINCULACAO (INSERT itens_vinculados) ----
    chave_vinc = (contrato_raw, db_id)
    if chave_vinc not in vinculacoes_existentes:
        inserts_vinculacao.append((contrato_raw, tipo_letra, catserv_id, catmat_id, db_id))
        acoes.append('VINCULAR')
        vinculacoes_existentes.add(chave_vinc)
    else:
        acoes.append('VINCULAR(ja existia)')

    lista_sucesso.append((linha, contrato_raw, db_id, tipo_norm, item_desc_raw, cat_id_raw, cat_desc_raw, acoes))


# ==========================================================================
# RELATORIO
# ==========================================================================
log("\n" + "=" * 90)
log("RELATORIO GERAL")
log("=" * 90)

log(f"  Linhas Excel:                  {len(df)}")
log(f"  [OK] SUCESSO:                  {len(lista_sucesso)}")
log(f"     -> De-para novos:           {len(updates_depara)}")
log(f"     -> Vinculacoes novas:       {len(inserts_vinculacao)}")
log(f"  [FALHA]")
log(f"     -> Contrato invalido:       {len(lista_contrato_invalido)}")
log(f"     -> Sem match no banco:      {len(lista_sem_match)}")
log(f"     -> IDs esgotados:           {len(lista_ids_esgotados)}")
log(f"     -> Catalogo invalido:       {len(lista_catalogo_invalido)}")

# -- Detalhes SUCESSO ------------------------------------------------------
log("\n" + "=" * 90)
log(f"ITENS COM SUCESSO ({len(lista_sucesso)})")
log("=" * 90)
for linha, contrato, db_id, tipo, desc, cat_id, cat_desc, acoes in lista_sucesso:
    acoes_str = ' + '.join(acoes)
    log(f"  [OK] Ln {linha:>2} | Contrato {contrato} | db.id={db_id:>4} [{tipo[:4]:>4}]")
    log(f"         Item: {desc[:65]}")
    log(f"         Catalogo: {cat_id} - {cat_desc[:50]}")
    log(f"         Acoes: {acoes_str}")

# -- Detalhes CONTRATO INVALIDO -------------------------------------------
if lista_contrato_invalido:
    log("\n" + "=" * 90)
    log(f"CONTRATO INVALIDO ({len(lista_contrato_invalido)})")
    log("=" * 90)
    for linha, contrato, desc in lista_contrato_invalido:
        log(f"  [X] Ln {linha:>2} | Contrato {contrato} | {desc[:60]}")

# -- Detalhes SEM MATCH ---------------------------------------------------
if lista_sem_match:
    log("\n" + "=" * 90)
    log(f"SEM MATCH NO BANCO ({len(lista_sem_match)})")
    log("=" * 90)
    for linha, contrato, desc in lista_sem_match:
        log(f"  [X] Ln {linha:>2} | Contrato {contrato} | {desc[:60]}")

# -- Detalhes IDS ESGOTADOS -----------------------------------------------
if lista_ids_esgotados:
    log("\n" + "=" * 90)
    log(f"IDS ESGOTADOS ({len(lista_ids_esgotados)})")
    log("=" * 90)
    for linha, contrato, desc in lista_ids_esgotados:
        log(f"  [!] Ln {linha:>2} | Contrato {contrato} | {desc[:60]}")

# -- Detalhes CATALOGO INVALIDO -------------------------------------------
if lista_catalogo_invalido:
    log("\n" + "=" * 90)
    log(f"CATALOGO INVALIDO ({len(lista_catalogo_invalido)})")
    log("=" * 90)
    for linha, desc, motivo in lista_catalogo_invalido:
        log(f"  [X] Ln {linha:>2} | {desc[:55]}")
        log(f"         Motivo: {motivo}")

# -- PREVIEW ---------------------------------------------------------------
log("\n" + "-" * 90)
log("PREVIEW DE-PARA (UPDATEs itens_contrato):")
log("-" * 90)
for db_id, campo, valor in updates_depara[:20]:
    log(f"  UPDATE itens_contrato SET {campo} = {valor} WHERE id = {db_id}")

log("\n" + "-" * 90)
log("PREVIEW VINCULACOES (INSERTs itens_vinculados):")
log("-" * 90)
for contrato, tipo, catserv_id, catmat_id, item_contrato_id in inserts_vinculacao[:20]:
    log(f"  INSERT itens_vinculados (contrato={contrato}, tipo={tipo}, catserv={catserv_id}, catmat={catmat_id}, item_contrato_id={item_contrato_id})")


# ==========================================================================
# EXECUTAR
# ==========================================================================
if EXECUTAR and (updates_depara or inserts_vinculacao):
    log("\n" + "=" * 90)
    log("APLICANDO ALTERACOES...")
    log("=" * 90)

    with ENGINE.begin() as conn:
        # 1. De-para
        depara_ok = 0
        for db_id, campo, valor in updates_depara:
            conn.execute(text(
                f"UPDATE itens_contrato SET {campo} = :valor WHERE id = :id"
            ), {"valor": valor, "id": db_id})
            depara_ok += 1

        # 2. Vinculacoes
        vinc_ok = 0
        for contrato, tipo, catserv_id, catmat_id, item_contrato_id in inserts_vinculacao:
            conn.execute(text("""
                INSERT INTO itens_vinculados
                    (codigo_contrato, tipo, catserv_servico_id, catmat_item_id, item_contrato_id, data_vinculacao)
                VALUES
                    (:contrato, :tipo, :catserv, :catmat, :item_contrato_id, NOW())
            """), {
                "contrato": contrato,
                "tipo": tipo,
                "catserv": catserv_id,
                "catmat": catmat_id,
                "item_contrato_id": item_contrato_id,
            })
            vinc_ok += 1

    log(f"\n  [OK] De-para atualizados: {depara_ok}")
    log(f"  [OK] Vinculacoes criadas: {vinc_ok}")

elif EXECUTAR:
    log("\n  Nenhuma alteracao a aplicar.")

else:
    log(f"\n  Modo DRY-RUN. Para aplicar, rode:")
    log(f"     python scripts/importar_itens_restantes.py --executar")

log("\n" + "=" * 90)
log("Concluido.")
log("=" * 90)

salvar_log()
