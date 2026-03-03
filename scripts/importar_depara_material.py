"""
Script de importacao do DE-PARA MATERIAL: Excel -> itens_contrato.catmat_item_id

Mapeia itens da tabela itens_contrato para CATMAT usando ID direto.
O Excel traz: id (itens_contrato.id), descricao, tipo_item, codigo_item (catmat_itens.codigo).

Logica:
  1. Le Excel
  2. Para cada linha, verifica tipo_item:
     - "material" -> busca catmat_itens.id pelo codigo_item, seta catmat_item_id
     - "servico"  -> busca catserv_servicos pelo codigo_item, seta catserv_servico_id
  3. Pula itens que ja tem de-para preenchido
  4. Faz UPDATE no banco

Modo DRY-RUN por padrao. Use --executar para aplicar.
Gera log detalhado em scripts/log_depara_material.txt
"""
import os
import sys
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

XLSX_PATH = r"C:\Users\guilh\OneDrive\Documentos\csv de para material.xlsx"
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log_depara_material.txt')

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


# -- Carregar dados --------------------------------------------------------
log("=" * 90)
log("IMPORTACAO DE-PARA MATERIAL: Excel -> itens_contrato")
log(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
log(f"Modo: {'EXECUCAO REAL' if EXECUTAR else 'DRY-RUN (use --executar para aplicar)'}")
log("=" * 90)

# Ler Excel
df = pd.read_excel(XLSX_PATH, dtype=str)
log(f"\nExcel carregado: {len(df)} linhas")
log(f"Colunas: {list(df.columns)}")

# -- Carregar dados do banco -----------------------------------------------
with ENGINE.connect() as conn:
    # itens_contrato existentes (por id)
    db_itens = conn.execute(text(
        "SELECT id, descricao, catserv_servico_id, catmat_item_id FROM itens_contrato ORDER BY id"
    )).fetchall()

    # catmat_itens: codigo -> id
    catmat_rows = conn.execute(text("SELECT id, codigo FROM catmat_itens")).fetchall()

    # catserv_servicos: codigo_servico (para o servico misturado)
    catserv_rows = conn.execute(text("SELECT codigo_servico FROM catserv_servicos")).fetchall()

db_map = {row[0]: {'descricao': row[1], 'catserv': row[2], 'catmat': row[3]} for row in db_itens}
catmat_codigo_to_id = {str(r[1]): r[0] for r in catmat_rows}
catserv_validos = {str(r[0]) for r in catserv_rows}

log(f"Itens no banco: {len(db_map)}")
log(f"CATMAT itens carregados: {len(catmat_codigo_to_id)}")
log(f"CATSERV servicos validos: {len(catserv_validos)}")

# -- Processar Excel -------------------------------------------------------
log("\n" + "=" * 90)
log("PROCESSAMENTO")
log("=" * 90)

lista_sucesso = []         # (linha, id, tipo, desc, codigo, campo, valor_db)
lista_ja_preenchido = []   # (linha, id, desc, motivo)
lista_id_inexistente = []  # (linha, id, desc)
lista_catalogo_invalido = []  # (linha, id, desc, motivo)
updates = []               # (id, campo, valor, desc, tipo)

for idx, row in df.iterrows():
    linha = idx + 2  # +2: header=1, pandas 0-indexed
    item_id_raw = str(row['id']).strip()
    descricao = str(row['descricao']).strip() if pd.notna(row['descricao']) else ''
    tipo_raw = str(row['tipo_item']).strip().lower() if pd.notna(row['tipo_item']) else ''
    codigo_raw = str(row['codigo_item']).strip() if pd.notna(row['codigo_item']) else ''

    # Validar ID
    if not item_id_raw.isdigit():
        lista_catalogo_invalido.append((linha, item_id_raw, descricao, f"ID invalido: '{item_id_raw}'"))
        continue

    item_id = int(item_id_raw)

    # Verificar se existe no banco
    if item_id not in db_map:
        lista_id_inexistente.append((linha, item_id, descricao))
        continue

    db_info = db_map[item_id]

    # Determinar tipo: servico ou material
    is_servico = 'servi' in tipo_raw

    if is_servico:
        # SERVICO: verificar se ja tem catserv_servico_id
        if db_info['catserv'] is not None:
            lista_ja_preenchido.append((linha, item_id, descricao, f"Ja tem catserv_servico_id={db_info['catserv']}"))
            continue

        # Validar codigo
        if not codigo_raw.isdigit():
            lista_catalogo_invalido.append((linha, item_id, descricao, f"Codigo servico invalido: '{codigo_raw}'"))
            continue

        if codigo_raw not in catserv_validos:
            lista_catalogo_invalido.append((linha, item_id, descricao, f"CATSERV codigo_servico={codigo_raw} nao existe"))
            continue

        updates.append((item_id, 'catserv_servico_id', int(codigo_raw), descricao, 'SERVICO'))
        lista_sucesso.append((linha, item_id, 'SERVICO', descricao, codigo_raw, 'catserv_servico_id', int(codigo_raw)))

    else:
        # MATERIAL: verificar se ja tem catmat_item_id
        if db_info['catmat'] is not None:
            lista_ja_preenchido.append((linha, item_id, descricao, f"Ja tem catmat_item_id={db_info['catmat']}"))
            continue

        # Validar codigo
        if not codigo_raw.isdigit():
            lista_catalogo_invalido.append((linha, item_id, descricao, f"Codigo material invalido: '{codigo_raw}'"))
            continue

        if codigo_raw not in catmat_codigo_to_id:
            lista_catalogo_invalido.append((linha, item_id, descricao, f"CATMAT codigo={codigo_raw} nao existe em catmat_itens"))
            continue

        catmat_id = catmat_codigo_to_id[codigo_raw]
        updates.append((item_id, 'catmat_item_id', catmat_id, descricao, 'MATERIAL'))
        lista_sucesso.append((linha, item_id, 'MATERIAL', descricao, codigo_raw, 'catmat_item_id', catmat_id))


# ==========================================================================
# RELATORIO DETALHADO
# ==========================================================================
log("\n" + "=" * 90)
log("RELATORIO GERAL")
log("=" * 90)

total_ok = len(lista_sucesso)
total_falha = len(lista_id_inexistente) + len(lista_catalogo_invalido)

log(f"  Linhas processadas:            {len(df)}")
log(f"  Ja tinha de-para (pulados):    {len(lista_ja_preenchido)}")
log("")
log(f"  [OK] SUCESSO (prontos p/ UPDATE): {total_ok}")
log(f"     -> Servicos:                 {sum(1 for s in lista_sucesso if s[2] == 'SERVICO')}")
log(f"     -> Materiais:                {sum(1 for s in lista_sucesso if s[2] == 'MATERIAL')}")
log(f"  [FALHA] (sem UPDATE):           {total_falha}")
log(f"     -> ID inexistente no banco:  {len(lista_id_inexistente)}")
log(f"     -> Catalogo invalido:        {len(lista_catalogo_invalido)}")

# -- Lista de SUCESSOS -----------------------------------------------------
log("\n" + "=" * 90)
log(f"ITENS COM SUCESSO ({total_ok} itens)")
log("=" * 90)
if lista_sucesso:
    for ln, db_id, tipo, desc, cod, campo, val in lista_sucesso:
        tipo_label = 'SERV' if tipo == 'SERVICO' else 'MAT '
        log(f"  [OK] Excel ln {ln:>3} -> db.id={db_id:>4} [{tipo_label}] {desc[:55]}")
        log(f"                          {campo} = {val} (codigo={cod})")
else:
    log("  (nenhum)")

# -- JA PREENCHIDOS --------------------------------------------------------
if lista_ja_preenchido:
    log("\n" + "=" * 90)
    log(f"ITENS JA PREENCHIDOS - PULADOS ({len(lista_ja_preenchido)} itens)")
    log("=" * 90)
    for ln, db_id, desc, motivo in lista_ja_preenchido:
        log(f"  [>>] Excel ln {ln:>3} -> db.id={db_id:>4} | {desc[:50]}")
        log(f"                          {motivo}")

# -- ID INEXISTENTE --------------------------------------------------------
if lista_id_inexistente:
    log("\n" + "=" * 90)
    log(f"IDs INEXISTENTES NO BANCO ({len(lista_id_inexistente)} itens)")
    log("=" * 90)
    for ln, db_id, desc in lista_id_inexistente:
        log(f"  [X] Excel ln {ln:>3} -> id={db_id:>4} | {desc[:60]}")

# -- CATALOGO INVALIDO -----------------------------------------------------
if lista_catalogo_invalido:
    log("\n" + "=" * 90)
    log(f"CATALOGO INVALIDO ({len(lista_catalogo_invalido)} itens)")
    log("=" * 90)
    for ln, db_id, desc, motivo in lista_catalogo_invalido:
        log(f"  [X] Excel ln {ln:>3} -> id={db_id} | {desc[:50]}")
        log(f"                          Motivo: {motivo}")

# -- PREVIEW UPDATEs -------------------------------------------------------
log("\n" + "-" * 90)
log(f"PREVIEW UPDATEs ({len(updates)} total):")
log("-" * 90)
for db_id, campo, valor, desc, tipo in updates:
    tipo_label = 'SERV' if tipo == 'SERVICO' else 'MAT '
    log(f"  [{tipo_label}] UPDATE itens_contrato SET {campo} = {valor} WHERE id = {db_id}")
    log(f"         Item: {desc[:65]}")
    log()

# -- Aplicar UPDATEs -------------------------------------------------------
if EXECUTAR and updates:
    log("\n" + "=" * 90)
    log("APLICANDO UPDATEs...")
    log("=" * 90)

    executados = 0
    with ENGINE.begin() as conn:
        for db_id, campo, valor, desc, tipo in updates:
            conn.execute(text(
                f"UPDATE itens_contrato SET {campo} = :valor WHERE id = :id"
            ), {"valor": valor, "id": db_id})
            executados += 1

    log(f"\n  [OK] {executados} registros atualizados com sucesso!")

elif EXECUTAR and not updates:
    log("\n  Nenhum UPDATE a aplicar.")

else:
    log(f"\n  Modo DRY-RUN. Para aplicar, rode:")
    log(f"     python scripts/importar_depara_material.py --executar")

log("\n" + "=" * 90)
log("Concluido.")
log("=" * 90)

# Salvar log em arquivo
salvar_log()
