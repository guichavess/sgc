"""
Script de importacao do DE-PARA: CSV -> itens_contrato.

Mapeia itens da tabela itens_contrato para CATSERV/CATMAT
usando MATCH POR DESCRICAO (o ID do CSV nao corresponde ao id do banco).

Logica:
  1. Le CSV de-para (separador ;)
  2. Para cada linha, normaliza a descricao do ITEM
  3. Busca na tabela itens_contrato pelo descricao normalizado
  4. Se SERVICO -> seta catserv_servico_id = codigo_servico do catalogo
  5. Se MATERIAL -> busca catmat_itens.id pelo codigo, seta catmat_item_id
  6. Faz UPDATE no banco

Modo DRY-RUN por padrao. Use --executar para aplicar.
Gera log detalhado em scripts/log_depara.txt
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

CSV_PATH = r"C:\Users\guilh\OneDrive\Documentos\csv de para itens.csv"
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log_depara.txt')

EXECUTAR = '--executar' in sys.argv


# -- Logger: imprime no console E salva no arquivo -------------------------
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


# -- Carregar dados --------------------------------------------------------
log("=" * 90)
log("IMPORTACAO DE-PARA: CSV -> itens_contrato")
log(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
log(f"Modo: {'EXECUCAO REAL' if EXECUTAR else 'DRY-RUN (use --executar para aplicar)'}")
log("=" * 90)

# Ler CSV
df = pd.read_csv(CSV_PATH, sep=';', encoding='utf-8-sig', dtype=str)
log(f"\nCSV carregado: {len(df)} linhas")

# Colunas (por posicao, conforme layout higienizado)
# ITEM_tabela_itens_contrato;TIPO DO OBJETO;GRUPO;ID;item das tabelas catmat e catserv
COL_ITEM = 0        # descricao do item (para match com itens_contrato.descricao)
COL_TIPO = 1        # SERVICO ou MATERIAL
COL_GRUPO = 2       # grupo do catalogo
COL_CAT_ID = 3      # codigo_servico (CATSERV) ou codigo (CATMAT)
COL_CAT_DESC = 4    # descricao do catalogo (catserv/catmat)

# -- Carregar todos os itens_contrato do banco -----------------------------
with ENGINE.connect() as conn:
    db_itens = conn.execute(text(
        "SELECT id, descricao, catserv_servico_id, catmat_item_id FROM itens_contrato ORDER BY id"
    )).fetchall()

log(f"Itens no banco: {len(db_itens)}")

# Criar dicionario: descricao_normalizada -> lista de {id, ja_tem}
db_map = {}
for row in db_itens:
    key = normalizar(row[1])
    ja_tem = (row[2] is not None) or (row[3] is not None)
    if key not in db_map:
        db_map[key] = []
    db_map[key].append({'id': row[0], 'ja_tem': ja_tem})

# -- Carregar mapas CATMAT: codigo -> id ------------------------------------
# O CSV de-para traz codigos que podem ser de catmat_itens OU catmat_pdms.
# Prioridade: tenta catmat_itens primeiro, fallback para catmat_pdms.
with ENGINE.connect() as conn:
    catmat_item_rows = conn.execute(text("SELECT id, codigo FROM catmat_itens")).fetchall()
    catmat_pdm_rows = conn.execute(text("SELECT id, codigo FROM catmat_pdms")).fetchall()

catmat_item_codigos = {str(r[1]): r[0] for r in catmat_item_rows}
catmat_pdm_codigos = {str(r[1]): r[0] for r in catmat_pdm_rows}
log(f"CATMAT Itens carregados: {len(catmat_item_codigos)}")
log(f"CATMAT PDMs carregados: {len(catmat_pdm_codigos)}")

# -- Carregar catserv existentes para validacao ----------------------------
with ENGINE.connect() as conn:
    catserv_rows = conn.execute(text("SELECT codigo_servico FROM catserv_servicos")).fetchall()

catserv_validos = {str(r[0]) for r in catserv_rows}
log(f"CATSERV servicos validos: {len(catserv_validos)}")

# -- Processar CSV ---------------------------------------------------------
log("\n" + "=" * 90)
log("PROCESSAMENTO")
log("=" * 90)

db_ids_usados = set()

# Listas detalhadas para o log
lista_sucesso = []        # (csv_linha, db_id, tipo, desc_item, cat_id, cat_desc)
lista_sem_match = []      # (csv_linha, desc_item)
lista_ids_esgotados = []  # (csv_linha, desc_item)
lista_catalogo_invalido = []  # (csv_linha, desc_item, motivo)
lista_ja_preenchido = []  # (csv_linha, db_id, desc_item)

stats = {
    'total_csv': 0,
    'skip_vazio': 0,
    'update_executados': 0,
}
updates = []  # (db_id, campo, valor, desc_item, tipo, cat_desc)

for idx, row in df.iterrows():
    csv_linha = idx + 2  # +2 porque header=linha1, pandas 0-indexed
    item_desc_raw = str(row.iloc[COL_ITEM]).strip() if pd.notna(row.iloc[COL_ITEM]) else ''
    tipo_raw = str(row.iloc[COL_TIPO]).strip() if pd.notna(row.iloc[COL_TIPO]) else ''
    cat_id_raw = str(row.iloc[COL_CAT_ID]).strip() if pd.notna(row.iloc[COL_CAT_ID]) else ''
    cat_desc_raw = str(row.iloc[COL_CAT_DESC]).strip() if pd.notna(row.iloc[COL_CAT_DESC]) else ''

    # Pular linhas vazias
    if not item_desc_raw or item_desc_raw == 'nan':
        stats['skip_vazio'] += 1
        continue

    stats['total_csv'] += 1
    item_norm = normalizar(item_desc_raw)
    tipo = tipo_raw.upper()

    # Normalizar tipo (remove acentos: SERVICO -> SERVICO, SERVIÇO -> SERVICO)
    tipo_norm = normalizar(tipo)

    # -- Match no banco ----------------------------------------------------
    if item_norm not in db_map:
        lista_sem_match.append((csv_linha, item_desc_raw))
        continue

    # Pegar o primeiro ID ainda nao usado
    candidatos = db_map[item_norm]
    db_match = None
    for c in candidatos:
        if c['id'] not in db_ids_usados:
            db_match = c
            db_ids_usados.add(c['id'])
            break

    if not db_match:
        lista_ids_esgotados.append((csv_linha, item_desc_raw))
        continue

    db_id = db_match['id']

    if db_match['ja_tem']:
        lista_ja_preenchido.append((csv_linha, db_id, item_desc_raw))
        continue

    # -- Verificar catalogo ------------------------------------------------
    if not cat_id_raw or not cat_id_raw.isdigit():
        lista_catalogo_invalido.append((csv_linha, item_desc_raw, f"ID catalogo invalido: '{cat_id_raw}'"))
        continue

    if tipo_norm == 'SERVICO':
        # CSV traz codigo_servico do CATSERV
        if cat_id_raw in catserv_validos:
            updates.append((db_id, 'catserv_servico_id', int(cat_id_raw), item_desc_raw, 'S', cat_desc_raw))
            lista_sucesso.append((csv_linha, db_id, 'SERVICO', item_desc_raw, cat_id_raw, cat_desc_raw))
        else:
            lista_catalogo_invalido.append((csv_linha, item_desc_raw, f"CATSERV codigo_servico={cat_id_raw} nao existe"))

    elif tipo_norm == 'MATERIAL':
        # CSV pode trazer codigo de catmat_itens OU catmat_pdms.
        # Tenta catmat_itens primeiro; se nao achar, tenta catmat_pdms.
        if cat_id_raw in catmat_item_codigos:
            # Nivel ITEM: grava catmat_itens.id
            item_id = catmat_item_codigos[cat_id_raw]
            updates.append((db_id, 'catmat_item_id', item_id, item_desc_raw, 'M', cat_desc_raw))
            lista_sucesso.append((csv_linha, db_id, 'MATERIAL(ITEM)', item_desc_raw, cat_id_raw, cat_desc_raw))
        elif cat_id_raw in catmat_pdm_codigos:
            # Nivel PDM: grava catmat_pdms.id (fallback)
            pdm_id = catmat_pdm_codigos[cat_id_raw]
            updates.append((db_id, 'catmat_item_id', pdm_id, item_desc_raw, 'M-PDM', cat_desc_raw))
            lista_sucesso.append((csv_linha, db_id, 'MATERIAL(PDM)', item_desc_raw, cat_id_raw, cat_desc_raw))
        else:
            lista_catalogo_invalido.append((csv_linha, item_desc_raw, f"CATMAT codigo={cat_id_raw} nao existe (nem item nem PDM)"))
    else:
        lista_catalogo_invalido.append((csv_linha, item_desc_raw, f"Tipo desconhecido: '{tipo_raw}'"))


# ==========================================================================
# RELATORIO DETALHADO
# ==========================================================================
log("\n" + "=" * 90)
log("RELATORIO GERAL")
log("=" * 90)

total_ok = len(lista_sucesso)
total_falha = len(lista_sem_match) + len(lista_ids_esgotados) + len(lista_catalogo_invalido)

log(f"  Linhas CSV processadas:        {stats['total_csv']}")
log(f"  Linhas vazias ignoradas:       {stats['skip_vazio']}")
log(f"  Ja tinha de-para (pulados):    {len(lista_ja_preenchido)}")
log("")
log(f"  [OK] SUCESSO (prontos p/ UPDATE): {total_ok}")
log(f"     -> Servicos:                 {sum(1 for s in lista_sucesso if s[2] == 'SERVICO')}")
log(f"     -> Materiais:                {sum(1 for s in lista_sucesso if s[2].startswith('MATERIAL'))}")
log(f"  [FALHA] (sem UPDATE):           {total_falha}")
log(f"     -> Sem match no banco:       {len(lista_sem_match)}")
log(f"     -> IDs duplicados esgotados: {len(lista_ids_esgotados)}")
log(f"     -> Catalogo invalido:        {len(lista_catalogo_invalido)}")

# -- Lista de SUCESSOS -----------------------------------------------------
log("\n" + "=" * 90)
log(f"ITENS COM SUCESSO ({total_ok} itens)")
log("=" * 90)
if lista_sucesso:
    for csv_ln, db_id, tipo, desc, cat_id, cat_desc in lista_sucesso:
        tipo_label = 'SERV' if tipo == 'SERVICO' else 'MAT '
        log(f"  [OK] CSV ln {csv_ln:>3} -> db.id={db_id:>4} [{tipo_label}] {desc[:55]}")
        log(f"                          Catalogo: {cat_id} - {cat_desc[:50]}")
else:
    log("  (nenhum)")

# -- Lista SEM MATCH -------------------------------------------------------
log("\n" + "=" * 90)
log(f"ITENS SEM MATCH NO BANCO ({len(lista_sem_match)} itens)")
log("  Motivo: descricao do CSV nao encontrou correspondencia na tabela itens_contrato")
log("=" * 90)
if lista_sem_match:
    for csv_ln, desc in lista_sem_match:
        log(f"  [X] CSV ln {csv_ln:>3} | {desc[:75]}")
else:
    log("  (nenhum - todos encontraram match)")

# -- Lista IDs ESGOTADOS ---------------------------------------------------
if lista_ids_esgotados:
    log("\n" + "=" * 90)
    log(f"ITENS COM IDs ESGOTADOS ({len(lista_ids_esgotados)} itens)")
    log("  Motivo: descricao encontrada no banco, mas todos os IDs ja foram usados por outra linha do CSV")
    log("=" * 90)
    for csv_ln, desc in lista_ids_esgotados:
        log(f"  [!] CSV ln {csv_ln:>3} | {desc[:75]}")

# -- Lista CATALOGO INVALIDO -----------------------------------------------
if lista_catalogo_invalido:
    log("\n" + "=" * 90)
    log(f"ITENS COM CATALOGO INVALIDO ({len(lista_catalogo_invalido)} itens)")
    log("  Motivo: encontrou match no banco, mas o codigo CATSERV/CATMAT nao existe")
    log("=" * 90)
    for csv_ln, desc, motivo in lista_catalogo_invalido:
        log(f"  [X] CSV ln {csv_ln:>3} | {desc[:55]}")
        log(f"                     Motivo: {motivo}")

# -- Lista JA PREENCHIDOS --------------------------------------------------
if lista_ja_preenchido:
    log("\n" + "=" * 90)
    log(f"ITENS JA PREENCHIDOS - PULADOS ({len(lista_ja_preenchido)} itens)")
    log("  Motivo: itens_contrato ja tinha catserv_servico_id ou catmat_item_id preenchido")
    log("=" * 90)
    for csv_ln, db_id, desc in lista_ja_preenchido:
        log(f"  [>>] CSV ln {csv_ln:>3} -> db.id={db_id:>4} | {desc[:60]}")

# -- PREVIEW dos UPDATEs ---------------------------------------------------
log("\n" + "-" * 90)
log(f"PREVIEW UPDATEs (mostrando {min(15, len(updates))} de {len(updates)}):")
log("-" * 90)
for db_id, campo, valor, desc, tipo, cat_desc in updates[:15]:
    tipo_label = 'SERV' if tipo == 'S' else 'MAT '
    log(f"  [{tipo_label}] UPDATE itens_contrato SET {campo} = {valor} WHERE id = {db_id}")
    log(f"         Item: {desc[:65]}")
    log(f"         Catalogo: {cat_desc[:65]}")
    log()

# -- Aplicar UPDATEs -------------------------------------------------------
if EXECUTAR and updates:
    log("\n" + "=" * 90)
    log("APLICANDO UPDATEs...")
    log("=" * 90)

    with ENGINE.begin() as conn:
        for db_id, campo, valor, desc, tipo, cat_desc in updates:
            conn.execute(text(
                f"UPDATE itens_contrato SET {campo} = :valor WHERE id = :id"
            ), {"valor": valor, "id": db_id})
            stats['update_executados'] += 1

    log(f"\n  [OK] {stats['update_executados']} registros atualizados com sucesso!")

elif EXECUTAR and not updates:
    log("\n  Nenhum UPDATE a aplicar.")

else:
    log(f"\n  Modo DRY-RUN. Para aplicar, rode:")
    log(f"     python scripts/importar_depara_itens.py --executar")

log("\n" + "=" * 90)
log("Concluido.")
log("=" * 90)

# Salvar log em arquivo
salvar_log()
