"""
Script de validação: mostra 4 exemplos do CSV de-para
e verifica se os itens existem no banco (itens_contrato, catserv, catmat).

Match é feito por DESCRIÇÃO normalizada (não por ID do CSV).
"""
import os
import sys
import re
import unicodedata
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carregar .env
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, '.env'))

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
ENGINE = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")


def normalizar(texto):
    """Remove acentos, converte para upper, colapsa espaços."""
    if not texto or not isinstance(texto, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', texto)
    sem_acento = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r'\s+', ' ', sem_acento.upper().strip())


# Ler CSV
CSV_PATH = r"C:\Users\guilh\OneDrive\Documentos\csv de para itens.csv"
df = pd.read_csv(CSV_PATH, sep=';', encoding='utf-8-sig', dtype=str)

# Colunas por posição (CSV higienizado)
# ITEM_tabela_itens_contrato;TIPO DO OBJETO;GRUPO;ID;item das tabelas catmat e catserv
COL_ITEM = 0
COL_TIPO = 1
COL_GRUPO = 2
COL_CAT_ID = 3
COL_CAT_DESC = 4

# Filtrar linhas com dados válidos
df_valido = df[df.iloc[:, COL_ITEM].notna() & (df.iloc[:, COL_ITEM].str.strip() != '')]

# Pegar 2 SERVIÇO e 2 MATERIAL
servicos = df_valido[df_valido.iloc[:, COL_TIPO].str.strip().str.upper() == 'SERVIÇO'].head(2)
materiais = df_valido[df_valido.iloc[:, COL_TIPO].str.strip().str.upper() == 'MATERIAL'].head(2)
exemplos = pd.concat([servicos, materiais])

print("=" * 90)
print("4 EXEMPLOS DO CSV DE-PARA (match por descrição)")
print("=" * 90)

with ENGINE.connect() as conn:
    for idx, row in exemplos.iterrows():
        descricao_item = str(row.iloc[COL_ITEM]).strip() if pd.notna(row.iloc[COL_ITEM]) else '?'
        tipo_objeto = str(row.iloc[COL_TIPO]).strip() if pd.notna(row.iloc[COL_TIPO]) else '?'
        grupo_codigo = str(row.iloc[COL_GRUPO]).strip() if pd.notna(row.iloc[COL_GRUPO]) else '?'
        catalogo_id = str(row.iloc[COL_CAT_ID]).strip() if pd.notna(row.iloc[COL_CAT_ID]) else '?'
        catalogo_desc = str(row.iloc[COL_CAT_DESC]).strip() if pd.notna(row.iloc[COL_CAT_DESC]) else '?'

        desc_norm = normalizar(descricao_item)

        print(f"\n--- Linha CSV (index {idx}) ---")
        print(f"  Descrição do item  = {descricao_item}")
        print(f"  Desc normalizada   = {desc_norm[:70]}")
        print(f"  TIPO DO OBJETO     = {tipo_objeto}")
        print(f"  GRUPO catálogo     = {grupo_codigo}")
        print(f"  ID catálogo        = {catalogo_id}")
        print(f"  Descrição catálogo = {catalogo_desc}")

        # Buscar no banco por descrição normalizada
        db_rows = conn.execute(text(
            "SELECT id, descricao, catserv_servico_id, catmat_item_id FROM itens_contrato"
        )).fetchall()

        matches = []
        for r in db_rows:
            if normalizar(r[1]) == desc_norm:
                matches.append(r)

        if matches:
            for m in matches:
                ja_tem = 'SIM' if (m[2] is not None or m[3] is not None) else 'NÃO'
                print(f"  ✅ Match: itens_contrato.id={m[0]}, descricao='{m[1][:60]}' (já tem de-para: {ja_tem})")
        else:
            print(f"  ❌ NENHUM match por descrição no banco!")

        # Verificar catálogo
        if tipo_objeto.upper() in ('SERVIÇO', 'SERVICO') and catalogo_id.isdigit():
            r = conn.execute(text(
                "SELECT codigo_servico, nome FROM catserv_servicos WHERE codigo_servico = :cod"
            ), {"cod": int(catalogo_id)}).fetchone()
            if r:
                print(f"  ✅ CATSERV encontrado: codigo_servico={r[0]}, nome='{r[1][:60]}'")
                if matches:
                    print(f"     → UPDATE itens_contrato SET catserv_servico_id = {r[0]} WHERE id = {matches[0][0]}")
            else:
                print(f"  ❌ CATSERV codigo_servico={catalogo_id} NÃO encontrado no banco")

        elif tipo_objeto.upper() == 'MATERIAL' and catalogo_id.isdigit():
            r = conn.execute(text(
                "SELECT id, codigo, descricao FROM catmat_itens WHERE codigo = :cod LIMIT 1"
            ), {"cod": int(catalogo_id)}).fetchone()
            if r:
                print(f"  ✅ CATMAT encontrado: id={r[0]}, codigo={r[1]}, descricao='{r[2][:60]}'")
                if matches:
                    print(f"     → UPDATE itens_contrato SET catmat_item_id = {r[0]} WHERE id = {matches[0][0]}")
            else:
                print(f"  ❌ CATMAT codigo={catalogo_id} NÃO encontrado no banco")

        print()

print("=" * 90)
print("Validação concluída. Verifique os resultados acima.")
print("Para aplicar a carga real: python scripts/importar_depara_itens.py --executar")
print("=" * 90)
