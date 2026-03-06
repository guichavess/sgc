import time
import warnings
import requests
import pandas as pd
import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text, bindparam, create_engine

# Suprime warnings de SSL (verify=False)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# 1. CARREGAR VARIÁVEIS DE AMBIENTE
# =========================
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')

from dotenv import load_dotenv
load_dotenv(dotenv_path)

if not os.getenv('DB_USER'):
    print(f"AVISO: Arquivo .env não encontrado ou variáveis vazias. Buscado em: {dotenv_path}")

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO CRÍTICO: Variáveis de banco de dados ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
ENGINE = create_engine(DATABASE_URI, echo=False)

# =========================
# 2. AUTENTICAÇÃO SIAFE
# =========================
def get_token():
    siafe_user = os.getenv('SIAFE_USUARIO')
    siafe_pass = os.getenv('SIAFE_SENHA')

    if not siafe_user or not siafe_pass:
        print("ERRO: Variáveis SIAFE_USUARIO e SIAFE_SENHA não encontradas no .env")
        sys.exit(1)

    url = "https://tesouro.sefaz.pi.gov.br/siafe-api/auth"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"usuario": siafe_user, "senha": siafe_pass})

    max_tentativas = 3
    for tentativa in range(1, max_tentativas + 1):
        try:
            print(f"  Tentativa {tentativa}/{max_tentativas}...")
            response = requests.post(url, headers=headers, data=payload, timeout=60, verify=False)
            if response.status_code == 200:
                return response.json()['token']
            else:
                raise Exception(f"Erro Auth SIAFE: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"  Falha na tentativa {tentativa}: {e}")
            if tentativa < max_tentativas:
                print(f"  Aguardando 5s antes de tentar novamente...")
                time.sleep(5)
            else:
                print("ERRO: Não foi possível autenticar após todas as tentativas.")
                sys.exit(1)


print("Autenticando no SIAFE...")
TOKEN = get_token()

# =========================
# 3. CONFIGURAÇÕES
# =========================

# Anos a processar
YEARS = [2026]

# UG
UG_MODE = "single"      # "single" ou "all"
UG_SINGLE = "210101"
UG_PAD_SIZE = 6

# Tabelas destino
TABELA_EMPENHO = "empenho"
TABELA_PRODUTOS = "empenho_produtos"
TABELA_ITENS = "empenho_itens"

# DELETE por ano
DELETE_DATE_FIELD = "dataEmissao"

# Performance
MAX_WORKERS_CAP = 16
CHUNKSIZE_SQL = 100

# =========================
# 4. COLUNAS
# =========================
COLUMNS_EMPENHO = [
    "id", "codigo", "codProcesso", "dataProcesso", "assuntoProcesso", "anoProcesso",
    "statusDocumento", "codigoUG", "nomeUG", "codFonte", "codNatureza", "codigoCredor",
    "nomeCredor", "dataEmissao", "dataCancelamento", "dataContabilizacao", "valor",
    "observacao", "cnpjCredor", "idNR", "codNR", "modalidade", "tipoAlteracaoNE",
    "codContrato", "codAcao", "codDetalhamentoFonte", "codigoOrgao",
    "codigoModalidadeLicitacao", "descModalidadeLicitacao", "codClassificacao"
]

INT_COLUMNS = [
    "anoProcesso", "codFonte", "codNatureza", "cnpjCredor", "idNR", "codContrato",
    "codAcao", "codDetalhamentoFonte", "codigoOrgao", "codigoModalidadeLicitacao"
]

DATE_COLUMNS = ["dataProcesso", "dataEmissao", "dataCancelamento", "dataContabilizacao"]

# =========================
# 5. CLASSIFICADORES (NORMALIZADA)
# =========================
CLASSIFICADOR_MAP = {
    28: "Fonte",
    33: "Natureza",
    54: "Contrato",
    116: "TipoPatrimonial",
    162: "SubItemDespesa",
}

# Naturezas a excluir
EXCLUDE_NATUREZA = {"3.3.90.92", "4.4.90.92"}

# =========================
# 6. FUNÇÕES AUXILIARES
# =========================

def make_session():
    session = requests.Session()
    session.verify = False
    retry = Retry(
        total=5, connect=5, read=5, status=5, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def normalize_ug(value, size):
    if value is None:
        return None
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = [c for c in s if c.isdigit()]
    s = "".join(digits)
    return s.zfill(size) if s else None


def load_ugs_from_db():
    q = text("SELECT codigo FROM ug")
    with ENGINE.connect() as conn:
        rows = conn.execute(q).fetchall()
    ugs = []
    seen = set()
    for r in rows:
        if not r:
            continue
        ug = normalize_ug(r[0], UG_PAD_SIZE)
        if ug and ug not in seen:
            seen.add(ug)
            ugs.append(ug)
    return ugs


def resolve_ugs():
    mode = (UG_MODE or "").lower().strip()
    if mode == "single":
        ug = normalize_ug(UG_SINGLE, UG_PAD_SIZE)
        if not ug:
            raise ValueError("UG_SINGLE está vazio/inválido, mas UG_MODE='single'.")
        print(f"Modo SINGLE selecionado. UG alvo: {ug}")
        return [ug]
    if mode == "all":
        ugs = load_ugs_from_db()
        if not ugs:
            raise ValueError("Nenhuma UG encontrada na tabela 'ug'.")
        return ugs
    raise ValueError("UG_MODE inválido. Use 'single' ou 'all'.")


def table_exists_mysql(conn, table_name):
    q = text("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t LIMIT 1")
    return conn.execute(q, {"t": table_name}).fetchone() is not None


def ensure_table_structure(conn, table_name, expected_columns):
    """
    Verifica se a tabela existe e tem as colunas esperadas.
    Se a estrutura for incompatível (colunas diferentes), dropa e deixa o to_sql recriar.
    Se não existe, não faz nada (to_sql com append cria automaticamente).
    Retorna True se a tabela foi dropada (para log).
    """
    if not table_exists_mysql(conn, table_name):
        return False

    q = text("""
        SELECT COLUMN_NAME
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = :t
        ORDER BY ORDINAL_POSITION
    """)
    existing_cols = {r[0] for r in conn.execute(q, {"t": table_name}).fetchall()}

    # Se as colunas esperadas não são subconjunto das existentes, estrutura incompatível
    if not expected_columns.issubset(existing_cols):
        print(f"    Tabela {table_name}: estrutura incompatível, recriando...")
        print(f"      Esperado: {sorted(expected_columns)}")
        print(f"      Existente: {sorted(existing_cols)}")
        conn.execute(text(f"DROP TABLE {table_name}"))
        return True

    return False


def _normalize_natureza(valor):
    """Remove pontos e não-numéricos. Ex: 3.3.90.30 -> 339030"""
    if valor is None:
        return None
    if isinstance(valor, float):
        return None
    s = str(valor).strip()
    if not s:
        return None
    digits = [c for c in s if c.isdigit()]
    return "".join(digits) if digits else None


def _safe_str(v):
    if v is None:
        return None
    if isinstance(v, float):
        return None
    s = str(v).strip()
    return s if s else None


# =========================
# 7. DELETE POR ANO + UG
# =========================

def delete_year_data(conn, year):
    """Deleta registros do ano nas 3 tabelas, filtrando por UG no modo single."""
    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"
    where_ug = f"AND codigoUG = '{UG_SINGLE}'" if UG_MODE == "single" else ""

    print(f"  Limpando dados de {year}...")

    # 1. Coletar IDs dos empenhos do ano (para deletar produtos filhos)
    ids_ano = []
    if table_exists_mysql(conn, TABELA_EMPENHO):
        ids_stmt = text(
            f"SELECT id FROM {TABELA_EMPENHO} "
            f"WHERE {DELETE_DATE_FIELD} >= :start AND {DELETE_DATE_FIELD} < :end {where_ug}"
        )
        ids_ano = [r[0] for r in conn.execute(ids_stmt, {"start": start_date, "end": end_date}).fetchall()]

    deleted_total = 0

    # 2. Deletar itens (classificadores normalizados)
    if table_exists_mysql(conn, TABELA_ITENS):
        del_stmt = text(
            f"DELETE FROM {TABELA_ITENS} "
            f"WHERE {DELETE_DATE_FIELD} >= :start AND {DELETE_DATE_FIELD} < :end {where_ug}"
        )
        r = conn.execute(del_stmt, {"start": start_date, "end": end_date})
        deleted_total += (r.rowcount or 0)

    # 3. Deletar produtos (por id_empenho)
    if table_exists_mysql(conn, TABELA_PRODUTOS) and ids_ano:
        stmt = text(f"DELETE FROM {TABELA_PRODUTOS} WHERE id_empenho IN :ids")
        stmt = stmt.bindparams(bindparam("ids", expanding=True))
        r = conn.execute(stmt, {"ids": ids_ano})
        deleted_total += (r.rowcount or 0)

    # 4. Deletar tabela principal
    if table_exists_mysql(conn, TABELA_EMPENHO):
        del_stmt = text(
            f"DELETE FROM {TABELA_EMPENHO} "
            f"WHERE {DELETE_DATE_FIELD} >= :start AND {DELETE_DATE_FIELD} < :end {where_ug}"
        )
        r = conn.execute(del_stmt, {"start": start_date, "end": end_date})
        deleted_total += (r.rowcount or 0)

    print(f"  Limpeza {year} concluída. {deleted_total} registros removidos.")
    return deleted_total


# =========================
# 8. EXPANSÃO: PRODUTOS
# =========================

def expand_produtos(df_raw):
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()
    rows = []
    for _, row in df_raw.iterrows():
        produtos = row.get("produtos", None)
        if not isinstance(produtos, list):
            continue
        for p in produtos:
            if not isinstance(p, dict):
                continue
            rows.append({
                "id_empenho": row.get("id"),
                "codigoUG": row.get("codigoUG"),
                "id": p.get("id"),
                "descricaoProdutoPPA": p.get("descricaoProdutoPPA"),
                "nomeProdutoGenerico": p.get("nomeProdutoGenerico"),
                "descricaoProdutoGenerico": p.get("descricaoProdutoGenerico"),
                "unidadeFornecimentoGenerico": p.get("unidadeFornecimentoGenerico"),
                "quantidadePrevista": p.get("quantidadePrevista"),
                "quantidade": p.get("quantidade"),
                "precoUnitario": p.get("precoUnitario"),
                "precoTotal": p.get("precoTotal"),
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# =========================
# 9. EXPANSÃO: ITENS (1 linha por empenho, classificadores como colunas)
# =========================

def _collect_classificadores_recursivo(bucket, obj):
    """Varre recursivamente dict/list procurando 'classificadores' em qualquer nível."""
    if obj is None:
        return
    if isinstance(obj, float):
        return
    if isinstance(obj, dict):
        cls = obj.get("classificadores", None)
        if isinstance(cls, list):
            for c in cls:
                if not isinstance(c, dict):
                    continue
                cod_tipo = c.get("codigoTipoClassificador")
                nome_class = c.get("nomeClassificador")
                if cod_tipo is None or nome_class is None:
                    continue
                try:
                    cod_tipo = int(cod_tipo)
                except Exception:
                    continue
                col = CLASSIFICADOR_MAP.get(cod_tipo)
                if not col:
                    continue
                nome_class = str(nome_class).strip()
                if not nome_class:
                    continue
                # Excluir naturezas específicas
                if col == "Natureza" and nome_class in EXCLUDE_NATUREZA:
                    continue
                # Normalizar Natureza (remover pontos)
                if col == "Natureza":
                    nome_class = _normalize_natureza(nome_class)
                if nome_class:
                    bucket.setdefault(col, set()).add(nome_class)
        for v in obj.values():
            _collect_classificadores_recursivo(bucket, v)
    elif isinstance(obj, list):
        for it in obj:
            _collect_classificadores_recursivo(bucket, it)


def expand_itens(df_raw):
    """
    Tabela empenho_itens: 1 linha por empenho.
    Classificadores consolidados como colunas (Fonte, Natureza, Contrato, etc.).
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    rows = []
    for _, row in df_raw.iterrows():
        codigo_ne = _safe_str(row.get("codigo"))
        codigo_ug = _safe_str(row.get("codigoUG"))
        data_emissao = row.get("dataEmissao")
        cod_contrato = _safe_str(row.get("codContrato"))

        # Fallback de natureza via campo do empenho
        cod_natureza = _normalize_natureza(row.get("codNatureza"))

        # Busca recursiva de classificadores
        bucket = {}
        row_as_dict = row.to_dict()
        _collect_classificadores_recursivo(bucket, row_as_dict)

        out = {
            "codigo": codigo_ne,
            "codigoUG": codigo_ug,
            "dataEmissao": data_emissao,
            "Fonte": None,
            "Natureza": None,
            "Contrato": None,
            "TipoPatrimonial": None,
            "SubItemDespesa": None,
            "CodContrato": cod_contrato,
        }

        for col in ["Fonte", "Natureza", "Contrato", "TipoPatrimonial", "SubItemDespesa"]:
            vals = sorted(bucket.get(col, []))
            if not vals:
                out[col] = None
                continue
            out[col] = " | ".join([v for v in vals if v])

        # Fallback: se Natureza ficou vazia, usa codNatureza do empenho
        if not out["Natureza"] and cod_natureza:
            out["Natureza"] = cod_natureza

        rows.append(out)

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not df.empty:
        df = df.drop_duplicates(
            subset=["codigo", "codigoUG", "dataEmissao", "CodContrato"],
            keep="first"
        )
    return df


# =========================
# 10. API FETCH
# =========================

def fetch_data(session, ug, token, year):
    start = time.time()
    url = f"https://tesouro.sefaz.pi.gov.br/siafe-api/nota-empenho/{year}/{ug}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = session.get(url, headers=headers, timeout=(5, 30))
        elapsed = time.time() - start
        if resp.status_code != 200:
            return ug, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, elapsed, resp.status_code

        try:
            data = resp.json()
        except ValueError:
            return ug, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, elapsed, "json_error"

        if not data:
            return ug, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, elapsed, "no_data"

        df_raw = pd.json_normalize(data)

        # Tabela principal
        df_main = df_raw.reindex(columns=COLUMNS_EMPENHO)
        if "codigo" in df_main.columns:
            df_main = df_main.drop_duplicates(subset="codigo", keep="first")

        # Produtos
        df_prod = expand_produtos(df_raw)

        # Itens (1 linha por empenho, classificadores como colunas)
        df_itens = expand_itens(df_raw)

        return ug, df_main, df_prod, df_itens, len(df_main), elapsed, 200
    except Exception as e:
        print(f"  Erro na requisição UG {ug}: {e}")
        return ug, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, time.time() - start, "error"


# =========================
# 11. EXECUÇÃO PRINCIPAL
# =========================

def migrate_table_if_needed(conn):
    """
    Migração: se ainda existe 'empenho_itens_normalizada', renomeia para 'empenho_itens'.
    Se 'empenho_itens' antiga (modelo antigo) existir, dropa primeiro.
    """
    old_norm = "empenho_itens_normalizada"
    has_norm = table_exists_mysql(conn, old_norm)
    has_itens = table_exists_mysql(conn, TABELA_ITENS)

    if has_norm:
        # Se empenho_itens existe com modelo antigo, dropar
        if has_itens:
            print(f"  Dropando tabela antiga '{TABELA_ITENS}' (modelo antigo)...")
            conn.execute(text(f"DROP TABLE `{TABELA_ITENS}`"))
        print(f"  Renomeando '{old_norm}' → '{TABELA_ITENS}'...")
        conn.execute(text(f"RENAME TABLE `{old_norm}` TO `{TABELA_ITENS}`"))
        print("  Migração concluída.")
        return True

    # Se empenho_itens existe mas com estrutura antiga (modelo antigo), dropar
    if has_itens:
        q = text("""
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = :t
        """)
        cols = {r[0] for r in conn.execute(q, {"t": TABELA_ITENS}).fetchall()}
        # Modelo antigo tem 'codigoTipoClassificador', modelo novo tem 'CodContrato'
        if "codigoTipoClassificador" in cols and "CodContrato" not in cols:
            print(f"  Dropando tabela '{TABELA_ITENS}' (modelo antigo detectado)...")
            conn.execute(text(f"DROP TABLE `{TABELA_ITENS}`"))
            print("  Tabela removida. Será recriada pelo to_sql().")
            return True

    return False


def main():
    t0 = time.time()
    ugs = resolve_ugs()

    if not ugs:
        print("Nenhuma UG selecionada para processamento.")
        return

    print("=" * 70)
    print(f"Iniciando atualização de dados de EMPENHO")
    print(f"Anos: {YEARS}")
    print(f"UGs: {', '.join(ugs)}")
    print("=" * 70)

    session = make_session()

    # Migração: renomear empenho_itens_normalizada → empenho_itens
    print("\nVerificando migração de tabelas...")
    with ENGINE.begin() as conn:
        migrate_table_if_needed(conn)

    # Verificar/corrigir estrutura da tabela empenho_itens
    COLS_ITENS = {"codigo", "codigoUG", "dataEmissao", "CodContrato",
                  "Fonte", "Natureza", "Contrato", "TipoPatrimonial", "SubItemDespesa"}

    print("Verificando estrutura das tabelas...")
    with ENGINE.begin() as conn:
        ensure_table_structure(conn, TABELA_ITENS, COLS_ITENS)
    print("Verificação concluída.\n")

    for year in YEARS:
        print(f"\n{'='*70}")
        print(f"PROCESSANDO ANO {year}")
        print(f"{'='*70}")
        print(f"  Buscando dados da API...")

        dfs_main, dfs_prod, dfs_itens = [], [], []

        max_workers = min(MAX_WORKERS_CAP, max(1, len(ugs)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_data, session, ug, TOKEN, year) for ug in ugs]
            for fut in as_completed(futures):
                ug, df_main, df_prod, df_itens, qtd, elapsed, status = fut.result()
                print(f"  UG {ug}: Status {status}, Registros: {qtd} ({elapsed:.1f}s)")
                if df_main is not None and not df_main.empty:
                    dfs_main.append(df_main)
                if df_prod is not None and not df_prod.empty:
                    dfs_prod.append(df_prod)
                if df_itens is not None and not df_itens.empty:
                    dfs_itens.append(df_itens)

        if not dfs_main:
            print(f"  Nenhum dado retornado da API para {year}.")
            continue

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            final_main = pd.concat(dfs_main, ignore_index=True)
            final_prod = pd.concat(dfs_prod, ignore_index=True) if dfs_prod else pd.DataFrame()
            final_itens = pd.concat(dfs_itens, ignore_index=True) if dfs_itens else pd.DataFrame()

        # Converter tipos
        for col in INT_COLUMNS:
            if col in final_main.columns:
                final_main[col] = pd.to_numeric(final_main[col], errors="coerce").fillna(0).astype("int64")
        for col in DATE_COLUMNS:
            if col in final_main.columns:
                final_main[col] = pd.to_datetime(final_main[col], errors="coerce")

        # Remover coluna id dos DataFrames de itens (evitar conflito com AUTO_INCREMENT)
        if final_itens is not None and not final_itens.empty and "id" in final_itens.columns:
            final_itens = final_itens.drop(columns=["id"], errors="ignore")

        # Gravar no banco
        try:
            with ENGINE.begin() as conn:
                delete_year_data(conn, year)

                # 1) empenho
                print(f"  Inserindo {len(final_main)} empenhos...")
                final_main.to_sql(
                    TABELA_EMPENHO, conn, if_exists="append", index=False,
                    chunksize=CHUNKSIZE_SQL, method="multi"
                )

                # 2) empenho_produtos
                if final_prod is not None and not final_prod.empty:
                    print(f"  Inserindo {len(final_prod)} produtos...")
                    final_prod.to_sql(
                        TABELA_PRODUTOS, conn, if_exists="append", index=False,
                        chunksize=CHUNKSIZE_SQL, method="multi"
                    )

                # 3) empenho_itens (classificadores normalizados)
                if final_itens is not None and not final_itens.empty:
                    print(f"  Inserindo {len(final_itens)} itens...")
                    final_itens.to_sql(
                        TABELA_ITENS, conn, if_exists="append", index=False,
                        chunksize=CHUNKSIZE_SQL, method="multi"
                    )
                else:
                    print("  Nenhum item encontrado.")

            print(f"  Ano {year}: CONCLUÍDO!")
            print(f"    Empenhos: {len(final_main)}")
            print(f"    Produtos: {len(final_prod) if not final_prod.empty else 0}")
            print(f"    Itens: {len(final_itens) if not final_itens.empty else 0}")

        except Exception as e:
            print(f"  ERRO ao gravar ano {year} no banco (rollback automático): {e}")

    elapsed_total = time.time() - t0
    print(f"\n{'='*70}")
    print(f"FINALIZADO! Tempo total: {elapsed_total:.2f}s ({elapsed_total/60:.1f} min)")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
