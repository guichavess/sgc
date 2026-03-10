"""
Script para importar ações do JSON para a tabela 'acao' no banco de dados.
Filtra apenas registros com codigoOrgao = '21' (SEAD).
Deduplica por código de ação (1 registro por código).

Uso:
    python scripts/importar_acoes.py
    python scripts/importar_acoes.py --force   (recria a tabela mesmo se já existir)
"""
import os
import sys
import json
from sqlalchemy import text, create_engine

# =========================
# 1. CARREGAR VARIÁVEIS DE AMBIENTE
# =========================
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')

from dotenv import load_dotenv
load_dotenv(dotenv_path)

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO: Variáveis de banco de dados ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"
ENGINE = create_engine(DATABASE_URI, echo=False)

# =========================
# 2. CAMINHO DO JSON
# =========================
JSON_PATH = os.path.join(base_dir, 'acao.json')
CODIGO_ORGAO_SEAD = '21'

FORCE = '--force' in sys.argv


def criar_tabela(conn):
    """Cria a tabela 'acao' com codigo UNIQUE (1 registro por ação)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS acao (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo VARCHAR(10) NOT NULL,
            codigo_programa VARCHAR(10),
            titulo VARCHAR(500),
            tipo_acao VARCHAR(50),
            codigo_esfera VARCHAR(5),
            nome_esfera VARCHAR(100),
            codigo_orgao VARCHAR(5),
            nome_orgao VARCHAR(200),
            codigo_funcao VARCHAR(5),
            nome_funcao VARCHAR(200),
            codigo_subfuncao VARCHAR(5),
            nome_subfuncao VARCHAR(200),
            descricao TEXT,
            data_criacao DATETIME,
            UNIQUE INDEX idx_acao_codigo (codigo),
            INDEX idx_acao_orgao (codigo_orgao)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """))


def importar():
    # Ler JSON
    if not os.path.exists(JSON_PATH):
        print(f"ERRO: Arquivo não encontrado: {JSON_PATH}")
        sys.exit(1)

    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        dados = json.load(f)

    print(f"[INFO] Total de registros no JSON: {len(dados)}")

    # Filtrar apenas SEAD (codigoOrgao = 21)
    dados_sead = [r for r in dados if str(r.get('codigoOrgao', '')) == CODIGO_ORGAO_SEAD]
    print(f"[INFO] Registros filtrados (codigoOrgao={CODIGO_ORGAO_SEAD}): {len(dados_sead)}")

    if not dados_sead:
        print("[AVISO] Nenhum registro encontrado para a SEAD. Abortando.")
        return

    # Deduplicar por código de ação (mantém o primeiro de cada código)
    vistos = set()
    dados_unicos = []
    for r in dados_sead:
        cod = r.get('codigo')
        if cod and cod not in vistos:
            vistos.add(cod)
            dados_unicos.append(r)

    print(f"[INFO] Ações distintas após deduplicação: {len(dados_unicos)}")

    with ENGINE.begin() as conn:
        # Recriar tabela se --force
        if FORCE:
            print("[INFO] --force: Recriando tabela 'acao'...")
            conn.execute(text("DROP TABLE IF EXISTS acao"))

        criar_tabela(conn)
        print("[OK] Tabela 'acao' criada/verificada.")

        # Limpar dados existentes antes de reinserir
        result = conn.execute(text("DELETE FROM acao"))
        print(f"[INFO] {result.rowcount} registros antigos removidos.")

        # Inserir registros deduplicados
        sql_insert = text("""
            INSERT INTO acao (
                codigo, codigo_programa, titulo, tipo_acao,
                codigo_esfera, nome_esfera, codigo_orgao, nome_orgao,
                codigo_funcao, nome_funcao, codigo_subfuncao, nome_subfuncao,
                descricao, data_criacao
            ) VALUES (
                :codigo, :codigo_programa, :titulo, :tipo_acao,
                :codigo_esfera, :nome_esfera, :codigo_orgao, :nome_orgao,
                :codigo_funcao, :nome_funcao, :codigo_subfuncao, :nome_subfuncao,
                :descricao, :data_criacao
            )
        """)

        inseridos = 0
        erros = 0
        for r in dados_unicos:
            # Tratar dataCriacao
            data_criacao = None
            if r.get('dataCriacao'):
                try:
                    dt_str = r['dataCriacao']
                    if '.' in dt_str:
                        dt_str = dt_str.split('.')[0]
                    data_criacao = dt_str.replace('T', ' ')
                except Exception:
                    data_criacao = None

            params = {
                'codigo': r.get('codigo'),
                'codigo_programa': r.get('codigoPrograma'),
                'titulo': r.get('titulo'),
                'tipo_acao': r.get('tipoAcao'),
                'codigo_esfera': r.get('codigoEsfera'),
                'nome_esfera': r.get('nomeEsfera'),
                'codigo_orgao': r.get('codigoOrgao'),
                'nome_orgao': r.get('nomeOrgao'),
                'codigo_funcao': r.get('codigoFuncao'),
                'nome_funcao': r.get('nomeFuncao'),
                'codigo_subfuncao': r.get('codigoSubfuncao'),
                'nome_subfuncao': r.get('nomeSubfuncao'),
                'descricao': r.get('descricao'),
                'data_criacao': data_criacao,
            }

            try:
                conn.execute(sql_insert, params)
                inseridos += 1
            except Exception as e:
                erros += 1
                print(f"  [ERRO] codigo={r.get('codigo')}: {e}")

        print(f"\n{'='*50}")
        print(f"  Inseridos: {inseridos}")
        print(f"  Erros:     {erros}")
        print(f"  Total:     {len(dados_unicos)}")
        print(f"{'='*50}")


if __name__ == '__main__':
    importar()
