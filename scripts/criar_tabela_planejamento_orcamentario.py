"""
Script de migração: cria tabela planejamento_orcamentario.

Uso:
    python scripts/criar_tabela_planejamento_orcamentario.py
"""
import os
import sys
from sqlalchemy import text, create_engine

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')

from dotenv import load_dotenv
load_dotenv(dotenv_path)

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO: Variáveis de banco ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"
ENGINE = create_engine(DATABASE_URI, echo=False)


def criar_tabela():
    with ENGINE.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS planejamento_orcamentario (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cod_contrato VARCHAR(20) NOT NULL,
                competencia VARCHAR(7) NOT NULL,
                valor DECIMAL(15,2),
                dt_lancamento DATETIME DEFAULT CURRENT_TIMESTAMP,
                usuario BIGINT UNSIGNED,
                planejamento_inicial TINYINT(1) DEFAULT 0,
                repactuacao_prorrogacao TINYINT(1) DEFAULT 0,
                CONSTRAINT fk_plan_orc_usuario FOREIGN KEY (usuario) REFERENCES sis_usuarios(id),
                INDEX idx_plan_orc_contrato (cod_contrato),
                INDEX idx_plan_orc_competencia (competencia)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        print("[OK] Tabela 'planejamento_orcamentario' criada/verificada.")

        # Adicionar unique constraint se não existir
        try:
            conn.execute(text("""
                ALTER TABLE planejamento_orcamentario
                ADD UNIQUE INDEX uq_plan_contrato_comp (cod_contrato, competencia)
            """))
            print("[OK] Índice único (cod_contrato, competencia) criado.")
        except Exception:
            print("[OK] Índice único já existe.")


if __name__ == "__main__":
    criar_tabela()
