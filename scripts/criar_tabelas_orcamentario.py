"""
Script de migração: cria tabelas para o módulo Orçamentário do Financeiro.
Tabelas: fornecedores_sem_contrato, fornecedores_contratos, execucoes_orcamentarias

Uso:
    python scripts/criar_tabelas_orcamentario.py
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
    print("ERRO CRÍTICO: Variáveis de banco de dados ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"
ENGINE = create_engine(DATABASE_URI, echo=False)


def criar_tabelas():
    with ENGINE.begin() as conn:
        # 1. Fornecedores sem contrato
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fornecedores_sem_contrato (
                id INT AUTO_INCREMENT PRIMARY KEY,
                descricao VARCHAR(255) NOT NULL,
                cnpj VARCHAR(18) NOT NULL,
                telefone VARCHAR(20),
                data_criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
                criado_por BIGINT UNSIGNED,
                CONSTRAINT fk_fornecedor_criado_por FOREIGN KEY (criado_por) REFERENCES sis_usuarios(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        print("[OK] Tabela 'fornecedores_sem_contrato' criada/verificada.")

        # 2. Fornecedores contratos (ligação 1:N)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fornecedores_contratos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                fornecedor_id INT NOT NULL,
                cod_contrato VARCHAR(20) NOT NULL,
                data_vinculacao DATETIME DEFAULT CURRENT_TIMESTAMP,
                vinculado_por BIGINT UNSIGNED,
                CONSTRAINT fk_forncontr_fornecedor FOREIGN KEY (fornecedor_id) REFERENCES fornecedores_sem_contrato(id) ON DELETE CASCADE,
                CONSTRAINT fk_forncontr_vinculado_por FOREIGN KEY (vinculado_por) REFERENCES sis_usuarios(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        print("[OK] Tabela 'fornecedores_contratos' criada/verificada.")

        # 3. Execuções orçamentárias
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS execucoes_orcamentarias (
                id INT AUTO_INCREMENT PRIMARY KEY,
                fornecedor_id INT NOT NULL,
                descricao TEXT NOT NULL,
                item VARCHAR(255),
                quantidade DECIMAL(15,2),
                competencia VARCHAR(7),
                cod_contrato VARCHAR(20),
                acao VARCHAR(50),
                natureza VARCHAR(50),
                fonte VARCHAR(50),
                data_criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
                criado_por BIGINT UNSIGNED,
                CONSTRAINT fk_execorc_fornecedor FOREIGN KEY (fornecedor_id) REFERENCES fornecedores_sem_contrato(id),
                CONSTRAINT fk_execorc_criado_por FOREIGN KEY (criado_por) REFERENCES sis_usuarios(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        print("[OK] Tabela 'execucoes_orcamentarias' criada/verificada.")

    print("\nTodas as tabelas do módulo Orçamentário foram criadas com sucesso!")


if __name__ == "__main__":
    criar_tabelas()
