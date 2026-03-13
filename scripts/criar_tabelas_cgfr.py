"""
Script de migração: cria tabelas do módulo CGFR.
- cgfr_acao (lookup de ações orçamentárias)
- cgfr_processo_enviado (tabela principal)

Não cria natdespesas nem class_fonte (já existem no banco).

Uso: python scripts/criar_tabelas_cgfr.py
"""
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Carrega .env do diretório raiz do projeto
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'sgc')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"


def criar_tabelas():
    engine = create_engine(DATABASE_URI)

    with engine.connect() as conn:
        # 1. Tabela cgfr_acao
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cgfr_acao (
                id INT AUTO_INCREMENT PRIMARY KEY,
                acao VARCHAR(255) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        print("[OK] Tabela cgfr_acao criada/verificada.")

        # 2. Tabela cgfr_processo_enviado
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cgfr_processo_enviado (
                processo_formatado VARCHAR(255) NOT NULL PRIMARY KEY,

                -- Campos SYNC (atualizados pelo sync, nunca editáveis pelo usuário)
                especificacao VARCHAR(500) NULL,
                link_acesso VARCHAR(500) NULL,
                id_unidade_geradora VARCHAR(50) NULL,
                geracao_sigla VARCHAR(100) NULL,
                geracao_data DATETIME NULL,
                geracao_descricao VARCHAR(500) NULL,
                usuario_gerador VARCHAR(255) NULL,

                ultimo_andamento_sigla VARCHAR(100) NULL,
                ultimo_andamento_descricao VARCHAR(500) NULL,
                ultimo_andamento_data VARCHAR(50) NULL,
                ultimo_andamento_usuario VARCHAR(255) NULL,

                tramitado_sead_cgfr VARCHAR(50) NULL,
                recebido_cgfr INT DEFAULT 0,
                data_recebido_cgfr VARCHAR(50) NULL,
                devolvido_cgfr_sead INT DEFAULT 0,
                data_devolvido_cgfr_sead VARCHAR(50) NULL,

                tipo_processo VARCHAR(200) NULL,

                -- Campos EDITÁVEIS (preenchidos pelo usuário, NUNCA sobrescritos pelo sync)
                natureza_despesa_id INT NULL,
                fonte_id INT NULL,
                acao_id INT NULL,

                fornecedor VARCHAR(255) NULL,
                objeto_do_pedido TEXT NULL,
                necessidade TEXT NULL,
                deliberacao TEXT NULL,
                tipo_despesa VARCHAR(50) NULL,

                valor_solicitado DECIMAL(12,2) NULL,
                valor_aprovado DECIMAL(12,2) NULL,

                data_da_reuniao DATE NULL,
                observacao TEXT NULL,
                possui_reserva INT DEFAULT 0,
                valor_reserva VARCHAR(30) NULL,
                nivel_prioridade VARCHAR(10) NULL,

                data_inclusao DATETIME NULL,

                -- Foreign Keys
                CONSTRAINT fk_cgfr_natureza FOREIGN KEY (natureza_despesa_id) REFERENCES natdespesas(id),
                CONSTRAINT fk_cgfr_fonte FOREIGN KEY (fonte_id) REFERENCES class_fonte(id),
                CONSTRAINT fk_cgfr_acao FOREIGN KEY (acao_id) REFERENCES cgfr_acao(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        print("[OK] Tabela cgfr_processo_enviado criada/verificada.")

        conn.commit()

    print("\n=== Migração CGFR concluída com sucesso! ===")


if __name__ == '__main__':
    criar_tabelas()
