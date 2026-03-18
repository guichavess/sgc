"""
Script de migração: cria tabela cgfrmovimentacao.
Espelha a estrutura de seimovimentacao para armazenar
documentos SEI dos processos CGFR.

Uso: python scripts/criar_tabela_cgfrmovimentacao.py
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'sgc')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"


def criar_tabela():
    engine = create_engine(DATABASE_URI)

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cgfrmovimentacao (
                IdDocumento VARCHAR(50) NOT NULL PRIMARY KEY,
                protocolo_procedimento VARCHAR(50) NULL,
                IdProcedimento VARCHAR(50) NULL,
                ProcedimentoFormatado VARCHAR(50) NULL,
                DocumentoFormatado VARCHAR(50) NULL,
                LinkAcesso TEXT NULL,
                Descricao TEXT NULL,
                `Data` VARCHAR(20) NULL,
                Numero VARCHAR(50) NULL,
                IdSerie INT NULL,
                `Serie.Nome` VARCHAR(255) NULL,
                `Serie.Aplicabilidade` VARCHAR(100) NULL,
                `UnidadeElaboradora.IdUnidade` VARCHAR(50) NULL,
                `UnidadeElaboradora.Sigla` VARCHAR(50) NULL,
                `UnidadeElaboradora.Descricao` VARCHAR(255) NULL,
                obs TEXT NULL,
                tempo_execucao FLOAT NULL,

                INDEX idx_cgfrmov_protocolo (protocolo_procedimento)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        print("[OK] Tabela cgfrmovimentacao criada/verificada.")
        conn.commit()

    print("\n=== Migração cgfrmovimentacao concluída com sucesso! ===")


if __name__ == '__main__':
    criar_tabela()
