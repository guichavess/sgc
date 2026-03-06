"""
Script de migracao: Cria tabela diarias_cotacoes_voos.

Tabela para armazenar cotacoes detalhadas de voos com suporte a conexoes.

Uso:
  python scripts/criar_tabela_cotacoes_voos.py
"""
import os
import sys
import pymysql
from dotenv import load_dotenv

# Carrega .env do diretorio raiz do projeto
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = os.getenv('DB_NAME', 'sgc')


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )


def run_migration():
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 60)
    print("Migracao: Tabela diarias_cotacoes_voos")
    print("=" * 60)

    print("\n[1/1] Criando tabela diarias_cotacoes_voos...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS diarias_cotacoes_voos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            itinerario_id INT NOT NULL,
            contrato_codigo VARCHAR(20) NULL,
            tipo_trecho VARCHAR(10) NOT NULL COMMENT 'ida ou volta',
            -- Trecho 1 (obrigatorio)
            cia VARCHAR(50) NOT NULL COMMENT 'Companhia aerea (LATAM, GOL, Azul)',
            voo VARCHAR(20) NOT NULL COMMENT 'Numero do voo (LA 3853, G3 1519)',
            saida DATETIME NOT NULL,
            chegada DATETIME NOT NULL,
            origem VARCHAR(100) NOT NULL,
            destino VARCHAR(100) NOT NULL,
            -- Trecho 2 - conexao (opcional)
            cia_conexao VARCHAR(50) NULL,
            voo_conexao VARCHAR(20) NULL,
            saida_conexao DATETIME NULL,
            chegada_conexao DATETIME NULL,
            origem_conexao VARCHAR(100) NULL,
            destino_conexao VARCHAR(100) NULL,
            -- Dados gerais
            bagagem VARCHAR(50) NULL COMMENT '1 mala despachada, Apenas mao, etc.',
            valor DECIMAL(10,2) NOT NULL COMMENT 'Valor total da opcao',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_cotacao_voo_itin (itinerario_id),
            FOREIGN KEY (itinerario_id) REFERENCES diarias_itinerario(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    print("   OK - tabela diarias_cotacoes_voos criada/verificada.")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("Migracao concluida com sucesso!")
    print("=" * 60)


if __name__ == '__main__':
    run_migration()
