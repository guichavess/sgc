"""
Script de migração: Cria tabelas de controle de diárias.

Tabelas:
  1. diarias_controle_viagens     — 1 registro por processo SEI
  2. diarias_controle_servidores  — 1 registro por servidor por viagem
  3. diarias_controle_prestacao   — 1 registro por prestação de contas

Uso:
  python scripts/criar_tabelas_controle_diarias.py
"""
import os
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = os.getenv('DB_NAME', 'sgc')


def get_connection():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor,
    )


SQL_VIAGENS = """
CREATE TABLE IF NOT EXISTS diarias_controle_viagens (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    processo VARCHAR(50) NOT NULL,
    itinerario_id INT NULL,
    setor_id BIGINT NULL,
    origem VARCHAR(100),
    destino VARCHAR(255),
    data_inicio DATE NOT NULL,
    data_termino DATE NOT NULL,
    status_viagem SMALLINT DEFAULT 1 COMMENT '1=Realizada, 2=Cancelada',
    observacao TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_processo (processo),
    KEY idx_data_inicio (data_inicio),
    KEY idx_setor (setor_id),
    KEY idx_itinerario (itinerario_id),

    CONSTRAINT fk_cv_itinerario FOREIGN KEY (itinerario_id)
        REFERENCES diarias_itinerario(id) ON DELETE SET NULL,
    CONSTRAINT fk_cv_setor FOREIGN KEY (setor_id)
        REFERENCES setor(identidade) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Viagens de diárias - nível processo';
"""

SQL_SERVIDORES = """
CREATE TABLE IF NOT EXISTS diarias_controle_servidores (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    viagem_id BIGINT NOT NULL,
    cpf VARCHAR(14) NOT NULL,
    nome VARCHAR(255),
    vinculo VARCHAR(50),
    qtd_diarias DECIMAL(4,1) NOT NULL DEFAULT 0,
    valor_unitario DECIMAL(10,2),
    valor_total DECIMAL(10,2),
    natureza_despesa VARCHAR(10),
    sub_item VARCHAR(10),
    fonte_recursos VARCHAR(20),
    baixa_np VARCHAR(50) NULL,
    sistema_scdp VARCHAR(20) NULL,

    KEY idx_cpf (cpf),
    KEY idx_viagem (viagem_id),
    KEY idx_cpf_viagem (cpf, viagem_id),

    CONSTRAINT fk_cs_viagem FOREIGN KEY (viagem_id)
        REFERENCES diarias_controle_viagens(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Servidores por viagem - dados financeiros';
"""

SQL_PRESTACAO = """
CREATE TABLE IF NOT EXISTS diarias_controle_prestacao (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    servidor_id BIGINT NOT NULL,
    status SMALLINT DEFAULT 2 COMMENT '1=Entregue, 2=Pendente',
    data_entrega DATE NULL,
    relatorio SMALLINT NULL COMMENT '1=Aprovado, 2=Reprovado, 3=Pendente',
    ano_referencia SMALLINT NULL,

    UNIQUE KEY uk_servidor (servidor_id),

    CONSTRAINT fk_cp_servidor FOREIGN KEY (servidor_id)
        REFERENCES diarias_controle_servidores(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Prestação de contas por servidor por viagem';
"""


def run():
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 60)
    print("Criação de tabelas de controle de diárias")
    print("=" * 60)

    tables = [
        ('diarias_controle_viagens', SQL_VIAGENS),
        ('diarias_controle_servidores', SQL_SERVIDORES),
        ('diarias_controle_prestacao', SQL_PRESTACAO),
    ]

    for name, sql in tables:
        cursor.execute(f"SHOW TABLES LIKE '{name}'")
        if cursor.fetchone():
            print(f"  SKIP - {name} já existe.")
        else:
            cursor.execute(sql)
            print(f"  OK   - {name} criada.")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("Tabelas criadas com sucesso!")
    print("=" * 60)


if __name__ == '__main__':
    run()
