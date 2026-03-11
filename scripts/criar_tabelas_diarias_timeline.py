"""
Script de migração: Cria tabelas de timeline para o módulo de Diárias.

Tabelas criadas:
  - diarias_etapas (etapas do fluxo)
  - diarias_historico_movimentacoes (histórico de transições)

Alterações:
  - diarias_itinerario: ADD COLUMN etapa_atual_id

Seed:
  - 3 etapas iniciais (Solicitação Iniciada, Financeiro, Aquisição de Passagens)

Retroativo:
  - Insere histórico de etapa 1 para todos os itinerários existentes

Uso:
  python scripts/criar_tabelas_diarias_timeline.py
"""
import os
import sys
import pymysql
from dotenv import load_dotenv

# Carrega .env do diretório raiz do projeto
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
    print("Migração: Timeline do módulo de Diárias")
    print("=" * 60)

    # 1. Criar tabela diarias_etapas
    print("\n[1/5] Criando tabela diarias_etapas...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS diarias_etapas (
            id INT PRIMARY KEY,
            nome VARCHAR(100) NOT NULL,
            alias VARCHAR(50) NOT NULL,
            ordem INT NOT NULL,
            cor_hex VARCHAR(10),
            icone VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    print("   OK - tabela diarias_etapas criada/verificada.")

    # 2. Criar tabela diarias_historico_movimentacoes
    print("\n[2/5] Criando tabela diarias_historico_movimentacoes...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS diarias_historico_movimentacoes (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            id_itinerario BIGINT NOT NULL,
            id_etapa_anterior INT NULL,
            id_etapa_nova INT NOT NULL,
            id_usuario_responsavel BIGINT UNSIGNED NULL,
            data_movimentacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            comentario TEXT NULL,
            FOREIGN KEY (id_itinerario) REFERENCES diarias_itinerario(id) ON DELETE CASCADE,
            FOREIGN KEY (id_etapa_nova) REFERENCES diarias_etapas(id),
            FOREIGN KEY (id_usuario_responsavel) REFERENCES sis_usuarios(id) ON DELETE SET NULL,
            INDEX idx_diarias_hist_itinerario (id_itinerario),
            INDEX idx_diarias_hist_etapa (id_etapa_nova)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    print("   OK - tabela diarias_historico_movimentacoes criada/verificada.")

    # 3. Seed: inserir etapas (ANTES de criar FK para evitar erro de integridade)
    print("\n[3/5] Inserindo etapas (seed)...")
    etapas = [
        (1, 'Solicitacao Iniciada', 'solicitacao_iniciada', 1, '#0d6efd', 'fas fa-file-alt'),
        (2, 'Financeiro', 'financeiro', 2, '#fd7e14', 'fas fa-search-dollar'),
        (3, 'Aquisição de Passagens', 'aquisicao_passagens', 3, '#6f42c1', 'fas fa-plane-departure'),
    ]
    for etapa in etapas:
        cursor.execute("""
            INSERT INTO diarias_etapas (id, nome, alias, ordem, cor_hex, icone)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE nome=VALUES(nome), alias=VALUES(alias),
                                     ordem=VALUES(ordem), cor_hex=VALUES(cor_hex), icone=VALUES(icone)
        """, etapa)
    print(f"   OK - {len(etapas)} etapas inseridas/atualizadas.")

    # 4. Adicionar coluna etapa_atual_id no diarias_itinerario
    print("\n[4/5] Adicionando coluna etapa_atual_id em diarias_itinerario...")
    cursor.execute("""
        SELECT COUNT(*) as cnt
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = 'diarias_itinerario'
          AND COLUMN_NAME = 'etapa_atual_id'
    """, (DB_NAME,))
    exists = cursor.fetchone()['cnt'] > 0

    if exists:
        print("   SKIP - coluna etapa_atual_id já existe.")
    else:
        # Adiciona coluna primeiro sem FK (para preencher default)
        cursor.execute("""
            ALTER TABLE diarias_itinerario
            ADD COLUMN etapa_atual_id INT DEFAULT 1
        """)
        # Garante que todos os registros existentes tenham valor 1
        cursor.execute("UPDATE diarias_itinerario SET etapa_atual_id = 1 WHERE etapa_atual_id IS NULL")
        # Agora adiciona a FK
        cursor.execute("""
            ALTER TABLE diarias_itinerario
            ADD CONSTRAINT fk_diarias_itin_etapa
                FOREIGN KEY (etapa_atual_id) REFERENCES diarias_etapas(id)
        """)
        print("   OK - coluna etapa_atual_id adicionada com FK.")

    # 5. Retroativo: criar histórico para itinerários existentes
    print("\n[5/5] Criando histórico retroativo para itinerários existentes...")
    cursor.execute("""
        SELECT i.id, i.created_at
        FROM diarias_itinerario i
        LEFT JOIN diarias_historico_movimentacoes h ON h.id_itinerario = i.id
        WHERE h.id IS NULL
    """)
    sem_historico = cursor.fetchall()

    if sem_historico:
        for row in sem_historico:
            data_mov = row['created_at'] or '2026-01-01 00:00:00'
            cursor.execute("""
                INSERT INTO diarias_historico_movimentacoes
                    (id_itinerario, id_etapa_anterior, id_etapa_nova, data_movimentacao, comentario)
                VALUES (%s, NULL, 1, %s, 'Registro retroativo - Solicitacao criada')
            """, (row['id'], data_mov))
        # Atualizar etapa_atual_id para todos que não tinham
        cursor.execute("""
            UPDATE diarias_itinerario SET etapa_atual_id = 1
            WHERE etapa_atual_id IS NULL OR etapa_atual_id = 0
        """)
        print(f"   OK - {len(sem_historico)} registros retroativos criados.")
    else:
        print("   SKIP - todos os itinerários já possuem histórico.")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("Migração concluída com sucesso!")
    print("=" * 60)


if __name__ == '__main__':
    run_migration()
