"""
Script de migração: Adiciona colunas para requisição de passagens aéreas.

Alterações:
  - diarias_itinerario: ADD COLUMN sei_id_requisicao_passagens VARCHAR(50)
  - diarias_itinerario: ADD COLUMN sei_requisicao_passagens_formatado VARCHAR(50)

Uso:
  python scripts/adicionar_colunas_passagens.py
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


def column_exists(cursor, table, column):
    """Verifica se uma coluna já existe na tabela."""
    cursor.execute(
        "SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (DB_NAME, table, column)
    )
    return cursor.fetchone()['cnt'] > 0


def main():
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 60)
    print("Migração: Colunas de Requisição de Passagens Aéreas")
    print("=" * 60)

    try:
        # 1. Adicionar sei_id_requisicao_passagens
        col = 'sei_id_requisicao_passagens'
        if column_exists(cursor, 'diarias_itinerario', col):
            print(f"  [OK] Coluna '{col}' já existe.")
        else:
            cursor.execute(
                f"ALTER TABLE diarias_itinerario "
                f"ADD COLUMN {col} VARCHAR(50) NULL "
                f"AFTER sei_requisicao_formatado"
            )
            print(f"  [+] Coluna '{col}' adicionada.")

        # 2. Adicionar sei_requisicao_passagens_formatado
        col = 'sei_requisicao_passagens_formatado'
        if column_exists(cursor, 'diarias_itinerario', col):
            print(f"  [OK] Coluna '{col}' já existe.")
        else:
            cursor.execute(
                f"ALTER TABLE diarias_itinerario "
                f"ADD COLUMN {col} VARCHAR(50) NULL "
                f"AFTER sei_id_requisicao_passagens"
            )
            print(f"  [+] Coluna '{col}' adicionada.")

        conn.commit()
        print("\n✅ Migração concluída com sucesso!")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Erro na migração: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
