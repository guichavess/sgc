'''
Migração: adicionar coluna item_vinculado_id na tabela execucoes
================================================================
Rodar em produção para resolver o erro:
  "Unknown column 'execucoes.item_vinculado_id' in 'field list'"

Uso:
  python scripts/migrar_execucoes_item_vinculado.py
'''
import os
import sys
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DB = dict(
    host=os.getenv('DB_HOST', 'localhost'),
    user=os.getenv('DB_USER', 'root'),
    password=os.getenv('DB_PASS', ''),
    database=os.getenv('DB_NAME', 'sgc'),
    charset='utf8mb4',
)


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    # Verificar se a coluna já existe
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = 'execucoes'
          AND COLUMN_NAME = 'item_vinculado_id'
    """, (DB['database'],))
    existe = cur.fetchone()[0] > 0

    if existe:
        print('Coluna item_vinculado_id já existe em execucoes. Nada a fazer.')
    else:
        print('Adicionando coluna item_vinculado_id em execucoes...')
        cur.execute("""
            ALTER TABLE execucoes
            ADD COLUMN item_vinculado_id INT NULL,
            ADD INDEX idx_exec_vinculado (item_vinculado_id),
            ADD CONSTRAINT fk_exec_vinculado
                FOREIGN KEY (item_vinculado_id)
                REFERENCES itens_vinculados(id)
                ON DELETE SET NULL
        """)
        conn.commit()
        print('Coluna adicionada com sucesso!')

    conn.close()


if __name__ == '__main__':
    main()
