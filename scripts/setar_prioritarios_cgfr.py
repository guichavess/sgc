"""
Script para definir processos CGFR como prioritários (nivel_prioridade = 'Alto').

Uso: python scripts/setar_prioritarios_cgfr.py
"""
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'sgc')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"

PROTOCOLOS = [
    '00002.010660/2025-45',
    '00002.009949/2025-11',
    '00002.004235/2025-17',
    '00002.003312/2025-11',
    '00002.003207/2025-82',
    '00002.002300/2025-70',
    '00002.000987/2025-17',
    '00002.000864/2025-78',
    '00002.010122/2024-70',
    '00002.009073/2024-22',
    '00002.009021/2024-56',
    '00002.006348/2024-76',
    '00002.006343/2024-43',
    '00002.000777/2026-00',
    '00002.010043/2025-40',
]


def main():
    engine = create_engine(DATABASE_URI)

    with engine.connect() as conn:
        # Verificar quais existem
        for p in PROTOCOLOS:
            row = conn.execute(
                text("SELECT processo_formatado, nivel_prioridade FROM cgfr_processo_enviado WHERE processo_formatado = :p"),
                {'p': p}
            ).fetchone()
            if row:
                print(f"  {p} -> atual: {row[1] or 'NULL'}")
            else:
                print(f"  {p} -> NAO ENCONTRADO")

        # Atualizar
        placeholders = ', '.join(f':p{i}' for i in range(len(PROTOCOLOS)))
        params = {f'p{i}': p for i, p in enumerate(PROTOCOLOS)}

        result = conn.execute(
            text(f"UPDATE cgfr_processo_enviado SET nivel_prioridade = 'Alto' WHERE processo_formatado IN ({placeholders})"),
            params
        )
        conn.commit()

        print(f"\n[OK] {result.rowcount} processos atualizados para nivel_prioridade = 'Alto'")


if __name__ == '__main__':
    main()
