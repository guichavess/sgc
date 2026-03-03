"""
Script de migração: Adiciona colunas da API pessoaSGA à tabela diarias_itens_itinerario.

Novas colunas:
- banco_agencia (VARCHAR 50) — agência bancária do servidor
- banco_conta (VARCHAR 50) — conta bancária do servidor
- vinculo (VARCHAR 100) — tipo de vínculo (efetivo, comissionado, etc.)
- cargo_folha (VARCHAR 255) — cargo real da folha de pagamento
- setor (VARCHAR 255) — setor/lotação do servidor
- orgao (VARCHAR 255) — órgão do servidor

Uso:
    python scripts/migrar_itens_itinerario_sga.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db

app = create_app()

COLUNAS = [
    ("banco_agencia", "VARCHAR(50) NULL DEFAULT NULL"),
    ("banco_conta", "VARCHAR(50) NULL DEFAULT NULL"),
    ("vinculo", "VARCHAR(100) NULL DEFAULT NULL"),
    ("cargo_folha", "VARCHAR(255) NULL DEFAULT NULL"),
    ("setor", "VARCHAR(255) NULL DEFAULT NULL"),
    ("orgao", "VARCHAR(255) NULL DEFAULT NULL"),
]

TABELA = "diarias_itens_itinerario"


def coluna_existe(conn, tabela, coluna):
    """Verifica se uma coluna já existe na tabela."""
    result = conn.execute(
        db.text(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :tabela AND COLUMN_NAME = :coluna"
        ),
        {"tabela": tabela, "coluna": coluna},
    )
    return result.scalar() > 0


def main():
    with app.app_context():
        with db.engine.connect() as conn:
            print(f"[INFO] Migrando tabela '{TABELA}'...")

            adicionadas = 0
            for nome, tipo in COLUNAS:
                if coluna_existe(conn, TABELA, nome):
                    print(f"  [SKIP] Coluna '{nome}' já existe.")
                else:
                    sql = f"ALTER TABLE `{TABELA}` ADD COLUMN `{nome}` {tipo}"
                    conn.execute(db.text(sql))
                    print(f"  [OK]   Coluna '{nome}' adicionada.")
                    adicionadas += 1

            conn.commit()

            if adicionadas > 0:
                print(f"\n[SUCESSO] {adicionadas} coluna(s) adicionada(s) à tabela '{TABELA}'.")
            else:
                print(f"\n[INFO] Nenhuma alteração necessária. Todas as colunas já existem.")


if __name__ == "__main__":
    main()
