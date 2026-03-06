"""
Script de migração: adiciona colunas do Quadro Orçamentário à tabela diarias_itinerarios.

Uso:
    python scripts/adicionar_quadro_orcamentario.py
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db

app = create_app()

COLUNAS = [
    ("quadro_ug", "VARCHAR(20) NULL"),
    ("quadro_funcao", "VARCHAR(10) NULL"),
    ("quadro_subfuncao", "VARCHAR(10) NULL"),
    ("quadro_programa", "VARCHAR(10) NULL"),
    ("quadro_plano_interno", "VARCHAR(10) NULL"),
    ("quadro_fonte_recursos", "VARCHAR(20) NULL"),
    ("quadro_natureza_despesa", "VARCHAR(20) NULL"),
    ("quadro_valor_inicial_nr", "DECIMAL(14,2) NULL"),
    ("quadro_saldo_nr", "DECIMAL(14,2) NULL"),
    ("quadro_valor_despesa", "DECIMAL(14,2) NULL"),
    ("quadro_saldo_atual_nr", "DECIMAL(14,2) NULL"),
    ("sei_id_quadro_orcamentario", "VARCHAR(50) NULL"),
    ("sei_quadro_orcamentario_formatado", "VARCHAR(50) NULL"),
]

TABLE = "diarias_itinerario"


def main():
    with app.app_context():
        inspector = db.inspect(db.engine)
        existing = [c['name'] for c in inspector.get_columns(TABLE)]

        added = 0
        for col_name, col_type in COLUNAS:
            if col_name in existing:
                print(f"  [SKIP] {col_name} já existe.")
                continue
            sql = f"ALTER TABLE {TABLE} ADD COLUMN {col_name} {col_type}"
            db.session.execute(db.text(sql))
            print(f"  [ADD]  {col_name} {col_type}")
            added += 1

        db.session.commit()
        print(f"\nMigração concluída. {added} coluna(s) adicionada(s).")


if __name__ == '__main__':
    main()
