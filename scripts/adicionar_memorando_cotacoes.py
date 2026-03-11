"""
Adiciona colunas sei_id_memorando_cotacoes e sei_memorando_cotacoes_formatado
à tabela diarias_itinerario para o 2º SEAD_MEMORANDO_SGA (cotações).
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app, db

TABLE = "diarias_itinerario"

COLUMNS = [
    ("sei_id_memorando_cotacoes", "VARCHAR(50) NULL"),
    ("sei_memorando_cotacoes_formatado", "VARCHAR(50) NULL"),
]

def main():
    app = create_app()
    with app.app_context():
        insp = db.inspect(db.engine)
        existing = [c['name'] for c in insp.get_columns(TABLE)]

        for col_name, col_def in COLUMNS:
            if col_name in existing:
                print(f"  [SKIP] {col_name} ja existe.")
            else:
                sql = f"ALTER TABLE {TABLE} ADD COLUMN {col_name} {col_def}"
                db.session.execute(db.text(sql))
                print(f"  [ADD]  {col_name}")

        db.session.commit()
        print("\nMigracao concluida com sucesso!")

if __name__ == '__main__':
    main()
