"""
Script para adicionar coluna 'num_ne' na tabela sis_solicitacoes.

Uso:
    python scripts/add_coluna_num_ne.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from sqlalchemy import text


def main():
    app = create_app()
    with app.app_context():
        inspector = db.inspect(db.engine)
        colunas = [col['name'] for col in inspector.get_columns('sis_solicitacoes')]

        if 'num_ne' in colunas:
            print('[--] Coluna "num_ne" ja existe.')
            return

        db.session.execute(text(
            "ALTER TABLE sis_solicitacoes ADD COLUMN num_ne VARCHAR(50) NULL AFTER status_geral"
        ))
        db.session.commit()
        print('[OK] Coluna "num_ne" adicionada com sucesso.')


if __name__ == '__main__':
    main()
