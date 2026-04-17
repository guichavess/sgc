"""
Cria a tabela `diarias_notas_empenho` — 1 Nota de Empenho por servidor
em cada solicitação de diária (mesmo padrão de `diarias_notas_reserva`).

Uso:
    python scripts/criar_tabela_notas_empenho.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasNotaEmpenho


def criar_tabela():
    inspector = db.inspect(db.engine)
    tabelas = inspector.get_table_names()

    if 'diarias_notas_empenho' in tabelas:
        print('[--] Tabela "diarias_notas_empenho" ja existe.')
        return

    DiariasNotaEmpenho.__table__.create(db.engine)
    print('[OK] Tabela "diarias_notas_empenho" criada.')


def main():
    app = create_app()
    with app.app_context():
        print('=== Criacao da tabela diarias_notas_empenho ===\n')
        criar_tabela()
        print('\n=== Concluido! ===')


if __name__ == '__main__':
    main()
