"""
Cria a tabela `diarias_notas_reserva` — 1 Nota de Reserva por servidor
em cada solicitação de diária.

Uso:
    python scripts/criar_tabela_notas_reserva.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasNotaReserva


def criar_tabela():
    inspector = db.inspect(db.engine)
    tabelas = inspector.get_table_names()

    if 'diarias_notas_reserva' in tabelas:
        print('[--] Tabela "diarias_notas_reserva" já existe.')
        return

    DiariasNotaReserva.__table__.create(db.engine)
    print('[OK] Tabela "diarias_notas_reserva" criada.')


def main():
    app = create_app()
    with app.app_context():
        print('=== Criação da tabela diarias_notas_reserva ===\n')
        criar_tabela()
        print('\n=== Concluído! ===')


if __name__ == '__main__':
    main()
