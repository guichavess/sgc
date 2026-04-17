"""
Cria as 3 tabelas NL / PD / OB por servidor:
- diarias_notas_liquidacao
- diarias_programacoes_desembolso
- diarias_ordens_bancarias

Mesmo padrao de diarias_notas_reserva / diarias_notas_empenho.

Uso:
    python scripts/criar_tabelas_nl_pd_ob.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.diaria import (
    DiariasNotaLiquidacao, DiariasProgramacaoDesembolso, DiariasOrdemBancaria
)


def criar():
    inspector = db.inspect(db.engine)
    tabelas_existentes = set(inspector.get_table_names())

    modelos = [
        DiariasNotaLiquidacao,
        DiariasProgramacaoDesembolso,
        DiariasOrdemBancaria,
    ]

    for modelo in modelos:
        nome = modelo.__tablename__
        if nome in tabelas_existentes:
            print(f'[--] Tabela "{nome}" ja existe.')
            continue
        modelo.__table__.create(db.engine)
        print(f'[OK] Tabela "{nome}" criada.')


def main():
    app = create_app()
    with app.app_context():
        print('=== Criacao das tabelas NL/PD/OB por servidor ===\n')
        criar()
        print('\n=== Concluido! ===')


if __name__ == '__main__':
    main()
