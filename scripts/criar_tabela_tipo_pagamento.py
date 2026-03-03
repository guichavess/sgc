"""
Script para criar a tabela tipo_pagamento e adicionar a coluna em sis_solicitacoes.

Uso:
    python scripts/criar_tabela_tipo_pagamento.py
"""
import sys
import os

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.tipo_pagamento import TipoPagamento


def criar_tabela():
    """Cria a tabela tipo_pagamento se não existir."""
    inspector = db.inspect(db.engine)
    tabelas_existentes = inspector.get_table_names()

    if 'tipo_pagamento' not in tabelas_existentes:
        TipoPagamento.__table__.create(db.engine)
        print('[OK] Tabela "tipo_pagamento" criada.')
    else:
        print('[--] Tabela "tipo_pagamento" já existe.')


def seed_tipos():
    """Insere os 3 tipos de pagamento padrão."""
    tipos = [
        (1, 'DEA'),
        (2, 'Indenizatório'),
        (3, 'Regular'),
    ]

    for tipo_id, nome in tipos:
        existente = TipoPagamento.query.filter_by(id=tipo_id).first()
        if existente:
            print(f'[--] Tipo "{nome}" (id={tipo_id}) já existe.')
        else:
            tipo = TipoPagamento(id=tipo_id, nome=nome)
            db.session.add(tipo)
            print(f'[OK] Tipo "{nome}" (id={tipo_id}) inserido.')

    db.session.commit()


def adicionar_coluna_sis_solicitacoes():
    """Adiciona coluna id_tipo_pagamento em sis_solicitacoes se não existir."""
    inspector = db.inspect(db.engine)
    colunas = [col['name'] for col in inspector.get_columns('sis_solicitacoes')]

    if 'id_tipo_pagamento' not in colunas:
        with db.engine.begin() as conn:
            conn.execute(db.text(
                'ALTER TABLE sis_solicitacoes ADD COLUMN id_tipo_pagamento INT NULL, '
                'ADD CONSTRAINT fk_solicitacao_tipo_pagamento '
                'FOREIGN KEY (id_tipo_pagamento) REFERENCES tipo_pagamento(id)'
            ))
        print('[OK] Coluna "id_tipo_pagamento" adicionada em sis_solicitacoes.')
    else:
        print('[--] Coluna "id_tipo_pagamento" já existe em sis_solicitacoes.')


def main():
    app = create_app()
    with app.app_context():
        print('=== Criando tabela tipo_pagamento ===')
        criar_tabela()

        print('\n=== Inserindo tipos padrão ===')
        seed_tipos()

        print('\n=== Adicionando coluna em sis_solicitacoes ===')
        adicionar_coluna_sis_solicitacoes()

        print('\n=== Concluído! ===')


if __name__ == '__main__':
    main()
