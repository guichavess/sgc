"""
Script para adicionar coluna 'criado_em_lote' na tabela sis_solicitacoes.

Uso:
    python scripts/add_coluna_lote.py

O que faz:
    1. Adiciona a coluna criado_em_lote (TINYINT/Boolean, default=0) se nao existir
    2. Retroativamente marca como criado_em_lote=1 os registros cujo historico
       contem o comentario "Processo criado em lote."
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from sqlalchemy import text


def adicionar_coluna():
    """Adiciona a coluna criado_em_lote se nao existir."""
    inspector = db.inspect(db.engine)
    colunas = [col['name'] for col in inspector.get_columns('sis_solicitacoes')]

    if 'criado_em_lote' in colunas:
        print('[--] Coluna "criado_em_lote" ja existe.')
        return False

    db.session.execute(text(
        "ALTER TABLE sis_solicitacoes ADD COLUMN criado_em_lote TINYINT(1) NOT NULL DEFAULT 0"
    ))
    db.session.commit()
    print('[OK] Coluna "criado_em_lote" adicionada com sucesso.')
    return True


def atualizar_retroativo():
    """Marca retroativamente solicitacoes criadas em lote baseado no historico."""
    resultado = db.session.execute(text("""
        UPDATE sis_solicitacoes s
        INNER JOIN sis_historico_movimentacoes h ON h.id_solicitacao = s.id
        SET s.criado_em_lote = 1
        WHERE h.comentario LIKE '%criado em lote%'
          AND s.criado_em_lote = 0
    """))
    db.session.commit()

    total = resultado.rowcount
    if total > 0:
        print(f'[OK] {total} solicitacao(oes) marcada(s) como criado_em_lote=1.')
    else:
        print('[--] Nenhuma solicitacao retroativa encontrada para marcar.')


def main():
    app = create_app()
    with app.app_context():
        print('=' * 50)
        print('  Migracao: criado_em_lote')
        print('=' * 50)

        adicionar_coluna()
        atualizar_retroativo()

        # Resumo
        total_lote = db.session.execute(text(
            "SELECT COUNT(*) FROM sis_solicitacoes WHERE criado_em_lote = 1"
        )).scalar()
        total_individual = db.session.execute(text(
            "SELECT COUNT(*) FROM sis_solicitacoes WHERE criado_em_lote = 0"
        )).scalar()

        print()
        print(f'  Resumo:')
        print(f'    Em lote:     {total_lote}')
        print(f'    Individual:  {total_individual}')
        print(f'    Total:       {total_lote + total_individual}')
        print('=' * 50)


if __name__ == '__main__':
    main()
