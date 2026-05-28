"""
Script de migração: Move itinerários da etapa 2 (Escolha do Voo — descontinuada)
para a etapa 6 (Análise 2ª Parte).

Contexto: A cotação de passagens foi desacoplada do fluxo principal de etapas
e agora funciona como fluxo paralelo independente. A etapa 2 não é mais usada.

Uso:
    python scripts/migrar_etapa2_para_etapa6.py              # DRY-RUN (mostra o que faria)
    python scripts/migrar_etapa2_para_etapa6.py --executar    # Aplica as alterações
"""
import argparse
import sys
import os
import io
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db


def main():
    parser = argparse.ArgumentParser(description='Migrar itinerários da etapa 2 para etapa 6')
    parser.add_argument('--executar', action='store_true', help='Aplicar alterações (padrão: dry-run)')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        from app.models.diaria import DiariasItinerario, DiariasHistoricoMovimentacao

        itinerarios = DiariasItinerario.query.filter(
            DiariasItinerario.etapa_atual_id == 2
        ).all()

        if not itinerarios:
            print('Nenhum itinerário encontrado na etapa 2. Nada a migrar.')
            return

        print(f'\n{"=" * 60}')
        print(f'  Itinerários na etapa 2 (Escolha do Voo): {len(itinerarios)}')
        print(f'  Modo: {"EXECUTAR" if args.executar else "DRY-RUN (simulação)"}')
        print(f'{"=" * 60}\n')

        for it in itinerarios:
            protocolo = it.n_processo or it.sei_protocolo or f'ID={it.id}'
            print(f'  [{protocolo}] etapa 2 → 6  (tipo_sol={it.tipo_solicitacao_id})')

            if args.executar:
                it.etapa_atual_id = 6

                historico = DiariasHistoricoMovimentacao(
                    itinerario_id=it.id,
                    etapa_id=6,
                    descricao='Migração automática: etapa "Escolha do Voo" descontinuada. '
                              'Passagens agora são fluxo paralelo independente.',
                    data=datetime.now(),
                )
                db.session.add(historico)

        if args.executar:
            db.session.commit()
            print(f'\n  OK: {len(itinerarios)} itinerário(s) migrado(s) para etapa 6.')
        else:
            print(f'\n  DRY-RUN: {len(itinerarios)} itinerário(s) seriam migrado(s).')
            print('  Use --executar para aplicar.')

        print()


if __name__ == '__main__':
    main()
