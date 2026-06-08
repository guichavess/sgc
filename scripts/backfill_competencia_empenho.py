"""
Backfill de empenho.competencia por encadeamento reverso SIAFE.

Uso:
    python scripts/backfill_competencia_empenho.py
    python scripts/backfill_competencia_empenho.py --executar
    python scripts/backfill_competencia_empenho.py --limit 50

O modo padrao e DRY-RUN: calcula e relata o que seria feito, sem gravar no
banco. Use --executar apenas quando quiser aplicar os UPDATEs.
"""
import argparse
import os
import sys
from collections import Counter

from dotenv import load_dotenv
from sqlalchemy import create_engine, or_
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.empenho import Empenho  # noqa: E402
from app.services.backfill_competencia_service import (  # noqa: E402
    mapear_competencias_empenho,
)


DEFAULT_BATCH_SIZE = 500


def _chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _conectar():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(base_dir, '.env'))

    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS', '')
    host = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')

    if not all([user, host, database]):
        print('ERRO: variaveis DB_USER, DB_HOST e DB_NAME ausentes no .env')
        sys.exit(1)

    url = URL.create(
        'mysql+pymysql',
        username=user,
        password=password,
        host=host,
        database=database,
    )
    engine = create_engine(url, echo=False)
    return sessionmaker(bind=engine)()


def _ne_pendentes(session, limit=None):
    query = (
        session.query(Empenho.codigo)
        .filter(
            Empenho.codigo.isnot(None),
            or_(Empenho.competencia.is_(None), Empenho.competencia == ''),
        )
        .order_by(Empenho.codigo.asc())
    )
    if limit:
        query = query.limit(limit)
    return [codigo for (codigo,) in query.all()]


def _aplicar_updates(session, mapeamento):
    total = 0
    for codigo_ne, info in mapeamento.items():
        atualizados = (
            session.query(Empenho)
            .filter(
                Empenho.codigo == codigo_ne,
                or_(Empenho.competencia.is_(None), Empenho.competencia == ''),
            )
            .update(
                {Empenho.competencia: info['competencia']},
                synchronize_session=False,
            )
        )
        total += atualizados or 0
    return total


def _relatar_amostra(mapeamento, limite=5):
    amostra = list(mapeamento.items())[:limite]
    if not amostra:
        return

    print('Amostra:')
    for codigo, info in amostra:
        print(f"  {codigo} -> {info['competencia']} (via {info['origem']})")
    print()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--executar',
        action='store_true',
        help='Aplica UPDATEs no banco. Sem esta flag, roda em dry-run.',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Processa apenas N empenhos pendentes.',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f'Tamanho do lote de NEs. Padrao: {DEFAULT_BATCH_SIZE}.',
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error('--batch-size deve ser maior que zero.')

    modo = 'EXECUTAR' if args.executar else 'DRY-RUN'
    print(f'\n=== Backfill competencia Empenho - modo {modo} ===\n')

    session = _conectar()
    try:
        pendentes = _ne_pendentes(session, limit=args.limit)
        print(f'NEs sem competencia: {len(pendentes)}')
        if not pendentes:
            print('Nada a fazer.')
            return

        total_com_match = 0
        total_atualizado = 0
        origens = Counter()
        primeira_amostra = {}

        for idx, lote in enumerate(_chunks(pendentes, args.batch_size), start=1):
            mapeamento = mapear_competencias_empenho(session, lote)
            total_com_match += len(mapeamento)
            origens.update(info['origem'] for info in mapeamento.values())

            if not primeira_amostra and mapeamento:
                primeira_amostra = dict(list(mapeamento.items())[:5])

            if args.executar and mapeamento:
                total_atualizado += _aplicar_updates(session, mapeamento)
                session.commit()
                print(
                    f'Lote {idx}: {len(lote)} NEs analisadas, '
                    f'{len(mapeamento)} matches, {total_atualizado} updates aplicados.'
                )
            else:
                print(
                    f'Lote {idx}: {len(lote)} NEs analisadas, '
                    f'{len(mapeamento)} matches.'
                )

        print()
        print(f'NEs com match em fase posterior: {total_com_match}')
        print(f'NEs sem match: {len(pendentes) - total_com_match}')
        print()
        print('Resolucao por origem:')
        for origem in ('NL', 'PD', 'OB'):
            print(f'  {origem}: {origens.get(origem, 0)}')
        print()

        _relatar_amostra(primeira_amostra)

        if args.executar:
            print(f'{total_atualizado} empenhos atualizados com sucesso.')
        else:
            print('DRY-RUN: nenhuma alteracao gravada. Use --executar para aplicar.')
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()
