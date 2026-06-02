"""
Backfill one-shot dos snapshots de saldo do Fundo Rotativo (SIAFE).

Por que existe: a UI da aba Saldo (`/financeiro/fundo-rotativo/saldo`) tem
apenas o botao "Sincronizar Saldos", que grava o snapshot do mes corrente
via `sincronizar_saldos_mes_atual`. Para popular o historico completo da
UG 210102 / conta contabil 111111901 (necessario apos o deploy inicial),
este script chama `sincronizar_saldos_periodos` para o intervalo desejado.

Idempotente: a UNIQUE `uq_fr_saldo_snapshot` impede duplicacao; reexecutar
sobrescreve o snapshot daquele periodo (delete-then-insert dentro do service).

Uso:
    python scripts/backfill_fundo_rotativo_saldos.py
        # padrao: 01/2025 ate o mes corrente do ano corrente

    python scripts/backfill_fundo_rotativo_saldos.py 2025-01 2026-06
        # intervalo arbitrario (inclusive)

    python scripts/backfill_fundo_rotativo_saldos.py --usuario-id 7
        # registra `sincronizado_por` = usuario 7 (default: primeiro admin)
"""
import argparse
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _parse_periodo(texto):
    try:
        ano_s, mes_s = texto.split('-')
        ano = int(ano_s)
        mes = int(mes_s)
        if not (1 <= mes <= 12) or ano < 2000:
            raise ValueError
    except (ValueError, AttributeError):
        raise argparse.ArgumentTypeError(
            f'Periodo invalido: {texto!r}. Use YYYY-MM (ex.: 2025-01).'
        )
    return (ano, mes)


def _expandir_intervalo(inicio, fim):
    (ai, mi), (af, mf) = inicio, fim
    if (af, mf) < (ai, mi):
        raise SystemExit(f'Periodo final ({af}-{mf:02d}) anterior ao inicial ({ai}-{mi:02d}).')
    periodos = []
    ano, mes = ai, mi
    while (ano, mes) <= (af, mf):
        periodos.append((ano, mes))
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return periodos


def _resolver_usuario_id(arg_id):
    from app.models.usuario import Usuario
    if arg_id is not None:
        usuario = Usuario.query.get(arg_id)
        if not usuario:
            raise SystemExit(f'Usuario id={arg_id} nao encontrado.')
        return usuario.id
    admin = Usuario.query.filter_by(is_admin=True).order_by(Usuario.id.asc()).first()
    if not admin:
        raise SystemExit('Nenhum usuario admin encontrado para registrar sincronizado_por.')
    return admin.id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inicio', nargs='?', type=_parse_periodo,
                        help='Periodo inicial YYYY-MM (default: 2025-01)')
    parser.add_argument('fim', nargs='?', type=_parse_periodo,
                        help='Periodo final YYYY-MM inclusive (default: mes corrente)')
    parser.add_argument('--usuario-id', type=int, default=None,
                        help='Id do usuario para `sincronizado_por` (default: primeiro admin)')
    args = parser.parse_args()

    inicio = args.inicio or (2025, 1)
    if args.fim:
        fim = args.fim
    else:
        agora = datetime.now()
        fim = (agora.year, agora.month)

    periodos = _expandir_intervalo(inicio, fim)

    from app import create_app
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos

    app = create_app()
    with app.app_context():
        usuario_id = _resolver_usuario_id(args.usuario_id)

        print('=' * 70)
        print(f'BACKFILL Fundo Rotativo - Saldos SIAFE')
        print(f'Periodos: {inicio[0]}-{inicio[1]:02d} ate {fim[0]}-{fim[1]:02d} '
              f'({len(periodos)} meses)')
        print(f'Usuario id={usuario_id}')
        print('=' * 70)

        resultado = sincronizar_saldos_periodos(periodos, usuario_id=usuario_id)

        print()
        print('=' * 70)
        print(f'CONCLUIDO: {resultado["registros"]} registros em '
              f'{resultado["periodos"]} periodo(s).')
        print('Acesse /financeiro/fundo-rotativo/saldo para validar.')
        print('=' * 70)


if __name__ == '__main__':
    main()
