"""
Backfill one-shot de Empenho e Reserva para anos anteriores.

Por que existe: `atualizar_empenho.py` e `atualizar_reserva.py` só baixam o ano
corrente e o anterior (YEARS = [year-1, year]). Contratos antigos (ex.: 2023)
cujas NEs/NRs foram emitidas fora dessa janela ficam sem registro, mesmo quando
suas Liquidações de 2025/2026 (que SÃO importadas) referenciam essas notas
antigas. Isso aparece no dashboard do Fundo Rotativo como "Liquidação > 0,
Reserva = 0, Empenho = 0" — fisicamente impossível.

Este script reaproveita a lógica de fetch+upsert dos scripts oficiais,
sobrescrevendo `YEARS` apenas em memória, e baixa os anos faltantes para as
mesmas UGs já configuradas (UG_LIST = ["210101", "210102"]).

Uso (execução única, com .env populado):
    python scripts/backfill_empenho_reserva.py            # padrão: 2023 e 2024
    python scripts/backfill_empenho_reserva.py 2022 2023  # anos arbitrários

Após rodar, o dashboard `/financeiro/fundo-rotativo/dashboard` deve passar a
exibir Reserva e Empenho para contratos antigos com Liquidação registrada.
"""
import sys
from datetime import datetime


def _parse_anos(argv):
    if len(argv) > 1:
        try:
            anos = sorted({int(a) for a in argv[1:]})
        except ValueError:
            print('Anos devem ser inteiros. Ex.: python backfill_empenho_reserva.py 2023 2024')
            sys.exit(1)
        return anos
    ano_atual = datetime.now().year
    return [ano_atual - 3, ano_atual - 2]


def main():
    anos = _parse_anos(sys.argv)
    print('=' * 70)
    print(f'BACKFILL Empenho + Reserva — anos: {anos}')
    print('=' * 70)

    import atualizar_empenho
    atualizar_empenho.YEARS = anos
    print('\n>>> ETAPA 1/2: EMPENHO')
    atualizar_empenho.main()

    import atualizar_reserva
    atualizar_reserva.YEARS = anos
    print('\n>>> ETAPA 2/2: RESERVA')
    atualizar_reserva.main_reserva()

    print('\n' + '=' * 70)
    print('BACKFILL CONCLUÍDO. Verifique o dashboard /financeiro/fundo-rotativo')
    print('=' * 70)


if __name__ == '__main__':
    main()
