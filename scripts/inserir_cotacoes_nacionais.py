"""
Insere cotacoes de voo e dados de escolha nos processos nacionais.
Dados extraidos manualmente dos PDFs de cotacao e HTMLs de escolha.

Uso:
    python scripts/inserir_cotacoes_nacionais.py             # dry-run
    python scripts/inserir_cotacoes_nacionais.py --executar   # gravar
"""
import sys
import os
import argparse
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasItinerario, DiariasCotacaoVoo

# ═══════════════════════════════════════════════════════════════════════
# DADOS EXTRAIDOS DOS PDFs DE COTACAO E HTMLs DE ESCOLHA
# ═══════════════════════════════════════════════════════════════════════

PROCESSOS = {

    # ── 00002.001113/2026-50 (Brasilia) ──
    '00002.001113/2026-50': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3851', 'saida': '2026-02-06 04:35', 'chegada': '2026-02-06 06:50', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '3451.61'},
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1519', 'saida': '2026-02-06 04:40', 'chegada': '2026-02-06 06:55', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '3448.40'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3852', 'saida': '2026-02-06 21:40', 'chegada': '2026-02-06 23:55', 'origem': 'Brasilia', 'destino': 'Teresina', 'valor': '3433.40'},
            {'tipo': 'volta', 'cia': 'GOL', 'voo': 'G3 1518', 'saida': '2026-02-06 22:35', 'chegada': '2026-02-07 00:50', 'origem': 'Brasilia', 'destino': 'Teresina', 'valor': '3430.18'},
        ],
        'escolha': {'ida_idx': 0, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J4', 'declaracao': True},
    },

    # ── 00002.001240/2026-59 (Brasilia) ──
    '00002.001240/2026-59': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1519', 'saida': '2026-02-03 04:40', 'chegada': '2026-02-03 06:55', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '4293.42'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3851', 'saida': '2026-02-03 04:35', 'chegada': '2026-02-03 06:50', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '3949.89'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3850', 'saida': '2026-02-04 09:20', 'chegada': '2026-02-04 11:35', 'origem': 'Brasilia', 'destino': 'Teresina', 'valor': '3928.04'},
        ],
        'escolha': {'ida_idx': 1, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J4', 'declaracao': True},
    },

    # ── 00002.001502/2026-85 (Sao Paulo/Guarulhos) ──
    '00002.001502/2026-85': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3843', 'saida': '2026-03-15 12:05', 'chegada': '2026-03-15 15:10', 'origem': 'Teresina', 'destino': 'Guarulhos', 'valor': '1803.84'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3195', 'saida': '2026-03-15 18:40', 'chegada': '2026-03-15 21:55', 'origem': 'Teresina', 'destino': 'Guarulhos', 'valor': '1256.38'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3842', 'saida': '2026-03-21 08:10', 'chegada': '2026-03-21 11:20', 'origem': 'Guarulhos', 'destino': 'Teresina', 'valor': '1159.12'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3194', 'saida': '2026-03-21 13:55', 'chegada': '2026-03-21 17:10', 'origem': 'Guarulhos', 'destino': 'Teresina', 'valor': '1367.67'},
        ],
        'escolha': None,  # Escolha nao clara no doc
    },

    # ── 00002.001588/2026-46 (Brasilia multi-leg) ──
    '00002.001588/2026-46': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3851', 'saida': '2026-02-24 04:35', 'chegada': '2026-02-24 06:50', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '2937.54'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3001', 'saida': '2026-02-26 10:55', 'chegada': '2026-02-26 12:45', 'origem': 'Brasilia', 'destino': 'Congonhas', 'valor': '1641.99'},
            {'tipo': 'volta', 'cia': 'GOL', 'voo': 'G3 1518', 'saida': '2026-02-27 22:35', 'chegada': '2026-02-28 00:50', 'origem': 'Brasilia', 'destino': 'Teresina', 'valor': '1904.86'},
        ],
        'escolha': None,
    },

    # ── 00002.001618/2026-14 (Macapa - ultimo cotacao) ──
    '00002.001618/2026-14': {
        'cotacoes': [
            # Macapa - Fortaleza (ida)
            {'tipo': 'ida', 'cia': 'AZUL', 'voo': 'AD 4235', 'saida': '2026-03-27 03:50', 'chegada': '2026-03-27 04:45', 'origem': 'Macapa', 'destino': 'Belem',
             'cia_con': 'AZUL', 'voo_con': 'AD 4651', 'saida_con': '2026-03-27 06:50', 'chegada_con': '2026-03-27 08:45', 'origem_con': 'Belem', 'destino_con': 'Fortaleza', 'valor': '1809.01'},
            {'tipo': 'ida', 'cia': 'AZUL', 'voo': 'AD 2705', 'saida': '2026-03-27 14:40', 'chegada': '2026-03-27 15:35', 'origem': 'Macapa', 'destino': 'Belem',
             'cia_con': 'AZUL', 'voo_con': 'AD 4101', 'saida_con': '2026-03-27 18:00', 'chegada_con': '2026-03-27 19:55', 'origem_con': 'Belem', 'destino_con': 'Fortaleza', 'valor': '3731.94'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3861', 'saida': '2026-03-27 13:50', 'chegada': '2026-03-27 17:50', 'origem': 'Macapa', 'destino': 'Guarulhos',
             'cia_con': 'LATAM', 'voo_con': 'LA 3316', 'saida_con': '2026-03-27 18:40', 'chegada_con': '2026-03-27 21:55', 'origem_con': 'Guarulhos', 'destino_con': 'Fortaleza', 'valor': '2826.88'},
        ],
        'escolha': None,  # Processo com 2 cotacoes, escolha referente a outra cotacao
    },

    # ── 00002.001901/2026-46 (Brasilia) ──
    '00002.001901/2026-46': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3851', 'saida': '2026-02-24 04:35', 'chegada': '2026-02-24 06:50', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '4178.79'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3852', 'saida': '2026-02-25 21:40', 'chegada': '2026-02-25 23:55', 'origem': 'Brasilia', 'destino': 'Teresina', 'valor': '3925.53'},
        ],
        'escolha': {'ida_idx': 0, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J4', 'declaracao': True},
    },

    # ── 00002.001950/2026-89 (Fortaleza) ──
    '00002.001950/2026-89': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3492', 'saida': '2026-05-18 12:15', 'chegada': '2026-05-18 13:25', 'origem': 'Teresina', 'destino': 'Fortaleza', 'valor': '1626.44'},
            {'tipo': 'ida', 'cia': 'AZUL', 'voo': 'AD 2789', 'saida': '2026-05-18 22:40', 'chegada': '2026-05-19 00:25', 'origem': 'Teresina', 'destino': 'Recife',
             'cia_con': 'AZUL', 'voo_con': 'AD 2642', 'saida_con': '2026-05-19 03:15', 'chegada_con': '2026-05-19 04:35', 'origem_con': 'Recife', 'destino_con': 'Fortaleza', 'valor': '1924.46'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3493', 'saida': '2026-05-23 14:20', 'chegada': '2026-05-23 15:25', 'origem': 'Fortaleza', 'destino': 'Teresina', 'valor': '1635.88'},
            {'tipo': 'volta', 'cia': 'AZUL', 'voo': 'AD 2435', 'saida': '2026-05-23 17:25', 'chegada': '2026-05-23 20:55', 'origem': 'Fortaleza', 'destino': 'Campinas',
             'cia_con': 'AZUL', 'voo_con': 'AD 4378', 'saida_con': '2026-05-23 23:50', 'chegada_con': '2026-05-24 02:45', 'origem_con': 'Campinas', 'destino_con': 'Teresina', 'valor': '2066.24'},
        ],
        'escolha': {'ida_idx': 0, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J4', 'declaracao': True},
    },

    # ── 00002.002118/2026-08 (Brasilia - 2a cotacao) ──
    '00002.002118/2026-08': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1519', 'saida': '2026-03-11 04:40', 'chegada': '2026-03-11 06:50', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '3086.42'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3717', 'saida': '2026-03-13 05:05', 'chegada': '2026-03-13 07:40', 'origem': 'Campo Grande', 'destino': 'Brasilia',
             'cia_con': 'LATAM', 'voo_con': 'LA 3850', 'saida_con': '2026-03-13 09:20', 'chegada_con': '2026-03-13 11:35', 'origem_con': 'Brasilia', 'destino_con': 'Teresina', 'valor': '4881.41'},
        ],
        'escolha': None,
    },

    # ── 00002.002489/2026-81 (Macapa) ──
    '00002.002489/2026-81': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3853', 'saida': '2026-03-04 16:10', 'chegada': '2026-03-04 18:30', 'origem': 'Teresina', 'destino': 'Brasilia',
             'cia_con': 'LATAM', 'voo_con': 'LA 3768', 'saida_con': '2026-03-04 23:45', 'chegada_con': '2026-03-05 02:25', 'origem_con': 'Brasilia', 'destino_con': 'Macapa', 'valor': '5001.87'},
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1519', 'saida': '2026-03-04 04:40', 'chegada': '2026-03-04 06:55', 'origem': 'Teresina', 'destino': 'Brasilia',
             'cia_con': 'GOL', 'voo_con': 'G3 1778', 'saida_con': '2026-03-04 09:00', 'chegada_con': '2026-03-04 13:45', 'origem_con': 'Brasilia', 'destino_con': 'Macapa', 'valor': '4934.37'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3769', 'saida': '2026-03-06 03:05', 'chegada': '2026-03-06 05:50', 'origem': 'Macapa', 'destino': 'Brasilia',
             'cia_con': 'LATAM', 'voo_con': 'LA 3850', 'saida_con': '2026-03-06 09:20', 'chegada_con': '2026-03-06 11:35', 'origem_con': 'Brasilia', 'destino_con': 'Teresina', 'valor': '6010.35'},
            {'tipo': 'volta', 'cia': 'AZUL', 'voo': 'AD 4235', 'saida': '2026-03-06 03:50', 'chegada': '2026-03-06 04:45', 'origem': 'Macapa', 'destino': 'Belem',
             'cia_con': 'AZUL', 'voo_con': 'AD 4160', 'saida_con': '2026-03-06 12:20', 'chegada_con': '2026-03-06 14:05', 'origem_con': 'Recife', 'destino_con': 'Teresina', 'valor': '4785.89'},
        ],
        'escolha': {'ida_idx': 0, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J4', 'declaracao': True},
    },

    # ── 00002.002539/2026-21 (Foz do Iguacu) ──
    '00002.002539/2026-21': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3843', 'saida': '2026-03-23 12:05', 'chegada': '2026-03-23 15:10', 'origem': 'Teresina', 'destino': 'Sao Paulo',
             'cia_con': 'LATAM', 'voo_con': 'LA 3206', 'saida_con': '2026-03-23 22:20', 'chegada_con': '2026-03-24 00:05', 'origem_con': 'Sao Paulo', 'destino_con': 'Foz do Iguacu', 'valor': '2258.42'},
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1952', 'saida': '2026-03-23 03:50', 'chegada': '2026-03-23 06:55', 'origem': 'Teresina', 'destino': 'Sao Paulo',
             'cia_con': 'GOL', 'voo_con': 'G3 1170', 'saida_con': '2026-03-23 08:50', 'chegada_con': '2026-03-23 10:40', 'origem_con': 'Sao Paulo', 'destino_con': 'Foz do Iguacu', 'valor': '2410.17'},
            {'tipo': 'ida', 'cia': 'AZUL', 'voo': 'AD 4379', 'saida': '2026-03-23 03:30', 'chegada': '2026-03-23 06:40', 'origem': 'Teresina', 'destino': 'Campinas',
             'cia_con': 'AZUL', 'voo_con': 'AD 2955', 'saida_con': '2026-03-23 08:10', 'chegada_con': '2026-03-23 09:45', 'origem_con': 'Campinas', 'destino_con': 'Foz do Iguacu', 'valor': '4155.96'},
            {'tipo': 'volta', 'cia': 'AZUL', 'voo': 'AD 4865', 'saida': '2026-03-26 19:50', 'chegada': '2026-03-26 21:20', 'origem': 'Foz do Iguacu', 'destino': 'Campinas',
             'cia_con': 'AZUL', 'voo_con': 'AD 4378', 'saida_con': '2026-03-26 23:50', 'chegada_con': '2026-03-27 02:45', 'origem_con': 'Campinas', 'destino_con': 'Teresina', 'valor': '4195.11'},
        ],
        'escolha': {'ida_idx': 0, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J5', 'declaracao': True},
    },

    # ── 00002.001488/2026-10 (Brasilia multi-leg) ──
    '00002.001488/2026-10': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1519', 'saida': '2026-02-10 04:40', 'chegada': '2026-02-10 06:50', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '4422.51'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3851', 'saida': '2026-02-10 04:35', 'chegada': '2026-02-10 06:50', 'origem': 'Teresina', 'destino': 'Brasilia', 'valor': '4447.86'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3493', 'saida': '2026-02-12 14:15', 'chegada': '2026-02-12 15:25', 'origem': 'Fortaleza', 'destino': 'Teresina', 'valor': '1601.11'},
        ],
        'escolha': {'ida_idx': 0, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J4', 'declaracao': True},
    },

    # ── 00002.003101/2026-60 (Sao Paulo - tabela) ──
    '00002.003101/2026-60': {
        'cotacoes': [
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3195', 'saida': '2026-04-08 18:35', 'chegada': '2026-04-08 21:45', 'origem': 'Teresina', 'destino': 'Guarulhos', 'valor': '2701.75'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3853', 'saida': '2026-04-08 16:15', 'chegada': '2026-04-08 18:25', 'origem': 'Teresina', 'destino': 'Brasilia',
             'cia_con': 'LATAM', 'voo_con': 'LA 3021', 'saida_con': '2026-04-08 19:20', 'chegada_con': '2026-04-08 21:05', 'origem_con': 'Brasilia', 'destino_con': 'Congonhas', 'valor': '2714.17'},
            {'tipo': 'ida', 'cia': 'AZUL', 'voo': 'AD 4161', 'saida': '2026-04-08 15:05', 'chegada': '2026-04-08 16:45', 'origem': 'Teresina', 'destino': 'Recife',
             'cia_con': 'AZUL', 'voo_con': 'AD 4820', 'saida_con': '2026-04-08 18:25', 'chegada_con': '2026-04-08 21:45', 'origem_con': 'Recife', 'destino_con': 'Guarulhos', 'valor': '2853.16'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3842', 'saida': '2026-04-11 07:35', 'chegada': '2026-04-11 10:45', 'origem': 'Guarulhos', 'destino': 'Teresina', 'valor': '2110.17'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 4739', 'saida': '2026-04-11 09:20', 'chegada': '2026-04-11 12:50', 'origem': 'Congonhas', 'destino': 'Fortaleza',
             'cia_con': 'LATAM', 'voo_con': 'LA 3493', 'saida_con': '2026-04-11 14:20', 'chegada_con': '2026-04-11 15:25', 'origem_con': 'Fortaleza', 'destino_con': 'Teresina', 'valor': '3652.58'},
            {'tipo': 'volta', 'cia': 'AZUL', 'voo': 'AD 4004', 'saida': '2026-04-11 07:45', 'chegada': '2026-04-11 10:45', 'origem': 'Congonhas', 'destino': 'Recife',
             'cia_con': 'AZUL', 'voo_con': 'AD 4160', 'saida_con': '2026-04-11 12:40', 'chegada_con': '2026-04-11 14:25', 'origem_con': 'Recife', 'destino_con': 'Teresina', 'valor': '3791.81'},
        ],
        'escolha': None,  # Processo ainda em etapa 2
    },

    # ── 00002.009305/2025-23 (Sao Paulo - tabela complexa) ──
    '00002.009305/2025-23': {
        'cotacoes': [
            # Congonhas ida
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3853', 'saida': '2025-09-10 15:55', 'chegada': '2025-09-10 18:05', 'origem': 'Teresina', 'destino': 'Brasilia',
             'cia_con': 'LATAM', 'voo_con': 'LA 4737', 'saida_con': '2025-09-10 19:50', 'chegada_con': '2025-09-10 21:40', 'origem_con': 'Brasilia', 'destino_con': 'Congonhas', 'valor': '4635.35'},
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1519', 'saida': '2025-09-11 04:20', 'chegada': '2025-09-11 06:25', 'origem': 'Teresina', 'destino': 'Brasilia',
             'cia_con': 'GOL', 'voo_con': 'G3 1427', 'saida_con': '2025-09-11 07:00', 'chegada_con': '2025-09-11 08:50', 'origem_con': 'Brasilia', 'destino_con': 'Congonhas', 'valor': '3117.21'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3851', 'saida': '2025-09-11 05:10', 'chegada': '2025-09-11 07:25', 'origem': 'Teresina', 'destino': 'Brasilia',
             'cia_con': 'LATAM', 'voo_con': 'LA 3001', 'saida_con': '2025-09-11 10:40', 'chegada_con': '2025-09-11 12:30', 'origem_con': 'Brasilia', 'destino_con': 'Congonhas', 'valor': '3140.19'},
            # Congonhas volta
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3467', 'saida': '2025-09-14 06:10', 'chegada': '2025-09-14 09:40', 'origem': 'Congonhas', 'destino': 'Fortaleza',
             'cia_con': 'LATAM', 'voo_con': 'LA 3493', 'saida_con': '2025-09-14 14:20', 'chegada_con': '2025-09-14 15:25', 'origem_con': 'Fortaleza', 'destino_con': 'Teresina', 'valor': '3782.49'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3184', 'saida': '2025-09-14 09:55', 'chegada': '2025-09-14 13:25', 'origem': 'Congonhas', 'destino': 'Fortaleza',
             'cia_con': 'LATAM', 'voo_con': 'LA 3493', 'saida_con': '2025-09-14 14:20', 'chegada_con': '2025-09-14 15:25', 'origem_con': 'Fortaleza', 'destino_con': 'Teresina', 'valor': '3782.49'},
            {'tipo': 'volta', 'cia': 'AZUL', 'voo': 'AD 5020', 'saida': '2025-09-14 10:25', 'chegada': '2025-09-14 11:40', 'origem': 'Congonhas', 'destino': 'Belo Horizonte',
             'cia_con': 'AZUL', 'voo_con': 'AD 4319', 'saida_con': '2025-09-14 14:50', 'chegada_con': '2025-09-14 17:25', 'origem_con': 'Belo Horizonte', 'destino_con': 'Teresina', 'valor': '3906.52'},
            # Guarulhos ida
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3195', 'saida': '2025-09-10 16:30', 'chegada': '2025-09-10 19:40', 'origem': 'Teresina', 'destino': 'Guarulhos', 'valor': '4371.50'},
            {'tipo': 'ida', 'cia': 'GOL', 'voo': 'G3 1952', 'saida': '2025-09-11 04:55', 'chegada': '2025-09-11 08:10', 'origem': 'Teresina', 'destino': 'Guarulhos', 'valor': '4009.10'},
            {'tipo': 'ida', 'cia': 'LATAM', 'voo': 'LA 3197', 'saida': '2025-09-11 02:35', 'chegada': '2025-09-11 05:45', 'origem': 'Teresina', 'destino': 'Guarulhos', 'valor': '4009.95'},
            # Guarulhos volta
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3194', 'saida': '2025-09-14 12:40', 'chegada': '2025-09-14 15:50', 'origem': 'Guarulhos', 'destino': 'Teresina', 'valor': '3855.51'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3196', 'saida': '2025-09-14 22:35', 'chegada': '2025-09-15 01:45', 'origem': 'Guarulhos', 'destino': 'Teresina', 'valor': '3855.51'},
            {'tipo': 'volta', 'cia': 'GOL', 'voo': 'G3 1949', 'saida': '2025-09-14 21:40', 'chegada': '2025-09-15 00:40', 'origem': 'Guarulhos', 'destino': 'Teresina', 'valor': '4200.74'},
            {'tipo': 'volta', 'cia': 'GOL', 'voo': 'G3 1456', 'saida': '2025-09-14 17:55', 'chegada': '2025-09-14 19:50', 'origem': 'Congonhas', 'destino': 'Brasilia',
             'cia_con': 'GOL', 'voo_con': 'G3 1518', 'saida_con': '2025-09-14 22:30', 'chegada_con': '2025-09-15 00:45', 'origem_con': 'Brasilia', 'destino_con': 'Teresina', 'valor': '3082.19'},
            {'tipo': 'volta', 'cia': 'LATAM', 'voo': 'LA 3606', 'saida': '2025-09-14 18:00', 'chegada': '2025-09-14 19:45', 'origem': 'Congonhas', 'destino': 'Brasilia',
             'cia_con': 'LATAM', 'voo_con': 'LA 3852', 'saida_con': '2025-09-14 21:25', 'chegada_con': '2025-09-14 23:35', 'origem_con': 'Brasilia', 'destino_con': 'Teresina', 'valor': '4605.67'},
        ],
        'escolha': {'ida_idx': 0, 'volta_idx': 0, 'menor_valor': False, 'justificativa': 'J4', 'declaracao': True},
    },
}


# ═══════════════════════════════════════════════════════════════════════
# INSERÇÃO
# ═══════════════════════════════════════════════════════════════════════

def inserir_cotacoes_processo(protocolo, dados, executar=False):
    """Insere cotacoes e escolha para um processo."""
    itinerario = DiariasItinerario.query.filter_by(sei_protocolo=protocolo).first()
    if not itinerario:
        print(f'  ERRO: Itinerario nao encontrado para {protocolo}')
        return False

    # Verificar se ja tem cotacoes
    existentes = itinerario.cotacoes_voos.count()
    if existentes > 0:
        print(f'  SKIP: Ja tem {existentes} cotacoes')
        return True

    cotacoes_data = dados['cotacoes']
    escolha_data = dados.get('escolha')

    cotacoes_inseridas = []

    for i, cot in enumerate(cotacoes_data):
        saida = datetime.strptime(cot['saida'], '%Y-%m-%d %H:%M')
        chegada = datetime.strptime(cot['chegada'], '%Y-%m-%d %H:%M')

        novo = DiariasCotacaoVoo(
            itinerario_id=itinerario.id,
            tipo_trecho=cot['tipo'],
            cia=cot['cia'],
            voo=cot['voo'],
            saida=saida,
            chegada=chegada,
            origem=cot['origem'],
            destino=cot['destino'],
            valor=Decimal(cot['valor']),
            fonte='manual',
        )

        # Conexao
        if cot.get('cia_con'):
            novo.cia_conexao = cot['cia_con']
            novo.voo_conexao = cot['voo_con']
            novo.saida_conexao = datetime.strptime(cot['saida_con'], '%Y-%m-%d %H:%M')
            novo.chegada_conexao = datetime.strptime(cot['chegada_con'], '%Y-%m-%d %H:%M')
            novo.origem_conexao = cot['origem_con']
            novo.destino_conexao = cot['destino_con']

        if executar:
            db.session.add(novo)
            db.session.flush()

        cotacoes_inseridas.append(novo)
        tipo = cot['tipo']
        con = ' (conexao)' if cot.get('cia_con') else ''
        print(f'    {tipo}: {cot["cia"]} {cot["voo"]} R$ {cot["valor"]}{con}')

    # Escolha
    if escolha_data and len(cotacoes_inseridas) > 0:
        idas = [c for c in cotacoes_inseridas if c.tipo_trecho == 'ida']
        voltas = [c for c in cotacoes_inseridas if c.tipo_trecho == 'volta']

        ida_idx = escolha_data['ida_idx']
        volta_idx = escolha_data['volta_idx']

        if ida_idx < len(idas) and volta_idx < len(voltas):
            if executar:
                itinerario.escolha_voo_ida_id = idas[ida_idx].id
                itinerario.escolha_voo_volta_id = voltas[volta_idx].id
                itinerario.escolha_menor_valor = escolha_data.get('menor_valor', False)
                itinerario.escolha_justificativa_codigos = escolha_data.get('justificativa', '')
                itinerario.escolha_declaracao_responsabilidade = escolha_data.get('declaracao', True)
                itinerario.escolha_via_sei = True

            print(f'    ESCOLHA: ida=opcao {ida_idx+1}, volta=opcao {volta_idx+1}, menor={escolha_data["menor_valor"]}')
        else:
            print(f'    AVISO: indice de escolha invalido (idas={len(idas)}, voltas={len(voltas)})')
    elif not escolha_data:
        print(f'    SEM ESCOLHA (sera feita manualmente)')

    if executar:
        db.session.commit()

    return True


def main():
    parser = argparse.ArgumentParser(description='Inserir cotacoes dos processos nacionais')
    parser.add_argument('--executar', action='store_true')
    parser.add_argument('--processo', help='Processar apenas este protocolo')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        print(f"Modo: {'EXECUTAR' if args.executar else 'DRY-RUN'}")
        print(f"Processos: {len(PROCESSOS)}\n")

        ok = 0
        for protocolo, dados in PROCESSOS.items():
            if args.processo and args.processo != protocolo:
                continue
            print(f'\n{protocolo}:')
            if inserir_cotacoes_processo(protocolo, dados, args.executar):
                ok += 1

        print(f'\n{"="*60}')
        print(f'Resultado: {ok}/{len(PROCESSOS)} processos')


if __name__ == '__main__':
    main()
