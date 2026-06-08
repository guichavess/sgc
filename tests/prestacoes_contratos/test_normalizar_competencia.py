"""
tests/prestacoes_contratos/test_normalizar_competencia.py

TDD para normalizar_competencia() — converte texto descritivo da API SIAFE
(formato observado em OB) para o formato canônico MM/YYYY usado em PD,
liquidação e empenho.

Casos reais observados na tabela `ob` apos primeira execucao do script:
  '12 - Dezembro/2024'           → '12/2024'
  '1 - Janeiro/2025'             → '01/2025'  (zero-padding)
  '13 - Encerramento - 13/2024'  → '13/2024'  (pega o ultimo MM/YYYY)
"""
import pytest

from app.utils.competencia import normalizar_competencia


def test_formato_ja_canonico_passa_direto():
    assert normalizar_competencia('12/2024') == '12/2024'
    assert normalizar_competencia('01/2025') == '01/2025'


def test_descritivo_dezembro():
    assert normalizar_competencia('12 - Dezembro/2024') == '12/2024'


def test_descritivo_mes_um_digito_recebe_zero_padding():
    assert normalizar_competencia('1 - Janeiro/2025') == '01/2025'


def test_descritivo_encerramento_pega_ultimo_MM_YYYY():
    assert normalizar_competencia('13 - Encerramento - 13/2024') == '13/2024'


def test_string_vazia_retorna_none():
    assert normalizar_competencia('') is None


def test_none_retorna_none():
    assert normalizar_competencia(None) is None


def test_nan_string_retorna_none():
    """pandas NaN convertido para string vira 'nan' — não deve passar."""
    assert normalizar_competencia('nan') is None
    assert normalizar_competencia('None') is None


def test_string_sem_padrao_MM_YYYY_retorna_none():
    assert normalizar_competencia('texto sem nada') is None


def test_trailing_whitespace_eh_tolerado():
    assert normalizar_competencia('12/2024  ') == '12/2024'
    assert normalizar_competencia('12 - Dezembro/2024 ') == '12/2024'
