"""
tests/prestacoes_contratos/test_contratos_exercicio.py

TDD da escolha de exercicio SIAFE em `scripts/atualizar_contratos.py`.

Contexto: o SIAFE mantem um registro do contrato POR EXERCICIO. O contrato
25018627 foi atualizado no exercicio 2025 (LICITADO, DATEN) enquanto o registro
de 2026 continuou "A Contratar" e vazio. Como o script so consultava o ano
corrente, o hash nunca mudava e o banco ficou parado desde 01/06/2026.

Politica:
  1) consulta o ano corrente e, se o contrato ja existia antes, o ano anterior
  2) fica com o registro mais completo (campos preenchidos + fiscais + aditivos)
  3) empate -> ano corrente
"""
from scripts.contratos_exercicio import (
    escolher_registro,
    exercicios_a_consultar,
    pontuar_completude,
)

COLUNAS = ["codigo", "situacao", "nomeContratado", "objeto", "dataFimVigencia"]

REG_2026_VAZIO = {
    "codigo": "25018627",
    "situacao": "A_CONTRATAR",
    "nomeContratado": " - ",
    "objeto": "Aquisição de Infraestrutura de Tecnologia da Informação - TI",
    "dataFimVigencia": None,
    "responsaveisContrato": None,
    "aditivos": None,
}

REG_2025_COMPLETO = {
    "codigo": "25018627",
    "situacao": "LICITADO",
    "nomeContratado": "04602789000101 - DATEN TECNOLOGIA LTDA",
    "objeto": "Aquisição de Microcomputadores Desktop",
    "dataFimVigencia": None,
    "responsaveisContrato": None,
    "aditivos": None,
}


# ── exercicios_a_consultar ───────────────────────────────────────────────────


def test_contrato_de_ano_anterior_consulta_ano_corrente_e_anterior():
    assert exercicios_a_consultar("25018627", 2026) == [2026, 2025]


def test_contrato_antigo_consulta_so_ano_corrente_e_anterior():
    assert exercicios_a_consultar("23005147", 2026) == [2026, 2025]


def test_contrato_criado_no_ano_corrente_consulta_so_ano_corrente():
    assert exercicios_a_consultar("26000123", 2026) == [2026]


def test_codigo_sem_prefixo_de_ano_consulta_so_ano_corrente():
    assert exercicios_a_consultar("abc", 2026) == [2026]
    assert exercicios_a_consultar("", 2026) == [2026]
    assert exercicios_a_consultar(None, 2026) == [2026]


# ── pontuar_completude ───────────────────────────────────────────────────────


def test_placeholder_de_contratado_vazio_nao_conta():
    assert pontuar_completude(REG_2026_VAZIO, COLUNAS) == 3  # codigo, situacao, objeto
    assert pontuar_completude(REG_2025_COMPLETO, COLUNAS) == 4


def test_fiscais_e_aditivos_somam_na_completude():
    reg = dict(REG_2026_VAZIO, responsaveisContrato=[{"cpf": "1"}, {"cpf": "2"}], aditivos=[{"codAditivo": "1"}])
    assert pontuar_completude(reg, COLUNAS) == 3 + 2 + 1


# ── escolher_registro ────────────────────────────────────────────────────────


def test_escolhe_exercicio_anterior_quando_mais_completo():
    ano, data = escolher_registro({2026: REG_2026_VAZIO, 2025: REG_2025_COMPLETO}, COLUNAS, 2026)
    assert ano == 2025
    assert data["situacao"] == "LICITADO"


def test_empate_fica_com_ano_corrente():
    reg_2025 = dict(REG_2025_COMPLETO, situacao="VIGENTE")
    reg_2026 = dict(REG_2025_COMPLETO, situacao="ADITIVADO")
    ano, data = escolher_registro({2025: reg_2025, 2026: reg_2026}, COLUNAS, 2026)
    assert ano == 2026
    assert data["situacao"] == "ADITIVADO"


def test_aditivo_no_ano_corrente_ganha_do_anterior():
    reg_2026 = dict(REG_2025_COMPLETO, aditivos=[{"codAditivo": "1"}])
    ano, _ = escolher_registro({2026: reg_2026, 2025: REG_2025_COMPLETO}, COLUNAS, 2026)
    assert ano == 2026


def test_so_ano_corrente_disponivel():
    ano, data = escolher_registro({2026: REG_2026_VAZIO}, COLUNAS, 2026)
    assert ano == 2026
    assert data is REG_2026_VAZIO


def test_sem_ano_corrente_nao_escolhe_nada():
    """Se o ano corrente falhou (erro transitorio), nao grava dado do ano anterior."""
    assert escolher_registro({2025: REG_2025_COMPLETO}, COLUNAS, 2026) == (None, None)
    assert escolher_registro({}, COLUNAS, 2026) == (None, None)
