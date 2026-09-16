"""
tests/prestacoes_contratos/test_contratos_exercicio.py

TDD da escolha de exercicio SIAFE em `scripts/atualizar_contratos.py`.

Contexto: o SIAFE mantem um registro do contrato POR EXERCICIO. No contrato
25018627 o registro de 2026 ficou "A Contratar" sem contratado (planejamento,
R$ 2,97 mi, Pregao) enquanto o de 2025 recebeu a contratacao (LICITADO, DATEN,
contrato 31/2026, R$ 1,33 mi). Os dois tinham 17 campos preenchidos, entao
contar campos nao distingue: o que distingue e ter contratado identificado.

Politica:
  1) consulta o ano corrente e, se o contrato ja existia antes, o ano anterior
  2) prefere o registro com contratado identificado (codigoContratado)
  3) ambos ou nenhum com contratado -> ano corrente
  4) sem registro do ano corrente (erro na consulta) -> nao escolhe nada
"""
from scripts.contratos_exercicio import (
    escolher_registro,
    exercicios_a_consultar,
    tem_contratado,
)

# Dados reais do SIAFE (consultados em 2026-09-16), campos relevantes
REG_2026_PLANEJAMENTO = {
    "codigo": "25018627",
    "situacao": "A_CONTRATAR",
    "numeroOriginal": None,
    "tipoContratado": "TIPO_CREDOR_PF",
    "codigoContratado": None,
    "nomeContratado": " - ",
    "valor": 2970048.28,
    "codigoModalidadeLicitacao": "12",
    "nomeModalidadeLicitacao": "Pregão",
    "regimeExecucao": "PRESTACAO_MENSAL",
    "modalidade": "FORNECIMENTO_BENS",
}

REG_2025_CONTRATADO = {
    "codigo": "25018627",
    "situacao": "LICITADO",
    "numeroOriginal": "31/2026",
    "tipoContratado": "TIPO_CREDOR_PJ",
    "codigoContratado": "04602789000101",
    "nomeContratado": "04602789000101 - DATEN TECNOLOGIA LTDA",
    "valor": 1332998.35,
    "codigoModalidadeLicitacao": None,
    "nomeModalidadeLicitacao": None,
    "regimeExecucao": None,
    "modalidade": "OUTROS",
}


# ── exercicios_a_consultar ───────────────────────────────────────────────────


def test_contrato_de_ano_anterior_consulta_ano_corrente_e_anterior():
    assert exercicios_a_consultar("25018627", 2026) == [2026, 2025]


def test_contrato_antigo_consulta_ano_corrente_e_anterior():
    assert exercicios_a_consultar("23005147", 2026) == [2026, 2025]


def test_contrato_criado_no_ano_corrente_consulta_so_ano_corrente():
    assert exercicios_a_consultar("26000123", 2026) == [2026]


def test_codigo_sem_prefixo_de_ano_consulta_so_ano_corrente():
    assert exercicios_a_consultar("abc", 2026) == [2026]
    assert exercicios_a_consultar("", 2026) == [2026]
    assert exercicios_a_consultar(None, 2026) == [2026]


# ── tem_contratado ───────────────────────────────────────────────────────────


def test_tem_contratado():
    assert tem_contratado(REG_2025_CONTRATADO) is True
    assert tem_contratado(REG_2026_PLANEJAMENTO) is False


def test_codigo_contratado_vazio_ou_placeholder_nao_conta():
    assert tem_contratado({"codigoContratado": ""}) is False
    assert tem_contratado({"codigoContratado": "  "}) is False
    assert tem_contratado({"codigoContratado": "-"}) is False
    assert tem_contratado({}) is False


# ── escolher_registro ────────────────────────────────────────────────────────


def test_caso_real_25018627_escolhe_exercicio_com_contratado():
    ano, data = escolher_registro({2026: REG_2026_PLANEJAMENTO, 2025: REG_2025_CONTRATADO}, 2026)
    assert ano == 2025
    assert data["nomeContratado"] == "04602789000101 - DATEN TECNOLOGIA LTDA"


def test_ambos_com_contratado_fica_com_ano_corrente():
    reg_2026 = dict(REG_2025_CONTRATADO, situacao="ADITIVADO")
    ano, data = escolher_registro({2025: REG_2025_CONTRATADO, 2026: reg_2026}, 2026)
    assert ano == 2026
    assert data["situacao"] == "ADITIVADO"


def test_nenhum_com_contratado_fica_com_ano_corrente():
    reg_2025 = dict(REG_2026_PLANEJAMENTO, valor=1.0)
    ano, data = escolher_registro({2026: REG_2026_PLANEJAMENTO, 2025: reg_2025}, 2026)
    assert ano == 2026
    assert data is REG_2026_PLANEJAMENTO


def test_ano_corrente_com_contratado_nao_troca_pelo_anterior_sem():
    ano, _ = escolher_registro({2026: REG_2025_CONTRATADO, 2025: REG_2026_PLANEJAMENTO}, 2026)
    assert ano == 2026


def test_so_ano_corrente_disponivel():
    ano, data = escolher_registro({2026: REG_2026_PLANEJAMENTO}, 2026)
    assert ano == 2026
    assert data is REG_2026_PLANEJAMENTO


def test_sem_ano_corrente_nao_escolhe_nada():
    """Se o ano corrente falhou (erro transitorio), nao grava dado do ano anterior."""
    assert escolher_registro({2025: REG_2025_CONTRATADO}, 2026) == (None, None)
    assert escolher_registro({}, 2026) == (None, None)
