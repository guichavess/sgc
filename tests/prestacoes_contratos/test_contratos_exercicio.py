"""
tests/prestacoes_contratos/test_contratos_exercicio.py

TDD da mescla de exercicios SIAFE em `scripts/atualizar_contratos.py`.

Contexto: o SIAFE mantem um registro do contrato POR EXERCICIO. No contrato
25018627 o registro de 2026 ficou sem contratado (" - ") enquanto o de 2025
recebeu a contratacao (DATEN, contrato 31/2026). O script so lia 2026.

Politica (definida com o usuario em 2026-09-16):
  1) consulta o ano corrente e, se o contrato ja existia antes, o ano anterior
  2) base = ano corrente; campo vazio ("", "-", " - ", None...) e preenchido
     com o valor do ano anterior
  3) contratado (tipo, codigo, nome) e um bloco: sem codigo no ano corrente,
     os tres vem do ano anterior (evita "PF" com CNPJ)
  4) falha no ano corrente -> nao grava; falha transitoria no ano anterior ->
     nao grava (evita alternar entre dado mesclado e nao mesclado)
"""
from scripts.contratos_exercicio import (
    eh_vazio,
    exercicios_a_consultar,
    falha_transitoria,
    mesclar_registros,
)

COLUNAS = [
    "codigo", "situacao", "numeroOriginal", "objeto", "tipoContratado",
    "codigoContratado", "nomeContratado", "valor", "nomeModalidadeLicitacao",
    "regimeExecucao", "modalidade", "dataFimVigencia",
]

# Dados reais do SIAFE (consultados em 2026-09-16), campos relevantes
REG_2026 = {
    "codigo": "25018627",
    "situacao": "A_CONTRATAR",
    "numeroOriginal": None,
    "objeto": "Aquisição de Infraestrutura de Tecnologia da Informação - TI",
    "tipoContratado": "TIPO_CREDOR_PF",
    "codigoContratado": None,
    "nomeContratado": " - ",
    "valor": 2970048.28,
    "nomeModalidadeLicitacao": "Pregão",
    "regimeExecucao": "PRESTACAO_MENSAL",
    "modalidade": "FORNECIMENTO_BENS",
    "dataFimVigencia": None,
    "responsaveisContrato": None,
    "aditivos": None,
}

REG_2025 = {
    "codigo": "25018627",
    "situacao": "LICITADO",
    "numeroOriginal": "31/2026",
    "objeto": "Aquisição de Microcomputadores Desktop",
    "tipoContratado": "TIPO_CREDOR_PJ",
    "codigoContratado": "04602789000101",
    "nomeContratado": "04602789000101 - DATEN TECNOLOGIA LTDA",
    "valor": 1332998.35,
    "nomeModalidadeLicitacao": None,
    "regimeExecucao": None,
    "modalidade": "OUTROS",
    "dataFimVigencia": None,
    "responsaveisContrato": None,
    "aditivos": None,
}


# ── exercicios_a_consultar ───────────────────────────────────────────────────


def test_contrato_de_ano_anterior_consulta_ano_corrente_e_anterior():
    assert exercicios_a_consultar("25018627", 2026) == [2026, 2025]
    assert exercicios_a_consultar("23005147", 2026) == [2026, 2025]


def test_contrato_criado_no_ano_corrente_consulta_so_ano_corrente():
    assert exercicios_a_consultar("26000123", 2026) == [2026]


def test_codigo_sem_prefixo_de_ano_consulta_so_ano_corrente():
    assert exercicios_a_consultar("abc", 2026) == [2026]
    assert exercicios_a_consultar("", 2026) == [2026]
    assert exercicios_a_consultar(None, 2026) == [2026]


# ── eh_vazio ─────────────────────────────────────────────────────────────────


def test_indicios_de_vazio():
    for v in (None, "", "   ", "-", " - ", "--", ".", "null", "None", "N/A", [], {}):
        assert eh_vazio(v), repr(v)


def test_valores_preenchidos():
    for v in ("DATEN", "31/2026", "A_CONTRATAR", 2970048.28, 0, [{"cpf": "1"}]):
        assert not eh_vazio(v), repr(v)


# ── falha_transitoria ────────────────────────────────────────────────────────


def test_falha_transitoria():
    for st in ("error", 429, 500, 502, 503, 504):
        assert falha_transitoria(st), st
    for st in (404, 400, "no_data", 200):
        assert not falha_transitoria(st), st


# ── mesclar_registros ────────────────────────────────────────────────────────


def test_caso_real_25018627():
    data, campos = mesclar_registros(REG_2026, REG_2025, COLUNAS)

    # Preenchidos em 2026: mantem o valor novo
    assert data["situacao"] == "A_CONTRATAR"
    assert data["objeto"] == "Aquisição de Infraestrutura de Tecnologia da Informação - TI"
    assert data["valor"] == 2970048.28
    assert data["nomeModalidadeLicitacao"] == "Pregão"
    assert data["regimeExecucao"] == "PRESTACAO_MENSAL"
    assert data["modalidade"] == "FORNECIMENTO_BENS"

    # Vazios em 2026: completa com 2025
    assert data["numeroOriginal"] == "31/2026"
    assert data["codigoContratado"] == "04602789000101"
    assert data["nomeContratado"] == "04602789000101 - DATEN TECNOLOGIA LTDA"

    # Bloco do contratado: tipo acompanha o CNPJ
    assert data["tipoContratado"] == "TIPO_CREDOR_PJ"

    # Vazio nos dois: continua vazio
    assert data["dataFimVigencia"] is None

    assert sorted(campos) == ["codigoContratado", "nomeContratado", "numeroOriginal", "tipoContratado"]


def test_nao_altera_os_registros_originais():
    antes_2026, antes_2025 = dict(REG_2026), dict(REG_2025)
    mesclar_registros(REG_2026, REG_2025, COLUNAS)
    assert REG_2026 == antes_2026
    assert REG_2025 == antes_2025


def test_contratado_do_ano_corrente_nao_e_trocado():
    atual = dict(REG_2026, tipoContratado="TIPO_CREDOR_PJ", codigoContratado="11111111000111",
                 nomeContratado="11111111000111 - OUTRA LTDA")
    data, campos = mesclar_registros(atual, REG_2025, COLUNAS)
    assert data["codigoContratado"] == "11111111000111"
    assert data["nomeContratado"] == "11111111000111 - OUTRA LTDA"
    assert data["tipoContratado"] == "TIPO_CREDOR_PJ"
    assert "codigoContratado" not in campos


def test_nome_vazio_com_cnpj_no_ano_corrente_nao_puxa_empresa_do_anterior():
    atual = dict(REG_2026, tipoContratado="TIPO_CREDOR_PJ", codigoContratado="11111111000111", nomeContratado=" - ")
    data, campos = mesclar_registros(atual, REG_2025, COLUNAS)
    assert data["codigoContratado"] == "11111111000111"
    assert data["nomeContratado"] == " - "
    assert "nomeContratado" not in campos


def test_fiscais_e_aditivos_vazios_vem_do_ano_anterior():
    anterior = dict(REG_2025, responsaveisContrato=[{"cpf": "1"}], aditivos=[{"codAditivo": "1"}])
    data, campos = mesclar_registros(REG_2026, anterior, COLUNAS)
    assert data["responsaveisContrato"] == [{"cpf": "1"}]
    assert data["aditivos"] == [{"codAditivo": "1"}]
    assert "responsaveisContrato" in campos and "aditivos" in campos


def test_fiscais_e_aditivos_do_ano_corrente_sao_mantidos():
    atual = dict(REG_2026, responsaveisContrato=[{"cpf": "2"}], aditivos=[{"codAditivo": "2"}])
    anterior = dict(REG_2025, responsaveisContrato=[{"cpf": "1"}], aditivos=[{"codAditivo": "1"}])
    data, _ = mesclar_registros(atual, anterior, COLUNAS)
    assert data["responsaveisContrato"] == [{"cpf": "2"}]
    assert data["aditivos"] == [{"codAditivo": "2"}]


def test_fichas_iguais_nao_completam_nada():
    data, campos = mesclar_registros(REG_2025, dict(REG_2025), COLUNAS)
    assert data == REG_2025
    assert campos == []
