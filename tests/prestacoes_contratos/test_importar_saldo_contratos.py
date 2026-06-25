from datetime import date
from decimal import Decimal

from scripts.importar_saldo_contratos import (
    ExecucaoPlanilha,
    ReconciliacaoBanco,
    ReconciliacaoResultado,
    analisar_planilha_rows,
    gerar_sql_reset_saldo_contrato,
    reconciliar_contratos,
    reconciliar_execucoes,
    serial_excel_para_data,
)


def test_serial_excel_para_data_converte_corretamente():
    assert serial_excel_para_data(45292) == date(2024, 1, 1)
    assert serial_excel_para_data(45323) == date(2024, 2, 1)
    assert serial_excel_para_data(46023) == date(2026, 1, 1)


def _rows_contratos_vigentes(linhas_dados):
    """Monta a estrutura da aba CONTRATOS VIGENTES.

    - Linha 0: serial Excel dos meses (pareados entre QTD/VALOR), comecando na col 5.
    - Linha 1: cabecalho.
    - Linhas seguintes: dados.
    """
    linha_meses = [None, None, None, None, None, 45292, 45292, 45323, 45323]
    linha_header = [
        "N° SIAFE",
        "CONTRATO",
        "CONTRATADA",
        "VALOR(R$) Global do Contrato",
        "ITEM",
        "QTD",
        "VALOR",
        "QTD",
        "VALOR",
    ]
    return [linha_meses, linha_header, *linhas_dados]


def test_agrega_linhas_duplicadas_a_partir_da_aba_contratos_vigentes():
    rows = _rows_contratos_vigentes(
        [
            ["23005147", "484/2023", "Fornecedor A", "1.000,00", "Item X", 99, "100,00", 88, "200,00"],
            ["23005147", "484/2023", "Fornecedor A", "500,00", "Item Y", 77, "50,00", None, None],
        ]
    )
    resultado = analisar_planilha_rows(rows)

    contrato = resultado.contratos["23005147"]
    assert contrato.linhas == 2
    assert contrato.valor_global_total == Decimal("1500.00")
    assert contrato.valor_mensal_executado_total == Decimal("350.00")
    assert contrato.saldo_restante_calculado == Decimal("1150.00")
    assert contrato.valores_por_mes["2024-01"] == Decimal("150.00")
    assert contrato.valores_por_mes["2024-02"] == Decimal("200.00")


def test_trata_valor_global_vazio_hifen_e_codigo_invalido():
    rows = _rows_contratos_vigentes(
        [
            ["S/N", "Sem contrato", "X", "-", "Item invalido", None, "10,00", None, None],
            ["24000142", "29/2024", "Y", None, "Item ok", None, "30,00", None, None],
        ]
    )
    resultado = analisar_planilha_rows(rows)

    assert [item.codigo for item in resultado.codigos_invalidos] == ["S/N"]
    assert resultado.contratos["24000142"].valor_global_total == Decimal("0.00")
    assert resultado.contratos["24000142"].saldo_restante_calculado == Decimal("-30.00")


def test_extrai_execucoes_por_item_para_diff():
    rows = _rows_contratos_vigentes(
        [
            ["23005147", "484/2023", "Forn", "1.000,00", "Item X", 1, "100,00", 2, "200,00"],
            ["23005147", "484/2023", "Forn", "500,00", "Item Y", None, None, 3, "50,00"],
        ]
    )
    resultado = analisar_planilha_rows(rows)

    chaves = {
        (e.codigo_contrato, e.ano, e.mes, str(e.valor), e.item_descricao)
        for e in resultado.execucoes_planilha
    }
    assert ("23005147", 2024, 1, "100.00", "Item X") in chaves
    assert ("23005147", 2024, 2, "200.00", "Item X") in chaves
    assert ("23005147", 2024, 2, "50.00", "Item Y") in chaves
    # Linha sem valor mensal para Item Y/jan/2024 nao deve gerar execucao
    assert all(
        not (e.codigo_contrato == "23005147" and e.ano == 2024 and e.mes == 1 and e.item_descricao == "Item Y")
        for e in resultado.execucoes_planilha
    )


def test_marca_contrato_nao_encontrado_e_sem_saldo_na_reconciliacao():
    resultado = reconciliar_contratos(
        codigos_planilha={"100", "200", "400"},
        banco=ReconciliacaoBanco(
            contratos_existentes={"100", "200", "300"},
            contratos_com_saldo={"200"},
            total_contratos=3,
            total_contratos_com_saldo=1,
        ),
    )

    assert resultado.planilha_nao_encontrados == ["400"]
    assert resultado.planilha_existentes_sem_saldo == ["100"]
    assert resultado.planilha_existentes_com_saldo == ["200"]
    assert resultado.producao_sem_saldo == ["100", "300"]


def test_reset_sql_emite_delete_e_insert_apenas_contratos_existentes():
    rows = _rows_contratos_vigentes(
        [
            ["100", "C-100", "X", "150,00", "I1", 1, "50,00", None, None],
            ["200", "C-200", "Y", "300,00", "I2", 1, "10,00", None, None],
            ["300", "C-300", "Z", "20,00", "I3", 1, "40,00", None, None],
            ["400", "C-400", "W", "500,00", "I4", 1, "0,00", None, None],
        ]
    )
    planilha = analisar_planilha_rows(rows)
    reconciliacao = ReconciliacaoResultado(
        producao_sem_saldo=["300"],
        planilha_nao_encontrados=["400"],
        planilha_existentes_sem_saldo=["100", "300"],
        planilha_existentes_com_saldo=["200"],
    )

    sql = gerar_sql_reset_saldo_contrato(planilha, reconciliacao)

    assert "DELETE FROM saldo_contrato;" in sql.conteudo
    assert "START TRANSACTION;" in sql.conteudo
    assert "COMMIT;" in sql.conteudo
    assert "'100', 100.00" in sql.conteudo
    assert "'200', 290.00" in sql.conteudo
    assert "'400'" not in sql.conteudo
    # Saldo negativo (300: 20 - 40 = -20) deve ser ignorado por padrao
    assert "'300'" not in sql.conteudo
    assert sql.total_inserts == 2
    assert sql.contratos_ignorados_negativos == ["300"]


def test_diff_execucoes_classifica_apenas_planilha_apenas_db_e_divergente():
    execucoes_planilha = [
        ExecucaoPlanilha(
            codigo_contrato="100", ano=2024, mes=1, data=date(2024, 1, 1),
            valor=Decimal("100.00"), quantidade=1, item_descricao="A",
        ),
        ExecucaoPlanilha(
            codigo_contrato="100", ano=2024, mes=2, data=date(2024, 2, 1),
            valor=Decimal("200.00"), quantidade=1, item_descricao="A",
        ),
        ExecucaoPlanilha(
            codigo_contrato="200", ano=2024, mes=3, data=date(2024, 3, 1),
            valor=Decimal("300.00"), quantidade=1, item_descricao="B",
        ),
    ]
    execucoes_db = {
        ("100", 2024, 1): {"valor": Decimal("100.00"), "quantidade": 1},
        ("100", 2024, 2): {"valor": Decimal("150.00"), "quantidade": 1},
        ("100", 2024, 5): {"valor": Decimal("400.00"), "quantidade": 1},
    }

    resultado = reconciliar_execucoes(execucoes_planilha, execucoes_db)

    chaves_match = {(d.codigo_contrato, d.ano, d.mes) for d in resultado.match}
    chaves_divergentes = {(d.codigo_contrato, d.ano, d.mes) for d in resultado.divergentes}
    chaves_apenas_planilha = {(d.codigo_contrato, d.ano, d.mes) for d in resultado.apenas_planilha}
    chaves_apenas_db = {(d.codigo_contrato, d.ano, d.mes) for d in resultado.apenas_db}

    assert ("100", 2024, 1) in chaves_match
    assert ("100", 2024, 2) in chaves_divergentes
    assert ("200", 2024, 3) in chaves_apenas_planilha
    assert ("100", 2024, 5) in chaves_apenas_db
