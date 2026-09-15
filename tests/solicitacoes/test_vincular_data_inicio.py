"""
Data de início do processo ao VINCULAR um processo SEI existente (Pagamentos).

Bug (contrato 26101511): ao vincular, a solicitação ficava com a data/hora da
vinculação em `data_solicitacao` e no histórico da etapa 1, em vez da data do
1º documento do processo SEI. Isso distorcia timeline, "tempo decorrido" e
`tempo_total`.

Regra: data de início = menor data entre TODOS os documentos do processo.
Sem documentos datados → mantém a data atual (fallback).
"""
from datetime import datetime
from unittest.mock import patch

import pytest

from app.utils.sei_datas import parse_data_documento_sei, data_primeiro_documento_sei


# ──────────────────────────────────────────────────────────────────────────────
# Funções auxiliares (lógica pura)
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('valor,esperado', [
    ('10/02/2026', datetime(2026, 2, 10)),
    ('10/02/2026 14:30:00', datetime(2026, 2, 10, 14, 30)),
    ('2026-02-10', datetime(2026, 2, 10)),
    ('2026-02-10 14:30:00', datetime(2026, 2, 10, 14, 30)),
    (" '10/02/2026' ", datetime(2026, 2, 10)),
])
def test_parse_data_documento_formatos_sei(valor, esperado):
    assert parse_data_documento_sei(valor) == esperado


@pytest.mark.parametrize('valor', [None, '', 'invalida', '31/02/2026'])
def test_parse_data_documento_invalida_retorna_none(valor):
    assert parse_data_documento_sei(valor) is None


def test_data_primeiro_documento_e_a_menor_data_independente_da_ordem():
    docs = [
        {'IdDocumento': '3', 'Data': '15/03/2026'},
        {'IdDocumento': '1', 'Data': '20/02/2026'},
        {'IdDocumento': '2', 'Data': '10/02/2026'},
    ]
    assert data_primeiro_documento_sei(docs) == datetime(2026, 2, 10)


def test_data_primeiro_documento_usa_datageracao_e_ignora_invalidas():
    docs = [
        {'IdDocumento': '1', 'Data': ''},
        {'IdDocumento': '2', 'DataGeracao': '05/01/2026'},
        {'IdDocumento': '3', 'Data': 'lixo'},
        {'IdDocumento': '4', 'Data': '07/01/2026'},
    ]
    assert data_primeiro_documento_sei(docs) == datetime(2026, 1, 5)


@pytest.mark.parametrize('docs', [None, [], [{'IdDocumento': '1'}]])
def test_data_primeiro_documento_sem_datas_retorna_none(docs):
    assert data_primeiro_documento_sei(docs) is None


# ──────────────────────────────────────────────────────────────────────────────
# Rota POST /solicitacoes/vincular
# ──────────────────────────────────────────────────────────────────────────────

PROTOCOLO = '00002.001234/2026-10'
CONTRATO = '26101511'


def _proc_sei():
    return {
        'sucesso': True,
        'protocolo_formatado': PROTOCOLO,
        'id_procedimento': '555001',
        'link_acesso': 'https://sei/proc/555001',
        'especificacao': 'PAGAMENTO DE CONTRATO',
        'erro': None,
    }


def _doc(id_doc, data, serie):
    return {
        'IdDocumento': id_doc,
        'IdProcedimento': '555001',
        'ProcedimentoFormatado': PROTOCOLO,
        'DocumentoFormatado': f'00{id_doc}',
        'Data': data,
        'Numero': '',
        'Serie': {'IdSerie': serie, 'Nome': f'Serie {serie}'},
        'UnidadeElaboradora': {'IdUnidade': '1', 'Sigla': 'SEAD'},
    }


@pytest.fixture()
def cenario_vinculo(db_session, usuario_com_permissoes, logar):
    from app.models import Contrato, Etapa

    admin = usuario_com_permissoes(9001, is_admin=True)
    db_session.add(Contrato(codigo=CONTRATO, numeroOriginal='01/2026'))
    db_session.add(Etapa(id=1, nome='Criada', alias='criada', ordem=1))
    db_session.commit()
    return logar(admin)


def _vincular(client, documentos):
    with patch('app.solicitacoes.routes.crud.gerar_token_sei_admin', return_value='TOKEN'), \
         patch('app.solicitacoes.routes.crud.consultar_procedimento_sei', return_value=_proc_sei()), \
         patch('app.solicitacoes.routes.crud.listar_documentos_procedimento_sei',
               return_value={'sucesso': True, 'documentos': documentos, 'erro': None}):
        resp = client.post('/solicitacoes/vincular', data={
            'protocolo_sei': PROTOCOLO,
            'contrato_selecionado': CONTRATO,
            'competencia': '08/2026',
        })
    assert resp.status_code == 302
    assert '/solicitacoes/solicitacao/' in resp.headers['Location'], (
        f"Vinculação não concluiu — redirect para {resp.headers['Location']}"
    )
    return resp


def _solicitacao_e_hist_etapa1():
    from app.models import Solicitacao, HistoricoMovimentacao

    sol = Solicitacao.query.filter_by(protocolo_gerado_sei=PROTOCOLO).one()
    hist1 = HistoricoMovimentacao.query.filter_by(
        id_solicitacao=sol.id, id_etapa_nova=1
    ).order_by(HistoricoMovimentacao.id.asc()).first()
    return sol, hist1


def test_vincular_usa_data_do_primeiro_documento_sem_serie_solicitacao(cenario_vinculo):
    """Processo sem doc 'Solicitação' (2614): antes ficava com a data da vinculação."""
    docs = [
        _doc('1001', '10/02/2026', '999'),   # 1º documento, série não mapeada
        _doc('1002', '15/03/2026', '64'),    # Requerimento → etapas 8/12
    ]
    _vincular(cenario_vinculo, docs)

    sol, hist1 = _solicitacao_e_hist_etapa1()
    assert sol.data_solicitacao.date() == datetime(2026, 2, 10).date()
    assert hist1 is not None
    assert hist1.data_movimentacao.date() == datetime(2026, 2, 10).date()


def test_vincular_etapa1_usa_primeiro_documento_mesmo_com_serie_solicitacao_posterior(cenario_vinculo):
    """Doc 'Solicitação' datado depois do 1º documento não pode empurrar o início."""
    docs = [
        _doc('2001', '10/02/2026', '999'),
        _doc('2002', '20/02/2026', '2614'),  # SerieDocumentoSEI.SOLICITACAO
    ]
    _vincular(cenario_vinculo, docs)

    sol, hist1 = _solicitacao_e_hist_etapa1()
    assert sol.data_solicitacao.date() == datetime(2026, 2, 10).date()
    assert hist1.data_movimentacao.date() == datetime(2026, 2, 10).date()


def test_vincular_sem_documentos_datados_mantem_data_atual(cenario_vinculo):
    antes = datetime.now()
    _vincular(cenario_vinculo, [])

    sol, hist1 = _solicitacao_e_hist_etapa1()
    assert sol.data_solicitacao >= antes.replace(microsecond=0)
    assert hist1.data_movimentacao >= antes.replace(microsecond=0)
