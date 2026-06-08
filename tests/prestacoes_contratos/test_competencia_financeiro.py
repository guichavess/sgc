"""
tests/prestacoes_contratos/test_competencia_financeiro.py

TDD para exibicao do campo `competencia` nas sub-abas da aba Financeiro
do gerenciamento de contrato (Empenhos, Liquidacoes, PDs, Pagamentos).

Bug reportado: a sub-aba PD nao trazia a coluna competencia para a tabela,
embora a coluna exista no banco (gravada por scripts/atualizar_pd.py).

Causa raiz: o model `PD` nao declarava o campo `competencia`, e o template
usava um workaround (mapa_competencias_pd) que cruzava por num_pd com a
tabela sis_solicitacoes -- so funcionava quando havia solicitacao SGC
vinculada com num_pd correspondente.

Fix: adicionar campo `competencia` aos models PD, Liquidacao e Empenho, e
ler direto do model no template (como ja era feito com OB.competencia).
"""
from datetime import datetime

import pytest

from app.models.pd import PD
from app.models.ob import OB
from app.models.liquidacao import Liquidacao
from app.models.empenho import Empenho


# ── Models: campo competencia deve existir ───────────────────────────────────


def test_pd_model_tem_campo_competencia(db_session):
    """Garante que o model PD declara competencia (estava faltando)."""
    pd = PD(
        id=1,
        codigo='2025NE000001',
        codContrato=12345,
        competencia='03/2025',
    )
    db_session.add(pd)
    db_session.flush()

    refetched = PD.query.filter_by(id=1).first()
    assert refetched is not None
    assert refetched.competencia == '03/2025'


def test_liquidacao_model_tem_campo_competencia(db_session):
    """Liquidacao precisa expor competencia (script ja calcula, falta gravar)."""
    liq = Liquidacao(
        id=1,
        codigo='2025NL000001',
        codContrato=12345,
        competencia='04/2025',
    )
    db_session.add(liq)
    db_session.flush()

    refetched = Liquidacao.query.filter_by(id=1).first()
    assert refetched is not None
    assert refetched.competencia == '04/2025'


def test_empenho_model_tem_campo_competencia(db_session):
    """Empenho precisa expor competencia para exibicao no template."""
    emp = Empenho(
        id=1,
        codigo='2025NE000001',
        codContrato=12345,
        competencia='02/2025',
    )
    db_session.add(emp)
    db_session.flush()

    refetched = Empenho.query.filter_by(id=1).first()
    assert refetched is not None
    assert refetched.competencia == '02/2025'


def test_ob_model_ja_tinha_competencia_continua_funcionando(db_session):
    """Regressao: OB ja expunha competencia, garante que nao quebramos."""
    ob = OB(
        id=1,
        codigo='2025OB000001',
        codContrato='12345',
        competencia='05/2025',
    )
    db_session.add(ob)
    db_session.flush()

    refetched = OB.query.filter_by(id=1).first()
    assert refetched is not None
    assert refetched.competencia == '05/2025'


# ── Service: listar_pds retorna objetos com competencia ──────────────────────


def test_listar_pds_retorna_competencia_do_model_diretamente(db_session):
    """
    PrestacaoContratoService.listar_pds deve retornar objetos PD com o atributo
    competencia preenchido a partir da coluna do banco -- sem precisar do
    workaround mapa_competencias_pd que dependia de sis_solicitacoes.
    """
    from app.services.prestacao_contrato_service import PrestacaoContratoService

    pd1 = PD(
        id=1,
        codigo='2025PD000001',
        codContrato=99999,
        statusDocumento='CONTABILIZADO',
        competencia='06/2025',
    )
    pd2 = PD(
        id=2,
        codigo='2025PD000002',
        codContrato=99999,
        statusDocumento='CONTABILIZADO',
        competencia=None,
    )
    db_session.add_all([pd1, pd2])
    db_session.flush()

    pds = PrestacaoContratoService.listar_pds(99999)
    competencias = {p.codigo: p.competencia for p in pds}

    assert competencias['2025PD000001'] == '06/2025'
    assert competencias['2025PD000002'] is None


def test_listar_pds_filtra_apenas_contabilizadas_do_contrato(db_session):
    """A aba Financeiro deve ignorar PD anulada e PD de outro contrato."""
    from app.services.prestacao_contrato_service import PrestacaoContratoService

    pd_contabilizada = PD(
        id=10,
        codigo='2025PD01203',
        codContrato=99999,
        statusDocumento='CONTABILIZADO',
        dataEmissao=datetime(2025, 5, 20),
        valor=100,
    )
    pd_anulada = PD(
        id=11,
        codigo='2025PD01204',
        codContrato=99999,
        statusDocumento='Anulado',
        dataEmissao=datetime(2025, 5, 21),
        valor=200,
    )
    pd_outro_contrato = PD(
        id=12,
        codigo='2025PD01205',
        codContrato=88888,
        statusDocumento='CONTABILIZADO',
        dataEmissao=datetime(2025, 5, 22),
        valor=300,
    )
    db_session.add_all([pd_contabilizada, pd_anulada, pd_outro_contrato])
    db_session.flush()

    pds = PrestacaoContratoService.listar_pds(99999)

    assert [p.codigo for p in pds] == ['2025PD01203']
    assert all(p.codContrato == 99999 for p in pds)
    assert all(p.statusDocumento == 'CONTABILIZADO' for p in pds)
