"""
tests/prestacoes_contratos/test_itens_para_execucao.py

TDD para listar_itens_para_execucao() — bug de re-filtro por tipificacao.

Cenario do bug (contrato 23000466):
  Contrato tipificado com PDM X, mas item vinculado pertence ao PDM Y.
  ANTES da correcao: item oculto → aviso enganoso "nenhum item compativel".
  APOS  a correcao: item sempre retornado — a vinculacao ja e a triagem certa.

Principio: listar_catalogo_para_vincular() filtra o catalogo por tipificacao
na hora de VINCULAR. listar_itens_para_execucao() deve confiar nessa triagem
e retornar TODOS os itens ja vinculados, sem re-filtrar.

Nota: todos os testes operam diretamente sobre db_session (sem with app_context
aninhado) para compartilhar a mesma sessao SQLAlchemy e evitar que Model.query
use uma sessao diferente da que tem os dados flushed.
"""
import pytest

from app.models.contrato import Contrato
from app.models.item_vinculado import ItemVinculado
from app.models.catmat import CatmatPdm, CatmatItem
from app.services.prestacao_contrato_service import PrestacaoContratoService


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def pdm_tipificado(db_session):
    """PDM registrado no banco — necessario para que o filtro de tipificacao seja ativado."""
    pdm = CatmatPdm(id=1, codigo=100, codigo_classe=10, nome='PDM Tipificado')
    db_session.add(pdm)
    db_session.flush()
    return pdm


@pytest.fixture()
def item_dentro_pdm(db_session, pdm_tipificado):
    """Item CATMAT que pertence AO PDM tipificado (caso sem bug)."""
    item = CatmatItem(id=20, codigo=2000, codigo_pdm=100, descricao='Item no PDM Correto')
    db_session.add(item)
    db_session.flush()
    return item


@pytest.fixture()
def item_fora_pdm(db_session):
    """Item CATMAT que pertence a um PDM diferente do tipificado — cenario do bug."""
    item = CatmatItem(id=10, codigo=1000, codigo_pdm=999, descricao='Item em Outro PDM')
    db_session.add(item)
    db_session.flush()
    return item


@pytest.fixture()
def contrato_catmat(db_session, pdm_tipificado):
    """Contrato do tipo M tipificado com CatmatPdm id=1 (codigo=100)."""
    c = Contrato(
        codigo='TC001',
        tipo_contrato='M',
        catmat_pdm_id=1,
        objeto='Contrato teste CATMAT',
    )
    db_session.add(c)
    db_session.flush()
    return c


@pytest.fixture()
def contrato_sem_tipo(db_session):
    """Contrato sem tipo_contrato — fallback que retorna tudo."""
    c = Contrato(codigo='TC002', tipo_contrato=None, objeto='Contrato sem tipo')
    db_session.add(c)
    db_session.flush()
    return c


# ── Testes ────────────────────────────────────────────────────────────────────

def test_item_fora_pdm_tipificado_deve_aparecer(db_session, contrato_catmat,
                                                 pdm_tipificado, item_fora_pdm):
    """
    BUG PRINCIPAL: item vinculado com catmat_item_id=10 (PDM 999) em contrato
    tipificado pelo PDM 100. Com o re-filtro antigo, seria ocultado. Apos a
    correcao, deve sempre aparecer — a vinculacao ja foi a triagem.
    """
    v = ItemVinculado(codigo_contrato='TC001', tipo='M', catmat_item_id=10)
    db_session.add(v)
    db_session.flush()

    servicos, materiais, ocultados = PrestacaoContratoService.listar_itens_para_execucao(
        contrato_catmat
    )

    assert len(materiais) == 1, (
        "Item vinculado fora do PDM tipificado deve aparecer: "
        "listar_catalogo_para_vincular() ja filtra o catalogo na hora de vincular."
    )
    assert ocultados == 0
    assert materiais[0]['catmat_item_id'] == 10


def test_item_dentro_pdm_tipificado_continua_aparecendo(db_session, contrato_catmat,
                                                         pdm_tipificado, item_dentro_pdm):
    """Item dentro da tipificacao deve aparecer antes e depois da correcao."""
    v = ItemVinculado(codigo_contrato='TC001', tipo='M', catmat_item_id=20)
    db_session.add(v)
    db_session.flush()

    servicos, materiais, ocultados = PrestacaoContratoService.listar_itens_para_execucao(
        contrato_catmat
    )

    assert len(materiais) == 1
    assert ocultados == 0
    assert materiais[0]['catmat_item_id'] == 20


def test_sem_itens_vinculados_retorna_vazio(db_session, contrato_catmat, pdm_tipificado):
    """Contrato sem itens vinculados retorna listas vazias — aviso correto no fluxo."""
    servicos, materiais, ocultados = PrestacaoContratoService.listar_itens_para_execucao(
        contrato_catmat
    )

    assert servicos == []
    assert materiais == []
    assert ocultados == 0


def test_contrato_sem_tipo_retorna_todos_itens(db_session, contrato_sem_tipo):
    """Contrato sem tipo_contrato usa o fallback e retorna todos os vinculados."""
    item_m = CatmatItem(id=50, codigo=5000, codigo_pdm=50, descricao='Item Qualquer')
    db_session.add(item_m)
    db_session.flush()

    v = ItemVinculado(codigo_contrato='TC002', tipo='M', catmat_item_id=50)
    db_session.add(v)
    db_session.flush()

    servicos, materiais, ocultados = PrestacaoContratoService.listar_itens_para_execucao(
        contrato_sem_tipo
    )

    assert len(materiais) == 1
    assert ocultados == 0


def test_total_ocultados_sempre_zero_apos_correcao(db_session, contrato_catmat,
                                                    pdm_tipificado, item_fora_pdm):
    """Apos a correcao, total_ocultados e sempre 0 — o aviso de filtro nao deve aparecer."""
    v = ItemVinculado(codigo_contrato='TC001', tipo='M', catmat_item_id=10)
    db_session.add(v)
    db_session.flush()

    _, _, ocultados = PrestacaoContratoService.listar_itens_para_execucao(contrato_catmat)

    assert ocultados == 0, (
        "total_ocultados deve ser 0 apos a correcao — o template nao deve "
        "exibir o aviso de 'filtro de tipificacao ativo'."
    )


def test_multiplos_itens_tipos_mistos(db_session, contrato_catmat, pdm_tipificado):
    """Contrato SM com itens de tipos diferentes — todos retornados sem filtrar."""
    contrato_catmat.tipo_contrato = 'SM'
    db_session.flush()

    item_m = CatmatItem(id=30, codigo=3000, codigo_pdm=999, descricao='Material')
    db_session.add(item_m)
    db_session.flush()

    v_m = ItemVinculado(codigo_contrato='TC001', tipo='M', catmat_item_id=30)
    db_session.add(v_m)
    db_session.flush()

    servicos, materiais, ocultados = PrestacaoContratoService.listar_itens_para_execucao(
        contrato_catmat
    )

    assert len(materiais) == 1
    assert ocultados == 0
