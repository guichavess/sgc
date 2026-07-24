"""
tests/prestacoes_contratos/test_listar_contratos.py

TDD para listar_contratos() — versao nao paginada de listar_contratos_paginado(),
usada pela pagina de Execucoes de Contratos para carregar todos os contratos de
uma vez (busca global client-side no estilo CGFR).

Deve ter paridade de filtros com listar_contratos_paginado(), apenas sem paginar.
"""
import pytest
from sqlalchemy import text

from app.models.contrato import Contrato
from app.repositories.info_contrato_repository import InfoContratoRepository
from app.services.prestacao_contrato_service import PrestacaoContratoService


@pytest.fixture()
def contratos_variados(db_session):
    """Três contratos com situações/UGs diferentes para testar filtros."""
    c1 = Contrato(
        codigo='C001', situacao='EM_VIGOR', nomeContratado='Empresa Alfa',
        codigoUG='210101', modalidade='SERVICOS',
    )
    c2 = Contrato(
        codigo='C002', situacao='ENCERRADO', nomeContratado='Empresa Beta',
        codigoUG='210101', modalidade='FORNECIMENTO_MATERIAIS',
    )
    c3 = Contrato(
        codigo='C003', situacao='EM_VIGOR', nomeContratado='Empresa Gama',
        codigoUG='210102', modalidade='SERVICOS',
    )
    db_session.add_all([c1, c2, c3])
    db_session.flush()
    return [c1, c2, c3]


def test_listar_todos_com_filtros_sem_filtro_retorna_todos(db_session, contratos_variados):
    """Sem filtros, retorna todos os contratos (lista simples, sem paginacao)."""
    resultado = InfoContratoRepository.listar_todos_com_filtros()

    assert isinstance(resultado, list)
    assert len(resultado) == 3


def test_listar_todos_com_filtros_respeita_filtro_situacao(db_session, contratos_variados):
    resultado = InfoContratoRepository.listar_todos_com_filtros(situacao=['EM_VIGOR'])

    codigos = {c.codigo for c in resultado}
    assert codigos == {'C001', 'C003'}


def test_listar_todos_com_filtros_respeita_filtro_codigo(db_session, contratos_variados):
    resultado = InfoContratoRepository.listar_todos_com_filtros(codigo='C002')

    assert len(resultado) == 1
    assert resultado[0].codigo == 'C002'


def test_listar_todos_com_filtros_respeita_filtro_ug(db_session, contratos_variados):
    resultado = InfoContratoRepository.listar_todos_com_filtros(codigoUG='210102')

    assert len(resultado) == 1
    assert resultado[0].codigo == 'C003'


def test_listar_todos_com_filtros_respeita_filtro_tipo_contrato(db_session, contratos_variados):
    resultado = InfoContratoRepository.listar_todos_com_filtros(tipo_contrato=['MATERIAL'])

    assert len(resultado) == 1
    assert resultado[0].codigo == 'C002'


def test_listar_todos_com_filtros_paridade_com_paginado(db_session, contratos_variados):
    """Mesmos filtros aplicados devem produzir o mesmo conjunto de códigos,
    só que um paginado (com per_page grande) e outro não."""
    paginado = InfoContratoRepository.listar_com_filtros(situacao=['EM_VIGOR'], page=1, per_page=50)
    todos = InfoContratoRepository.listar_todos_com_filtros(situacao=['EM_VIGOR'])

    assert {c.codigo for c in paginado.items} == {c.codigo for c in todos}


def test_service_listar_contratos_retorna_lista_nao_paginada(db_session, contratos_variados):
    """PrestacaoContratoService.listar_contratos() delega ao repositório e
    retorna uma lista simples (não um objeto Pagination)."""
    resultado = PrestacaoContratoService.listar_contratos()

    assert isinstance(resultado, list)
    assert len(resultado) == 3
    assert not hasattr(resultado, 'items')


def test_service_listar_contratos_aplica_filtros(db_session, contratos_variados):
    resultado = PrestacaoContratoService.listar_contratos(situacao=['ENCERRADO'])

    assert len(resultado) == 1
    assert resultado[0].codigo == 'C002'


def _login_admin(client, db_session):
    from app.models.usuario import Usuario

    usuario = Usuario(
        id_usuario_sei='admin_prest_teste',
        nome='ADMIN PRESTACOES TESTE',
        sigla_login='admin_prest_teste',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True
    return usuario


def _criar_tabela_ug(db_session):
    """Cria a tabela `ug` (não existe como model) — usada por listar_ugs()."""
    db_session.execute(text(
        "CREATE TABLE IF NOT EXISTS ug (codigo VARCHAR(10) PRIMARY KEY, titulo VARCHAR(200))"
    ))


def test_dashboard_route_200_sem_paginacao(client, db_session, contratos_variados):
    """A rota principal deve responder 200 e não depender mais de `pagination`
    (agora carrega todos os contratos de uma vez para a busca client-side)."""
    _criar_tabela_ug(db_session)
    _login_admin(client, db_session)

    resp = client.get('/prestacoes-contratos/contratos')

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'tabelaContratos' in body
    assert 'C001' in body and 'C002' in body and 'C003' in body


def test_dashboard_route_401_sem_login(client, db_session, contratos_variados):
    resp = client.get('/prestacoes-contratos/contratos')
    assert resp.status_code in (302, 401)
