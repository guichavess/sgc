"""Conftest local para testes do módulo Identidade Visual."""
import pytest


@pytest.fixture()
def logged_client(client, db_session, app):
    """Client HTTP já autenticado como admin."""
    from app.models.usuario import Usuario
    with app.app_context():
        u = Usuario(
            id=999,
            id_usuario_sei='admin_iv_test',
            nome='ADMIN IV',
            sigla_login='admin_iv',
            is_admin=True,
            ativo=True,
        )
        db_session.add(u)
        db_session.commit()
        with client.session_transaction() as sess:
            sess['_user_id'] = str(u.id)
            sess['_fresh'] = True
    return client


def _criar_usuario_com_permissoes(db_session, app, client, uid, acoes):
    """Cria um usuário não-admin com um perfil contendo as ações informadas
    no módulo identidade_visual, e devolve um client autenticado como ele.

    acoes: lista de strings, ex. ['visualizar', 'editar'] ou ['visualizar', 'editar', 'excluir'].
    """
    from app.models.usuario import Usuario
    from app.models.perfil import Perfil, PerfilPermissao
    with app.app_context():
        perfil = Perfil(nome=f'Perfil IV {uid}', descricao='teste', ativo=True)
        db_session.add(perfil)
        db_session.flush()
        for acao in acoes:
            db_session.add(PerfilPermissao(
                perfil_id=perfil.id, modulo='identidade_visual', acao=acao,
            ))
        u = Usuario(
            id=uid,
            id_usuario_sei=f'iv_user_{uid}',
            nome=f'USER IV {uid}',
            sigla_login=f'iv_user_{uid}',
            is_admin=False,
            ativo=True,
            perfil_id=perfil.id,
        )
        db_session.add(u)
        db_session.commit()
        with client.session_transaction() as sess:
            sess['_user_id'] = str(u.id)
            sess['_fresh'] = True
    return client


@pytest.fixture()
def client_editor(client, db_session, app):
    """Client autenticado como usuário com visualizar+editar (SEM excluir)."""
    return _criar_usuario_com_permissoes(db_session, app, client, 1001,
                                         ['visualizar', 'editar'])


@pytest.fixture()
def client_full(client, db_session, app):
    """Client autenticado como usuário com acesso full (inclui excluir)."""
    return _criar_usuario_com_permissoes(db_session, app, client, 1002,
                                         ['visualizar', 'editar', 'excluir'])


@pytest.fixture()
def municipio_teresina(db_session):
    from app.models.identidade_visual import MunicipioPiaui
    m = MunicipioPiaui(id=1, nome='Teresina', codigo_ibge='2211001')
    db_session.add(m)
    db_session.commit()
    return m


@pytest.fixture()
def municipio_picos(db_session):
    from app.models.identidade_visual import MunicipioPiaui
    m = MunicipioPiaui(id=2, nome='Picos', codigo_ibge='2208007')
    db_session.add(m)
    db_session.commit()
    return m
