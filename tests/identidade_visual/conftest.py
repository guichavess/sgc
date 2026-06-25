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
