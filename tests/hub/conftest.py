"""Conftest local para testes do Hub de módulos."""
import pytest


def criar_usuario(db_session, uid, permissoes=None, is_admin=False):
    """Cria um usuário (opcionalmente com perfil) e devolve o objeto.

    permissoes: dict {modulo: [acoes]} — None cria o usuário sem perfil.
    """
    from app.models.usuario import Usuario
    from app.models.perfil import Perfil, PerfilPermissao

    perfil_id = None
    if permissoes is not None:
        perfil = Perfil(nome=f'Perfil Hub {uid}', descricao='teste', ativo=True)
        db_session.add(perfil)
        db_session.flush()
        for modulo, acoes in permissoes.items():
            for acao in acoes:
                db_session.add(PerfilPermissao(perfil_id=perfil.id, modulo=modulo, acao=acao))
        perfil_id = perfil.id

    u = Usuario(
        id=uid,
        id_usuario_sei=f'hub_user_{uid}',
        nome=f'USUARIO HUB {uid}',
        sigla_login=f'hub_user_{uid}',
        is_admin=is_admin,
        ativo=True,
        perfil_id=perfil_id,
    )
    db_session.add(u)
    db_session.commit()
    return u


def logar(client, usuario):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True
    return client


@pytest.fixture()
def novo_usuario(db_session):
    """Fábrica: novo_usuario(uid, permissoes=None, is_admin=False)."""
    def _criar(uid, permissoes=None, is_admin=False):
        return criar_usuario(db_session, uid, permissoes, is_admin)
    return _criar
