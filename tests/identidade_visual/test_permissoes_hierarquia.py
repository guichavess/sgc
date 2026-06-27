"""Testes da hierarquia de permissões — "acesso full a um módulo = acesso a tudo".

Regra: ações de maior privilégio implicam as de menor
(``excluir`` ⇒ ``editar`` ⇒ ``criar``) e qualquer ação concedida no módulo
implica poder ``visualizar``. ``aprovar`` é ortogonal (não implica escrita,
mas concede visualização).
"""
from app.models.perfil import Perfil, PerfilPermissao
from app.models.usuario import Usuario
from app.models.identidade_visual import IdentidadeVisualLocal


def _perfil(db_session, *acoes, modulo='identidade_visual'):
    perfil = Perfil(nome=f'Perfil {"-".join(acoes) or "vazio"} {id(acoes)}', ativo=True)
    db_session.add(perfil)
    db_session.flush()
    for a in acoes:
        db_session.add(PerfilPermissao(perfil_id=perfil.id, modulo=modulo, acao=a))
    db_session.commit()
    return perfil


def _client_com_acoes(db_session, app, client, uid, acoes):
    with app.app_context():
        perfil = _perfil(db_session, *acoes)
        u = Usuario(id=uid, id_usuario_sei=f'h_{uid}', nome=f'H {uid}',
                    sigla_login=f'h_{uid}', is_admin=False, ativo=True,
                    perfil_id=perfil.id)
        db_session.add(u)
        db_session.commit()
        with client.session_transaction() as sess:
            sess['_user_id'] = str(u.id)
            sess['_fresh'] = True
    return client


class TestHierarquiaUnitaria:

    def test_excluir_implica_editar_criar_visualizar(self, app, db_session):
        with app.app_context():
            p = _perfil(db_session, 'excluir')
            assert p.tem_permissao('identidade_visual', 'visualizar')
            assert p.tem_permissao('identidade_visual', 'criar')
            assert p.tem_permissao('identidade_visual', 'editar')
            assert p.tem_permissao('identidade_visual', 'excluir')

    def test_editar_implica_criar_e_visualizar_mas_nao_excluir(self, app, db_session):
        with app.app_context():
            p = _perfil(db_session, 'editar')
            assert p.tem_permissao('identidade_visual', 'visualizar')
            assert p.tem_permissao('identidade_visual', 'criar')
            assert p.tem_permissao('identidade_visual', 'editar')
            assert not p.tem_permissao('identidade_visual', 'excluir')

    def test_criar_nao_implica_editar(self, app, db_session):
        with app.app_context():
            p = _perfil(db_session, 'criar')
            assert p.tem_permissao('identidade_visual', 'visualizar')
            assert p.tem_permissao('identidade_visual', 'criar')
            assert not p.tem_permissao('identidade_visual', 'editar')
            assert not p.tem_permissao('identidade_visual', 'excluir')

    def test_visualizar_nao_implica_escrita(self, app, db_session):
        with app.app_context():
            p = _perfil(db_session, 'visualizar')
            assert p.tem_permissao('identidade_visual', 'visualizar')
            assert not p.tem_permissao('identidade_visual', 'criar')
            assert not p.tem_permissao('identidade_visual', 'editar')
            assert not p.tem_permissao('identidade_visual', 'excluir')

    def test_aprovar_concede_visualizar_mas_nao_editar(self, app, db_session):
        with app.app_context():
            p = _perfil(db_session, 'aprovar')
            assert p.tem_permissao('identidade_visual', 'visualizar')
            assert p.tem_permissao('identidade_visual', 'aprovar')
            assert not p.tem_permissao('identidade_visual', 'editar')

    def test_sem_permissao_no_modulo_nega_tudo(self, app, db_session):
        with app.app_context():
            p = _perfil(db_session, 'editar', modulo='outro_modulo')
            assert not p.tem_permissao('identidade_visual', 'visualizar')
            assert not p.tem_permissao('identidade_visual')


class TestHierarquiaHTTP:
    """O acesso full (perfil com excluir) deve conseguir TUDO: criar, editar,
    salvar/anexar e excluir — sem precisar marcar cada ação isoladamente."""

    PAYLOAD = {
        'tipo_local': 'Sala da Cidadania',
        'endereco': 'Rua Teste, 123',
        'bairro': 'Centro',
        'cep': '64000-000',
    }

    def test_full_so_com_excluir_cria_local(self, client, db_session, app, municipio_teresina):
        c = _client_com_acoes(db_session, app, client, 1100, ['excluir'])
        with app.app_context():
            resp = c.post('/identidade-visual/api/criar-local',
                          json={**self.PAYLOAD, 'municipio_id': municipio_teresina.id})
            assert resp.status_code == 200
            assert resp.get_json()['ok'] is True

    def test_criar_only_cria_local(self, client, db_session, app, municipio_teresina):
        c = _client_com_acoes(db_session, app, client, 1101, ['criar'])
        with app.app_context():
            resp = c.post('/identidade-visual/api/criar-local',
                          json={**self.PAYLOAD, 'municipio_id': municipio_teresina.id})
            assert resp.status_code == 200

    def test_visualizar_only_nao_cria_local(self, client, db_session, app, municipio_teresina):
        c = _client_com_acoes(db_session, app, client, 1102, ['visualizar'])
        with app.app_context():
            resp = c.post('/identidade-visual/api/criar-local',
                          json={**self.PAYLOAD, 'municipio_id': municipio_teresina.id})
            assert resp.status_code in (302, 403)
            assert IdentidadeVisualLocal.query.count() == 0

    def test_full_so_com_excluir_edita_local(self, client, db_session, app, municipio_teresina):
        local = IdentidadeVisualLocal(
            cidade='Teresina', municipio_id=municipio_teresina.id,
            tipo_local='Sala da Cidadania', endereco='Rua A', bairro='X', cep='64000-000')
        db_session.add(local)
        db_session.commit()
        lid = local.id
        c = _client_com_acoes(db_session, app, client, 1103, ['excluir'])
        with app.app_context():
            resp = c.post(f'/identidade-visual/api/editar-local/{lid}',
                          json={**self.PAYLOAD, 'municipio_id': municipio_teresina.id,
                                'endereco': 'Rua Nova, 999'})
            assert resp.status_code == 200
            assert resp.get_json()['ok'] is True
