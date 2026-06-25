"""Testes para edição de locais e endereços (Identidade Visual)."""
from app.models.identidade_visual import IdentidadeVisualLocal, MunicipioPiaui


def _criar_local(db_session, municipio=None, **kwargs):
    defaults = {
        'cidade': 'Teresina',
        'tipo_local': 'Sala da Cidadania',
        'endereco': 'Rua Padrão, 1',
        'bairro': 'Centro',
        'cep': '64000-000',
    }
    if municipio:
        defaults['municipio_id'] = municipio.id
        defaults['cidade'] = municipio.nome
    defaults.update(kwargs)
    local = IdentidadeVisualLocal(**defaults)
    db_session.add(local)
    db_session.commit()
    return local


class TestEditarLocal:

    def test_editar_endereco_sucesso(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)

            resp = logged_client.post(f'/identidade-visual/api/editar-local/{local.id}', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua Nova, 456',
                'bairro': 'Jóquei',
                'cep': '64049-000',
            })
            assert resp.status_code == 200
            assert resp.get_json()['ok'] is True

            db_session.expire_all()
            local = db_session.get(IdentidadeVisualLocal, local.id)
            assert local.endereco == 'Rua Nova, 456'
            assert local.bairro == 'Jóquei'
            assert local.cep == '64049-000'

    def test_editar_cidade_e_tipo(self, logged_client, db_session, app, municipio_teresina, municipio_picos):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)

            resp = logged_client.post(f'/identidade-visual/api/editar-local/{local.id}', json={
                'municipio_id': municipio_picos.id,
                'tipo_local': 'Espaço da Cidadania',
                'endereco': 'Av. Principal, 100',
                'bairro': 'Centro',
                'cep': '64600-000',
            })
            assert resp.status_code == 200
            db_session.expire_all()
            local = db_session.get(IdentidadeVisualLocal, local.id)
            assert local.cidade == 'Picos'
            assert local.municipio_id == municipio_picos.id
            assert local.tipo_local == 'Espaço da Cidadania'

    def test_editar_endereco_obrigatorio(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)

            resp = logged_client.post(f'/identidade-visual/api/editar-local/{local.id}', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': '',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Endereço' in resp.get_json()['erro']

    def test_editar_campos_obrigatorios_vazios(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)

            resp = logged_client.post(f'/identidade-visual/api/editar-local/{local.id}', json={
                'municipio_id': None,
                'tipo_local': '',
            })
            assert resp.status_code == 400
            assert 'erro' in resp.get_json()

    def test_editar_tipo_local_invalido(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)

            resp = logged_client.post(f'/identidade-visual/api/editar-local/{local.id}', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Tipo Errado',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Tipo Local' in resp.get_json()['erro']

    def test_editar_local_inexistente(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/editar-local/99999', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 404

    def test_editar_sem_login(self, client, app, db_session, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)

            resp = client.post(f'/identidade-visual/api/editar-local/{local.id}', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua Nova, 456',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code in (302, 401, 403)

    def test_editar_municipio_inexistente(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)

            resp = logged_client.post(f'/identidade-visual/api/editar-local/{local.id}', json={
                'municipio_id': 99999,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Município' in resp.get_json()['erro']


class TestListarArquivosComEndereco:

    def test_retorna_endereco_na_resposta(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(
                db_session,
                municipio=municipio_teresina,
                endereco='Rua das Flores, 789',
                bairro='Centro',
                cep='64000-100',
            )

            resp = logged_client.get(f'/identidade-visual/api/arquivos/{local.id}')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['endereco'] == 'Rua das Flores, 789'
            assert data['bairro'] == 'Centro'
            assert data['cep'] == '64000-100'
            assert data['cidade'] == 'Teresina'
            assert data['municipio_id'] == municipio_teresina.id
            assert data['tipo_local'] == 'Sala da Cidadania'

    def test_retorna_endereco_vazio(self, logged_client, db_session, app):
        with app.app_context():
            local = _criar_local(db_session, endereco=None, bairro=None, cep=None)

            resp = logged_client.get(f'/identidade-visual/api/arquivos/{local.id}')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['endereco'] == ''
            assert data['bairro'] == ''
            assert data['cep'] == ''
