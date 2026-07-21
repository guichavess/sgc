"""Testes para criação de novos locais (Identidade Visual)."""
from app.models.identidade_visual import IdentidadeVisualLocal


class TestCriarLocal:

    def test_criar_local_sucesso(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua Teste, 123',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['ok'] is True
            assert data['id'] is not None

            local = db_session.get(IdentidadeVisualLocal, data['id'])
            assert local.cidade == 'Teresina'
            assert local.municipio_id == municipio_teresina.id
            assert local.tipo_local == 'Sala da Cidadania'
            assert local.endereco == 'Rua Teste, 123'
            assert local.bairro == 'Centro'
            assert local.cep == '64000-000'
            assert local.status == 'PENDENTE'

    def test_criar_local_justo_acesso(self, logged_client, db_session, app, municipio_teresina):
        """'Justo Acesso' é um tipo de local físico válido (como Sala/Espaço da Cidadania)."""
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Justo Acesso',
                'endereco': 'Rua Teste, 456',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['ok'] is True

            local = db_session.get(IdentidadeVisualLocal, data['id'])
            assert local.tipo_local == 'Justo Acesso'
            assert local.cidade == 'Teresina'

    def test_criar_local_cidade_obrigatoria(self, logged_client, app, municipio_teresina):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Cidade' in resp.get_json()['erro']

    def test_criar_local_endereco_obrigatorio(self, logged_client, app, municipio_teresina):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': '',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Endereço' in resp.get_json()['erro']

    def test_criar_local_bairro_obrigatorio(self, logged_client, app, municipio_teresina):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua X',
                'bairro': '',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Bairro' in resp.get_json()['erro']

    def test_criar_local_cep_obrigatorio(self, logged_client, app, municipio_teresina):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '',
            })
            assert resp.status_code == 400
            assert 'CEP' in resp.get_json()['erro']

    def test_criar_local_tipo_local_invalido(self, logged_client, app, municipio_teresina):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Tipo Inventado',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Tipo Local' in resp.get_json()['erro']

    def test_criar_local_municipio_inexistente(self, logged_client, app):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': 99999,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 400
            assert 'Município' in resp.get_json()['erro']

    def test_criar_local_sem_login(self, client, app):
        with app.app_context():
            resp = client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': 1,
                'tipo_local': 'Espaço da Cidadania',
                'endereco': 'Rua X',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code in (302, 401, 403)
