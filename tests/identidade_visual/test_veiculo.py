"""Testes da categoria 'Veículo' no módulo Identidade Visual.

Cobre o serviço DETRAN (mapeamento + descarte de dados pessoais) e as rotas de
criação/consulta que dependem dele (com a API externa mockada).
"""
from app.models.identidade_visual import IdentidadeVisualLocal, TIPO_LOCAL_VEICULO
from app.services import detran_service


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


# Resposta real (reduzida) do endpoint consulta-veiculo-local, com PII incluída.
_PAYLOAD_DETRAN = {
    'status': 'OK',
    'data': {
        'placa': 'RSK6J04',
        'tipoVeiculo': {'id': 6, 'descricao': 'AUTOMOVEL', 'descricaoCrv': 'AUTOMOVEL'},
        'marcaModelo': {'id': 100189, 'descricao': 'HYUNDAI/CRETA20A ULTIMTE '},
        'cor': {'id': 5, 'descricao': None, 'descricaoCrv': 'CINZA'},
        'nomePossuidor': 'FULANO DE TAL',
        'documentoPossuidor': '99202743304',
        'logradouroProprietario': 'Rua X',
    },
}


def _fake_consultar(placa):
    """Substitui a chamada externa nas rotas — devolve dados já normalizados."""
    return {
        'placa': detran_service.normalizar_placa(placa),
        'tipo_veiculo': 'AUTOMOVEL',
        'marca': 'HYUNDAI',
        'modelo': 'CRETA20A ULTIMTE',
        'cor': 'CINZA',
    }


def _fake_consultar_variado(placa):
    """Devolve dados diferentes por placa — para exercitar os filtros."""
    p = detran_service.normalizar_placa(placa)
    if p == 'ABC1D23':
        return {'placa': p, 'tipo_veiculo': 'MOTOCICLETA',
                'marca': 'HONDA', 'modelo': 'CG160', 'cor': 'PRETA'}
    return {'placa': p, 'tipo_veiculo': 'AUTOMOVEL',
            'marca': 'HYUNDAI', 'modelo': 'CRETA', 'cor': 'CINZA'}


class TestDetranService:

    def test_normalizar_placa_remove_simbolos_e_maiuscula(self):
        assert detran_service.normalizar_placa('rsk-6j04') == 'RSK6J04'
        assert detran_service.normalizar_placa(' rsk 6j04 ') == 'RSK6J04'

    def test_placa_invalida_levanta_erro(self, app):
        with app.app_context():
            import pytest
            with pytest.raises(detran_service.DetranError):
                detran_service.consultar_veiculo('ABC12')  # curta demais

    def test_consultar_veiculo_mapeia_e_descarta_pii(self, app, monkeypatch):
        monkeypatch.setattr(detran_service.requests, 'get',
                            lambda *a, **k: _FakeResp(_PAYLOAD_DETRAN))
        with app.app_context():
            app.config['DETRAN_API_URL'] = 'https://gateway.example'
            app.config['DETRAN_API_KEY'] = 'chave-teste'
            info = detran_service.consultar_veiculo('rsk-6j04')

        # DETRAN devolve "MARCA/MODELO" numa string única — o serviço divide.
        assert info == {
            'placa': 'RSK6J04',
            'tipo_veiculo': 'AUTOMOVEL',
            'marca': 'HYUNDAI',
            'modelo': 'CRETA20A ULTIMTE',
            'cor': 'CINZA',
        }
        # Garante que dados pessoais do proprietário NÃO vazam.
        assert 'nomePossuidor' not in info
        assert 'documentoPossuidor' not in info

    def test_split_marca_modelo_divide_no_primeiro_slash(self):
        # "CHEV/PRISMA 1.4MT LT" e "HONDA/CG150 FAN ESDI" são casos reais do banco.
        assert detran_service.split_marca_modelo('CHEV/PRISMA 1.4MT LT') == ('CHEV', 'PRISMA 1.4MT LT')
        assert detran_service.split_marca_modelo('HONDA/CG150 FAN ESDI') == ('HONDA', 'CG150 FAN ESDI')
        # Espaços em volta do "/" são descartados.
        assert detran_service.split_marca_modelo(' HYUNDAI / CRETA20A ULTIMTE ') == ('HYUNDAI', 'CRETA20A ULTIMTE')
        # Sem "/" — vira só marca; modelo fica None.
        assert detran_service.split_marca_modelo('FIAT UNO') == ('FIAT UNO', None)
        # Vazio / None → (None, None), nunca lança.
        assert detran_service.split_marca_modelo('') == (None, None)
        assert detran_service.split_marca_modelo(None) == (None, None)


class TestConsultarPlacaEndpoint:

    def test_consultar_placa_ok(self, logged_client, app, monkeypatch):
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar)
        with app.app_context():
            resp = logged_client.get('/identidade-visual/api/consultar-placa?placa=RSK6J04')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['ok'] is True
            assert data['placa'] == 'RSK6J04'
            assert data['marca'] == 'HYUNDAI'
            assert data['modelo'] == 'CRETA20A ULTIMTE'
            assert data['cor'] == 'CINZA'
            assert data['tipo_veiculo'] == 'AUTOMOVEL'


class TestCriarVeiculo:

    def test_criar_veiculo_sucesso(self, logged_client, db_session, app, monkeypatch):
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar)
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'tipo_local': TIPO_LOCAL_VEICULO,
                'placa': 'RSK-6J04',
            })
            assert resp.status_code == 200
            local = db_session.get(IdentidadeVisualLocal, resp.get_json()['id'])
            assert local.is_veiculo
            assert local.placa == 'RSK6J04'
            assert local.tipo_veiculo == 'AUTOMOVEL'
            assert local.marca == 'HYUNDAI'
            assert local.modelo == 'CRETA20A ULTIMTE'
            assert local.cor == 'CINZA'
            # Veículo não tem localização física.
            assert local.cidade is None
            assert local.endereco is None
            assert local.status == 'PENDENTE'

    def test_criar_veiculo_sem_placa_falha(self, logged_client, app, monkeypatch):
        # Não deve nem chamar a API — validação barra antes.
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar)
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'tipo_local': TIPO_LOCAL_VEICULO,
            })
            assert resp.status_code == 400
            assert 'Placa' in resp.get_json()['erro']

    def test_criar_veiculo_nao_exige_cidade_endereco(self, logged_client, app, monkeypatch):
        """Para 'Veículo', município/endereço/bairro/CEP não são obrigatórios."""
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar)
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/criar-local', json={
                'tipo_local': TIPO_LOCAL_VEICULO,
                'placa': 'ABC1D23',
            })
            assert resp.status_code == 200


class TestDashboardAbas:
    """A aba Veículos lista veículos; a aba Fachadas (tabela física) os exclui."""

    def test_veiculo_aparece_na_aba_veiculos_e_nao_nas_fachadas(
            self, logged_client, app, monkeypatch):
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar)
        with app.app_context():
            logged_client.post('/identidade-visual/api/criar-local', json={
                'tipo_local': TIPO_LOCAL_VEICULO,
                'placa': 'RSK6J04',
            })
            html = logged_client.get('/identidade-visual/').get_data(as_text=True)

            # Placa e tabela de veículos presentes.
            assert 'RSK6J04' in html
            assert 'id="tabelaVeiculos"' in html
            assert 'pane-veiculos' in html
            # Como só existe um veículo (nenhum local físico), a tabela de
            # Fachadas deve mostrar o estado vazio.
            assert 'Nenhum registro encontrado' in html

    def test_kpis_gerais_com_breakdown_por_tipo(
            self, logged_client, app, monkeypatch, municipio_teresina):
        """Os KPIs gerais somam fachadas + veículos e têm collapse de detalhamento."""
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar)
        with app.app_context():
            logged_client.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id, 'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua X', 'bairro': 'Centro', 'cep': '64000-000'})
            logged_client.post('/identidade-visual/api/criar-local', json={
                'tipo_local': TIPO_LOCAL_VEICULO, 'placa': 'RSK6J04'})
            html = logged_client.get('/identidade-visual/').get_data(as_text=True)

            # Card geral clicável + collapse de detalhamento presentes.
            assert 'data-bs-target="#bdTotal"' in html
            assert 'id="bdTotal"' in html
            assert 'kpiBreakdowns' in html
            # O detalhamento por tipo referencia os dois tipos existentes.
            assert 'Sala da Cidadania' in html
            assert 'Veículo' in html


class TestVeiculoFiltros:
    """Filtros exclusivos da aba Veículos (v_tipo / v_cor / v_q)."""

    def _criar_dois(self, client):
        for placa in ('RSK6J04', 'ABC1D23'):
            client.post('/identidade-visual/api/criar-local', json={
                'tipo_local': TIPO_LOCAL_VEICULO, 'placa': placa})

    def test_filtra_por_tipo_veiculo(self, logged_client, app, monkeypatch):
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar_variado)
        with app.app_context():
            self._criar_dois(logged_client)
            html = logged_client.get(
                '/identidade-visual/?aba=veiculos&v_tipo=MOTOCICLETA').get_data(as_text=True)
            assert 'ABC1D23' in html       # motocicleta passa
            assert 'RSK6J04' not in html   # automóvel filtrado fora

    def test_busca_por_marca(self, logged_client, app, monkeypatch):
        monkeypatch.setattr('app.identidade_visual.routes.dashboard.consultar_veiculo',
                            _fake_consultar_variado)
        with app.app_context():
            self._criar_dois(logged_client)
            html = logged_client.get(
                '/identidade-visual/?aba=veiculos&v_q=honda').get_data(as_text=True)
            assert 'ABC1D23' in html
            assert 'RSK6J04' not in html
