"""Testes da rota /hub (grade de cards expansíveis)."""
import pytest

from tests.hub.conftest import logar


class TestHubRoute:

    def test_sem_login_redireciona(self, client):
        resp = client.get('/hub')
        assert resp.status_code in (302, 401)
        if resp.status_code == 302:
            assert '/auth/login' in resp.headers['Location']

    def test_admin_ve_cards_e_secao_administracao(self, client, novo_usuario):
        logar(client, novo_usuario(3001, is_admin=True))
        resp = client.get('/hub')
        html = resp.get_data(as_text=True)

        assert resp.status_code == 200
        for mid in ('solicitacoes', 'financeiro', 'prestacoes_contratos', 'diarias', 'identidade_visual', 'cgfr', 'usuarios', 'dashboards'):
            assert f'data-id="{mid}"' in html, mid
        assert 'id="adminBlock"' in html
        assert 'Acesso total' in html
        assert 'Aguardando liberação de acesso' not in html
        # o caminho da URL não aparece para o usuário
        assert 'detail-path' not in html

    def test_card_resume_o_modulo_e_nao_mostra_a_timeline(self, client, novo_usuario):
        """O detalhe traz o resumo prático do módulo; o fluxo de etapas saiu."""
        logar(client, novo_usuario(3006, is_admin=True))
        html = client.get('/hub').get_data(as_text=True)

        assert 'detail-flow' not in html and 'flow-dot' not in html
        assert 'detail-features' in html
        assert 'Acompanhamento de NE, NL, PD e OB em um só lugar' in html
        assert 'Saldo da LOA por ação, natureza e fonte' in html

    def test_gsap_vendorizado_sem_cdn(self, client, novo_usuario):
        logar(client, novo_usuario(3002, is_admin=True))
        html = client.get('/hub').get_data(as_text=True)

        for arquivo in ('vendor/gsap/gsap.min.js', 'vendor/gsap/Flip.min.js', 'vendor/gsap/CustomEase.min.js',
                        'js/sgc-motion.js', 'js/hub.js', 'css/components/hub.css'):
            assert arquivo in html, arquivo
        assert 'cdn.jsdelivr.net/npm/gsap' not in html
        assert 'cdnjs.cloudflare.com/ajax/libs/gsap' not in html

    @pytest.mark.parametrize('caminho', [
        '/static/vendor/gsap/gsap.min.js', '/static/vendor/gsap/Flip.min.js', '/static/vendor/gsap/CustomEase.min.js',
        '/static/js/sgc-motion.js', '/static/js/hub.js', '/static/css/components/hub.css', '/static/css/base/motion.css',
    ])
    def test_assets_existem(self, client, caminho):
        resp = client.get(caminho)
        assert resp.status_code == 200
        resp.close()

    def test_perfil_ve_so_o_que_tem_permissao(self, client, novo_usuario):
        logar(client, novo_usuario(3003, permissoes={'diarias': ['criar']}))
        html = client.get('/hub').get_data(as_text=True)

        assert 'data-id="diarias"' in html
        assert 'data-id="solicitacoes"' not in html
        assert 'id="adminBlock"' not in html
        assert 'Visualizar e criar' in html

    def test_sem_permissao_mostra_aguardando_liberacao(self, client, novo_usuario):
        logar(client, novo_usuario(3004))
        html = client.get('/hub').get_data(as_text=True)

        assert 'Aguardando liberação de acesso' in html
        assert 'module-card' not in html

    def test_so_identidade_visual_nao_mostra_aguardando_liberacao(self, client, novo_usuario):
        """Bug do hub antigo: o aviso ignorava identidade_visual e dashboards."""
        logar(client, novo_usuario(3005, permissoes={'identidade_visual': ['visualizar']}))
        html = client.get('/hub').get_data(as_text=True)

        assert 'data-id="identidade_visual"' in html
        assert 'Aguardando liberação de acesso' not in html
