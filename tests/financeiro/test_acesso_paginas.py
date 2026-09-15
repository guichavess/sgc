"""Acesso às páginas do Financeiro: entrada `/financeiro/`, bloqueio por página,
menu lateral e botão "Atualizar SIAFE" (alta gestão, sem depender do nome)."""
from pathlib import Path

import pytest

ANTIGAS = [('financeiro', p, 'criar') for p in ('insercao_ne', 'diarias', 'fornecedores', 'execucoes')]
SO_FUNDO = [('financeiro', 'fundo_rotativo', 'visualizar')]

HREF = {
    'orcamento': 'href="/financeiro/orcamentaria"',
    'insercao_ne': 'href="/financeiro/pendencias_ne"',
    'diarias': 'href="/financeiro/diarias"',
    'fornecedores': 'href="/financeiro/fornecedores"',
    'execucoes': 'href="/financeiro/execucoes"',
    'fundo_rotativo': 'href="/financeiro/fundo-rotativo/dashboard"',
    'planejamento': 'href="/financeiro/planejamento/relatorio"',
}


def _vai_para_hub(resp):
    return resp.status_code == 302 and resp.location.endswith('/hub')


class TestSoFundoRotativo:

    def test_entrada_leva_direto_ao_fundo_rotativo(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3401, SO_FUNDO))
        resp = client.get('/financeiro/')
        assert resp.status_code == 302
        assert resp.location.endswith('/financeiro/fundo-rotativo/dashboard')

    @pytest.mark.parametrize('url', [
        '/financeiro/pendencias_ne', '/financeiro/diarias', '/financeiro/fornecedores',
        '/financeiro/execucoes', '/financeiro/orcamentaria', '/financeiro/planejamento/relatorio',
    ])
    def test_outras_paginas_voltam_ao_hub(self, client, usuario_com_permissoes, logar, url):
        logar(usuario_com_permissoes(3402, SO_FUNDO))
        assert _vai_para_hub(client.get(url))

    def test_menu_mostra_so_o_fundo_rotativo(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3403, SO_FUNDO))
        resp = client.get('/financeiro/fundo-rotativo/dashboard')
        assert resp.status_code == 200
        html = resp.data.decode('utf-8')
        assert HREF['fundo_rotativo'] in html
        for pagina in ('orcamento', 'insercao_ne', 'diarias', 'fornecedores', 'execucoes', 'planejamento'):
            assert HREF[pagina] not in html, pagina

    def test_visualizar_nao_libera_sincronizar(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3404, SO_FUNDO))
        assert _vai_para_hub(client.post('/financeiro/fundo-rotativo/saldo/sincronizar'))


class TestPerfilFinanceiroAntigo:
    """As 4 páginas que a permissão `financeiro` liberava antes."""

    def test_entrada_continua_em_inserir_nes(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3410, ANTIGAS))
        resp = client.get('/financeiro/')
        assert resp.status_code == 302 and resp.location.endswith('/financeiro/pendencias_ne')

    @pytest.mark.parametrize('url', [
        '/financeiro/pendencias_ne', '/financeiro/diarias', '/financeiro/fornecedores', '/financeiro/execucoes',
    ])
    def test_paginas_abrem(self, client, usuario_com_permissoes, logar, url):
        logar(usuario_com_permissoes(3411, ANTIGAS))
        assert client.get(url).status_code == 200

    @pytest.mark.parametrize('url', [
        '/financeiro/orcamentaria', '/financeiro/planejamento/relatorio', '/financeiro/fundo-rotativo/dashboard',
    ])
    def test_nao_abre_orcamento_planejamento_nem_fundo(self, client, usuario_com_permissoes, logar, url):
        logar(usuario_com_permissoes(3412, ANTIGAS))
        assert _vai_para_hub(client.get(url))

    def test_menu_ganha_fornecedores_e_execucoes(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3413, ANTIGAS))
        html = client.get('/financeiro/pendencias_ne').data.decode('utf-8')
        for pagina in ('insercao_ne', 'diarias', 'fornecedores', 'execucoes'):
            assert HREF[pagina] in html, pagina
        for pagina in ('orcamento', 'fundo_rotativo', 'planejamento'):
            assert HREF[pagina] not in html, pagina

    def test_botoes_de_criar_aparecem(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3414, ANTIGAS))
        html = client.get('/financeiro/fornecedores').data.decode('utf-8')
        assert 'Novo Fornecedor' in html


class TestAltaGestao:

    def test_entrada_em_orcamento(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3420, is_alta_gestao=True))
        resp = client.get('/financeiro/')
        assert resp.status_code == 302 and resp.location.endswith('/financeiro/orcamentaria')

    @pytest.mark.parametrize('url', ['/financeiro/orcamentaria', '/financeiro/planejamento/relatorio'])
    def test_orcamento_e_planejamento_abrem(self, client, usuario_com_permissoes, logar, url):
        logar(usuario_com_permissoes(3421, is_alta_gestao=True))
        assert client.get(url).status_code == 200

    def test_menu_sem_nome_no_template(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3422, is_alta_gestao=True))
        html = client.get('/financeiro/planejamento/relatorio').data.decode('utf-8')
        assert HREF['orcamento'] in html and HREF['planejamento'] in html
        assert HREF['insercao_ne'] not in html

    def test_homonimo_nao_entra(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3423, ANTIGAS, nome='JOAO PEDRO ALEXANDRE'))
        assert _vai_para_hub(client.get('/financeiro/orcamentaria'))
        assert _vai_para_hub(client.get('/financeiro/planejamento/relatorio'))
        assert client.get('/financeiro/').location.endswith('/financeiro/pendencias_ne')

    def test_sem_nenhuma_pagina_volta_ao_hub(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3424, [('solicitacoes', '', 'visualizar')]))
        assert _vai_para_hub(client.get('/financeiro/'))


class TestAtualizarSiafe:

    def test_alta_gestao_consulta_status(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3430, is_alta_gestao=True))
        assert client.get('/api/atualizar-siafe/status').status_code == 200

    @pytest.mark.parametrize('campos', [
        {'is_admin': True},
        {'nome': 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA'},
    ], ids=['admin_sem_marcacao', 'homonimo'])
    def test_sem_marcacao_recebe_403(self, client, usuario_com_permissoes, logar, campos):
        logar(usuario_com_permissoes(3431, **campos))
        assert client.get('/api/atualizar-siafe/status').status_code == 403
        assert client.post('/api/atualizar-siafe', json={}).status_code == 403

    def test_botao_so_para_alta_gestao(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3432, is_alta_gestao=True))
        assert 'id="btn-atualizar-siafe"' in client.get('/hub').data.decode('utf-8')

    def test_botao_nao_aparece_para_homonimo(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3433, [('solicitacoes', '', 'visualizar')], nome='PEDRO ALEXANDRE'))
        assert 'btn-atualizar-siafe' not in client.get('/hub').data.decode('utf-8')


def test_templates_sem_uso_foram_removidos():
    templates = Path(__file__).resolve().parents[2] / 'app' / 'templates'
    for nome in ('financeiro/navbar_financeiro.html', 'components/navbar.html', 'financeiro/dashboard.html'):
        assert not (templates / nome).exists(), nome
