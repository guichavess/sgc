"""
Assinatura PI (listra colorida sob o título): 4 cores do logo do Governo do
Piauí — verde, vermelho, amarelo, azul — e mais grossa. Vale para o título da
tela de Solicitações, o topo do modal de sincronização e o Hub.
"""
import re
from pathlib import Path

import pytest

from tests.hub.conftest import criar_usuario, logar as logar_hub

RAIZ = Path(__file__).resolve().parents[1]
CORES = ('--pi-green', '--pi-red', '--pi-yellow', '--pi-blue')
QUATRO_FAIXAS = r'\s*<span></span>\s*<span></span>\s*<span></span>\s*<span></span>\s*</div>'


def _css(nome):
    return (RAIZ / 'app/static/css/components' / nome).read_text(encoding='utf-8')


def _regra(css, seletor):
    return re.search(re.escape(seletor) + r' \{(.*?)\}', css, re.S).group(1)


@pytest.mark.parametrize('arquivo, prefixo', [
    ('sgc-ui.css', '.sol-signature'),
    ('hub.css', '.hub-signature'),
])
def test_listra_com_quatro_cores_na_ordem_do_logo(arquivo, prefixo):
    css = _css(arquivo)
    assert '--pi-red: #e4411b' in css
    for i, cor in enumerate(CORES, 1):
        assert re.search(re.escape(f'{prefixo} span:nth-child({i})') + r' \{ background: var\(' + cor + r'\); \}', css), (arquivo, cor)


@pytest.mark.parametrize('arquivo, seletor', [
    ('sgc-ui.css', '.sol-signature'),
    ('hub.css', '.hub-signature'),
])
def test_listra_mais_grossa(arquivo, seletor):
    regra = _regra(_css(arquivo), seletor)
    assert 'height: 6px' in regra
    assert 'width: 96px' in regra


def test_listra_do_modal_de_sincronizacao():
    regra = _regra(_css('sync-modal.css'), '.sync-assinatura')
    assert 'height: 6px' in regra
    for cor in ('#00843d', '#e4411b', '#f6c200', '#0b3d91'):
        assert cor in regra, cor


def test_dashboard_renderiza_quatro_faixas(client, db_session, usuario_com_permissoes, logar):
    cli = logar(usuario_com_permissoes(7501, permissoes=[('solicitacoes', '', 'visualizar')]))
    html = cli.get('/solicitacoes/dashboard').get_data(as_text=True)
    assert re.search(r'class="sol-signature"[^>]*>' + QUATRO_FAIXAS, html)


def test_hub_renderiza_quatro_faixas(client, db_session):
    logar_hub(client, criar_usuario(db_session, 7502, is_admin=True))
    html = client.get('/hub').get_data(as_text=True)
    assert re.search(r'class="hub-signature" id="hubSignature"[^>]*>' + QUATRO_FAIXAS, html)
