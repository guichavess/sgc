"""
Padrão visual compartilhado (components/sgc-ui.css + components/sgc_ui.html).

Os componentes aprovados no dashboard de Solicitações (cabeçalho sol-head,
assinatura PI, botões sgc-mbtn, tags sol-tag) passam a ser globais: vivem em
sgc-ui.css, importado por main.css, sem cópia em CSS de tela.
"""
import re
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
CSS = RAIZ / 'app/static/css'


def _ler(caminho):
    return (CSS / caminho).read_text(encoding='utf-8')


def test_main_css_importa_sgc_ui_depois_do_sync_modal():
    main = _ler('main.css')
    assert "@import 'components/sgc-ui.css';" in main
    assert main.index("components/sync-modal.css") < main.index("components/sgc-ui.css")


@pytest.mark.parametrize('seletor', [
    ':root {', '.sol-num {', '.sol-head {', '.sol-title {', '.sol-subtitle {',
    '.sol-signature {', '.sol-head-acoes {', '.sgc-mbtn {', '.sgc-mbtn__fill {',
    '.sgc-mbtn--sucesso {', '.sol-mono {', '.sol-protocolo {', '.sol-tag {',
])
def test_componente_mora_no_sgc_ui_e_nao_no_css_do_dashboard(seletor):
    assert seletor in _ler('components/sgc-ui.css')
    assert f'\n{seletor}' not in _ler('components/solicitacoes-dashboard.css')


def test_dashboard_mantem_so_o_que_e_da_tela():
    dash = _ler('components/solicitacoes-dashboard.css')
    for seletor in ('.sol-sync-status {', '.sol-filtro-chip {', '.sol-tabela tbody tr.sol-row'):
        assert seletor in dash


def test_reduced_motion_dos_componentes_globais():
    reduzido = _ler('components/sgc-ui.css').split('@media (prefers-reduced-motion: reduce)', 1)[1]
    assert '.sol-signature { animation: none; }' in reduzido
    assert '--mbtn-icon-giro: none' in reduzido


def test_gesto_voltar_desliza_para_a_esquerda():
    regra = re.search(r'\.sgc-mbtn__icon--voltar \{(.*?)\}', _ler('components/sgc-ui.css'), re.S).group(1)
    assert 'translateX(-' in regra


def test_modal_de_processo_compacto():
    modal = _ler('components/sync-modal.css')
    regra = re.search(r'\.modal-sync-sei--compacto \.modal-dialog \{(.*?)\}', modal, re.S).group(1)
    assert 'max-width' in regra
    conteudo = re.search(r'\.modal-sync-sei--compacto \.modal-content \{(.*?)\}', modal, re.S).group(1)
    assert 'height: auto' in conteudo


def test_macros_do_padrao():
    macros = (RAIZ / 'app/templates/components/sgc_ui.html').read_text(encoding='utf-8')
    for nome in ('assinatura(', 'mbtn(', 'ampulheta(', 'passo('):
        assert f'{{% macro {nome}' in macros, nome


def test_macros_renderizam(app):
    with app.test_request_context():
        from flask import render_template_string
        html = render_template_string(
            "{% import 'components/sgc_ui.html' as ui %}"
            "{{ ui.assinatura() }}"
            "{{ ui.mbtn('primary', 'bi-link-45deg', 'Vincular', gesto='conectar', attrs={'id': 'x', 'disabled': true}) }}"
            "{{ ui.mbtn('secondary', 'bi-arrow-left', 'Voltar', gesto='voltar', href='/v') }}"
            "{{ ui.ampulheta('abc') }}"
            "{% call ui.passo(1, 'Contrato', 'andamento', id='p1') %}corpo{% endcall %}"
        )
    assert re.search(r'class="sol-signature"[^>]*>\s*<span></span>\s*<span></span>\s*<span></span>\s*<span></span>\s*</div>', html)
    assert re.search(r'<button type="button" class="sgc-mbtn sgc-mbtn--primary" id="x" disabled>', html)
    assert 'class="bi bi-link-45deg sgc-mbtn__icon sgc-mbtn__icon--conectar"' in html
    assert re.search(r'<a href="/v" class="sgc-mbtn sgc-mbtn--secondary">', html)
    assert 'id="abcClipTopo"' in html and 'url(#abcClipBase)' in html
    assert re.search(r'<li class="sync-fase sgc-passo" id="p1" data-estado="andamento"', html)
    assert '<span class="sync-fase-estado">Em andamento</span>' in html
    assert 'corpo' in html
