"""
Rebranding da tela Nova solicitação (/solicitacoes/nova) — fluxo guiado do DESIGN.md
(docs/design/solicitacoes/DESIGN.md) e mockups Stitch 3-nova-busca / 3-nova:

- cabeçalho sol-head + assinatura PI + ações em pílula (Voltar, Criar em lote)
- grade sgc-fluxo: 3 passos (Contrato → Dados do pagamento → Unidade SEI) + resumo sticky
- busca de contrato em cartões (compartilhada com o Vincular processo)
- modal de assinatura no padrão modal-sync-sei--compacto

O contrato com crud.py/api.py (ids, names, fluxo POST → modal) não muda.
"""
import json
import re
from pathlib import Path

import pytest

PERM_CRIAR = [('solicitacoes', '', 'criar')]
URL = '/solicitacoes/nova'
RAIZ = Path(__file__).resolve().parents[2]
UNIDADES = [{'id': '110000001', 'sigla': 'SEAD-PI/GFC'}, {'id': '110000002', 'sigla': 'SEAD-PI/SUPARC'}]


@pytest.fixture()
def dados(db_session):
    from app.models import Contrato, TipoPagamento

    db_session.add(TipoPagamento(id=1, nome='Regular'))
    db_session.add(TipoPagamento(id=2, nome='DEA: Indenizatório'))
    db_session.add(Contrato(
        codigo='22026000123', numeroOriginal='045/2025',
        nomeContratado='LIDERANCA SERVICOS DE LIMPEZA LTDA', objeto='Limpeza e conservação',
        numProcesso='00002.001100/2025-10',
    ))
    db_session.commit()


@pytest.fixture()
def cliente(dados, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7301, permissoes=PERM_CRIAR))
    with c.session_transaction() as sess:
        sess['unidades'] = UNIDADES
    return c


def _html(cliente):
    resp = cliente.get(URL)
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def _pagina(html):
    """Conteúdo próprio da tela: do cabeçalho até o fim do <main> + scripts da página."""
    conteudo = html.split('<header class="sol-head">', 1)[1].split('</main>', 1)[0]
    scripts = html.split('js/sgc-fluxo.js', 1)[1]
    return conteudo + scripts


def _tag(html, id_):
    m = re.search(r'<[a-z]+\b[^>]*\bid="%s"[^>]*>' % re.escape(id_), html)
    assert m, f'#{id_} não encontrado'
    return m.group(0)


# ─ acesso
def test_exige_login(client, dados):
    resp = client.get(URL)
    assert resp.status_code == 302
    assert '/auth/login' in resp.headers['Location']


def test_sem_permissao_volta_ao_hub(dados, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7302, permissoes=[('solicitacoes', '', 'visualizar')]))
    resp = c.get(URL)
    assert resp.status_code == 302
    assert resp.headers['Location'].rstrip('/').endswith('/hub')


def test_com_permissao_criar_abre(cliente):
    assert 'Nova solicitação' in _html(cliente)


# ─ cabeçalho
def test_cabecalho_com_titulo_e_assinatura(cliente):
    html = _html(cliente)
    assert 'js/sgc-fluxo.js' in html
    assert '<h3 class="sol-title">Nova solicitação</h3>' in html
    cab = html.split('<header class="sol-head">', 1)[1].split('</header>', 1)[0]
    assert re.search(r'class="sol-signature"[^>]*>\s*(<span></span>\s*){4}</div>', cab)
    assert 'Escolha o contrato e os dados do pagamento para abrir o processo no SEI.' in cab


def test_acoes_do_cabecalho(cliente):
    html = _html(cliente)
    cab = html.split('<header class="sol-head">', 1)[1].split('</header>', 1)[0]
    # voltar deixa de ser pílula ao lado de "Criar em lote": vira link de retorno acima do título
    voltar = re.search(r'<a href="/solicitacoes/dashboard" class="sgc-voltar"[^>]*>.*?</a>', cab, re.S)
    assert voltar and 'Solicitações' in voltar.group(0) and 'bi-arrow-left' in voltar.group(0)
    assert 'aria-label="Voltar para Solicitações"' in voltar.group(0)
    assert cab.index('class="sgc-voltar"') < cab.index('class="sol-title"')
    acoes = cab.split('<div class="sol-head-acoes">', 1)[1]
    assert 'sgc-mbtn__icon--voltar' not in acoes
    lote = re.search(r'<a href="/solicitacoes/nova-lote" class="sgc-mbtn sgc-mbtn--secondary">.*?</a>', acoes, re.S)
    assert lote and 'Criar em lote' in lote.group(0)


def test_tipo_e_unidade_viram_dropdown_estilizado(cliente):
    pagina = _pagina(_html(cliente))
    assert "F.dropdown(byId('id_tipo_pagamento')" in pagina
    assert re.search(r"F\.dropdown\(byId\('unidade_procedimento'\), \{[^}]*busca: true", pagina)


# ─ fluxo guiado
def test_tres_passos_com_estado_inicial(cliente):
    html = _html(cliente)
    passos = re.findall(r'<li class="sync-fase sgc-passo"[^>]*data-estado="(\w+)"[^>]*data-passo="(\d)"', html)
    assert passos == [('andamento', '1'), ('pendente', '2'), ('pendente', '3')]
    for titulo in ('Contrato', 'Dados do pagamento', 'Unidade SEI'):
        assert f'<h2 class="sync-fase-nome sgc-passo-titulo">{titulo}</h2>' in html
    corpos = re.findall(r'<div class="sgc-passo-corpo"([^>]*)>', html)
    assert len(corpos) == 3
    assert 'inert' not in corpos[0]
    assert all('inert' in c for c in corpos[1:])


def test_grade_com_resumo_lateral(cliente):
    html = _html(cliente)
    assert 'class="sgc-fluxo"' in html
    resumo = re.search(r'<aside class="sgc-resumo".*?</aside>', html, re.S)
    assert resumo
    bloco = resumo.group(0)
    for rotulo in ('Contrato', 'Competência', 'Tipo', 'Unidade'):
        assert f'<dt>{rotulo}</dt>' in bloco
    for chave in ('contrato', 'competencia', 'tipo', 'unidade'):
        assert f'data-resumo="{chave}"' in bloco
    for frase in ('Abre o processo de pagamento no SEI', 'Gera o documento de requisição de pagamento',
                  'Você assina com sua senha do SEI'):
        assert frase in bloco
    assert 'aria-live="polite"' in bloco


def test_botao_final_no_resumo(cliente):
    html = _html(cliente)
    botao = _tag(html, 'btn-confirmar')
    assert 'class="sgc-mbtn sgc-mbtn--sucesso"' in botao
    assert 'type="submit"' in botao
    assert 'form="form-solicitacao"' in botao
    assert re.search(r'\sdisabled[\s>]', botao)
    assert 'Criar processo no SEI' in html


def test_ids_e_names_preservados(cliente):
    html = _html(cliente)
    for id_ in ('termo_busca', 'btn-buscar', 'container-resultados', 'tabela-corpo', 'msg-nenhum-resultado',
                'form-solicitacao', 'contrato_selecionado', 'competencia', 'id_tipo_pagamento',
                'unidade_procedimento', 'btn-confirmar', 'modalAssinatura', 'formAssinatura', 'senhaSei',
                'msgErroAssinatura', 'btnConfirmarAssinatura'):
        assert f'id="{id_}"' in html, id_
    form = _tag(html, 'form-solicitacao')
    assert 'method="POST"' in form and 'action="/solicitacoes/nova"' in form
    corpo_form = html.split('id="form-solicitacao"', 1)[1].split('</form>', 1)[0]
    for name in ('contrato_selecionado', 'competencia', 'id_tipo_pagamento', 'unidade_procedimento'):
        assert f'name="{name}"' in corpo_form, name
    assert 'type="hidden"' in _tag(html, 'contrato_selecionado')
    # os cartões de contrato são montados pelo JS compartilhado
    js = (RAIZ / 'app/static/js/sgc-fluxo.js').read_text(encoding='utf-8')
    assert 'name="selecao_contrato_visual"' in js and "'radio_'" in js


def test_botao_pesquisar_branco_com_circulo_azul(cliente):
    # branco como o secundário: só o círculo da lupa é azul (e cresce no hover)
    botao = _tag(_html(cliente), 'btn-buscar')
    assert 'class="sgc-mbtn sgc-mbtn--secondary"' in botao
    css = _css_ui()
    assert '.sgc-mbtn--secondary' not in css  # sem override: vale a base (fundo branco, fill primário)
    base = _regra(css, '.sgc-mbtn')
    assert '--mbtn-bg: var(--color-surface, #fff)' in base and '--mbtn-fill: var(--color-primary, #343990)' in base


def test_busca_aponta_para_api(cliente):
    html = _html(cliente)
    assert 'data-url-busca="/solicitacoes/api/contratos"' in html


def test_opcoes_de_tipo_e_unidade(cliente):
    html = _html(cliente)
    assert '<option value="1">Regular</option>' in html
    assert '<option value="2">DEA: Indenizatório</option>' in html
    assert '<option value="110000001">SEAD-PI/GFC</option>' in html
    assert '<option value="110000002">SEAD-PI/SUPARC</option>' in html


def test_competencia_com_mascara_e_ajuda(cliente):
    html = _html(cliente)
    campo = _tag(html, 'competencia')
    assert 'placeholder="MM/AAAA"' in campo
    assert 'inputmode="numeric"' in campo
    assert 'maxlength="7"' in campo
    assert 'id="competencia-erro"' in html


def test_sem_padroes_antigos(cliente):
    pagina = _pagina(_html(cliente))
    for proibido in ('btn-lg', 'alert alert-', 'card shadow-sm', 'step-badge', 'progress-bar-striped',
                     'alert(', 'jquery', 'style="opacity'):
        assert proibido not in pagina, proibido


# ─ modal de assinatura
def test_modal_de_assinatura_no_padrao_compacto(cliente):
    html = _html(cliente)
    modal = html.split('id="modalAssinatura"', 1)[1]
    abertura = html.split('id="modalAssinatura"', 1)[0].rsplit('<div', 1)[1]
    assert 'modal-sync-sei modal-sync-sei--compacto' in abertura
    assert 'data-bs-backdrop="static"' in modal.split('>', 1)[0]
    assert '<div class="modal-header sync-header"' in modal
    assert 'Pagamentos · Assinatura SEI' in modal
    assert 'Assinar documento' in modal
    assert 'id="assinaturaClipTopo"' in modal
    senha = _tag(html, 'senhaSei')
    assert 'type="password"' in senha
    assert 'autocomplete="current-password"' in senha
    assert 'value=' not in senha
    assert re.search(r'<a href="/solicitacoes/dashboard" class="sgc-mbtn sgc-mbtn--secondary">.*?Assinar depois', modal, re.S)
    assert 'class="sgc-mbtn sgc-mbtn--sucesso"' in _tag(html, 'btnConfirmarAssinatura')


def test_modal_nao_abre_no_get(cliente):
    assert "getOrCreateInstance(document.getElementById('modalAssinatura')).show()" not in _html(cliente)


def test_post_valido_abre_modal_de_assinatura(cliente, monkeypatch):
    from app.solicitacoes.routes import crud

    monkeypatch.setattr(crud, 'gerar_token_sei_admin', lambda: 'token-teste')
    monkeypatch.setattr(crud, 'criar_procedimento_pagamento', lambda token, unidade, dados, competencia, detalhe_erro=None: {
        'IdProcedimento': '999', 'ProcedimentoFormatado': '00002.004512/2026-31',
        'LinkAcesso': 'https://sei.exemplo/proc', 'EspecificacaoGerada': 'Pagamento 08/2026',
    })
    monkeypatch.setattr(crud, 'gerar_documento_pagamento', lambda token, unidade, id_proc, ctx, detalhe_erro=None: {
        'DocumentoFormatado': '0123456',
    })

    resp = cliente.post(URL, data={
        'contrato_selecionado': '22026000123', 'competencia': '08/2026',
        'unidade_procedimento': '110000001', 'id_tipo_pagamento': '1',
    })
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "getOrCreateInstance(document.getElementById('modalAssinatura')).show()" in html
    assert 'protocolo: ' + json.dumps('0123456') in html
    assert 'unidade: ' + json.dumps('110000001') in html
    assert '<span class="sol-mono sgc-doc-numero">0123456</span>' in html


def test_post_sem_contrato_volta_com_erro(cliente):
    resp = cliente.post(URL, data={'competencia': '08/2026', 'unidade_procedimento': '110000001'})
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith(URL)
    with cliente.session_transaction() as sess:
        assert ('danger', 'Preencha todos os campos obrigatórios.') in sess['_flashes']


# ─ assets compartilhados
def _css_ui():
    return (RAIZ / 'app/static/css/components/sgc-ui.css').read_text(encoding='utf-8')


def _js_fluxo():
    return (RAIZ / 'app/static/js/sgc-fluxo.js').read_text(encoding='utf-8')


@pytest.mark.parametrize('seletor', [
    '.sgc-fluxo {', '.sgc-passos {', '.sgc-passo-resumo {', '.sgc-passo-trocar {',
    '.sgc-contrato {', '.sgc-resumo {', '.sgc-input {', '.sgc-campo-erro {', '.sgc-vazio {',
])
def test_css_do_fluxo_no_sgc_ui(seletor):
    assert seletor in _css_ui()


def test_css_resumo_sticky_e_contrato_selecionado():
    css = _css_ui()
    regra = re.search(r'\n\.sgc-resumo \{(.*?)\}', css, re.S).group(1)
    assert 'position: sticky' in regra and 'var(--navbar-height' in regra
    assert re.search(r'\.sgc-contrato:has\(:checked\)', css)
    assert '.sgc-contrato.is-selecionado' in css
    abaixo_992 = css.split('@media (max-width: 991.98px)', 1)[1]
    assert re.search(r'\.sgc-resumo \{[^}]*position: static', abaixo_992)


def test_css_reduced_motion_do_fluxo():
    reduzido = _css_ui().split('@media (prefers-reduced-motion: reduce)', 1)[1]
    assert '.sgc-passo-corpo' in reduzido and '.sgc-contrato' in reduzido


def test_js_do_fluxo_local_e_sem_jquery():
    js = _js_fluxo()
    assert 'http' not in js and 'import ' not in js
    assert 'jQuery' not in js and '$(' not in js and 'alert(' not in js
    assert 'window.SGCFluxo' in js
    for api in ('escapeHtml', 'setPasso', 'buscaContratos', 'mascaraCompetencia', 'mascaraMoeda', 'fluxo',
                'dropdown', 'resumoPares'):
        assert re.search(r'\b%s\b' % api, js), api


def test_js_resumo_do_passo_com_rotulo_e_dado():
    js = _js_fluxo()
    assert 'sgc-par-rotulo' in js and 'sgc-par-valor' in js
    for rotulo in ("'Código'", "'Nº'", "'Contratado'", "'Processo'"):
        assert rotulo in js, rotulo
    pagina = (RAIZ / 'app/templates/solicitacoes/nova.html').read_text(encoding='utf-8')
    for rotulo in ("'Competência'", "'Tipo'", "'Unidade'"):
        assert rotulo in pagina.split('F.fluxo(', 1)[1], rotulo


def test_js_dropdown_acessivel_com_busca():
    js = _js_fluxo()
    for trecho in ('role', 'listbox', 'option', 'aria-expanded', 'aria-activedescendant', 'aria-selected',
                   'ArrowDown', 'ArrowUp', 'Escape', 'Home', 'End', "new Event('change', { bubbles: true })",
                   'sgc-dropdown-busca', 'sgc-dropdown-vazio', 'normalize('):
        assert trecho in js, trecho


@pytest.mark.parametrize('seletor', [
    '.sgc-par {', '.sgc-par-rotulo {', '.sgc-dropdown {', '.sgc-dropdown-gatilho {', '.sgc-dropdown-painel {',
    '.sgc-dropdown-opcao {', '.sgc-dropdown-busca {', '.sgc-voltar {', '.sgc-voltar__icone {',
])
def test_css_novos_componentes(seletor):
    assert seletor in _css_ui()


def test_css_resumo_quebra_linha_e_rotulo_em_destaque():
    css = _css_ui()
    dd = re.search(r'\n\.sgc-resumo-item dd \{(.*?)\}', css, re.S).group(1)
    assert 'nowrap' not in dd and 'ellipsis' not in dd
    assert 'overflow-wrap: anywhere' in dd
    dt = re.search(r'\n\.sgc-resumo-item dt \{(.*?)\}', css, re.S).group(1)
    assert 'font-weight: 600' in dt and 'text-transform: uppercase' in dt


def test_css_reduced_motion_dos_novos_componentes():
    reduzido = _css_ui().split('@media (prefers-reduced-motion: reduce)', 1)[1]
    assert '.sgc-dropdown-painel' in reduzido and '.sgc-voltar__icone' in reduzido


def test_busca_contrato_e_resumo_sao_macros_compartilhadas():
    macros = (RAIZ / 'app/templates/components/sgc_ui.html').read_text(encoding='utf-8')
    for nome in ('busca_contrato(', 'resumo(', 'voltar('):
        assert f'{{% macro {nome}' in macros, nome


def _regra(css, seletor):
    return re.search(r'\n%s \{(.*?)\}' % re.escape(seletor), css, re.S).group(1)


def test_css_passo_recolhido_como_ficha():
    css = _css_ui()
    ficha = _regra(css, '.sgc-passo-resumo')
    assert 'background' in ficha and 'border: 1px solid' in ficha and 'border-radius' in ficha
    par = _regra(css, '.sgc-par')
    assert 'flex-direction: column' in par
    rotulo = _regra(css, '.sgc-par-rotulo')
    assert 'text-transform: uppercase' in rotulo and 'letter-spacing' in rotulo
    assert '\n.sgc-par-detalhe {' in css
    largo = _regra(css, '.sgc-par--largo')
    assert 'border-top' in largo


def test_js_contratado_separa_nome_e_documento():
    js = _js_fluxo()
    assert re.search(r'\bseparaDocumento\b', js)
    assert 'separaDocumento: separaDocumento' in js
    assert "'CNPJ '" in js and "'CPF '" in js
    assert 'sgc-par-detalhe' in js and 'p.detalhe' in js
    pagina = (RAIZ / 'app/templates/solicitacoes/nova.html').read_text(encoding='utf-8')
    assert 'F.separaDocumento(' in pagina


def test_macro_trocar_com_icone_e_nome_acessivel():
    macros = (RAIZ / 'app/templates/components/sgc_ui.html').read_text(encoding='utf-8')
    assert re.search(r'<button type="button" class="sgc-passo-trocar">\s*<i class="bi bi-pencil" aria-hidden="true"></i>\s*Trocar\s*</button>', macros)


@pytest.mark.parametrize('seletor', [
    '.sgc-alerta {', '.sgc-alerta--erro {', '.sgc-alerta--aviso {', '.sgc-alerta--sucesso {', '.sgc-alerta-icone {',
    '.sgc-alerta-titulo {', '.sgc-alerta-dica {', '.sgc-alerta-acao {', '.sgc-alerta-codigo {', '.sgc-alerta-fechar {',
])
def test_css_alerta_da_pagina(seletor):
    assert seletor in _css_ui()


def test_css_link_do_processo():
    css = _css_ui()
    link = _regra(css, '.sgc-processo-link')
    assert 'text-decoration' in link and 'font-variant-numeric: tabular-nums' in link
    assert '.sgc-processo-link:focus-visible' in css


def test_css_alerta_entra_com_movimento_reduzivel():
    css = _css_ui()
    assert re.search(r'\n\.sgc-alerta \{[^}]*animation:', css)
    reduzido = css.split('@media (prefers-reduced-motion: reduce)', 1)[1]
    assert '.sgc-alerta' in reduzido
