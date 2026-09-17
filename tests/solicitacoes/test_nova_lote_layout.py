"""
Rebranding da tela Criar em lote (/solicitacoes/nova-lote) no padrão da Nova solicitação:

- cabeçalho sol-head (voltar, título, assinatura PI, ação "Solicitação única")
- grade sgc-fluxo: 3 passos (Contratos → Dados do pagamento → Unidade SEI) + resumo sticky
- contratos em cartões com seleção múltipla (checkbox), "Carregar todos"
- progresso/relatório no modal-sync-sei--compacto (sem alert/confirm nativos nem jQuery)

O contrato com api_criar_lote (payload JSON + SSE) não muda.
"""
import re

import pytest

PERM_CRIAR = [('solicitacoes', '', 'criar')]
URL = '/solicitacoes/nova-lote'
UNIDADES = [{'id': '110000001', 'sigla': 'SEAD-PI/GFC'}]


@pytest.fixture()
def dados(db_session):
    from app.models import TipoPagamento

    db_session.add(TipoPagamento(id=1, nome='Regular'))
    db_session.commit()


@pytest.fixture()
def cliente(dados, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7311, permissoes=PERM_CRIAR))
    with c.session_transaction() as sess:
        sess['unidades'] = UNIDADES
    return c


def _html(cliente):
    resp = cliente.get(URL)
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def _pagina(html):
    conteudo = html.split('<header class="sol-head">', 1)[1].split('</main>', 1)[0]
    scripts = html.split('js/sgc-fluxo.js', 1)[1]
    return conteudo + scripts


# ─ acesso
def test_exige_login(client, dados):
    resp = client.get(URL)
    assert resp.status_code == 302
    assert '/auth/login' in resp.headers['Location']


def test_sem_permissao_volta_ao_hub(dados, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7312, permissoes=[('solicitacoes', '', 'visualizar')]))
    resp = c.get(URL)
    assert resp.status_code == 302
    assert resp.headers['Location'].rstrip('/').endswith('/hub')


# ─ cabeçalho
def test_cabecalho_no_padrao_da_nova(cliente):
    html = _html(cliente)
    cab = html.split('<header class="sol-head">', 1)[1].split('</header>', 1)[0]
    assert '<h3 class="sol-title">Criar em lote</h3>' in cab
    assert re.search(r'class="sol-signature"[^>]*>\s*(<span></span>\s*){4}</div>', cab)
    assert 'aria-label="Voltar para Solicitações"' in cab
    unica = re.search(r'<a href="/solicitacoes/nova" class="sgc-mbtn sgc-mbtn--secondary">.*?</a>', cab, re.S)
    assert unica and 'Solicitação única' in unica.group(0)


# ─ fluxo guiado
def test_tres_passos_com_estado_inicial(cliente):
    html = _html(cliente)
    passos = re.findall(r'<li class="sync-fase sgc-passo"[^>]*data-estado="(\w+)"[^>]*data-passo="(\d)"', html)
    assert passos == [('andamento', '1'), ('pendente', '2'), ('pendente', '3')]
    for titulo in ('Contratos', 'Dados do pagamento', 'Unidade SEI'):
        assert f'<h2 class="sync-fase-nome sgc-passo-titulo">{titulo}</h2>' in html


def test_campos_mantem_ids(cliente):
    html = _html(cliente)
    for id_ in ('competencia', 'id_tipo_pagamento', 'unidade_procedimento', 'termo_busca',
                'btn-buscar', 'btn-selecionar-todos', 'btn-criar-lote'):
        assert f'id="{id_}"' in html
    assert 'SEAD-PI/GFC' in html and 'Regular' in html


def test_resumo_lateral(cliente):
    bloco = re.search(r'<aside class="sgc-resumo".*?</aside>', _html(cliente), re.S).group(0)
    for chave in ('contratos', 'competencia', 'tipo', 'unidade'):
        assert f'data-resumo="{chave}"' in bloco
    assert 'Abre um processo no SEI para cada contrato' in bloco
    assert re.search(r'<button type="button" class="sgc-mbtn sgc-mbtn--sucesso" id="btn-criar-lote"[^>]*disabled', bloco)


def test_js_usa_sgc_fluxo_sem_jquery_nem_dialogos_nativos(cliente):
    html = _html(cliente)
    pagina = _pagina(html)
    assert 'jquery' not in html.lower().split('<header class="sol-head">', 1)[1]
    assert 'F.mascaraCompetencia(' in pagina
    assert "F.dropdown(byId('unidade_procedimento'), { busca: true" in pagina
    assert not re.search(r'\b(alert|confirm)\(', pagina)
    assert '/solicitacoes/api/criar-lote' in pagina
    assert 'vendor/html2pdf/html2pdf.bundle.min.js' in html


def test_modal_de_progresso(cliente):
    html = _html(cliente)
    modal = html.split('id="modalLote"', 1)[1]
    assert 'modal-sync-sei--compacto' in html.split('id="modalLote"', 1)[0][-200:]
    for id_ in ('statusLote', 'barraLote', 'logLote', 'relatorioLote', 'btnIniciarLote', 'btnBaixarPdf'):
        assert f'id="{id_}"' in modal


def test_continuar_branco_com_icone_azul(cliente):
    assert re.search(r'<button type="button" class="sgc-mbtn sgc-mbtn--secondary" id="btn-continuar-contratos"', _html(cliente))


def test_js_trata_recusa_e_stream_sem_fim(cliente):
    pagina = _pagina(_html(cliente))
    # 400 da API: mostra o `erro` devolvido; stream que acaba sem `concluido` não deixa o modal preso
    assert 'if (!resp.ok || !resp.body)' in pagina and 'd.erro' in pagina
    assert 'if (!concluido)' in pagina
    assert 'id_tipo_pagamento: parseInt(tipo.value, 10)' in pagina
