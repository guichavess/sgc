"""
Envio da tela Nova solicitação (POST /solicitacoes/nova) com o SEI simulado.

O POST é montado a partir do formulário renderizado (names e values reais das opções),
como o navegador faria. Cobre:
- sucesso: cria processo + documento, salva a solicitação e abre o modal de assinatura
- falhas do SEI: alerta na própria tela (sgc-alerta, role=alert) com causa e próximo passo,
  sem redirect e com os dados preservados para tentar de novo
- falha depois do processo criado: avisa o protocolo e aponta "Vincular processo"
- mensagens flash da tela saem no alerta da página, não na faixa genérica do topo
"""
import json
import re

import pytest

from tests.solicitacoes.test_nova_layout import PERM_CRIAR, UNIDADES, URL, dados  # noqa: F401

CRUD = 'app.solicitacoes.routes.crud'
PROC = {
    'IdProcedimento': '555', 'ProcedimentoFormatado': '00002.000555/2026-55',
    'LinkAcesso': 'https://sei.pi.gov.br/p/555', 'EspecificacaoGerada': 'PAGAMENTO DE CONTRATO 045/2025',
}


@pytest.fixture()
def cliente(dados, usuario_com_permissoes, logar):  # noqa: F811
    c = logar(usuario_com_permissoes(7311, permissoes=PERM_CRIAR))
    with c.session_transaction() as sess:
        sess['unidades'] = UNIDADES
        sess['sei_token'] = 'token-sessao'
    return c


@pytest.fixture()
def sei(monkeypatch):
    """SEI simulado: registra chamadas; `falha` define o detalhe devolvido ao criar o processo."""
    estado = {'criar': [], 'documento': [], 'falha': None, 'erro_documento': None, 'falha_documento': None}

    def criar(token, unidade_id, dados_contrato, competencia, detalhe_erro=None):
        estado['criar'].append((token, unidade_id, dados_contrato, competencia))
        if estado['falha'] is not None:
            if detalhe_erro is not None:
                detalhe_erro.update(estado['falha'])
            return None
        return dict(PROC)

    def documento(token, unidade_id, id_procedimento, ctx, detalhe_erro=None):
        estado['documento'].append((unidade_id, id_procedimento, ctx))
        if estado['erro_documento']:
            raise estado['erro_documento']
        if estado['falha_documento'] is not None:
            if detalhe_erro is not None:
                detalhe_erro.update(estado['falha_documento'])
            return None
        return {'DocumentoFormatado': '0012345'}

    monkeypatch.setattr(f'{CRUD}.criar_procedimento_pagamento', criar)
    monkeypatch.setattr(f'{CRUD}.gerar_documento_pagamento', documento)
    monkeypatch.setattr(f'{CRUD}.gerar_token_sei_admin', lambda: None)
    return estado


def _form(html):
    return html.split('<form id="form-solicitacao"', 1)[1].split('</form>', 1)[0]


def _post_da_pagina(cliente, competencia='12/2025'):
    """Monta o POST com os names do formulário renderizado e valores de opções reais."""
    form = _form(cliente.get(URL).get_data(as_text=True))
    nomes = set(re.findall(r'\bname="([^"]+)"', form))
    assert {'contrato_selecionado', 'competencia', 'id_tipo_pagamento', 'unidade_procedimento'} <= nomes

    def opcoes(nome):
        select = re.search(r'<select[^>]*name="%s".*?</select>' % nome, form, re.S).group(0)
        return [v for v in re.findall(r'<option value="([^"]*)"', select) if v]

    return {
        'contrato_selecionado': '22026000123',
        'competencia': competencia,
        'id_tipo_pagamento': opcoes('id_tipo_pagamento')[1],
        'unidade_procedimento': opcoes('unidade_procedimento')[0],
    }


def _alerta(html):
    m = re.search(r'<div class="sgc-alerta sgc-alerta--(\w+)"[^>]*>(.*?)</div>\s*<!-- /sgc-alerta -->', html, re.S)
    assert m, 'alerta da página não encontrado'
    return m.group(1), m.group(0)


def _reenvio(html):
    m = re.search(r'<script type="application/json" id="nova-reenvio">(.*?)</script>', html, re.S)
    assert m, 'dados de reenvio não encontrados'
    return json.loads(m.group(1))


def _solicitacoes():
    from app.models import Solicitacao
    return Solicitacao.query.count()


# ─ sucesso
def test_envio_com_sucesso_cria_processo_e_abre_assinatura(cliente, sei):
    resp = cliente.post(URL, data=_post_da_pagina(cliente))
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200
    token, unidade, contrato, competencia = sei['criar'][0]
    assert (token, unidade, competencia) == ('token-sessao', '110000001', '12/2025')
    assert contrato['codigo'] == '22026000123' and contrato['numeroOriginal'] == '045/2025'
    assert sei['documento'][0][:2] == ('110000001', '555')

    from app.models import Solicitacao
    sol = Solicitacao.query.order_by(Solicitacao.id.desc()).first()
    assert sol.protocolo_gerado_sei == '00002.000555/2026-55'
    assert sol.id_tipo_pagamento == 2 and sol.id_caixa_sei == '110000001'

    assert "getOrCreateInstance(document.getElementById('modalAssinatura')).show()" in html
    tipo, alerta = _alerta(html)
    assert tipo == 'sucesso' and 'Processo aberto no SEI' in alerta
    assert LINK_PROCESSO in alerta
    # o modal de assinatura também leva ao processo
    modal = html.split('id="modalAssinatura"', 1)[1].split('id="formAssinatura"', 1)[0]
    assert LINK_PROCESSO in modal


LINK_PROCESSO = ('<a class="sgc-processo-link" href="https://sei.pi.gov.br/p/555" target="_blank" rel="noopener"'
                 ' aria-label="Abrir o processo 00002.000555/2026-55 no SEI (nova aba)">')


def test_processo_sem_link_valido_mostra_so_o_numero(cliente, sei, monkeypatch):
    for link in ('', 'javascript:alert(1)'):
        monkeypatch.setattr(f'{CRUD}.criar_procedimento_pagamento',
                            lambda *a, _l=link, **k: dict(PROC, LinkAcesso=_l))
        html = cliente.post(URL, data=_post_da_pagina(cliente)).get_data(as_text=True)
        alerta = _alerta(html)[1]
        assert '00002.000555/2026-55' in alerta
        assert 'class="sgc-processo-link"' not in html and 'javascript:alert' not in html


# ─ falhas do SEI ao criar o processo
@pytest.mark.parametrize('falha,titulo', [
    ({'status': 401, 'mensagem': ''}, 'Sua sessão no SEI expirou'),
    ({'status': 422, 'mensagem': 'Especificação <b>inválida</b>'}, 'O SEI recusou a abertura do processo'),
    ({'status': None, 'mensagem': ''}, 'O SEI não respondeu'),
])
def test_falha_do_sei_mostra_alerta_na_tela_sem_redirect(cliente, sei, falha, titulo):
    sei['falha'] = falha
    resp = cliente.post(URL, data=_post_da_pagina(cliente))
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200  # re-renderiza, não redireciona
    assert _solicitacoes() == 0
    tipo, alerta = _alerta(html)
    assert tipo == 'erro'
    assert 'role="alert"' in alerta and 'tabindex="-1"' in alerta
    assert titulo in alerta
    assert 'Erro ao criar processo no SEI. Tente novamente.' not in html
    if falha['mensagem']:
        assert 'Especificação &lt;b&gt;inválida&lt;/b&gt;' in alerta  # mensagem do SEI escapada
    assert 'modalAssinatura\')).show()' not in html


def test_falha_do_sei_preserva_os_dados_para_tentar_de_novo(cliente, sei):
    sei['falha'] = {'status': 401, 'mensagem': ''}
    dados_post = _post_da_pagina(cliente)
    html = cliente.post(URL, data=dados_post).get_data(as_text=True)

    reenvio = _reenvio(html)
    assert reenvio['competencia'] == '12/2025'
    assert reenvio['id_tipo_pagamento'] == dados_post['id_tipo_pagamento']
    assert reenvio['unidade_procedimento'] == '110000001'
    assert reenvio['contrato'] == {
        'codigo': '22026000123', 'numeroOriginal': '045/2025',
        'nomeContratado': 'LIDERANCA SERVICOS DE LIMPEZA LTDA', 'objeto': 'Limpeza e conservação',
        'numProcesso': '00002.001100/2025-10',
    }


def test_alerta_de_falha_nao_duplica_na_faixa_do_topo(cliente, sei):
    sei['falha'] = {'status': 503, 'mensagem': ''}
    html = cliente.post(URL, data=_post_da_pagina(cliente)).get_data(as_text=True)
    assert 'class="flash-messages"' not in html
    assert html.count('class="sgc-alerta ') == 1
    # o alerta fica dentro da página, logo abaixo do cabeçalho
    assert html.index('class="sol-head"') < html.index('class="sgc-alerta ') < html.index('class="sgc-fluxo"')


def test_sem_token_sei_mostra_alerta_de_autenticacao(cliente, sei):
    with cliente.session_transaction() as sess:
        sess.pop('sei_token')
    html = cliente.post(URL, data=_post_da_pagina(cliente)).get_data(as_text=True)
    assert sei['criar'] == []
    tipo, alerta = _alerta(html)
    assert tipo == 'erro' and 'Não foi possível autenticar no SEI' in alerta
    assert _reenvio(html)['competencia'] == '12/2025'


def test_falha_depois_do_processo_criado_avisa_protocolo_e_vincular(cliente, sei):
    sei['erro_documento'] = RuntimeError('SEI caiu no meio')
    html = cliente.post(URL, data=_post_da_pagina(cliente)).get_data(as_text=True)

    assert _solicitacoes() == 0
    tipo, alerta = _alerta(html)
    assert tipo == 'erro'
    assert LINK_PROCESSO in alerta
    assert '/solicitacoes/vincular' in alerta
    assert 'SEI caiu no meio' not in html  # detalhe técnico fica no log
    assert 'id="nova-reenvio"' not in html  # não convida a criar um segundo processo


# ─ processo criado, mas o documento de requisição não foi gerado
@pytest.mark.parametrize('falha_documento', [
    {'status': 422, 'mensagem': 'Série não permitida'},
    {'status': None, 'mensagem': ''},
])
def test_documento_nao_gerado_nao_abre_assinatura_vazia(cliente, sei, falha_documento):
    sei['falha_documento'] = falha_documento
    html = cliente.post(URL, data=_post_da_pagina(cliente)).get_data(as_text=True)

    assert "modalAssinatura')).show()" not in html  # sem documento não há o que assinar
    assert 'Insira sua senha para assinar' not in html
    tipo, alerta = _alerta(html)
    assert tipo == 'aviso'
    assert 'Processo criado, mas a requisição de pagamento não foi gerada' in alerta
    assert LINK_PROCESSO in alerta
    assert 'https://sei.pi.gov.br/p/555' in alerta  # abre o processo no SEI para incluir o documento
    if falha_documento['mensagem']:
        assert 'Série não permitida' in alerta
    assert 'id="nova-reenvio"' not in html  # não convida a abrir um segundo processo
    # a solicitação fica registrada: o processo existe no SEI
    from app.models import Solicitacao
    assert Solicitacao.query.filter_by(protocolo_gerado_sei='00002.000555/2026-55').count() == 1


def test_documento_sem_numero_tambem_avisa(cliente, sei, monkeypatch):
    monkeypatch.setattr(f'{CRUD}.gerar_documento_pagamento', lambda *a, **k: {'IdDocumento': '77'})
    html = cliente.post(URL, data=_post_da_pagina(cliente)).get_data(as_text=True)
    assert "modalAssinatura')).show()" not in html
    assert _alerta(html)[0] == 'aviso'


# ─ validações que redirecionam: flash sai no alerta da página
def test_campos_faltando_mostram_alerta_da_pagina(cliente, sei):
    resp = cliente.post(URL, data={'contrato_selecionado': '22026000123'}, follow_redirects=True)
    html = resp.get_data(as_text=True)
    assert sei['criar'] == []
    tipo, alerta = _alerta(html)
    assert tipo == 'erro' and 'Preencha todos os campos obrigatórios.' in alerta
    assert 'class="flash-messages"' not in html


def test_contrato_inexistente_mostra_alerta_da_pagina(cliente, sei):
    dados_post = dict(_post_da_pagina(cliente), contrato_selecionado='99999999')
    html = cliente.post(URL, data=dados_post, follow_redirects=True).get_data(as_text=True)
    assert 'Contrato não encontrado.' in _alerta(html)[1]


def test_get_sem_mensagens_nao_tem_alerta(cliente):
    html = cliente.get(URL).get_data(as_text=True)
    assert 'class="sgc-alerta ' not in html and 'id="nova-reenvio"' not in html


# ─ a página traz o alerta para a vista e retoma o preenchimento
def test_script_da_pagina_foca_alerta_e_reaplica_dados():
    from tests.solicitacoes.test_nova_layout import RAIZ
    pagina = (RAIZ / 'app/templates/solicitacoes/nova.html').read_text(encoding='utf-8')
    js = (RAIZ / 'app/static/js/sgc-fluxo.js').read_text(encoding='utf-8')
    assert 'F.alerta(' in pagina and 'nova-reenvio' in pagina
    corpo = js.split('function alerta(', 1)[1].split('\n  }\n', 1)[0]
    assert 'scrollIntoView' in corpo and 'focus(' in corpo and 'preventScroll' in corpo


def test_outras_telas_continuam_com_flash_no_topo(app):
    fonte = (app.jinja_loader.searchpath[0] + '/base_layout.html')
    base = open(fonte, encoding='utf-8').read()
    assert re.search(r"\{% block flash_messages %\}\s*\{% include 'components/flash_messages.html' %\}\s*\{% endblock %\}", base)
