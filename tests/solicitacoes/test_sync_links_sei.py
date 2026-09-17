"""
Sincronização unificada — todo processo com erro vira link para o SEI.

Processos sem link nem id do procedimento gravados na solicitação apareciam no
modal só como texto. Agora o link vem, nesta ordem, de: link gravado → id do
procedimento → IdProcedimento dos documentos já baixados → consulta ao SEI
(``POST /solicitacoes/api/links-sei``), que também grava o que encontrou.
"""
import json
from datetime import datetime

import pytest

PROT_COM_LINK = '00002.000101/2026-11'
PROT_DOCS = '00002.000202/2026-22'      # sem link/id, mas com documentos já baixados
PROT_SEI = '00002.000303/2026-33'       # sem nada no banco: só o SEI sabe
PROT_FORA = '00002.000404/2026-44'      # não é de nenhuma solicitação
LINK_GRAVADO = 'https://sei.pi.gov.br/sei/controlador.php?acao=procedimento_trabalhar&id_procedimento=101'
URL_PROC = 'https://sei.pi.gov.br/sei/controlador.php?acao=procedimento_trabalhar&id_procedimento={}'
URL_DOCS = '/solicitacoes/api/sincronizar-documentos'
URL_LINKS = '/solicitacoes/api/links-sei'


@pytest.fixture()
def processos(db_session):
    from app.models import Solicitacao, Contrato, Etapa, SeiMovimentacao

    db_session.add(Etapa(id=1, nome='Criada', alias='criada', ordem=1, cor_hex='#64748B'))
    db_session.add(Contrato(codigo='22026000123', nomeContratado='LIDERANCA LIMPEZA LTDA'))
    for sid, protocolo, link in ((1, PROT_COM_LINK, LINK_GRAVADO), (2, PROT_DOCS, None), (3, PROT_SEI, '')):
        db_session.add(Solicitacao(
            id=sid, codigo_contrato='22026000123', id_usuario_solicitante=1, etapa_atual_id=1,
            competencia='08/2026', data_solicitacao=datetime(2026, 8, 10),
            protocolo_gerado_sei=protocolo, link_processo_sei=link, id_procedimento_sei=None,
            status_geral='ABERTO',
        ))
    db_session.add(SeiMovimentacao(id_documento='9001', protocolo_procedimento=PROT_DOCS, id_procedimento='202202'))
    db_session.commit()


@pytest.fixture()
def consultas_sei(monkeypatch):
    from app.solicitacoes.routes import api
    chamados = []

    def _falso(token, protocolo, timeout=120):
        chamados.append(protocolo)
        if protocolo == PROT_SEI:
            return {'sucesso': True, 'id_procedimento': '303303', 'link_acesso': URL_PROC.format('303303') + '&infra_hash=x'}
        return {'sucesso': False, 'erro': 'HTTP 404'}

    monkeypatch.setattr(api, 'consultar_procedimento_sei', _falso)
    monkeypatch.setattr(api, 'gerar_token_sei_admin', lambda *a, **k: None)
    return chamados


@pytest.fixture()
def cli(processos, client, usuario_com_permissoes, logar, monkeypatch):
    from app.solicitacoes.routes import api

    monkeypatch.setattr(api.time, 'sleep', lambda s: None)
    monkeypatch.setattr(api, 'baixar_documentos_thread',
                        lambda app_obj, protocolo, token, base_url, inline=False: (False, f'Erro API {protocolo}: 504'))
    c = logar(usuario_com_permissoes(7601, permissoes=[('solicitacoes', '', 'aprovar')]))
    with c.session_transaction() as sess:
        sess['sei_token'] = 'token-falso'
    return c


def _eventos(resp):
    assert resp.status_code == 200
    return [json.loads(l[6:]) for l in resp.get_data(as_text=True).splitlines() if l.startswith('data: ')]


# ──────────────────────────────────────────────────────────────────────────────
# Fase 1: link pelos documentos já baixados
# ──────────────────────────────────────────────────────────────────────────────

def test_alerta_usa_id_procedimento_dos_documentos_baixados(cli):
    alertas = {e['protocolo']: e['link_sei'] for e in _eventos(cli.get(URL_DOCS)) if e.get('tipo') == 'alerta_processo'}

    assert alertas[PROT_COM_LINK] == LINK_GRAVADO
    assert alertas[PROT_DOCS] == URL_PROC.format('202202')
    assert alertas[PROT_SEI] == ''  # o modal completa depois via /api/links-sei


# ──────────────────────────────────────────────────────────────────────────────
# POST /api/links-sei
# ──────────────────────────────────────────────────────────────────────────────

def test_links_sei_completa_pelo_banco_e_pelo_sei(cli, consultas_sei):
    resp = cli.post(URL_LINKS, json={'protocolos': [PROT_COM_LINK, PROT_DOCS, PROT_SEI, PROT_FORA]})

    assert resp.status_code == 200
    links = resp.get_json()['links']
    assert links[PROT_COM_LINK] == LINK_GRAVADO
    assert links[PROT_DOCS] == URL_PROC.format('202202')
    assert links[PROT_SEI].startswith(URL_PROC.format('303303'))
    assert PROT_FORA not in links
    # só consulta o SEI para processo de solicitação que continuou sem link
    assert consultas_sei == [PROT_SEI]


def test_links_sei_grava_o_que_o_sei_informou(cli, consultas_sei, db_session):
    from app.models import Solicitacao

    cli.post(URL_LINKS, json={'protocolos': [PROT_SEI]})

    sol = db_session.get(Solicitacao, 3)
    db_session.refresh(sol)
    assert sol.id_procedimento_sei == '303303'
    assert sol.link_processo_sei.startswith(URL_PROC.format('303303'))


def test_links_sei_sem_token_responde_so_com_o_banco(cli, consultas_sei):
    with cli.session_transaction() as sess:
        sess.pop('sei_token', None)

    links = cli.post(URL_LINKS, json={'protocolos': [PROT_DOCS, PROT_SEI]}).get_json()['links']

    assert links == {PROT_DOCS: URL_PROC.format('202202')}
    assert consultas_sei == []


def test_links_sei_ignora_protocolos_invalidos(cli, consultas_sei):
    resp = cli.post(URL_LINKS, json={'protocolos': ["<script>", "';drop", 42]})
    assert resp.status_code == 200
    assert resp.get_json()['links'] == {}
    assert consultas_sei == []


def test_links_sei_exige_lista(cli, consultas_sei):
    assert cli.post(URL_LINKS, json={'protocolos': PROT_SEI}).status_code == 400


def test_links_sei_exige_permissao(processos, client, usuario_com_permissoes, logar, consultas_sei):
    sem_aprovar = logar(usuario_com_permissoes(7602, permissoes=[('solicitacoes', '', 'visualizar')]))
    resp = sem_aprovar.post(URL_LINKS, json={'protocolos': [PROT_SEI]})
    assert resp.status_code != 200
    assert consultas_sei == []


def test_links_sei_exige_login(client):
    assert client.post(URL_LINKS, json={'protocolos': [PROT_SEI]}).status_code in (302, 401)


# ──────────────────────────────────────────────────────────────────────────────
# Modal: troca o protocolo sem link pelo link assim que a resposta chega
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def html_dashboard(client, db_session, usuario_com_permissoes, logar):
    cli = logar(usuario_com_permissoes(7603, permissoes=[('solicitacoes', '', 'aprovar')]))
    return cli.get('/solicitacoes/dashboard').get_data(as_text=True)


def test_modal_completa_links_que_faltaram(html_dashboard):
    assert 'var URL_LINKS_SEI = "/solicitacoes/api/links-sei"' in html_dashboard
    assert 'function completarLinksSei()' in html_dashboard
    assert 'function aplicarLinksSei(links)' in html_dashboard
    # protocolo sem link fica marcado para ser trocado depois
    assert '<span class="sync-protocolo" data-protocolo="\' + escapeAttr(protocolo) + \'">' in html_dashboard
    corpo_final = html_dashboard.split('function finalizarTudo()', 1)[1].split('function setBotaoFecharConcluir', 1)[0]
    assert 'completarLinksSei();' in corpo_final
    corpo_reexec = html_dashboard.split('function concluirReexecucao(', 1)[1].split('function bloquearAcoesSync', 1)[0]
    assert 'completarLinksSei();' in corpo_reexec
