"""
Sincronização unificada — reexecução só dos processos que deram erro.

Ao final da sincronização o modal permite atualizar um processo por vez ou
rodar de novo apenas os que falharam. O backend aceita ``?protocolos=`` nas
fases 1 (download SEI) e 2 (etapas) e restringe o trabalho a esses processos.
"""
import json
import re
from datetime import datetime

import pytest

PROT_A = '00002.010665/2024-97'
PROT_B = '00002.000928/2025-31'
PROT_PAGO = '00002.000111/2025-10'
PROT_SEM_LINK = '00002.000222/2025-20'
URL_DOCS = '/solicitacoes/api/sincronizar-documentos'
URL_ETAPAS = '/solicitacoes/api/atualizar-etapas-sei'


@pytest.fixture()
def processos(db_session):
    from app.models import Solicitacao, Contrato, Etapa

    db_session.add(Etapa(id=1, nome='Criada', alias='criada', ordem=1, cor_hex='#64748B'))
    db_session.add(Etapa(id=6, nome='Pago', alias='pago', ordem=6, cor_hex='#00843D'))
    db_session.add(Contrato(codigo='22026000123', nomeContratado='LIDERANCA LIMPEZA LTDA'))
    for sid, protocolo, etapa, link, id_proc in (
        (1, PROT_A, 1, 'https://sei.pi.gov.br/sei/controlador.php?id_procedimento=1', '1'),
        (2, PROT_B, 1, 'https://sei.pi.gov.br/sei/controlador.php?id_procedimento=2', '2'),
        (3, PROT_PAGO, 6, 'https://sei.pi.gov.br/sei/controlador.php?id_procedimento=3', '3'),
        (4, PROT_SEM_LINK, 1, None, '987654'),
    ):
        db_session.add(Solicitacao(
            id=sid, codigo_contrato='22026000123', id_usuario_solicitante=1, etapa_atual_id=etapa,
            competencia='08/2026', data_solicitacao=datetime(2026, 8, 10),
            protocolo_gerado_sei=protocolo, link_processo_sei=link, id_procedimento_sei=id_proc,
            status_geral='ABERTO',
        ))
    db_session.commit()


@pytest.fixture()
def cli(processos, client, usuario_com_permissoes, logar, monkeypatch):
    from app.solicitacoes.routes import api

    monkeypatch.setattr(api.time, 'sleep', lambda s: None)
    c = logar(usuario_com_permissoes(7401, permissoes=[('solicitacoes', '', 'aprovar')]))
    with c.session_transaction() as sess:
        sess['sei_token'] = 'token-falso'
    return c


@pytest.fixture()
def downloads(monkeypatch):
    """Registra os protocolos baixados; PROT_B e PROT_SEM_LINK falham de novo."""
    from app.solicitacoes.routes import api
    chamados = []

    def _falso(app_obj, protocolo, token, base_url, inline=False):
        chamados.append(protocolo)
        if protocolo in (PROT_B, PROT_SEM_LINK):
            return (False, f'Erro API {protocolo}: 504')
        return (True, f'OK: {protocolo}')

    monkeypatch.setattr(api, 'baixar_documentos_thread', _falso)
    return chamados


def _eventos(resp):
    assert resp.status_code == 200
    corpo = resp.get_data(as_text=True)
    return [json.loads(l[6:]) for l in corpo.splitlines() if l.startswith('data: ')]


# ──────────────────────────────────────────────────────────────────────────────
# Fase 1 filtrada
# ──────────────────────────────────────────────────────────────────────────────

def test_fase1_com_protocolos_baixa_so_os_informados(cli, downloads):
    eventos = _eventos(cli.get(URL_DOCS, query_string={'protocolos': f'{PROT_A},{PROT_B}'}))

    assert sorted(downloads) == sorted([PROT_A, PROT_B])
    ok = [e for e in eventos if e.get('tipo') == 'processo_ok']
    assert [e['protocolo'] for e in ok] == [PROT_A]
    assert ok[0]['link_sei'].startswith('https://')
    alerta = next(e for e in eventos if e.get('tipo') == 'alerta_processo')
    assert alerta['protocolo'] == PROT_B
    assert eventos[-1]['concluido'] is True


def test_fase1_filtrada_informa_protocolos_fora_da_sincronizacao(cli, downloads):
    eventos = _eventos(cli.get(URL_DOCS, query_string={'protocolos': f'{PROT_A},{PROT_PAGO}'}))

    assert downloads == [PROT_A]  # processo já pago não entra
    assert eventos[-1]['ignorados'] == [PROT_PAGO]


def test_fase1_sem_filtro_nao_emite_evento_por_processo_ok(cli, downloads):
    eventos = _eventos(cli.get(URL_DOCS))

    assert sorted(downloads) == sorted([PROT_A, PROT_B, PROT_SEM_LINK])
    assert not any(e.get('tipo') == 'processo_ok' for e in eventos)


def test_fase1_com_protocolos_invalidos_nao_baixa_nada(cli, downloads):
    eventos = _eventos(cli.get(URL_DOCS, query_string={'protocolos': "<script>,';drop"}))

    assert downloads == []
    assert eventos[-1]['concluido'] is True
    assert 'Nenhum protocolo válido' in eventos[-1]['msg']


def test_link_sei_montado_pelo_id_do_procedimento_quando_nao_ha_link(cli, downloads):
    eventos = _eventos(cli.get(URL_DOCS, query_string={'protocolos': PROT_SEM_LINK}))

    alerta = next(e for e in eventos if e.get('tipo') == 'alerta_processo')
    assert alerta['link_sei'] == (
        'https://sei.pi.gov.br/sei/controlador.php?acao=procedimento_trabalhar&id_procedimento=987654'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Fase 2 filtrada
# ──────────────────────────────────────────────────────────────────────────────

def test_fase2_com_protocolos_processa_so_os_informados(cli, monkeypatch):
    from app.solicitacoes.routes import api
    processados = []

    def _falso(app_obj, sid, token, usuario_id, mapa_ordem, inline=False):
        processados.append(sid)
        return PROT_A if sid == 1 else None

    monkeypatch.setattr(api, 'processar_item_sei', _falso)
    eventos = _eventos(cli.get(URL_ETAPAS, query_string={'protocolos': PROT_A}))

    assert processados == [1]
    assert any(e.get('msg') == f'Movimentado: {PROT_A}' for e in eventos)
    assert eventos[-1]['concluido'] is True


def test_fase2_sem_filtro_processa_todos_os_pendentes(cli, monkeypatch):
    from app.solicitacoes.routes import api
    processados = []
    monkeypatch.setattr(api, 'processar_item_sei',
                        lambda app_obj, sid, *a, **k: processados.append(sid))

    _eventos(cli.get(URL_ETAPAS))

    assert sorted(processados) == [1, 2, 4]


# ──────────────────────────────────────────────────────────────────────────────
# Front: reexecução, legado sem protocolo e botões em pílula
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def html_dashboard(db_session, client, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7402, permissoes=[('solicitacoes', '', 'aprovar')]))
    resp = c.get('/solicitacoes/dashboard')
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def _css(nome):
    with open(f'app/static/css/components/{nome}', encoding='utf-8') as f:
        return f.read()


def test_js_reexecuta_processos_com_erro(html_dashboard):
    assert 'function reexecutarProcessos(protocolos)' in html_dashboard
    assert 'function reexecutarTodosComErro()' in html_dashboard
    # a lista de protocolos vai codificada na URL das fases 1 e 2
    assert html_dashboard.count("'?protocolos=' + encodeURIComponent(") >= 2
    assert "dados.tipo === 'processo_ok'" in html_dashboard
    assert 'sync-btn-reexecutar' in html_dashboard
    assert 'sync-btn-atualizar' in html_dashboard


def test_js_extrai_protocolo_da_mensagem_quando_evento_nao_traz(html_dashboard):
    # servidor antigo (sem campo protocolo): ainda assim o alerta fica reexecutável
    assert 'function protocoloDoAlerta(dados)' in html_dashboard


def test_rodape_do_modal_com_botoes_em_pilula(html_dashboard):
    rodape = html_dashboard.split('id="modalSyncSei"', 1)[1].split('id="formRelatorioSync"', 1)[0]
    rodape = rodape.split('class="modal-footer"', 1)[1]
    assert re.search(r'class="sgc-mbtn sgc-mbtn--secondary"[^>]*id="btnRelatorioSync"', rodape)
    assert re.search(r'class="sgc-mbtn sgc-mbtn--secondary"[^>]*id="btnFecharModalSei"', rodape)
    assert rodape.count('class="sgc-mbtn__fill"') == 2
    assert 'function setBotaoFecharSync(' in html_dashboard


def test_css_variante_alerta_e_botoes_de_reexecucao_em_pilula():
    ui = _css('sgc-ui.css')
    assert re.search(r'\.sgc-mbtn--alerta \{', ui)
    modal = _css('sync-modal.css')
    for seletor in ('.sync-btn-reexecutar', '.sync-btn-atualizar'):
        regra = re.search(re.escape(seletor) + r' \{(.*?)\}', modal, re.S).group(1)
        assert 'border-radius: 999px' in regra, seletor
    for estado in ('atualizando', 'resolvido'):
        assert f'.sync-pendencia-item[data-estado="{estado}"]' in modal
