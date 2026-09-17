"""
Sincronização unificada — Fase 1 (download de documentos SEI).

Cada alerta de erro de um processo precisa levar o protocolo e o link do SEI
para o modal exibir o protocolo como link clicável, e o modal ganhou um
visual mais cuidadoso para o painel de pendências.
"""
import json
import re
from datetime import datetime

import pytest

PROTOCOLO_ERRO = '00002.004512/2026-31'
PROTOCOLO_OK = '00002.003984/2026-12'
PROTOCOLO_EXC = '00002.007777/2026-40'
LINK_ERRO = 'https://sei.pi.gov.br/sei/controlador.php?acao=procedimento_trabalhar&id_procedimento=111'
LINK_EXC = 'https://sei.pi.gov.br/sei/controlador.php?acao=procedimento_trabalhar&id_procedimento=333'
URL_SYNC_DOCS = '/solicitacoes/api/sincronizar-documentos'


@pytest.fixture()
def processos(db_session):
    from app.models import Solicitacao, Contrato, Etapa

    db_session.add(Etapa(id=1, nome='Criada', alias='criada', ordem=1, cor_hex='#64748B'))
    db_session.add(Contrato(codigo='22026000123', nomeContratado='LIDERANCA LIMPEZA LTDA'))
    for sid, protocolo, link in (
        (1, PROTOCOLO_ERRO, LINK_ERRO),
        (2, PROTOCOLO_OK, 'https://sei.pi.gov.br/sei/controlador.php?id_procedimento=222'),
        (3, PROTOCOLO_EXC, LINK_EXC),
    ):
        db_session.add(Solicitacao(
            id=sid, codigo_contrato='22026000123', id_usuario_solicitante=1, etapa_atual_id=1,
            competencia='08/2026', data_solicitacao=datetime(2026, 8, 10),
            protocolo_gerado_sei=protocolo, link_processo_sei=link, status_geral='ABERTO',
        ))
    db_session.commit()


@pytest.fixture()
def eventos_fase1(processos, client, usuario_com_permissoes, logar, monkeypatch):
    from app.solicitacoes.routes import api

    def _falso_download(app_obj, protocolo, token, base_url, inline=False):
        if protocolo == PROTOCOLO_ERRO:
            return (False, f'Erro API {protocolo}: 504')
        if protocolo == PROTOCOLO_EXC:
            raise RuntimeError('falha inesperada')
        return (True, f'OK: {protocolo}')

    monkeypatch.setattr(api, 'baixar_documentos_thread', _falso_download)
    monkeypatch.setattr(api.time, 'sleep', lambda s: None)

    cli = logar(usuario_com_permissoes(7301, permissoes=[('solicitacoes', '', 'aprovar')]))
    with cli.session_transaction() as sess:
        sess['sei_token'] = 'token-falso'

    resp = cli.get(URL_SYNC_DOCS)
    assert resp.status_code == 200
    corpo = resp.get_data(as_text=True)
    return [json.loads(l[6:]) for l in corpo.splitlines() if l.startswith('data: ')]


def test_alerta_de_processo_leva_protocolo_e_link_sei(eventos_fase1):
    alertas = [e for e in eventos_fase1 if e.get('tipo') == 'alerta_processo']
    por_protocolo = {e['protocolo']: e for e in alertas}

    assert por_protocolo[PROTOCOLO_ERRO]['link_sei'] == LINK_ERRO
    # a mensagem não muda: o relatório classifica os alertas por ela
    assert por_protocolo[PROTOCOLO_ERRO]['msg'] == f'[ALERTA] Erro API {PROTOCOLO_ERRO}: 504'


def test_excecao_na_thread_tambem_leva_link_sei(eventos_fase1):
    alerta = next(e for e in eventos_fase1 if e.get('protocolo') == PROTOCOLO_EXC)

    assert alerta['tipo'] == 'alerta_processo'
    assert alerta['link_sei'] == LINK_EXC
    assert alerta['msg'].startswith(f'Erro thread {PROTOCOLO_EXC}')


def test_processo_sem_erro_nao_gera_alerta(eventos_fase1):
    assert not any(e.get('protocolo') == PROTOCOLO_OK for e in eventos_fase1)
    assert eventos_fase1[-1]['concluido'] is True


# ──────────────────────────────────────────────────────────────────────────────
# Front: protocolo vira link e painel de pendências redesenhado
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def html_dashboard(db_session, client, usuario_com_permissoes, logar):
    cli = logar(usuario_com_permissoes(7302, permissoes=[('solicitacoes', '', 'aprovar')]))
    resp = cli.get('/solicitacoes/dashboard')
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def _css_modal():
    with open('app/static/css/components/sync-modal.css', encoding='utf-8') as f:
        return f.read()


def test_js_guarda_protocolo_e_link_de_cada_alerta(html_dashboard):
    assert "dados.tipo === 'alerta_processo'" in html_dashboard
    assert re.search(r'function registrarAlerta\(n, faseNome, texto, protocolo, linkSei\)', html_dashboard)


def test_js_monta_link_do_protocolo_com_seguranca(html_dashboard):
    assert 'function htmlComLinkSei(' in html_dashboard
    # só aceita http(s): evita javascript: vindo do banco
    assert '/^https?:\\/\\//i' in html_dashboard
    assert 'class="sync-link-sei"' in html_dashboard
    assert 'target="_blank" rel="noopener"' in html_dashboard


def test_log_da_execucao_tambem_linka_protocolo(html_dashboard):
    assert re.search(r'function logSync\(texto, classe, protocolo, linkSei\)', html_dashboard)


def test_modal_com_cabecalho_redesenhado(html_dashboard):
    assert 'class="sync-header-icone"' in html_dashboard
    assert 'class="sync-header-eyebrow"' in html_dashboard
    assert 'class="sync-assinatura"' in html_dashboard


def test_css_link_sei_e_pendencias(html_dashboard):
    css = _css_modal()
    for seletor in ('.sync-link-sei', '.sync-pendencia-cabecalho', '.sync-pendencia-qtd',
                    '.sync-pendencia-fase', '.sync-header-icone', '.sync-assinatura'):
        assert seletor in css, seletor
    # botão de excluir deixou de ser um bloco vermelho de largura total
    assert 'btn btn-danger btn-sm w-100' not in html_dashboard


# ──────────────────────────────────────────────────────────────────────────────
# Cabeçalho: ampulheta girando ao lado do título
# ──────────────────────────────────────────────────────────────────────────────

def test_cabecalho_usa_ampulheta_svg_com_areia(html_dashboard):
    icone = re.search(r'<span class="sync-header-icone" aria-hidden="true">(.*?)</span>', html_dashboard, re.S).group(1)
    assert '<svg class="sync-ampulheta"' in icone
    for classe in ('sync-ampulheta-corpo', 'sync-ampulheta-areia-topo', 'sync-ampulheta-areia-base', 'sync-ampulheta-fio'):
        assert f'class="{classe}"' in icone, classe
    # clipPaths com ids próprios (não colidem com outros SVGs da página) e cor herdada do estado
    assert 'id="syncAmpulhetaClipTopo"' in icone and 'url(#syncAmpulhetaClipTopo)' in icone
    assert 'id="syncAmpulhetaClipBase"' in icone and 'url(#syncAmpulhetaClipBase)' in icone
    assert 'currentColor' in icone and '#0033a0' not in icone
    # ícone do resultado continua ao lado, vazio até terminar
    assert '<i class="bi"></i>' in icone
    assert 'bi-hourglass-split' not in icone
    assert 'var ICONES_CABECALHO_SYNC' in html_dashboard
    assert "sucesso: 'bi-check-lg'" in html_dashboard
    assert "andamento: 'bi-hourglass-split'" not in html_dashboard


def test_css_ampulheta_svg_anima_so_em_andamento():
    css = _css_modal()
    assert '@keyframes sync-ampulheta {' not in css  # giro antigo do ícone da fonte saiu
    for nome, seletor in (('sync-ampulheta-girar', 'corpo'), ('sync-ampulheta-topo', 'areia-topo'),
                          ('sync-ampulheta-base', 'areia-base'), ('sync-ampulheta-fio', 'fio')):
        assert re.search(r'\.sync-header\[data-estado="andamento"\] \.sync-ampulheta-' + seletor
                         + r' \{ animation: ' + nome + r' 2\.4s [^}]*infinite; \}', css), nome
        assert f'@keyframes {nome} {{' in css, nome
    assert 'rotate(180deg)' in re.search(r'@keyframes sync-ampulheta-girar \{(.*?)\n\}', css, re.S).group(1)
    # terminado: some a ampulheta e aparece o ícone do resultado
    for estado in ('sucesso', 'alerta', 'erro'):
        assert f'.sync-header[data-estado="{estado}"] .sync-ampulheta' in css, estado
    reduzido = css.split('@media (prefers-reduced-motion: reduce)', 1)[1]
    assert '.sync-header[data-estado="andamento"] .sync-ampulheta *' in reduzido
