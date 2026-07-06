"""Testes do sync individual de documentos CGFR via SSE (nao-bloqueante).

Motivacao: a rota antiga era sincrona e podia levar minutos (API SEI lenta),
estourando o idle timeout do ALB (60s) -> 504 no navegador, mesmo com os
documentos sendo salvos no banco. A versao SSE responde na hora e mantem a
conexao viva com heartbeats ate a sincronizacao terminar.
"""
import json
import time
import uuid
from unittest.mock import patch

from app.cgfr.models import CgfrProcessoEnviado
from app.models.usuario import Usuario


def _login_admin_cgfr(client, db_session):
    uid = uuid.uuid4().hex[:12]
    usuario = Usuario(
        id_usuario_sei=f'cgfr_{uid}',
        nome='Admin CGFR',
        sigla_login=f'admin_cgfr_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()
    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
    return usuario


def _criar_processo(db_session, protocolo):
    processo = CgfrProcessoEnviado(processo_formatado=protocolo)
    db_session.add(processo)
    db_session.flush()
    return processo


def _parse_sse(text):
    """Converte o corpo text/event-stream em lista de dicts (payload dos 'data:')."""
    eventos = []
    for bloco in text.split('\n\n'):
        for linha in bloco.splitlines():
            linha = linha.strip()
            if linha.startswith('data:'):
                eventos.append(json.loads(linha[len('data:'):].strip()))
    return eventos


def test_sync_documentos_responde_stream_sse(client, app, db_session):
    """GET /sync retorna text/event-stream (nao JSON bloqueante)."""
    protocolo = '00002.000501/2026-01'
    _criar_processo(db_session, protocolo)

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        with patch('app.cgfr.routes.acompanhar.gerar_token_sei_admin', return_value='TOKEN'), \
             patch('app.cgfr.routes.acompanhar._update_processo_from_sei'), \
             patch('app.cgfr.routes.acompanhar._fetch_and_save_docs',
                   return_value=(True, '5 documentos sincronizados')):
            resp = client.get(f'/cgfr/acompanhar/{protocolo}/sync')
            corpo = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert resp.mimetype == 'text/event-stream'
    eventos = _parse_sse(corpo)
    final = [e for e in eventos if e.get('concluido')]
    assert final, 'stream deveria terminar com um evento concluido'
    assert final[-1]['success'] is True
    assert '5 documentos' in final[-1]['message']


def test_sync_documentos_headers_anti_buffering(client, app, db_session):
    """Stream precisa desabilitar buffering do proxy (X-Accel-Buffering: no)."""
    protocolo = '00002.000502/2026-02'
    _criar_processo(db_session, protocolo)

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        with patch('app.cgfr.routes.acompanhar.gerar_token_sei_admin', return_value='TOKEN'), \
             patch('app.cgfr.routes.acompanhar._update_processo_from_sei'), \
             patch('app.cgfr.routes.acompanhar._fetch_and_save_docs',
                   return_value=(True, 'ok')):
            resp = client.get(f'/cgfr/acompanhar/{protocolo}/sync')
            resp.get_data(as_text=True)

    assert resp.headers.get('X-Accel-Buffering') == 'no'
    assert 'no-cache' in resp.headers.get('Cache-Control', '')


def test_sync_documentos_processo_inexistente_404(client, app, db_session):
    """Processo desconhecido continua sendo 404 (checagem pre-stream)."""
    with app.app_context():
        _login_admin_cgfr(client, db_session)
        resp = client.get('/cgfr/acompanhar/00002.999999%2F2026-99/sync')

    assert resp.status_code == 404
    assert resp.is_json
    assert resp.get_json()['success'] is False


def test_sync_documentos_falha_token_vira_evento_sse(client, app, db_session):
    """Falha ao obter token e entregue como evento SSE (nao trava o navegador)."""
    protocolo = '00002.000503/2026-03'
    _criar_processo(db_session, protocolo)

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        with patch('app.cgfr.routes.acompanhar.gerar_token_sei_admin', return_value=None):
            resp = client.get(f'/cgfr/acompanhar/{protocolo}/sync')
            corpo = resp.get_data(as_text=True)

    assert resp.status_code == 200
    eventos = _parse_sse(corpo)
    final = [e for e in eventos if e.get('concluido')]
    assert final and final[-1]['success'] is False
    assert 'token' in final[-1]['error'].lower()


def test_sync_documentos_erro_no_fetch_vira_evento_sse(client, app, db_session):
    """Erro durante a busca de docs vira evento SSE com success=False."""
    protocolo = '00002.000504/2026-04'
    _criar_processo(db_session, protocolo)

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        with patch('app.cgfr.routes.acompanhar.gerar_token_sei_admin', return_value='TOKEN'), \
             patch('app.cgfr.routes.acompanhar._update_processo_from_sei'), \
             patch('app.cgfr.routes.acompanhar._fetch_and_save_docs',
                   return_value=(False, 'falha ao salvar documentos')):
            resp = client.get(f'/cgfr/acompanhar/{protocolo}/sync')
            corpo = resp.get_data(as_text=True)

    assert resp.status_code == 200
    eventos = _parse_sse(corpo)
    final = [e for e in eventos if e.get('concluido')]
    assert final and final[-1]['success'] is False
    assert 'falha ao salvar' in final[-1]['error']


def test_sync_documentos_excecao_inesperada_vira_evento_sse(client, app, db_session):
    """Excecao inesperada no worker vira evento SSE, nao derruba a conexao."""
    protocolo = '00002.000505/2026-05'
    _criar_processo(db_session, protocolo)

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        with patch('app.cgfr.routes.acompanhar.gerar_token_sei_admin', return_value='TOKEN'), \
             patch('app.cgfr.routes.acompanhar._update_processo_from_sei'), \
             patch('app.cgfr.routes.acompanhar._fetch_and_save_docs',
                   side_effect=RuntimeError('falha simulada')):
            resp = client.get(f'/cgfr/acompanhar/{protocolo}/sync')
            corpo = resp.get_data(as_text=True)

    assert resp.status_code == 200
    eventos = _parse_sse(corpo)
    final = [e for e in eventos if e.get('concluido')]
    assert final and final[-1]['success'] is False
    assert 'falha simulada' in final[-1]['error']


def test_sync_usa_timeout_generoso_pois_sse_remove_limite_do_alb():
    """Com SSE + heartbeats a conexao sobrevive alem dos 60s do ALB, entao o
    timeout por tentativa ao SEI pode ser generoso (processos grandes levam >60s).
    O heartbeat, por sua vez, precisa caber dentro do idle timeout do ALB.
    """
    from app.cgfr.routes import acompanhar

    assert acompanhar._SEI_TIMEOUT >= 120, (
        'listagem de docs precisa de timeout generoso; processos grandes '
        'demoram >60s e o SSE ja evita o 504 do ALB'
    )
    assert acompanhar._SSE_HEARTBEAT < 60, (
        'heartbeat precisa ser menor que o idle timeout do ALB (60s)'
    )


def test_sync_documentos_emite_heartbeat_durante_processamento(client, app, db_session):
    """Enquanto o SEI processa, o stream emite heartbeats (evita idle timeout do ALB)."""
    protocolo = '00002.000506/2026-06'
    _criar_processo(db_session, protocolo)

    def _fetch_lento(*args, **kwargs):
        time.sleep(0.06)
        return (True, 'ok')

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        with patch('app.cgfr.routes.acompanhar._SSE_HEARTBEAT', 0.01), \
             patch('app.cgfr.routes.acompanhar.gerar_token_sei_admin', return_value='TOKEN'), \
             patch('app.cgfr.routes.acompanhar._update_processo_from_sei'), \
             patch('app.cgfr.routes.acompanhar._fetch_and_save_docs', side_effect=_fetch_lento):
            resp = client.get(f'/cgfr/acompanhar/{protocolo}/sync')
            corpo = resp.get_data(as_text=True)

    eventos = _parse_sse(corpo)
    assert any(e.get('heartbeat') for e in eventos), 'deveria emitir ao menos um heartbeat'
    # heartbeats vem antes do evento final
    idx_final = next(i for i, e in enumerate(eventos) if e.get('concluido'))
    assert any(e.get('heartbeat') for e in eventos[:idx_final])
