"""
Testes de resiliencia da renovacao do token SEI.

Contexto do bug (producao, 28/07/2026): o hook before_request em
app/__init__.py chama renovar_token_sessao() a cada requisicao de usuario
autenticado. Sem backoff apos falha e sem coalescencia de chamadas
concorrentes, uma credencial rejeitada pelo SEI (HTTP 422) gerava dezenas
de POSTs por segundo — cada um segurando uma thread do gunicorn por ate
30s (timeout). Com 2 workers x 4 threads, isso esgotava o pool e travava
o sistema inteiro sem consumir CPU (espera de I/O de rede).
"""
import threading
import time

import pytest

from app.services import sei_token


class _RespostaFake:
    """Resposta minima de requests.post usada nos mocks."""

    def __init__(self, status_code, dados=None):
        self.status_code = status_code
        self._dados = dados or {}
        self.text = str(self._dados)

    def json(self):
        return self._dados


def _preencher_sessao(login='usuario.teste'):
    """Grava na sessao as credenciais que renovar_token_sessao() consome."""
    from flask import session

    session['_sei_user'] = login
    session['_sei_pwd'] = 'senha-secreta'
    session['_sei_orgao'] = 'SEAD-PI'


@pytest.fixture(autouse=True)
def limpa_estado_modulo():
    """Isola o cache/backoff module-level entre testes."""
    sei_token.limpar_estado()
    yield
    sei_token.limpar_estado()


# ── Backoff apos falha ────────────────────────────────────────────────────────

def test_falha_ativa_backoff_e_bloqueia_novas_chamadas(app, monkeypatch):
    """Apos um 422, requisicoes seguintes nao devem chamar a API do SEI."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(422)

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)

    with app.test_request_context():
        _preencher_sessao()
        assert sei_token.renovar_token_sessao() is None
        assert sei_token.renovar_token_sessao() is None
        assert sei_token.renovar_token_sessao() is None

    assert len(chamadas) == 1, (
        f'Esperada 1 chamada a API do SEI durante o backoff, houve {len(chamadas)}'
    )


def test_timeout_tambem_ativa_backoff(app, monkeypatch):
    """Timeout e o caso que mais prende thread — tem que entrar em backoff."""
    import requests as _requests

    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        raise _requests.exceptions.Timeout()

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)

    with app.test_request_context():
        _preencher_sessao()
        assert sei_token.renovar_token_sessao() is None
        assert sei_token.renovar_token_sessao() is None

    assert len(chamadas) == 1


def test_backoff_expira_e_permite_nova_tentativa(app, monkeypatch):
    """Falha antiga nao pode bloquear a renovacao para sempre."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(422)

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)
    monkeypatch.setattr(sei_token, 'SEI_TOKEN_RETRY_BACKOFF', 0)

    with app.test_request_context():
        _preencher_sessao()
        sei_token.renovar_token_sessao()
        sei_token.renovar_token_sessao()

    assert len(chamadas) == 2


def test_backoff_e_por_usuario(app, monkeypatch):
    """Credencial ruim de um usuario nao pode bloquear a renovacao de outro."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(kwargs['json']['Usuario'])
        return _RespostaFake(422)

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)

    with app.test_request_context():
        _preencher_sessao('usuario.a')
        sei_token.renovar_token_sessao()
        sei_token.renovar_token_sessao()

    with app.test_request_context():
        _preencher_sessao('usuario.b')
        sei_token.renovar_token_sessao()

    assert chamadas == ['usuario.a', 'usuario.b']


# ── Coalescencia de chamadas concorrentes ─────────────────────────────────────

def test_renovacoes_concorrentes_fazem_uma_unica_chamada(app, monkeypatch):
    """
    Uma tela que dispara varios AJAX em paralelo com o token expirado gerava
    um POST por requisicao (visivel no log: 5 tentativas em 100ms). Todas as
    threads devem compartilhar o resultado de uma unica chamada.
    """
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        time.sleep(0.3)  # janela para as outras threads chegarem
        return _RespostaFake(200, {'Token': 'tok-123'})

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)

    resultados = []

    def worker():
        with app.test_request_context():
            _preencher_sessao()
            resultados.append(sei_token.renovar_token_sessao())

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(chamadas) == 1, (
        f'Esperada 1 chamada coalescida, houve {len(chamadas)}'
    )
    assert resultados == ['tok-123'] * 5


def test_falhas_concorrentes_fazem_uma_unica_chamada(app, monkeypatch):
    """Coalescencia tambem vale quando a renovacao falha."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        time.sleep(0.3)
        return _RespostaFake(422)

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)

    resultados = []

    def worker():
        with app.test_request_context():
            _preencher_sessao()
            resultados.append(sei_token.renovar_token_sessao())

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(chamadas) == 1
    assert resultados == [None] * 5


# ── Comportamento preservado no caminho feliz ─────────────────────────────────

def test_renovacao_bem_sucedida_atualiza_sessao(app, monkeypatch):
    """Regressao: token, unidades e unidade atual continuam indo para a sessao."""
    from flask import session

    dados = {
        'Token': 'tok-ok',
        'Unidades': [{'Id': '110000001', 'Sigla': 'SEAD/GEO'}],
        'Login': {'IdUnidadeAtual': 110000001},
    }
    monkeypatch.setattr(
        sei_token.requests, 'post',
        lambda *a, **kw: _RespostaFake(200, dados),
    )

    with app.test_request_context():
        _preencher_sessao()
        token = sei_token.renovar_token_sessao()

        assert token == 'tok-ok'
        assert session['sei_token'] == 'tok-ok'
        assert session['sei_token_time'] > 0
        assert session['unidades'] == [{'id': '110000001', 'sigla': 'SEAD/GEO'}]
        assert session['unidade_atual_id'] == '110000001'


def test_sucesso_limpa_backoff_anterior(app, monkeypatch):
    """Depois que o usuario reloga com credencial valida, o backoff sai do caminho."""
    respostas = [_RespostaFake(422), _RespostaFake(200, {'Token': 'tok-ok'})]
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return respostas.pop(0)

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)
    monkeypatch.setattr(sei_token, 'SEI_TOKEN_RETRY_BACKOFF', 0)

    with app.test_request_context():
        _preencher_sessao()
        assert sei_token.renovar_token_sessao() is None       # falha -> backoff
        assert sei_token.renovar_token_sessao() == 'tok-ok'   # backoff expirado
        assert sei_token.esta_em_backoff('usuario.teste') is False

    assert len(chamadas) == 2


def test_sem_credenciais_na_sessao_nao_chama_api(app, monkeypatch):
    """Regressao: sessao sem _sei_user/_sei_pwd retorna None sem tocar a rede."""
    chamadas = []
    monkeypatch.setattr(
        sei_token.requests, 'post',
        lambda *a, **kw: chamadas.append(1) or _RespostaFake(200, {'Token': 'x'}),
    )

    with app.test_request_context():
        assert sei_token.renovar_token_sessao() is None

    assert chamadas == []


def test_token_em_cache_e_reaproveitado_sem_nova_chamada(app, monkeypatch):
    """
    Requisicao seguinte cuja sessao ainda nao tem o token aproveita o token
    ja obtido, em vez de abrir outra conexao com o SEI.
    """
    from flask import session

    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(200, {'Token': 'tok-cache'})

    monkeypatch.setattr(sei_token.requests, 'post', fake_post)

    with app.test_request_context():
        _preencher_sessao()
        assert sei_token.renovar_token_sessao() == 'tok-cache'

    # Nova requisicao (sessao "limpa", mesmo login)
    with app.test_request_context():
        _preencher_sessao()
        assert sei_token.renovar_token_sessao() == 'tok-cache'
        assert session['sei_token'] == 'tok-cache'

    assert len(chamadas) == 1
