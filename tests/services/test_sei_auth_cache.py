"""
Testes do cache do token admin SEI (gerar_token_sei_admin).

Contexto: gerar_token_sei_admin() era chamada ~40+ vezes por ação sem
nenhum cache — cada chamada fazia um POST síncrono à API SEI com
timeout de 120s. Com o cache, o token é obtido 1 vez por dia e servido
do cache até meia-noite de Brasília.
"""
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from app.services import sei_auth


class _RespostaFake:
    """Resposta minima de requests.post usada nos mocks."""

    def __init__(self, status_code, dados=None):
        self.status_code = status_code
        self._dados = dados or {}
        self.text = str(self._dados)
        self.headers = {}

    def json(self):
        return self._dados


@pytest.fixture(autouse=True)
def limpa_cache_admin():
    """Isola o cache admin entre testes."""
    sei_auth.limpar_cache_admin()
    yield
    sei_auth.limpar_cache_admin()


# ── Cache básico ──────────────────────────────────────────────────────────────

def test_cache_evita_chamadas_repetidas(app, monkeypatch):
    """Após obter o token, chamadas seguintes servem do cache sem POST."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(200, {'Token': 'tok-admin-123'})

    monkeypatch.setattr(sei_auth.requests, 'post', fake_post)
    monkeypatch.setenv('SEI_USER', 'admin@sead.pi.gov.br')
    monkeypatch.setenv('SEI_PASSWORD', 'senha-admin')

    with app.app_context():
        t1 = sei_auth.gerar_token_sei_admin()
        t2 = sei_auth.gerar_token_sei_admin()
        t3 = sei_auth.gerar_token_sei_admin()

    assert t1 == 'tok-admin-123'
    assert t2 == 'tok-admin-123'
    assert t3 == 'tok-admin-123'
    assert len(chamadas) == 1, (
        f'Esperada 1 chamada à API, houve {len(chamadas)}'
    )


def test_cache_expira_apos_meia_noite(app, monkeypatch):
    """Quando passa da meia-noite, o cache expira e uma nova chamada é feita."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(200, {'Token': f'tok-{len(chamadas)}'})

    monkeypatch.setattr(sei_auth.requests, 'post', fake_post)
    monkeypatch.setenv('SEI_USER', 'admin@sead.pi.gov.br')
    monkeypatch.setenv('SEI_PASSWORD', 'senha-admin')

    with app.app_context():
        t1 = sei_auth.gerar_token_sei_admin()
        assert t1 == 'tok-1'

        # Simula que o token foi obtido ontem (cache expirado)
        sei_auth._admin_obtido_em = time.time() - 86400  # 24h atrás

        t2 = sei_auth.gerar_token_sei_admin()
        assert t2 == 'tok-2'

    assert len(chamadas) == 2


# ── Backoff após falha ────────────────────────────────────────────────────────

def test_backoff_apos_falha(app, monkeypatch):
    """Após falha, novas chamadas dentro do backoff retornam None sem POST."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(500)

    monkeypatch.setattr(sei_auth.requests, 'post', fake_post)
    monkeypatch.setenv('SEI_USER', 'admin@sead.pi.gov.br')
    monkeypatch.setenv('SEI_PASSWORD', 'senha-admin')

    with app.app_context():
        assert sei_auth.gerar_token_sei_admin() is None
        assert sei_auth.gerar_token_sei_admin() is None
        assert sei_auth.gerar_token_sei_admin() is None

    assert len(chamadas) == 1


def test_backoff_expira_e_permite_nova_tentativa(app, monkeypatch):
    """Após o período de backoff, uma nova tentativa é permitida."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(500)

    monkeypatch.setattr(sei_auth.requests, 'post', fake_post)
    monkeypatch.setenv('SEI_USER', 'admin@sead.pi.gov.br')
    monkeypatch.setenv('SEI_PASSWORD', 'senha-admin')

    with app.app_context():
        sei_auth.gerar_token_sei_admin()

        # Simula que o backoff já expirou
        sei_auth._admin_falha_em = time.time() - 120  # 2 min atrás (backoff=60s)

        sei_auth.gerar_token_sei_admin()

    assert len(chamadas) == 2


def test_timeout_ativa_backoff(app, monkeypatch):
    """Timeout na API também ativa o backoff."""
    import requests as _requests

    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        raise _requests.exceptions.Timeout()

    monkeypatch.setattr(sei_auth.requests, 'post', fake_post)
    monkeypatch.setenv('SEI_USER', 'admin@sead.pi.gov.br')
    monkeypatch.setenv('SEI_PASSWORD', 'senha-admin')

    with app.app_context():
        assert sei_auth.gerar_token_sei_admin() is None
        assert sei_auth.gerar_token_sei_admin() is None

    assert len(chamadas) == 1


# ── Coalescência de threads ──────────────────────────────────────────────────

def test_coalescencia_threads(app, monkeypatch):
    """5 threads concorrentes fazem apenas 1 POST à API."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        time.sleep(0.3)  # janela para as outras threads chegarem
        return _RespostaFake(200, {'Token': 'tok-coal'})

    monkeypatch.setattr(sei_auth.requests, 'post', fake_post)
    monkeypatch.setenv('SEI_USER', 'admin@sead.pi.gov.br')
    monkeypatch.setenv('SEI_PASSWORD', 'senha-admin')

    resultados = []

    def worker():
        with app.app_context():
            resultados.append(sei_auth.gerar_token_sei_admin())

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(chamadas) == 1, (
        f'Esperada 1 chamada coalescida, houve {len(chamadas)}'
    )
    assert resultados == ['tok-coal'] * 5


# ── Limpeza ──────────────────────────────────────────────────────────────────

def test_limpar_cache_reseta_tudo(app, monkeypatch):
    """limpar_cache_admin() zera cache e backoff."""
    chamadas = []

    def fake_post(*args, **kwargs):
        chamadas.append(1)
        return _RespostaFake(200, {'Token': 'tok-limpo'})

    monkeypatch.setattr(sei_auth.requests, 'post', fake_post)
    monkeypatch.setenv('SEI_USER', 'admin@sead.pi.gov.br')
    monkeypatch.setenv('SEI_PASSWORD', 'senha-admin')

    with app.app_context():
        sei_auth.gerar_token_sei_admin()
        assert len(chamadas) == 1

        sei_auth.limpar_cache_admin()

        sei_auth.gerar_token_sei_admin()
        assert len(chamadas) == 2


def test_sem_credenciais_retorna_none(app, monkeypatch):
    """Sem SEI_USER/SEI_PASSWORD, retorna None sem tocar a rede."""
    chamadas = []

    monkeypatch.setattr(
        sei_auth.requests, 'post',
        lambda *a, **kw: chamadas.append(1) or _RespostaFake(200, {'Token': 'x'}),
    )
    # Garante que as variáveis não estejam definidas
    monkeypatch.delenv('SEI_USER', raising=False)
    monkeypatch.delenv('SEI_PASSWORD', raising=False)

    with app.app_context():
        # Limpa config do Flask também
        app.config.pop('SEI_USER', None)
        app.config.pop('SEI_PASSWORD', None)
        result = sei_auth.gerar_token_sei_admin()

    assert result is None
    assert chamadas == []
