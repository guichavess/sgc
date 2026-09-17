"""
Validação de NE no SIAFE com o console do Windows (cp1252).

Bug: siafe_service fazia print("🚀 …") / print("✅ …"). Com o servidor rodando com stdout
em cp1252, o print levantava UnicodeEncodeError: validar_ne_siafe estourava 500 e
get_siafe_token caía no `except Exception` (cujo print também estourava).

Agora: nenhum print no módulo (logs via logger da aplicação, prefixo [SIAFE]) e
token/senha nunca vão para o log.
"""
import logging

import pytest
import requests

from app.services import siafe_service
from app.services.siafe_service import get_siafe_token, validar_ne_siafe

USUARIO = 'usuario.siafe'
SENHA = 'SENHA-SUPER-SECRETA'
TOKEN = 'TOKEN-SUPER-SECRETO'


class Resposta:
    def __init__(self, status, corpo=None, texto=''):
        self.status_code = status
        self._corpo = corpo
        self.text = texto

    def json(self):
        if self._corpo is None:
            raise ValueError('sem json')
        return self._corpo


@pytest.fixture()
def credenciais(monkeypatch):
    monkeypatch.setattr(siafe_service, 'SIAFE_USER', USUARIO)
    monkeypatch.setattr(siafe_service, 'SIAFE_PASS', SENHA)


@pytest.fixture()
def log_capturado(caplog):
    caplog.set_level(logging.DEBUG)
    return caplog


def _fake_post(monkeypatch, resposta=None, excecao=None, capturado=None):
    def chamada(url, **kwargs):
        if capturado is not None:
            capturado.update(kwargs, url=url)
        if excecao:
            raise excecao
        return resposta
    monkeypatch.setattr(siafe_service.requests, 'post', chamada)


def _sem_segredos(caplog):
    assert SENHA not in caplog.text
    assert TOKEN not in caplog.text


def test_modulo_siafe_nao_usa_print():
    codigo = open(siafe_service.__file__, encoding='utf-8').read()
    linhas = [l for l in codigo.splitlines() if 'print(' in l and not l.strip().startswith('#')]
    assert linhas == []


# ── get_siafe_token ───────────────────────────────────────────────────────────

def test_token_obtido_com_console_cp1252(monkeypatch, app, credenciais, console_cp1252, log_capturado):
    capturado = {}
    _fake_post(monkeypatch, Resposta(200, {'token': TOKEN}), capturado=capturado)
    console_cp1252()
    with app.app_context():
        assert get_siafe_token() == TOKEN
    assert capturado['json'] == {'usuario': USUARIO, 'senha': SENHA}
    assert '[SIAFE]' in log_capturado.text
    _sem_segredos(log_capturado)


def test_token_obtido_fora_do_contexto_flask(monkeypatch, credenciais, console_cp1252, log_capturado):
    _fake_post(monkeypatch, Resposta(200, {'token': TOKEN}))
    console_cp1252()
    assert get_siafe_token() == TOKEN
    assert '[SIAFE]' in log_capturado.text
    _sem_segredos(log_capturado)


def test_token_sem_credenciais(monkeypatch, app, console_cp1252, log_capturado):
    monkeypatch.setattr(siafe_service, 'SIAFE_USER', None)
    monkeypatch.setattr(siafe_service, 'SIAFE_PASS', None)
    console_cp1252()
    with app.app_context():
        assert get_siafe_token() is None
    assert '[SIAFE]' in log_capturado.text


def test_token_200_sem_token(monkeypatch, app, credenciais, console_cp1252, log_capturado):
    _fake_post(monkeypatch, Resposta(200, {}))
    console_cp1252()
    with app.app_context():
        assert get_siafe_token() is None
    _sem_segredos(log_capturado)


def test_token_erro_http(monkeypatch, app, credenciais, console_cp1252, log_capturado):
    _fake_post(monkeypatch, Resposta(401, texto='credenciais inválidas'))
    console_cp1252()
    with app.app_context():
        assert get_siafe_token() is None
    assert '401' in log_capturado.text
    _sem_segredos(log_capturado)


def test_token_erro_conexao(monkeypatch, app, credenciais, console_cp1252, log_capturado):
    _fake_post(monkeypatch, excecao=requests.ConnectionError('recusada'))
    console_cp1252()
    with app.app_context():
        assert get_siafe_token() is None
    _sem_segredos(log_capturado)


# ── validar_ne_siafe ──────────────────────────────────────────────────────────

@pytest.fixture()
def token_fake(monkeypatch):
    monkeypatch.setattr(siafe_service, 'get_siafe_token', lambda: TOKEN)


def test_validar_ne_sucesso_com_console_cp1252(monkeypatch, app, token_fake, console_cp1252, log_capturado):
    capturado = {}
    _fake_post(monkeypatch, Resposta(200, {'codContrato': '12345', 'nomeCredor': 'LIDERANÇA LTDA'}),
               capturado=capturado)
    console_cp1252()
    with app.app_context():
        r = validar_ne_siafe('2026NE000123', '12345')
    assert r['sucesso'] is True
    assert 'LIDERANÇA LTDA' in r['mensagem']
    assert capturado['json'] == {'codigo': '2026NE000123', 'codigoUG': '210101'}
    assert capturado['url'].endswith('/nota-empenho/2026')
    assert '[SIAFE]' in log_capturado.text
    _sem_segredos(log_capturado)


def test_validar_ne_fora_do_contexto_flask(monkeypatch, token_fake, console_cp1252):
    _fake_post(monkeypatch, Resposta(200, {'codContrato': '12345'}))
    console_cp1252()
    assert validar_ne_siafe('2026NE000123', '12345')['sucesso'] is True


def test_validar_ne_divergente(monkeypatch, app, token_fake, console_cp1252):
    _fake_post(monkeypatch, Resposta(200, {'codContrato': '999'}))
    console_cp1252()
    with app.app_context():
        r = validar_ne_siafe('2026NE000123', '12345')
    assert r['sucesso'] is False
    assert 'DIVERGÊNCIA' in r['mensagem']


def test_validar_ne_nao_encontrada(monkeypatch, app, token_fake, console_cp1252):
    _fake_post(monkeypatch, Resposta(404))
    console_cp1252()
    with app.app_context():
        r = validar_ne_siafe('2026NE000123', '12345')
    assert r['sucesso'] is False
    assert 'não encontrada' in r['mensagem']


def test_validar_ne_erro_api(monkeypatch, app, token_fake, console_cp1252):
    _fake_post(monkeypatch, Resposta(500))
    console_cp1252()
    with app.app_context():
        r = validar_ne_siafe('2026NE000123', '12345')
    assert r == {'sucesso': False, 'mensagem': 'Erro na consulta SIAFE. Código: 500.', 'categoria': 'danger'}


def test_validar_ne_erro_conexao(monkeypatch, app, token_fake, console_cp1252, log_capturado):
    _fake_post(monkeypatch, excecao=requests.Timeout('lento'))
    console_cp1252()
    with app.app_context():
        r = validar_ne_siafe('2026NE000123', '12345')
    assert r['mensagem'] == 'Falha de comunicação com o SIAFE. Tente novamente.'
    _sem_segredos(log_capturado)


def test_validar_ne_erro_interno(monkeypatch, app, token_fake, console_cp1252):
    _fake_post(monkeypatch, Resposta(200))  # .json() levanta ValueError
    console_cp1252()
    with app.app_context():
        r = validar_ne_siafe('2026NE000123', '12345')
    assert r['mensagem'] == 'Erro interno ao validar NE.'


def test_validar_ne_falha_login(monkeypatch, app, console_cp1252):
    monkeypatch.setattr(siafe_service, 'get_siafe_token', lambda: None)
    console_cp1252()
    with app.app_context():
        r = validar_ne_siafe('2026NE000123', '12345')
    assert r['sucesso'] is False
    assert 'Falha no Login' in r['mensagem']
