"""
Envio de e-mail com o console do Windows (cp1252).

Bug: email_service fazia print("✅ [Email] Enviado …") dentro do try. Com stdout em cp1252
o print levantava UnicodeEncodeError DEPOIS do envio; o `except` (com outro print com
emoji) estourava de novo e a notificação quebrava mesmo com o e-mail enviado.

Agora: nenhum print no módulo (logs via logger da aplicação, prefixo [EMAIL]) e a senha
nunca vai para o log.
"""
import logging
import smtplib

import pytest

from app.services import email_service
from app.services.email_service import enviar_email_teste

REMETENTE = 'sgc@exemplo.gov.br'
SENHA = 'SENHA-SMTP-SECRETA'
DESTINOS = ['fulano@exemplo.gov.br', 'ciclano@exemplo.gov.br']


class SMTPFake:
    enviados = []
    erro = None

    def __init__(self, servidor, porta):
        self.servidor, self.porta = servidor, porta

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        pass

    def login(self, usuario, senha):
        if SMTPFake.erro:
            raise SMTPFake.erro
        self.login_usado = (usuario, senha)

    def send_message(self, msg):
        SMTPFake.enviados.append(msg)


@pytest.fixture()
def smtp_fake(monkeypatch):
    SMTPFake.enviados = []
    SMTPFake.erro = None
    monkeypatch.setattr(email_service.smtplib, 'SMTP', SMTPFake)
    return SMTPFake


@pytest.fixture()
def credenciais(monkeypatch):
    monkeypatch.setenv('EMAIL_ADDRESS', REMETENTE)
    monkeypatch.setenv('EMAIL_PASSWORD', SENHA)


@pytest.fixture()
def log_capturado(caplog):
    caplog.set_level(logging.DEBUG)
    return caplog


def test_modulo_email_nao_usa_print():
    codigo = open(email_service.__file__, encoding='utf-8').read()
    linhas = [l for l in codigo.splitlines() if 'print(' in l and not l.strip().startswith('#')]
    assert linhas == []


def test_envio_com_console_cp1252(app, credenciais, smtp_fake, console_cp1252, log_capturado):
    console_cp1252()
    with app.app_context():
        assert enviar_email_teste('Assunto ção', '<p>corpo</p>', DESTINOS) is True
    assert len(smtp_fake.enviados) == 1
    assert smtp_fake.enviados[0]['To'] == ', '.join(DESTINOS)
    assert '[EMAIL]' in log_capturado.text
    assert SENHA not in log_capturado.text


def test_envio_fora_do_contexto_flask(credenciais, smtp_fake, console_cp1252, log_capturado):
    console_cp1252()
    assert enviar_email_teste('Assunto', '<p>corpo</p>', DESTINOS) is True
    assert '[EMAIL]' in log_capturado.text


def test_sem_credenciais(monkeypatch, app, smtp_fake, console_cp1252, log_capturado):
    monkeypatch.delenv('EMAIL_ADDRESS', raising=False)
    monkeypatch.delenv('EMAIL_PASSWORD', raising=False)
    console_cp1252()
    with app.app_context():
        assert enviar_email_teste('Assunto', '<p>corpo</p>', DESTINOS) is False
    assert smtp_fake.enviados == []
    assert '[EMAIL]' in log_capturado.text


def test_sem_destinatarios(app, credenciais, smtp_fake, console_cp1252, log_capturado):
    console_cp1252()
    with app.app_context():
        assert enviar_email_teste('Assunto', '<p>corpo</p>', []) is False
    assert smtp_fake.enviados == []
    assert '[EMAIL]' in log_capturado.text


def test_falha_smtp_com_console_cp1252(app, credenciais, smtp_fake, console_cp1252, log_capturado):
    smtp_fake.erro = smtplib.SMTPAuthenticationError(535, b'5.7.8 Username and Password not accepted')
    console_cp1252()
    with app.app_context():
        assert enviar_email_teste('Assunto', '<p>corpo</p>', DESTINOS) is False
    assert smtp_fake.enviados == []
    assert '[EMAIL]' in log_capturado.text
    assert SENHA not in log_capturado.text
