"""
Integração SEI de pagamentos com o console do Windows (cp1252).

Bug: as funções faziam print("📡 …") dentro do try. Com o servidor rodando com stdout em
cp1252, o print levantava UnicodeEncodeError, o `except Exception` engolia e a função
devolvia None ANTES de chamar o SEI: "Erro ao criar processo no SEI" e, depois, o modal
de assinatura com "Requisição de pagamento: —".

Agora: nenhuma função do módulo usa print (logs via logger da aplicação), e
gerar_documento_pagamento descreve a falha em `detalhe_erro` como criar_procedimento_pagamento.
"""
import io
import sys

import pytest
import requests

from app.services import sei_integration
from app.services.sei_integration import consultar_procedimento_sei, gerar_documento_pagamento

CTX = {'num_contrato': '045/2025', 'empresa': 'LIDERANCA LTDA', 'competencia': '12/2025',
       'usuario_nome': 'Fulano', 'usuario_cargo': 'Gestor', 'objeto': 'Limpeza'}


class Resposta:
    def __init__(self, status, corpo=None, texto=''):
        self.status_code = status
        self._corpo = corpo
        self.text = texto

    def json(self):
        if self._corpo is None:
            raise ValueError('sem json')
        return self._corpo

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f'{self.status_code} erro', response=self)


@pytest.fixture()
def console_cp1252(monkeypatch):
    """stdout como o do servidor no Windows: emoji no print levanta UnicodeEncodeError.

    Devolve um ativador: o pytest reinstala a captura de stdout depois das fixtures,
    então a troca precisa acontecer no corpo do teste.
    """
    def ativar():
        monkeypatch.setattr(sys, 'stdout', io.TextIOWrapper(io.BytesIO(), encoding='cp1252'))
        with pytest.raises(UnicodeEncodeError):
            print('📡')
    return ativar


def _fake(monkeypatch, metodo, resposta=None, excecao=None, capturado=None):
    def chamada(url, **kwargs):
        if capturado is not None:
            capturado.update(kwargs, url=url)
        if excecao:
            raise excecao
        return resposta
    monkeypatch.setattr(sei_integration.requests, metodo, chamada)


def test_modulo_sei_nao_usa_print():
    codigo = open(sei_integration.__file__, encoding='utf-8').read()
    linhas = [l for l in codigo.splitlines() if 'print(' in l and not l.strip().startswith('#')]
    assert linhas == []


def test_gerar_documento_funciona_com_console_cp1252(monkeypatch, app, console_cp1252):
    capturado = {}
    _fake(monkeypatch, 'post', Resposta(200, {'IdDocumento': '77', 'DocumentoFormatado': '0012345'}), capturado=capturado)
    console_cp1252()
    with app.app_context():
        doc = gerar_documento_pagamento('tok', '110000001', '555', CTX)
    assert doc['DocumentoFormatado'] == '0012345'
    assert capturado['json']['Procedimento'] == '555'
    assert capturado['timeout'] == 60


def test_consultar_procedimento_funciona_com_console_cp1252(monkeypatch, app, console_cp1252):
    _fake(monkeypatch, 'get', Resposta(200, {'ProcedimentoFormatado': '00002.000555/2026-55', 'IdProcedimento': '555'}))
    console_cp1252()
    with app.app_context():
        r = consultar_procedimento_sei('tok', '00002.000555/2026-55')
    assert r['sucesso'] is True and r['id_procedimento'] == '555'


def test_criar_procedimento_funciona_com_console_cp1252(monkeypatch, app, console_cp1252):
    _fake(monkeypatch, 'post', Resposta(201, {'IdProcedimento': '9', 'ProcedimentoFormatado': 'X'}))
    console_cp1252()
    with app.app_context():
        assert sei_integration.criar_procedimento_pagamento('tok', '1', {'numeroOriginal': '1'}, '12/2025')


def test_gerar_documento_http_erro_descreve_e_loga(monkeypatch, app, caplog):
    _fake(monkeypatch, 'post', Resposta(422, {'mensagem': 'Série 2614 não permitida na unidade'}))
    detalhe = {}
    with app.app_context(), caplog.at_level('WARNING'):
        assert gerar_documento_pagamento('tok-SEGREDO', '110000001', '555', CTX, detalhe_erro=detalhe) is None
    assert detalhe == {'status': 422, 'mensagem': 'Série 2614 não permitida na unidade'}
    assert '[SEI]' in caplog.text and '422' in caplog.text and 'Série 2614' in caplog.text
    assert 'SEGREDO' not in caplog.text


def test_gerar_documento_sem_resposta(monkeypatch, app):
    _fake(monkeypatch, 'post', excecao=requests.Timeout('lento'))
    detalhe = {}
    with app.app_context():
        assert gerar_documento_pagamento('tok', '110000001', '555', CTX, detalhe_erro=detalhe) is None
    assert detalhe['status'] is None


def test_gerar_documento_detalhe_e_opcional(monkeypatch, app):
    _fake(monkeypatch, 'post', Resposta(500, texto='boom'))
    with app.app_context():
        assert gerar_documento_pagamento('tok', '110000001', '555', CTX) is None
