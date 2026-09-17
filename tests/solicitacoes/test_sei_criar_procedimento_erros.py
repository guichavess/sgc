"""
Falhas ao criar o processo de pagamento no SEI.

Antes: criar_procedimento_pagamento devolvia None e o motivo só ia para o stdout (print);
a tela mostrava "Erro ao criar processo no SEI. Tente novamente." sem distinguir sessão
expirada, unidade sem permissão, dado recusado ou SEI fora do ar.

Agora: o retorno continua dict|None (api.py/lote não muda), mas quem passa `detalhe_erro`
recebe status + mensagem do SEI, e descrever_falha_criacao traduz isso para a tela.
"""
import pytest
import requests

from app.services import sei_integration
from app.services.sei_integration import criar_procedimento_pagamento, descrever_falha_criacao

CONTRATO = {'numeroOriginal': '24/2021', 'codigo': '21006213', 'nomeContratadoResumido': 'PRIME CONSULTORIA'}


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


def _post_devolvendo(monkeypatch, resposta=None, excecao=None, capturado=None):
    def fake_post(url, **kwargs):
        if capturado is not None:
            capturado.update(kwargs, url=url)
        if excecao:
            raise excecao
        return resposta
    monkeypatch.setattr(sei_integration.requests, 'post', fake_post)


def test_sucesso_nao_preenche_detalhe_e_usa_timeout(monkeypatch, app):
    capturado = {}
    _post_devolvendo(monkeypatch, Resposta(201, {'IdProcedimento': '9', 'ProcedimentoFormatado': '00002.000009/2026-09'}),
                     capturado=capturado)
    detalhe = {}
    with app.app_context():
        retorno = criar_procedimento_pagamento('tok', '110000001', CONTRATO, '12/2025', detalhe_erro=detalhe)
    assert retorno['IdProcedimento'] == '9'
    assert detalhe == {}
    assert capturado['timeout'] == 60


def test_parametro_detalhe_e_opcional(monkeypatch, app):
    _post_devolvendo(monkeypatch, Resposta(500, texto='boom'))
    with app.app_context():
        assert criar_procedimento_pagamento('tok', '110000001', CONTRATO, '12/2025') is None


@pytest.mark.parametrize('corpo,texto,esperado', [
    ({'mensagem': 'Unidade [110000001] não encontrada.'}, '', 'Unidade [110000001] não encontrada.'),
    ({'message': 'Token inválido'}, '', 'Token inválido'),
    ({'erros': ['Especificação obrigatória', 'Assunto inválido']}, '', 'Especificação obrigatória; Assunto inválido'),
    (None, '<html><body><h1>Bad Gateway</h1></body></html>', 'Bad Gateway'),
    (None, '', ''),
])
def test_http_erro_guarda_status_e_mensagem_do_sei(monkeypatch, app, corpo, texto, esperado):
    _post_devolvendo(monkeypatch, Resposta(422, corpo, texto))
    detalhe = {}
    with app.app_context():
        assert criar_procedimento_pagamento('tok', '110000001', CONTRATO, '12/2025', detalhe_erro=detalhe) is None
    assert detalhe['status'] == 422
    assert detalhe['mensagem'] == esperado


def test_mensagem_do_sei_e_limitada(monkeypatch, app):
    _post_devolvendo(monkeypatch, Resposta(400, {'mensagem': 'x' * 1000}))
    detalhe = {}
    with app.app_context():
        criar_procedimento_pagamento('tok', '110000001', CONTRATO, '12/2025', detalhe_erro=detalhe)
    assert len(detalhe['mensagem']) <= 300


@pytest.mark.parametrize('excecao', [requests.Timeout('lento'), requests.ConnectionError('fora')])
def test_sei_sem_resposta_fica_sem_status(monkeypatch, app, excecao):
    _post_devolvendo(monkeypatch, excecao=excecao)
    detalhe = {}
    with app.app_context():
        assert criar_procedimento_pagamento('tok', '110000001', CONTRATO, '12/2025', detalhe_erro=detalhe) is None
    assert detalhe['status'] is None


def test_falha_vai_para_o_log_da_aplicacao(monkeypatch, app, caplog):
    _post_devolvendo(monkeypatch, Resposta(403, {'mensagem': 'Acesso negado à unidade'}))
    with app.app_context(), caplog.at_level('WARNING'):
        criar_procedimento_pagamento('tok-SEGREDO-123', '110000001', CONTRATO, '12/2025', detalhe_erro={})
    registro = caplog.text
    assert '[SEI]' in registro and '403' in registro and 'Acesso negado à unidade' in registro
    assert 'SEGREDO' not in registro  # nunca loga o token


def test_nao_usa_print():
    codigo = open(sei_integration.__file__, encoding='utf-8').read()
    corpo = codigo.split('def criar_procedimento_pagamento', 1)[1].split('\ndef ', 1)[0]
    assert 'print(' not in corpo


# ─ tradução para a tela
@pytest.mark.parametrize('detalhe,titulo', [
    ({'status': 401, 'mensagem': ''}, 'Sua sessão no SEI expirou'),
    ({'status': 403, 'mensagem': ''}, 'Sem permissão na unidade escolhida'),
    ({'status': 422, 'mensagem': 'Especificação obrigatória'}, 'O SEI recusou a abertura do processo'),
    ({'status': 400, 'mensagem': ''}, 'O SEI recusou a abertura do processo'),
    ({'status': 503, 'mensagem': ''}, 'O SEI está instável'),
    ({'status': None, 'mensagem': ''}, 'O SEI não respondeu'),
    ({}, 'Não foi possível criar o processo no SEI'),
    ({'status': 418, 'mensagem': ''}, 'Não foi possível criar o processo no SEI'),
])
def test_descrever_falha_criacao_titulos(detalhe, titulo):
    falha = descrever_falha_criacao(detalhe)
    assert falha['titulo'] == titulo
    assert falha['mensagem'] and falha['dica']


def test_descrever_falha_inclui_mensagem_e_codigo_do_sei():
    falha = descrever_falha_criacao({'status': 422, 'mensagem': 'Especificação obrigatória'})
    assert 'Especificação obrigatória' in falha['mensagem']
    assert falha['codigo'] == 'SEI 422'
    assert descrever_falha_criacao({'status': None})['codigo'] == 'SEI sem resposta'


def test_sucesso_registra_protocolo_criado_no_log(monkeypatch, app, caplog):
    # histórico para auditoria: quais processos o SGC abriu no SEI (ex.: excluir processos de teste)
    _post_devolvendo(monkeypatch, Resposta(201, {'IdProcedimento': '9', 'ProcedimentoFormatado': '00002.000009/2026-09'}))
    with app.app_context(), caplog.at_level('INFO'):
        criar_procedimento_pagamento('tok-SEGREDO', '110006254', CONTRATO, '12/2025')
    assert '[SEI] Procedimento criado' in caplog.text
    assert '00002.000009/2026-09' in caplog.text and '110006254' in caplog.text and '21006213' in caplog.text
    assert 'SEGREDO' not in caplog.text
