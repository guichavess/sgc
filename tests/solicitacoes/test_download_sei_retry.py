"""
Retry do download de documentos SEI (baixar_documentos_thread) para respostas
HTTP transitórias do gateway do SEI (502/503/504).

Antes: só exceções de rede (ReadTimeout, ConnectionError, SSLError) eram
repetidas; um 504 devolvido pelo gateway falhava na 1ª tentativa.
"""
import pytest

PROTOCOLO = '00002.000928/2025-31'


class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


def _doc(id_doc):
    return {
        'IdDocumento': id_doc, 'IdProcedimento': '1', 'DocumentoFormatado': str(id_doc),
        'Serie': {'IdSerie': '10', 'Nome': 'Despacho'}, 'UnidadeElaboradora': {'IdUnidade': '1', 'Sigla': 'SEAD'},
        'Data': '01/01/2025',
    }


@pytest.fixture
def fake_sei(monkeypatch):
    """Substitui requests.Session do módulo por uma fila de respostas roteirizadas."""
    from app.solicitacoes.routes import api

    estado = {'respostas': [], 'chamadas': [], 'esperas': []}

    class _Session:
        verify = True

        def get(self, url, headers=None, params=None, timeout=None):
            estado['chamadas'].append({'params': dict(params or {}), 'timeout': timeout})
            resposta = estado['respostas'].pop(0)
            if isinstance(resposta, Exception):
                raise resposta
            return resposta

        def close(self):
            pass

    monkeypatch.setattr(api.http_requests, 'Session', _Session)
    monkeypatch.setattr(api.time, 'sleep', lambda s: estado['esperas'].append(s))
    return estado


def _baixar(app):
    from app.solicitacoes.routes.api import baixar_documentos_thread
    return baixar_documentos_thread(app, PROTOCOLO, 'token', 'http://sei/documentos', inline=True)


@pytest.mark.parametrize('status', [502, 503, 504])
def test_repete_em_status_transitorio_e_conclui(app, db_session, fake_sei, status):
    fake_sei['respostas'] = [_Resp(status), _Resp(200, {'Documentos': [_doc(1), _doc(2)]})]

    sucesso, msg = _baixar(app)

    assert sucesso is True
    assert msg == f'OK: {PROTOCOLO}'
    assert len(fake_sei['chamadas']) == 2
    assert fake_sei['esperas'], 'deve aguardar antes de repetir'


def test_504_persistente_esgota_tentativas_e_mantem_mensagem(app, db_session, fake_sei):
    fake_sei['respostas'] = [_Resp(504), _Resp(504), _Resp(504)]

    sucesso, msg = _baixar(app)

    assert sucesso is False
    # Formato consumido por classificar_alerta no relatório da sincronização
    assert msg == f'Erro API {PROTOCOLO}: 504'
    assert len(fake_sei['chamadas']) == 3


def test_504_preserva_documentos_anteriores(app, db_session, fake_sei):
    from app.models import SeiMovimentacao

    db_session.add(SeiMovimentacao(id_documento='antigo', protocolo_procedimento=PROTOCOLO, obs=''))
    db_session.flush()
    fake_sei['respostas'] = [_Resp(504), _Resp(504), _Resp(504)]

    _baixar(app)

    assert SeiMovimentacao.query.filter_by(protocolo_procedimento=PROTOCOLO).count() == 1


@pytest.mark.parametrize('status', [401, 403, 404, 422])
def test_nao_repete_status_definitivo(app, db_session, fake_sei, status):
    fake_sei['respostas'] = [_Resp(status)]

    sucesso, _ = _baixar(app)

    assert sucesso is False
    assert len(fake_sei['chamadas']) == 1


def test_timeout_seguido_de_504_e_200_usa_todas_as_tentativas(app, db_session, fake_sei):
    from app.solicitacoes.routes import api

    fake_sei['respostas'] = [
        api.http_requests.exceptions.ReadTimeout('lento'),
        _Resp(504),
        _Resp(200, {'Documentos': [_doc(1)]}),
    ]

    sucesso, _ = _baixar(app)

    assert sucesso is True
    assert len(fake_sei['chamadas']) == 3
