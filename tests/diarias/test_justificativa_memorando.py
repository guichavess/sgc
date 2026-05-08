"""
Testes: justificativa do solicitante (< 10 dias) no memorando SEI.

Verifica que gerar_memorando_diarias inclui/omite a justificativa
do solicitante no HTML do memorando conforme o campo esteja preenchido ou não.
"""
from unittest.mock import patch, MagicMock
import pytest


@pytest.fixture
def app_ctx(app):
    with app.app_context():
        yield app


def _build_dados_memorando(justificativa_solicitante=''):
    return {
        'justificativa': 'Texto padrão do memorando',
        'data_viagem': '2026-06-01',
        'data_retorno': '2026-06-05',
        'tipo_solicitacao_nome': 'Diárias + Passagens Aéreas',
        'id_serie_memorando': '2986',
        'justificativa_solicitante': justificativa_solicitante,
    }


@patch('app.services.diarias_sei_integration.requests.post')
def test_memorando_inclui_justificativa_solicitante(mock_post, app_ctx):
    """Quando justificativa_solicitante está preenchida, o HTML do memorando deve contê-la."""
    from app.services.diarias_sei_integration import gerar_memorando_diarias

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'IdDocumento': '999', 'DocumentoFormatado': 'DOC-999'}
    mock_post.return_value = mock_response

    texto_justificativa = 'Viagem urgente por convocação ministerial'
    dados = _build_dados_memorando(justificativa_solicitante=texto_justificativa)

    gerar_memorando_diarias('fake-token', '12345', dados)

    mock_post.assert_called_once()
    payload = mock_post.call_args[1].get('json') or mock_post.call_args[0][1] if len(mock_post.call_args[0]) > 1 else None
    if payload is None:
        payload = mock_post.call_args.kwargs.get('json')

    conteudo = payload['Conteudo']
    assert texto_justificativa in conteudo


@patch('app.services.diarias_sei_integration.requests.post')
def test_memorando_omite_justificativa_quando_vazia(mock_post, app_ctx):
    """Quando justificativa_solicitante é vazia, não deve haver parágrafo extra."""
    from app.services.diarias_sei_integration import gerar_memorando_diarias

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'IdDocumento': '888', 'DocumentoFormatado': 'DOC-888'}
    mock_post.return_value = mock_response

    dados = _build_dados_memorando(justificativa_solicitante='')

    gerar_memorando_diarias('fake-token', '12345', dados)

    mock_post.assert_called_once()
    payload = mock_post.call_args.kwargs.get('json')
    conteudo = payload['Conteudo']

    assert 'Texto padr' in conteudo
    lines = [l.strip() for l in conteudo.split('\n') if l.strip()]
    justificativa_paragraphs = [l for l in lines if l == '<p></p>']
    assert len(justificativa_paragraphs) == 0, "Não deve haver parágrafo vazio extra"
