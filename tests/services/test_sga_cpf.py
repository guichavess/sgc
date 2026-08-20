"""
Busca de servidor no SGA parava de encontrar qualquer CPF (producao, 20/08/2026).

Causa raiz: a API pessoaSGA do Gestor SEAD exige o CPF FORMATADO
('992.027.433-04'). O SGAService limpava a pontuacao antes de enviar, e a API
respondia HTTP 200 com corpo '[]' — indistinguivel de "nao encontrado". Como
nao havia excecao, nada era logado e o erro passou despercebido.

Verificado em producao:
    -d 'cpf=99202743304'    -> [] (200)
    -d 'cpf=992.027.433-04' -> {"matricula": "...", "nome": "...", ...} (200)

Como rodar:
    pytest tests/services/test_sga_cpf.py -v
"""
import pytest

from app.services.sga_service import SGAService
from app.utils.cpf import formatar_cpf, limpar_cpf


class TestFormatacaoCpf:
    """Utilitario de CPF — normaliza entre os dois formatos que circulam."""

    def test_formatar_a_partir_de_digitos(self):
        assert formatar_cpf('99202743304') == '992.027.433-04'

    def test_formatar_mantem_cpf_ja_formatado(self):
        assert formatar_cpf('992.027.433-04') == '992.027.433-04'

    def test_formatar_preserva_zeros_a_esquerda(self):
        # CPF com zero inicial nao pode perder o digito
        assert formatar_cpf('06387512336') == '063.875.123-36'

    def test_formatar_retorna_none_para_tamanho_invalido(self):
        assert formatar_cpf('123') is None
        assert formatar_cpf('') is None
        assert formatar_cpf(None) is None

    def test_limpar_remove_pontuacao(self):
        assert limpar_cpf('992.027.433-04') == '99202743304'
        assert limpar_cpf('99202743304') == '99202743304'


class _RespostaFake:
    """Resposta minima de requests.post usada nos mocks."""

    def __init__(self, status_code=200, dados=None):
        self.status_code = status_code
        self._dados = dados if dados is not None else []

    def json(self):
        return self._dados

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.exceptions.HTTPError(f'{self.status_code} Error')


_PESSOA_API = {
    'matricula': '427189-X',
    'cpf': '992.027.433-04',
    'nome': 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA',
    'banco_agencia': '3178-X',
    'banco_conta': '28118-2',
    'vinculo': 'Estatutário Comissionado',
    'cargo': 'Superintendente',
    'setor': None,
    'superintendencia': '',
    'orgao': 'SEADPREVPI',
    'cod_sefaz_orgao': '002',
}


@pytest.fixture
def config_sga(app):
    """Garante hashkey configurada — sem ela o service retorna None antes do POST."""
    app.config['SGA_API_HASHKEY'] = 'hash-de-teste'
    return app


class TestPayloadEnviadoAoSGA:
    """O CPF precisa sair FORMATADO no corpo do POST."""

    def test_envia_cpf_formatado_quando_recebe_digitos(self, config_sga, monkeypatch):
        capturado = {}

        def fake_post(url, headers=None, data=None, verify=None, timeout=None):
            capturado['data'] = data
            return _RespostaFake(200, _PESSOA_API)

        monkeypatch.setattr('app.services.sga_service.requests.post', fake_post)

        with config_sga.app_context():
            SGAService.buscar_pessoa_por_cpf('99202743304')

        assert capturado['data']['cpf'] == '992.027.433-04', (
            'A API pessoaSGA responde [] para CPF sem pontuacao. '
            'O payload deve enviar o CPF formatado.'
        )

    def test_envia_cpf_formatado_quando_ja_recebe_formatado(self, config_sga, monkeypatch):
        capturado = {}

        def fake_post(url, headers=None, data=None, verify=None, timeout=None):
            capturado['data'] = data
            return _RespostaFake(200, _PESSOA_API)

        monkeypatch.setattr('app.services.sga_service.requests.post', fake_post)

        with config_sga.app_context():
            SGAService.buscar_pessoa_por_cpf('992.027.433-04')

        assert capturado['data']['cpf'] == '992.027.433-04'

    def test_retorna_dados_da_pessoa_encontrada(self, config_sga, monkeypatch):
        monkeypatch.setattr(
            'app.services.sga_service.requests.post',
            lambda *a, **kw: _RespostaFake(200, _PESSOA_API),
        )

        with config_sga.app_context():
            pessoa = SGAService.buscar_pessoa_por_cpf('99202743304')

        assert pessoa is not None
        assert pessoa['nome'] == 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA'
        assert pessoa['matricula'] == '427189-X'
        # setor vem null da API — nao pode virar None cru no retorno
        assert pessoa['setor'] == ''

    def test_lista_vazia_continua_significando_nao_encontrado(self, config_sga, monkeypatch):
        monkeypatch.setattr(
            'app.services.sga_service.requests.post',
            lambda *a, **kw: _RespostaFake(200, []),
        )

        with config_sga.app_context():
            assert SGAService.buscar_pessoa_por_cpf('99202743304') is None

    def test_cpf_invalido_nao_chama_api(self, config_sga, monkeypatch):
        def nao_deve_chamar(*a, **kw):
            pytest.fail('CPF invalido nao deve gerar requisicao ao SGA.')

        monkeypatch.setattr('app.services.sga_service.requests.post', nao_deve_chamar)

        with config_sga.app_context():
            assert SGAService.buscar_pessoa_por_cpf('123') is None
