"""
VINCULAR PROCESSO SEI — Testes (TDD Red → Green)
=================================================
Arquivo: tests/diarias/test_vincular_processo.py

Cobre TODOS os comportamentos da funcionalidade "Vincular Processo" no módulo
de Administração de Diárias:

  1. Verificação de protocolo via AJAX (GET endpoint)
  2. Vinculação completa: protocolo → consulta SEI → documentos → parse requisição → integrantes
  3. Mapeamento IdSerie → tipo_documento (SERIE_TIPO_DOCUMENTO_MAP)
  4. Parsing HTML da Requisição de Diárias (integrantes, CPF, diárias, banco, trecho)
  5. Detecção de tipo itinerário (Estadual PI/PI vs Nacional PI/SP)
  6. Persistência: sei_protocolo, sei_id_procedimento, link_processo_sei, especificacao_sei
  7. Etapa escolhida pelo usuário é respeitada (não auto-detecção)
  8. Criação de DiariasHistoricoMovimentacao (auditoria)
  9. Idempotência: vincular mesmo processo 2x não duplica documentos/integrantes
 10. Falha graciosa: SEI offline, processo não encontrado, sem token

Como rodar:
    pytest tests/diarias/test_vincular_processo.py -v
    pytest tests/diarias/test_vincular_processo.py -v -k "TestParsear"
"""
import json
import pytest
from decimal import Decimal
from datetime import date, datetime
from unittest.mock import patch, MagicMock


# ────────────────────────────────────────────────────────────────────────────
# HTML FIXTURE — Mínima Requisição de Diárias (ISO-8859-1 simulada)
# ────────────────────────────────────────────────────────────────────────────

HTML_REQUISICAO_DIARIAS = """
<!DOCTYPE html>
<html>
<head><title>REQUISIÇÃO DE DIÁRIAS Nº 42/2026 - SEI 00002.003034/2026-83</title></head>
<body>
<h2>REQUISIÇÃO DE DIÁRIAS Nº 42/2026</h2>
<p>Processo SEI 00002.003034/2026-83</p>
<table>
  <tr>
    <th>Matrícula</th>
    <th>Nome</th>
    <th>Cargo/Função</th>
    <th>Vínculo</th>
    <th>CPF</th>
    <th>Banco/Agência/Conta</th>
    <th>Quantidade de Diárias</th>
    <th>Valor Unitário</th>
    <th>Valor Total</th>
  </tr>
  <tr>
    <td>123456</td>
    <td>JOAO DA SILVA SANTOS</td>
    <td>ANALISTA</td>
    <td>Efetivo</td>
    <td>123.456.789-01</td>
    <td>AG. 3178-X CONTA CORRENTE: 28118-2 BANCO DO BRASIL</td>
    <td>2,5</td>
    <td>R$ 320,00</td>
    <td>R$ 800,00</td>
  </tr>
  <tr>
    <td>654321</td>
    <td>MARIA OLIVEIRA COSTA</td>
    <td>SECRETÁRIO</td>
    <td>Comissionado</td>
    <td>987.654.321-00</td>
    <td>AG. 0001-9 CONTA CORRENTE: 99999-0 CAIXA ECONOMICA</td>
    <td>2,5</td>
    <td>R$ 640,00</td>
    <td>R$ 1.600,00</td>
  </tr>
  <tr>
    <td colspan="8">TRECHO: TERESINA-PI/TERESINA-PI   PERÍODO: 01/05/2026 a 03/05/2026</td>
  </tr>
  <tr>
    <td colspan="8">OBJETIVO DA VIAGEM: Participação em reunião técnica sobre gestão de contratos</td>
  </tr>
  <tr>
    <td colspan="7">VALOR TOTAL:</td>
    <td>R$ 2.400,00</td>
  </tr>
</table>
</body>
</html>
""".encode('iso-8859-1')


HTML_REQUISICAO_NACIONAL = """
<!DOCTYPE html>
<html>
<head><title>REQUISIÇÃO DE DIÁRIAS Nº 55/2026 - SEI 00002.009000/2026-10</title></head>
<body>
<h2>REQUISIÇÃO DE DIÁRIAS Nº 55/2026</h2>
<p>Processo SEI 00002.009000/2026-10</p>
<table>
  <tr>
    <th>Matrícula</th>
    <th>Nome</th>
    <th>Cargo/Função</th>
    <th>Vínculo</th>
    <th>CPF</th>
    <th>Banco/Agência/Conta</th>
    <th>Quantidade de Diárias</th>
    <th>Valor Unitário</th>
    <th>Valor Total</th>
  </tr>
  <tr>
    <td>111111</td>
    <td>PEDRO ALVES JUNIOR</td>
    <td>GERENTE</td>
    <td>Efetivo</td>
    <td>111.111.111-11</td>
    <td>AG. 1234-0 CONTA CORRENTE: 56789-1 BANCO DO BRASIL</td>
    <td>3,5</td>
    <td>R$ 500,00</td>
    <td>R$ 1.750,00</td>
  </tr>
  <tr>
    <td colspan="8">TRECHO: TERESINA-PI/SÃO PAULO-SP   PERÍODO: 10/06/2026 a 13/06/2026</td>
  </tr>
  <tr>
    <td colspan="8">OBJETIVO DA VIAGEM: Capacitação em fiscalização de contratos</td>
  </tr>
  <tr>
    <td colspan="7">VALOR TOTAL:</td>
    <td>R$ 1.750,00</td>
  </tr>
</table>
</body>
</html>
""".encode('iso-8859-1')


# ────────────────────────────────────────────────────────────────────────────
# Helper: cria itinerário mínimo para testes
# ────────────────────────────────────────────────────────────────────────────

def _criar_itinerario_base(db_session):
    """Cria itinerário sem processo SEI vinculado."""
    from app.models.diaria import DiariasItinerario
    it = DiariasItinerario(
        usuario_gerador='usuario.teste',
        tipo_solicitacao_id=1,
        tipo_itinerario=1,
        status_id=1,
        data_solicitacao=date.today(),
        data_viagem=datetime(2026, 5, 1),
        data_retorno=datetime(2026, 5, 3),
        qtd_diarias_solicitadas=Decimal('2.5'),
    )
    db_session.add(it)
    db_session.flush()
    return it


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 1 — Parse da Requisição de Diárias (lógica pura, sem HTTP)
# ════════════════════════════════════════════════════════════════════════════

class TestParsearRequisicaoDiarias:
    """
    Testa o parser HTML da Requisição de Diárias.

    Antes da implementação: ImportError (módulo não existe).
    Após a implementação: todos passam.
    """

    def test_importar_modulo_parser(self):
        """O módulo app.services.requisicao_parser deve existir."""
        try:
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
        except ImportError:
            pytest.fail(
                "app.services.requisicao_parser não encontrado. "
                "Crie o arquivo app/services/requisicao_parser.py com a função "
                "parsear_html_requisicao_diarias(html_bytes) -> dict."
            )

    def test_parsear_retorna_dois_integrantes(self, app):
        """HTML com 2 linhas de integrantes → lista com 2 dicts."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            assert len(resultado['integrantes']) == 2, (
                f"Esperado 2 integrantes, obtido {len(resultado['integrantes'])}. "
                f"Integrantes: {resultado['integrantes']}"
            )

    def test_parsear_extrai_cpf_primeiro_integrante(self, app):
        """CPF do primeiro integrante deve ser extraído corretamente."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            cpf = resultado['integrantes'][0]['cpf']
            # Remove pontuação para comparar apenas os dígitos
            cpf_digits = ''.join(filter(str.isdigit, cpf))
            assert cpf_digits == '12345678901', (
                f"CPF incorreto: '{cpf}' (dígitos: '{cpf_digits}'). Esperado '12345678901'."
            )

    def test_parsear_extrai_nome(self, app):
        """Nome do primeiro integrante deve ser extraído."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            nome = resultado['integrantes'][0]['nome']
            assert nome, f"Nome do integrante não foi extraído: '{nome}'"
            assert 'JOAO' in nome.upper() or 'SILVA' in nome.upper(), (
                f"Nome inesperado: '{nome}'. Esperava 'JOAO DA SILVA SANTOS'."
            )

    def test_parsear_extrai_qtd_diarias(self, app):
        """Quantidade de diárias '2,5' deve ser convertida para float 2.5."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            qtd = resultado['integrantes'][0]['qtd_diarias']
            assert float(qtd) == 2.5, (
                f"Quantidade de diárias incorreta: {qtd}. Esperado 2.5."
            )

    def test_parsear_extrai_valor_unitario(self, app):
        """Valor unitário 'R$ 320,00' deve ser convertido para Decimal('320.00')."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            vu = resultado['integrantes'][0]['valor_unitario']
            assert Decimal(str(vu)) == Decimal('320.00'), (
                f"Valor unitário incorreto: {vu}. Esperado 320.00."
            )

    def test_parsear_extrai_trecho_estadual(self, app):
        """Trecho 'TERESINA-PI/TERESINA-PI' deve ser extraído."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            trecho = resultado['trecho']
            assert trecho, f"Trecho não extraído: '{trecho}'"
            assert 'PI' in trecho.upper(), (
                f"Trecho não contém 'PI': '{trecho}'"
            )

    def test_parsear_detecta_periodo_inicio(self, app):
        """Data de início '01/05/2026' deve ser extraída."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            inicio = resultado['periodo_inicio']
            assert inicio is not None, "período_inicio não foi extraído."
            # Pode ser datetime ou date — apenas verifica o dia
            dia = inicio.day if hasattr(inicio, 'day') else None
            assert dia == 1, f"Dia de início esperado 1, obtido {dia}."

    def test_parsear_extrai_valor_total_geral(self, app):
        """Valor total geral 'R$ 2.400,00' deve ser extraído."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            total = resultado['valor_total_geral']
            assert Decimal(str(total)) == Decimal('2400.00'), (
                f"Valor total geral incorreto: {total}. Esperado 2400.00."
            )

    def test_parsear_extrai_banco_agencia(self, app):
        """Banco/agência deve ser extraído da célula de banco info."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            intg = resultado['integrantes'][0]
            assert intg.get('banco_agencia'), (
                f"Agência bancária não extraída. Integrante: {intg}"
            )

    def test_parsear_html_nacional_dois_estados_distintos(self, app):
        """Trecho PI/SP → estado_origem=22 (PI) e estado_destino=35 (SP)."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_NACIONAL)
            # cod_ibge PI=22, SP=35
            assert resultado['estado_origem'] == 22, (
                f"estado_origem esperado 22 (PI), obtido {resultado['estado_origem']}."
            )
            assert resultado['estado_destino'] == 35, (
                f"estado_destino esperado 35 (SP), obtido {resultado['estado_destino']}."
            )

    def test_parsear_html_estadual_mesmo_estado(self, app):
        """Trecho PI/PI → estado_origem=22 e estado_destino=22."""
        with app.app_context():
            from app.services.requisicao_parser import parsear_html_requisicao_diarias
            resultado = parsear_html_requisicao_diarias(HTML_REQUISICAO_DIARIAS)
            assert resultado['estado_origem'] == 22, (
                f"estado_origem esperado 22 (PI), obtido {resultado['estado_origem']}."
            )
            assert resultado['estado_destino'] == 22, (
                f"estado_destino esperado 22 (PI), obtido {resultado['estado_destino']}."
            )


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 2 — Detecção de tipo de itinerário
# ════════════════════════════════════════════════════════════════════════════

class TestDetectarTipoItinerario:
    """
    Testa a função que decide Estadual(1) vs Nacional(2) vs Internacional(3)
    com base no estado de destino extraído do trecho.
    """

    def test_importar_funcao_detectar_tipo(self):
        """A função detectar_tipo_itinerario deve existir no serviço."""
        try:
            from app.services.vincular_processo_diaria import detectar_tipo_itinerario
        except ImportError:
            pytest.fail(
                "Função detectar_tipo_itinerario não encontrada. "
                "Crie em app/services/vincular_processo_diaria.py."
            )

    def test_destino_piaui_e_estadual(self):
        """Estado destino = 22 (Piauí) → tipo_itinerario = 1 (Estadual)."""
        from app.services.vincular_processo_diaria import detectar_tipo_itinerario
        assert detectar_tipo_itinerario(22, 22) == 1, (
            "Destino PI (22) deveria resultar em tipo 1 (Estadual)."
        )

    def test_destino_outro_estado_e_nacional(self):
        """Estado destino = 35 (SP) → tipo_itinerario = 2 (Nacional)."""
        from app.services.vincular_processo_diaria import detectar_tipo_itinerario
        assert detectar_tipo_itinerario(22, 35) == 2, (
            "Destino SP (35) deveria resultar em tipo 2 (Nacional)."
        )

    def test_sem_destino_retorna_none(self):
        """Sem estado destino → retorna None (não altera tipo)."""
        from app.services.vincular_processo_diaria import detectar_tipo_itinerario
        assert detectar_tipo_itinerario(22, None) is None, (
            "Sem destino deveria retornar None para não alterar o tipo."
        )

    def test_origem_none_e_destino_piaui_e_estadual(self):
        """Sem origem mas com destino PI → tipo 1 (Estadual)."""
        from app.services.vincular_processo_diaria import detectar_tipo_itinerario
        assert detectar_tipo_itinerario(None, 22) == 1


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 3 — Serviço de verificação de protocolo (AJAX)
# ════════════════════════════════════════════════════════════════════════════

class TestVerificarProtocoloSei:
    """
    Testa a função verificar_protocolo_sei() usada pelo endpoint AJAX.
    Toda chamada SEI é mockada.
    """

    def test_importar_funcao_verificar(self):
        """A função verificar_protocolo_sei deve existir."""
        try:
            from app.services.vincular_processo_diaria import verificar_protocolo_sei
        except ImportError:
            pytest.fail(
                "Função verificar_protocolo_sei não encontrada em "
                "app/services/vincular_processo_diaria.py."
            )

    def test_protocolo_vazio_retorna_erro(self, app):
        """Protocolo vazio → {sucesso: False, erro: '...'}."""
        with app.app_context():
            from app.services.vincular_processo_diaria import verificar_protocolo_sei
            resultado = verificar_protocolo_sei('')
            assert resultado['sucesso'] is False
            assert resultado.get('erro'), (
                "Resultado deveria conter 'erro' para protocolo vazio."
            )

    def test_protocolo_valido_encontrado(self, app):
        """
        Protocolo válido + SEI retorna 200 → {sucesso: True, protocolo_formatado, especificacao}.
        """
        with app.app_context():
            from app.services.vincular_processo_diaria import verificar_protocolo_sei

            mock_resultado = {
                'sucesso': True,
                'protocolo_formatado': '00002.003034/2026-83',
                'id_procedimento': '12345',
                'link_acesso': 'https://sei.pi.gov.br/...',
                'especificacao': 'SOLICITACAO DE DIARIAS - ANALISTA - 123456 - Estadual',
                'dados_procedimento': {},
                'erro': None,
            }

            with patch(
                'app.services.vincular_processo_diaria.consultar_procedimento_sei',
                return_value=mock_resultado
            ), patch(
                'app.services.vincular_processo_diaria.gerar_token_sei_admin',
                return_value='TOKEN_MOCK'
            ):
                resultado = verificar_protocolo_sei('00002.003034/2026-83')

            assert resultado['sucesso'] is True, (
                f"Deveria ter sucesso com processo válido. Erro: {resultado.get('erro')}"
            )
            assert resultado.get('protocolo_formatado') == '00002.003034/2026-83'

    def test_processo_nao_encontrado_no_sei(self, app):
        """SEI retorna erro → {sucesso: False, erro: mensagem}."""
        with app.app_context():
            from app.services.vincular_processo_diaria import verificar_protocolo_sei

            mock_falha = {
                'sucesso': False,
                'protocolo_formatado': '',
                'id_procedimento': '',
                'link_acesso': '',
                'especificacao': '',
                'dados_procedimento': None,
                'erro': 'Processo não encontrado no SEI (HTTP 404).',
            }

            with patch(
                'app.services.vincular_processo_diaria.consultar_procedimento_sei',
                return_value=mock_falha
            ), patch(
                'app.services.vincular_processo_diaria.gerar_token_sei_admin',
                return_value='TOKEN_MOCK'
            ):
                resultado = verificar_protocolo_sei('00000.000000/0000-00')

            assert resultado['sucesso'] is False
            assert resultado.get('erro'), "Deveria retornar descrição do erro."

    def test_sem_token_sei_retorna_erro(self, app):
        """Falha de autenticação SEI → {sucesso: False, erro: '...'}."""
        with app.app_context():
            from app.services.vincular_processo_diaria import verificar_protocolo_sei

            with patch(
                'app.services.vincular_processo_diaria.gerar_token_sei_admin',
                return_value=None
            ):
                resultado = verificar_protocolo_sei('00002.003034/2026-83')

            assert resultado['sucesso'] is False
            assert resultado.get('erro'), "Sem token deve retornar erro."


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 4 — Serviço de vinculação completo
# ════════════════════════════════════════════════════════════════════════════

class TestVincularProcessoService:
    """
    Testa vincular_processo_sei() com mocks das chamadas HTTP/SEI.
    Verifica efeitos colaterais no banco (sem tocar o SEI real).
    """

    def _mock_sei_completo(self):
        """Retorna um conjunto de patches para simular SEI funcionando."""
        return {
            'token': 'TOKEN_MOCK',
            'procedimento': {
                'sucesso': True,
                'protocolo_formatado': '00002.003034/2026-83',
                'id_procedimento': '99001',
                'link_acesso': 'https://sei.pi.gov.br/proc/99001',
                'especificacao': 'SOLICITACAO DE DIARIAS - ANALISTA - Estadual',
                'dados_procedimento': {},
                'erro': None,
            },
            'documentos': {
                'sucesso': True,
                'documentos': [
                    {
                        'IdDocumento': '777001',
                        'DocumentoFormatado': '0000777001',
                        'Serie': {'IdSerie': '532', 'Nome': 'Requisição de Diárias'},
                        'Descricao': '',
                        'Numero': '42',
                    },
                    {
                        'IdDocumento': '777002',
                        'DocumentoFormatado': '0000777002',
                        'Serie': {'IdSerie': '2986', 'Nome': 'Memorando SGA'},
                        'Descricao': '',
                        'Numero': '',
                    },
                ],
                'erro': None,
            },
        }

    def test_importar_funcao_vincular(self):
        """A função vincular_processo_sei deve existir."""
        try:
            from app.services.vincular_processo_diaria import vincular_processo_sei
        except ImportError:
            pytest.fail(
                "Função vincular_processo_sei não encontrada em "
                "app/services/vincular_processo_diaria.py."
            )

    def test_vincular_salva_sei_protocolo(self, db_session, app):
        """Após vincular, itinerario.sei_protocolo deve estar preenchido."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                resultado = vincular_processo_sei(
                    itinerario_id=it.id,
                    protocolo_sei='00002.003034/2026-83',
                    etapa_id=1,
                    usuario_id=1,
                )

            assert resultado['sucesso'] is True, (
                f"vincular_processo_sei falhou: {resultado.get('erro')}"
            )
            db_session.refresh(it)
            assert it.sei_protocolo == '00002.003034/2026-83', (
                f"sei_protocolo não foi salvo: '{it.sei_protocolo}'"
            )

    def test_vincular_salva_id_procedimento(self, db_session, app):
        """Após vincular, itinerario.sei_id_procedimento deve estar preenchido."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            db_session.refresh(it)
            assert it.sei_id_procedimento == '99001', (
                f"sei_id_procedimento esperado '99001', obtido '{it.sei_id_procedimento}'"
            )

    def test_vincular_salva_link_processo_sei(self, db_session, app):
        """Após vincular, itinerario.link_processo_sei deve estar preenchido."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            db_session.refresh(it)
            assert it.link_processo_sei, (
                "link_processo_sei não foi salvo. "
                "Adicione o campo link_processo_sei ao modelo DiariasItinerario."
            )

    def test_vincular_respeita_etapa_escolhida(self, db_session, app):
        """Etapa 3 escolhida pelo usuário → itinerario.etapa_atual_id = 3."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            from app.models.diaria import DiariasEtapa
            # Garante que etapa 3 existe
            db_session.add(DiariasEtapa(id=3, nome='Análise 1ª Parte', alias='analise_1', ordem=3))
            db_session.flush()

            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                vincular_processo_sei(it.id, '00002.003034/2026-83', etapa_id=3, usuario_id=1)

            db_session.refresh(it)
            assert it.etapa_atual_id == 3, (
                f"etapa_atual_id deveria ser 3 (escolha do usuário), obtido {it.etapa_atual_id}."
            )

    def test_vincular_cria_documentos_sei(self, db_session, app):
        """Docs do SEI são mapeados e salvos como DiariasDocumentoSei."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            from app.models.diaria import DiariasDocumentoSei
            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            docs = DiariasDocumentoSei.query.filter_by(itinerario_id=it.id).all()
            tipos = {d.tipo_documento for d in docs}

            # IdSerie 532 → 'requisicao', 2986 → 'memorando'
            assert 'requisicao' in tipos, (
                f"Documento 'requisicao' (IdSerie 532) não foi criado. Tipos: {tipos}"
            )
            assert 'memorando' in tipos, (
                f"Documento 'memorando' (IdSerie 2986) não foi criado. Tipos: {tipos}"
            )

    def test_vincular_cria_historico_movimentacao(self, db_session, app):
        """Vincular processo deve criar registro em DiariasHistoricoMovimentacao."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            from app.models.diaria import DiariasHistoricoMovimentacao, DiariasEtapa
            db_session.add(DiariasEtapa(id=1, nome='Solicitação', alias='solicitacao', ordem=1))
            db_session.flush()

            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, usuario_id=1)

            hists = DiariasHistoricoMovimentacao.query.filter_by(id_itinerario=it.id).all()
            assert len(hists) >= 1, (
                "vincular_processo_sei deve registrar pelo menos 1 movimentação no histórico."
            )
            comentarios = ' '.join(h.comentario or '' for h in hists).lower()
            assert 'vinculado' in comentarios or 'processo' in comentarios, (
                f"Comentário do histórico deveria mencionar vinculação. "
                f"Comentários: {[h.comentario for h in hists]}"
            )

    def test_vincular_cria_integrantes_da_requisicao(self, db_session, app):
        """
        Após download e parse da Requisição (IdSerie 532), os integrantes
        devem ser inseridos/atualizados em DiariasItemItinerario.
        """
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            from app.models.diaria import DiariasItemItinerario
            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            itens = DiariasItemItinerario.query.filter_by(id_itinerario=it.id).all()
            assert len(itens) == 2, (
                f"HTML tem 2 integrantes, mas {len(itens)} foram inseridos em DiariasItemItinerario."
            )

    def test_vincular_detecta_tipo_estadual_para_pi_pi(self, db_session, app):
        """
        Requisição com trecho PI/PI → tipo_itinerario do itinerário atualizado para 1 (Estadual).
        """
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            it = _criar_itinerario_base(db_session)
            it.tipo_itinerario = 2  # começa como Nacional
            db_session.flush()
            mocks = self._mock_sei_completo()

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):  # PI/PI = Estadual

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            db_session.refresh(it)
            assert it.tipo_itinerario == 1, (
                f"Trecho PI/PI deveria resultar em tipo_itinerario=1 (Estadual), "
                f"obtido {it.tipo_itinerario}."
            )

    def test_vincular_detecta_tipo_nacional_para_pi_sp(self, db_session, app):
        """
        Requisição com trecho PI/SP → tipo_itinerario atualizado para 2 (Nacional).
        """
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            it = _criar_itinerario_base(db_session)
            it.tipo_itinerario = 1  # começa como Estadual
            db_session.flush()

            mocks = self._mock_sei_completo()
            # Documentos com requisição nacional
            mocks_nac = dict(mocks)
            mocks_nac['documentos'] = {
                'sucesso': True,
                'documentos': [
                    {
                        'IdDocumento': '888001',
                        'DocumentoFormatado': '0000888001',
                        'Serie': {'IdSerie': '532', 'Nome': 'Requisição de Diárias'},
                        'Descricao': '',
                        'Numero': '55',
                    },
                ],
                'erro': None,
            }

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks_nac['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_NACIONAL):  # PI/SP = Nacional

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            db_session.refresh(it)
            assert it.tipo_itinerario == 2, (
                f"Trecho PI/SP deveria resultar em tipo_itinerario=2 (Nacional), "
                f"obtido {it.tipo_itinerario}."
            )

    def test_vincular_idempotente_documentos(self, db_session, app):
        """Chamar vincular_processo_sei 2x com mesmo processo não duplica DiariasDocumentoSei."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            from app.models.diaria import DiariasDocumentoSei
            it = _criar_itinerario_base(db_session)
            mocks = self._mock_sei_completo()

            patches = dict(
                gerar_token_sei_admin=mocks['token'],
                consultar_procedimento_sei=mocks['procedimento'],
                listar_documentos_procedimento_sei=mocks['documentos'],
                baixar_documento_sei=HTML_REQUISICAO_DIARIAS,
            )

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=mocks['token']), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mocks['procedimento']), \
                 patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
                       return_value=mocks['documentos']), \
                 patch('app.services.vincular_processo_diaria.baixar_documento_sei',
                       return_value=HTML_REQUISICAO_DIARIAS):

                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)
                vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            docs = DiariasDocumentoSei.query.filter_by(itinerario_id=it.id).all()
            tipos_count = {}
            for d in docs:
                tipos_count[d.tipo_documento] = tipos_count.get(d.tipo_documento, 0) + 1

            duplicados = {t: c for t, c in tipos_count.items() if c > 1}
            assert not duplicados, (
                f"Documentos duplicados após 2 chamadas: {duplicados}. "
                "set_doc() deve ser idempotente."
            )

    def test_vincular_sem_token_retorna_erro(self, db_session, app):
        """Sem token SEI, vincular_processo_sei retorna {sucesso: False}."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            it = _criar_itinerario_base(db_session)

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=None):
                resultado = vincular_processo_sei(it.id, '00002.003034/2026-83', 1, 1)

            assert resultado['sucesso'] is False, (
                "Sem token SEI deveria retornar sucesso=False."
            )
            assert resultado.get('erro'), "Deve conter mensagem de erro."
            # Banco não deve ter sido alterado
            db_session.refresh(it)
            assert it.sei_protocolo is None, (
                "sei_protocolo não deveria ter sido salvo com falha de autenticação."
            )

    def test_vincular_processo_nao_encontrado_retorna_erro(self, db_session, app):
        """Processo não encontrado no SEI → {sucesso: False}, sem alterações no banco."""
        with app.app_context():
            from app.services.vincular_processo_diaria import vincular_processo_sei
            it = _criar_itinerario_base(db_session)

            mock_falha = {
                'sucesso': False,
                'protocolo_formatado': '',
                'id_procedimento': '',
                'link_acesso': '',
                'especificacao': '',
                'dados_procedimento': None,
                'erro': 'Processo não encontrado no SEI (HTTP 404).',
            }

            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value='TOKEN_MOCK'), \
                 patch('app.services.vincular_processo_diaria.consultar_procedimento_sei',
                       return_value=mock_falha):
                resultado = vincular_processo_sei(it.id, '00000.000000/0000-00', 1, 1)

            assert resultado['sucesso'] is False
            db_session.refresh(it)
            assert it.sei_protocolo is None


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 5 — Endpoint AJAX de verificação de protocolo
# ════════════════════════════════════════════════════════════════════════════

class TestEndpointVerificarProcesso:
    """
    Testa o endpoint HTTP GET /diarias/api/verificar-processo-sei.
    Retorna JSON.
    """

    def _login_admin(self, client, db_session):
        """Cria usuário admin único e faz login via session."""
        import uuid
        from app.models.usuario import Usuario
        uid = uuid.uuid4().hex[:12]
        u = Usuario(
            id_usuario_sei=f'verify_{uid}',
            nome='API Verify Admin',
            sigla_login=f'api_admin_{uid}',
            is_admin=True,
            ativo=True,
        )
        db_session.add(u)
        db_session.flush()
        with client.session_transaction() as sess:
            sess['_user_id'] = str(u.id)
        return u

    def test_endpoint_existe(self, client, app, db_session):
        """GET /diarias/api/verificar-processo-sei deve existir (não 404)."""
        with app.app_context():
            self._login_admin(client, db_session)
            with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin',
                       return_value=None):
                resp = client.get(
                    '/diarias/api/verificar-processo-sei',
                    query_string={'protocolo': '00002.003034/2026-83'}
                )
            assert resp.status_code != 404, (
                "Endpoint GET /diarias/api/verificar-processo-sei não encontrado. "
                "Crie a rota em app/diarias/routes/admin.py."
            )

    def test_endpoint_retorna_json(self, client, app, db_session):
        """Endpoint deve retornar Content-Type application/json."""
        with app.app_context():
            self._login_admin(client, db_session)
            with patch('app.services.vincular_processo_diaria.verificar_protocolo_sei',
                       return_value={'sucesso': False, 'erro': 'sem token'}):
                resp = client.get(
                    '/diarias/api/verificar-processo-sei',
                    query_string={'protocolo': 'teste'}
                )
            if resp.status_code == 404:
                pytest.skip("Endpoint não encontrado.")
            assert 'application/json' in (resp.content_type or ''), (
                f"Content-Type esperado JSON, obtido '{resp.content_type}'."
            )

    def test_endpoint_protocolo_vazio_retorna_erro_json(self, client, app, db_session):
        """GET sem protocolo → JSON com sucesso=false e campo erro."""
        with app.app_context():
            self._login_admin(client, db_session)
            resp = client.get('/diarias/api/verificar-processo-sei')
            if resp.status_code == 404:
                pytest.skip("Endpoint não implementado ainda.")
            data = resp.get_json()
            assert data is not None, "Resposta não é JSON válido."
            assert data.get('sucesso') is False
            assert data.get('erro'), "Deve conter campo 'erro' para protocolo vazio."


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 6 — Rota POST de vinculação (administracao_detalhe)
# ════════════════════════════════════════════════════════════════════════════

class TestRotaVincularProcesso:
    """
    Testa POST /diarias/administracao/<id>/vincular-processo.
    """

    def _login_admin(self, client, app, db_session):
        """Faz login de usuário admin único para testes de rota."""
        import uuid
        from app.models.usuario import Usuario
        uid = uuid.uuid4().hex[:12]
        u = Usuario(
            id_usuario_sei=f'vincular_{uid}',
            nome='Admin Vincular',
            sigla_login=f'admin_{uid}',
            is_admin=True,
            ativo=True,
        )
        db_session.add(u)
        db_session.flush()
        with client.session_transaction() as sess:
            sess['_user_id'] = str(u.id)
        return u

    def test_rota_existe(self, client, app, db_session):
        """POST /diarias/administracao/<id>/vincular-processo deve existir (não 404/405)."""
        with app.app_context():
            it = _criar_itinerario_base(db_session)
            self._login_admin(client, app, db_session)

            with patch('app.services.vincular_processo_diaria.vincular_processo_sei',
                       return_value={'sucesso': True, 'msgs': []}):
                resp = client.post(
                    f'/diarias/administracao/{it.id}/vincular-processo',
                    data={'protocolo_sei': '00002.003034/2026-83', 'etapa_id': 1},
                    follow_redirects=False,
                )

            assert resp.status_code != 404, (
                f"Rota POST /diarias/administracao/{it.id}/vincular-processo não encontrada. "
                "Crie a rota em app/diarias/routes/admin.py."
            )

    def test_post_sem_protocolo_redireciona_com_erro(self, client, app, db_session):
        """POST sem protocolo_sei → redirect com flash de erro."""
        with app.app_context():
            it = _criar_itinerario_base(db_session)
            self._login_admin(client, app, db_session)

            resp = client.post(
                f'/diarias/administracao/{it.id}/vincular-processo',
                data={'protocolo_sei': '', 'etapa_id': 1},
                follow_redirects=True,
            )
            if resp.status_code == 404:
                pytest.skip("Rota ainda não implementada.")
            # Deve redirecionar com flash de erro
            html = resp.data.decode('utf-8', errors='replace')
            assert 'alert' in html.lower() or 'danger' in html.lower() or 'erro' in html.lower(), (
                "POST sem protocolo deveria exibir mensagem de erro."
            )

    def test_post_sem_etapa_redireciona_com_erro(self, client, app, db_session):
        """POST sem etapa_id → redirect com flash de erro."""
        with app.app_context():
            it = _criar_itinerario_base(db_session)
            self._login_admin(client, app, db_session)

            resp = client.post(
                f'/diarias/administracao/{it.id}/vincular-processo',
                data={'protocolo_sei': '00002.003034/2026-83', 'etapa_id': ''},
                follow_redirects=True,
            )
            if resp.status_code == 404:
                pytest.skip("Rota ainda não implementada.")
            html = resp.data.decode('utf-8', errors='replace')
            assert 'alert' in html.lower() or 'danger' in html.lower() or 'erro' in html.lower(), (
                "POST sem etapa deveria exibir mensagem de erro."
            )

    def test_post_sucesso_redireciona_para_detalhe(self, client, app, db_session):
        """POST bem-sucedido → redirect 302 para /diarias/administracao/<id>."""
        with app.app_context():
            it = _criar_itinerario_base(db_session)
            self._login_admin(client, app, db_session)

            with patch('app.services.vincular_processo_diaria.vincular_processo_sei',
                       return_value={'sucesso': True, 'msgs': ['Processo vinculado.']}):
                resp = client.post(
                    f'/diarias/administracao/{it.id}/vincular-processo',
                    data={'protocolo_sei': '00002.003034/2026-83', 'etapa_id': '1'},
                    follow_redirects=False,
                )

            if resp.status_code == 404:
                pytest.skip("Rota ainda não implementada.")

            assert resp.status_code in (302, 303), (
                f"POST bem-sucedido deveria redirecionar (302/303), obtido {resp.status_code}."
            )
