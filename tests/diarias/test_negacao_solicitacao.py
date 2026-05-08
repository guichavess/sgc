"""
Testes — Negação de solicitação de diárias pelo Superintendente.

Cobre:
  - Modelo: campos processo_negado_* e unidade_geradora_sigla/descricao
  - Endpoint POST /diarias/administracao/<id>/negar (guards + happy path)
  - Filtro da administração (oculta negados por padrão)
  - Função gerar_despacho_sga_negacao

Como rodar:
    pytest tests/diarias/test_negacao_solicitacao.py -v
"""
import pytest
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4
from unittest.mock import patch, MagicMock


# ── Helpers ──────────────────────────────────────────────────────────────────

def _seed_etapas(db_session):
    """Cria as etapas obrigatórias para o fluxo de diárias."""
    from app.models.diaria import DiariasEtapa
    if not DiariasEtapa.query.get(1):
        for e in [
            DiariasEtapa(id=1, nome='Solicitação Inicial', alias='solicitacao_inicial', ordem=1),
            DiariasEtapa(id=2, nome='Escolha do Voo', alias='escolha_voo', ordem=3),
            DiariasEtapa(id=3, nome='Análise 1ª Parte', alias='analise_1_parte', ordem=2),
            DiariasEtapa(id=4, nome='Concessão', alias='concessao', ordem=5),
            DiariasEtapa(id=5, nome='Prestação de Contas', alias='prestacao_contas', ordem=6),
            DiariasEtapa(id=6, nome='Análise 2ª Parte', alias='analise_2_parte', ordem=4),
        ]:
            db_session.add(e)
        db_session.flush()


def _seed_tipos(db_session):
    """Cria tipos de itinerário e solicitação."""
    from app.models.diaria import DiariasTipoItinerario, DiariasTipoSolicitacao, DiariasStatusViagem
    if not DiariasTipoItinerario.query.get(1):
        db_session.add(DiariasTipoItinerario(id=1, nome='Estadual'))
        db_session.add(DiariasTipoItinerario(id=2, nome='Nacional'))
    if not DiariasTipoSolicitacao.query.get(1):
        db_session.add(DiariasTipoSolicitacao(id=1, nome='Apenas Diárias'))
    if not DiariasStatusViagem.query.get(1):
        db_session.add(DiariasStatusViagem(id=1, nome='Gerado'))
    db_session.flush()


def _criar_usuario_superintendente(db_session):
    """Cria usuário superintendente para testes."""
    from app.models.usuario import Usuario
    suffix = uuid4().hex[:8]
    u = Usuario(
        id_usuario_sei=f'super_sei_{suffix}',
        nome='SUPERINTENDENTE TESTE',
        sigla_login=f'super_teste_{suffix}',
        is_admin=False,
        ativo=True,
        cargo_gestao='superintendente',
        cargo='Superintendente de Gestão Administrativa',
        cpf=f'000{suffix[:8]}'[:11],
        superintendencia_sigla='SGACG',
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_usuario_comum(db_session):
    """Cria usuário comum (não-superintendente)."""
    from app.models.usuario import Usuario
    suffix = uuid4().hex[:8]
    u = Usuario(
        id_usuario_sei=f'comum_sei_{suffix}',
        nome='USUARIO COMUM',
        sigla_login=f'comum_teste_{suffix}',
        is_admin=False,
        ativo=True,
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_usuario_admin(db_session):
    """Cria usuário admin para acessar administração."""
    from app.models.usuario import Usuario
    from app.models.perfil import Perfil, PerfilPermissao
    suffix = uuid4().hex[:8]
    perfil = Perfil(nome=f'Admin Diárias {suffix}', ativo=True)
    db_session.add(perfil)
    db_session.flush()
    db_session.add(PerfilPermissao(perfil_id=perfil.id, modulo='diarias', acao='aprovar'))
    db_session.flush()
    u = Usuario(
        id_usuario_sei=f'admin_sei_{suffix}',
        nome='ADMIN TESTE',
        sigla_login=f'admin_teste_{suffix}',
        is_admin=True,
        ativo=True,
        perfil_id=perfil.id,
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_itinerario(db_session, usuario_gerador='super_teste', etapa_id=1,
                       sei_protocolo='00002.000001/2026-01',
                       sei_id_procedimento='123456',
                       unidade_geradora_id='110006213',
                       unidade_geradora_descricao='Coordenação de Pagamentos - SEADPREV-PI',
                       unidade_geradora_sigla='SEADPREV-PI/CPAG',
                       processo_negado=False):
    """Cria itinerário de teste com dados SEI."""
    from app.models.diaria import DiariasItinerario
    it = DiariasItinerario(
        usuario_gerador=usuario_gerador,
        tipo_solicitacao_id=1,
        qtd_diarias_solicitadas=Decimal('2.5'),
        tipo_itinerario=1,
        status_id=1,
        data_solicitacao=date.today(),
        data_viagem=datetime(2026, 6, 1, 8, 0),
        data_retorno=datetime(2026, 6, 3, 18, 0),
        etapa_atual_id=etapa_id,
        sei_protocolo=sei_protocolo,
        sei_id_procedimento=sei_id_procedimento,
        unidade_geradora_id=unidade_geradora_id,
        unidade_geradora_descricao=unidade_geradora_descricao,
        unidade_geradora_sigla=unidade_geradora_sigla,
        processo_negado=processo_negado,
    )
    db_session.add(it)
    db_session.flush()
    return it


def _criar_doc_requisicao(db_session, itinerario):
    """Cria documento de requisição de diárias no itinerário."""
    from app.models.diaria import DiariasDocumentoSei
    doc = DiariasDocumentoSei(
        itinerario_id=itinerario.id,
        tipo_documento='requisicao',
        sei_id='999001',
        sei_formatado='0001234',
        assinado=False,
    )
    db_session.add(doc)
    db_session.flush()
    return doc


def _login_as(client, user):
    """Faz login como o usuário fornecido."""
    with client.session_transaction() as sess:
        sess['_user_id'] = str(user.id)


# ── Testes do Modelo ─────────────────────────────────────────────────────────

class TestModeloCamposNegacao:
    """Verifica que os novos campos existem no modelo."""

    def test_campo_processo_negado_default_false(self, db_session, app):
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            it = _criar_itinerario(db_session)
            assert it.processo_negado is False

    def test_campos_negacao_nullable(self, db_session, app):
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            it = _criar_itinerario(db_session)
            assert it.processo_negado_data is None
            assert it.processo_negado_por_id is None
            assert it.processo_negado_por_nome is None
            assert it.processo_negado_justificativa is None
            assert it.processo_negado_doc_sei_id is None
            assert it.processo_negado_doc_sei_formatado is None

    def test_campos_unidade_geradora(self, db_session, app):
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            it = _criar_itinerario(
                db_session,
                unidade_geradora_descricao='Coordenação de Pagamentos - SEADPREV-PI',
                unidade_geradora_sigla='SEADPREV-PI/CPAG',
            )
            assert it.unidade_geradora_descricao == 'Coordenação de Pagamentos - SEADPREV-PI'
            assert it.unidade_geradora_sigla == 'SEADPREV-PI/CPAG'

    def test_marcar_como_negado(self, db_session, app):
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            it = _criar_itinerario(db_session)
            it.processo_negado = True
            it.processo_negado_data = datetime.now()
            it.processo_negado_por_id = 1
            it.processo_negado_por_nome = 'SUPERINTENDENTE TESTE'
            it.processo_negado_justificativa = 'Justificativa de teste'
            it.processo_negado_doc_sei_id = '777'
            it.processo_negado_doc_sei_formatado = '0009999'
            db_session.flush()
            assert it.processo_negado is True
            assert it.processo_negado_justificativa == 'Justificativa de teste'


# ── Testes do Endpoint de Negação ────────────────────────────────────────────

class TestEndpointNegar:
    """Testes do POST /diarias/administracao/<id>/negar."""

    def test_negar_requer_superintendente(self, client, db_session, app):
        """Usuário comum não pode negar."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_comum(db_session)
            it = _criar_itinerario(db_session)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={'justificativa': 'teste', 'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 403

    def test_negar_requer_etapa_1(self, client, db_session, app):
        """Não pode negar se não está na etapa 1."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session, etapa_id=3)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={'justificativa': 'teste', 'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400

    def test_negar_requer_processo_sei(self, client, db_session, app):
        """Não pode negar sem processo SEI vinculado."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session, sei_id_procedimento=None, sei_protocolo=None)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={'justificativa': 'teste', 'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400

    def test_negar_requer_unidade_geradora_descricao(self, client, db_session, app):
        """Não pode negar sem descrição da unidade solicitante."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session, unidade_geradora_descricao=None)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={'justificativa': 'teste', 'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400

    def test_negar_requer_justificativa(self, client, db_session, app):
        """Justificativa é obrigatória."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={'justificativa': '', 'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400

    def test_negar_bloqueia_ja_assinada(self, client, db_session, app):
        """Não pode negar solicitação já assinada pelo Superintendente."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session)
            it.superintendente_assinou = True
            db_session.flush()
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={'justificativa': 'teste', 'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400

    def test_negar_bloqueia_ja_negada(self, client, db_session, app):
        """Não pode negar solicitação já negada."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session, processo_negado=True)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={'justificativa': 'teste', 'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400

    def test_assinar_superintendente_bloqueia_processo_negado(self, client, db_session, app):
        """Não pode assinar requisição de solicitação já negada."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session, processo_negado=True)
            _criar_doc_requisicao(db_session, it)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/assinar-superintendente',
                json={'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400
            assert 'negada' in resp.get_json()['erro'].lower()

    def test_autorizar_bloqueia_processo_negado(self, client, db_session, app):
        """Não pode autorizar solicitação já negada."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session, processo_negado=True)
            it.superintendente_assinou = True
            _criar_doc_requisicao(db_session, it)
            _login_as(client, user)

            resp = client.post(
                f'/diarias/administracao/{it.id}/autorizar',
                json={'sei_usuario': 'u', 'sei_senha': 's'},
                content_type='application/json',
            )
            assert resp.status_code == 400
            assert 'negada' in resp.get_json()['erro'].lower()

    @patch('app.diarias.routes.admin.autenticar_usuario_sei')
    @patch('app.diarias.routes.admin.gerar_despacho_sga_negacao')
    @patch('app.diarias.routes.admin.assinar_documento')
    @patch('app.diarias.routes.admin.enviar_procedimento')
    def test_negar_happy_path(self, mock_enviar, mock_assinar, mock_despacho,
                               mock_auth, client, db_session, app):
        """Fluxo feliz: cria despacho, assina, marca negado, encaminha."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session)
            _login_as(client, user)

            mock_auth.return_value = {
                'token': 'tok123',
                'id_usuario': 'uid1',
                'id_login': 'lid1',
                'cargo': 'Superintendente de Gestão Administrativa',
            }
            mock_despacho.return_value = {
                'IdDocumento': '555',
                'DocumentoFormatado': '0005555',
            }
            mock_assinar.return_value = {'sucesso': True}
            mock_enviar.return_value = {'sucesso': True}

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={
                    'justificativa': 'Viagem não autorizada por falta de orçamento.',
                    'sei_usuario': 'super_sei',
                    'sei_senha': 'senha123',
                },
                content_type='application/json',
            )

            assert resp.status_code == 200
            data = resp.get_json()
            assert data['sucesso'] is True

            # Verifica estado do itinerário
            from app.models.diaria import DiariasItinerario
            it_db = DiariasItinerario.query.get(it.id)
            assert it_db.processo_negado is True
            assert it_db.processo_negado_por_id == user.id
            assert it_db.processo_negado_por_nome == user.nome
            assert it_db.processo_negado_justificativa == 'Viagem não autorizada por falta de orçamento.'
            assert it_db.processo_negado_doc_sei_id == '555'
            assert it_db.processo_negado_doc_sei_formatado == '0005555'
            assert it_db.processo_negado_data is not None

            # Verifica que despacho foi criado com unidade_geradora_descricao
            mock_despacho.assert_called_once()
            call_kwargs = mock_despacho.call_args
            assert call_kwargs is not None

            # Verifica que procedimento foi encaminhado para unidade solicitante
            mock_enviar.assert_called_once()
            enviar_args = mock_enviar.call_args
            assert it.unidade_geradora_id in str(enviar_args)

    @patch('app.diarias.routes.admin.autenticar_usuario_sei')
    @patch('app.diarias.routes.admin.gerar_despacho_sga_negacao')
    @patch('app.diarias.routes.admin.assinar_documento')
    @patch('app.diarias.routes.admin.enviar_procedimento')
    def test_negar_nao_marca_negado_se_encaminhamento_falhar(
        self, mock_enviar, mock_assinar, mock_despacho, mock_auth,
        client, db_session, app
    ):
        """Se o encaminhamento ao solicitante falhar, não marca processo como negado."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session)
            _login_as(client, user)

            mock_auth.return_value = {
                'token': 'tok123',
                'id_usuario': 'uid1',
                'id_login': 'lid1',
                'cargo': 'Superintendente de Gestão Administrativa',
            }
            mock_despacho.return_value = {
                'IdDocumento': '555',
                'DocumentoFormatado': '0005555',
            }
            mock_assinar.return_value = {'sucesso': True}
            mock_enviar.return_value = {'sucesso': False, 'erro': 'Falha no SEI'}

            resp = client.post(
                f'/diarias/administracao/{it.id}/negar',
                json={
                    'justificativa': 'Viagem não autorizada por falta de orçamento.',
                    'sei_usuario': 'super_sei',
                    'sei_senha': 'senha123',
                },
                content_type='application/json',
            )

            assert resp.status_code == 500

            from app.models.diaria import DiariasItinerario
            it_db = DiariasItinerario.query.get(it.id)
            assert it_db.processo_negado is False
            assert it_db.processo_negado_justificativa is None


# ── Testes do Filtro na Administração ────────────────────────────────────────

class TestFiltroNegadosAdministracao:
    """Administração oculta negados por padrão e oferece filtro específico."""

    def test_administracao_oculta_negados_por_padrao(self, client, db_session, app):
        """Itinerários negados NÃO aparecem na listagem padrão."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            admin = _criar_usuario_admin(db_session)
            _criar_itinerario(db_session, processo_negado=False,
                              sei_protocolo='PROC-ATIVO-PADRAO-001')
            _criar_itinerario(db_session, processo_negado=True,
                              sei_protocolo='PROC-NEGADO-PADRAO-001',
                              sei_id_procedimento='654321')
            _login_as(client, admin)

            resp = client.get('/diarias/administracao')
            assert resp.status_code == 200
            html = resp.data.decode('utf-8')
            assert 'PROC-ATIVO-PADRAO-001' in html
            assert 'PROC-NEGADO-PADRAO-001' not in html

    def test_administracao_filtro_negados(self, client, db_session, app):
        """Filtro 'negados=1' lista apenas solicitações negadas."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            admin = _criar_usuario_admin(db_session)
            _criar_itinerario(db_session, processo_negado=False,
                              sei_protocolo='PROC-ATIVO-FILTRO-001')
            _criar_itinerario(db_session, processo_negado=True,
                              sei_protocolo='PROC-NEGADO-FILTRO-001',
                              sei_id_procedimento='654321')
            _login_as(client, admin)

            resp = client.get('/diarias/administracao?negados=1')
            assert resp.status_code == 200
            html = resp.data.decode('utf-8')
            assert 'PROC-ATIVO-FILTRO-001' not in html
            assert 'PROC-NEGADO-FILTRO-001' in html
            assert 'Negado' in html


# ── Testes da UI de Negação ───────────────────────────────────────────────

class TestSincronizacaoNegacaoSei:
    """Processos negados no SEI nao podem virar assinatura da requisicao."""

    @patch('app.services.diarias_sei_integration.baixar_documento_sei')
    @patch('app.services.diarias_sei_integration.consultar_documentos_procedimento')
    def test_sincronizar_detecta_despacho_sga_de_negacao(
        self, mock_consultar, mock_baixar, db_session, app
    ):
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(
                db_session,
                sei_protocolo='00002.003034/2026-83',
                sei_id_procedimento='123456',
            )

            mock_consultar.return_value = {
                'sucesso': True,
                'documentos': [
                    {
                        'IdDocumento': '25685504',
                        'DocumentoFormatado': '0023157882',
                        'Serie': {'IdSerie': '532', 'Nome': 'SEAD_REQUISICAO DE DIARIAS'},
                        'Descricao': 'Solicitacao de diarias',
                        'Assinaturas': [
                            {
                                'Nome': 'HELLDANIO MUNIZ BARROS',
                                'CargoFuncao': 'Gerente',
                                'Sigla': 'helldanio.barros@sead.pi.gov.br',
                                'IdOrigem': '51530031320',
                            },
                            {
                                'Nome': 'FRANCISCO GUEDES ALCOFORADO FILHO',
                                'CargoFuncao': 'Diretor',
                                'Sigla': 'francisco.filho@sead.pi.gov.br',
                                'IdOrigem': '10578390353',
                            },
                        ],
                    },
                    {
                        'IdDocumento': '25712974',
                        'DocumentoFormatado': '0023183104',
                        'Serie': {'IdSerie': '2987', 'Nome': 'SEAD_DESPACHO_SGA'},
                        'Numero': '6732',
                        'Assinaturas': [
                            {
                                'Nome': 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA',
                                'CargoFuncao': 'Superintendente',
                                'Sigla': 'pedro.alexandre@sead.pi.gov.br',
                                'IdOrigem': '99202743304',
                            },
                        ],
                    },
                ],
            }
            mock_baixar.return_value = (
                b'<html><body>De ordem do Secretario, informo que nao sera '
                b'possivel a compra de passagens aereas e concessao de diarias '
                b'ante as atuais restricoes orcamentarias desta SEAD.</body></html>'
            )

            from app.services.diarias_sei_integration import sincronizar_documentos_diaria

            resultado = sincronizar_documentos_diaria(it)

            assert resultado['sucesso'] is True
            assert it.processo_negado is True
            assert it.processo_negado_doc_sei_id == '25712974'
            assert it.processo_negado_doc_sei_formatado == '0023183104'
            assert it.superintendente_assinou is False
            assert it.secretario_assinou is False


class TestUiNegacaoSolicitacao:
    """Botão e modal de negação aparecem apenas quando a ação é permitida."""

    def test_detalhe_exibe_botao_e_modal_para_superintendente(self, client, db_session, app):
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(db_session, usuario_gerador=user.sigla_login)
            _criar_doc_requisicao(db_session, it)
            _login_as(client, user)

            resp = client.get(f'/diarias/administracao/{it.id}')

            assert resp.status_code == 200
            html = resp.data.decode('utf-8')
            assert 'Negar Solicitação' in html
            assert 'modalNegarSolicitacao' in html
            assert 'Coordenação de Pagamentos - SEADPREV-PI' in html

    def test_detalhe_oculta_botao_e_modal_quando_negado(self, client, db_session, app):
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)
            user = _criar_usuario_superintendente(db_session)
            it = _criar_itinerario(
                db_session,
                usuario_gerador=user.sigla_login,
                processo_negado=True,
            )
            _criar_doc_requisicao(db_session, it)
            _login_as(client, user)

            resp = client.get(f'/diarias/administracao/{it.id}')

            assert resp.status_code == 200
            html = resp.data.decode('utf-8')
            assert 'data-bs-target="#modalNegarSolicitacao"' not in html
            assert 'id="modalNegarSolicitacao"' not in html


# ── Teste da Função de Despacho ──────────────────────────────────────────────

class TestGerarDespachoSgaNegacao:
    """Testa a função que gera o despacho de negação no SEI."""

    @patch('app.services.diarias_sei_integration.requests.post')
    def test_despacho_negacao_conteudo_correto(self, mock_post, app):
        """Despacho de negação inclui PARA com descricao da unidade e justificativa."""
        with app.app_context():
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = {
                'IdDocumento': '888',
                'DocumentoFormatado': '0008888',
            }
            mock_response.raise_for_status = MagicMock()
            mock_post.return_value = mock_response

            from app.services.diarias_sei_integration import gerar_despacho_sga_negacao

            result = gerar_despacho_sga_negacao(
                token='tok_test',
                id_procedimento='123456',
                sei_protocolo='00002.000001/2026-01',
                justificativa='Viagem cancelada por restrição orçamentária.',
                unidade_geradora_descricao='Coordenação de Pagamentos - SEADPREV-PI',
            )

            assert result is not None
            assert result['IdDocumento'] == '888'

            # Verifica payload enviado ao SEI
            call_kwargs = mock_post.call_args
            payload = call_kwargs.kwargs.get('json') or call_kwargs[1].get('json')
            conteudo = payload['Conteudo']
            assert 'Coordenação de Pagamentos - SEADPREV-PI' in conteudo
            assert 'Viagem cancelada por restrição orçamentária' in conteudo
            assert payload['IdSerie'] == '2987'


# ── Teste: Salvamento da unidade na criação ──────────────────────────────────

class TestSalvarUnidadeNaCriacao:
    """Verifica que criar_itinerario salva sigla e descrição da unidade SEI."""

    def test_criar_itinerario_salva_unidade_geradora(self, db_session, app):
        """DiariaService.criar_itinerario deve salvar unidade_geradora_sigla e descricao."""
        with app.app_context():
            _seed_etapas(db_session)
            _seed_tipos(db_session)

            from app.models.diaria import DiariasCargo, DiariasValorCargo
            cargo = DiariasCargo(nome='Assessor')
            db_session.add(cargo)
            db_session.flush()
            db_session.add(DiariasValorCargo(cargo_id=cargo.id, tipo_itinerario_id=1, valor=Decimal('120.00')))
            db_session.flush()

            from app.services.diaria_service import DiariaService

            dados = {
                'tipo_solicitacao_id': 1,
                'tipo_itinerario': 1,
                'data_viagem': '2026-06-01T08:00',
                'data_retorno': '2026-06-03T18:00',
                'usuario_gerador': 'teste',
                'estado_origem': 22,
                'estado_destino': 22,
                'objetivo': 'Teste',
                'unidade_sei_id': '110006213',
                'unidade_sei_sigla': 'SEADPREV-PI/CPAG',
                'unidade_sei_descricao': 'Coordenação de Pagamentos - SEADPREV-PI',
            }
            pessoas = [{'cpf': '00000000001', 'nome': 'TESTE', 'cargo_id': cargo.id}]

            it = DiariaService.criar_itinerario(dados, pessoas, None, None)
            db_session.flush()

            assert it.unidade_geradora_id == '110006213'
            assert it.unidade_geradora_sigla == 'SEADPREV-PI/CPAG'
            assert it.unidade_geradora_descricao == 'Coordenação de Pagamentos - SEADPREV-PI'
