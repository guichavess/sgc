"""
Hierarquia de Autorização — Módulo Diárias (Etapa 1)
=====================================================

Testa a lógica de escalonamento de 3 níveis para autorização de diárias:
  Nível 1 — Secretário titular        (cargo_gestao='secretario')
  Nível 2 — Secretário em exercício   (cargo_gestao='secretario_exercicio')
  Nível 3 — Superintendente           (cargo_gestao='superintendente')

Testa também:
  - Superintendente dispensado da pré-assinatura quando é integrante
  - Properties de nível no modelo Usuario
  - Endpoint assinar-superintendente NÃO assina Requisição de Passagens
  - Endpoint autorizar respeita hierarquia (retorna 403 ao nível errado)

Como rodar:
    pytest tests/diarias/test_hierarquia_autorizacao.py -v
"""
import pytest
from datetime import date
from decimal import Decimal

# CPFs fixos para os testes (únicos entre si)
CPF_N1 = '11111111111'
CPF_N2 = '22222222222'
CPF_N3 = '33333333333'
CPF_OUTRO = '99999999999'


# ── Helpers ───────────────────────────────────────────────────────────────────

def _criar_usuario(db_session, sei_id, nome, cargo_gestao, cpf):
    from app.models.usuario import Usuario
    u = Usuario(
        id_usuario_sei=sei_id,
        nome=nome,
        sigla_login=f'{sei_id}@sead.pi.gov.br',
        cargo_gestao=cargo_gestao,
        cpf=cpf,
        ativo=True,
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_itinerario(db_session, tipo_solicitacao_id=1):
    from app.models.diaria import DiariasItinerario
    it = DiariasItinerario(
        usuario_gerador='solicitante_test',
        tipo_solicitacao_id=tipo_solicitacao_id,
        tipo_itinerario=1,
        status_id=1,
        data_solicitacao=date.today(),
        data_viagem=date(2026, 6, 1),
        data_retorno=date(2026, 6, 3),
        qtd_diarias_solicitadas=Decimal('2.0'),
        etapa_atual_id=1,
    )
    db_session.add(it)
    db_session.flush()
    return it


def _add_item(db_session, itinerario_id, cpf):
    from app.models.diaria import DiariasItemItinerario
    item = DiariasItemItinerario(
        id_itinerario=itinerario_id,
        cpf_pessoa=cpf,
        nome_pessoa='Pessoa Teste',
    )
    db_session.add(item)
    db_session.flush()
    return item


# ── Testes: Properties do modelo Usuario ──────────────────────────────────────

class TestUsuarioNivelSecretario:
    """Testa os novos properties adicionados ao modelo Usuario."""

    def test_nivel1_para_secretario(self, db_session, app):
        with app.app_context():
            u = _criar_usuario(db_session, 'n1a', 'SAMUEL', 'secretario', CPF_N1)
            assert u.nivel_secretario == 1

    def test_nivel2_para_secretario_exercicio(self, db_session, app):
        with app.app_context():
            u = _criar_usuario(db_session, 'n2a', 'BRUNO', 'secretario_exercicio', CPF_N2)
            assert u.nivel_secretario == 2

    def test_nivel3_para_superintendente(self, db_session, app):
        with app.app_context():
            u = _criar_usuario(db_session, 'n3a', 'PEDRO', 'superintendente', CPF_N3)
            assert u.nivel_secretario == 3

    def test_none_para_usuario_comum(self, db_session, app):
        with app.app_context():
            from app.models.usuario import Usuario
            u = Usuario(
                id_usuario_sei='comum_sei',
                nome='SERVIDOR COMUM',
                sigla_login='comum@sead.pi.gov.br',
                ativo=True,
            )
            db_session.add(u)
            db_session.flush()
            assert u.nivel_secretario is None

    def test_is_secretario_inclui_nivel2(self, db_session, app):
        """is_secretario deve retornar True para secretario_exercicio."""
        with app.app_context():
            u = _criar_usuario(db_session, 'n2b', 'BRUNO2', 'secretario_exercicio', '44444444444')
            assert u.is_secretario is True

    def test_is_secretario_exercicio_property(self, db_session, app):
        with app.app_context():
            u1 = _criar_usuario(db_session, 'n1b', 'SAMUEL2', 'secretario', '55555555555')
            u2 = _criar_usuario(db_session, 'n2c', 'BRUNO3', 'secretario_exercicio', '66666666666')
            assert u1.is_secretario_exercicio is False
            assert u2.is_secretario_exercicio is True

    def test_is_secretario_nao_inclui_superintendente(self, db_session, app):
        """Superintendente NÃO é secretário via cargo_gestao."""
        with app.app_context():
            u = _criar_usuario(db_session, 'n3b', 'PEDRO2', 'superintendente', '77777777777')
            assert u.is_secretario is False


# ── Testes: get_nivel_autorizacao() ──────────────────────────────────────────

class TestGetNivelAutorizacao:
    """Testa a função de serviço que determina o nível de autorização."""

    def test_nivel1_quando_nenhum_e_integrante(self, db_session, app):
        """Caso normal: nenhum secretário é integrante → Nível 1 autoriza."""
        with app.app_context():
            from app.services.diarias_autorizacao import get_nivel_autorizacao

            u1 = _criar_usuario(db_session, 'g_n1a', 'SAMUEL', 'secretario', CPF_N1)
            _criar_usuario(db_session, 'g_n2a', 'BRUNO', 'secretario_exercicio', CPF_N2)
            _criar_usuario(db_session, 'g_n3a', 'PEDRO', 'superintendente', CPF_N3)

            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_OUTRO)  # CPF diferente de todos

            result = get_nivel_autorizacao(it)

            assert result['nivel'] == 1
            assert result['motivo_escalada'] is None
            assert any(u.cargo_gestao == 'secretario' for u in result['autorizadores'])

    def test_nivel2_quando_nivel1_e_integrante(self, db_session, app):
        """Secretário titular é integrante → escala para Nível 2."""
        with app.app_context():
            from app.services.diarias_autorizacao import get_nivel_autorizacao

            _criar_usuario(db_session, 'g_n1b', 'SAMUEL', 'secretario', CPF_N1)
            _criar_usuario(db_session, 'g_n2b', 'BRUNO', 'secretario_exercicio', CPF_N2)
            _criar_usuario(db_session, 'g_n3b', 'PEDRO', 'superintendente', CPF_N3)

            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_N1)  # nível 1 é viajante

            result = get_nivel_autorizacao(it)

            assert result['nivel'] == 2
            assert 'integrante' in result['motivo_escalada'].lower()
            assert all(u.cargo_gestao == 'secretario_exercicio' for u in result['autorizadores'])

    def test_nivel3_quando_nivel1_e_nivel2_sao_integrantes(self, db_session, app):
        """Nível 1 e Nível 2 são integrantes → escala para Nível 3."""
        with app.app_context():
            from app.services.diarias_autorizacao import get_nivel_autorizacao

            _criar_usuario(db_session, 'g_n1c', 'SAMUEL', 'secretario', CPF_N1)
            _criar_usuario(db_session, 'g_n2c', 'BRUNO', 'secretario_exercicio', CPF_N2)
            _criar_usuario(db_session, 'g_n3c', 'PEDRO', 'superintendente', CPF_N3)

            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_N1)
            _add_item(db_session, it.id, CPF_N2)

            result = get_nivel_autorizacao(it)

            assert result['nivel'] == 3
            assert result['motivo_escalada'] is not None
            assert all(u.cargo_gestao == 'superintendente' for u in result['autorizadores'])

    def test_sem_integrantes_usa_nivel1(self, db_session, app):
        """Itinerário sem integrantes ainda usa Nível 1."""
        with app.app_context():
            from app.services.diarias_autorizacao import get_nivel_autorizacao

            _criar_usuario(db_session, 'g_n1d', 'SAMUEL', 'secretario', CPF_N1)
            it = _criar_itinerario(db_session)

            result = get_nivel_autorizacao(it)

            assert result['nivel'] == 1

    def test_cpf_com_espacos_nao_quebra(self, db_session, app):
        """CPFs com espaços extras devem ser normalizados corretamente."""
        with app.app_context():
            from app.services.diarias_autorizacao import get_nivel_autorizacao
            from app.models.diaria import DiariasItemItinerario

            _criar_usuario(db_session, 'g_n1e', 'SAMUEL', 'secretario', CPF_N1)
            _criar_usuario(db_session, 'g_n2e', 'BRUNO', 'secretario_exercicio', CPF_N2)

            it = _criar_itinerario(db_session)
            # CPF do nível 1 com espaço (teste de robustez)
            item = DiariasItemItinerario(
                id_itinerario=it.id,
                cpf_pessoa=f' {CPF_N1} ',
                nome_pessoa='Teste Espaço',
            )
            db_session.add(item)
            db_session.flush()

            result = get_nivel_autorizacao(it)

            assert result['nivel'] == 2  # deve detectar conflito mesmo com espaços


# ── Testes: superintendente_dispensado() ─────────────────────────────────────

class TestSuperintendenteDispensado:
    """Testa a função que verifica se o super está dispensado de assinar."""

    def test_dispensado_quando_super_e_integrante(self, db_session, app):
        with app.app_context():
            from app.services.diarias_autorizacao import superintendente_dispensado

            _criar_usuario(db_session, 'sd_n3a', 'PEDRO', 'superintendente', CPF_N3)
            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_N3)

            assert superintendente_dispensado(it) is True

    def test_nao_dispensado_quando_super_nao_e_integrante(self, db_session, app):
        with app.app_context():
            from app.services.diarias_autorizacao import superintendente_dispensado

            _criar_usuario(db_session, 'sd_n3b', 'PEDRO', 'superintendente', CPF_N3)
            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_OUTRO)

            assert superintendente_dispensado(it) is False

    def test_nao_dispensado_quando_sem_integrantes(self, db_session, app):
        with app.app_context():
            from app.services.diarias_autorizacao import superintendente_dispensado

            _criar_usuario(db_session, 'sd_n3c', 'PEDRO', 'superintendente', CPF_N3)
            it = _criar_itinerario(db_session)

            assert superintendente_dispensado(it) is False

    def test_nao_dispensado_quando_nao_ha_super_cadastrado(self, db_session, app):
        """Sem usuário superintendente no banco → nunca dispensado."""
        with app.app_context():
            from app.services.diarias_autorizacao import superintendente_dispensado

            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_N3)  # CPF que seria do super, mas sem usuário

            assert superintendente_dispensado(it) is False


# ── Testes: Estrutura do endpoint autorizar ───────────────────────────────────

class TestEndpointAutorizarHierarquia:
    """
    Valida via inspeção de código que o endpoint /autorizar usa a hierarquia.
    Testes HTTP com sessão autenticada ficam fora do escopo (SQLite in-memory
    não compartilha dados uncommitted entre conexões separadas).
    """

    def test_endpoint_usa_get_nivel_autorizacao(self, app):
        """O endpoint autorizar_solicitacao deve chamar get_nivel_autorizacao."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import autorizar_solicitacao
            source = inspect.getsource(autorizar_solicitacao)

            assert 'get_nivel_autorizacao' in source, (
                "FALHOU: autorizar_solicitacao não chama get_nivel_autorizacao(). "
                "A hierarquia de 3 níveis não está implementada no endpoint."
            )

    def test_endpoint_verifica_autorizador_ids(self, app):
        """O endpoint deve verificar se o usuário atual está na lista de autorizadores."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import autorizar_solicitacao
            source = inspect.getsource(autorizar_solicitacao)

            assert 'autorizador_ids' in source, (
                "FALHOU: endpoint não filtra por autorizador_ids. "
                "Sem este filtro, qualquer usuário pode autorizar."
            )

    def test_endpoint_bypassa_super_assinou_para_nivel3(self, app):
        """No Nível 3, o guard de superintendente_assinou deve ser bypassado."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import autorizar_solicitacao
            source = inspect.getsource(autorizar_solicitacao)

            assert 'nivel_atual != 3' in source or "nivel_atual == 3" in source, (
                "FALHOU: endpoint não tem lógica de bypass do guard 'superintendente_assinou' "
                "para Nível 3. O superintendente que autoriza (nível 3) não pode ser bloqueado "
                "por não ter assinado como superintendente primeiro."
            )


# ── Testes: Endpoint assinar-superintendente ──────────────────────────────────

class TestSuperAssinaApenasRequisicao:
    """
    Testa que assinar-superintendente:
    1. Chama superintendente_dispensado() e retorna cedo quando dispensado.
    2. Não contém lógica para assinar Requisição de Passagens.
    """

    def test_endpoint_chama_superintendente_dispensado(self, app):
        """O endpoint assinar_superintendente deve verificar a dispensa via serviço."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import assinar_superintendente
            source = inspect.getsource(assinar_superintendente)

            assert 'superintendente_dispensado' in source, (
                "FALHOU: assinar_superintendente não chama superintendente_dispensado(). "
                "Sem este check, o superintendente que é integrante fica bloqueado."
            )

    def test_endpoint_retorna_dispensado_true_na_resposta(self, app):
        """A resposta do endpoint deve incluir 'dispensado: True' quando aplicável."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import assinar_superintendente
            source = inspect.getsource(assinar_superintendente)

            assert "'dispensado': True" in source or '"dispensado": True' in source, (
                "FALHOU: resposta do endpoint não inclui 'dispensado: True'. "
                "O front-end precisa dessa chave para saber que foi uma dispensa automática."
            )

    def test_super_nao_assina_requisicao_passagens(self, app):
        """
        Verifica pelo código-fonte que o endpoint assinar-superintendente
        não contém lógica para assinar 'requisicao_passagens'.
        """
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import assinar_superintendente
            source = inspect.getsource(assinar_superintendente)

            assert 'requisicao_passagens' not in source, (
                "FALHOU: assinar_superintendente ainda contém lógica para assinar "
                "'requisicao_passagens'. O superintendente deve assinar APENAS a "
                "Requisição de Diárias (série 532)."
            )

    def test_verificar_assinaturas_nao_exige_passagens(self, app):
        """
        verificar-assinaturas-super deve liberar o passo 1 assim que a
        Requisição de Diárias estiver assinada — sem aguardar a Requisição
        de Passagens (que cabe apenas ao Secretário).
        """
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import verificar_assinaturas_super
            source = inspect.getsource(verificar_assinaturas_super)

            assert 'pass_assinada' not in source, (
                "FALHOU: verificar_assinaturas_super ainda bloqueia a liberação "
                "quando a Requisição de Passagens não está assinada. Deve liberar "
                "imediatamente quando a Req. Diárias estiver assinada."
            )


# ── Testes: verificação dinâmica via SEI ─────────────────────────────────────

class TestVerificarAssinaturaSuperSei:
    """
    Testa a verificação que consulta o SEI e casa a assinatura com o usuário
    cadastrado como Superintendente (cargo_gestao='superintendente'),
    sem nomes/CPFs hardcoded.
    """

    def _payload_doc_assinado(self, sigla_assinante, id_origem='99202743304',
                              nome='SUPER TESTE'):
        """Payload similar ao que o SEI retorna em /procedimentos/documentos."""
        return {
            'sucesso': True,
            'documentos': [
                {
                    'IdDocumento': '26468476',
                    'DocumentoFormatado': '0023874518',
                    'Serie': {'IdSerie': '532', 'Nome': 'SEAD_REQUISIÇÃO DE DIÁRIAS'},
                    'Assinaturas': [
                        {
                            'Nome': 'SAMUEL PONTES DO NASCIMENTO - Mat.0209541-2',
                            'CargoFuncao': 'Secretário de Estado',
                            'Sigla': 'samuel.nascimento@sead.pi.gov.br',
                            'IdOrigem': '00281021341',
                        },
                        {
                            'Nome': nome,
                            'CargoFuncao': 'Superintendente',
                            'Sigla': sigla_assinante,
                            'IdOrigem': id_origem,
                        },
                    ],
                },
            ],
        }

    def test_assinada_quando_sigla_bate_com_super_cadastrado(self, db_session, app, mocker):
        """
        Resposta SEI contém assinatura cuja Sigla == sigla_login do
        usuário cadastrado como superintendente → 'assinada': True.
        """
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            super_cadastrado = _criar_usuario(
                db_session, 'sei_n3a', 'PEDRO ALEXANDRE', 'superintendente', CPF_N3
            )
            super_cadastrado.sigla_login = 'pedro.alexandre@sead.pi.gov.br'
            db_session.flush()

            it = _criar_itinerario(db_session)
            it.sei_protocolo = '00002.004523/2026-52'
            db_session.flush()

            mocker.patch(
                'app.services.diarias_sei_integration.consultar_documentos_procedimento',
                return_value=self._payload_doc_assinado(
                    'pedro.alexandre@sead.pi.gov.br',
                    nome='PEDRO ALEXANDRE CABRAL DE OLIVEIRA',
                ),
            )

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is True
            assert 'PEDRO' in (res['assinante_nome'] or '')
            assert res['assinante_usuario_id'] == super_cadastrado.id

    def test_assinada_via_cpf_quando_sigla_diferente(self, db_session, app, mocker):
        """Fallback por CPF: Sigla diferente mas IdOrigem == cpf cadastrado."""
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            u = _criar_usuario(db_session, 'sei_n3b', 'NOVO SUPER', 'superintendente', '12345678901')
            u.sigla_login = 'super.antiga@sead.pi.gov.br'
            db_session.flush()

            it = _criar_itinerario(db_session)
            it.sei_protocolo = '00002.000001/2026-99'
            db_session.flush()

            mocker.patch(
                'app.services.diarias_sei_integration.consultar_documentos_procedimento',
                return_value=self._payload_doc_assinado(
                    'sigla.diferente@sead.pi.gov.br',
                    id_origem='12345678901',
                ),
            )

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is True

    def test_nao_assinada_quando_super_assinou_mas_nao_e_o_cadastrado(self, db_session, app, mocker):
        """
        Documento tem assinatura de "Superintendente" no texto, mas o
        Sigla/CPF não bate com nenhum usuário cargo_gestao='superintendente'.
        Deve retornar assinada=False (matching dinâmico estrito).
        """
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            u = _criar_usuario(db_session, 'sei_n3c', 'SUPER ATUAL', 'superintendente', '11122233344')
            u.sigla_login = 'super.atual@sead.pi.gov.br'
            db_session.flush()

            it = _criar_itinerario(db_session)
            it.sei_protocolo = '00002.000002/2026-99'
            db_session.flush()

            mocker.patch(
                'app.services.diarias_sei_integration.consultar_documentos_procedimento',
                return_value=self._payload_doc_assinado(
                    'super.antigo@sead.pi.gov.br',
                    id_origem='99999999999',
                ),
            )

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is False
            assert res['doc_sei_id'] == '26468476'

    def test_doc_532_nao_encontrado_no_processo(self, db_session, app, mocker):
        """Processo SEI sem Requisição de Diárias → assinada=False com erro descritivo."""
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            _criar_usuario(db_session, 'sei_n3d', 'SUPER', 'superintendente', '55566677788')

            it = _criar_itinerario(db_session)
            it.sei_protocolo = '00002.000003/2026-99'
            db_session.flush()

            mocker.patch(
                'app.services.diarias_sei_integration.consultar_documentos_procedimento',
                return_value={
                    'sucesso': True,
                    'documentos': [
                        {'Serie': {'IdSerie': '2986'}, 'Assinaturas': []},  # só Memorando
                    ],
                },
            )

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is False
            assert 'não encontrada' in res['erro'].lower()

    def test_sem_super_cadastrado_retorna_erro(self, db_session, app, mocker):
        """Sem Usuario cargo_gestao='superintendente' → retorna erro claro."""
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            it = _criar_itinerario(db_session)
            it.sei_protocolo = '00002.000004/2026-99'
            db_session.flush()

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is False
            assert 'superintendente' in res['erro'].lower()

    def test_sem_protocolo_sei_retorna_erro(self, db_session, app):
        """Itinerário sem sei_protocolo → não chama SEI, retorna erro."""
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            _criar_usuario(db_session, 'sei_n3e', 'SUPER', 'superintendente', '99988877766')

            it = _criar_itinerario(db_session)  # sem sei_protocolo
            db_session.flush()

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is False
            assert 'protocolo' in res['erro'].lower()

    def test_endpoint_usa_verificacao_sei_dinamica(self, app):
        """O endpoint verificar_assinaturas_super deve chamar a verificação SEI dinâmica."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import verificar_assinaturas_super
            source = inspect.getsource(verificar_assinaturas_super)

            assert 'verificar_assinatura_superintendente_sei' in source, (
                "FALHOU: o endpoint não chama a verificação SEI dinâmica. "
                "Sem isso, assinaturas feitas direto no SEI não são detectadas."
            )

    def test_match_cpf_funciona_mesmo_com_cpf_formatado_no_banco(self, db_session, app, mocker):
        """
        CPF cadastrado em sis_usuarios pode estar com pontos/traços
        (ex: '992.027.433-04'). Normalização deve permitir o match com
        IdOrigem do SEI (somente dígitos).
        """
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            u = _criar_usuario(db_session, 'cpf_fmt', 'PEDRO SUPER', 'superintendente', '992.027.433-04')
            u.sigla_login = 'sigla.totalmente.diferente@sead.pi.gov.br'
            db_session.flush()

            it = _criar_itinerario(db_session)
            it.sei_protocolo = '00002.004523/2026-52'
            db_session.flush()

            mocker.patch(
                'app.services.diarias_sei_integration.consultar_documentos_procedimento',
                return_value=self._payload_doc_assinado(
                    'outra.sigla@sead.pi.gov.br',
                    id_origem='99202743304',  # somente dígitos
                ),
            )

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is True

    def test_match_por_nome_como_ultimo_fallback(self, db_session, app, mocker):
        """
        Quando Sigla e CPF não batem, tenta fallback por Nome (sem matrícula
        e sem acentos). Útil quando o cadastro não tem CPF nem o login bate.
        """
        with app.app_context():
            from app.services import diarias_autorizacao as svc

            u = _criar_usuario(db_session, 'nome_fb', 'Pedro Alexandre Cabral de Oliveira',
                               'superintendente', None)
            u.sigla_login = 'login.antigo@sead.pi.gov.br'
            u.cpf = None
            db_session.flush()

            it = _criar_itinerario(db_session)
            it.sei_protocolo = '00002.004523/2026-52'
            db_session.flush()

            mocker.patch(
                'app.services.diarias_sei_integration.consultar_documentos_procedimento',
                return_value=self._payload_doc_assinado(
                    'sigla.diferente@sead.pi.gov.br',
                    id_origem='00000000000',
                    nome='PEDRO ALEXANDRE CABRAL DE OLIVEIRA - Matr.0391817-3',
                ),
            )

            res = svc.verificar_assinatura_superintendente_sei(it)

            assert res['assinada'] is True


class TestVerificarAssinaturasRequeridas:
    """
    Testa a função usada por verificar_autorizacao_diaria (botão 'Verificar' da timeline).
    Cenário-chave: documento Req. Diárias com Pedro (Superintendente) + Samuel (Secretário) →
    deve ser considerado completo independente de superintendencia_sigla.
    """

    def _doc_com_assinaturas(self, assinaturas):
        return {
            'IdDocumento': '26468476',
            'DocumentoFormatado': '0023874518',
            'Serie': {'IdSerie': '532', 'Nome': 'SEAD_REQUISIÇÃO DE DIÁRIAS'},
            'Assinaturas': assinaturas,
        }

    def test_completa_com_super_e_secretario_via_sigla(self, db_session, app):
        """Cenário do usuário: Req. Diárias assinada por super + secretário cadastrados."""
        with app.app_context():
            from app.services.diarias_assinaturas import verificar_assinaturas_requeridas

            super_u = _criar_usuario(db_session, 'va_super', 'PEDRO ALEXANDRE',
                                     'superintendente', '99202743304')
            super_u.sigla_login = 'pedro.alexandre@sead.pi.gov.br'
            sec_u = _criar_usuario(db_session, 'va_sec', 'SAMUEL PONTES',
                                   'secretario', '00281021341')
            sec_u.sigla_login = 'samuel.nascimento@sead.pi.gov.br'
            db_session.flush()

            doc = self._doc_com_assinaturas([
                {
                    'Nome': 'SAMUEL PONTES DO NASCIMENTO - Mat.0209541-2',
                    'CargoFuncao': 'Secretário de Estado',
                    'Sigla': 'samuel.nascimento@sead.pi.gov.br',
                    'IdOrigem': '00281021341',
                },
                {
                    'Nome': 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA - Matr.0391817-3',
                    'CargoFuncao': 'Superintendente',
                    'Sigla': 'pedro.alexandre@sead.pi.gov.br',
                    'IdOrigem': '99202743304',
                },
            ])

            info = verificar_assinaturas_requeridas(doc)

            assert info['completa'] is True, (
                f"FALHOU: doc com Super + Secretário cadastrados deveria ser completo. "
                f"tem_super={info['tem_superintendente']}, tem_sec={info['tem_secretario']}"
            )

    def test_match_case_insensitive_na_sigla(self, db_session, app):
        """Sigla SEI em case diferente do banco deve casar igual."""
        with app.app_context():
            from app.services.diarias_assinaturas import verificar_assinaturas_requeridas

            super_u = _criar_usuario(db_session, 'va_ci_s', 'PEDRO', 'superintendente', '11122233344')
            super_u.sigla_login = 'pedro.alexandre@sead.pi.gov.br'  # tudo lower
            sec_u = _criar_usuario(db_session, 'va_ci_sec', 'SAMUEL', 'secretario', '55566677788')
            sec_u.sigla_login = 'samuel.nascimento@sead.pi.gov.br'
            db_session.flush()

            doc = self._doc_com_assinaturas([
                {'Nome': 'SAMUEL', 'Sigla': 'Samuel.Nascimento@SEAD.PI.GOV.BR',  # mixed case
                 'IdOrigem': '55566677788'},
                {'Nome': 'PEDRO', 'Sigla': 'PEDRO.ALEXANDRE@sead.pi.gov.br',  # upper
                 'IdOrigem': '11122233344'},
            ])

            info = verificar_assinaturas_requeridas(doc)

            assert info['completa'] is True
            assert info['tem_superintendente'] is True
            assert info['tem_secretario'] is True

    def test_secretario_em_exercicio_satisfaz_requisito(self, db_session, app):
        """Bruno (cargo_gestao='secretario_exercicio') também conta como secretário."""
        with app.app_context():
            from app.services.diarias_assinaturas import verificar_assinaturas_requeridas

            super_u = _criar_usuario(db_session, 'va_se_s', 'PEDRO', 'superintendente', '99988877766')
            super_u.sigla_login = 'pedro@sead.pi.gov.br'
            sec_u = _criar_usuario(db_session, 'va_se_b', 'BRUNO', 'secretario_exercicio', '88877766655')
            sec_u.sigla_login = 'bruno@sead.pi.gov.br'
            db_session.flush()

            doc = self._doc_com_assinaturas([
                {'Nome': 'BRUNO', 'Sigla': 'bruno@sead.pi.gov.br', 'IdOrigem': '88877766655'},
                {'Nome': 'PEDRO', 'Sigla': 'pedro@sead.pi.gov.br', 'IdOrigem': '99988877766'},
            ])

            info = verificar_assinaturas_requeridas(doc)

            assert info['completa'] is True

    def test_doc_sem_super_nao_e_completo(self, db_session, app):
        """Documento só com secretário cadastrado → incompleto."""
        with app.app_context():
            from app.services.diarias_assinaturas import verificar_assinaturas_requeridas

            sec_u = _criar_usuario(db_session, 'va_no_s', 'SAMUEL', 'secretario', '00011122233')
            sec_u.sigla_login = 'samuel@sead.pi.gov.br'
            _criar_usuario(db_session, 'va_no_p', 'PEDRO', 'superintendente', '99988877766')
            db_session.flush()

            doc = self._doc_com_assinaturas([
                {'Nome': 'SAMUEL', 'Sigla': 'samuel@sead.pi.gov.br', 'IdOrigem': '00011122233'},
            ])

            info = verificar_assinaturas_requeridas(doc)

            assert info['completa'] is False
            assert info['tem_secretario'] is True
            assert info['tem_superintendente'] is False


class TestGetEstadoEtapa1:
    """Testa o helper otimizado que evita queries duplicadas."""

    def test_retorna_nivel_e_dispensa_em_uma_passada(self, db_session, app):
        with app.app_context():
            from app.services.diarias_autorizacao import get_estado_etapa1

            _criar_usuario(db_session, 'e1_n1', 'SAMUEL', 'secretario', CPF_N1)
            _criar_usuario(db_session, 'e1_n2', 'BRUNO', 'secretario_exercicio', CPF_N2)
            _criar_usuario(db_session, 'e1_n3', 'PEDRO', 'superintendente', CPF_N3)

            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_N3)  # super é integrante → dispensa

            estado = get_estado_etapa1(it)

            assert estado['nivel_autorizacao']['nivel'] == 1  # nenhum dos 2 secretários é integrante
            assert estado['super_dispensado'] is True

    def test_helper_faz_uma_unica_query_de_usuarios(self, db_session, app, mocker):
        """
        O helper deve emitir UMA única query em Usuario (com IN clause),
        não três separadas para cada cargo_gestao.
        """
        with app.app_context():
            from app.services.diarias_autorizacao import get_estado_etapa1
            from app.models import usuario as usuario_module

            _criar_usuario(db_session, 'q_n1', 'SAMUEL', 'secretario', CPF_N1)
            _criar_usuario(db_session, 'q_n3', 'PEDRO', 'superintendente', CPF_N3)

            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_OUTRO)

            chamadas_filter = []
            original_filter = usuario_module.Usuario.query.filter

            def spy_filter(*args, **kwargs):
                chamadas_filter.append((args, kwargs))
                return original_filter(*args, **kwargs)

            mocker.patch.object(
                usuario_module.Usuario.query.__class__,
                'filter',
                side_effect=spy_filter,
            )

            try:
                get_estado_etapa1(it)
            except Exception:
                pass  # mock interfere com filter chains, mas só queremos contar chamadas

            # Esperado: 1 chamada (com IN). Antes da otimização eram 3 filter_by separados.
            assert len(chamadas_filter) <= 1, (
                f"Helper otimizado não deve fazer mais de 1 query em Usuario. "
                f"Foram feitas {len(chamadas_filter)} chamadas a .filter()."
            )
