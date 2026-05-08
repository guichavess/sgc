"""
Override Manual de Nivel 3 — Diarias (Etapa 1)
==============================================

Testa o caminho de excecao em que o Superintendente assume a autorizacao
(Nivel 3) quando o nivel computado e 2 mas o Secretario em Exercicio esta
indisponivel (viagem, licenca, etc.) ou nao ha cadastro para o cargo.

Comportamento esperado:

  * Apenas usuario com cargo_gestao='superintendente' pode acionar o override
  * Override so e aceito quando nivel computado == 2
  * Motivo da indisponibilidade e obrigatorio (>= 10 caracteres)
  * Quando aceito, o motivo e registrado em DiariasHistoricoMovimentacao.comentario
  * Caminho normal (sem override) preserva o guard original (Sec. em Exercicio Nivel 2)

Como rodar:
    pytest tests/diarias/test_autorizacao_override_n3.py -v
"""
import uuid
from datetime import date, datetime
from decimal import Decimal


CPF_TITULAR = '11111111111'
CPF_EXERCICIO = '22222222222'
CPF_SUPER = '33333333333'
CPF_OUTRO = '99999999999'


# ── Helpers ───────────────────────────────────────────────────────────────────

def _uid():
    return uuid.uuid4().hex[:8]


def _criar_usuario(db_session, cargo_gestao, cpf, sei_id=None, sigla=None, nome=None):
    from app.models.usuario import Usuario
    sid = sei_id or f'usr_{_uid()}'
    u = Usuario(
        id_usuario_sei=sid,
        nome=nome or f'TESTE {cargo_gestao.upper()}',
        sigla_login=sigla or f'{sid}@sead.pi.gov.br',
        cargo_gestao=cargo_gestao,
        cpf=cpf,
        ativo=True,
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_itinerario(db_session, tipo_solicitacao_id=2, etapa=1):
    """Itinerario tipo 2 (com passagens) na etapa 1, super ja assinou.

    tipo 2 garante que o caminho de override roda o branch que gera doc 574.
    superintendente_assinou=True permite que o guard 'super precisa assinar antes'
    nao bloqueie o override.
    """
    from app.models.diaria import DiariasItinerario
    it = DiariasItinerario(
        usuario_gerador=f'gen_{_uid()}',
        tipo_solicitacao_id=tipo_solicitacao_id,
        tipo_itinerario=tipo_solicitacao_id,
        status_id=1,
        data_solicitacao=date(2026, 5, 1),
        data_viagem=datetime(2026, 6, 1),
        data_retorno=datetime(2026, 6, 3),
        qtd_diarias_solicitadas=Decimal('2.0'),
        etapa_atual_id=etapa,
        sei_protocolo=f'00002.{_uid()}/2026-99',
        sei_id_procedimento=f'PROC{_uid()}',
        superintendente_assinou=True,
        superintendente_assinou_data=datetime(2026, 5, 7),
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


def _login_como(client, usuario):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True


# ── Testes: estrutura do codigo (inspecao) ─────────────────────────────────

class TestEstruturaOverride:
    """Verifica via inspecao de codigo que a logica de override foi implementada."""

    def test_endpoint_aceita_override_nivel(self, app):
        """O endpoint deve ler o parametro override_nivel do payload."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import autorizar_solicitacao
            source = inspect.getsource(autorizar_solicitacao)
            assert 'override_nivel' in source, (
                "FALHOU: autorizar_solicitacao nao trata 'override_nivel' no payload."
            )

    def test_endpoint_exige_motivo_no_override(self, app):
        """O override deve exigir motivo da indisponibilidade."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import autorizar_solicitacao
            source = inspect.getsource(autorizar_solicitacao)
            assert 'motivo_override' in source, (
                "FALHOU: endpoint nao trata 'motivo_override'."
            )

    def test_endpoint_restringe_override_a_superintendente(self, app):
        """O override so pode ser acionado por usuario com cargo_gestao=superintendente."""
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import autorizar_solicitacao
            source = inspect.getsource(autorizar_solicitacao)
            assert "cargo_gestao" in source and "superintendente" in source, (
                "FALHOU: endpoint nao verifica cargo_gestao=superintendente no override."
            )


# ── Testes: HTTP — guards de override ─────────────────────────────────────

class TestOverrideGuards:
    """
    Testes de proteção do endpoint /administracao/<id>/autorizar com override_nivel=3.

    Cenario base: Secretario titular e integrante (forca nivel computado = 2).
    Sec. em Exercicio NAO esta cadastrado, simulando indisponibilidade real.
    """

    def _setup(self, db_session):
        """Setup padrao: titular integrante, super cadastrado, sec.exerc. ausente."""
        sec_titular = _criar_usuario(db_session, 'secretario', CPF_TITULAR,
                                      sei_id=f'sec_t_{_uid()}', nome='SAMUEL TITULAR')
        super_u = _criar_usuario(db_session, 'superintendente', CPF_SUPER,
                                  sei_id=f'sup_{_uid()}', nome='PEDRO ALEXANDRE')
        outro = _criar_usuario(db_session, None, CPF_OUTRO,
                               sei_id=f'out_{_uid()}', nome='OUTRO USUARIO')
        outro.cargo_gestao = None  # garante que nao e autorizador
        db_session.flush()

        it = _criar_itinerario(db_session, tipo_solicitacao_id=2, etapa=1)
        _add_item(db_session, it.id, CPF_TITULAR)  # titular e integrante -> nivel computado = 2

        return {'sec_titular': sec_titular, 'super_u': super_u, 'outro': outro, 'it': it}

    def test_override_negado_para_nao_superintendente(self, client, db_session, app):
        """Usuario sem cargo_gestao='superintendente' nao pode acionar override."""
        with app.app_context():
            ctx = self._setup(db_session)
            _login_como(client, ctx['outro'])

            resp = client.post(
                f'/diarias/administracao/{ctx["it"].id}/autorizar',
                json={
                    'sei_usuario': 'qualquer',
                    'sei_senha': 'qualquer',
                    'override_nivel': 3,
                    'motivo_override': 'Sec. Exercicio em viagem oficial',
                },
            )
            assert resp.status_code == 403
            data = resp.get_json()
            assert data['sucesso'] is False
            assert 'superintendente' in data['erro'].lower()

    def test_override_negado_quando_nivel_computado_diferente_de_2(self, client, db_session, app):
        """Override com nivel computado=1 (titular nao integrante) deve ser rejeitado."""
        with app.app_context():
            sec_titular = _criar_usuario(db_session, 'secretario', CPF_TITULAR,
                                          sei_id=f'sec_t2_{_uid()}')
            super_u = _criar_usuario(db_session, 'superintendente', CPF_SUPER,
                                      sei_id=f'sup2_{_uid()}')
            it = _criar_itinerario(db_session)
            _add_item(db_session, it.id, CPF_OUTRO)  # nem titular nem super sao integrantes -> nivel 1

            _login_como(client, super_u)

            resp = client.post(
                f'/diarias/administracao/{it.id}/autorizar',
                json={
                    'sei_usuario': 'qualquer',
                    'sei_senha': 'qualquer',
                    'override_nivel': 3,
                    'motivo_override': 'Motivo qualquer aceitavel',
                },
            )
            assert resp.status_code == 400
            data = resp.get_json()
            assert 'nivel computado' in data['erro'].lower() or '2' in data['erro']

    def test_override_negado_sem_motivo(self, client, db_session, app):
        """Override sem campo motivo_override deve retornar 400."""
        with app.app_context():
            ctx = self._setup(db_session)
            _login_como(client, ctx['super_u'])

            resp = client.post(
                f'/diarias/administracao/{ctx["it"].id}/autorizar',
                json={
                    'sei_usuario': 'qualquer',
                    'sei_senha': 'qualquer',
                    'override_nivel': 3,
                    # sem motivo_override
                },
            )
            assert resp.status_code == 400
            data = resp.get_json()
            assert 'motivo' in data['erro'].lower()

    def test_override_negado_com_motivo_curto(self, client, db_session, app):
        """Motivo com menos de 10 caracteres deve ser rejeitado."""
        with app.app_context():
            ctx = self._setup(db_session)
            _login_como(client, ctx['super_u'])

            resp = client.post(
                f'/diarias/administracao/{ctx["it"].id}/autorizar',
                json={
                    'sei_usuario': 'qualquer',
                    'sei_senha': 'qualquer',
                    'override_nivel': 3,
                    'motivo_override': 'curto',  # < 10
                },
            )
            assert resp.status_code == 400
            data = resp.get_json()
            assert 'motivo' in data['erro'].lower()

    def test_caminho_normal_super_sem_override_continua_403(self, client, db_session, app):
        """
        Sem override, o Superintendente NAO pode autorizar quando o nivel computado e 2 —
        preserva o guard original.
        """
        with app.app_context():
            ctx = self._setup(db_session)
            _login_como(client, ctx['super_u'])

            resp = client.post(
                f'/diarias/administracao/{ctx["it"].id}/autorizar',
                json={
                    'sei_usuario': 'qualquer',
                    'sei_senha': 'qualquer',
                    # sem override
                },
            )
            assert resp.status_code == 403


# ── Testes: HTTP — caminho feliz do override ──────────────────────────────

class TestOverrideHappyPath:
    """Override aceito por Superintendente quando todas as condicoes estao OK."""

    def _setup_e_mocks(self, db_session, mocker):
        """Setup completo + mocks para todas as integracoes SEI."""
        sec_titular = _criar_usuario(db_session, 'secretario', CPF_TITULAR,
                                      sei_id=f'sec_t3_{_uid()}', nome='SAMUEL')
        super_u = _criar_usuario(db_session, 'superintendente', CPF_SUPER,
                                  sei_id=f'sup3_{_uid()}', nome='PEDRO ALEXANDRE')
        it = _criar_itinerario(db_session, tipo_solicitacao_id=2, etapa=1)
        _add_item(db_session, it.id, CPF_TITULAR)  # forca nivel 2

        # Mocks SEI — caminhos do source porque a rota importa localmente
        mocker.patch(
            'app.services.sei_auth.autenticar_usuario_sei',
            return_value={
                'token': 'tk_sec',
                'id_usuario': 'idu_test',
                'id_login': 'idl_test',
                'cargo': 'Superintendente de Gestão Administrativa',
            },
        )
        mocker.patch(
            'app.services.diarias_sei_integration.gerar_token_sei_admin',
            return_value='tk_admin',
        )
        mocker.patch(
            'app.services.diarias_sei_integration.consultar_documentos_procedimento',
            return_value={'sucesso': True, 'documentos': []},
        )
        mocker.patch(
            'app.services.sei_integration.assinar_documento',
            return_value={'sucesso': True},
        )
        mocker.patch(
            'app.services.diarias_sei_integration.gerar_autorizacao_secretario',
            return_value={'IdDocumento': 'DOC574', 'DocumentoFormatado': '0023987000'},
        )
        mocker.patch(
            'app.services.diarias_sei_integration.enviar_procedimento',
            return_value={'sucesso': True},
        )
        mocker.patch(
            'app.services.notification_engine.NotificationEngine.notificar',
            return_value=None,
        )

        return {'sec_titular': sec_titular, 'super_u': super_u, 'it': it}

    def test_override_aceito_avanca_etapa_e_registra_motivo(self, client, db_session, app, mocker):
        """
        Caminho feliz: Pedro Alexandre (Superintendente) aciona override com motivo valido.
        Resultado: 200, etapa avanca para 3, motivo registrado no historico.
        """
        with app.app_context():
            ctx = self._setup_e_mocks(db_session, mocker)
            _login_como(client, ctx['super_u'])

            motivo = 'Secretario em Exercicio em viagem oficial ate 15/05'

            resp = client.post(
                f'/diarias/administracao/{ctx["it"].id}/autorizar',
                json={
                    'sei_usuario': 'pedro.alexandre',
                    'sei_senha': 'senha_qualquer',
                    'override_nivel': 3,
                    'motivo_override': motivo,
                },
            )

            assert resp.status_code == 200, f'Esperado 200, recebeu {resp.status_code}: {resp.get_json()}'
            data = resp.get_json()
            assert data['sucesso'] is True

            # Recarrega itinerario do banco para checar avanco de etapa
            from app.models.diaria import DiariasItinerario, DiariasHistoricoMovimentacao
            it_atual = db_session.get(DiariasItinerario, ctx['it'].id)
            assert it_atual.etapa_atual_id == 3, (
                f'Etapa deveria avancar para 3 (Analise), atual={it_atual.etapa_atual_id}'
            )

            historico = (
                db_session.query(DiariasHistoricoMovimentacao)
                .filter_by(id_itinerario=ctx['it'].id)
                .order_by(DiariasHistoricoMovimentacao.id.desc())
                .first()
            )
            assert historico is not None
            assert 'OVERRIDE' in (historico.comentario or '').upper(), (
                f'Comentario deveria mencionar OVERRIDE; atual: {historico.comentario!r}'
            )
            assert motivo in (historico.comentario or ''), (
                f'Motivo deveria estar no comentario; atual: {historico.comentario!r}'
            )
