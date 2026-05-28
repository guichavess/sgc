"""
P1-FIX-1 — Bloquear avanço quando assinatura do Secretário falha em "Apenas Diárias"
======================================================================================
Arquivo alvo: app/diarias/routes/admin.py — função autorizar_solicitacao

Problema:
    Quando tipo_solicitacao_id == 1 (Apenas Diárias), a assinatura da Requisição
    pelo Secretário cai no bloco linhas ~1907-1911 que apenas loga warning e CONTINUA
    executando (marca secretario_assinou=True, avança etapa). O processo fica num
    estado inconsistente: secretário "autorizou" sem assinar de fato.

Fix esperado:
    Retornar jsonify({'sucesso': False, 'erro': f'Falha ao assinar Requisição: {erro}'}), 500
    — mesmo padrão já usado para tipos 2/3 nas linhas 1978-1983.

Como rodar:
    pytest tests/diarias/test_autorizacao_secretario.py -v
"""
import uuid
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def _uid():
    return uuid.uuid4().hex[:8]


def _criar_usuario(db_session, cargo_gestao, cpf, sei_id=None, sigla=None, nome=None):
    from app.models.usuario import Usuario
    sid = sei_id or f'usr_{_uid()}'
    u = Usuario(
        id_usuario_sei=sid,
        nome=nome or f'TESTE {(cargo_gestao or "usuario").upper()}',
        sigla_login=sigla or f'{sid}@sead.pi.gov.br',
        cargo_gestao=cargo_gestao,
        cpf=cpf,
        ativo=True,
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_itinerario(db_session, tipo_solicitacao_id=1, etapa=1, sei_protocolo=None,
                      superintendente_assinou=True):
    from app.models.diaria import DiariasItinerario
    protocolo = sei_protocolo or f'00002.{_uid()}/2026-99'
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
        sei_protocolo=protocolo,
        sei_id_procedimento=f'PROC{_uid()}',
        superintendente_assinou=superintendente_assinou,
        superintendente_assinou_data=datetime(2026, 5, 7) if superintendente_assinou else None,
    )
    db_session.add(it)
    db_session.flush()
    return it


def _add_requisicao_doc(db_session, itinerario):
    """Adiciona doc 'requisicao' (tipo_documento='requisicao') ao itinerário."""
    from app.models.diaria import DiariasDocumentoSei
    doc = DiariasDocumentoSei(
        itinerario_id=itinerario.id,
        tipo_documento='requisicao',
        sei_id=f'SEIREQ{_uid()}',
        sei_formatado=f'0000{_uid()[:4]}',
    )
    db_session.add(doc)
    db_session.flush()
    return doc


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


# ── Mocks padrão para integração SEI ──────────────────────────────────────────

def _mock_sei_sucesso(mocker, assinar_retorno=None):
    """Aplica mocks nos serviços SEI que retornam sucesso por padrão."""
    if assinar_retorno is None:
        assinar_retorno = {'sucesso': True}

    mocker.patch(
        'app.services.sei_auth.autenticar_usuario_sei',
        return_value={
            'token': 'tk_sec',
            'id_usuario': 'idu_test',
            'id_login': 'idl_test',
            'cargo': 'Secretário de Administração do Estado do Piauí',
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
        return_value=assinar_retorno,
    )
    mocker.patch(
        'app.services.diarias_sei_integration.enviar_procedimento',
        return_value={'sucesso': True},
    )
    mocker.patch(
        'app.services.diarias_sei_integration.gerar_despacho_dfin',
        return_value=None,  # despacho dfin não é crítico para estes testes
    )
    mocker.patch(
        'app.services.notification_engine.NotificationEngine.notificar',
        return_value=None,
    )


# ══════════════════════════════════════════════════════════════════════════════
# MUDANÇA 1 — Bloquear avanço quando assinatura falha em "Apenas Diárias"
# ══════════════════════════════════════════════════════════════════════════════

class TestAutorizacaoSecretarioApenasDiarias:
    """
    Testa que quando a assinatura da Requisição falha no caminho "Apenas Diárias"
    (tipo_solicitacao_id == 1), o endpoint retorna erro 500 e NÃO avança a etapa.
    """

    CPF_SEC = '11111111110'
    CPF_SEC2 = '11111111112'
    CPF_SUPER = '33333333330'

    def _setup(self, db_session):
        """Setup: apenas diárias (tipo 1), super já assinou, sem integrantes especiais."""
        secretario = _criar_usuario(
            db_session, 'secretario', self.CPF_SEC,
            sei_id=f'sec_{_uid()}', nome='SECRETARIO TESTE'
        )
        super_u = _criar_usuario(
            db_session, 'superintendente', self.CPF_SUPER,
            sei_id=f'sup_{_uid()}', nome='SUPER TESTE'
        )
        it = _criar_itinerario(db_session, tipo_solicitacao_id=1, etapa=1,
                                superintendente_assinou=True)
        _add_item(db_session, it.id, f'CPF{_uid()}')  # não é super nem secretário → nível 1
        _add_requisicao_doc(db_session, it)
        return {'secretario': secretario, 'super_u': super_u, 'it': it}

    def test_apenas_diarias_falha_assinatura_nao_avanca_etapa(
        self, client, db_session, app, mocker
    ):
        """
        TESTE CRÍTICO — P1 Fix 1.

        Quando assinar_documento() retorna falha no caminho tipo 1 (Apenas Diárias),
        o endpoint DEVE retornar 500 e NÃO deve avançar a etapa para ANALISE_SOLICITACAO.

        ANTES do fix: warning logado, etapa avança de qualquer forma (BUG).
        APÓS o fix: retorna 500, etapa permanece em SOLICITACAO_INICIAL.
        """
        with app.app_context():
            ctx = self._setup(db_session)
            _login_como(client, ctx['secretario'])

            # Assinatura falha
            _mock_sei_sucesso(mocker, assinar_retorno={'sucesso': False, 'erro': 'Token inválido'})

            resp = client.post(
                f'/diarias/administracao/{ctx["it"].id}/autorizar',
                json={
                    'sei_usuario': 'secretario.teste',
                    'sei_senha': 'senha_qualquer',
                },
            )

            # Deve retornar erro — não pode avançar sem assinatura
            assert resp.status_code in (400, 500), (
                f'P1-FIX-1 NÃO APLICADO: esperado 4xx/5xx quando assinatura falha, '
                f'recebeu {resp.status_code}. O endpoint deve bloquear o avanço.'
            )
            data = resp.get_json()
            assert data['sucesso'] is False, (
                'P1-FIX-1 NÃO APLICADO: sucesso=True mesmo com assinatura falhando.'
            )

            # Etapa NÃO deve ter avançado
            from app.models.diaria import DiariasItinerario
            it_recarregado = db_session.get(DiariasItinerario, ctx['it'].id)
            assert it_recarregado.etapa_atual_id == 1, (
                f'P1-FIX-1 NÃO APLICADO: etapa avançou para {it_recarregado.etapa_atual_id} '
                f'mesmo com falha na assinatura. Deve permanecer em 1 (SOLICITACAO_INICIAL).'
            )

    def test_apenas_diarias_assinatura_ok_avanca(
        self, client, db_session, app, mocker
    ):
        """
        Caminho feliz: assinatura OK → etapa deve avançar para ANALISE_SOLICITACAO (3).
        """
        with app.app_context():
            ctx = self._setup(db_session)
            _login_como(client, ctx['secretario'])

            # Assinatura bem-sucedida
            _mock_sei_sucesso(mocker, assinar_retorno={'sucesso': True})

            resp = client.post(
                f'/diarias/administracao/{ctx["it"].id}/autorizar',
                json={
                    'sei_usuario': 'secretario.teste',
                    'sei_senha': 'senha_qualquer',
                },
            )

            assert resp.status_code == 200, (
                f'Esperado 200 no caminho feliz, recebeu {resp.status_code}: {resp.get_json()}'
            )
            data = resp.get_json()
            assert data['sucesso'] is True

            # Etapa deve ter avançado para 3
            from app.models.diaria import DiariasItinerario
            it_recarregado = db_session.get(DiariasItinerario, ctx['it'].id)
            assert it_recarregado.etapa_atual_id == 3, (
                f'Etapa deveria avançar para 3, atual={it_recarregado.etapa_atual_id}'
            )

    def test_tipo_com_passagens_falha_assinatura_nao_avanca(
        self, client, db_session, app, mocker
    ):
        """
        Teste de regressão: tipos 2/3 (com passagens) já bloqueavam falha de assinatura.
        Garante que o comportamento não foi alterado por esta mudança.
        """
        with app.app_context():
            secretario = _criar_usuario(
                db_session, 'secretario', self.CPF_SEC2,
                sei_id=f'sec2_{_uid()}', nome='SECRETARIO PASSAGENS'
            )
            super_u2 = _criar_usuario(
                db_session, 'superintendente', f'SUPER{_uid()[:8]}',
                sei_id=f'sup2_{_uid()}', nome='SUPER PASSAGENS'
            )
            it = _criar_itinerario(db_session, tipo_solicitacao_id=2, etapa=1,
                                    superintendente_assinou=True)
            _add_item(db_session, it.id, f'CPF{_uid()}')
            # Tipo 2 exige doc 574 (autorizacao) — mockamos gerar_autorizacao_secretario

            _login_como(client, secretario)

            # Assinatura falha para tipo 2
            _mock_sei_sucesso(mocker, assinar_retorno={'sucesso': False, 'erro': 'Sessão expirada'})
            mocker.patch(
                'app.services.diarias_sei_integration.gerar_autorizacao_secretario',
                return_value={'IdDocumento': 'DOC574', 'DocumentoFormatado': '0000574X'},
            )

            resp = client.post(
                f'/diarias/administracao/{it.id}/autorizar',
                json={
                    'sei_usuario': 'secretario.passagens',
                    'sei_senha': 'senha_qualquer',
                },
            )

            # Tipos 2/3 já tinham este guard — deve continuar retornando erro
            assert resp.status_code in (400, 500), (
                f'Regressão detectada em tipo 2: esperado 4xx/5xx, recebeu {resp.status_code}'
            )
            data = resp.get_json()
            assert data['sucesso'] is False

            # Etapa não deve ter avançado
            from app.models.diaria import DiariasItinerario
            it_recarregado = db_session.get(DiariasItinerario, it.id)
            assert it_recarregado.etapa_atual_id == 1, (
                f'Etapa avançou indevidamente para {it_recarregado.etapa_atual_id} (tipo 2)'
            )
