"""
P1-FIX-2 — Despachos Diretor DFIN / GEO exigem assinatura
===========================================================
Arquivo alvo: app/financeiro/routes/diarias.py
Funções: despacho_diretor (linhas ~1705) e despacho_geo (linhas ~1839)

Problema:
    Quando resultado_assinatura.get('sucesso') é False, o código atual apenas
    seta `aviso` e continua — chamando enviar_procedimento, marcando
    ciencia_diretor/geo=True, salvando o doc no banco e commitando.
    O documento fica órfão no SEI (criado mas não assinado) enquanto o banco
    registra sucesso.

Fix esperado:
    Se not resultado_assinatura.get('sucesso'), abortar com 500.
    Não marcar ciência, não chamar enviar_procedimento, não commitar.
    Mensagem clara: doc criado no SEI mas falhou ao assinar — tente novamente.

Como rodar:
    pytest tests/diarias/test_despachos_financeiros.py -v
"""
import uuid
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def _uid():
    return uuid.uuid4().hex[:8]


def _criar_usuario(db_session, cargo_gestao=None, cpf=None, sei_id=None, sigla=None, nome=None,
                   is_admin=True):
    from app.models.usuario import Usuario
    sid = sei_id or f'usr_{_uid()}'
    u = Usuario(
        id_usuario_sei=sid,
        nome=nome or f'USUARIO TESTE',
        sigla_login=sigla or f'{sid}@sead.pi.gov.br',
        cargo_gestao=cargo_gestao,
        cpf=cpf or f'{_uid()[:11]}',
        ativo=True,
        is_admin=is_admin,  # is_admin=True permite acesso aos endpoints sem perfil específico
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_itinerario(db_session, tipo_solicitacao_id=1, etapa=6, com_despacho_sga=True,
                      com_despacho_diretor=False):
    """
    Cria itinerário com estado adequado para testes dos despachos financeiros.
    etapa=6 (ANALISE_SOLICITACAO_2) é o estado típico quando despacho_diretor ocorre.
    """
    from app.models.diaria import DiariasItinerario, DiariasDocumentoSei
    from app.extensions import db

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
        secretario_assinou=True,
    )
    db_session.add(it)
    db_session.flush()

    if com_despacho_sga:
        doc = DiariasDocumentoSei(
            itinerario_id=it.id,
            tipo_documento='despacho_sga',
            sei_id=f'SEISGA{_uid()}',
            sei_formatado=f'000SGA{_uid()[:4]}',
        )
        db_session.add(doc)

    if com_despacho_diretor:
        doc_dir = DiariasDocumentoSei(
            itinerario_id=it.id,
            tipo_documento='despacho_diretor',
            sei_id=f'SEIDIR{_uid()}',
            sei_formatado=f'000DIR{_uid()[:4]}',
        )
        db_session.add(doc_dir)

    db_session.flush()
    return it


def _login_como(client, usuario):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True


def _mock_base_despacho(mocker, assinar_retorno, gerar_retorno=None, unidade_mock=None):
    """
    Mocks mínimos para as rotas despacho_diretor e despacho_geo.
    assinar_retorno: valor retornado por assinar_documento.

    Nota: autenticar_usuario_sei é importado no nível do módulo em
    app.financeiro.routes.diarias (linha 25), então o mock deve ser
    aplicado no namespace do módulo, não no namespace da fonte.
    """
    if gerar_retorno is None:
        gerar_retorno = {'IdDocumento': f'DOC{_uid()}', 'DocumentoFormatado': f'000DESP{_uid()[:4]}'}

    # Mock no namespace do módulo que usa a função (importação no nível do módulo)
    mocker.patch(
        'app.financeiro.routes.diarias.autenticar_usuario_sei',
        return_value={
            'token': 'tk_test',
            'id_usuario': 'idu_test',
            'id_login': 'idl_test',
            'cargo': 'Diretor de Planejamento e Financas',
        },
    )
    mocker.patch(
        'app.financeiro.routes.diarias.gerar_token_sei_admin',
        return_value='tk_admin',
    )
    mocker.patch(
        'app.financeiro.routes.diarias.gerar_despacho_diretor',
        return_value=gerar_retorno,
    )
    mocker.patch(
        'app.financeiro.routes.diarias.gerar_despacho_geo',
        return_value=gerar_retorno,
    )
    mocker.patch(
        'app.financeiro.routes.diarias.assinar_documento',
        return_value=assinar_retorno,
    )
    mocker.patch(
        'app.financeiro.routes.diarias.enviar_procedimento',
        return_value={'sucesso': True},
    )
    mocker.patch(
        'app.services.notification_engine.NotificationEngine.notificar',
        return_value=None,
    )

    # Mock das verificações de caixa (usuario_tem_caixa)
    mocker.patch(
        'app.financeiro.routes.diarias.usuario_tem_caixa',
        return_value=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# MUDANÇA 2 — Despacho Diretor / GEO exigem assinatura
# ══════════════════════════════════════════════════════════════════════════════

class TestDespachoDiretorAssinatura:
    """
    Testa que despacho_diretor aborta quando a assinatura falha — não persiste
    ciencia_diretor=True nem o documento no banco, nem encaminha o procedimento.
    """

    def test_despacho_diretor_assinatura_falha_salva_pendente(
        self, client, db_session, app, mocker
    ):
        """
        TESTE CRÍTICO — Retry Diretor (plano item 5).

        Quando assinar_documento falha em despacho_diretor, o padrão CCDP se aplica:
        - Retorna 200 com sucesso=True e pendente_assinatura=True
        - DiariasDocumentoSei 'despacho_diretor' É persistido com assinado=False
        - ciencia_diretor NÃO deve ser marcada True
        - Próxima chamada a despacho_diretor retorna pendente_assinatura (não gera novo doc)

        Isso permite retry via assinar_despacho_diretor sem criar doc órfão no SEI.
        """
        with app.app_context():
            usuario = _criar_usuario(db_session, nome='DIRETOR DFIN TESTE')
            it = _criar_itinerario(db_session, com_despacho_sga=True)
            _login_como(client, usuario)

            _mock_base_despacho(
                mocker,
                assinar_retorno={'sucesso': False, 'erro': 'Assinatura recusada pelo SEI'},
            )

            resp = client.post(
                f'/financeiro/diarias/{it.id}/despacho-diretor',
                json={
                    'ciencia': True,
                    'sei_usuario': 'diretor.dfin',
                    'sei_senha': 'senha_test',
                },
            )

            assert resp.status_code == 200, (
                f'RETRY-DIRETOR NÃO APLICADO: esperado 200 (pendente_assinatura), '
                f'recebeu {resp.status_code}. Padrão CCDP: salvar doc + retornar pendente.'
            )
            data = resp.get_json()
            assert data.get('sucesso') is True, (
                'RETRY-DIRETOR: sucesso deve ser True (doc criado, aguarda assinatura).'
            )
            assert data.get('pendente_assinatura') is True, (
                'RETRY-DIRETOR NÃO APLICADO: pendente_assinatura deve ser True quando '
                'assinatura falha. Use o botão de retry para completar.'
            )

            # ciencia_diretor NÃO deve ter sido marcada
            from app.models.diaria import DiariasItinerario, DiariasDocumentoSei
            it_reload = db_session.get(DiariasItinerario, it.id)
            assert not it_reload.ciencia_diretor, (
                'RETRY-DIRETOR: ciencia_diretor não deve ser marcada enquanto assinatura '
                'está pendente.'
            )

            # Documento DEVE existir no banco com assinado=False
            doc_dir = it_reload.get_doc('despacho_diretor')
            assert doc_dir is not None, (
                'RETRY-DIRETOR NÃO APLICADO: despacho_diretor deve ser persistido no banco '
                'com assinado=False para permitir retry sem criar doc órfão no SEI.'
            )
            assert doc_dir.assinado is False, (
                'RETRY-DIRETOR: documento deve ter assinado=False enquanto pendente.'
            )

    def test_despacho_diretor_segundo_call_retorna_pendente(
        self, client, db_session, app, mocker
    ):
        """
        Idempotência: segunda chamada a despacho_diretor quando doc já existe
        com assinado=False deve retornar pendente_assinatura sem criar novo doc.
        """
        with app.app_context():
            from app.models.diaria import DiariasDocumentoSei
            usuario = _criar_usuario(db_session, nome='DIRETOR DFIN IDEM')
            it = _criar_itinerario(db_session, com_despacho_sga=True)

            # Salva doc diretamente com assinado=False (simulando primeira chamada que falhou)
            doc = DiariasDocumentoSei(
                itinerario_id=it.id,
                tipo_documento='despacho_diretor',
                sei_id=f'DOCDIR{_uid()[:6]}',
                sei_formatado=f'000DIR{_uid()[:4]}',
                assinado=False,
            )
            db_session.add(doc)
            db_session.flush()

            _login_como(client, usuario)
            _mock_base_despacho(mocker, assinar_retorno={'sucesso': True})

            resp = client.post(
                f'/financeiro/diarias/{it.id}/despacho-diretor',
                json={'ciencia': True, 'sei_usuario': 'u', 'sei_senha': 's'},
            )

            data = resp.get_json()
            assert data.get('pendente_assinatura') is True, (
                'RETRY-DIRETOR idempotência: segunda chamada deve retornar pendente_assinatura '
                'quando doc já existe com assinado=False. Não deve gerar novo doc.'
            )
            # Verificar que não criou segundo doc
            from app.models.diaria import DiariasItinerario
            it_reload = db_session.get(DiariasItinerario, it.id)
            docs_dir = DiariasDocumentoSei.query.filter_by(
                itinerario_id=it.id, tipo_documento='despacho_diretor'
            ).count()
            assert docs_dir == 1, (
                f'RETRY-DIRETOR: não deve criar segundo documento. Encontrado: {docs_dir}.'
            )

    def test_despacho_diretor_sucesso_persiste_e_encaminha(
        self, client, db_session, app, mocker
    ):
        """
        Caminho feliz: assinatura OK → ciencia_diretor=True + doc salvo + processo encaminhado.
        """
        with app.app_context():
            usuario = _criar_usuario(db_session, nome='DIRETOR DFIN FELIZ')
            it = _criar_itinerario(db_session, com_despacho_sga=True)
            _login_como(client, usuario)

            enviar_mock = mocker.patch(
                'app.services.diarias_sei_integration.enviar_procedimento',
                return_value={'sucesso': True},
            )
            _mock_base_despacho(
                mocker,
                assinar_retorno={'sucesso': True},
            )

            resp = client.post(
                f'/financeiro/diarias/{it.id}/despacho-diretor',
                json={
                    'ciencia': True,
                    'sei_usuario': 'diretor.dfin',
                    'sei_senha': 'senha_test',
                },
            )

            assert resp.status_code == 200, (
                f'Caminho feliz: esperado 200, recebeu {resp.status_code}: {resp.get_json()}'
            )
            data = resp.get_json()
            assert data['sucesso'] is True

            from app.models.diaria import DiariasItinerario
            it_reload = db_session.get(DiariasItinerario, it.id)
            assert it_reload.ciencia_diretor is True, (
                'Caminho feliz: ciencia_diretor deveria ser True após sucesso.'
            )
            assert it_reload.has_doc('despacho_diretor'), (
                'Caminho feliz: despacho_diretor deveria ter sido salvo no banco.'
            )


class TestDespachoGeoAssinatura:
    """
    Testa que despacho_geo aborta quando a assinatura falha — não persiste
    ciencia_geo=True nem o documento no banco, nem encaminha o procedimento.
    """

    def test_despacho_geo_assinatura_falha_salva_pendente(
        self, client, db_session, app, mocker
    ):
        """
        TESTE CRÍTICO — Retry GEO (plano item 5).

        Quando assinar_documento falha em despacho_geo, o padrão CCDP se aplica:
        - Retorna 200 com sucesso=True e pendente_assinatura=True
        - DiariasDocumentoSei 'despacho_geo' É persistido com assinado=False
        - ciencia_geo NÃO deve ser marcada True
        """
        with app.app_context():
            usuario = _criar_usuario(db_session, nome='GERENTE GEO TESTE')
            it = _criar_itinerario(db_session, com_despacho_sga=True, com_despacho_diretor=True)
            _login_como(client, usuario)

            _mock_base_despacho(
                mocker,
                assinar_retorno={'sucesso': False, 'erro': 'Certificado inválido'},
            )

            resp = client.post(
                f'/financeiro/diarias/{it.id}/despacho-geo',
                json={
                    'ciencia': True,
                    'sei_usuario': 'gerente.geo',
                    'sei_senha': 'senha_test',
                },
            )

            assert resp.status_code == 200, (
                f'RETRY-GEO NÃO APLICADO: esperado 200 (pendente_assinatura), '
                f'recebeu {resp.status_code}.'
            )
            data = resp.get_json()
            assert data.get('sucesso') is True, 'RETRY-GEO: sucesso deve ser True.'
            assert data.get('pendente_assinatura') is True, (
                'RETRY-GEO NÃO APLICADO: pendente_assinatura deve ser True quando '
                'assinatura falha.'
            )

            from app.models.diaria import DiariasItinerario
            it_reload = db_session.get(DiariasItinerario, it.id)
            assert not it_reload.ciencia_geo, (
                'RETRY-GEO: ciencia_geo não deve ser marcada enquanto assinatura pendente.'
            )
            doc_geo = it_reload.get_doc('despacho_geo')
            assert doc_geo is not None, (
                'RETRY-GEO NÃO APLICADO: despacho_geo deve ser persistido com assinado=False.'
            )
            assert doc_geo.assinado is False, (
                'RETRY-GEO: documento deve ter assinado=False enquanto pendente.'
            )


class TestAssinarDespachosDiretor:
    """Testa o endpoint de retry assinar_despacho_diretor."""

    def test_assinar_despacho_diretor_marca_assinado_e_ciencia(
        self, client, db_session, app, mocker
    ):
        """
        Endpoint assinar_despacho_diretor: quando doc pendente existe e assinatura OK,
        marca assinado=True e ciencia_diretor=True.
        """
        with app.app_context():
            from app.models.diaria import DiariasDocumentoSei, DiariasItinerario
            usuario = _criar_usuario(db_session, nome='DIRETOR RETRY')
            it = _criar_itinerario(db_session, com_despacho_sga=True)

            # Doc pendente (criado mas não assinado)
            doc = DiariasDocumentoSei(
                itinerario_id=it.id,
                tipo_documento='despacho_diretor',
                sei_id=f'DOCDIR{_uid()[:6]}',
                sei_formatado=f'000DIR{_uid()[:4]}',
                assinado=False,
            )
            db_session.add(doc)
            db_session.flush()

            _login_como(client, usuario)
            _mock_base_despacho(mocker, assinar_retorno={'sucesso': True})

            resp = client.post(
                f'/financeiro/diarias/{it.id}/assinar-despacho-diretor',
                json={'sei_usuario': 'diretor.dfin', 'sei_senha': 'senha_test'},
            )

            assert resp.status_code == 200, (
                f'assinar_despacho_diretor: esperado 200, recebeu {resp.status_code}: '
                f'{resp.get_json()}'
            )
            data = resp.get_json()
            assert data.get('sucesso') is True, (
                f'assinar_despacho_diretor: sucesso deve ser True. Resposta: {data}'
            )

            it_reload = db_session.get(DiariasItinerario, it.id)
            doc_reload = it_reload.get_doc('despacho_diretor')
            assert doc_reload and doc_reload.assinado is True, (
                'assinar_despacho_diretor: assinado deve ser True após retry bem-sucedido.'
            )
            assert it_reload.ciencia_diretor is True, (
                'assinar_despacho_diretor: ciencia_diretor deve ser True após retry OK.'
            )

    def test_assinar_despacho_diretor_sem_doc_retorna_400(
        self, client, db_session, app, mocker
    ):
        """Sem doc pendente, assinar_despacho_diretor retorna 400."""
        with app.app_context():
            usuario = _criar_usuario(db_session, nome='DIRETOR RETRY VAZIO')
            it = _criar_itinerario(db_session, com_despacho_sga=True)
            _login_como(client, usuario)
            _mock_base_despacho(mocker, assinar_retorno={'sucesso': True})

            resp = client.post(
                f'/financeiro/diarias/{it.id}/assinar-despacho-diretor',
                json={'sei_usuario': 'diretor.dfin', 'sei_senha': 'senha_test'},
            )
            assert resp.status_code == 400, (
                f'assinar_despacho_diretor sem doc pendente deve retornar 400. '
                f'Recebido: {resp.status_code}.'
            )


class TestAssinarDespachoGeo:
    """Testa o endpoint de retry assinar_despacho_geo."""

    def test_assinar_despacho_geo_marca_assinado_e_ciencia(
        self, client, db_session, app, mocker
    ):
        """
        Endpoint assinar_despacho_geo: quando doc pendente existe e assinatura OK,
        marca assinado=True e ciencia_geo=True.
        """
        with app.app_context():
            from app.models.diaria import DiariasDocumentoSei, DiariasItinerario
            usuario = _criar_usuario(db_session, nome='GEO RETRY')
            it = _criar_itinerario(db_session, com_despacho_sga=True, com_despacho_diretor=True)

            doc = DiariasDocumentoSei(
                itinerario_id=it.id,
                tipo_documento='despacho_geo',
                sei_id=f'DOCGEO{_uid()[:6]}',
                sei_formatado=f'000GEO{_uid()[:4]}',
                assinado=False,
            )
            db_session.add(doc)
            db_session.flush()

            _login_como(client, usuario)
            _mock_base_despacho(mocker, assinar_retorno={'sucesso': True})

            resp = client.post(
                f'/financeiro/diarias/{it.id}/assinar-despacho-geo',
                json={'sei_usuario': 'gerente.geo', 'sei_senha': 'senha_test'},
            )

            assert resp.status_code == 200, (
                f'assinar_despacho_geo: esperado 200, recebeu {resp.status_code}: '
                f'{resp.get_json()}'
            )
            data = resp.get_json()
            assert data.get('sucesso') is True

            it_reload = db_session.get(DiariasItinerario, it.id)
            doc_reload = it_reload.get_doc('despacho_geo')
            assert doc_reload and doc_reload.assinado is True, (
                'assinar_despacho_geo: assinado deve ser True após retry bem-sucedido.'
            )
            assert it_reload.ciencia_geo is True, (
                'assinar_despacho_geo: ciencia_geo deve ser True após retry OK.'
            )
