"""
Contagem de documentos confirmados no SEI (#2 do plano)
========================================================
Arquivo alvo: app/financeiro/routes/diarias.py

Problema:
    Registros NR/NE/NL/PD/OB são COMMITADOS antes do upload ao SEI.
    Se o upload falhar, a linha fica no banco com sei_id=NULL.
    O marcador agregado (set_doc) e os gates de próxima etapa contam
    `modelo.query.count()` — que inclui linhas sem sei_id.
    Isso pode avançar a etapa e liberar PD/OB mesmo sem confirmação no SEI.

Fix esperado:
    Toda contagem de completude deve usar `.filter(modelo.sei_id.isnot(None))`.

Como rodar:
    pytest tests/diarias/test_contagem_sei.py -v
"""
import uuid
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock


def _uid():
    return uuid.uuid4().hex[:8]


def _criar_usuario(db_session):
    from app.models.usuario import Usuario
    sid = f'usr_{_uid()}'
    u = Usuario(
        id_usuario_sei=sid,
        nome='USUARIO FINANCEIRO TESTE',
        sigla_login=f'{sid}@sead.pi.gov.br',
        ativo=True,
        is_admin=True,
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_itinerario(db_session, etapa_id):
    from app.models.diaria import DiariasItinerario
    it = DiariasItinerario(
        usuario_gerador=f'gen_{_uid()}',
        tipo_solicitacao_id=1,
        tipo_itinerario=1,
        status_id=1,
        data_solicitacao=date(2026, 5, 1),
        data_viagem=datetime(2026, 6, 1),
        data_retorno=datetime(2026, 6, 3),
        qtd_diarias_solicitadas=Decimal('2.0'),
        etapa_atual_id=etapa_id,
        sei_protocolo=f'00002.{_uid()}/2026-99',
        sei_id_procedimento=f'PROC{_uid()}',
    )
    db_session.add(it)
    db_session.flush()
    return it


def _add_item(db_session, itinerario_id, cpf=None):
    from app.models.diaria import DiariasItemItinerario
    item = DiariasItemItinerario(
        id_itinerario=itinerario_id,
        cpf_pessoa=cpf or f'{_uid()[:11]}',
        nome_pessoa='Servidor Teste',
    )
    db_session.add(item)
    db_session.flush()
    return item


def _add_doc_sei(db_session, itinerario, tipo_documento):
    from app.models.diaria import DiariasDocumentoSei
    doc = DiariasDocumentoSei(
        itinerario_id=itinerario.id,
        tipo_documento=tipo_documento,
        sei_id=f'SEI{_uid()}',
        sei_formatado=f'000{_uid()[:6]}',
    )
    db_session.add(doc)
    db_session.flush()
    return doc


def _login_como(client, usuario):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True


class TestNRMarcadorAgregarExigeSeiId:
    """
    Testa que o marcador agregado 'nota_reserva' só é gravado quando
    TODAS as NRs têm sei_id preenchido (upload ao SEI confirmado).
    """

    def test_marcador_nr_nao_disparado_sem_sei_id(self, client, db_session, app, mocker):
        """
        TESTE CRÍTICO — Contagem sei_id: NR

        Cenário: 2 servidores. 1 NR já tem sei_id (upload OK).
        O 2º servidor insere NR mas o upload ao SEI falha (adicionar_documento_externo retorna None).
        O marcador agregado 'nota_reserva' NÃO deve ser gravado.

        ANTES do fix: a NR é commitada sem sei_id → count() = 2 ≥ 2 = True → set_doc disparado (BUG).
        APÓS o fix: count com sei_id.isnot(None) = 1 < 2 → set_doc NÃO disparado (CORRETO).
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasNotaReserva

            usuario = _criar_usuario(db_session)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO))
            _add_doc_sei(db_session, it, 'despacho_dfin')

            item1 = _add_item(db_session, it.id)
            item2 = _add_item(db_session, it.id)

            # NR do servidor 1 já existe COM sei_id (upload anterior OK)
            nr1 = DiariasNotaReserva(
                itinerario_id=it.id,
                item_itinerario_id=item1.id,
                codigo='2026NR11111',
                sei_id=f'SEI{_uid()}',
                sei_formatado=f'000{_uid()[:6]}',
            )
            db_session.add(nr1)
            db_session.flush()

            # Mock: GPO tem caixa, token OK, mas upload ao SEI falha (retorna None)
            mocker.patch('app.financeiro.routes.diarias.usuario_tem_caixa', return_value=True)
            mocker.patch('app.financeiro.routes.diarias.gerar_token_sei_admin', return_value='tk_admin')
            mocker.patch(
                'app.financeiro.routes.diarias.adicionar_documento_externo',
                return_value=None,  # Upload falhou
            )
            mocker.patch('app.financeiro.routes.diarias.enviar_procedimento', return_value={'sucesso': True})

            _login_como(client, usuario)

            import io
            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-nr',
                data={
                    'item_itinerario_id': str(item2.id),
                    'nota_reserva': '2026NR22222',
                    'valor_nr': '100,00',
                    'arquivo_nr': (io.BytesIO(b'%PDF-1.4 dummy'), 'nr2.pdf'),
                },
                content_type='multipart/form-data',
            )

            # NR deve ter sido salva no banco (sem sei_id)
            nr2 = DiariasNotaReserva.query.filter_by(
                itinerario_id=it.id, item_itinerario_id=item2.id
            ).first()
            assert nr2 is not None, 'NR deve ter sido salva no banco mesmo sem upload ao SEI.'
            assert nr2.sei_id is None, 'NR sem upload SEI não deve ter sei_id.'

            # Marcador agregado NÃO deve ter sido disparado (1 sem sei_id)
            marcador = it.get_doc('nota_reserva')
            assert marcador is None, (
                'CONTAGEM-SEI NÃO APLICADO: set_doc("nota_reserva") foi chamado mesmo com '
                f'NR sem sei_id. O marcador agregado exige que TODAS as NRs tenham sei_id. '
                f'Marcador encontrado: {marcador}'
            )


class TestNEGateDespachoExigeSeiId:
    """
    Testa que o gate do Despacho CCDP (exige NE por servidor) só conta
    NEs com sei_id preenchido.
    """

    def test_despacho_ccdp_bloqueado_se_ne_sem_sei_id(self, client, db_session, app, mocker):
        """
        TESTE CRÍTICO — Contagem sei_id: NE gate do Despacho CCDP

        Cenário: 2 servidores, 2 NEs no banco, mas apenas 1 tem sei_id.
        O gate do despacho_ccdp deve BLOQUEAR (não todas as NEs confirmadas no SEI).

        ANTES do fix: total_nes_g = 2 ≥ 2 → gate passa (BUG).
        APÓS o fix: total_nes_g (sei_id) = 1 < 2 → retorna 400 (CORRETO).
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasNotaEmpenho

            usuario = _criar_usuario(db_session)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO_2))
            _add_doc_sei(db_session, it, 'despacho_sga')

            item1 = _add_item(db_session, it.id)
            item2 = _add_item(db_session, it.id)

            # NE 1 — com sei_id (upload confirmado)
            ne1 = DiariasNotaEmpenho(
                itinerario_id=it.id,
                item_itinerario_id=item1.id,
                codigo='2026NE11111',
                sei_id=f'SEI{_uid()}',
            )
            # NE 2 — SEM sei_id (upload falhou)
            ne2 = DiariasNotaEmpenho(
                itinerario_id=it.id,
                item_itinerario_id=item2.id,
                codigo='2026NE22222',
                sei_id=None,
            )
            db_session.add(ne1)
            db_session.add(ne2)
            db_session.flush()

            mocker.patch('app.financeiro.routes.diarias.gerar_token_sei_admin', return_value='tk_admin')
            mocker.patch('app.financeiro.routes.diarias.autenticar_usuario_sei',
                         return_value={'token': 'tk', 'id_usuario': 'u', 'id_login': 'l', 'cargo': 'C'})

            _login_como(client, usuario)

            resp = client.post(
                f'/financeiro/diarias/{it.id}/despacho-ccdp',
                json={'sei_usuario': 'u', 'sei_senha': 's'},
            )

            assert resp.status_code == 400, (
                f'CONTAGEM-SEI NÃO APLICADO: despacho_ccdp deveria retornar 400 pois '
                f'NE do servidor 2 não tem sei_id (upload pendente). '
                f'Recebido: {resp.status_code}. '
                f'O gate deve contar apenas NEs com sei_id preenchido.'
            )
            data = resp.get_json()
            assert data is not None and data.get('sucesso') is False, (
                'CONTAGEM-SEI: despacho_ccdp deve retornar sucesso=False.'
            )


class TestNLGateExigeSeiId:
    """
    Testa que o gate NL→PD conta apenas NLs com sei_id preenchido.
    """

    def test_inserir_pd_bloqueado_se_nl_sem_sei_id(self, client, db_session, app, mocker):
        """
        TESTE CRÍTICO — Contagem sei_id: gate NL para PD

        Cenário: 2 servidores, 2 NLs no banco, mas 1 sem sei_id.
        Inserir PD deve ser BLOQUEADO.

        ANTES do fix: total_nl = 2 ≥ 2 → gate passa (BUG).
        APÓS o fix: total_nl (sei_id) = 1 < 2 → retorna 400 (CORRETO).
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasNotaLiquidacao

            usuario = _criar_usuario(db_session)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.CONCESSAO_DIARIAS))
            _add_doc_sei(db_session, it, 'despacho_geo')

            item1 = _add_item(db_session, it.id)
            item2 = _add_item(db_session, it.id)

            # NL 1 — com sei_id
            nl1 = DiariasNotaLiquidacao(
                itinerario_id=it.id, item_itinerario_id=item1.id,
                codigo='2026NL11111', sei_id=f'SEI{_uid()}',
            )
            # NL 2 — SEM sei_id
            nl2 = DiariasNotaLiquidacao(
                itinerario_id=it.id, item_itinerario_id=item2.id,
                codigo='2026NL22222', sei_id=None,
            )
            db_session.add(nl1)
            db_session.add(nl2)
            db_session.flush()

            mocker.patch('app.financeiro.routes.diarias.usuario_tem_caixa', return_value=True)

            _login_como(client, usuario)

            import io
            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-pd',
                data={
                    'item_itinerario_id': str(item1.id),
                    'codigo': '2026PD11111',
                    'valor': '100,00',
                    'arquivo': (io.BytesIO(b'%PDF-1.4 dummy'), 'pd1.pdf'),
                },
                content_type='multipart/form-data',
            )

            assert resp.status_code == 400, (
                f'CONTAGEM-SEI NÃO APLICADO: inserir_pd deveria retornar 400 pois '
                f'NL do servidor 2 não tem sei_id. '
                f'Recebido: {resp.status_code}.'
            )
