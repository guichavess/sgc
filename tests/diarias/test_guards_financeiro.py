"""
P1-FIX-4 — Guards de etapa/pré-requisitos em NE / PD / OB
===========================================================
Arquivo alvo: app/financeiro/routes/diarias.py
Funções: inserir_nota_empenho, inserir_pd, inserir_ob

Problema:
    inserir_nota_empenho não valida:
      (a) se etapa_atual_id >= ANALISE_SOLICITACAO_2 (6)
      (b) se o itinerário tem o despacho_sga gerado

    inserir_pd não valida:
      (a) se etapa_atual_id >= CONCESSAO_DIARIAS (4)
      (b) se o itinerário tem o despacho_geo gerado

    inserir_ob não valida:
      (a) se etapa_atual_id >= CONCESSAO_DIARIAS (4) [mesmo guard que PD]

    Isso permite inserir NE/PD/OB em etapas indevidas, corrompendo o fluxo.

Fix esperado:
    inserir_nota_empenho: guard etapa + guard despacho_sga antes do processamento.
    inserir_pd: guard etapa + guard despacho_geo antes da contagem de NLs.
    inserir_ob: guard etapa + guard despacho_geo antes da contagem de PDs.

Como rodar:
    pytest tests/diarias/test_guards_financeiro.py -v
"""
import uuid
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def _uid():
    return uuid.uuid4().hex[:8]


def _criar_usuario(db_session, nome=None):
    from app.models.usuario import Usuario
    sid = f'usr_{_uid()}'
    u = Usuario(
        id_usuario_sei=sid,
        nome=nome or 'USUARIO FINANCEIRO TESTE',
        sigla_login=f'{sid}@sead.pi.gov.br',
        ativo=True,
        is_admin=True,  # is_admin=True bypassa @requires_permission nos testes
    )
    db_session.add(u)
    db_session.flush()
    return u


def _criar_itinerario(db_session, etapa_id, com_sei=True):
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
        sei_protocolo=f'00002.{_uid()}/2026-99' if com_sei else None,
        sei_id_procedimento=f'PROC{_uid()}' if com_sei else None,
    )
    db_session.add(it)
    db_session.flush()
    return it


def _add_doc(db_session, itinerario, tipo_documento):
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


def _login_como(client, usuario):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True


# ══════════════════════════════════════════════════════════════════════════════
# MUDANÇA 4 — Guards de etapa/pré-requisitos
# ══════════════════════════════════════════════════════════════════════════════

class TestInserirNEGuards:
    """
    Testa guards de etapa e pré-requisito de despacho_sga em inserir_nota_empenho.
    """

    def test_inserir_ne_bloqueado_etapa_anterior(self, client, db_session, app):
        """
        TESTE CRÍTICO — P1 Fix 4a.

        NE não pode ser inserida se etapa_atual_id < ANALISE_SOLICITACAO_2 (6).
        Itinerário em etapa 3 (ANALISE_SOLICITACAO) → deve ser bloqueado.

        ANTES do fix: processamento continua sem validação de etapa (BUG — NE é criada).
        APÓS o fix: redireciona (302) com flash de erro, NE não é criada.

        Nota: inserir_nota_empenho é um endpoint de form que faz redirect.
        O guard de etapa deve redirecionar antes do UPSERT da NE.
        O resultado esperado é 302 (redirect de volta ao detalhe) SEM a NE no banco.
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            usuario = _criar_usuario(db_session, nome='FIN NE ETAPA TESTE')
            # Etapa 3 — antes da ANALISE_SOLICITACAO_2 (6)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO))
            _add_doc(db_session, it, 'despacho_sga')  # tem despacho_sga, mas etapa errada
            item = _add_item(db_session, it.id)

            _login_como(client, usuario)

            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-nota-empenho',
                data={
                    'item_itinerario_id': str(item.id),
                    'nota_empenho_codigo': '210101-2026NE99999',
                },
            )

            # NE não deve ter sido criada (independente do código HTTP)
            from app.models.diaria import DiariasNotaEmpenho
            ne = DiariasNotaEmpenho.query.filter_by(itinerario_id=it.id).first()
            assert ne is None, (
                f'P1-FIX-4a NÃO APLICADO: NE foi criada mesmo em etapa indevida '
                f'(etapa={int(DiariasEtapaID.ANALISE_SOLICITACAO)} < ANALISE_SOLICITACAO_2=6). '
                f'código={ne.codigo if ne else None}. O guard deve bloquear antes do UPSERT.'
            )

    def test_inserir_ne_bloqueado_sem_despacho_sga(self, client, db_session, app, mocker):
        """
        TESTE CRÍTICO — P1 Fix 4b.

        NE não pode ser inserida se despacho_sga ainda não foi gerado,
        mesmo que a etapa esteja correta.

        ANTES do fix: processamento continua sem verificar despacho_sga (BUG — NE é criada).
        APÓS o fix: redireciona (302) com flash de erro, NE não é criada.

        Nota: inserir_nota_empenho é endpoint de form. O guard deve redirecionar
        antes do UPSERT. A chave do teste é: NE não foi criada no banco.
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            usuario = _criar_usuario(db_session, nome='FIN NE SGA TESTE')
            # Etapa correta (6 = ANALISE_SOLICITACAO_2), mas SEM despacho_sga
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO_2))
            # Deliberadamente NÃO adiciona despacho_sga
            item = _add_item(db_session, it.id)

            _login_como(client, usuario)

            # Mock para evitar chamadas reais ao SEI
            mocker.patch(
                'app.financeiro.routes.diarias.gerar_token_sei_admin',
                return_value='tk_admin',
            )

            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-nota-empenho',
                data={
                    'item_itinerario_id': str(item.id),
                    'nota_empenho_codigo': '210101-2026NE88888',
                },
            )

            # NE não deve ter sido criada (independente do código HTTP)
            from app.models.diaria import DiariasNotaEmpenho
            ne = DiariasNotaEmpenho.query.filter_by(itinerario_id=it.id).first()
            assert ne is None, (
                f'P1-FIX-4b NÃO APLICADO: NE foi criada mesmo sem despacho_sga. '
                f'código={ne.codigo if ne else None}. O guard deve bloquear antes do UPSERT.'
            )


class TestInserirPDGuards:
    """
    Testa guards de etapa e pré-requisito de despacho_geo em inserir_pd.
    """

    def test_inserir_pd_bloqueado_sem_despacho_geo(self, client, db_session, app):
        """
        TESTE CRÍTICO — P1 Fix 4c.

        PD não pode ser inserida se despacho_geo não foi gerado, mesmo que
        a etapa esteja correta e todas as NLs estejam inseridas.

        ANTES do fix: PD só valida contagem de NLs, ignora despacho_geo (BUG).
        APÓS o fix: retorna 400 JSON com mensagem sobre despacho_geo.

        Nota: inserir_pd retorna JSON (via _inserir_doc_financeiro_servidor não,
        mas o guard adicionado antes da chamada deve retornar JSON 400).
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasNotaLiquidacao

            usuario = _criar_usuario(db_session, nome='FIN PD GEO TESTE')
            # Etapa correta (4 = CONCESSAO_DIARIAS), COM NL, SEM despacho_geo
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.CONCESSAO_DIARIAS))
            item = _add_item(db_session, it.id)
            # Adiciona NL para o servidor (gate de NLs seria OK)
            nl = DiariasNotaLiquidacao(
                itinerario_id=it.id,
                item_itinerario_id=item.id,
                codigo='210101-2026NL11111',
            )
            db_session.add(nl)
            db_session.flush()
            # Deliberadamente NÃO adiciona despacho_geo

            _login_como(client, usuario)

            # inserir_pd usa request.form, NÃO JSON
            import io
            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-pd',
                data={
                    'item_itinerario_id': str(item.id),
                    'codigo': '210101-2026PD11111',
                    'valor': '100,00',
                },
                content_type='multipart/form-data',
            )

            # Guard deve retornar JSON 400 antes de chegar no processamento de form
            assert resp.status_code in (400, 403), (
                f'P1-FIX-4c NÃO APLICADO: inserir_pd não bloqueou quando despacho_geo ausente. '
                f'Recebido: {resp.status_code}. '
                f'O guard deve retornar JSON 400 antes do processamento.'
            )
            data = resp.get_json()
            assert data is not None, 'P1-FIX-4c: Resposta deveria ser JSON'
            assert data['sucesso'] is False, (
                'P1-FIX-4c NÃO APLICADO: sucesso=True mesmo sem despacho_geo.'
            )
            # Mensagem deve mencionar despacho_geo
            assert 'despacho' in (data.get('erro') or '').lower() or 'geo' in (data.get('erro') or '').lower(), (
                f'P1-FIX-4c: Mensagem de erro deveria mencionar despacho_geo. '
                f'Erro atual: {data.get("erro")!r}'
            )


class TestInserirOBGuards:
    """
    Testa guard de etapa em inserir_ob.
    """

    def test_inserir_ob_bloqueado_etapa_anterior(self, client, db_session, app):
        """
        TESTE CRÍTICO — P1 Fix 4d.

        OB não pode ser inserida se etapa_atual_id < CONCESSAO_DIARIAS (4).
        Itinerário em etapa 3 → deve ser bloqueado.

        ANTES do fix: OB só valida contagem de PDs, não valida etapa (BUG).
        APÓS o fix: retorna 400 JSON quando etapa < CONCESSAO_DIARIAS.

        Nota: inserir_ob chama _inserir_doc_financeiro_servidor que redireciona (302).
        O guard adicionado ANTES da chamada deve retornar JSON 400.
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasProgramacaoDesembolso

            usuario = _criar_usuario(db_session, nome='FIN OB ETAPA TESTE')
            # Etapa 3 (ANALISE_SOLICITACAO) — antes de CONCESSAO_DIARIAS (4)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO))
            item = _add_item(db_session, it.id)
            # Adiciona PD para o servidor (gate de PDs seria OK)
            pd = DiariasProgramacaoDesembolso(
                itinerario_id=it.id,
                item_itinerario_id=item.id,
                codigo='210101-2026PD22222',
            )
            db_session.add(pd)
            db_session.flush()

            _login_como(client, usuario)

            # inserir_ob usa request.form, não JSON
            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-ob',
                data={
                    'item_itinerario_id': str(item.id),
                    'codigo': '210101-2026OB22222',
                    'valor': '100,00',
                },
                content_type='multipart/form-data',
            )

            # Guard deve retornar JSON 400 antes de chegar no processamento de form
            assert resp.status_code in (400, 403), (
                f'P1-FIX-4d NÃO APLICADO: inserir_ob não bloqueou quando etapa='
                f'{int(DiariasEtapaID.ANALISE_SOLICITACAO)} < CONCESSAO_DIARIAS(4). '
                f'Recebido: {resp.status_code}. '
                f'O guard deve retornar JSON 400 antes do processamento.'
            )
            data = resp.get_json()
            assert data is not None, 'P1-FIX-4d: Resposta deveria ser JSON'
            assert data['sucesso'] is False, (
                'P1-FIX-4d NÃO APLICADO: sucesso=True mesmo em etapa indevida para OB.'
            )


# ══════════════════════════════════════════════════════════════════════════════
# GUARDS POR ORDEM CRONOLÓGICA — etapa 6 (ANALISE_SOLICITACAO_2) NÃO pode
# inserir NL/PD/OB, mesmo que 6 > 4 numericamente.
# ══════════════════════════════════════════════════════════════════════════════

class TestGuardsOrdemCronologica:
    """
    Testa que NL/PD/OB só podem ser inseridos a partir de CONCESSAO_DIARIAS (etapa 4)
    na ORDEM CRONOLÓGICA real, não pela comparação numérica de ID.

    O bug: guard `etapa_atual_id < CONCESSAO_DIARIAS(4)` é furado para etapa 6
    porque 6 > 4 numericamente, logo 6 < 4 = False → guard passa → NL/PD/OB liberados
    indevidamente em ANALISE_SOLICITACAO_2.

    Fix: usar etapa_diaria_em_ou_apos() que compara pela ordem cronológica real.
    """

    def test_inserir_nl_bloqueado_em_etapa_analise_solicitacao_2(self, client, db_session, app):
        """
        TESTE CRÍTICO — Guards por ordem.

        NL não pode ser inserida se o processo está em ANALISE_SOLICITACAO_2 (etapa 6),
        mesmo que 6 > 4 (CONCESSAO_DIARIAS) numericamente.
        Na ordem real: etapa 6 vem ANTES de etapa 4.

        ANTES do fix: 6 < 4 = False → guard não bloqueia → NL criada (BUG).
        APÓS o fix: ordem(6)=3 < ordem(4)=4 → guard bloqueia → retorna 400 (CORRETO).
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasDocumentoSei

            usuario = _criar_usuario(db_session, nome='FIN NL ORDEM TESTE')
            # Etapa 6 — ANALISE_SOLICITACAO_2 (ID=6, mas ordem=3, antes de CONCESSAO=ordem=4)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO_2))
            # Adiciona despacho_geo para não cair no guard de pré-requisito
            geo = DiariasDocumentoSei(
                itinerario_id=it.id,
                tipo_documento='despacho_geo',
                sei_id=f'SEIGEO{_uid()[:6]}',
                sei_formatado=f'000GEO{_uid()[:4]}',
            )
            db_session.add(geo)
            item = _add_item(db_session, it.id)
            db_session.flush()

            _login_como(client, usuario)

            import io
            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-nl',
                data={
                    'item_itinerario_id': str(item.id),
                    'codigo': '210101-2026NL99999',
                    'valor': '100,00',
                    'arquivo': (io.BytesIO(b'%PDF-1.4 dummy'), 'nl.pdf'),
                },
                content_type='multipart/form-data',
            )

            assert resp.status_code in (400, 403), (
                f'GUARD-ORDEM NÃO APLICADO: inserir_nl não bloqueou para etapa 6 '
                f'(ANALISE_SOLICITACAO_2). 6>4 numericamente, mas etapa 6 vem ANTES '
                f'de etapa 4 na ordem real. Recebido: {resp.status_code}.'
            )
            data = resp.get_json()
            assert data is not None and data.get('sucesso') is False, (
                'GUARD-ORDEM: inserir_nl deve retornar JSON com sucesso=False para etapa 6.'
            )

    def test_inserir_pd_bloqueado_em_etapa_analise_solicitacao_2(self, client, db_session, app):
        """
        TESTE CRÍTICO — Guards por ordem: PD bloqueado em etapa 6.
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasDocumentoSei, DiariasNotaLiquidacao

            usuario = _criar_usuario(db_session, nome='FIN PD ORDEM TESTE')
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO_2))
            geo = DiariasDocumentoSei(
                itinerario_id=it.id, tipo_documento='despacho_geo',
                sei_id=f'SEIGEO{_uid()[:6]}', sei_formatado=f'000GEO{_uid()[:4]}',
            )
            db_session.add(geo)
            item = _add_item(db_session, it.id)
            # NL com sei_id (gate de NL seria OK)
            nl = DiariasNotaLiquidacao(
                itinerario_id=it.id, item_itinerario_id=item.id,
                codigo='210101-2026NL11111', sei_id=f'SEI{_uid()}',
            )
            db_session.add(nl)
            db_session.flush()

            _login_como(client, usuario)

            import io
            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-pd',
                data={
                    'item_itinerario_id': str(item.id),
                    'codigo': '210101-2026PD99999',
                    'valor': '100,00',
                    'arquivo': (io.BytesIO(b'%PDF-1.4 dummy'), 'pd.pdf'),
                },
                content_type='multipart/form-data',
            )

            assert resp.status_code in (400, 403), (
                f'GUARD-ORDEM NÃO APLICADO: inserir_pd não bloqueou para etapa 6. '
                f'Recebido: {resp.status_code}.'
            )

    def test_inserir_ob_bloqueado_em_etapa_analise_solicitacao_2(self, client, db_session, app):
        """
        TESTE CRÍTICO — Guards por ordem: OB bloqueada em etapa 6.
        """
        with app.app_context():
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasDocumentoSei, DiariasProgramacaoDesembolso

            usuario = _criar_usuario(db_session, nome='FIN OB ORDEM TESTE')
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO_2))
            geo = DiariasDocumentoSei(
                itinerario_id=it.id, tipo_documento='despacho_geo',
                sei_id=f'SEIGEO{_uid()[:6]}', sei_formatado=f'000GEO{_uid()[:4]}',
            )
            db_session.add(geo)
            item = _add_item(db_session, it.id)
            # PD com sei_id (gate de PD seria OK)
            pd = DiariasProgramacaoDesembolso(
                itinerario_id=it.id, item_itinerario_id=item.id,
                codigo='210101-2026PD11111', sei_id=f'SEI{_uid()}',
            )
            db_session.add(pd)
            db_session.flush()

            _login_como(client, usuario)

            import io
            resp = client.post(
                f'/financeiro/diarias/{it.id}/inserir-ob',
                data={
                    'item_itinerario_id': str(item.id),
                    'codigo': '210101-2026OB99999',
                    'valor': '100,00',
                    'arquivo': (io.BytesIO(b'%PDF-1.4 dummy'), 'ob.pdf'),
                },
                content_type='multipart/form-data',
            )

            assert resp.status_code in (400, 403), (
                f'GUARD-ORDEM NÃO APLICADO: inserir_ob não bloqueou para etapa 6. '
                f'Recebido: {resp.status_code}.'
            )
