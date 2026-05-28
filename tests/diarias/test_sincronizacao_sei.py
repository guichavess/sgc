"""
P1-FIX-3 — Sincronização SEI monotônica + registro de regressão
=================================================================
Arquivo alvo: app/services/diarias_sei_integration.py
Função: sincronizar_documentos_diaria (~linha 4185)

Problema:
    A função sincronizar_documentos_diaria pode determinar uma etapa baseada nos
    documentos encontrados no SEI que seja MENOR que a etapa atual. Isso causa
    uma regressão do processo — o itinerário "volta" de etapa 4 para etapa 3,
    por exemplo, se algum documento não for encontrado numa determinada consulta.

Fix esperado:
    Antes de atribuir itinerario.etapa_atual_id = int(etapa_nova):
    - Se int(etapa_nova) < int(itinerario.etapa_atual_id), bloquear a regressão.
    - Logar warning com detalhes.
    - Inserir DiariasHistoricoMovimentacao de auditoria com comentário explicativo.
    - Ajustar resultado['etapa_nova'] para a etapa atual (monotônica).
    - Manter etapa_atual_id intocado.

Como rodar:
    pytest tests/diarias/test_sincronizacao_sei.py -v
"""
import uuid
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def _uid():
    return uuid.uuid4().hex[:8]


def _criar_etapa(db_session, etapa_id, nome, ordem):
    """Cria DiariasEtapa no banco (necessária para FK em DiariasHistoricoMovimentacao).

    Usa get-or-create para ser idempotente: a sessão é de scope 'session' e
    sincronizar_documentos_diaria faz db.session.commit() internamente, então
    os dados podem persistir entre testes.
    """
    from app.models.diaria import DiariasEtapa
    # Tenta buscar primeiro para evitar UNIQUE constraint
    try:
        existing = db_session.get(DiariasEtapa, etapa_id)
    except Exception:
        existing = None
    if existing:
        return existing
    # Fallback: tenta via query
    try:
        existing = db_session.query(DiariasEtapa).filter_by(id=etapa_id).first()
        if existing:
            return existing
    except Exception:
        pass
    etapa = DiariasEtapa(
        id=etapa_id,
        nome=nome,
        alias=nome.lower().replace(' ', '_'),
        ordem=ordem,
    )
    db_session.add(etapa)
    try:
        db_session.flush()
    except Exception:
        db_session.rollback()
        # Segunda tentativa: já existe (race condition)
        return db_session.query(DiariasEtapa).filter_by(id=etapa_id).first()
    return etapa


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


def _criar_etapas_minimas(db_session):
    """Cria as etapas necessárias para FK em DiariasHistoricoMovimentacao."""
    etapas = [
        (1, 'Solicitação Inicial', 1),
        (2, 'Escolha do Voo', 2),
        (3, 'Análise 1ª Parte', 3),
        (4, 'Concessão de Diárias', 4),
        (5, 'Prestação de Contas', 5),
        (6, 'Análise 2ª Parte', 6),
    ]
    for eid, nome, ordem in etapas:
        _criar_etapa(db_session, eid, nome, ordem)


# ══════════════════════════════════════════════════════════════════════════════
# MUDANÇA 3 — Sincronização SEI monotônica
# ══════════════════════════════════════════════════════════════════════════════

class TestSincronizacaoSEIMonotonica:
    """
    Testa que sincronizar_documentos_diaria não regride a etapa quando os
    documentos encontrados no SEI sugerem uma etapa anterior à atual.
    """

    def _mock_consultar_docs(self, mocker, tipos_documentos):
        """
        Mock de consultar_documentos_procedimento retornando documentos específicos.

        tipos_documentos: lista de strings como 'nota_reserva', 'relatorio_viagem', etc.
        Cada string é mapeada para seu IdSerie correspondente via SERIE_TIPO_DOCUMENTO_MAP
        (invertido).
        """
        from app.services.diarias_sei_integration import SERIE_TIPO_DOCUMENTO_MAP
        # Inverte o mapa: tipo → serie
        tipo_para_serie = {v: k for k, v in SERIE_TIPO_DOCUMENTO_MAP.items()}

        docs = []
        for tipo in tipos_documentos:
            id_serie = tipo_para_serie.get(tipo, f'9999{_uid()[:4]}')
            docs.append({
                'IdDocumento': f'DOC{_uid()}',
                'DocumentoFormatado': f'000{_uid()[:6]}',
                'Descricao': tipo,
                'Numero': '',
                'Serie': {'IdSerie': id_serie, 'Nome': tipo},
                'Assinaturas': [],
            })

        mocker.patch(
            'app.services.diarias_sei_integration.consultar_documentos_procedimento',
            return_value={'sucesso': True, 'documentos': docs},
        )
        # Mock de verificar_autorizacao_diaria para evitar chamadas SEI adicionais
        mocker.patch(
            'app.services.diarias_sei_integration.verificar_autorizacao_diaria',
            return_value={'autorizada': False, 'avancou_etapa': False},
        )
        mocker.patch(
            'app.services.diarias_sei_integration.detectar_despacho_sga_negacao',
            return_value={'negado': False},
        )
        mocker.patch(
            'app.services.diarias_sei_integration.aplicar_negacao_detectada_sei',
            return_value=False,
        )

    def test_sincronizacao_bloqueia_regressao(self, db_session, app, mocker):
        """
        TESTE CRÍTICO — P1 Fix 3.

        Itinerário está na etapa 4 (CONCESSAO_DIARIAS). SEI retorna apenas
        nota_reserva → lógica calcularia etapa 3 (ANALISE_SOLICITACAO).
        Fix: etapa deve PERMANECER em 4, regressão bloqueada.

        ANTES do fix: etapa regride de 4 → 3 (BUG).
        APÓS o fix: etapa mantida em 4, DiariasHistoricoMovimentacao de auditoria criado.

        Nota: sincronizar_documentos_diaria chama db.session.commit() internamente.
        Mockamos o commit para evitar persistência inter-testes no SQLite in-memory.
        """
        with app.app_context():
            _criar_etapas_minimas(db_session)
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasHistoricoMovimentacao

            # Itinerário na etapa 4 (Concessão)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.CONCESSAO_DIARIAS))

            # SEI retorna apenas nota_reserva → etapa calculada seria 3 (regressão)
            self._mock_consultar_docs(mocker, ['nota_reserva'])

            # Mock apenas do commit interno para evitar persistência inter-testes.
            # O add() não é mockado pois db_session e _db.session são o mesmo objeto
            # (SQLAlchemy session compartilhada), evitando recursão.
            # Os objetos adicionados ficam na sessão como pending (new) até flush.
            from app.extensions import db as _db
            mocker.patch.object(_db.session, 'commit')

            from app.services.diarias_sei_integration import sincronizar_documentos_diaria
            resultado = sincronizar_documentos_diaria(it)

            # Etapa NÃO deve ter regredido
            assert it.etapa_atual_id == int(DiariasEtapaID.CONCESSAO_DIARIAS), (
                f'P1-FIX-3 NÃO APLICADO: etapa regrediu de 4 para {it.etapa_atual_id}. '
                f'sincronizar_documentos_diaria deve ser monotônica (nunca regredir).'
            )

            # resultado['etapa_nova'] deve ser ajustado para a etapa atual (4), não 3
            assert resultado['etapa_nova'] == int(DiariasEtapaID.CONCESSAO_DIARIAS), (
                f'P1-FIX-3 NÃO APLICADO: resultado["etapa_nova"]={resultado["etapa_nova"]} '
                f'deveria ser {int(DiariasEtapaID.CONCESSAO_DIARIAS)} (etapa atual mantida).'
            )

            # Verifica que o registro de auditoria foi _tentado_ (add foi chamado)
            # O histórico está pendente na sessão (mock do commit impede persistência real)
            from app.models.diaria import DiariasHistoricoMovimentacao
            # Verifica nos objetos pendentes na sessão (new objects ainda não commitados)
            pendentes = [
                obj for obj in db_session.new
                if isinstance(obj, DiariasHistoricoMovimentacao)
                and obj.id_itinerario == it.id
            ]
            # Ou já commitados (flush antes do mock)
            commitados = (
                db_session.query(DiariasHistoricoMovimentacao)
                .filter_by(id_itinerario=it.id)
                .all()
            )
            todos = pendentes + commitados
            assert len(todos) > 0, (
                'P1-FIX-3 NÃO APLICADO: Nenhum DiariasHistoricoMovimentacao criado para '
                'registrar a tentativa de regressão.'
            )
            # Verifica o comentário no último registro encontrado
            ultimo = todos[-1]
            assert '[SYNC SEI]' in (ultimo.comentario or '') or 'regressão' in (ultimo.comentario or '').lower(), (
                f'P1-FIX-3: Histórico de auditoria deveria mencionar [SYNC SEI] ou regressão. '
                f'Comentário atual: {ultimo.comentario!r}'
            )

    def test_sincronizacao_permite_avanco(self, db_session, app, mocker):
        """
        Avanço normal: itinerário em etapa 1, SEI retorna nota_reserva → deve ir para etapa 3.
        """
        with app.app_context():
            _criar_etapas_minimas(db_session)
            from app.constants import DiariasEtapaID

            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.SOLICITACAO_INICIAL))

            # SEI retorna nota_reserva → etapa 3 (ANALISE_SOLICITACAO) — avanço legítimo
            self._mock_consultar_docs(mocker, ['nota_reserva'])

            # Mock do commit interno para evitar persistência inter-testes
            from app.extensions import db as _db
            mocker.patch.object(_db.session, 'commit')

            from app.services.diarias_sei_integration import sincronizar_documentos_diaria
            resultado = sincronizar_documentos_diaria(it)

            # Deve ter avançado para etapa 3
            assert it.etapa_atual_id == int(DiariasEtapaID.ANALISE_SOLICITACAO), (
                f'Avanço legítimo: etapa deveria ser 3 (ANALISE_SOLICITACAO), '
                f'atual={it.etapa_atual_id}'
            )
            assert resultado['etapa_nova'] == int(DiariasEtapaID.ANALISE_SOLICITACAO)
            assert resultado['sucesso'] is True

    def test_sincronizacao_etapa_igual_nao_cria_historico_extra(self, db_session, app, mocker):
        """
        Quando SEI retorna documentos que resultam na MESMA etapa, não deve criar
        registro de regressão no histórico (não é uma tentativa de regressão).
        """
        with app.app_context():
            _criar_etapas_minimas(db_session)
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasHistoricoMovimentacao

            # Itinerário em etapa 3
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO))

            # SEI retorna nota_reserva → etapa calculada = 3 = mesma etapa atual
            self._mock_consultar_docs(mocker, ['nota_reserva'])

            # Rastreia objetos adicionados à sessão durante a sincronização
            adicionados = []
            from app.extensions import db as _db
            orig_add = _db.session.add

            def _track_add(obj):
                adicionados.append(obj)
                return orig_add(obj)

            mocker.patch.object(_db.session, 'commit')
            mocker.patch.object(_db.session, 'add', side_effect=_track_add)

            from app.services.diarias_sei_integration import sincronizar_documentos_diaria
            resultado = sincronizar_documentos_diaria(it)

            # Etapa mantida
            assert it.etapa_atual_id == int(DiariasEtapaID.ANALISE_SOLICITACAO), (
                f'Mesma etapa: não deve mudar, atual={it.etapa_atual_id}'
            )

            # NÃO deve ter criado registro de regressão (etapa é igual, não menor)
            historicos_adicionados = [
                obj for obj in adicionados
                if isinstance(obj, DiariasHistoricoMovimentacao)
                and obj.id_itinerario == it.id
            ]
            for hist in historicos_adicionados:
                assert 'regressão' not in (hist.comentario or '').lower(), (
                    f'Para mesma etapa, não deve haver registro de regressão. '
                    f'Comentário: {hist.comentario!r}'
                )

    def test_sincronizacao_permite_avanco_6_para_4(self, db_session, app, mocker):
        """
        TESTE CRÍTICO — Guard monotônico por ordem: avanço 6 → 4 não é regressão.

        O fluxo real: ANALISE_SOLICITACAO_2 (ID=6, ordem=3) → CONCESSAO_DIARIAS (ID=4, ordem=4).
        Numericamente 4 < 6, então o guard antigo bloquearia como "regressão" (BUG).

        Com o fix: o guard usa ordem cronológica. ordem(4)=4 > ordem(6)=3 → é avanço legítimo.

        Cenário: itinerário em etapa 6 (ANALISE_SOLICITACAO_2), SEI retorna ob/pd/nl
        (indicando que está na etapa 4). Deve avançar para 4, sem bloquear.

        ANTES do fix: 4 < 6 → regressão bloqueada → etapa mantida em 6 (BUG).
        APÓS o fix: ordem(4)=4 > ordem(6)=3 → etapa avança para 4 (CORRETO).
        """
        with app.app_context():
            _criar_etapas_minimas(db_session)
            from app.constants import DiariasEtapaID
            from app.models.diaria import DiariasHistoricoMovimentacao

            # Itinerário em etapa 6 (ANALISE_SOLICITACAO_2)
            it = _criar_itinerario(db_session, etapa_id=int(DiariasEtapaID.ANALISE_SOLICITACAO_2))

            # SEI retorna ob (e pd, nl) → etapa calculada deve ser CONCESSAO_DIARIAS (4)
            self._mock_consultar_docs(mocker, ['ob', 'pd', 'nl'])

            from app.extensions import db as _db
            mocker.patch.object(_db.session, 'commit')

            from app.services.diarias_sei_integration import sincronizar_documentos_diaria
            resultado = sincronizar_documentos_diaria(it)

            # Etapa DEVE ter avançado para 4 (CONCESSAO_DIARIAS)
            assert it.etapa_atual_id == int(DiariasEtapaID.CONCESSAO_DIARIAS), (
                f'GUARD-MONOTÔNICO NÃO CORRIGIDO: etapa deveria ser 4 (CONCESSAO_DIARIAS) '
                f'mas está em {it.etapa_atual_id}. '
                f'O avanço 6→4 é legítimo (ordem: 3→4) mas o guard antigo '
                f'trata como regressão numérica (4<6). '
                f'resultado["etapa_nova"]={resultado.get("etapa_nova")}'
            )

            assert resultado['etapa_nova'] == int(DiariasEtapaID.CONCESSAO_DIARIAS), (
                f'resultado["etapa_nova"] deve ser 4 (CONCESSAO_DIARIAS), '
                f'atual={resultado["etapa_nova"]}.'
            )

            # NÃO deve ter criado registro de regressão (é avanço, não regressão)
            pendentes = [
                obj for obj in db_session.new
                if isinstance(obj, DiariasHistoricoMovimentacao)
                and obj.id_itinerario == it.id
            ]
            for hist in pendentes:
                assert '[SYNC SEI] Regressão' not in (hist.comentario or ''), (
                    f'GUARD-MONOTÔNICO: avanço 6→4 foi tratado como regressão. '
                    f'Comentário: {hist.comentario!r}'
                )
