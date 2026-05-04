"""
BUG-07 — Condição de corrida em set_doc() / get_or_create
==========================================================
Arquivo: app/models/diaria.py, método set_doc()

Problema:
    Duplo-clique em "Gerar Documento" → dois requests simultâneos chamam set_doc().
    Ambos fazem get_doc() → None → ambos criam o documento → IntegrityError
    (ou, pior, documento duplicado silencioso).

Fix esperado:
    Usar lógica get_or_create com proteção contra race condition.
    A função deve ser idempotente: chamadas múltiplas com os mesmos parâmetros
    devem resultar em exatamente UM documento.

Como rodar:
    pytest tests/diarias/test_bug07_set_doc_race.py -v
"""
import pytest
from decimal import Decimal
from datetime import date


class TestSetDocIdempotencia:
    """
    Verifica que set_doc() é idempotente — múltiplas chamadas não criam duplicatas.
    """

    def _criar_itinerario(self, db_session, app):
        """Helper para criar itinerário de teste."""
        from app.models.diaria import DiariasItinerario
        it = DiariasItinerario(
            usuario_gerador=1,
            tipo_solicitacao_id=1,
            tipo_itinerario=1,
            status_id=1,
            data_solicitacao=date.today(),
            data_viagem=date(2026, 5, 1),
            data_retorno=date(2026, 5, 3),
            qtd_diarias_solicitadas=Decimal('2.5'),
        )
        db_session.add(it)
        db_session.flush()
        return it

    def test_set_doc_cria_documento(self, db_session, app):
        """Caso base: set_doc() deve criar o documento corretamente."""
        with app.app_context():
            it = self._criar_itinerario(db_session, app)

            it.set_doc('nr_global', sei_id='DOC001', sei_formatado='0000001')
            db_session.flush()

            doc = it.get_doc('nr_global')
            assert doc is not None, "set_doc() deveria ter criado o documento"
            assert doc.sei_id == 'DOC001'

    def test_set_doc_idempotente_mesmos_dados(self, db_session, app):
        """
        TESTE CRÍTICO — Detecta BUG-07.

        Chamar set_doc() duas vezes com os mesmos parâmetros não deve criar
        dois documentos — deve atualizar o existente ou ignorar a segunda chamada.

        ANTES da correção: pode criar dois documentos ou lançar IntegrityError.
        APÓS a correção: exatamente 1 documento existe com os dados mais recentes.
        """
        with app.app_context():
            from app.models.diaria import DiariasDocumentoSei as DiariasDocumento

            it = self._criar_itinerario(db_session, app)

            # Primeira chamada
            it.set_doc('nr_global', sei_id='DOC001', sei_formatado='0000001')
            db_session.flush()

            # Segunda chamada com mesmos parâmetros (simula duplo-clique)
            try:
                it.set_doc('nr_global', sei_id='DOC001', sei_formatado='0000001')
                db_session.flush()
            except Exception as e:
                pytest.fail(
                    f"BUG-07 DETECTADO: set_doc() lançou exceção na segunda chamada: {e}. "
                    f"Fix: implementar get_or_create com ON CONFLICT IGNORE ou verificação explícita."
                )

            # Conta documentos do mesmo tipo para este itinerário
            docs = DiariasDocumento.query.filter_by(
                itinerario_id=it.id,
                tipo_documento='nr_global',
            ).all()

            assert len(docs) == 1, (
                f"BUG-07 DETECTADO: {len(docs)} documentos criados para o mesmo tipo. "
                f"set_doc() deve ser idempotente — apenas 1 documento por tipo permitido. "
                f"Fix: usar 'INSERT OR IGNORE' ou 'get or create' pattern."
            )

    def test_set_doc_atualiza_dados_existente(self, db_session, app):
        """
        Quando o documento já existe, set_doc() com novos dados deve ATUALIZAR
        o registro existente, não criar um novo.
        """
        with app.app_context():
            from app.models.diaria import DiariasDocumentoSei as DiariasDocumento

            it = self._criar_itinerario(db_session, app)

            # Cria documento original
            it.set_doc('nr_global', sei_id='DOC001', sei_formatado='0000001')
            db_session.flush()

            # Atualiza com novos dados SEI
            it.set_doc('nr_global', sei_id='DOC002', sei_formatado='0000002')
            db_session.flush()

            docs = DiariasDocumento.query.filter_by(
                itinerario_id=it.id,
                tipo_documento='nr_global',
            ).all()

            assert len(docs) == 1, (
                f"set_doc() criou {len(docs)} documentos ao invés de atualizar o existente."
            )

            doc = docs[0]
            assert doc.sei_id == 'DOC002', (
                f"set_doc() não atualizou o sei_id: esperado 'DOC002', obtido '{doc.sei_id}'"
            )

    def test_get_doc_retorna_none_para_tipo_inexistente(self, db_session, app):
        """get_doc() deve retornar None para tipo de documento não existente."""
        with app.app_context():
            it = self._criar_itinerario(db_session, app)

            doc = it.get_doc('tipo_que_nao_existe')
            assert doc is None, (
                f"get_doc() deveria retornar None para tipo inexistente, "
                f"mas retornou: {doc}"
            )
