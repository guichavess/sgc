"""Testes: filtros com quantitativo por item + multi-seleção (estilo Pagamentos)."""
import re
from datetime import datetime
from app.models.identidade_visual import IdentidadeVisualLocal, IdentidadeVisualArquivo


def _criar_local(db_session, cidade='Teresina', tipo_local='Sala da Cidadania',
                 bairro='Centro', com_acao=False):
    local = IdentidadeVisualLocal(
        cidade=cidade, tipo_local=tipo_local,
        endereco='Rua X, 1', bairro=bairro, cep='64000-000',
        data_acao=datetime(2026, 1, 1, 10, 0) if com_acao else None,
    )
    db_session.add(local)
    db_session.commit()
    if com_acao:
        db_session.add(IdentidadeVisualArquivo(
            local_id=local.id, nome_original='x.jpg',
            nome_servidor='x.jpg', tipo='jpg'))
        db_session.commit()
    return local


class TestContagemFiltros:

    def test_contagem_por_cidade(self, logged_client, db_session, app):
        with app.app_context():
            _criar_local(db_session, cidade='Teresina')
            _criar_local(db_session, cidade='Teresina')
            _criar_local(db_session, cidade='Picos')

            resp = logged_client.get('/identidade-visual/')
            html = resp.get_data(as_text=True)
            assert re.search(r'data-filtro-valor="Teresina"\s+data-count="2"', html)
            assert re.search(r'data-filtro-valor="Picos"\s+data-count="1"', html)

    def test_contagem_por_status(self, logged_client, db_session, app):
        with app.app_context():
            _criar_local(db_session, cidade='Teresina', com_acao=True)   # REALIZADO
            _criar_local(db_session, cidade='Picos')                      # PENDENTE
            _criar_local(db_session, cidade='Oeiras')                     # PENDENTE

            resp = logged_client.get('/identidade-visual/')
            html = resp.get_data(as_text=True)
            assert re.search(r'data-filtro-valor="PENDENTE"\s+data-count="2"', html)
            assert re.search(r'data-filtro-valor="REALIZADO"\s+data-count="1"', html)

    def test_contagem_por_tipo_local(self, logged_client, db_session, app):
        with app.app_context():
            _criar_local(db_session, tipo_local='Sala da Cidadania')
            _criar_local(db_session, tipo_local='Espaço da Cidadania')
            _criar_local(db_session, tipo_local='Espaço da Cidadania')

            resp = logged_client.get('/identidade-visual/')
            html = resp.get_data(as_text=True)
            assert re.search(r'data-filtro-valor="Espaço da Cidadania"\s+data-count="2"', html)
            assert re.search(r'data-filtro-valor="Sala da Cidadania"\s+data-count="1"', html)


class TestMultiSelecao:

    def test_filtra_multiplas_cidades(self, logged_client, db_session, app):
        with app.app_context():
            _criar_local(db_session, cidade='Teresina')
            _criar_local(db_session, cidade='Picos')
            _criar_local(db_session, cidade='Oeiras')

            resp = logged_client.get('/identidade-visual/?cidade=Teresina&cidade=Picos')
            html = resp.get_data(as_text=True)
            assert 'data-label="Cidade">Teresina' in html
            assert 'data-label="Cidade">Picos' in html
            assert 'data-label="Cidade">Oeiras' not in html

    def test_filtra_status_pendente(self, logged_client, db_session, app):
        with app.app_context():
            _criar_local(db_session, cidade='Teresina', com_acao=True)   # REALIZADO
            _criar_local(db_session, cidade='Picos')                      # PENDENTE

            resp = logged_client.get('/identidade-visual/?status=PENDENTE')
            html = resp.get_data(as_text=True)
            assert 'data-label="Cidade">Picos' in html
            assert 'data-label="Cidade">Teresina' not in html
