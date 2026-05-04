"""
BUG-04 — administracao_detalhe retorna 500 em vez de 404 para ID inexistente
BUG-06 — parâmetro `page` sem validação (aceita valores negativos/zero)
=============================================================================
Arquivo: app/diarias/routes/admin.py

Como rodar:
    pytest tests/diarias/test_bug04_bug06_rotas.py -v
"""
import pytest


class TestAdministracaoDetalhe404:
    """BUG-04: Rota de detalhe deve retornar 404 (não 500) para ID inexistente."""

    def test_detalhe_id_inexistente_retorna_404(self, client, app):
        """
        ANTES da correção: retorna 500 (AttributeError em template tentando acessar None.campo)
        APÓS a correção: retorna 404 com abort(404) explícito.
        """
        with app.app_context():
            # Login de admin necessário para acessar a rota
            with client.session_transaction() as sess:
                # Flask-Login usa _user_id na sessão
                sess['_user_id'] = '999999'

            response = client.get('/diarias/administracao/999999999')

            assert response.status_code in (404, 302), (
                f"BUG-04 DETECTADO: Status {response.status_code} retornado para ID inexistente. "
                f"Esperado 404 (ou 302 redirect para login). "
                f"Fix: adicionar 'if not dados: abort(404)' em administracao_detalhe()"
            )

    def test_detalhe_id_zero_nao_causa_500(self, client, app):
        """ID 0 não deve causar 500 — deve retornar 404 ou redirect."""
        with app.app_context():
            response = client.get('/diarias/administracao/0')
            assert response.status_code != 500, (
                f"Resposta 500 para ID=0. Deve retornar 404 ou redirect para login."
            )


class TestAdministracaoPaginacao:
    """BUG-06: Parâmetro `page` deve ser validado contra valores inválidos."""

    def test_page_zero_nao_causa_erro(self, client, app):
        """
        page=0 deve ser tratado como page=1 (ou redirecionar).
        ANTES da correção: SQLAlchemy lança erro com página 0.
        """
        with app.app_context():
            response = client.get('/diarias/administracao?page=0')
            assert response.status_code != 500, (
                f"BUG-06: page=0 causou status {response.status_code}. "
                f"Fix: page = max(1, int(page or 1))"
            )

    def test_page_negativa_nao_causa_erro(self, client, app):
        """page=-5 deve ser tratado como page=1."""
        with app.app_context():
            response = client.get('/diarias/administracao?page=-5')
            assert response.status_code != 500, (
                f"BUG-06: page=-5 causou status {response.status_code}. "
                f"Fix: page = max(1, int(page or 1))"
            )

    def test_page_string_nao_causa_500(self, client, app):
        """page=abc deve ser tratado graciosamente sem 500."""
        with app.app_context():
            response = client.get('/diarias/administracao?page=abc')
            assert response.status_code != 500, (
                f"BUG-06: page=abc causou status {response.status_code}. "
                f"Fix: usar try/except ao converter page para int."
            )
