"""Testes do relatório fotográfico da Identidade Visual."""
from datetime import datetime

from app.models.identidade_visual import (
    IdentidadeVisualArquivo,
    IdentidadeVisualLocal,
    TIPO_LOCAL_VEICULO,
)


def _criar_realizado(db_session, *, veiculo=False):
    if veiculo:
        local = IdentidadeVisualLocal(
            tipo_local=TIPO_LOCAL_VEICULO,
            placa='ABC1D23',
            tipo_veiculo='AUTOMOVEL',
            marca='FIAT',
            modelo='MOBI',
            cor='BRANCA',
            data_acao=datetime(2026, 7, 20, 9, 30),
        )
    else:
        local = IdentidadeVisualLocal(
            cidade='Teresina',
            tipo_local='Sala da Cidadania',
            endereco='Rua das Flores, 123',
            bairro='Centro',
            cep='64000-000',
            data_acao=datetime(2026, 7, 20, 9, 30),
        )
    db_session.add(local)
    db_session.flush()
    db_session.add_all([
        IdentidadeVisualArquivo(
            local_id=local.id, nome_original='fachada.jpg',
            nome_servidor='fachada.jpg', tipo='jpg',
        ),
        IdentidadeVisualArquivo(
            local_id=local.id, nome_original='comprovante.pdf',
            nome_servidor='comprovante.pdf', tipo='pdf',
        ),
    ])
    db_session.commit()
    return local


class TestRelatorioFotografico:

    def test_admin_visualiza_apenas_registros_realizados_com_fotos(
        self, logged_client, db_session, app
    ):
        with app.app_context():
            _criar_realizado(db_session)
            _criar_realizado(db_session, veiculo=True)
            db_session.add(IdentidadeVisualLocal(
                cidade='Picos', tipo_local='Sala da Cidadania',
                endereco='Rua Pendente, 1', bairro='Centro', cep='64600-000',
            ))
            db_session.commit()

        response = logged_client.get('/identidade-visual/relatorio-fotografico')
        html = response.get_data(as_text=True)

        assert response.status_code == 200
        assert 'Relatório Fotográfico' in html
        assert 'Rua das Flores, 123' in html
        assert 'ABC1D23' in html
        assert 'fachada.jpg' in html
        assert 'comprovante.pdf' not in html
        assert 'Rua Pendente, 1' not in html

    def test_usuario_nao_admin_nao_acessa_relatorio(self, client_editor):
        response = client_editor.get(
            '/identidade-visual/relatorio-fotografico', follow_redirects=False
        )

        assert response.status_code == 302
        assert response.headers['Location'].endswith('/hub')

    def test_admin_visualiza_botao_do_relatorio(self, logged_client):
        admin_html = logged_client.get('/identidade-visual/').get_data(as_text=True)
        assert 'Gerar Relatório Fotográfico' in admin_html

    def test_usuario_nao_admin_nao_visualiza_botao(self, client_editor):
        usuario_html = client_editor.get('/identidade-visual/').get_data(as_text=True)
        assert 'Gerar Relatório Fotográfico' not in usuario_html


class TestRelatorioFotograficoPDF:
    """O PDF é montado no servidor lendo as fotos direto do disco (UPLOAD_FOLDER)."""

    def test_admin_baixa_pdf_com_fotos_do_servidor(
        self, logged_client, db_session, app, tmp_path, monkeypatch
    ):
        from PIL import Image
        from app.identidade_visual.routes import dashboard as rotas

        monkeypatch.setattr(rotas, 'UPLOAD_FOLDER', str(tmp_path))
        Image.new('RGB', (800, 600), 'white').save(tmp_path / 'fachada.jpg')

        with app.app_context():
            _criar_realizado(db_session)

        response = logged_client.get('/identidade-visual/relatorio-fotografico/pdf')

        assert response.status_code == 200
        assert response.mimetype == 'application/pdf'
        assert response.data[:4] == b'%PDF'
        assert 'Relatorio_Fotografico_' in response.headers['Content-Disposition']

    def test_pdf_sem_registros_nao_quebra(self, logged_client):
        response = logged_client.get('/identidade-visual/relatorio-fotografico/pdf')

        assert response.status_code == 200
        assert response.data[:4] == b'%PDF'

    def test_usuario_nao_admin_nao_baixa_pdf(self, client_editor):
        response = client_editor.get(
            '/identidade-visual/relatorio-fotografico/pdf', follow_redirects=False
        )

        assert response.status_code == 302
        assert response.headers['Location'].endswith('/hub')


class TestMenuExportar:
    """O botão Exportar do dashboard vira um menu com Excel e PDF."""

    def test_admin_escolhe_entre_excel_e_pdf(self, logged_client):
        html = logged_client.get('/identidade-visual/').get_data(as_text=True)

        assert 'id="menuExportar"' in html
        assert 'data-bs-toggle="dropdown"' in html
        assert '/identidade-visual/exportar-excel' in html
        assert '/identidade-visual/relatorio-fotografico/pdf' in html

    def test_nao_admin_ve_apenas_excel_no_menu(self, client_editor):
        html = client_editor.get('/identidade-visual/').get_data(as_text=True)

        assert 'id="menuExportar"' in html
        assert '/identidade-visual/exportar-excel' in html
        assert '/identidade-visual/relatorio-fotografico/pdf' not in html
