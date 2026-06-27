"""Testes: limite de tamanho por arquivo no upload + robustez do nome do arquivo."""
import io
from app.identidade_visual.routes import dashboard as iv_dashboard
from app.models.identidade_visual import IdentidadeVisualLocal, IdentidadeVisualArquivo


def _criar_local(db_session):
    local = IdentidadeVisualLocal(
        cidade='Teresina', tipo_local='Sala da Cidadania',
        endereco='Rua X, 1', bairro='Centro', cep='64000-000',
    )
    db_session.add(local)
    db_session.commit()
    return local


class TestLimiteTamanho:

    def test_rejeita_arquivo_acima_do_limite(self, logged_client, db_session, app, monkeypatch, tmp_path):
        monkeypatch.setattr(iv_dashboard, 'MAX_FILE_SIZE', 10)
        monkeypatch.setattr(iv_dashboard, 'UPLOAD_FOLDER', str(tmp_path))
        with app.app_context():
            local = _criar_local(db_session)
            resp = logged_client.post(f'/identidade-visual/api/salvar-acao/{local.id}', data={
                'data_acao': '2026-01-01T10:00',
                'arquivos': (io.BytesIO(b'x' * 50), 'foto.jpg'),
            }, content_type='multipart/form-data')
            assert resp.status_code == 400
            assert 'limite' in resp.get_json()['erro'].lower()

    def test_nao_salva_nada_quando_um_arquivo_estoura(self, logged_client, db_session, app, monkeypatch, tmp_path):
        """Se o 2º arquivo excede o limite, o 1º não pode ficar órfão no disco."""
        monkeypatch.setattr(iv_dashboard, 'MAX_FILE_SIZE', 100)
        monkeypatch.setattr(iv_dashboard, 'UPLOAD_FOLDER', str(tmp_path))
        with app.app_context():
            local = _criar_local(db_session)
            resp = logged_client.post(f'/identidade-visual/api/salvar-acao/{local.id}', data={
                'data_acao': '2026-01-01T10:00',
                'arquivos': [
                    (io.BytesIO(b'x' * 10), 'ok.jpg'),
                    (io.BytesIO(b'x' * 500), 'grande.jpg'),
                ],
            }, content_type='multipart/form-data')
            assert resp.status_code == 400
            # nenhum arquivo físico gravado
            assert list(tmp_path.iterdir()) == []
            # nenhum registro de arquivo no banco
            assert db_session.query(IdentidadeVisualArquivo).filter_by(local_id=local.id).count() == 0

    def test_aceita_arquivo_dentro_do_limite(self, logged_client, db_session, app, monkeypatch, tmp_path):
        monkeypatch.setattr(iv_dashboard, 'MAX_FILE_SIZE', 25 * 1024 * 1024)
        monkeypatch.setattr(iv_dashboard, 'UPLOAD_FOLDER', str(tmp_path))
        with app.app_context():
            local = _criar_local(db_session)
            resp = logged_client.post(f'/identidade-visual/api/salvar-acao/{local.id}', data={
                'data_acao': '2026-01-01T10:00',
                'arquivos': (io.BytesIO(b'conteudo-pequeno'), 'foto.jpg'),
            }, content_type='multipart/form-data')
            assert resp.status_code == 200
            assert resp.get_json()['ok'] is True
            assert db_session.query(IdentidadeVisualArquivo).filter_by(local_id=local.id).count() == 1


class TestNomeArquivoRobusto:

    def test_nome_sem_ascii_nao_quebra(self, logged_client, db_session, app, monkeypatch, tmp_path):
        """secure_filename pode descartar o nome inteiro (ex.: emoji/acentos);
        o upload não deve estourar 500 ao derivar a extensão."""
        monkeypatch.setattr(iv_dashboard, 'UPLOAD_FOLDER', str(tmp_path))
        with app.app_context():
            local = _criar_local(db_session)
            resp = logged_client.post(f'/identidade-visual/api/salvar-acao/{local.id}', data={
                'data_acao': '2026-01-01T10:00',
                'arquivos': (io.BytesIO(b'abc'), '📷.jpg'),
            }, content_type='multipart/form-data')
            assert resp.status_code == 200
            arq = db_session.query(IdentidadeVisualArquivo).filter_by(local_id=local.id).first()
            assert arq is not None
            assert arq.tipo == 'jpg'
