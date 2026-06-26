"""Testes para exclusão de locais (Identidade Visual).

Regra de negócio: apenas usuários com acesso FULL ao módulo
(permissão `identidade_visual.excluir` — ou is_admin) podem excluir.
Toda exclusão é registrada em `identidade_visual_log`.
"""
from app.models.identidade_visual import (
    IdentidadeVisualLocal, IdentidadeVisualArquivo, IdentidadeVisualLog,
)


def _criar_local(db_session, municipio=None, **kwargs):
    defaults = {
        'cidade': 'Teresina',
        'tipo_local': 'Sala da Cidadania',
        'endereco': 'Rua Padrão, 1',
        'bairro': 'Centro',
        'cep': '64000-000',
    }
    if municipio:
        defaults['municipio_id'] = municipio.id
        defaults['cidade'] = municipio.nome
    defaults.update(kwargs)
    local = IdentidadeVisualLocal(**defaults)
    db_session.add(local)
    db_session.commit()
    return local


class TestExcluirLocal:

    def test_admin_pode_excluir(self, logged_client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)
            lid = local.id

            resp = logged_client.post(f'/identidade-visual/api/excluir-local/{lid}')
            assert resp.status_code == 200
            assert resp.get_json()['ok'] is True

            db_session.expire_all()
            assert db_session.get(IdentidadeVisualLocal, lid) is None

    def test_usuario_full_pode_excluir(self, client_full, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)
            lid = local.id

            resp = client_full.post(f'/identidade-visual/api/excluir-local/{lid}')
            assert resp.status_code == 200
            assert resp.get_json()['ok'] is True
            db_session.expire_all()
            assert db_session.get(IdentidadeVisualLocal, lid) is None

    def test_usuario_editor_nao_pode_excluir(self, client_editor, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)
            lid = local.id

            resp = client_editor.post(f'/identidade-visual/api/excluir-local/{lid}')
            # sem permissão excluir → redirect para hub (302) ou 403
            assert resp.status_code in (302, 403)
            db_session.expire_all()
            assert db_session.get(IdentidadeVisualLocal, lid) is not None

    def test_excluir_sem_login(self, client, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)
            resp = client.post(f'/identidade-visual/api/excluir-local/{local.id}')
            assert resp.status_code in (302, 401, 403)

    def test_excluir_inexistente(self, logged_client, app):
        with app.app_context():
            resp = logged_client.post('/identidade-visual/api/excluir-local/99999')
            assert resp.status_code == 404

    def test_exclusao_registra_log(self, client_full, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)
            lid = local.id

            client_full.post(f'/identidade-visual/api/excluir-local/{lid}')

            db_session.expire_all()
            log = db_session.query(IdentidadeVisualLog).filter_by(
                acao='EXCLUIR', local_id=lid,
            ).first()
            assert log is not None
            assert log.usuario_id == 1002
            assert log.usuario_nome == 'USER IV 1002'

    def test_exclusao_remove_arquivos_filhos(self, client_full, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)
            arq = IdentidadeVisualArquivo(
                local_id=local.id, nome_original='foto.jpg',
                nome_servidor='abc_foto.jpg', tipo='jpg',
            )
            db_session.add(arq)
            db_session.commit()
            arq_id = arq.id

            client_full.post(f'/identidade-visual/api/excluir-local/{local.id}')

            db_session.expire_all()
            assert db_session.get(IdentidadeVisualArquivo, arq_id) is None
