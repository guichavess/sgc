"""Testes: registro do usuário que criou/atualizou + ordenação de pendentes."""
from app.models.identidade_visual import IdentidadeVisualLocal, IdentidadeVisualLog


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


class TestRegistroUsuario:

    def test_criar_local_registra_autor(self, client_full, db_session, app, municipio_teresina):
        with app.app_context():
            resp = client_full.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua Teste, 123',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            assert resp.status_code == 200
            lid = resp.get_json()['id']

            local = db_session.get(IdentidadeVisualLocal, lid)
            assert local.criado_por_id == 1002
            assert local.atualizado_por_id == 1002

    def test_criar_local_gera_log(self, client_full, db_session, app, municipio_teresina):
        with app.app_context():
            resp = client_full.post('/identidade-visual/api/criar-local', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua Teste, 123',
                'bairro': 'Centro',
                'cep': '64000-000',
            })
            lid = resp.get_json()['id']
            log = db_session.query(IdentidadeVisualLog).filter_by(
                acao='CRIAR', local_id=lid,
            ).first()
            assert log is not None
            assert log.usuario_id == 1002

    def test_editar_local_registra_atualizador(self, client_full, db_session, app, municipio_teresina):
        with app.app_context():
            local = _criar_local(db_session, municipio=municipio_teresina)
            lid = local.id

            resp = client_full.post(f'/identidade-visual/api/editar-local/{lid}', json={
                'municipio_id': municipio_teresina.id,
                'tipo_local': 'Sala da Cidadania',
                'endereco': 'Rua Nova, 456',
                'bairro': 'Centro',
                'cep': '64049-000',
            })
            assert resp.status_code == 200

            db_session.expire_all()
            local = db_session.get(IdentidadeVisualLocal, lid)
            assert local.atualizado_por_id == 1002


class TestOrdemPendentesPrimeiro:

    def test_dashboard_lista_pendentes_antes(self, logged_client, db_session, app, municipio_teresina):
        """Local PENDENTE de cidade alfabeticamente posterior deve vir antes
        de um REALIZADO de cidade anterior, pois pendentes têm prioridade."""
        from datetime import datetime
        from app.models.identidade_visual import IdentidadeVisualArquivo

        # Realizado: cidade 'Aaa' (alfabeticamente primeiro), mas concluído
        realizado = _criar_local(db_session, cidade='Aaa Cidade',
                                  data_acao=datetime(2026, 1, 1, 10, 0))
        db_session.add(IdentidadeVisualArquivo(
            local_id=realizado.id, nome_original='x.jpg',
            nome_servidor='x.jpg', tipo='jpg'))
        db_session.commit()

        # Pendente: cidade 'Zzz' (alfabeticamente último), sem ação
        pendente = _criar_local(db_session, cidade='Zzz Cidade')

        resp = logged_client.get('/identidade-visual/')
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        # Busca dentro das células da tabela (data-label="Cidade"), não no
        # dropdown de filtro que também lista as cidades.
        pos_pendente = html.find('data-label="Cidade">Zzz Cidade')
        pos_realizado = html.find('data-label="Cidade">Aaa Cidade')
        assert pos_pendente != -1 and pos_realizado != -1
        assert pos_pendente < pos_realizado
