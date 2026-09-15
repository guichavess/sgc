"""Testes do catálogo do Hub (app/services/hub_service.py).

Regra: o hub mostra só os módulos que o usuário pode acessar — mesmas regras
de `Usuario.tem_permissao` — e, para cada um, o rótulo "Seu acesso".
"""
import pytest
from sqlalchemy import event

from app.services.hub_service import montar_hub, rotulo_acesso

MODULOS_NEGOCIO = ['solicitacoes', 'financeiro', 'prestacoes_contratos', 'diarias', 'identidade_visual', 'cgfr']


def _ids(itens):
    return [m['id'] for m in itens]


class TestMontarHub:

    def test_admin_ve_todos_os_modulos_e_a_administracao(self, app, novo_usuario):
        u = novo_usuario(2001, is_admin=True)
        with app.test_request_context():
            hub = montar_hub(u)

        assert _ids(hub['modulos']) == MODULOS_NEGOCIO
        assert _ids(hub['admin']) == ['usuarios', 'dashboards']
        assert hub['total'] == 8
        acessos = {m['id']: m['acesso'] for m in hub['modulos'] + hub['admin']}
        assert acessos.pop('usuarios') == 'Exclusivo para administradores'
        assert set(acessos.values()) == {'Acesso total'}

    def test_perfil_ve_apenas_modulos_liberados_com_rotulo_de_acesso(self, app, novo_usuario):
        u = novo_usuario(2002, permissoes={'solicitacoes': ['visualizar'], 'diarias': ['criar', 'aprovar']})
        with app.test_request_context():
            hub = montar_hub(u)

        assert _ids(hub['modulos']) == ['solicitacoes', 'diarias']
        assert hub['admin'] == []
        assert hub['total'] == 2
        acessos = {m['id']: m['acesso'] for m in hub['modulos']}
        assert acessos == {'solicitacoes': 'Somente visualizar', 'diarias': 'Visualizar, criar e aprovar'}

    def test_dashboards_liberado_por_perfil_aparece_na_administracao(self, app, novo_usuario):
        u = novo_usuario(2003, permissoes={'dashboards': ['visualizar']})
        with app.test_request_context():
            hub = montar_hub(u)

        assert hub['modulos'] == []
        assert _ids(hub['admin']) == ['dashboards']

    def test_usuario_sem_perfil_nao_ve_nada(self, app, novo_usuario):
        u = novo_usuario(2004)
        with app.test_request_context():
            hub = montar_hub(u)

        assert hub == {'modulos': [], 'admin': [], 'total': 0}

    def test_permissao_de_modulo_sem_card_nao_conta(self, app, novo_usuario):
        u = novo_usuario(2005, permissoes={'fundo_rotativo': ['visualizar']})
        with app.test_request_context():
            assert montar_hub(u)['total'] == 0

    def test_cada_card_tem_os_detalhes_da_expansao(self, app, novo_usuario):
        u = novo_usuario(2006, is_admin=True)
        with app.test_request_context():
            hub = montar_hub(u)

        for m in hub['modulos'] + hub['admin']:
            assert m['url'].startswith('/'), m['id']
            assert m['nome'] and m['descricao'] and m['resumo'], m['id']
            assert len(m['recursos']) == 3, m['id']
            assert m['cor'].startswith('#') and m['icone'].startswith('bi-'), m['id']
        fluxos = {m['id']: m['fluxo'] for m in hub['modulos']}
        assert fluxos['solicitacoes'][0] == 'Criada' and len(fluxos['solicitacoes']) == 6
        assert len(fluxos['diarias']) == 6

    def test_permissoes_carregadas_sem_query_por_modulo(self, app, db_session, novo_usuario):
        """O hub antigo fazia uma query por card (tem_permissao em cada módulo)."""
        u = novo_usuario(2007, permissoes={'solicitacoes': ['visualizar'], 'financeiro': ['editar'], 'cgfr': ['visualizar']})
        db_session.expire_all()
        u = db_session.get(type(u), 2007)

        queries = []
        engine = db_session.get_bind()
        listener = lambda *args, **kw: queries.append(args[2])  # noqa: E731
        event.listen(engine, 'before_cursor_execute', listener)
        try:
            with app.test_request_context():
                hub = montar_hub(u)
        finally:
            event.remove(engine, 'before_cursor_execute', listener)

        assert hub['total'] == 3
        assert len(queries) <= 2, queries


class TestRotuloAcesso:

    @pytest.mark.parametrize('acoes, esperado', [
        ({'visualizar'}, 'Somente visualizar'),
        ({'criar'}, 'Visualizar e criar'),
        ({'visualizar', 'editar'}, 'Visualizar, criar e editar'),
        ({'excluir'}, 'Visualizar, criar, editar e excluir'),
        ({'aprovar'}, 'Visualizar e aprovar'),
        ({'excluir', 'aprovar'}, 'Acesso total'),
    ])
    def test_rotulo_segue_a_hierarquia_de_acoes(self, acoes, esperado):
        assert rotulo_acesso(acoes) == esperado
