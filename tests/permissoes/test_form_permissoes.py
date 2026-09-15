"""Telas de administração: leitor único dos checkboxes, formulários de perfil e de
usuário (páginas, alta gestão, cargo diretor_dfin) e listagem de usuários."""
from sqlalchemy import event
from werkzeug.datastructures import MultiDict

from app.models.perfil import Perfil, todas_permissoes_liberaveis
from app.models.usuario import Usuario
from app.services.usuario_service import UsuarioService


def _ordenar(perms):
    return sorted(perms, key=lambda p: (p['modulo'], p['pagina'], p['acao']))


class TestLeitorDoFormulario:

    def test_aceita_pagina_e_modulo_sem_paginas(self):
        form = MultiDict({
            'perm:financeiro:fundo_rotativo:criar': '1',
            'perm:solicitacoes::visualizar': '1',
            'nome': 'qualquer',
        })
        assert _ordenar(UsuarioService.extrair_permissoes_form(form)) == [
            {'modulo': 'financeiro', 'pagina': 'fundo_rotativo', 'acao': 'criar'},
            {'modulo': 'solicitacoes', 'pagina': '', 'acao': 'visualizar'},
        ]

    def test_rejeita_chaves_desconhecidas(self):
        form = MultiDict({
            'perm:inexistente::criar': '1',
            'perm:financeiro:inexistente:criar': '1',
            'perm:financeiro:fundo_rotativo:voar': '1',
            'perm:usuarios::criar': '1',
            'perm:financeiro::criar': '1',          # módulo com páginas exige página
            'perm:solicitacoes:qualquer:criar': '1',  # módulo sem páginas não aceita página
            'perm_financeiro_criar': '1',           # formato antigo
            'perm:financeiro:fundo_rotativo': '1',
        })
        assert UsuarioService.extrair_permissoes_form(form) == []

    def test_rejeita_paginas_de_alta_gestao(self):
        form = MultiDict({
            'perm:financeiro:orcamento:visualizar': '1',
            'perm:financeiro:planejamento:excluir': '1',
        })
        assert UsuarioService.extrair_permissoes_form(form) == []

    def test_seed_acesso_total_usa_modulos_e_paginas(self):
        perms = todas_permissoes_liberaveis()
        assert ('financeiro', 'fundo_rotativo', 'excluir') in perms
        assert ('solicitacoes', '', 'aprovar') in perms
        assert not any(m == 'financeiro' and p in ('', 'orcamento', 'planejamento') for m, p, _ in perms)
        assert not any(m == 'fundo_rotativo' for m, _, _ in perms)


class TestPermissoesDict:

    def test_chaves_modulo_ou_modulo_pagina(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3201, [('financeiro', 'fundo_rotativo', 'criar'), ('solicitacoes', '', 'visualizar')])
        assert u.perfil.listar_permissoes_dict() == {
            'financeiro:fundo_rotativo': ['criar'],
            'solicitacoes': ['visualizar'],
        }


class TestTelaUsuario:

    def test_formulario_mostra_paginas_alta_gestao_e_diretor_dfin(self, client, usuario_com_permissoes, logar):
        admin = usuario_com_permissoes(3210, is_admin=True)
        alvo = usuario_com_permissoes(3211, [('financeiro', 'fundo_rotativo', 'criar')], cargo_gestao='diretor_dfin')
        logar(admin)

        resp = client.get(f'/usuarios/{alvo.id}/editar')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8')
        assert 'value="diretor_dfin" selected' in html
        assert 'name="is_alta_gestao"' in html
        assert 'name="perm:financeiro:fundo_rotativo:criar"' in html
        assert 'name="perm:solicitacoes::visualizar"' in html
        assert 'perm:financeiro:orcamento' not in html
        assert 'exclusivo da alta gestão' in html

    def test_salvar_grava_paginas_alta_gestao_e_mantem_diretor_dfin(self, client, db_session, usuario_com_permissoes, logar):
        admin = usuario_com_permissoes(3212, is_admin=True)
        alvo = usuario_com_permissoes(3213, cargo_gestao='diretor_dfin')
        logar(admin)

        resp = client.post(f'/usuarios/{alvo.id}/editar', data={
            'ativo': '1',
            'cargo_gestao': 'diretor_dfin',
            'is_alta_gestao': '1',
            'perm:financeiro:fundo_rotativo:criar': '1',
            'perm:financeiro:orcamento:excluir': '1',
        })

        assert resp.status_code == 302
        db_session.expire_all()
        alvo = db_session.get(Usuario, 3213)
        assert alvo.cargo_gestao == 'diretor_dfin'
        assert alvo.is_alta_gestao is True
        assert alvo.perfil.listar_permissoes_dict() == {'financeiro:fundo_rotativo': ['criar']}

    def test_salvar_sem_marcar_remove_alta_gestao(self, client, db_session, usuario_com_permissoes, logar):
        admin = usuario_com_permissoes(3214, is_admin=True)
        alvo = usuario_com_permissoes(3215, is_alta_gestao=True)
        logar(admin)

        client.post(f'/usuarios/{alvo.id}/editar', data={'ativo': '1'})

        db_session.expire_all()
        assert db_session.get(Usuario, 3215).is_alta_gestao is False


class TestTelaPerfil:

    def test_formulario_tem_sublinhas_das_paginas(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3220, is_admin=True))

        html = client.get('/usuarios/perfis/novo').data.decode('utf-8')

        for pagina in ('insercao_ne', 'diarias', 'fornecedores', 'execucoes', 'fundo_rotativo'):
            assert f'name="perm:financeiro:{pagina}:visualizar"' in html
        assert 'perm:financeiro:orcamento' not in html
        assert 'perm:fundo_rotativo' not in html
        assert 'exclusivo da alta gestão' in html

    def test_criar_perfil_grava_pagina(self, client, db_session, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3221, is_admin=True))

        resp = client.post('/usuarios/perfis/novo', data={
            'nome': 'Perfil Só Fundo Rotativo',
            'perm:financeiro:fundo_rotativo:visualizar': '1',
            'perm:cgfr::criar': '1',
        })

        assert resp.status_code == 302
        perfil = Perfil.query.filter_by(nome='Perfil Só Fundo Rotativo').one()
        assert perfil.listar_permissoes_dict() == {
            'financeiro:fundo_rotativo': ['visualizar'],
            'cgfr': ['criar'],
        }


class TestListagemUsuarios:

    def test_badge_financeiro_lista_as_paginas(self, client, usuario_com_permissoes, logar):
        logar(usuario_com_permissoes(3230, is_admin=True))
        usuario_com_permissoes(3231, [('financeiro', 'fundo_rotativo', 'criar'), ('financeiro', 'diarias', 'visualizar')])

        resp = client.get('/usuarios/')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8')
        assert 'Financeiro' in html
        assert 'Diárias: visualizar' in html
        assert 'Fundo Rotativo: criar' in html

    def test_resumo_carrega_permissoes_de_todos_em_uma_consulta(self, db_session, usuario_com_permissoes):
        u1 = usuario_com_permissoes(3232, [('financeiro', 'fundo_rotativo', 'criar')])
        u2 = usuario_com_permissoes(3233, [('solicitacoes', '', 'visualizar')])
        u3 = usuario_com_permissoes(3234)
        db_session.expire_all()
        usuarios = Usuario.query.filter(Usuario.id.in_([3232, 3233, 3234])).order_by(Usuario.id).all()

        queries = []
        engine = db_session.get_bind()
        listener = lambda *args, **kw: queries.append(args[2])  # noqa: E731
        event.listen(engine, 'before_cursor_execute', listener)
        try:
            resumo = UsuarioService.resumo_permissoes_por_usuario(usuarios)
        finally:
            event.remove(engine, 'before_cursor_execute', listener)

        assert len(queries) <= 1, queries
        assert resumo[u1.id] == {'financeiro': ['Fundo Rotativo: criar']}
        assert resumo[u2.id] == {'solicitacoes': ['visualizar']}
        assert resumo[u3.id] == {}
