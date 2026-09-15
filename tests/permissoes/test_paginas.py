"""Permissões por página (módulos com páginas, ex.: Financeiro) e alta gestão.

Regras:
- a hierarquia de ações (excluir ⇒ editar ⇒ criar ⇒ visualizar; aprovar ortogonal)
  vale dentro de cada página;
- sem página informada, um módulo com páginas libera se qualquer página liberar;
- páginas de regra "alta_gestao" (Orçamento, Planejamento) só abrem para admin ou
  `is_alta_gestao` — nunca pelo perfil, nunca pelo nome.
"""
import pytest
from flask_login import login_user
from sqlalchemy import event

from app.models.perfil import MODULOS, PAGINAS_MODULO, parse_permissao
from app.models.usuario import Usuario
from app.utils.permissions import requires_permission

PAGINAS_FINANCEIRO = [p.chave for p in PAGINAS_MODULO['financeiro']]


class TestHierarquiaPorPagina:

    def test_acao_maior_implica_menores_na_mesma_pagina(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3001, [('financeiro', 'fundo_rotativo', 'editar')])
        assert u.tem_permissao('financeiro', 'visualizar', 'fundo_rotativo')
        assert u.tem_permissao('financeiro', 'criar', 'fundo_rotativo')
        assert u.tem_permissao('financeiro', 'editar', 'fundo_rotativo')
        assert not u.tem_permissao('financeiro', 'excluir', 'fundo_rotativo')

    def test_permissao_de_uma_pagina_nao_vale_para_outra(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3002, [('financeiro', 'fundo_rotativo', 'excluir')])
        assert not u.tem_permissao('financeiro', 'visualizar', 'insercao_ne')
        assert not u.tem_permissao('financeiro', 'criar', 'diarias')

    def test_aprovar_concede_visualizar_mas_nao_escrita(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3003, [('financeiro', 'diarias', 'aprovar')])
        assert u.tem_permissao('financeiro', 'visualizar', 'diarias')
        assert u.tem_permissao('financeiro', 'aprovar', 'diarias')
        assert not u.tem_permissao('financeiro', 'criar', 'diarias')

    def test_sem_pagina_basta_uma_pagina_liberada(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3004, [('financeiro', 'fundo_rotativo', 'visualizar')])
        assert u.tem_permissao('financeiro')
        assert u.tem_permissao('financeiro', 'visualizar')
        assert not u.tem_permissao('financeiro', 'criar')

    def test_linha_antiga_de_modulo_inteiro_e_ignorada(self, usuario_com_permissoes):
        """Linhas `financeiro` com pagina='' ficam no banco até a Fase B e não valem mais."""
        u = usuario_com_permissoes(3005, [('financeiro', '', 'excluir')])
        assert not u.tem_permissao('financeiro')
        assert not u.tem_permissao('financeiro', 'criar', 'insercao_ne')

    def test_modulo_sem_paginas_continua_igual(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3006, [('identidade_visual', '', 'editar')])
        assert u.tem_permissao('identidade_visual')
        assert u.tem_permissao('identidade_visual', 'criar')
        assert not u.tem_permissao('identidade_visual', 'excluir')

    def test_fundo_rotativo_virou_pagina_de_financeiro(self):
        assert 'fundo_rotativo' not in dict(MODULOS)
        assert 'fundo_rotativo' in PAGINAS_FINANCEIRO


class TestAltaGestao:

    @pytest.mark.parametrize('pagina', ['orcamento', 'planejamento'])
    def test_admin_libera(self, usuario_com_permissoes, pagina):
        u = usuario_com_permissoes(3010, is_admin=True)
        assert u.tem_permissao('financeiro', pagina=pagina)

    @pytest.mark.parametrize('pagina', ['orcamento', 'planejamento'])
    def test_alta_gestao_libera_sem_perfil(self, usuario_com_permissoes, pagina):
        u = usuario_com_permissoes(3011, is_alta_gestao=True)
        assert u.tem_permissao('financeiro', pagina=pagina)
        assert u.tem_permissao('financeiro', 'visualizar', pagina)

    @pytest.mark.parametrize('pagina', ['orcamento', 'planejamento'])
    def test_homonimo_sem_marcacao_e_bloqueado(self, usuario_com_permissoes, pagina):
        todas = [('financeiro', p, 'excluir') for p in PAGINAS_FINANCEIRO]
        u = usuario_com_permissoes(3012, todas, nome='JOAO PEDRO ALEXANDRE')
        assert not u.tem_permissao('financeiro', pagina=pagina)

    def test_alta_gestao_nao_libera_paginas_do_perfil(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3013, is_alta_gestao=True)
        assert not u.tem_permissao('financeiro', 'visualizar', 'insercao_ne')


class TestPaginasAcessiveis:

    def test_segue_a_ordem_do_cadastro(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3020, [
            ('financeiro', 'fundo_rotativo', 'visualizar'),
            ('financeiro', 'insercao_ne', 'criar'),
        ])
        assert [p.chave for p in u.paginas_acessiveis('financeiro')] == ['insercao_ne', 'fundo_rotativo']

    def test_alta_gestao_entra_primeiro_em_orcamento(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3021, [('financeiro', 'fundo_rotativo', 'visualizar')], is_alta_gestao=True)
        assert [p.chave for p in u.paginas_acessiveis('financeiro')] == ['orcamento', 'fundo_rotativo', 'planejamento']

    def test_admin_ve_todas(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3022, is_admin=True)
        assert [p.chave for p in u.paginas_acessiveis('financeiro')] == PAGINAS_FINANCEIRO

    def test_sem_permissao_lista_vazia(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3023)
        assert u.paginas_acessiveis('financeiro') == []

    def test_endpoints_de_entrada_existem(self, app):
        from flask import url_for
        with app.test_request_context():
            for pagina in PAGINAS_MODULO['financeiro']:
                assert url_for(pagina.endpoint).startswith('/financeiro/'), pagina.chave


class TestParsePermissao:

    @pytest.mark.parametrize('texto, esperado', [
        ('financeiro', ('financeiro', None, None)),
        ('solicitacoes.criar', ('solicitacoes', None, 'criar')),
        ('financeiro.orcamento', ('financeiro', 'orcamento', None)),
        ('financeiro.fundo_rotativo.criar', ('financeiro', 'fundo_rotativo', 'criar')),
    ])
    def test_formatos(self, texto, esperado):
        assert parse_permissao(texto) == esperado

    def test_pode_usa_a_mesma_regra(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3030, [('financeiro', 'fundo_rotativo', 'criar'), ('solicitacoes', '', 'visualizar')])
        assert u.pode('financeiro.fundo_rotativo.criar')
        assert not u.pode('financeiro.fundo_rotativo.editar')
        assert not u.pode('financeiro.insercao_ne.visualizar')
        assert u.pode('solicitacoes.visualizar')
        assert not u.pode('financeiro.orcamento')


class TestConsultas:

    def test_varias_checagens_no_mesmo_request_fazem_no_maximo_uma_query(self, db_session, usuario_com_permissoes):
        usuario_com_permissoes(3040, [
            ('financeiro', 'fundo_rotativo', 'criar'),
            ('financeiro', 'diarias', 'editar'),
            ('solicitacoes', '', 'visualizar'),
        ])
        db_session.expire_all()
        u = db_session.get(Usuario, 3040)
        assert u.perfil is not None  # carrega o perfil fora da contagem

        queries = []
        engine = db_session.get_bind()
        listener = lambda *args, **kw: queries.append(args[2])  # noqa: E731
        event.listen(engine, 'before_cursor_execute', listener)
        try:
            for _ in range(5):
                u.tem_permissao('financeiro', 'criar', 'fundo_rotativo')
                u.tem_permissao('financeiro', 'criar', 'diarias')
                u.tem_permissao('solicitacoes', 'visualizar')
                u.pode('financeiro.insercao_ne.criar')
                u.paginas_acessiveis('financeiro')
        finally:
            event.remove(engine, 'before_cursor_execute', listener)

        assert len(queries) <= 1, queries


class TestRequiresPermission:

    def _chamar(self, app, usuario, permissao):
        with app.test_request_context():
            login_user(usuario)
            return requires_permission(permissao)(lambda: 'ok')()

    def test_tres_partes(self, app, usuario_com_permissoes):
        u = usuario_com_permissoes(3050, [('financeiro', 'fundo_rotativo', 'criar')])
        assert self._chamar(app, u, 'financeiro.fundo_rotativo.criar') == 'ok'
        negado = self._chamar(app, u, 'financeiro.insercao_ne.visualizar')
        assert negado.status_code == 302 and negado.location.endswith('/hub')

    def test_duas_partes_modulo_acao(self, app, usuario_com_permissoes):
        u = usuario_com_permissoes(3051, [('identidade_visual', '', 'criar')])
        assert self._chamar(app, u, 'identidade_visual.criar') == 'ok'
        assert self._chamar(app, u, 'identidade_visual.excluir').status_code == 302

    def test_duas_partes_pagina_de_alta_gestao(self, app, usuario_com_permissoes):
        alta = usuario_com_permissoes(3052, is_alta_gestao=True)
        comum = usuario_com_permissoes(3053, [('financeiro', 'insercao_ne', 'excluir')])
        assert self._chamar(app, alta, 'financeiro.orcamento') == 'ok'
        assert self._chamar(app, comum, 'financeiro.orcamento').status_code == 302
