"""Destinatários de notificação seguem a hierarquia de ações e as páginas.

Antes a consulta exigia `acao='visualizar'` exato no módulo: quem tinha só
`criar` não recebia, e as linhas de `financeiro` virando páginas quebrariam tudo.
"""
from app.models.notificacao import NotificacaoTipo
from app.services.diarias_notification import DiariasNotifier
from app.services.notification_engine import NotificationEngine
from app.services.permissao_service import usuarios_com_acesso


def _tipo(db_session, codigo, modulo):
    db_session.add(NotificacaoTipo(codigo=codigo, modulo=modulo, nome=codigo, nivel='alerta'))
    db_session.commit()


class TestUsuariosComAcesso:

    def test_qualquer_acao_na_pagina_conta(self, usuario_com_permissoes):
        criar = usuario_com_permissoes(3301, [('financeiro', 'insercao_ne', 'criar')])
        aprovar = usuario_com_permissoes(3302, [('financeiro', 'insercao_ne', 'aprovar')])
        outra = usuario_com_permissoes(3303, [('financeiro', 'fundo_rotativo', 'excluir')])

        ids = usuarios_com_acesso('financeiro', 'insercao_ne')

        assert criar.id in ids and aprovar.id in ids
        assert outra.id not in ids

    def test_ignora_usuarios_inativos(self, usuario_com_permissoes):
        inativo = usuario_com_permissoes(3304, [('diarias', '', 'visualizar')], ativo=False)
        assert inativo.id not in usuarios_com_acesso('diarias')

    def test_nao_repete_usuario(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3305, [('diarias', '', 'visualizar'), ('diarias', '', 'criar')])
        assert usuarios_com_acesso('diarias').count(u.id) == 1


class TestResolverDestinatarios:

    def test_criar_em_inserir_nes_recebe_nova_solicitacao(self, db_session, usuario_com_permissoes):
        _tipo(db_session, 'financeiro.nova_solicitacao', 'financeiro')
        so_criar = usuario_com_permissoes(3310, [('financeiro', 'insercao_ne', 'criar')])
        so_fundo = usuario_com_permissoes(3311, [('financeiro', 'fundo_rotativo', 'visualizar')])

        ids = NotificationEngine.resolver_destinatarios('financeiro.nova_solicitacao')

        assert so_criar.id in ids
        assert so_fundo.id not in ids

    def test_modulo_sem_paginas_usa_hierarquia(self, db_session, usuario_com_permissoes):
        _tipo(db_session, 'solicitacao.criada', 'solicitacoes')
        u = usuario_com_permissoes(3312, [('solicitacoes', '', 'editar')])

        assert u.id in NotificationEngine.resolver_destinatarios('solicitacao.criada')


class TestDiariasNotifier:

    def test_financeiro_usa_a_pagina_diarias(self, usuario_com_permissoes):
        diarias = usuario_com_permissoes(3320, [('financeiro', 'diarias', 'criar')])
        fundo = usuario_com_permissoes(3321, [('financeiro', 'fundo_rotativo', 'visualizar')])

        ids = DiariasNotifier._resolver_usuarios_financeiro()

        assert diarias.id in ids
        assert fundo.id not in ids

    def test_diarias_aceita_qualquer_acao(self, usuario_com_permissoes):
        u = usuario_com_permissoes(3322, [('diarias', '', 'aprovar')])
        assert u.id in DiariasNotifier._resolver_usuarios_diarias()
