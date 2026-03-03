"""
Service Layer para o módulo de Usuários e Controle de Acesso.
"""
from app.extensions import db
from app.models.usuario import Usuario
from app.models.perfil import Perfil, PerfilPermissao, MODULOS, ACOES


class UsuarioService:
    """Serviço para operações de usuários e perfis."""

    # ── Usuários ──────────────────────────────────────────────

    @staticmethod
    def listar_usuarios_paginado(nome=None, perfil_id=None, ativo=None,
                                  page=1, per_page=20):
        """Lista usuários com filtros e paginação."""
        query = Usuario.query

        if nome:
            query = query.filter(Usuario.nome.ilike(f'%{nome}%'))
        if perfil_id:
            query = query.filter(Usuario.perfil_id == perfil_id)
        if ativo is not None:
            query = query.filter(Usuario.ativo == ativo)

        query = query.order_by(Usuario.nome.asc())
        return query.paginate(page=page, per_page=per_page, error_out=False)

    @staticmethod
    def buscar_usuario(usuario_id):
        """Busca um usuário por ID."""
        return Usuario.query.get(usuario_id)

    @staticmethod
    def atualizar_usuario(usuario_id, ativo=None, permissoes=None):
        """Atualiza status e permissões de acesso de um usuário.

        Args:
            usuario_id: ID do usuário
            ativo: True/False para ativar/desativar
            permissoes: Lista de dicts {'modulo': str, 'acao': str}.
                        Se fornecida, cria/atualiza um perfil individual
                        para este usuário com essas permissões.
                        Se lista vazia, remove o perfil do usuário.
        """
        usuario = Usuario.query.get(usuario_id)
        if not usuario:
            raise ValueError('Usuário não encontrado.')

        if ativo is not None:
            usuario.ativo = ativo

        if permissoes is not None:
            if len(permissoes) == 0:
                # Sem permissões: remove vínculo com perfil
                _remover_perfil_individual(usuario)
            else:
                # Cria ou atualiza perfil individual do usuário
                _definir_perfil_individual(usuario, permissoes)

        db.session.commit()
        return usuario

    @staticmethod
    def obter_permissoes_usuario(usuario):
        """Retorna dict {modulo: [acao1, ...]} das permissões do usuário."""
        if not usuario.perfil:
            return {}
        return usuario.perfil.listar_permissoes_dict()

    # ── Perfis ────────────────────────────────────────────────

    @staticmethod
    def listar_perfis(apenas_ativos=False):
        """Lista todos os perfis."""
        query = Perfil.query
        if apenas_ativos:
            query = query.filter_by(ativo=True)
        return query.order_by(Perfil.nome.asc()).all()

    @staticmethod
    def buscar_perfil(perfil_id):
        """Busca um perfil por ID com suas permissões."""
        return Perfil.query.get(perfil_id)

    @staticmethod
    def criar_perfil(nome, descricao=None, permissoes=None):
        """Cria um novo perfil com permissões.

        Args:
            nome: Nome do perfil
            descricao: Descrição do perfil
            permissoes: Lista de dicts {'modulo': str, 'acao': str}
        """
        if Perfil.query.filter_by(nome=nome).first():
            raise ValueError(f'Já existe um perfil com o nome "{nome}".')

        perfil = Perfil(nome=nome, descricao=descricao)
        db.session.add(perfil)
        db.session.flush()

        if permissoes:
            for perm in permissoes:
                pp = PerfilPermissao(
                    perfil_id=perfil.id,
                    modulo=perm['modulo'],
                    acao=perm['acao']
                )
                db.session.add(pp)

        db.session.commit()
        return perfil

    @staticmethod
    def atualizar_perfil(perfil_id, nome=None, descricao=None, ativo=None,
                         permissoes=None):
        """Atualiza um perfil e suas permissões."""
        perfil = Perfil.query.get(perfil_id)
        if not perfil:
            raise ValueError('Perfil não encontrado.')

        if nome and nome != perfil.nome:
            existente = Perfil.query.filter_by(nome=nome).first()
            if existente:
                raise ValueError(f'Já existe um perfil com o nome "{nome}".')
            perfil.nome = nome

        if descricao is not None:
            perfil.descricao = descricao

        if ativo is not None:
            perfil.ativo = ativo

        if permissoes is not None:
            PerfilPermissao.query.filter_by(perfil_id=perfil.id).delete()
            for perm in permissoes:
                pp = PerfilPermissao(
                    perfil_id=perfil.id,
                    modulo=perm['modulo'],
                    acao=perm['acao']
                )
                db.session.add(pp)

        db.session.commit()
        return perfil

    @staticmethod
    def excluir_perfil(perfil_id):
        """Exclui um perfil."""
        perfil = Perfil.query.get(perfil_id)
        if not perfil:
            raise ValueError('Perfil não encontrado.')

        qtd_usuarios = Usuario.query.filter_by(perfil_id=perfil_id).count()
        if qtd_usuarios > 0:
            raise ValueError(
                f'Não é possível excluir: {qtd_usuarios} usuário(s) '
                f'vinculado(s) a este perfil.'
            )

        db.session.delete(perfil)
        db.session.commit()

    @staticmethod
    def contar_usuarios_por_perfil(perfil_id):
        """Conta quantos usuários estão vinculados a um perfil."""
        return Usuario.query.filter_by(perfil_id=perfil_id).count()

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def get_modulos():
        """Retorna lista de módulos disponíveis."""
        return MODULOS

    @staticmethod
    def get_acoes():
        """Retorna lista de ações disponíveis."""
        return ACOES


# ── Funções internas ─────────────────────────────────────────────

def _definir_perfil_individual(usuario, permissoes):
    """Cria ou atualiza o perfil individual de um usuário.

    Cada usuário não-admin recebe um perfil com nome baseado no seu ID.
    """
    if usuario.perfil:
        # Atualiza permissões do perfil existente
        perfil = usuario.perfil
        PerfilPermissao.query.filter_by(perfil_id=perfil.id).delete()
    else:
        # Cria perfil individual
        nome_perfil = f'_usuario_{usuario.id}'
        perfil = Perfil.query.filter_by(nome=nome_perfil).first()
        if not perfil:
            perfil = Perfil(
                nome=nome_perfil,
                descricao=f'Perfil individual - {usuario.nome}'
            )
            db.session.add(perfil)
            db.session.flush()
        else:
            PerfilPermissao.query.filter_by(perfil_id=perfil.id).delete()
        usuario.perfil_id = perfil.id

    for perm in permissoes:
        pp = PerfilPermissao(
            perfil_id=perfil.id,
            modulo=perm['modulo'],
            acao=perm['acao']
        )
        db.session.add(pp)


def _remover_perfil_individual(usuario):
    """Remove o perfil individual de um usuário (se existir)."""
    if usuario.perfil and usuario.perfil.nome.startswith('_usuario_'):
        perfil = usuario.perfil
        usuario.perfil_id = None
        db.session.flush()
        db.session.delete(perfil)
    else:
        usuario.perfil_id = None
