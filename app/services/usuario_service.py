"""
Service Layer para o módulo de Usuários e Controle de Acesso.
"""
from app.extensions import db
from app.models.usuario import Usuario
from app.models.perfil import (
    ACOES, MODULOS, PAGINAS_MODULO, REGRA_ALTA_GESTAO, Perfil, PerfilPermissao,
    pagina_do_modulo, paginas_liberaveis,
)


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
            permissoes: Lista de dicts {'modulo', 'pagina', 'acao'}.
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
        """Retorna dict {'modulo' ou 'modulo:pagina': [acao1, ...]} das permissões do usuário."""
        if not usuario.perfil:
            return {}
        return usuario.perfil.listar_permissoes_dict()

    @staticmethod
    def resumo_permissoes_por_usuario(usuarios):
        """{usuario_id: {modulo: [textos]}} para os badges da listagem, em uma consulta.

        Módulos sem páginas listam as ações; módulos com páginas listam
        "Página: ações" na ordem de PAGINAS_MODULO.
        """
        resumo = {u.id: {} for u in usuarios}
        usuarios_por_perfil = {}
        for u in usuarios:
            if u.perfil_id:
                usuarios_por_perfil.setdefault(u.perfil_id, []).append(u.id)
        if not usuarios_por_perfil:
            return resumo

        linhas = (
            db.session.query(PerfilPermissao.perfil_id, PerfilPermissao.modulo,
                             PerfilPermissao.pagina, PerfilPermissao.acao)
            .filter(PerfilPermissao.perfil_id.in_(usuarios_por_perfil))
            .order_by(PerfilPermissao.id)
            .all()
        )
        agrupado = {}
        for perfil_id, modulo, pagina, acao in linhas:
            agrupado.setdefault((perfil_id, modulo, pagina or ''), []).append(acao)

        def _ordem_pagina(item):
            (_, modulo, pagina), _ = item
            chaves = [p.chave for p in PAGINAS_MODULO.get(modulo, [])]
            return chaves.index(pagina) if pagina in chaves else -1

        for (perfil_id, modulo, pagina), acoes in sorted(agrupado.items(), key=_ordem_pagina):
            if modulo in PAGINAS_MODULO:
                info = pagina_do_modulo(modulo, pagina)
                if not info:
                    continue  # linha antiga (pagina='') que o código novo ignora
                textos = [f'{info.rotulo}: {", ".join(acoes)}']
            else:
                textos = acoes
            for usuario_id in usuarios_por_perfil[perfil_id]:
                resumo[usuario_id].setdefault(modulo, []).extend(textos)
        return resumo

    @staticmethod
    def extrair_permissoes_form(form):
        """Lê os checkboxes `perm:{modulo}:{pagina}:{acao}` dos formulários de perfil e de usuário.

        Aceita só combinações válidas: módulo de MODULOS; página vazia em módulo
        sem páginas, ou página de regra 'perfil' em módulo com páginas; ação de
        ACOES. O resto é ignorado — inclusive páginas da alta gestão.

        Returns:
            Lista de dicts {'modulo', 'pagina', 'acao'} sem repetição.
        """
        modulos = {m for m, _ in MODULOS}
        acoes = {a for a, _ in ACOES}
        vistas = set()
        permissoes = []
        for chave in form:
            partes = chave.split(':')
            if len(partes) != 4 or partes[0] != 'perm':
                continue
            _, modulo, pagina, acao = partes
            if modulo not in modulos or acao not in acoes:
                continue
            if modulo in PAGINAS_MODULO:
                if pagina not in {p.chave for p in paginas_liberaveis(modulo)}:
                    continue
            elif pagina:
                continue
            if (modulo, pagina, acao) in vistas:
                continue
            vistas.add((modulo, pagina, acao))
            permissoes.append({'modulo': modulo, 'pagina': pagina, 'acao': acao})
        return permissoes

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
            permissoes: Lista de dicts {'modulo', 'pagina', 'acao'}
        """
        if Perfil.query.filter_by(nome=nome).first():
            raise ValueError(f'Já existe um perfil com o nome "{nome}".')

        perfil = Perfil(nome=nome, descricao=descricao)
        db.session.add(perfil)
        db.session.flush()

        if permissoes:
            _gravar_permissoes(perfil, permissoes)

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
            _gravar_permissoes(perfil, permissoes)

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

    @staticmethod
    def get_paginas_liberaveis():
        """{modulo: [PaginaModulo]} das páginas que o perfil pode liberar."""
        return {modulo: paginas_liberaveis(modulo) for modulo in PAGINAS_MODULO}

    @staticmethod
    def get_paginas_alta_gestao():
        """Páginas exclusivas da alta gestão (não aparecem na matriz do perfil)."""
        return [p for paginas in PAGINAS_MODULO.values() for p in paginas if p.regra == REGRA_ALTA_GESTAO]


# ── Funções internas ─────────────────────────────────────────────

def _gravar_permissoes(perfil, permissoes):
    """Adiciona as linhas de permissão do perfil (sem commit)."""
    for perm in permissoes:
        db.session.add(PerfilPermissao(
            perfil_id=perfil.id,
            modulo=perm['modulo'],
            pagina=perm.get('pagina', ''),
            acao=perm['acao'],
        ))
    perfil.limpar_cache_permissoes()


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

    _gravar_permissoes(perfil, permissoes)


def _remover_perfil_individual(usuario):
    """Remove o perfil individual de um usuário (se existir)."""
    if usuario.perfil and usuario.perfil.nome.startswith('_usuario_'):
        perfil = usuario.perfil
        usuario.perfil_id = None
        db.session.flush()
        db.session.delete(perfil)
    else:
        usuario.perfil_id = None
