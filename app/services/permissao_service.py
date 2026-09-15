"""
Consultas de permissão sobre vários usuários (ex.: destinatários de notificação).

A regra para um usuário fica em `Usuario.tem_permissao`; aqui ela é aplicada em SQL.
"""
from app.extensions import db
from app.models.perfil import PerfilPermissao
from app.models.usuario import Usuario


def usuarios_com_acesso(modulo, pagina=''):
    """IDs de usuários ativos com qualquer ação no módulo (ou na página).

    Qualquer ação concedida implica visualizar (hierarquia de ações), então basta
    existir uma linha em perfil_permissoes. Admins não entram por esta regra.

    Args:
        modulo: módulo da permissão (ex.: 'diarias').
        pagina: página do módulo (ex.: 'insercao_ne'); '' = módulo inteiro.
    """
    linhas = (
        db.session.query(Usuario.id)
        .join(PerfilPermissao, PerfilPermissao.perfil_id == Usuario.perfil_id)
        .filter(
            PerfilPermissao.modulo == modulo,
            PerfilPermissao.pagina == (pagina or ''),
            Usuario.ativo == True,  # noqa: E712
        )
        .distinct()
        .all()
    )
    return [usuario_id for (usuario_id,) in linhas]
