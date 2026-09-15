"""
Decorators e helpers para controle de acesso por permissões.

Hierarquia de acesso:
  - is_admin = True  → acesso total (todos os módulos + módulo Usuários)
  - Perfil com permissões → acesso aos módulos definidos no perfil
  - Sem perfil → sem acesso a nenhum módulo
"""
from functools import wraps
from flask import flash, redirect, url_for, session
from flask_login import current_user

from app.models.perfil import parse_permissao


def requires_admin(f):
    """Decorator para rotas exclusivas de administradores (is_admin=True).

    Usado no módulo Usuários — apenas admins podem gerenciar usuários e perfis.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))

        if not current_user.is_admin:
            flash('Acesso restrito a administradores.', 'danger')
            return redirect(url_for('hub'))

        return f(*args, **kwargs)
    return decorated_function


def requires_permission(permissao):
    """Decorator para proteger rotas por permissão de perfil.

    Formatos da permissão:
      'modulo.acao'          → @requires_permission('prestacoes_contratos.editar')
      'modulo.pagina.acao'   → @requires_permission('financeiro.fundo_rotativo.criar')
      'modulo.pagina'        → @requires_permission('financeiro.orcamento')
      'modulo'               → qualquer ação no módulo

    Nota: Admins (is_admin=True) passam automaticamente e páginas da alta gestão
    seguem `is_alta_gestao` — a verificação está em Usuario.tem_permissao().
    """
    modulo, pagina, acao = parse_permissao(permissao)

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login'))

            if not current_user.tem_permissao(modulo, acao, pagina):
                flash('Você não tem permissão para acessar esta funcionalidade.', 'danger')
                return redirect(url_for('hub'))

            return f(*args, **kwargs)
        return decorated_function
    return decorator


# =============================================================================
# HELPERS DE CAIXA SEI
# =============================================================================

# IDs das caixas SEI relevantes para o fluxo de diárias
CAIXA_APOIOSGA = "110006213"     # SEAD-PI/GAB/SGACG/APOIOSGA
CAIXA_NCI = "110006211"          # SEAD-PI/GAB/NCI
CAIXA_CCDP = "110008607"         # SEAD-PI/SGACG/DFIN/GEO/CCDP
CAIXA_DFIN_APOIO = "110009066"   # SEAD-PI/GAB/SGACG/DFIN/APOIO
CAIXA_GEO = "110006439"          # SEAD-PI/GAB/SGACG/DFIN/GEO
CAIXA_DFIN = "110006438"         # SEAD-PI/GAB/SGACG/DFIN
CAIXA_GPO = "110006440"          # SEAD-PI/GAB/SGACG/DFIN/GPO


def usuario_tem_caixa(caixa_id):
    """Verifica se o usuário logado tem acesso a uma caixa/unidade SEI específica.

    Consulta primeiro a tabela usuario_unidades_sei (banco) e faz fallback
    para session['unidades'] se o banco não tiver registros.

    Admins sempre retornam True.

    Args:
        caixa_id: ID string da unidade SEI (ex: '110006213')

    Returns:
        True se o usuário tem acesso à caixa, False caso contrário.
    """
    from flask_login import current_user
    if current_user.is_admin:
        return True

    # Consulta banco (fonte primária — sincronizado no login)
    from app.models.usuario import UsuarioUnidadeSei
    tem = UsuarioUnidadeSei.query.filter_by(
        usuario_id=current_user.id,
        unidade_sei_id=str(caixa_id),
    ).first()
    if tem:
        return True

    # Fallback: sessão (caso tabela ainda não tenha sido populada)
    unidades = session.get('unidades', [])
    return any(str(u.get('id', '')) == str(caixa_id) for u in unidades)
