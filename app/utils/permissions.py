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

    Formato da permissão: 'modulo.acao'
    Exemplo: @requires_permission('prestacoes_contratos.editar')

    Se apenas o módulo for passado (sem ação), verifica acesso ao módulo
    com qualquer ação (ex: 'financeiro').

    Nota: Admins (is_admin=True) passam automaticamente — a verificação
    está no método Usuario.tem_permissao().
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login'))

            # Parse modulo.acao
            partes = permissao.split('.', 1)
            modulo = partes[0]
            acao = partes[1] if len(partes) > 1 else None

            if not current_user.tem_permissao(modulo, acao):
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


def usuario_tem_caixa(caixa_id):
    """Verifica se o usuário logado tem acesso a uma caixa/unidade SEI específica.

    Consulta session['unidades'] (populada no login) para verificar se o ID
    da caixa está na lista de unidades do usuário.

    Args:
        caixa_id: ID string da unidade SEI (ex: '110006213')

    Returns:
        True se o usuário tem acesso à caixa, False caso contrário.
    """
    unidades = session.get('unidades', [])
    return any(str(u.get('id', '')) == str(caixa_id) for u in unidades)
