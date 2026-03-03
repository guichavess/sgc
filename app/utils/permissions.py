"""
Decorators e helpers para controle de acesso por permissões.

Hierarquia de acesso:
  - is_admin = True  → acesso total (todos os módulos + módulo Usuários)
  - Perfil com permissões → acesso aos módulos definidos no perfil
  - Sem perfil → sem acesso a nenhum módulo
"""
from functools import wraps
from flask import flash, redirect, url_for
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
