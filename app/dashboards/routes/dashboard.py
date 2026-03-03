"""
Dashboard SPA Shell - Serve a aplicação React.
Acesso restrito a administradores (is_admin=True).
"""
from flask import render_template, session, url_for
from flask_login import login_required

from app.dashboards.routes import dashboards_bp
from app.utils.permissions import requires_admin


@dashboards_bp.route('/')
@dashboards_bp.route('/<path:path>')
@login_required
@requires_admin
def spa_shell(path=''):
    """Serve o shell HTML da SPA React. Todo o roteamento é client-side."""
    usuario_nome = session.get('usuario_nome', '')
    return render_template(
        'dashboards/spa_shell.html',
        usuario_nome=usuario_nome,
        hub_url=url_for('hub'),
        logout_url=url_for('auth.logout'),
        logo_url=url_for('static', filename='img/logo-sead-branca.png'),
    )
