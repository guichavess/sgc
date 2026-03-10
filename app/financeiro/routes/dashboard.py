"""
Dashboard do Módulo Financeiro — redireciona conforme perfil.
"""
from flask import redirect, url_for
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp


@financeiro_bp.route('/')
@login_required
def dashboard():
    """Admin vai para Orçamento; demais vão para Inserir NEs."""
    if current_user.is_admin or (current_user.nome and 'PEDRO ALEXANDRE' in current_user.nome.upper()):
        return redirect(url_for('financeiro.orcamentaria'))
    return redirect(url_for('financeiro.pendencias_ne'))
