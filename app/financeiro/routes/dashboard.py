"""
Entrada do Módulo Financeiro — leva à primeira página liberada para o usuário.
"""
from flask import flash, redirect, url_for
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp


@financeiro_bp.route('/')
@login_required
def dashboard():
    """Redireciona para a primeira página acessível, na ordem de PAGINAS_MODULO.

    Admin e alta gestão entram em Orçamento; os demais, na primeira página do perfil.
    """
    paginas = current_user.paginas_acessiveis('financeiro')
    if not paginas:
        flash('Você não tem permissão para acessar esta funcionalidade.', 'danger')
        return redirect(url_for('hub'))
    return redirect(url_for(paginas[0].endpoint))
