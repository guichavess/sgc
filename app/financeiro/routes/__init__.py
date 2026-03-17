"""
Módulo de Rotas do Financeiro.
Registra todos os sub-módulos de rotas.
"""
from functools import wraps
from flask import Blueprint, redirect, url_for, flash
from flask_login import current_user

# Blueprint principal
financeiro_bp = Blueprint('financeiro', __name__)


def requires_admin_or_pedro(f):
    """Permite acesso a admins gerais e ao Pedro Alexandre."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        is_pedro = current_user.nome and 'PEDRO ALEXANDRE' in current_user.nome.upper()
        if not current_user.is_admin and not is_pedro:
            flash('Acesso restrito.', 'danger')
            return redirect(url_for('hub'))
        return f(*args, **kwargs)
    return decorated

# Importa e registra sub-módulos de rotas
from app.financeiro.routes import dashboard
from app.financeiro.routes import pendencias
from app.financeiro.routes import api
from app.financeiro.routes import diarias
from app.financeiro.routes import orcamentaria
from app.financeiro.routes import fornecedores
from app.financeiro.routes import execucoes
from app.financeiro.routes import planejamento
