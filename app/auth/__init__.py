"""
Módulo de Autenticação
Blueprint para rotas de login/logout e gerenciamento de sessão
"""

from flask import Blueprint

# Cria o Blueprint - a factory principal está em app/__init__.py
auth_bp = Blueprint('auth', __name__, template_folder='../templates/auth')

# Importa as rotas após criar o blueprint para evitar import circular
from app.auth import routes
