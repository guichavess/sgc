"""
Blueprint de Notificacoes - Transversal a todos os modulos.
"""
from flask import Blueprint

notificacoes_bp = Blueprint('notificacoes', __name__)

# Importar rotas
from app.notificacoes.routes import api, pages, contato  # noqa: E402, F401
