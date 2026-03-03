"""
Módulo de Rotas de Solicitações.
Registra todos os sub-blueprints de rotas.
"""
from flask import Blueprint

# Blueprint principal
solicitacoes_bp = Blueprint('solicitacoes', __name__)

# Importa e registra sub-módulos de rotas
from app.solicitacoes.routes import dashboard
from app.solicitacoes.routes import crud
from app.solicitacoes.routes import reports
from app.solicitacoes.routes import api
