"""
Modulo de Rotas de Dashboards.
Registra todos os sub-modulos de rotas.
"""
from flask import Blueprint

# Blueprint principal
dashboards_bp = Blueprint('dashboards', __name__)

# Importa e registra sub-modulos de rotas
from app.dashboards.routes import dashboard
from app.dashboards.routes import pagamentos
from app.dashboards.routes import financeiro
from app.dashboards.routes import contratos
from app.dashboards.routes import api
