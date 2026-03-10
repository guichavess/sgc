"""
Módulo de Rotas de Prestações de Contratos.
Registra todos os sub-módulos de rotas.
"""
from flask import Blueprint

# Blueprint principal
prestacoes_contratos_bp = Blueprint('prestacoes_contratos', __name__)

# Importa e registra sub-módulos de rotas
from app.prestacoes_contratos.routes import contratos
from app.prestacoes_contratos.routes import prestacoes
from app.prestacoes_contratos.routes import api
from app.prestacoes_contratos.routes import exec_orcamentaria
