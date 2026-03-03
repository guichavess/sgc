"""
Módulo de Rotas de Diárias (Solicitação de Viagens).
Registra todos os sub-módulos de rotas.
"""
from flask import Blueprint

# Blueprint principal
diarias_bp = Blueprint('diarias', __name__)

# Importa e registra sub-módulos de rotas
from app.diarias.routes import dashboard
from app.diarias.routes import crud
from app.diarias.routes import api
from app.diarias.routes import admin
