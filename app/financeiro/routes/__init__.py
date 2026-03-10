"""
Módulo de Rotas do Financeiro.
Registra todos os sub-módulos de rotas.
"""
from flask import Blueprint

# Blueprint principal
financeiro_bp = Blueprint('financeiro', __name__)

# Importa e registra sub-módulos de rotas
from app.financeiro.routes import dashboard
from app.financeiro.routes import pendencias
from app.financeiro.routes import api
from app.financeiro.routes import diarias
from app.financeiro.routes import orcamentaria
from app.financeiro.routes import fornecedores
from app.financeiro.routes import execucoes
