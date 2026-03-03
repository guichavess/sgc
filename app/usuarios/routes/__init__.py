"""
Módulo de Rotas de Usuários e Controle de Acesso.
Registra todos os sub-módulos de rotas.
"""
from flask import Blueprint

# Blueprint principal
usuarios_bp = Blueprint('usuarios', __name__)

# Importa e registra sub-módulos de rotas
from app.usuarios.routes import dashboard
from app.usuarios.routes import perfis
from app.usuarios.routes import api
