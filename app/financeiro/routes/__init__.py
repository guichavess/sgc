"""
Módulo de Rotas do Financeiro.
Registra todos os sub-módulos de rotas.

Cada página tem sua permissão (`PAGINAS_MODULO['financeiro']` em app/models/perfil.py):
@requires_permission('financeiro.<pagina>.<acao>'); Orçamento e Planejamento são da
alta gestão (@requires_permission('financeiro.orcamento') / 'financeiro.planejamento').
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
from app.financeiro.routes import planejamento
from app.financeiro.routes import fundo_rotativo
