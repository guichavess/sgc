"""
Blueprint do módulo CGFR (Comissão de Gestão Financeira e Gestão Por Resultados).
"""
from flask import Blueprint

cgfr_bp = Blueprint('cgfr', __name__)

from app.cgfr.routes import dashboard, api, detalhes, reports, acompanhar, vincular  # noqa: E402, F401
