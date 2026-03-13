"""
Blueprint do módulo CGFR (Consultoria de Gestão Financeira).
"""
from flask import Blueprint

cgfr_bp = Blueprint('cgfr', __name__)

from app.cgfr.routes import dashboard, api, detalhes, reports  # noqa: E402, F401
