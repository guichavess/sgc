from flask import Blueprint

identidade_visual_bp = Blueprint('identidade_visual', __name__)

from app.identidade_visual.routes import dashboard
