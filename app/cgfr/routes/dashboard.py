"""
Rota principal do módulo CGFR — Dashboard.
"""
from flask import render_template
from flask_login import login_required

from app.cgfr.routes import cgfr_bp
from app.cgfr.services.processo_service import ProcessoService
from app.utils.permissions import requires_admin


@cgfr_bp.route('/')
@login_required
@requires_admin
def dashboard():
    """Dashboard CGFR com KPIs e DataTable."""
    stats = ProcessoService.get_dashboard_stats()
    filter_options = ProcessoService.get_filter_options()

    return render_template(
        'cgfr/dashboard.html',
        stats=stats,
        filter_options=filter_options,
    )
