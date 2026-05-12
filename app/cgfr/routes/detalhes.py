"""
Rota de detalhes de processo CGFR.
"""
from flask import render_template, abort
from flask_login import login_required

from app.cgfr.routes import cgfr_bp
from app.cgfr.services.processo_service import ProcessoService
from app.constants import CGFR_DELIBERACAO_OPTIONS
from app.utils.permissions import requires_permission


@cgfr_bp.route('/detalhe/<path:protocolo>')
@login_required
@requires_permission('cgfr')
def detalhes(protocolo):
    """Página de detalhe de um processo CGFR."""
    processo = ProcessoService.get_detalhes(protocolo)
    if not processo:
        abort(404)

    filter_options = ProcessoService.get_filter_options()

    return render_template(
        'cgfr/detalhes.html',
        processo=processo,
        filter_options=filter_options,
        cgfr_deliberacao_options=CGFR_DELIBERACAO_OPTIONS,
    )
