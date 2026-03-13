"""
Rotas de relatórios CGFR.
"""
from datetime import datetime
from flask import render_template, request
from flask_login import login_required

from app.cgfr.routes import cgfr_bp
from app.cgfr.services.processo_service import ProcessoService
from app.utils.permissions import requires_admin


@cgfr_bp.route('/report/')
@login_required
@requires_admin
def report():
    """Relatório HTML agrupado por natureza de despesa."""
    filtros = {
        'status': request.args.get('status'),
        'natureza_despesa_id': request.args.get('natureza_despesa_id'),
        'fonte_id': request.args.get('fonte_id'),
        'acao_id': request.args.get('acao_id'),
    }
    filtros = {k: v for k, v in filtros.items() if v}

    dados = ProcessoService.gerar_relatorio_agrupado(filtros)
    filter_options = ProcessoService.get_filter_options()

    return render_template(
        'cgfr/reports/report.html',
        dados=dados,
        filter_options=filter_options,
        filtros=filtros,
    )


@cgfr_bp.route('/report/pdf')
@login_required
@requires_admin
def report_pdf():
    """Versão print-friendly do relatório."""
    filtros = {
        'status': request.args.get('status'),
        'natureza_despesa_id': request.args.get('natureza_despesa_id'),
        'fonte_id': request.args.get('fonte_id'),
        'acao_id': request.args.get('acao_id'),
    }
    filtros = {k: v for k, v in filtros.items() if v}

    dados = ProcessoService.gerar_relatorio_agrupado(filtros)

    return render_template(
        'cgfr/reports/report_pdf.html',
        dados=dados,
        data_geracao=datetime.now().strftime('%d/%m/%Y %H:%M'),
    )
