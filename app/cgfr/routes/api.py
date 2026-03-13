"""
API endpoints do módulo CGFR.
Endpoints JSON para DataTable, classificação, filtros, stats, sync e export.
"""
import logging
from datetime import datetime
from flask import request, jsonify, send_file
from flask_login import login_required, current_user

from app.cgfr.routes import cgfr_bp
from app.cgfr.services.processo_service import ProcessoService
from app.cgfr.services.sync_service import SyncService
from app.utils.permissions import requires_admin

logger = logging.getLogger(__name__)


@cgfr_bp.route('/api/data', methods=['POST'])
@login_required
@requires_admin
def api_data():
    """Endpoint POST principal para carregar a DataTable (client-side).
    Espelha website/app/processos/routes/api.py::api_data.
    Recebe: search, statusFilter, showOnlyEdited
    Retorna: todos os records + KPIs (paginacao feita pelo DataTable client-side)
    """
    data = request.get_json(silent=True) or {}

    search = data.get('search', '').strip()
    status_filter = data.get('statusFilter', 'all')
    show_only_edited = bool(data.get('showOnlyEdited', False))

    result = ProcessoService.get_processos_paginados(
        search=search,
        status_filter=status_filter,
        show_only_edited=show_only_edited,
    )

    return jsonify(result)


@cgfr_bp.route('/api/get_record', methods=['POST'])
@login_required
@requires_admin
def api_get_record():
    """Busca dados completos de um processo (para o modal de edicao).
    Espelha website/app/processos/routes/api.py::api_get_record.
    """
    import traceback
    data = request.get_json(silent=True) or {}
    protocolo = data.get('protocolo_formatado', '').strip()

    if not protocolo:
        return jsonify({'error': 'Protocolo nao informado'}), 400

    try:
        record = ProcessoService.get_record_completo(protocolo)
        if not record:
            return jsonify({'error': 'Processo nao encontrado'}), 404
        return jsonify(record)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"[get_record] Erro para protocolo={protocolo}: {e}\n{tb}")
        return jsonify({'error': f'Erro interno: {str(e)}'}), 500


@cgfr_bp.route('/api/datatable', methods=['POST'])
@login_required
@requires_admin
def api_datatable():
    """Endpoint DataTable server-side processing (legado, mantido para compat)."""
    data = request.get_json() or {}

    draw = int(data.get('draw', 1))
    start = int(data.get('start', 0))
    length = int(data.get('length', 25))
    search = data.get('search', {}).get('value', '') if isinstance(data.get('search'), dict) else data.get('search', '')

    filtros = {
        'status': data.get('status'),
        'natureza_despesa_id': data.get('natureza_despesa_id'),
        'fonte_id': data.get('fonte_id'),
        'acao_id': data.get('acao_id'),
        'tipo_processo': data.get('tipo_processo'),
        'possui_reserva': data.get('possui_reserva'),
        'nivel_prioridade': data.get('nivel_prioridade'),
    }
    filtros = {k: v for k, v in filtros.items() if v is not None and v != ''}

    resultado = ProcessoService.listar_para_datatable(draw, start, length, filtros, search)
    return jsonify(resultado)


@cgfr_bp.route('/api/salvar', methods=['POST'])
@login_required
@requires_admin
def api_salvar():
    """Salva classificação de um processo."""
    data = request.get_json()
    if not data:
        return jsonify({'sucesso': False, 'msg': 'Dados não recebidos'}), 400

    protocolo = data.get('processo_formatado')
    if not protocolo:
        return jsonify({'sucesso': False, 'msg': 'Protocolo não informado'}), 400

    try:
        processo = ProcessoService.classificar_processo(
            protocolo=protocolo,
            dados=data,
            usuario_id=current_user.id if current_user.is_authenticated else None,
        )
        if not processo:
            return jsonify({'sucesso': False, 'msg': 'Processo não encontrado'}), 404

        return jsonify({
            'sucesso': True,
            'msg': 'Classificação salva com sucesso',
            'classificado': processo.classificado,
        })
    except Exception as e:
        logger.exception(f'Erro ao salvar classificação do processo {protocolo}')
        return jsonify({'sucesso': False, 'msg': f'Erro ao salvar: {str(e)}'}), 500


@cgfr_bp.route('/api/filter-options')
@login_required
@requires_admin
def api_filter_options():
    """Retorna opções para Select2 (naturezas, fontes, ações)."""
    options = ProcessoService.get_filter_options()
    return jsonify(options)


@cgfr_bp.route('/api/stats')
@login_required
@requires_admin
def api_stats():
    """Retorna KPIs atualizados."""
    stats = ProcessoService.get_dashboard_stats()
    return jsonify(stats)


@cgfr_bp.route('/admin/sync', methods=['POST'])
@login_required
@requires_admin
def admin_sync():
    """Executa sincronização Trino → MySQL (admin only)."""
    try:
        resultado = SyncService.sync()
        return jsonify({'sucesso': True, **resultado})
    except Exception as e:
        logger.exception('Erro na sincronização CGFR')
        return jsonify({'sucesso': False, 'msg': f'Erro: {str(e)}'}), 500


@cgfr_bp.route('/api/export-excel')
@login_required
@requires_admin
def api_export_excel():
    """Exporta processos filtrados como arquivo Excel."""
    filtros = {
        'status': request.args.get('status'),
        'natureza_despesa_id': request.args.get('natureza_despesa_id'),
        'fonte_id': request.args.get('fonte_id'),
        'acao_id': request.args.get('acao_id'),
    }
    filtros = {k: v for k, v in filtros.items() if v}
    search = request.args.get('search', '')

    output = ProcessoService.exportar_excel(filtros, search)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'cgfr_processos_{timestamp}.xlsx',
    )
