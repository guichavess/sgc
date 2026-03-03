"""
Rotas do dashboard (listagens) do módulo de Diárias.
"""
from flask import render_template, request
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission
from app.services.diaria_service import DiariaService


@diarias_bp.route('/')
@login_required
@requires_permission('diarias.visualizar')
def dashboard():
    """Lista as solicitações do usuário logado."""
    filtros = {
        'tipo_itinerario': request.args.get('tipo_itinerario', ''),
        'status': request.args.get('status', ''),
        'n_processo': request.args.get('n_processo', ''),
        'data_viagem': request.args.get('data_viagem', ''),
    }
    page = request.args.get('page', 1, type=int)

    # Usa o campo que identifica o usuário (sigla_login ou id)
    # No PHP o campo era CPF, aqui usamos id_usuario_sei ou sigla_login
    itinerarios = DiariaService.listar_itinerarios(
        usuario_cpf=current_user.sigla_login,
        filtros=filtros,
        page=page,
    )

    return render_template('diarias/dashboard.html',
        itinerarios=itinerarios,
        filtros=filtros,
        status_list=DiariaService.get_status_list(),
    )


@diarias_bp.route('/todas')
@login_required
@requires_permission('diarias.aprovar')
def todas():
    """Lista todas as solicitações (visão admin/aprovador)."""
    filtros = {
        'tipo_itinerario': request.args.get('tipo_itinerario', ''),
        'status': request.args.get('status', ''),
        'n_processo': request.args.get('n_processo', ''),
        'data_viagem': request.args.get('data_viagem', ''),
        'usuario': request.args.get('usuario', ''),
    }
    page = request.args.get('page', 1, type=int)

    itinerarios = DiariaService.listar_todos_itinerarios(
        filtros=filtros,
        page=page,
    )

    return render_template('diarias/todas.html',
        itinerarios=itinerarios,
        filtros=filtros,
        status_list=DiariaService.get_status_list(),
    )
