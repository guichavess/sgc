"""
Rotas do dashboard (listagens) do módulo de Diárias.
"""
from flask import render_template, request
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission
from app.services.diaria_service import DiariaService
from app.models.diaria import Estado


def _preload_estados_destino(itinerarios_items):
    """Pré-carrega nomes dos estados de destino em batch (evita N+1 via property)."""
    estado_ids = {it.estado_destino for it in itinerarios_items if it.estado_destino}
    if not estado_ids:
        return {}
    estados = Estado.query.filter(Estado.cod_ibge.in_(estado_ids)).all()
    return {e.cod_ibge: e.nome for e in estados}


@diarias_bp.route('/')
@login_required
@requires_permission('diarias.visualizar')
def dashboard():
    """Lista as solicitações do usuário logado."""
    # Filtros multi-select (mesma convencao da Administracao)
    filtro_tipos = request.args.getlist('filtro_tipo', type=int)
    filtro_status = request.args.getlist('filtro_status', type=int)
    filtros = {
        'tipo_itinerario': str(filtro_tipos[0]) if len(filtro_tipos) == 1 else '',
        'tipos_itinerario': filtro_tipos,
        'status': str(filtro_status[0]) if len(filtro_status) == 1 else '',
        'status_list': filtro_status,
        'n_processo': request.args.get('q', '') or request.args.get('n_processo', ''),
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

    # Pré-carrega estados de destino em batch (§2.1 — evita N+1 via property)
    estados_destino = _preload_estados_destino(itinerarios.items)

    # Pré-carrega paradas (viagens estaduais) para exibir roteiro empilhado
    paradas_por_itin = {}
    itin_ids = [it.id for it in itinerarios.items]
    if itin_ids:
        from app.models.diaria import DiariasParada
        todas_paradas = DiariasParada.query.filter(
            DiariasParada.itinerario_id.in_(itin_ids)
        ).order_by(DiariasParada.id).all()
        for p in todas_paradas:
            paradas_por_itin.setdefault(p.itinerario_id, []).append(p)

    return render_template('diarias/dashboard.html',
        itinerarios=itinerarios,
        filtros=filtros,
        status_list=DiariaService.get_status_list(),
        estados_destino=estados_destino,
        paradas_por_itin=paradas_por_itin,
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

    # Pré-carrega estados de destino em batch (§2.1 — evita N+1 via property)
    estados_destino = _preload_estados_destino(itinerarios.items)

    return render_template('diarias/todas.html',
        itinerarios=itinerarios,
        filtros=filtros,
        status_list=DiariaService.get_status_list(),
        estados_destino=estados_destino,
    )
