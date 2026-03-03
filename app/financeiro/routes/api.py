"""
Rotas de API - Endpoints AJAX/JSON do Módulo Financeiro.
"""
from flask import request, jsonify
from flask_login import login_required

from app.financeiro.routes import financeiro_bp
from app.models import SolicitacaoEmpenho
from app.extensions import db
from app.services.siafe_service import validar_ne_siafe
from app.utils.permissions import requires_permission


@financeiro_bp.route('/api/validar-ne', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def api_validar_ne():
    """Valida NE no SIAFE."""
    data = request.get_json() or {}
    ne = data.get('ne', '').strip()

    if not ne:
        return jsonify({'sucesso': False, 'mensagem': 'NE não informada'})

    resultado = validar_ne_siafe(ne)
    return jsonify(resultado)


@financeiro_bp.route('/api/salvar-ne', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def api_salvar_ne():
    """Salva NE em uma solicitação (via AJAX)."""
    data = request.get_json() or {}
    solicitacao_id = data.get('solicitacao_id')
    ne = data.get('ne', '').strip()

    if not solicitacao_id or not ne:
        return jsonify({'sucesso': False, 'msg': 'Dados incompletos'})

    if len(ne) < 4:
        return jsonify({'sucesso': False, 'msg': 'NE deve ter pelo menos 4 dígitos'})

    # Busca solicitação de empenho
    sol_empenho = SolicitacaoEmpenho.query.filter_by(
        id_solicitacao=solicitacao_id
    ).first()

    if not sol_empenho:
        return jsonify({'sucesso': False, 'msg': 'Solicitação não encontrada'})

    # Atualiza
    sol_empenho.ne = ne
    db.session.commit()

    return jsonify({'sucesso': True, 'msg': 'NE salva com sucesso'})
