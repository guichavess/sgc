"""
API endpoints JSON do módulo Usuários.
Acesso restrito a administradores (is_admin=True).
"""
from flask import jsonify
from flask_login import login_required

from app.usuarios.routes import usuarios_bp
from app.services.usuario_service import UsuarioService
from app.utils.permissions import requires_admin


@usuarios_bp.route('/api/perfis')
@login_required
@requires_admin
def api_perfis():
    """Retorna lista de perfis ativos (JSON)."""
    perfis = UsuarioService.listar_perfis(apenas_ativos=True)
    return jsonify([{
        'id': p.id,
        'nome': p.nome,
        'descricao': p.descricao
    } for p in perfis])
