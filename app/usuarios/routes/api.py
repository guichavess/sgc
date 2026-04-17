"""
API endpoints JSON do módulo Usuários.
Acesso restrito a administradores (is_admin=True).
"""
from flask import jsonify
from flask_login import login_required

from app.usuarios.routes import usuarios_bp
from app.services.usuario_service import UsuarioService
from app.utils.permissions import requires_admin
from app.extensions import db


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


@usuarios_bp.route('/api/setores/<int:superintendencia_id>')
@login_required
@requires_admin
def api_setores_por_superintendencia(superintendencia_id):
    """Retorna setores (Diretorias/Gerências/Coordenações) vinculados à Superintendência.

    Inclui a própria Superintendência como opção (para usuários do topo da SI).
    """
    from app.models.setor import Setor

    # A própria Superintendência + todos os setores cuja superintendencia_id == id
    setores = Setor.query.filter(
        db.or_(
            Setor.id == superintendencia_id,
            Setor.superintendencia_id == superintendencia_id,
        ),
        Setor.ativo == True,  # noqa: E712
    ).order_by(Setor.tipo_entidade_id, Setor.nome).all()

    return jsonify([{
        'id': s.id,
        'nome': s.nome,
        'sigla': s.sigla,
        'nome_exibicao': s.nome_exibicao,
        'tipo': s.tipo_entidade.nome if s.tipo_entidade else '',
    } for s in setores])
