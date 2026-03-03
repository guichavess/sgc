"""
Rotas de paginas do modulo de notificacoes.
"""
from flask import render_template, request
from flask_login import login_required, current_user

from app.notificacoes import notificacoes_bp
from app.repositories.notificacao_repository import NotificacaoRepository


@notificacoes_bp.route('/')
@login_required
def listar():
    """Pagina completa de notificacoes com filtros e paginacao."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    apenas_nao_lidas = request.args.get('nao_lidas', '', type=str) == '1'
    modulo = request.args.get('modulo', '', type=str) or None
    nivel = request.args.get('nivel', '', type=str) or None

    paginacao = NotificacaoRepository.listar_por_usuario(
        usuario_id=current_user.id,
        page=page,
        per_page=per_page,
        apenas_nao_lidas=apenas_nao_lidas,
        modulo=modulo,
        nivel=nivel,
    )

    return render_template(
        'notificacoes/index.html',
        notificacoes=paginacao.items,
        paginacao=paginacao,
        filtro_nao_lidas=apenas_nao_lidas,
        filtro_modulo=modulo or '',
        filtro_nivel=nivel or '',
    )


