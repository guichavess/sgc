"""
Rotas de Itens - CRUD de itens de contratos.
"""
from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from app.prestacoes_contratos.routes import prestacoes_contratos_bp
from app.services.prestacao_contrato_service import PrestacaoContratoService
from app.utils.permissions import requires_permission


@prestacoes_contratos_bp.route('/itens')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def itens_index():
    """Lista todos os itens com informação do criador e paginação."""
    page = request.args.get('page', 1, type=int)
    per_page = 20

    pagination = PrestacaoContratoService.listar_itens_paginado(
        page=page, per_page=per_page
    )

    itens = []
    for item, nome_criador in pagination.items:
        itens.append({
            'id': item.id,
            'descricao': item.descricao,
            'tipo_item': item.tipo_item or '',
            'editado_por': item.editado_por or ''
        })

    return render_template(
        'prestacoes_contratos/itens/index.html',
        itens=itens,
        pagination=pagination
    )


@prestacoes_contratos_bp.route('/itens/create')
@login_required
@requires_permission('prestacoes_contratos.criar')
def itens_create():
    """Formulário para novo item."""
    return render_template('prestacoes_contratos/itens/create.html')


@prestacoes_contratos_bp.route('/itens/store', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def itens_store():
    """Salva novo item."""
    descricao = request.form.get('descricao')
    tipo_item = request.form.get('tipo_item')

    PrestacaoContratoService.criar_item(descricao, tipo_item, current_user.id)
    flash('Item adicionado com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.itens_index'))


@prestacoes_contratos_bp.route('/itens/<int:item_id>/editar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def itens_editar(item_id):
    """Edita um item existente."""
    descricao = request.form.get('descricao')
    tipo_item = request.form.get('tipo_item')

    if not descricao or not descricao.strip():
        flash('A descrição não pode ser vazia.', 'warning')
        return redirect(url_for('prestacoes_contratos.itens_index'))

    nome_usuario = current_user.nome if hasattr(current_user, 'nome') else str(current_user.id)

    resultado = PrestacaoContratoService.editar_item(item_id, descricao.strip(), tipo_item, nome_usuario)
    if resultado:
        flash('Item atualizado com sucesso!', 'success')
    else:
        flash('Item não encontrado.', 'danger')

    return redirect(url_for('prestacoes_contratos.itens_index'))
