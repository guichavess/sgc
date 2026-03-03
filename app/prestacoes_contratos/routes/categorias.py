"""
Rotas de Categorias - CRUD de categorias de contratos.
"""
from flask import render_template, request, redirect, url_for, flash, session
from flask_login import login_required, current_user

from app.prestacoes_contratos.routes import prestacoes_contratos_bp
from app.services.prestacao_contrato_service import PrestacaoContratoService
from app.utils.permissions import requires_permission


@prestacoes_contratos_bp.route('/categorias')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def categorias_index():
    """Lista todas as categorias com paginação."""
    page = request.args.get('page', 1, type=int)
    per_page = 20

    pagination = PrestacaoContratoService.listar_categorias_paginado(
        page=page, per_page=per_page
    )

    categorias = []
    for categoria, nome_criador in pagination.items:
        categorias.append({
            'id': categoria.id,
            'descricao': categoria.descricao,
            'editado_por': categoria.editado_por or ''
        })

    return render_template(
        'prestacoes_contratos/categorias/index.html',
        categorias=categorias,
        pagination=pagination
    )


@prestacoes_contratos_bp.route('/categorias/create')
@login_required
@requires_permission('prestacoes_contratos.criar')
def categorias_create():
    """Formulário para nova categoria."""
    return render_template('prestacoes_contratos/categorias/create.html')


@prestacoes_contratos_bp.route('/categorias/store', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def categorias_store():
    """Salva nova categoria."""
    descricao = request.form.get('descricao')

    PrestacaoContratoService.criar_categoria(descricao, current_user.id)
    flash('Categoria adicionada com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.categorias_index'))


@prestacoes_contratos_bp.route('/categorias/<int:categoria_id>/editar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def categorias_editar(categoria_id):
    """Edita uma categoria existente."""
    descricao = request.form.get('descricao')
    if not descricao or not descricao.strip():
        flash('A descrição não pode ser vazia.', 'warning')
        return redirect(url_for('prestacoes_contratos.categorias_index'))

    # Busca o nome do usuário logado
    nome_usuario = current_user.nome if hasattr(current_user, 'nome') else str(current_user.id)

    resultado = PrestacaoContratoService.editar_categoria(categoria_id, descricao.strip(), nome_usuario)
    if resultado:
        flash('Categoria atualizada com sucesso!', 'success')
    else:
        flash('Categoria não encontrada.', 'danger')

    return redirect(url_for('prestacoes_contratos.categorias_index'))


@prestacoes_contratos_bp.route('/categorias/associar/<codigo_contrato>', methods=['GET'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def categorias_associar(codigo_contrato):
    """Associa categoria a um contrato."""
    contrato = PrestacaoContratoService.buscar_contrato(codigo_contrato)
    if not contrato:
        flash('Contrato não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    categorias = PrestacaoContratoService.listar_categorias()

    return render_template(
        'prestacoes_contratos/categorias/associar.html',
        contrato=contrato,
        categorias=categorias
    )


@prestacoes_contratos_bp.route('/categorias/associar_store/<codigo_contrato>', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def categorias_associar_store(codigo_contrato):
    """Salva associação de categoria ao contrato."""
    categoria_id = request.form.get('categoria_id')

    # Atualizar categoria do contrato
    PrestacaoContratoService.associar_categoria(codigo_contrato, categoria_id)

    flash('Categoria associada com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.contrato_gerenciar', codigo=codigo_contrato))
