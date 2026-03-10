"""
Rotas CRUD de Execuções Orçamentárias (módulo Financeiro).
"""
from flask import render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp
from app.extensions import db
from app.models.execucao_orcamentaria import ExecucaoOrcamentaria
from app.models.fornecedor import FornecedorSemContrato, FornecedorContrato
from app.utils.permissions import requires_permission


# =============================================================================
# Lista de Execuções
# =============================================================================
@financeiro_bp.route('/execucoes')
@login_required
@requires_permission('financeiro.visualizar')
def execucoes_lista():
    page = request.args.get('page', 1, type=int)
    busca = request.args.get('busca', '').strip()

    query = ExecucaoOrcamentaria.query.join(
        FornecedorSemContrato,
        ExecucaoOrcamentaria.fornecedor_id == FornecedorSemContrato.id
    )

    if busca:
        filtro = f'%{busca}%'
        query = query.filter(
            db.or_(
                ExecucaoOrcamentaria.descricao.ilike(filtro),
                ExecucaoOrcamentaria.item.ilike(filtro),
                FornecedorSemContrato.descricao.ilike(filtro),
            )
        )

    query = query.order_by(ExecucaoOrcamentaria.data_criacao.desc())
    pagination = query.paginate(page=page, per_page=20, error_out=False)

    # Lista de fornecedores para o select do modal
    fornecedores = FornecedorSemContrato.query.order_by(
        FornecedorSemContrato.descricao
    ).all()

    return render_template(
        'financeiro/execucoes.html',
        execucoes=pagination.items,
        pagination=pagination,
        busca=busca,
        fornecedores=fornecedores,
    )


# =============================================================================
# Cadastrar Execução
# =============================================================================
@financeiro_bp.route('/execucoes/cadastrar', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def execucoes_cadastrar():
    fornecedor_id = request.form.get('fornecedor_id', type=int)
    descricao = request.form.get('descricao', '').strip()
    item = request.form.get('item', '').strip()
    quantidade = request.form.get('quantidade', '').strip()
    competencia = request.form.get('competencia', '').strip()
    acao = request.form.get('acao', '').strip()
    natureza = request.form.get('natureza', '').strip()
    fonte = request.form.get('fonte', '').strip()

    if not fornecedor_id:
        flash('Fornecedor é obrigatório.', 'danger')
        return redirect(url_for('financeiro.execucoes_lista'))

    if not descricao:
        flash('Descrição é obrigatória.', 'danger')
        return redirect(url_for('financeiro.execucoes_lista'))

    # Verifica se fornecedor existe
    fornecedor = FornecedorSemContrato.query.get(fornecedor_id)
    if not fornecedor:
        flash('Fornecedor não encontrado.', 'danger')
        return redirect(url_for('financeiro.execucoes_lista'))

    # Parse quantidade
    qtd = None
    if quantidade:
        try:
            qtd = float(quantidade.replace(',', '.'))
        except ValueError:
            flash('Quantidade inválida.', 'danger')
            return redirect(url_for('financeiro.execucoes_lista'))

    execucao = ExecucaoOrcamentaria(
        fornecedor_id=fornecedor_id,
        descricao=descricao,
        item=item or None,
        quantidade=qtd,
        competencia=competencia or None,
        acao=acao or None,
        natureza=natureza or None,
        fonte=fonte or None,
        criado_por=current_user.id,
    )
    db.session.add(execucao)
    db.session.commit()

    flash('Execução cadastrada com sucesso!', 'success')
    return redirect(url_for('financeiro.execucoes_lista'))


# =============================================================================
# Vincular Contrato a Execução
# =============================================================================
@financeiro_bp.route('/execucoes/<int:id>/vincular-contrato', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def execucoes_vincular_contrato(id):
    execucao = ExecucaoOrcamentaria.query.get_or_404(id)
    cod_contrato = request.form.get('cod_contrato', '').strip()

    if not cod_contrato:
        flash('Código do contrato é obrigatório.', 'danger')
        return redirect(url_for('financeiro.execucoes_lista'))

    # Verifica se o contrato pertence ao fornecedor da execução
    vinculo = FornecedorContrato.query.filter_by(
        fornecedor_id=execucao.fornecedor_id,
        cod_contrato=cod_contrato
    ).first()

    if not vinculo:
        flash('Este contrato não está vinculado ao fornecedor desta execução.', 'danger')
        return redirect(url_for('financeiro.execucoes_lista'))

    execucao.cod_contrato = cod_contrato
    db.session.commit()

    flash(f'Execução vinculada ao contrato {cod_contrato}.', 'success')
    return redirect(url_for('financeiro.execucoes_lista'))
