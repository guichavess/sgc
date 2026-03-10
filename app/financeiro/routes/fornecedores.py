"""
Rotas CRUD de Fornecedores sem Contrato (módulo Financeiro).
"""
from flask import render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp
from app.extensions import db
from app.models.fornecedor import FornecedorSemContrato, FornecedorContrato
from app.utils.permissions import requires_permission


def _validar_cnpj(cnpj_str):
    """Valida CNPJ usando algoritmo dos dígitos verificadores."""
    digitos = ''.join(c for c in cnpj_str if c.isdigit())
    if len(digitos) != 14:
        return False
    if digitos == digitos[0] * 14:
        return False

    # Primeiro dígito verificador
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos1[i] for i in range(12))
    resto = soma % 11
    dv1 = 0 if resto < 2 else 11 - resto
    if int(digitos[12]) != dv1:
        return False

    # Segundo dígito verificador
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos2[i] for i in range(13))
    resto = soma % 11
    dv2 = 0 if resto < 2 else 11 - resto
    if int(digitos[13]) != dv2:
        return False

    return True


# =============================================================================
# Lista de Fornecedores
# =============================================================================
@financeiro_bp.route('/fornecedores')
@login_required
@requires_permission('financeiro.visualizar')
def fornecedores_lista():
    page = request.args.get('page', 1, type=int)
    busca = request.args.get('busca', '').strip()

    query = FornecedorSemContrato.query

    if busca:
        filtro = f'%{busca}%'
        query = query.filter(
            db.or_(
                FornecedorSemContrato.descricao.ilike(filtro),
                FornecedorSemContrato.cnpj.ilike(filtro),
            )
        )

    query = query.order_by(FornecedorSemContrato.data_criacao.desc())
    pagination = query.paginate(page=page, per_page=20, error_out=False)

    return render_template(
        'financeiro/fornecedores.html',
        fornecedores=pagination.items,
        pagination=pagination,
        busca=busca,
    )


# =============================================================================
# Cadastrar Fornecedor
# =============================================================================
@financeiro_bp.route('/fornecedores/cadastrar', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def fornecedores_cadastrar():
    descricao = request.form.get('descricao', '').strip()
    cnpj = request.form.get('cnpj', '').strip()
    telefone = request.form.get('telefone', '').strip()

    if not descricao:
        flash('Descrição é obrigatória.', 'danger')
        return redirect(url_for('financeiro.fornecedores_lista'))

    if not cnpj:
        flash('CNPJ é obrigatório.', 'danger')
        return redirect(url_for('financeiro.fornecedores_lista'))

    if not _validar_cnpj(cnpj):
        flash('CNPJ inválido. Verifique os dígitos.', 'danger')
        return redirect(url_for('financeiro.fornecedores_lista'))

    fornecedor = FornecedorSemContrato(
        descricao=descricao,
        cnpj=cnpj,
        telefone=telefone or None,
        criado_por=current_user.id,
    )
    db.session.add(fornecedor)
    db.session.commit()

    flash(f'Fornecedor "{descricao}" cadastrado com sucesso!', 'success')
    return redirect(url_for('financeiro.fornecedores_lista'))


# =============================================================================
# Vincular Contrato a Fornecedor
# =============================================================================
@financeiro_bp.route('/fornecedores/<int:id>/vincular-contrato', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def fornecedores_vincular_contrato(id):
    fornecedor = FornecedorSemContrato.query.get_or_404(id)
    cod_contrato = request.form.get('cod_contrato', '').strip()

    if not cod_contrato:
        flash('Código do contrato é obrigatório.', 'danger')
        return redirect(url_for('financeiro.fornecedores_lista'))

    # Verifica duplicidade
    existente = FornecedorContrato.query.filter_by(
        fornecedor_id=id, cod_contrato=cod_contrato
    ).first()
    if existente:
        flash(f'Contrato {cod_contrato} já está vinculado a este fornecedor.', 'warning')
        return redirect(url_for('financeiro.fornecedores_lista'))

    vinculo = FornecedorContrato(
        fornecedor_id=id,
        cod_contrato=cod_contrato,
        vinculado_por=current_user.id,
    )
    db.session.add(vinculo)
    db.session.commit()

    flash(f'Contrato {cod_contrato} vinculado ao fornecedor "{fornecedor.descricao}".', 'success')
    return redirect(url_for('financeiro.fornecedores_lista'))


# =============================================================================
# Remover vínculo de Contrato
# =============================================================================
@financeiro_bp.route('/fornecedores/contrato/<int:id>/remover', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def fornecedores_remover_contrato(id):
    vinculo = FornecedorContrato.query.get_or_404(id)
    cod = vinculo.cod_contrato
    db.session.delete(vinculo)
    db.session.commit()

    flash(f'Vínculo com contrato {cod} removido.', 'success')
    return redirect(url_for('financeiro.fornecedores_lista'))


# =============================================================================
# API: Contratos de um Fornecedor (usado pelo modal de execuções)
# =============================================================================
@financeiro_bp.route('/api/fornecedores/<int:id>/contratos')
@login_required
@requires_permission('financeiro.visualizar')
def api_fornecedor_contratos(id):
    fornecedor = FornecedorSemContrato.query.get_or_404(id)
    contratos = [
        {'id': c.id, 'cod_contrato': c.cod_contrato}
        for c in fornecedor.contratos
    ]
    return jsonify(contratos)
