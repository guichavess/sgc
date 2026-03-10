"""
Rotas de Execução Orçamentária (Fornecedores + Execuções) — módulo Contratos.
Migrado do módulo Financeiro para agrupar sob "Exec Orçamentária" com sub-abas.
"""
from flask import render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text

from app.prestacoes_contratos.routes import prestacoes_contratos_bp
from app.extensions import db
from app.models.execucao_orcamentaria import ExecucaoOrcamentaria
from app.models.fornecedor import FornecedorSemContrato, FornecedorContrato
from app.utils.permissions import requires_permission


def _validar_cnpj(cnpj_str):
    """Valida CNPJ usando algoritmo dos digitos verificadores."""
    digitos = ''.join(c for c in cnpj_str if c.isdigit())
    if len(digitos) != 14:
        return False
    if digitos == digitos[0] * 14:
        return False

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos1[i] for i in range(12))
    resto = soma % 11
    dv1 = 0 if resto < 2 else 11 - resto
    if int(digitos[12]) != dv1:
        return False

    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos2[i] for i in range(13))
    resto = soma % 11
    dv2 = 0 if resto < 2 else 11 - resto
    if int(digitos[13]) != dv2:
        return False

    return True


# =============================================================================
# Pagina combinada: Fornecedores + Execucoes (sub-abas)
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def exec_orcamentaria():
    aba = request.args.get('aba', 'fornecedores')

    # --- Dados da aba Fornecedores ---
    page_f = request.args.get('page_f', 1, type=int)
    busca_f = request.args.get('busca_f', '').strip()
    query_f = FornecedorSemContrato.query
    if busca_f:
        filtro = f'%{busca_f}%'
        query_f = query_f.filter(
            db.or_(
                FornecedorSemContrato.descricao.ilike(filtro),
                FornecedorSemContrato.cnpj.ilike(filtro),
            )
        )
    query_f = query_f.order_by(FornecedorSemContrato.data_criacao.desc())
    pag_fornecedores = query_f.paginate(page=page_f, per_page=20, error_out=False)

    # --- Dados da aba Execucoes ---
    page_e = request.args.get('page_e', 1, type=int)
    busca_e = request.args.get('busca_e', '').strip()
    filtro_contrato = request.args.get('filtro_contrato', '').strip()
    filtro_competencia = request.args.get('filtro_competencia', '').strip()
    filtro_item = request.args.get('filtro_item', '').strip()

    query_e = ExecucaoOrcamentaria.query.join(
        FornecedorSemContrato,
        ExecucaoOrcamentaria.fornecedor_id == FornecedorSemContrato.id
    )
    if busca_e:
        filtro = f'%{busca_e}%'
        query_e = query_e.filter(
            db.or_(
                ExecucaoOrcamentaria.descricao.ilike(filtro),
                ExecucaoOrcamentaria.item.ilike(filtro),
                FornecedorSemContrato.descricao.ilike(filtro),
            )
        )
    if filtro_contrato:
        if filtro_contrato == '__pendente__':
            query_e = query_e.filter(
                db.or_(
                    ExecucaoOrcamentaria.cod_contrato.is_(None),
                    ExecucaoOrcamentaria.cod_contrato == ''
                )
            )
        else:
            query_e = query_e.filter(ExecucaoOrcamentaria.cod_contrato == filtro_contrato)
    if filtro_competencia:
        query_e = query_e.filter(ExecucaoOrcamentaria.competencia == filtro_competencia)
    if filtro_item:
        query_e = query_e.filter(ExecucaoOrcamentaria.item == filtro_item)

    query_e = query_e.order_by(ExecucaoOrcamentaria.data_criacao.desc())
    pag_execucoes = query_e.paginate(page=page_e, per_page=20, error_out=False)

    # Valores distintos para os dropdowns de filtro
    contratos_distintos = [r[0] for r in db.session.query(
        ExecucaoOrcamentaria.cod_contrato
    ).filter(
        ExecucaoOrcamentaria.cod_contrato.isnot(None),
        ExecucaoOrcamentaria.cod_contrato != ''
    ).distinct().order_by(ExecucaoOrcamentaria.cod_contrato).all()]

    competencias_distintas = [r[0] for r in db.session.query(
        ExecucaoOrcamentaria.competencia
    ).filter(
        ExecucaoOrcamentaria.competencia.isnot(None),
        ExecucaoOrcamentaria.competencia != ''
    ).distinct().order_by(ExecucaoOrcamentaria.competencia).all()]

    itens_distintos = [r[0] for r in db.session.query(
        ExecucaoOrcamentaria.item
    ).filter(
        ExecucaoOrcamentaria.item.isnot(None),
        ExecucaoOrcamentaria.item != ''
    ).distinct().order_by(ExecucaoOrcamentaria.item).all()]

    # Lista de fornecedores para select do modal de execucao
    fornecedores_select = FornecedorSemContrato.query.order_by(
        FornecedorSemContrato.descricao
    ).all()

    # Listas para selects de Ação, Natureza e Fonte
    lista_acoes = _listar_acoes_select()
    lista_naturezas = _listar_naturezas_select()
    lista_fontes = _listar_fontes_select()

    return render_template(
        'prestacoes_contratos/exec_orcamentaria.html',
        aba=aba,
        # Fornecedores
        fornecedores=pag_fornecedores.items,
        pag_fornecedores=pag_fornecedores,
        busca_f=busca_f,
        # Execucoes
        execucoes=pag_execucoes.items,
        pag_execucoes=pag_execucoes,
        busca_e=busca_e,
        filtro_contrato=filtro_contrato,
        filtro_competencia=filtro_competencia,
        filtro_item=filtro_item,
        contratos_distintos=contratos_distintos,
        competencias_distintas=competencias_distintas,
        itens_distintos=itens_distintos,
        # Aux
        fornecedores_select=fornecedores_select,
        lista_acoes=lista_acoes,
        lista_naturezas=lista_naturezas,
        lista_fontes=lista_fontes,
    )


# =============================================================================
# FORNECEDORES: Cadastrar
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria/fornecedores/cadastrar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def eo_fornecedor_cadastrar():
    descricao = request.form.get('descricao', '').strip()
    cnpj = request.form.get('cnpj', '').strip()
    telefone = request.form.get('telefone', '').strip()

    if not descricao:
        flash('Descricao e obrigatoria.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    if not cnpj:
        flash('CNPJ e obrigatorio.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    if not _validar_cnpj(cnpj):
        flash('CNPJ invalido. Verifique os digitos.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    fornecedor = FornecedorSemContrato(
        descricao=descricao,
        cnpj=cnpj,
        telefone=telefone or None,
        criado_por=current_user.id,
    )
    db.session.add(fornecedor)
    db.session.commit()

    flash(f'Fornecedor "{descricao}" cadastrado com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))


# =============================================================================
# FORNECEDORES: Vincular Contrato
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria/fornecedores/<int:id>/vincular-contrato', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def eo_fornecedor_vincular_contrato(id):
    fornecedor = FornecedorSemContrato.query.get_or_404(id)
    cod_contrato = request.form.get('cod_contrato', '').strip()

    if not cod_contrato:
        flash('Codigo do contrato e obrigatorio.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    existente = FornecedorContrato.query.filter_by(
        fornecedor_id=id, cod_contrato=cod_contrato
    ).first()
    if existente:
        flash(f'Contrato {cod_contrato} ja esta vinculado a este fornecedor.', 'warning')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    vinculo = FornecedorContrato(
        fornecedor_id=id,
        cod_contrato=cod_contrato,
        vinculado_por=current_user.id,
    )
    db.session.add(vinculo)
    db.session.commit()

    flash(f'Contrato {cod_contrato} vinculado ao fornecedor "{fornecedor.descricao}".', 'success')
    return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))


# =============================================================================
# FORNECEDORES: Remover vinculo de Contrato
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria/fornecedores/contrato/<int:id>/remover', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def eo_fornecedor_remover_contrato(id):
    vinculo = FornecedorContrato.query.get_or_404(id)
    cod = vinculo.cod_contrato
    db.session.delete(vinculo)
    db.session.commit()

    flash(f'Vinculo com contrato {cod} removido.', 'success')
    return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))


# =============================================================================
# EXECUCOES: Cadastrar
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria/execucoes/cadastrar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def eo_execucao_cadastrar():
    fornecedor_id = request.form.get('fornecedor_id', type=int)
    descricao = request.form.get('descricao', '').strip()
    item = request.form.get('item', '').strip()
    quantidade = request.form.get('quantidade', '').strip()
    competencia = request.form.get('competencia', '').strip()
    acao = request.form.get('acao', '').strip()
    natureza = request.form.get('natureza', '').strip()
    fonte = request.form.get('fonte', '').strip()

    if not fornecedor_id:
        flash('Fornecedor e obrigatorio.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    if not descricao:
        flash('Descricao e obrigatoria.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    fornecedor = FornecedorSemContrato.query.get(fornecedor_id)
    if not fornecedor:
        flash('Fornecedor nao encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    qtd = None
    if quantidade:
        try:
            qtd = float(quantidade.replace(',', '.'))
        except ValueError:
            flash('Quantidade invalida.', 'danger')
            return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    valor_str = request.form.get('valor', '').strip()
    val = None
    if valor_str:
        try:
            val = float(valor_str.replace('.', '').replace(',', '.'))
        except ValueError:
            flash('Valor inválido.', 'danger')
            return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    execucao = ExecucaoOrcamentaria(
        fornecedor_id=fornecedor_id,
        descricao=descricao,
        item=item or None,
        quantidade=qtd,
        valor=val,
        competencia=competencia or None,
        acao=acao or None,
        natureza=natureza or None,
        fonte=fonte or None,
        criado_por=current_user.id,
    )
    db.session.add(execucao)
    db.session.commit()

    flash('Execucao cadastrada com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))


# =============================================================================
# EXECUCOES: Vincular Contrato
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria/execucoes/<int:id>/vincular-contrato', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def eo_execucao_vincular_contrato(id):
    execucao = ExecucaoOrcamentaria.query.get_or_404(id)
    cod_contrato = request.form.get('cod_contrato', '').strip()

    if not cod_contrato:
        flash('Codigo do contrato e obrigatorio.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    vinculo = FornecedorContrato.query.filter_by(
        fornecedor_id=execucao.fornecedor_id,
        cod_contrato=cod_contrato
    ).first()

    if not vinculo:
        flash('Este contrato nao esta vinculado ao fornecedor desta execucao.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    execucao.cod_contrato = cod_contrato
    db.session.commit()

    flash(f'Execucao vinculada ao contrato {cod_contrato}.', 'success')
    return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))


# =============================================================================
# API: Contratos de um Fornecedor (usado pelo modal de vincular execucao)
# =============================================================================
@prestacoes_contratos_bp.route('/api/fornecedores/<int:id>/contratos')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def api_eo_fornecedor_contratos(id):
    fornecedor = FornecedorSemContrato.query.get_or_404(id)
    contratos = [
        {'id': c.id, 'cod_contrato': c.cod_contrato}
        for c in fornecedor.contratos
    ]
    return jsonify(contratos)


# =============================================================================
# API: Naturezas filtradas por Ação (para selects dinâmicos)
# =============================================================================
@prestacoes_contratos_bp.route('/api/naturezas-por-acao/<acao>')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def api_naturezas_por_acao(acao):
    """Retorna naturezas disponíveis na LOA para uma dada ação."""
    try:
        sql = text("""
            SELECT DISTINCT l.codNatureza, n.titulo
            FROM loa_2026 l
            LEFT JOIN natdespesas n ON CAST(l.codNatureza AS CHAR) = CAST(n.codigo AS CHAR)
            WHERE l.codAcao = :acao
              AND l.codNatureza IS NOT NULL
            ORDER BY n.titulo, l.codNatureza
        """)
        rows = db.session.execute(sql, {"acao": acao}).fetchall()
        return jsonify([
            {"codigo": r[0], "descricao": r[1] or ""}
            for r in rows if r[0]
        ])
    except Exception:
        return jsonify([])


@prestacoes_contratos_bp.route('/api/fontes-por-acao-natureza/<acao>/<natureza>')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def api_fontes_por_acao_natureza(acao, natureza):
    """Retorna fontes disponíveis na LOA para uma dada ação+natureza."""
    try:
        sql = text("""
            SELECT DISTINCT l.codFonte, f.descricao
            FROM loa_2026 l
            LEFT JOIN class_fonte f ON CAST(l.codFonte AS CHAR) = CAST(f.codigo AS CHAR)
            WHERE l.codAcao = :acao
              AND l.codNatureza = :natureza
              AND l.codFonte IS NOT NULL
            ORDER BY f.descricao, l.codFonte
        """)
        rows = db.session.execute(sql, {"acao": acao, "natureza": natureza}).fetchall()
        return jsonify([
            {"codigo": r[0], "descricao": r[1] or ""}
            for r in rows if r[0]
        ])
    except Exception:
        return jsonify([])


# =============================================================================
# Helpers: listas para selects de Ação, Natureza, Fonte
# =============================================================================
def _listar_acoes_select():
    """Lista ações da tabela acao, ordenadas por descrição."""
    try:
        rows = db.session.execute(text("SELECT codigo, titulo FROM acao WHERE codigo IS NOT NULL ORDER BY titulo")).fetchall()
        return [type('Obj', (), {'codigo': r[0], 'descricao': r[1] or ''})() for r in rows if r[0]]
    except Exception:
        return []


def _listar_naturezas_select():
    """Lista naturezas da tabela natdespesas, ordenadas por descrição."""
    try:
        rows = db.session.execute(text("SELECT codigo, titulo FROM natdespesas WHERE codigo IS NOT NULL ORDER BY titulo")).fetchall()
        return [type('Obj', (), {'codigo': r[0], 'descricao': r[1] or ''})() for r in rows if r[0]]
    except Exception:
        return []


def _listar_fontes_select():
    """Lista fontes da tabela class_fonte, ordenadas por descrição."""
    try:
        rows = db.session.execute(text("SELECT codigo, descricao FROM class_fonte WHERE codigo IS NOT NULL ORDER BY descricao")).fetchall()
        return [type('Obj', (), {'codigo': r[0], 'descricao': r[1] or ''})() for r in rows if r[0]]
    except Exception:
        return []


# =============================================================================
# FORNECEDORES: Editar
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria/fornecedores/<int:id>/editar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def eo_fornecedor_editar(id):
    fornecedor = FornecedorSemContrato.query.get_or_404(id)
    descricao = request.form.get('descricao', '').strip()
    cnpj = request.form.get('cnpj', '').strip()
    telefone = request.form.get('telefone', '').strip()

    if not descricao:
        flash('Descrição é obrigatória.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    if not cnpj:
        flash('CNPJ é obrigatório.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    if not _validar_cnpj(cnpj):
        flash('CNPJ inválido. Verifique os dígitos.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))

    fornecedor.descricao = descricao
    fornecedor.cnpj = cnpj
    fornecedor.telefone = telefone or None
    db.session.commit()

    flash(f'Fornecedor "{descricao}" atualizado com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='fornecedores'))


# =============================================================================
# EXECUCOES: Editar
# =============================================================================
@prestacoes_contratos_bp.route('/exec-orcamentaria/execucoes/<int:id>/editar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def eo_execucao_editar(id):
    execucao = ExecucaoOrcamentaria.query.get_or_404(id)

    fornecedor_id = request.form.get('fornecedor_id', type=int)
    descricao = request.form.get('descricao', '').strip()
    item = request.form.get('item', '').strip()
    quantidade = request.form.get('quantidade', '').strip()
    valor_str = request.form.get('valor', '').strip()
    competencia = request.form.get('competencia', '').strip()
    acao = request.form.get('acao', '').strip()
    natureza = request.form.get('natureza', '').strip()
    fonte = request.form.get('fonte', '').strip()

    if not fornecedor_id:
        flash('Fornecedor é obrigatório.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    if not descricao:
        flash('Descrição é obrigatória.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    fornecedor = FornecedorSemContrato.query.get(fornecedor_id)
    if not fornecedor:
        flash('Fornecedor não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    qtd = None
    if quantidade:
        try:
            qtd = float(quantidade.replace('.', '').replace(',', '.'))
        except ValueError:
            flash('Quantidade inválida.', 'danger')
            return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    val = None
    if valor_str:
        try:
            val = float(valor_str.replace('.', '').replace(',', '.'))
        except ValueError:
            flash('Valor inválido.', 'danger')
            return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))

    execucao.fornecedor_id = fornecedor_id
    execucao.descricao = descricao
    execucao.item = item or None
    execucao.quantidade = qtd
    execucao.valor = val
    execucao.competencia = competencia or None
    execucao.acao = acao or None
    execucao.natureza = natureza or None
    execucao.fonte = fonte or None
    db.session.commit()

    flash('Execução atualizada com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.exec_orcamentaria', aba='execucoes'))
