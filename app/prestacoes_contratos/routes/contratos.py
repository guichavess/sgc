"""
Rotas de Contratos - Listagem, detalhes e gerenciamento de contratos.
"""
from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from app.prestacoes_contratos.routes import prestacoes_contratos_bp
from app.services.prestacao_contrato_service import PrestacaoContratoService
from app.utils.permissions import requires_permission


@prestacoes_contratos_bp.route('/')
@prestacoes_contratos_bp.route('/contratos')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def dashboard():
    """Página principal - Lista de contratos com paginação e filtros."""
    # Parâmetros de filtro
    filtro_codigo = request.args.get('codigo', '').strip()
    filtro_contratado = request.args.get('contratado', '').strip()
    filtro_situacao = request.args.get('situacao', '').strip()
    filtro_natureza = request.args.get('natureza', type=int) or None
    filtro_tipo_execucao = request.args.get('tipo_execucao', type=int) or None
    filtro_centro_custo = request.args.get('centro_custo', type=int) or None
    filtro_tipo_contrato = request.args.get('tipo_contrato', '').strip()
    filtro_pdm = request.args.get('pdm', type=int) or None
    filtro_subitem = request.args.get('subitem_despesa', '').strip()
    filtro_tipo_patrimonial = request.args.get('tipo_patrimonial', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 20

    # Busca paginada com filtros
    pagination = PrestacaoContratoService.listar_contratos_paginado(
        codigo=filtro_codigo or None,
        contratado=filtro_contratado or None,
        situacao=filtro_situacao or None,
        natureza_codigo=filtro_natureza,
        tipo_execucao_id=filtro_tipo_execucao,
        centro_de_custo_id=filtro_centro_custo,
        tipo_contrato=filtro_tipo_contrato or None,
        pdm_id=filtro_pdm,
        subitem_despesa=filtro_subitem or None,
        tipo_patrimonial=filtro_tipo_patrimonial or None,
        page=page,
        per_page=per_page
    )

    # Buscar naturezas múltiplas para todos os contratos da página
    codigos_pagina = [c.codigo for c in pagination.items]
    mapa_naturezas = PrestacaoContratoService.buscar_naturezas_por_contratos(codigos_pagina)

    # Buscar classificadores (SubItem + TipoPatrimonial) para contratos da página
    mapa_classificadores = PrestacaoContratoService.buscar_classificadores_por_contratos(codigos_pagina)

    # Monta lista de contratos a partir dos resultados
    contratos = []
    for contrato in pagination.items:
        classif = mapa_classificadores.get(contrato.codigo, {})
        contratos.append({
            'codigo': contrato.codigo,
            'numeroOriginal': contrato.numeroOriginal or '',
            'nomeContratado': contrato.nomeContratado or '',
            'objeto': contrato.objeto,
            'situacao': contrato.situacao,
            'naturezas': mapa_naturezas.get(contrato.codigo, []),
            'tipo_execucao': contrato.tipo_execucao.descricao if contrato.tipo_execucao else None,
            'centro_de_custo': contrato.centro_de_custo.descricao if contrato.centro_de_custo else None,
            'tipo_contrato': contrato.tipo_contrato_display,
            'valor': contrato.valor,
            'valor_formatado': contrato.valor_formatado,
            'subitens_despesa': classif.get('subitens', []),
            'tipos_patrimoniais': classif.get('tipos_patrimoniais', [])
        })

    # Dados para os selects de filtro
    todas_situacoes = PrestacaoContratoService.listar_situacoes()
    todos_tipos_execucao = PrestacaoContratoService.listar_tipos_execucao()
    todos_centros_custo = PrestacaoContratoService.listar_centros_de_custo()
    todos_pdms = PrestacaoContratoService.listar_pdms_utilizados()
    todas_naturezas = PrestacaoContratoService.listar_naturezas_utilizadas()
    todos_subitens = PrestacaoContratoService.listar_subitens_utilizados()
    todos_tipos_patrimoniais = PrestacaoContratoService.listar_tipos_patrimoniais_utilizados()

    # Verifica se algum filtro está ativo
    tem_filtro = any([filtro_codigo, filtro_contratado, filtro_situacao,
                      filtro_natureza, filtro_tipo_execucao, filtro_centro_custo,
                      filtro_tipo_contrato, filtro_pdm, filtro_subitem,
                      filtro_tipo_patrimonial])

    return render_template(
        'prestacoes_contratos/contratos/index.html',
        contratos=contratos,
        pagination=pagination,
        todas_situacoes=todas_situacoes,
        todos_tipos_execucao=todos_tipos_execucao,
        todos_centros_custo=todos_centros_custo,
        todos_pdms=todos_pdms,
        todas_naturezas=todas_naturezas,
        todos_subitens=todos_subitens,
        todos_tipos_patrimoniais=todos_tipos_patrimoniais,
        filtro_codigo=filtro_codigo,
        filtro_contratado=filtro_contratado,
        filtro_situacao=filtro_situacao,
        filtro_natureza=filtro_natureza,
        filtro_tipo_execucao=filtro_tipo_execucao,
        filtro_centro_custo=filtro_centro_custo,
        filtro_tipo_contrato=filtro_tipo_contrato,
        filtro_pdm=filtro_pdm,
        filtro_subitem=filtro_subitem,
        filtro_tipo_patrimonial=filtro_tipo_patrimonial,
        tem_filtro=tem_filtro
    )


@prestacoes_contratos_bp.route('/contratos/<codigo>/execucoes')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def contrato_execucoes(codigo):
    """Página de execuções do contrato."""
    contrato = PrestacaoContratoService.buscar_contrato(codigo)
    if not contrato:
        flash('Contrato não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    # Saldo do contrato
    saldo = PrestacaoContratoService.buscar_saldo(codigo)

    return render_template(
        'prestacoes_contratos/contratos/execucoes.html',
        contrato=contrato,
        saldo=saldo
    )


@prestacoes_contratos_bp.route('/contratos/<codigo>/gerenciar')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def contrato_gerenciar(codigo):
    """Página de gerenciamento do contrato com abas."""
    contrato = PrestacaoContratoService.buscar_contrato(codigo)
    if not contrato:
        flash('Contrato não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    # Aba ativa (default: tipificacao)
    aba = request.args.get('aba', 'tipificacao')
    sub_aba = request.args.get('sub_aba', 'empenhos')
    sub_aba_tipo = request.args.get('sub_aba_tipo', 'categorizacao')

    # Dados da tipificação
    tipificacao = PrestacaoContratoService.obter_tipificacao(codigo)

    # Dados de Centro de Custo e Tipo de Execução (para sub-abas de Tipo-Contrato)
    centros_de_custo = []
    tipos_execucao = []
    detalhamento_financeiro = None
    if aba == 'tipificacao':
        centros_de_custo = PrestacaoContratoService.listar_centros_de_custo()
        tipos_execucao = PrestacaoContratoService.listar_tipos_execucao()
        if sub_aba_tipo == 'detalhamento':
            detalhamento_financeiro = PrestacaoContratoService.obter_detalhamento_financeiro(codigo)

    # Itens vinculados ao contrato (para aba itens)
    itens_vinculados = []
    itens_catalogo = []
    if aba == 'itens' and contrato.esta_tipificado:
        itens_vinculados = PrestacaoContratoService.listar_itens_vinculados(contrato.codigo)
        itens_catalogo = PrestacaoContratoService.listar_catalogo_para_vincular(contrato)

    # Saldo do contrato
    saldo = PrestacaoContratoService.buscar_saldo(codigo)
    divisao_saldo = []
    itens_vinculados_saldo = []
    if aba == 'saldo' and saldo:
        divisao_saldo = PrestacaoContratoService.buscar_divisao_saldo(saldo.id)
        itens_vinculados_saldo = PrestacaoContratoService.listar_itens_vinculados(codigo)

    # Classificadores (Tipo de despesa + Sub item) do contrato
    mapa_classif = PrestacaoContratoService.buscar_classificadores_por_contratos([codigo])
    classificadores = mapa_classif.get(codigo, {})

    # Naturezas do contrato (todas, via empenho_itens)
    mapa_naturezas = PrestacaoContratoService.buscar_naturezas_por_contratos([codigo])
    naturezas_contrato = mapa_naturezas.get(codigo, [])

    # Totais financeiros (empenho, liquidação, pagamento)
    totais_financeiros = PrestacaoContratoService.obter_totais_financeiros(codigo)

    # Contagens para ícones de status nas abas
    from app.models.contrato_aditivo import ContratoAditivo
    from app.extensions import db as _db
    qtd_itens_vinculados = _db.session.execute(
        _db.text("SELECT COUNT(*) FROM itens_vinculados WHERE codigo_contrato = :cod"),
        {'cod': str(codigo)}
    ).scalar() or 0
    qtd_aditivos = ContratoAditivo.query.filter_by(codigo_contrato=str(codigo)).count()
    tem_empenhos = totais_financeiros['geral']['empenho'] > 0

    # Aditivos do contrato
    aditivos = []
    if aba == 'aditivos':
        aditivos = PrestacaoContratoService.listar_aditivos(codigo)

    # Dados financeiros
    empenhos = []
    liquidacoes = []
    pagamentos = []
    pds = []
    if aba == 'financeiro':
        empenhos = PrestacaoContratoService.listar_empenhos(codigo)
        liquidacoes = PrestacaoContratoService.listar_liquidacoes(codigo)
        pagamentos = PrestacaoContratoService.listar_pagamentos_contrato(codigo)
        pds = PrestacaoContratoService.listar_pds(codigo)

    return render_template(
        'prestacoes_contratos/contratos/gerenciar.html',
        contrato=contrato,
        aba=aba,
        sub_aba=sub_aba,
        sub_aba_tipo=sub_aba_tipo,
        tipificacao=tipificacao,
        centros_de_custo=centros_de_custo,
        tipos_execucao=tipos_execucao,
        itens_vinculados=itens_vinculados,
        itens_catalogo=itens_catalogo,
        saldo=saldo,
        classificadores=classificadores,
        totais_financeiros=totais_financeiros,
        qtd_itens_vinculados=qtd_itens_vinculados,
        qtd_aditivos=qtd_aditivos,
        tem_empenhos=tem_empenhos,
        aditivos=aditivos,
        empenhos=empenhos,
        liquidacoes=liquidacoes,
        pagamentos=pagamentos,
        pds=pds,
        detalhamento_financeiro=detalhamento_financeiro,
        naturezas_contrato=naturezas_contrato,
        divisao_saldo=divisao_saldo,
        itens_vinculados_saldo=itens_vinculados_saldo
    )


@prestacoes_contratos_bp.route('/contratos/<codigo>/tipificar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def contrato_tipificar_salvar(codigo):
    """Salva a tipificação do contrato.
    CATSERV: até Classe / CATMAT: até PDM.
    """
    contrato = PrestacaoContratoService.buscar_contrato(codigo)
    if not contrato:
        flash('Contrato não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    catserv_classe_id = request.form.get('catserv_classe_id', type=int)
    catmat_classe_id = request.form.get('catmat_classe_id', type=int)
    catmat_pdm_id = request.form.get('catmat_pdm_id', type=int)

    try:
        PrestacaoContratoService.tipificar_contrato(
            codigo_contrato=codigo,
            catserv_classe_id=catserv_classe_id,
            catmat_classe_id=catmat_classe_id,
            catmat_pdm_id=catmat_pdm_id,
            usuario_id=current_user.id
        )
        flash('Tipificação salva com sucesso!', 'success')
    except ValueError as e:
        flash(str(e), 'danger')

    return redirect(url_for('prestacoes_contratos.contrato_gerenciar',
                            codigo=codigo, aba='tipificacao'))


@prestacoes_contratos_bp.route('/contratos/<codigo>/saldo/criar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def contrato_saldo_criar(codigo):
    """Cria saldo para o contrato."""
    PrestacaoContratoService.criar_saldo(
        codigo_contrato=codigo,
        saldo_global=request.form.get('saldo_global'),
        data_inicio=request.form.get('data_inicio'),
        usuario_id=current_user.id
    )
    flash('Saldo adicionado com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.contrato_gerenciar', codigo=codigo, aba='saldo'))


@prestacoes_contratos_bp.route('/contratos/<codigo>/saldo/<int:saldo_id>/editar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def contrato_saldo_editar(codigo, saldo_id):
    """Edita saldo do contrato."""
    PrestacaoContratoService.atualizar_saldo(
        saldo_id=saldo_id,
        saldo_global=request.form.get('saldo_global'),
        data_inicio=request.form.get('data_inicio'),
        usuario_id=current_user.id
    )
    flash('Saldo atualizado com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.contrato_gerenciar', codigo=codigo, aba='saldo'))


@prestacoes_contratos_bp.route('/contratos/<codigo>/saldo/<int:saldo_id>/dividir', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def contrato_saldo_dividir(codigo, saldo_id):
    """Salva a divisão do saldo global por itens vinculados."""
    itens_valores = {}
    for key, val in request.form.items():
        if key.startswith('item_'):
            try:
                item_id = int(key.replace('item_', ''))
                valor = PrestacaoContratoService._converter_valor_br(val)
                if valor > 0:
                    itens_valores[item_id] = valor
            except (ValueError, TypeError):
                pass
    PrestacaoContratoService.salvar_divisao_saldo(saldo_id, itens_valores)
    flash('Divisão do saldo salva com sucesso!', 'success')
    return redirect(url_for('prestacoes_contratos.contrato_gerenciar', codigo=codigo, aba='saldo'))


    # Rotas de criação manual de empenho/liquidação/pagamento removidas.
    # Os dados financeiros agora vêm automaticamente das tabelas SIAFE
    # (empenho, liquidacao, ob) via codContrato.


@prestacoes_contratos_bp.route('/contratos/<codigo>/centro-custo/salvar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def contrato_centro_custo_salvar(codigo):
    """Salva o centro de custo do contrato."""
    centro_de_custo_id = request.form.get('centro_de_custo_id', type=int)
    try:
        PrestacaoContratoService.salvar_centro_de_custo(codigo, centro_de_custo_id)
        flash('Centro de custo salvo com sucesso!', 'success')
    except ValueError as e:
        flash(str(e), 'danger')
    return redirect(url_for('prestacoes_contratos.contrato_gerenciar',
                            codigo=codigo, aba='tipificacao', sub_aba_tipo='centro_custo'))


@prestacoes_contratos_bp.route('/contratos/<codigo>/tipo-execucao/salvar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.editar')
def contrato_tipo_execucao_salvar(codigo):
    """Salva o tipo de execução do contrato."""
    tipo_execucao_id = request.form.get('tipo_execucao_id', type=int)
    try:
        PrestacaoContratoService.salvar_tipo_execucao(codigo, tipo_execucao_id)
        flash('Tipo de execução salvo com sucesso!', 'success')
    except ValueError as e:
        flash(str(e), 'danger')
    return redirect(url_for('prestacoes_contratos.contrato_gerenciar',
                            codigo=codigo, aba='tipificacao', sub_aba_tipo='tipo_execucao'))
