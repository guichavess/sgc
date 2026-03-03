"""
Rotas de Execuções - Registro e listagem de execuções de contratos.
"""
from flask import render_template, request, redirect, url_for, flash, session
from flask_login import login_required, current_user

from app.prestacoes_contratos.routes import prestacoes_contratos_bp
from app.services.prestacao_contrato_service import PrestacaoContratoService
from app.utils.permissions import requires_permission


@prestacoes_contratos_bp.route('/prestacoes')
@login_required
@requires_permission('prestacoes_contratos.visualizar')
def prestacoes_index():
    """Lista todas as execuções realizadas com paginação."""
    page = request.args.get('page', 1, type=int)
    per_page = 20

    pagination = PrestacaoContratoService.listar_prestacoes_paginado(
        page=page, per_page=per_page
    )

    execucoes = []
    for prestacao, contrato_objeto, nome_criador in pagination.items:
        execucoes.append({
            'codigo_contrato': prestacao.codigo_contrato,
            'tipo': prestacao.tipo,
            'tipo_display': prestacao.tipo_display,
            'item_descricao': prestacao.item_descricao,
            'data': prestacao.data,
            'quantidade': prestacao.quantidade,
            'valor': prestacao.valor,
            'valor_formatado': prestacao.valor_formatado,
            'valor_total': prestacao.valor_total,
            'valor_total_formatado': prestacao.valor_total_formatado,
            'nome_criador': nome_criador or 'N/A'
        })

    return render_template(
        'prestacoes_contratos/prestacoes/index.html',
        execucoes=execucoes,
        pagination=pagination
    )


@prestacoes_contratos_bp.route('/contratos/<codigo>/execucoes/selecionar-itens')
@login_required
@requires_permission('prestacoes_contratos.criar')
def execucoes_selecionar_itens(codigo):
    """Página para selecionar itens antes de registrar execuções."""
    contrato = PrestacaoContratoService.buscar_contrato(codigo)
    if not contrato:
        flash('Contrato não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    # Verifica se contrato está tipificado
    if not contrato.esta_tipificado:
        flash('Tipifique o contrato antes de adicionar execuções.', 'warning')
        return redirect(url_for('prestacoes_contratos.contrato_gerenciar',
                                codigo=codigo, aba='tipificacao'))

    # Carrega itens VINCULADOS filtrados pela tipificação do contrato
    itens_servicos, itens_materiais, itens_ocultados = \
        PrestacaoContratoService.listar_itens_para_execucao(contrato)

    # Se não há itens vinculados compatíveis, redirecionar para aba itens
    if not itens_servicos and not itens_materiais:
        flash('Nenhum item vinculado compatível com a tipificação deste contrato. '
              'Vincule itens na aba Itens antes de criar execuções.', 'warning')
        return redirect(url_for('prestacoes_contratos.contrato_gerenciar',
                                codigo=codigo, aba='itens'))

    return render_template(
        'prestacoes_contratos/prestacoes/selecionar_itens.html',
        contrato=contrato,
        itens_servicos=itens_servicos,
        itens_materiais=itens_materiais,
        itens_ocultados=itens_ocultados
    )


@prestacoes_contratos_bp.route('/contratos/<codigo>/execucoes/iniciar', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def execucoes_iniciar(codigo):
    """Inicia o fluxo de registro de execuções para os itens selecionados."""
    contrato = PrestacaoContratoService.buscar_contrato(codigo)
    if not contrato:
        flash('Contrato não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    # Os valores agora vêm no formato "S:123" ou "M:456"
    itens_selecionados = request.form.getlist('itens')

    if not itens_selecionados:
        flash('Selecione pelo menos um item.', 'warning')
        return redirect(url_for('prestacoes_contratos.execucoes_selecionar_itens', codigo=codigo))

    # Salva contrato na sessão
    session['contrato_prestacao'] = {
        'codigo': contrato.codigo,
        'objeto': contrato.objeto,
        'valor': str(contrato.valor) if contrato.valor else '0',
        'valor_formatado': contrato.valor_formatado
    }

    # Salva itens restantes na sessão (exceto o primeiro)
    if len(itens_selecionados) > 1:
        session['itens_restantes'] = itens_selecionados[1:]
    else:
        session['itens_restantes'] = []

    # Primeiro item: "S:123" -> tipo='S', id=123
    primeiro = itens_selecionados[0]
    tipo, item_id = primeiro.split(':', 1)

    flash('Registre a execução para cada item selecionado.', 'info')
    return redirect(url_for('prestacoes_contratos.prestacoes_create',
                            item_tipo=tipo, item_id=item_id))


@prestacoes_contratos_bp.route('/prestacoes/create/<item_tipo>/<int:item_id>')
@login_required
@requires_permission('prestacoes_contratos.criar')
def prestacoes_create(item_tipo, item_id):
    """Formulário para registrar nova execução."""
    # Busca o item (serviço ou material)
    item = _buscar_item(item_tipo, item_id)
    if not item:
        flash('Item não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    # Busca contrato da sessão
    contrato_dados = session.get('contrato_prestacao')

    # Quantos itens restam
    itens_restantes = len(session.get('itens_restantes', []))

    return render_template(
        'prestacoes_contratos/prestacoes/create.html',
        item=item,
        contrato=contrato_dados,
        itens_restantes=itens_restantes
    )


@prestacoes_contratos_bp.route('/prestacoes/store/<item_tipo>/<int:item_id>', methods=['POST'])
@login_required
@requires_permission('prestacoes_contratos.criar')
def prestacoes_store(item_tipo, item_id):
    """Salva nova execução."""
    # Valida o item
    item = _buscar_item(item_tipo, item_id)
    if not item:
        flash('Item não encontrado.', 'danger')
        return redirect(url_for('prestacoes_contratos.dashboard'))

    codigo_contrato = request.form.get('codigo_contrato')
    quantidade = request.form.get('quantidade', type=int)
    valor_str = request.form.get('valor')
    data = request.form.get('data')

    try:
        PrestacaoContratoService.criar_prestacao(
            tipo=item_tipo,
            codigo_contrato=codigo_contrato,
            quantidade=quantidade,
            valor_str=valor_str,
            data=data,
            usuario_id=current_user.id,
            catserv_servico_id=item_id if item_tipo == 'S' else None,
            catmat_item_id=item_id if item_tipo == 'M' else None
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('prestacoes_contratos.prestacoes_create',
                                item_tipo=item_tipo, item_id=item_id))

    # Verifica se há mais itens para registrar
    itens_restantes = session.get('itens_restantes', [])

    if itens_restantes:
        proximo = itens_restantes.pop(0)
        session['itens_restantes'] = itens_restantes
        prox_tipo, prox_id = proximo.split(':', 1)
        flash('Execução registrada. Continue com o próximo item.', 'success')
        return redirect(url_for('prestacoes_contratos.prestacoes_create',
                                item_tipo=prox_tipo, item_id=prox_id))

    # Recupera código do contrato antes de limpar sessão
    contrato_dados = session.get('contrato_prestacao')
    codigo_retorno = contrato_dados.get('codigo') if contrato_dados else codigo_contrato

    # Limpa dados de sessão do fluxo
    session.pop('contrato_prestacao', None)
    session.pop('itens_restantes', None)

    flash('Execução registrada com sucesso!', 'success')

    # Retorna para a página de execuções do contrato
    if codigo_retorno:
        return redirect(url_for('prestacoes_contratos.contrato_execucoes',
                                codigo=codigo_retorno))

    return redirect(url_for('prestacoes_contratos.prestacoes_index'))


def _buscar_item(tipo, item_id):
    """
    Busca um item (serviço ou material) e retorna um dict normalizado.

    Returns:
        dict com 'id', 'nome', 'tipo' ou None se não encontrado.
    """
    from app.extensions import db

    if tipo == 'S':
        from app.models.catserv import CatservServico
        servico = db.session.get(CatservServico, item_id)
        if servico:
            return {'id': servico.codigo_servico, 'nome': servico.nome, 'tipo': 'S'}
    elif tipo == 'M':
        from app.models.catmat import CatmatItem
        item = db.session.get(CatmatItem, item_id)
        if item:
            return {'id': item.id, 'nome': item.descricao, 'tipo': 'M'}

    return None
