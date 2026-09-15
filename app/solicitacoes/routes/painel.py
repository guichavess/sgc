"""
Rotas do Painel de Pagamentos (indicadores) e sua API JSON.
"""
from flask import render_template, request, jsonify, abort, url_for
from flask_login import login_required

from app.solicitacoes.routes import solicitacoes_bp
from app.services import ReportService
from app.services import painel_pagamentos_service as painel_service
from app.models import TipoPagamento
from app.utils.permissions import requires_permission

ABAS_PAINEL = ('geral', 'metricas')


@solicitacoes_bp.route('/painel')
@login_required
@requires_permission('solicitacoes.visualizar')
def painel():
    """Painel com abas Visão Geral e Métricas de Tempo (só a aba ativa é calculada)."""
    aba = request.args.get('aba', 'geral')
    if aba not in ABAS_PAINEL:
        aba = 'geral'
    filtros = painel_service.ler_filtros(request.args)
    page_estoque = max(1, request.args.get('page_estoque', 1, type=int) or 1)
    page_matriz = max(1, request.args.get('page_matriz', 1, type=int) or 1)

    resumo = detalhes = pagination = contagem_etapa = None
    timeline_medias = matriz_tempos = colunas_etapas = pagination_matriz = None

    if aba == 'geral':
        resumo, detalhes, pagination, contagem_etapa = painel_service.dados_visao_geral(
            filtros, page_estoque
        )
    else:
        timeline_medias, matriz_tempos, colunas_etapas, pagination_matriz = painel_service.dados_metricas(
            filtros, page_matriz
        )

    # Impressão: mesmos filtros, sem paginação, aba na nomenclatura da rota de impressão
    params_impressao = request.args.to_dict(flat=False)
    for chave in ('aba', 'page_estoque', 'page_matriz'):
        params_impressao.pop(chave, None)
    params_impressao['aba_ativa'] = aba

    return render_template(
        'solicitacoes/painel.html',
        aba=aba,
        url_impressao=url_for('solicitacoes.relatorios_imprimir', **params_impressao),
        resumo=resumo or [],
        detalhes=detalhes or [],
        pagination=pagination,
        contagem_etapa=contagem_etapa or {},
        timeline_medias=timeline_medias or [],
        matriz_tempos=matriz_tempos or [],
        colunas_etapas=colunas_etapas or [],
        pagination_matriz=pagination_matriz,
        todas_competencias=ReportService.listar_competencias(),
        todos_tipos=TipoPagamento.query.order_by(TipoPagamento.id).all(),
        filtro_tipos=filtros.tipo_pagamento_ids or [],
    )


@solicitacoes_bp.route('/api/painel/<indicador>')
@login_required
@requires_permission('solicitacoes.visualizar')
def api_painel_indicador(indicador):
    """Dados JSON de um indicador do Painel, com os mesmos filtros da tela."""
    gerar = painel_service.INDICADORES.get(indicador)
    if gerar is None:
        abort(404)
    return jsonify(gerar(painel_service.ler_filtros(request.args)))
