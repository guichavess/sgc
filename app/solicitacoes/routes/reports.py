"""
Rotas de Relatórios (central de impressão, auditoria e exportação).
"""
from flask import render_template, request, Response, redirect, url_for
from flask_login import login_required
from datetime import datetime
import csv
import io

from app.solicitacoes.routes import solicitacoes_bp
from app.services import ReportService
from app.services import painel_pagamentos_service as painel_service
from app.models import TipoPagamento
from app.utils.permissions import requires_permission, requires_admin


@solicitacoes_bp.route('/relatorios')
@login_required
@requires_permission('solicitacoes.visualizar')
def relatorios():
    """Central de relatórios. Links antigos (com `aba_ativa`) vão para o Painel."""
    if 'aba_ativa' in request.args:
        params = request.args.to_dict(flat=False)
        params['aba'] = params.pop('aba_ativa')
        return redirect(url_for('solicitacoes.painel', **params))

    return render_template(
        'solicitacoes/relatorios.html',
        todas_competencias=ReportService.listar_competencias(),
        todos_tipos=TipoPagamento.query.order_by(TipoPagamento.id).all(),
    )


@solicitacoes_bp.route('/relatorios/imprimir')
@login_required
@requires_permission('solicitacoes.visualizar')
def relatorios_imprimir():
    """Versão para impressão dos relatórios."""
    aba_ativa = request.args.get('aba_ativa', 'geral')
    filtros = painel_service.ler_filtros(request.args)

    dados_render = {}

    if aba_ativa == 'geral':
        dados_render['resumo'] = painel_service.resumo_por_fase(filtros)
        dados_render['detalhes'] = painel_service.listar_detalhes(filtros)
        dados_render['contagem_etapa'] = painel_service.contagem_por_etapa(filtros)
        dados_render['titulo'] = "Relatório Geral de Processos"

    elif aba_ativa == 'metricas':
        timeline_medias, matriz_tempos, colunas_etapas, _ = painel_service.dados_metricas(
            filtros, page_matriz=1, per_page=9999
        )
        dados_render['timeline_medias'] = timeline_medias
        dados_render['matriz_tempos'] = matriz_tempos
        dados_render['colunas_etapas'] = colunas_etapas
        dados_render['titulo'] = "Relatório de Métricas e Performance"

    return render_template(
        'solicitacoes/relatorios_impressao.html',
        **dados_render,
        aba_ativa=aba_ativa,
        agora=datetime.now(),
        timestamps=ReportService.obter_timestamps_atualizacao(),
    )


# =============================================================================
# AUDITORIA DE DADOS
# =============================================================================

@solicitacoes_bp.route('/relatorios/auditoria')
@login_required
@requires_admin
def relatorios_auditoria():
    """Página de auditoria de dados — identifica informações faltantes."""
    resultados, contadores = painel_service.dados_auditoria()

    # Extrair listas distintas para os dropdowns (antes de filtrar)
    todas_competencias = sorted({r['solicitacao'].competencia for r in resultados
                                  if r['solicitacao'].competencia})
    todos_contratados = sorted({r['contratado'] for r in resultados if r['contratado']})
    todas_etapas = sorted({r['etapa_nome'] for r in resultados if r['etapa_nome']})
    todos_status = sorted({r['solicitacao'].status_geral for r in resultados
                           if r['solicitacao'].status_geral and r['solicitacao'].status_geral.strip()})

    # Ler filtros da query string (multi-select via getlist)
    f_competencias = request.args.getlist('filtro_competencia')
    f_contratados = request.args.getlist('filtro_contratado')
    f_etapas = request.args.getlist('filtro_etapa')
    f_status = request.args.getlist('filtro_status')
    f_docs = request.args.getlist('filtro_docs')       # sem_ne, sem_nl, sem_pd, sem_ob
    f_pendencia = request.args.getlist('filtro_pendencia')  # completos, incompletos

    # Aplicar filtros
    if f_competencias:
        resultados = [r for r in resultados
                      if r['solicitacao'].competencia in f_competencias]
    if f_contratados:
        resultados = [r for r in resultados
                      if r['contratado'] in f_contratados]
    if f_etapas:
        resultados = [r for r in resultados
                      if r['etapa_nome'] in f_etapas]
    if f_status:
        resultados = [r for r in resultados
                      if r['solicitacao'].status_geral in f_status]

    # Filtro documentos ausentes (AND — todos os selecionados devem faltar)
    mapa_docs = {
        'sem_ne': 'tem_ne', 'sem_nl': 'tem_nl',
        'sem_pd': 'tem_pd', 'sem_ob': 'tem_ob',
    }
    for doc_key in f_docs:
        flag = mapa_docs.get(doc_key)
        if flag:
            resultados = [r for r in resultados if not r[flag]]

    # Filtro pendência
    if f_pendencia:
        filtered = []
        for r in resultados:
            if 'completos' in f_pendencia and r['qtd_faltando'] == 0:
                filtered.append(r)
            elif 'incompletos' in f_pendencia and r['qtd_faltando'] > 0:
                filtered.append(r)
        resultados = filtered

    tem_filtros = any([f_competencias, f_contratados, f_etapas, f_status, f_docs, f_pendencia])

    return render_template(
        'solicitacoes/auditoria.html',
        resultados=resultados,
        contadores=contadores,
        todas_competencias=todas_competencias,
        todos_contratados=todos_contratados,
        todas_etapas=todas_etapas,
        todos_status=todos_status,
        tem_filtros=tem_filtros,
    )


@solicitacoes_bp.route('/relatorios/auditoria/csv')
@login_required
@requires_admin
def relatorios_auditoria_csv():
    """Download CSV da auditoria de dados."""
    resultados, _ = painel_service.dados_auditoria()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')

    # Cabeçalho
    writer.writerow([
        'ID', 'Protocolo SEI', 'Competência', 'Código Contrato', 'Contratado',
        'Etapa Atual', 'Data Solicitação', 'Tipo Pagamento',
        'Status Empenho', 'Valor Empenho', 'NE (Empenho)', 'NE (SEI)', 'NL', 'PD', 'OB',
        'Link SEI', 'ID Procedimento SEI', 'Tempo Total', 'Status Geral',
        'Qtd Histórico', 'Qtd Campos Faltando', 'Campos Faltando'
    ])

    for r in resultados:
        sol = r['solicitacao']
        writer.writerow([
            sol.id,
            sol.protocolo_gerado_sei or '',
            sol.competencia or '',
            sol.codigo_contrato or '',
            r['contratado'],
            r['etapa_nome'],
            sol.data_solicitacao.strftime('%d/%m/%Y %H:%M') if sol.data_solicitacao else '',
            sol.tipo_pagamento.nome if sol.tipo_pagamento else '',
            r['status_empenho_nome'],
            f"{r['empenho_valor']:.2f}" if r['empenho_valor'] else '',
            r['empenho_ne'] or '',
            r['mov_ne'].numero if r['mov_ne'] else (sol.num_ne or ''),
            r['mov_nl'].numero if r['mov_nl'] else (sol.num_nl or ''),
            r['mov_pd'].numero if r['mov_pd'] else (sol.num_pd or ''),
            r['mov_ob'].numero if r['mov_ob'] else (sol.num_ob or ''),
            sol.link_processo_sei or '',
            sol.id_procedimento_sei or '',
            sol.tempo_total or '',
            sol.status_geral or '',
            r['qtd_historico'],
            r['qtd_faltando'],
            ' | '.join(r['faltando']) if r['faltando'] else 'Completo'
        ])

    output.seek(0)
    agora = datetime.now().strftime('%Y%m%d_%H%M')
    return Response(
        '﻿' + output.getvalue(),  # BOM para Excel
        mimetype='text/csv; charset=utf-8',
        headers={
            'Content-Disposition': f'attachment; filename=auditoria_pagamentos_{agora}.csv'
        }
    )
