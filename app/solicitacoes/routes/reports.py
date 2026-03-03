"""
Rotas de Relatórios.
"""
from flask import render_template, request
from flask_login import login_required
from datetime import datetime, timedelta

from sqlalchemy import func

from app.extensions import db
from app.solicitacoes.routes import solicitacoes_bp
from app.services import ReportService
from app.models import Solicitacao, Contrato, Etapa, HistoricoMovimentacao, TipoPagamento
from app.constants import CHECKPOINTS_RELATORIO
from app.utils.permissions import requires_permission


def _aplicar_filtros(query_obj, filtro_competencia, filtro_contratado, data_inicio, data_fim, tipo_pagamento_ids=None):
    """Aplica filtros globais a uma query de Solicitacao já com join em Contrato."""
    if filtro_competencia:
        query_obj = query_obj.filter(Solicitacao.competencia == filtro_competencia)
    if filtro_contratado:
        query_obj = query_obj.filter(Contrato.nomeContratado.ilike(f'%{filtro_contratado}%'))
    if data_inicio:
        query_obj = query_obj.filter(Solicitacao.data_solicitacao >= data_inicio)
    if data_fim:
        query_obj = query_obj.filter(Solicitacao.data_solicitacao <= data_fim)
    if tipo_pagamento_ids:
        query_obj = query_obj.filter(Solicitacao.id_tipo_pagamento.in_(tipo_pagamento_ids))
    return query_obj


def _parse_datas(data_inicio_str, data_fim_str):
    """Converte strings de data para objetos datetime."""
    data_inicio = None
    data_fim = None
    if data_inicio_str:
        try:
            data_inicio = datetime.strptime(data_inicio_str, '%Y-%m-%d')
        except ValueError:
            pass
    if data_fim_str:
        try:
            data_fim = datetime.strptime(data_fim_str, '%Y-%m-%d')
            data_fim = data_fim.replace(hour=23, minute=59, second=59)
        except ValueError:
            pass
    return data_inicio, data_fim


def _formatar_delta_str(diff):
    """Formata um timedelta para string legível."""
    dias = diff.days
    if dias < 0:
        return "0d"
    if dias == 0 and diff.seconds > 3600:
        return "1d"
    return f"{dias}d"


def _construir_mapa_checkpoints():
    """Constrói o mapa de etapa_id -> índice de checkpoint."""
    todas_etapas_db = Etapa.query.all()
    mapa_nome_id = {e.nome.strip().lower(): e.id for e in todas_etapas_db}

    mapa_cp_idx = {}
    for idx, cp in enumerate(CHECKPOINTS_RELATORIO):
        for eid in cp['ids']:
            mapa_cp_idx[eid] = idx

    ids_adicionais = {
        'solicitação criada': 0, 'documentação solicitada': 1, 'documentação incompleta': 1,
        'documentação recebida': 2, 'documentação completa': 2, 'aguardando empenho': 2,
        'solicitação da nf': 3, 'empenho realizado': 3, 'nf com pendência': 3,
        'nf atestada': 5, 'liquidado': 6, 'pago': 6
    }
    for nome, idx in ids_adicionais.items():
        fid = mapa_nome_id.get(nome)
        if fid:
            mapa_cp_idx[fid] = idx

    return mapa_cp_idx


def _gerar_dados_visao_geral(filtro_competencia, filtro_contratado, data_inicio, data_fim, page, per_page=25, tipo_pagamento_ids=None):
    """Gera dados para a aba Visão Geral: resumo agrupado + detalhes paginados + contagem por etapa."""
    # Cards de resumo por checkpoint
    query_contagem = db.session.query(
        Solicitacao.etapa_atual_id, func.count(Solicitacao.id)
    ).join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
    query_contagem = _aplicar_filtros(query_contagem, filtro_competencia, filtro_contratado, data_inicio, data_fim, tipo_pagamento_ids)
    dados_contagem = query_contagem.group_by(Solicitacao.etapa_atual_id).all()

    mapa_contagem_bruta = {r[0]: r[1] for r in dados_contagem}

    resumo = []
    for cp in CHECKPOINTS_RELATORIO:
        qtd_total = sum(mapa_contagem_bruta.get(eid, 0) for eid in cp['ids'])
        resumo.append({
            'nome': cp['label'],
            'cor': cp.get('cor', '#6c757d'),
            'qtd': qtd_total,
            'eh_grupo': (cp['tipo'] == 'group')
        })

    # Detalhes paginados (ordenados por etapa.ordem)
    query_base = Solicitacao.query.join(Etapa).join(Contrato)
    query_base = _aplicar_filtros(query_base, filtro_competencia, filtro_contratado, data_inicio, data_fim, tipo_pagamento_ids)
    pagination = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()) \
        .paginate(page=page, per_page=per_page, error_out=False)
    detalhes = pagination.items

    # Contagem por nome de etapa (para exibir no cabeçalho do agrupamento)
    # Conta sobre TODOS os processos filtrados, não apenas a página atual
    query_contagem_etapa = db.session.query(
        Etapa.nome, func.count(Solicitacao.id)
    ).join(Solicitacao, Solicitacao.etapa_atual_id == Etapa.id) \
     .join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
    query_contagem_etapa = _aplicar_filtros(query_contagem_etapa, filtro_competencia, filtro_contratado, data_inicio, data_fim, tipo_pagamento_ids)
    contagem_etapa = dict(query_contagem_etapa.group_by(Etapa.nome).all())

    return resumo, detalhes, pagination, contagem_etapa


def _gerar_dados_metricas(filtro_competencia, filtro_contratado, data_inicio, data_fim, page_matriz, per_page=25, tipo_pagamento_ids=None):
    """Gera dados para a aba Métricas de Tempo: timeline + matriz de tempos."""
    mapa_cp_idx = _construir_mapa_checkpoints()
    agora = datetime.now()

    query_base = Solicitacao.query.join(Etapa).join(Contrato)
    query_base = _aplicar_filtros(query_base, filtro_competencia, filtro_contratado, data_inicio, data_fim, tipo_pagamento_ids)

    def calcular_tempos_processo(sol):
        hist = HistoricoMovimentacao.query.filter_by(id_solicitacao=sol.id) \
            .order_by(HistoricoMovimentacao.data_movimentacao.asc()).all()
        tempos = {}

        idx_fase_atual = mapa_cp_idx.get(sol.etapa_atual_id)
        eh_finalizado = (sol.status_geral in ['PAGO', 'CONCLUIDO', 'CANCELADO']) if sol.status_geral else False
        if idx_fase_atual == 6:
            eh_finalizado = True

        for i in range(len(hist)):
            m_atual = hist[i]
            idx_coluna = mapa_cp_idx.get(m_atual.id_etapa_nova)

            if idx_coluna is not None:
                if i < len(hist) - 1:
                    dt_saida = hist[i + 1].data_movimentacao
                else:
                    if eh_finalizado:
                        dt_saida = m_atual.data_movimentacao
                    elif idx_fase_atual is not None and idx_fase_atual != idx_coluna:
                        dt_saida = m_atual.data_movimentacao
                    else:
                        dt_saida = agora

                delta = dt_saida - m_atual.data_movimentacao
                if delta.total_seconds() < 0:
                    delta = timedelta(0)
                tempos[idx_coluna] = tempos.get(idx_coluna, timedelta()) + delta

        if idx_fase_atual is not None and idx_fase_atual not in tempos and not eh_finalizado:
            if hist:
                dt_entrada_fase = hist[-1].data_movimentacao
            else:
                dt_entrada_fase = sol.data_solicitacao or agora
            delta = agora - dt_entrada_fase
            if delta.total_seconds() < 0:
                delta = timedelta(0)
            tempos[idx_fase_atual] = delta

        return tempos

    # Timeline global (todos os processos)
    todos_processos = query_base.all()
    acumulador_medias = {i: [] for i in range(len(CHECKPOINTS_RELATORIO))}

    for sol in todos_processos:
        tempos = calcular_tempos_processo(sol)
        for idx_coluna, delta in tempos.items():
            acumulador_medias[idx_coluna].append(delta)

    timeline_medias = []
    for idx, cp in enumerate(CHECKPOINTS_RELATORIO):
        item_visual = {
            'id': idx,
            'nome': cp['label'],
            'cor': cp.get('cor', '#6c757d'),
            'eh_grupo': (cp['tipo'] == 'group')
        }

        lista_duracoes = acumulador_medias.get(idx, [])
        media_str = "--"
        qtd = 0

        if lista_duracoes:
            qtd = len(lista_duracoes)
            media = sum(lista_duracoes, timedelta()) / qtd
            media_str = _formatar_delta_str(media)

        item_visual['media_entrada'] = media_str
        item_visual['qtd_base'] = qtd
        timeline_medias.append(item_visual)

    # Matriz paginada
    pagination_matriz = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()) \
        .paginate(page=page_matriz, per_page=per_page, error_out=False)
    detalhes_matriz = pagination_matriz.items

    matriz_tempos = []
    for sol in detalhes_matriz:
        tempos = calcular_tempos_processo(sol)
        tempos_fmt = {k: _formatar_delta_str(v) for k, v in tempos.items()}
        matriz_tempos.append({'solicitacao': sol, 'tempos': tempos_fmt})

    colunas_etapas = [{'id': i, 'nome': cp['label']} for i, cp in enumerate(CHECKPOINTS_RELATORIO)]

    return timeline_medias, matriz_tempos, colunas_etapas, pagination_matriz


@solicitacoes_bp.route('/relatorios')
@login_required
@requires_permission('solicitacoes.visualizar')
def relatorios():
    """Exibe página de relatórios com múltiplas abas."""
    aba_ativa = request.args.get('aba_ativa', 'geral')
    filtro_competencia = request.args.get('competencia', '').strip()
    filtro_contratado = request.args.get('contratado', '').strip()
    data_inicio_str = request.args.get('data_inicio')
    data_fim_str = request.args.get('data_fim')
    filtro_tipos = [int(t) for t in request.args.getlist('filtro_tipo') if t.isdigit()]
    page_estoque = request.args.get('page_estoque', 1, type=int)
    page_matriz = request.args.get('page_matriz', 1, type=int)

    data_inicio, data_fim = _parse_datas(data_inicio_str, data_fim_str)

    # Visão Geral
    resumo, detalhes, pagination, contagem_etapa = _gerar_dados_visao_geral(
        filtro_competencia or None, filtro_contratado or None,
        data_inicio, data_fim, page_estoque,
        tipo_pagamento_ids=filtro_tipos or None
    )

    # Métricas
    timeline_medias, matriz_tempos, colunas_etapas, pagination_matriz = _gerar_dados_metricas(
        filtro_competencia or None, filtro_contratado or None,
        data_inicio, data_fim, page_matriz,
        tipo_pagamento_ids=filtro_tipos or None
    )

    todas_competencias = ReportService.listar_competencias()
    todos_tipos = TipoPagamento.query.order_by(TipoPagamento.id).all()

    return render_template(
        'solicitacoes/relatorios.html',
        aba_ativa=aba_ativa,
        resumo=resumo,
        detalhes=detalhes,
        pagination=pagination,
        contagem_etapa=contagem_etapa,
        timeline_medias=timeline_medias,
        matriz_tempos=matriz_tempos,
        colunas_etapas=colunas_etapas,
        pagination_matriz=pagination_matriz,
        todas_competencias=todas_competencias,
        todos_tipos=todos_tipos,
        filtro_tipos=filtro_tipos,
    )


@solicitacoes_bp.route('/relatorios/imprimir')
@login_required
@requires_permission('solicitacoes.visualizar')
def relatorios_imprimir():
    """Versão para impressão dos relatórios."""
    aba_ativa = request.args.get('aba_ativa', 'geral')
    filtro_competencia = request.args.get('competencia', '').strip()
    filtro_contratado = request.args.get('contratado', '').strip()
    data_inicio_str = request.args.get('data_inicio')
    data_fim_str = request.args.get('data_fim')
    filtro_tipos = [int(t) for t in request.args.getlist('filtro_tipo') if t.isdigit()]

    data_inicio, data_fim = _parse_datas(data_inicio_str, data_fim_str)

    timestamps = ReportService.obter_timestamps_atualizacao()

    dados_render = {}

    if aba_ativa == 'geral':
        resumo, _, _, contagem_etapa = _gerar_dados_visao_geral(
            filtro_competencia or None, filtro_contratado or None,
            data_inicio, data_fim, page=1, per_page=9999,
            tipo_pagamento_ids=filtro_tipos or None
        )

        # Busca lista completa (sem paginação) para impressão
        query_base = Solicitacao.query.join(Etapa).join(Contrato)
        query_base = _aplicar_filtros(query_base, filtro_competencia or None, filtro_contratado or None, data_inicio, data_fim, filtro_tipos or None)
        detalhes = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()).all()

        dados_render['resumo'] = resumo
        dados_render['detalhes'] = detalhes
        dados_render['contagem_etapa'] = contagem_etapa
        dados_render['titulo'] = "Relatório Geral de Processos"

    elif aba_ativa == 'metricas':
        timeline_medias, matriz_tempos, colunas_etapas, _ = _gerar_dados_metricas(
            filtro_competencia or None, filtro_contratado or None,
            data_inicio, data_fim, page_matriz=1, per_page=9999,
            tipo_pagamento_ids=filtro_tipos or None
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
        timestamps=timestamps,
    )
