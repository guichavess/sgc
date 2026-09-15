"""
Serviço do Painel de Pagamentos — indicadores do fluxo de solicitações.

Concentra filtros globais (competência, contratado, período, tipo de pagamento),
contagem por checkpoint (CHECKPOINTS_RELATORIO), tempos médios por fase e a
auditoria de dados. Usado pelas rotas do Painel, dos Relatórios (impressão/
auditoria) e pela API JSON dos gráficos.
"""
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import func

from app.extensions import db
from app.models import (
    Solicitacao, Contrato, Etapa, HistoricoMovimentacao,
    SeiMovimentacao, SolicitacaoEmpenho
)
from app.constants import CHECKPOINTS_RELATORIO, SerieDocumentoSEI


# =============================================================================
# FILTROS
# =============================================================================

@dataclass
class FiltrosPainel:
    competencia: Optional[str] = None
    contratado: Optional[str] = None
    data_inicio: Optional[datetime] = None
    data_fim: Optional[datetime] = None
    tipo_pagamento_ids: Optional[List[int]] = None


def parse_datas(data_inicio_str, data_fim_str):
    """Converte strings YYYY-MM-DD em datetime (fim do dia para data_fim)."""
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


def ler_filtros(args) -> FiltrosPainel:
    """Lê os filtros globais da query string (MultiDict de request.args)."""
    data_inicio, data_fim = parse_datas(args.get('data_inicio'), args.get('data_fim'))
    tipos = [int(t) for t in args.getlist('filtro_tipo') if t.isdigit()]
    return FiltrosPainel(
        competencia=(args.get('competencia') or '').strip() or None,
        contratado=(args.get('contratado') or '').strip() or None,
        data_inicio=data_inicio,
        data_fim=data_fim,
        tipo_pagamento_ids=tipos or None,
    )


def aplicar_filtros(query_obj, filtros: FiltrosPainel):
    """Aplica filtros globais a uma query de Solicitacao já com join em Contrato."""
    if filtros.competencia:
        query_obj = query_obj.filter(Solicitacao.competencia == filtros.competencia)
    if filtros.contratado:
        query_obj = query_obj.filter(Contrato.nomeContratado.ilike(f'%{filtros.contratado}%'))
    if filtros.data_inicio:
        query_obj = query_obj.filter(Solicitacao.data_solicitacao >= filtros.data_inicio)
    if filtros.data_fim:
        query_obj = query_obj.filter(Solicitacao.data_solicitacao <= filtros.data_fim)
    if filtros.tipo_pagamento_ids:
        query_obj = query_obj.filter(Solicitacao.id_tipo_pagamento.in_(filtros.tipo_pagamento_ids))
    return query_obj


def _query_solicitacoes(filtros: FiltrosPainel):
    query = Solicitacao.query.join(Etapa).join(Contrato)
    return aplicar_filtros(query, filtros)


# =============================================================================
# VISÃO GERAL
# =============================================================================

def formatar_delta_str(diff):
    """Formata um timedelta para string legível."""
    dias = diff.days
    if dias < 0:
        return "0d"
    if dias == 0 and diff.seconds > 3600:
        return "1d"
    return f"{dias}d"


def resumo_por_fase(filtros: FiltrosPainel):
    """Quantidade de solicitações em cada checkpoint (fase) do fluxo."""
    query_contagem = db.session.query(
        Solicitacao.etapa_atual_id, func.count(Solicitacao.id)
    ).join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
    query_contagem = aplicar_filtros(query_contagem, filtros)
    mapa_contagem_bruta = dict(query_contagem.group_by(Solicitacao.etapa_atual_id).all())

    resumo = []
    for cp in CHECKPOINTS_RELATORIO:
        resumo.append({
            'nome': cp['label'],
            'cor': cp.get('cor', '#6c757d'),
            'qtd': sum(mapa_contagem_bruta.get(eid, 0) for eid in cp['ids']),
            'eh_grupo': (cp['tipo'] == 'group'),
        })
    return resumo


def contagem_por_etapa(filtros: FiltrosPainel):
    """Quantidade por nome de etapa sobre TODOS os processos filtrados (não só a página)."""
    query_contagem_etapa = db.session.query(
        Etapa.nome, func.count(Solicitacao.id)
    ).join(Solicitacao, Solicitacao.etapa_atual_id == Etapa.id) \
     .join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
    query_contagem_etapa = aplicar_filtros(query_contagem_etapa, filtros)
    return dict(query_contagem_etapa.group_by(Etapa.nome).all())


def dados_visao_geral(filtros: FiltrosPainel, page, per_page=25):
    """Aba Visão Geral: resumo por fase + detalhes paginados + contagem por etapa."""
    pagination = _query_solicitacoes(filtros) \
        .order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()) \
        .paginate(page=page, per_page=per_page, error_out=False)

    return resumo_por_fase(filtros), pagination.items, pagination, contagem_por_etapa(filtros)


def listar_detalhes(filtros: FiltrosPainel):
    """Lista completa (sem paginação) ordenada por etapa — usada na impressão."""
    return _query_solicitacoes(filtros) \
        .order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()).all()


# =============================================================================
# MÉTRICAS DE TEMPO
# =============================================================================

def _construir_mapa_checkpoints():
    """Constrói o mapa de etapa_id -> índice de checkpoint."""
    mapa_nome_id = {e.nome.strip().lower(): e.id for e in Etapa.query.all()}

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


def dados_metricas(filtros: FiltrosPainel, page_matriz, per_page=25):
    """Aba Métricas de Tempo: fluxo médio (timeline) + matriz de tempos paginada."""
    mapa_cp_idx = _construir_mapa_checkpoints()
    agora = datetime.now()

    query_base = _query_solicitacoes(filtros)
    todos_processos = query_base.all()

    # Batch load do histórico (evita N+1)
    sol_ids = [s.id for s in todos_processos]
    hist_por_sol = defaultdict(list)
    if sol_ids:
        historicos = HistoricoMovimentacao.query.filter(
            HistoricoMovimentacao.id_solicitacao.in_(sol_ids)
        ).order_by(
            HistoricoMovimentacao.id_solicitacao,
            HistoricoMovimentacao.data_movimentacao.asc()
        ).all()
        for h in historicos:
            hist_por_sol[h.id_solicitacao].append(h)

    def calcular_tempos_processo(sol):
        hist = hist_por_sol.get(sol.id, [])
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

    acumulador_medias = {i: [] for i in range(len(CHECKPOINTS_RELATORIO))}
    for sol in todos_processos:
        for idx_coluna, delta in calcular_tempos_processo(sol).items():
            acumulador_medias[idx_coluna].append(delta)

    timeline_medias = []
    for idx, cp in enumerate(CHECKPOINTS_RELATORIO):
        lista_duracoes = acumulador_medias.get(idx, [])
        media_str = "--"
        if lista_duracoes:
            media_str = formatar_delta_str(sum(lista_duracoes, timedelta()) / len(lista_duracoes))
        timeline_medias.append({
            'id': idx,
            'nome': cp['label'],
            'cor': cp.get('cor', '#6c757d'),
            'eh_grupo': (cp['tipo'] == 'group'),
            'media_entrada': media_str,
            'qtd_base': len(lista_duracoes),
        })

    pagination_matriz = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()) \
        .paginate(page=page_matriz, per_page=per_page, error_out=False)

    matriz_tempos = []
    for sol in pagination_matriz.items:
        tempos_fmt = {k: formatar_delta_str(v) for k, v in calcular_tempos_processo(sol).items()}
        matriz_tempos.append({'solicitacao': sol, 'tempos': tempos_fmt})

    colunas_etapas = [{'id': i, 'nome': cp['label']} for i, cp in enumerate(CHECKPOINTS_RELATORIO)]

    return timeline_medias, matriz_tempos, colunas_etapas, pagination_matriz


# =============================================================================
# INDICADORES (API JSON do Painel)
# =============================================================================

def indicador_por_fase(filtros: FiltrosPainel):
    """Quantidade de processos por fase — série para gráfico de barras."""
    series = [
        {'nome': r['nome'], 'cor': r['cor'], 'qtd': r['qtd']}
        for r in resumo_por_fase(filtros)
    ]
    return {'series': series, 'total': sum(s['qtd'] for s in series)}


INDICADORES = {
    'por-fase': indicador_por_fase,
}


# =============================================================================
# AUDITORIA DE DADOS
# =============================================================================

def dados_auditoria():
    """Audita todas as solicitações e identifica informações faltantes.

    Para cada solicitação, verifica:
      - Dados básicos: protocolo, competência, contrato, data, tipo pagamento
      - Etapa: se possui etapa atual definida
      - Empenho: status e valor solicitado
      - Documentos financeiros (SEI): NE, NL, PD, OB
      - Links: link do processo e id_procedimento SEI
      - Tempo total calculado
      - Status geral
      - Histórico de movimentações
    """
    solicitacoes = (
        Solicitacao.query
        .join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
        .outerjoin(Etapa, Solicitacao.etapa_atual_id == Etapa.id)
        .order_by(Solicitacao.id)
        .all()
    )

    # Pré-carregar empenhos apenas das solicitações carregadas (não todos do banco)
    sol_ids = [s.id for s in solicitacoes]
    mapa_empenho = {}
    if sol_ids:
        empenhos_relevantes = (
            db.session.query(SolicitacaoEmpenho)
            .filter(SolicitacaoEmpenho.id_solicitacao.in_(sol_ids))
            .order_by(SolicitacaoEmpenho.data.desc())
            .all()
        )
        for emp in empenhos_relevantes:
            if emp.id_solicitacao not in mapa_empenho:
                mapa_empenho[emp.id_solicitacao] = emp

    # Pré-carregar documentos SEI (NE, NL, PD, OB) apenas dos protocolos relevantes
    series_financeiras = [
        SerieDocumentoSEI.NOTA_EMPENHO,
        SerieDocumentoSEI.LIQUIDACAO,
        SerieDocumentoSEI.PD,
        SerieDocumentoSEI.OB
    ]
    protocolos_validos = [s.protocolo_gerado_sei for s in solicitacoes if s.protocolo_gerado_sei]
    docs_sei = []
    if protocolos_validos:
        docs_sei = SeiMovimentacao.query.filter(
            SeiMovimentacao.id_serie.in_([int(s) for s in series_financeiras]),
            SeiMovimentacao.protocolo_procedimento.in_(protocolos_validos)
        ).all()

    # Agrupar por protocolo_procedimento
    mapa_docs = {}
    for doc in docs_sei:
        proto = doc.protocolo_procedimento
        if proto not in mapa_docs:
            mapa_docs[proto] = {}
        serie = str(doc.id_serie)
        if serie not in mapa_docs[proto]:
            mapa_docs[proto][serie] = doc

    # Pré-carregar contagem de histórico por solicitação
    hist_contagem = dict(
        db.session.query(
            HistoricoMovimentacao.id_solicitacao,
            func.count(HistoricoMovimentacao.id)
        ).group_by(HistoricoMovimentacao.id_solicitacao).all()
    )

    resultados = []
    contadores = {
        'total': 0,
        'sem_protocolo': 0,
        'sem_competencia': 0,
        'sem_contrato': 0,
        'sem_etapa': 0,
        'sem_data_solicitacao': 0,
        'sem_tipo_pagamento': 0,
        'sem_link_sei': 0,
        'sem_id_procedimento': 0,
        'sem_status_empenho': 0,
        'sem_valor_empenho': 0,
        'sem_ne': 0,
        'sem_nl': 0,
        'sem_pd': 0,
        'sem_ob': 0,
        'sem_tempo_total': 0,
        'sem_status_geral': 0,
        'sem_historico': 0,
        'completos': 0,
    }

    for sol in solicitacoes:
        contadores['total'] += 1
        proto = sol.protocolo_gerado_sei
        docs_proto = mapa_docs.get(proto, {}) if proto else {}
        empenho = mapa_empenho.get(sol.id)
        qtd_hist = hist_contagem.get(sol.id, 0)

        # Verificações
        faltando = []

        tem_protocolo = bool(proto and proto.strip())
        tem_competencia = bool(sol.competencia and sol.competencia.strip())
        tem_contrato = bool(sol.codigo_contrato)
        tem_etapa = bool(sol.etapa_atual_id)
        tem_data = bool(sol.data_solicitacao)
        tem_tipo_pgto = bool(sol.id_tipo_pagamento)
        tem_link_sei = bool(sol.link_processo_sei and sol.link_processo_sei.strip())
        tem_id_proc = bool(sol.id_procedimento_sei and sol.id_procedimento_sei.strip())
        tem_status_emp = bool(sol.status_empenho_id)
        tem_valor_emp = bool(empenho and empenho.valor and float(empenho.valor) > 0)
        tem_ne = bool(docs_proto.get(SerieDocumentoSEI.NOTA_EMPENHO) or (sol.num_ne and sol.num_ne.strip()))
        tem_nl = bool(docs_proto.get(SerieDocumentoSEI.LIQUIDACAO) or (sol.num_nl and sol.num_nl.strip()))
        tem_pd = bool(docs_proto.get(SerieDocumentoSEI.PD) or (sol.num_pd and sol.num_pd.strip()))
        tem_ob = bool(docs_proto.get(SerieDocumentoSEI.OB) or (sol.num_ob and sol.num_ob.strip()))
        tem_tempo = bool(sol.tempo_total and sol.tempo_total.strip())
        tem_status = bool(sol.status_geral and sol.status_geral.strip())
        tem_historico = qtd_hist > 0

        if not tem_protocolo:
            faltando.append('Protocolo SEI')
            contadores['sem_protocolo'] += 1
        if not tem_competencia:
            faltando.append('Competência')
            contadores['sem_competencia'] += 1
        if not tem_contrato:
            faltando.append('Contrato')
            contadores['sem_contrato'] += 1
        if not tem_etapa:
            faltando.append('Etapa Atual')
            contadores['sem_etapa'] += 1
        if not tem_data:
            faltando.append('Data Solicitação')
            contadores['sem_data_solicitacao'] += 1
        if not tem_tipo_pgto:
            faltando.append('Tipo Pagamento')
            contadores['sem_tipo_pagamento'] += 1
        if not tem_link_sei:
            faltando.append('Link SEI')
            contadores['sem_link_sei'] += 1
        if not tem_id_proc:
            faltando.append('ID Procedimento SEI')
            contadores['sem_id_procedimento'] += 1
        if not tem_status_emp:
            faltando.append('Status Empenho')
            contadores['sem_status_empenho'] += 1
        if not tem_valor_emp:
            faltando.append('Valor Empenho')
            contadores['sem_valor_empenho'] += 1
        if not tem_ne:
            faltando.append('NE')
            contadores['sem_ne'] += 1
        if not tem_nl:
            faltando.append('NL')
            contadores['sem_nl'] += 1
        if not tem_pd:
            faltando.append('PD')
            contadores['sem_pd'] += 1
        if not tem_ob:
            faltando.append('OB')
            contadores['sem_ob'] += 1
        if not tem_tempo:
            faltando.append('Tempo Total')
            contadores['sem_tempo_total'] += 1
        if not tem_status:
            faltando.append('Status Geral')
            contadores['sem_status_geral'] += 1
        if not tem_historico:
            faltando.append('Histórico Movimentações')
            contadores['sem_historico'] += 1

        if not faltando:
            contadores['completos'] += 1

        # Dados para NE da tabela SolicitacaoEmpenho (campo ne separado)
        ne_empenho = empenho.ne if empenho else None

        resultados.append({
            'solicitacao': sol,
            'contratado': sol.contrato.nomeContratado if sol.contrato else '',
            'etapa_nome': sol.etapa.nome if sol.etapa else '',
            'empenho_valor': float(empenho.valor) if empenho and empenho.valor else None,
            'empenho_ne': ne_empenho,
            'status_empenho_nome': sol.status_empenho.nome if sol.status_empenho else '',
            'mov_ne': docs_proto.get(SerieDocumentoSEI.NOTA_EMPENHO),
            'mov_nl': docs_proto.get(SerieDocumentoSEI.LIQUIDACAO),
            'mov_pd': docs_proto.get(SerieDocumentoSEI.PD),
            'mov_ob': docs_proto.get(SerieDocumentoSEI.OB),
            'qtd_historico': qtd_hist,
            'faltando': faltando,
            'qtd_faltando': len(faltando),
            # Flags individuais para template
            'tem_protocolo': tem_protocolo,
            'tem_competencia': tem_competencia,
            'tem_contrato': tem_contrato,
            'tem_etapa': tem_etapa,
            'tem_data': tem_data,
            'tem_tipo_pgto': tem_tipo_pgto,
            'tem_link_sei': tem_link_sei,
            'tem_id_proc': tem_id_proc,
            'tem_status_emp': tem_status_emp,
            'tem_valor_emp': tem_valor_emp,
            'tem_ne': tem_ne,
            'tem_nl': tem_nl,
            'tem_pd': tem_pd,
            'tem_ob': tem_ob,
            'tem_tempo': tem_tempo,
            'tem_status': tem_status,
            'tem_historico': tem_historico,
        })

    return resultados, contadores
