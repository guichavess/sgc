"""
API endpoints para graficos dos Dashboards.
Retornam JSON consumido pelo ApexCharts no frontend.
Acesso controlado pelo módulo 'dashboards' (perfil de permissões).
"""
from datetime import datetime, date, timedelta
import calendar

from flask import jsonify, request
from flask_login import login_required
from sqlalchemy import func, case, extract, text

from app.dashboards.routes import dashboards_bp
from app.models import (
    Solicitacao, Contrato, SolicitacaoEmpenho,
    Empenho, Liquidacao, Etapa, HistoricoMovimentacao,
)
from app.extensions import db
from app.repositories import SolicitacaoRepository, EmpenhoRepository, LiquidacaoRepository
from app.utils.permissions import requires_permission
from app.constants import UG_CODE


# ---------------------------------------------------------------------------
# Consolidado
# ---------------------------------------------------------------------------

@dashboards_bp.route('/api/status-solicitacoes')
@login_required
@requires_permission('dashboards')
def api_status_solicitacoes():
    """Donut chart: solicitacoes por status_geral."""
    counts = SolicitacaoRepository.contar_por_status()
    color_map = {
        'EM ANDAMENTO': '#0d6efd',
        'EM LIQUIDAÇÃO': '#ffc107',
        'PAGO': '#198754',
        'CANCELADO': '#dc3545',
    }
    labels = list(counts.keys())
    series = list(counts.values())
    colors = [color_map.get(s, '#6c757d') for s in labels]
    return jsonify({'labels': labels, 'series': series, 'colors': colors})


@dashboards_bp.route('/api/evolucao-mensal')
@login_required
@requires_permission('dashboards')
def api_evolucao_mensal():
    """Bar mensal: solicitacoes criadas por mes."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    result = db.session.query(
        extract('month', Solicitacao.data_solicitacao).label('mes'),
        func.count(Solicitacao.id)
    ).filter(
        Solicitacao.data_solicitacao.isnot(None),
        extract('year', Solicitacao.data_solicitacao) == ano
    ).group_by('mes').order_by('mes').all()

    meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
             'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    data = [0] * 12
    for mes, count in result:
        if mes:
            data[int(mes) - 1] = count

    return jsonify({
        'categories': meses,
        'series': [{'name': 'Solicitacoes', 'data': data}]
    })


@dashboards_bp.route('/api/top-contratos-empenhado')
@login_required
@requires_permission('dashboards')
def api_top_contratos_empenhado():
    """Horizontal bar: top 10 contratos por valor empenhado."""
    ano = request.args.get('ano', datetime.now().year, type=int)

    vlr_calc = case(
        (Empenho.statusDocumento == 'ANULADO', Empenho.valor * -1),
        else_=Empenho.valor
    )
    result = db.session.query(
        Empenho.codContrato,
        func.sum(vlr_calc).label('total')
    ).filter(
        Empenho.codigoUG == UG_CODE,
        Empenho.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
        Empenho.dataEmissao >= date(ano, 1, 1),
        Empenho.dataEmissao <= date(ano, 12, 31),
    ).group_by(Empenho.codContrato
    ).order_by(func.sum(vlr_calc).desc()
    ).limit(10).all()

    # Batch: buscar todos os contratos de uma vez
    cod_list = [str(c) for c, _ in result if c]
    contratos_map = {}
    if cod_list:
        all_contratos = Contrato.query.filter(
            func.replace(func.replace(Contrato.codigo, '.', ''), '/', '').in_(cod_list)
        ).all()
        for c in all_contratos:
            key = c.codigo.replace('.', '').replace('/', '')
            contratos_map[key] = c

    categories = []
    values = []
    for cod_contrato, total in result:
        if not cod_contrato:
            continue
        cod_str = str(cod_contrato)
        contrato = contratos_map.get(cod_str)
        nome = contrato.nomeContratadoResumido if contrato else f'Contrato {cod_contrato}'
        categories.append((nome or f'Contrato {cod_contrato}')[:35])
        values.append(round(float(total), 2))

    return jsonify({
        'categories': categories,
        'series': [{'name': 'Empenhado (R$)', 'data': values}]
    })


# ---------------------------------------------------------------------------
# Pagamentos
# ---------------------------------------------------------------------------

@dashboards_bp.route('/api/solicitacoes-por-etapa')
@login_required
@requires_permission('dashboards')
def api_solicitacoes_por_etapa():
    """Bar chart: solicitacoes por etapa com cores."""
    counts = SolicitacaoRepository.contar_por_etapa()
    etapas = Etapa.query.order_by(Etapa.ordem).all()
    categories = []
    data = []
    colors = []
    for e in etapas:
        c = counts.get(e.id, 0)
        if c > 0:
            categories.append(e.alias or e.nome)
            data.append(c)
            colors.append(e.cor_hex or '#6c757d')
    return jsonify({'categories': categories, 'data': data, 'colors': colors})


@dashboards_bp.route('/api/tempo-medio-etapa')
@login_required
@requires_permission('dashboards')
def api_tempo_medio_etapa():
    """Bar chart: tempo medio em dias por etapa."""
    etapas = Etapa.query.order_by(Etapa.ordem).all()
    etapa_map = {e.id: (e.alias or e.nome) for e in etapas}
    etapa_ordem = {e.id: e.ordem for e in etapas}

    # Calcular tempo médio por etapa usando SQL com LEAD window function
    # Subquery: pega a data da próxima movimentação por solicitação
    from sqlalchemy import text
    tempo_sql = text("""
        SELECT id_etapa_nova AS etapa_id,
               AVG(TIMESTAMPDIFF(SECOND, data_movimentacao, prox_data)) / 86400 AS media_dias
        FROM (
            SELECT id_etapa_nova, data_movimentacao,
                   LEAD(data_movimentacao) OVER (
                       PARTITION BY id_solicitacao ORDER BY data_movimentacao
                   ) AS prox_data
            FROM sis_historico_movimentacoes
        ) sub
        WHERE prox_data IS NOT NULL
          AND prox_data >= data_movimentacao
        GROUP BY id_etapa_nova
    """)
    tempo_result = db.session.execute(tempo_sql).fetchall()
    etapa_tempos = {int(row[0]): float(row[1]) for row in tempo_result if row[1] is not None}

    categories = []
    data = []
    for etapa_id in sorted(etapa_tempos.keys(), key=lambda x: etapa_ordem.get(x, 999)):
        media = etapa_tempos[etapa_id]
        if media > 0:
            nome = etapa_map.get(etapa_id, f'Etapa {etapa_id}')
            categories.append(nome)
            data.append(round(media, 1))

    return jsonify({'categories': categories, 'data': data})


@dashboards_bp.route('/api/distribuicao-competencia')
@login_required
@requires_permission('dashboards')
def api_distribuicao_competencia():
    """Bar mensal: solicitacoes por competencia."""
    result = db.session.query(
        Solicitacao.competencia, func.count(Solicitacao.id)
    ).filter(
        Solicitacao.competencia.isnot(None)
    ).group_by(Solicitacao.competencia).all()

    # Ordenar por ano/mes (formato MM/YYYY)
    def sort_key(comp):
        try:
            parts = comp.split('/')
            return (int(parts[1]), int(parts[0]))
        except (ValueError, IndexError):
            return (0, 0)

    sorted_result = sorted(result, key=lambda x: sort_key(x[0] or ''))
    # Ultimas 12 competencias
    sorted_result = sorted_result[-12:]

    categories = [comp for comp, _ in sorted_result]
    data = [count for _, count in sorted_result]
    return jsonify({'categories': categories, 'data': data})


# ---------------------------------------------------------------------------
# Financeiro
# ---------------------------------------------------------------------------

@dashboards_bp.route('/api/empenhado-vs-liquidado')
@login_required
@requires_permission('dashboards')
def api_empenhado_vs_liquidado():
    """Grouped bar: empenhado vs liquidado por contrato (top 15)."""
    ano = request.args.get('ano', datetime.now().year, type=int)

    # Top 15 contratos por valor empenhado
    vlr_calc = case(
        (Empenho.statusDocumento == 'ANULADO', Empenho.valor * -1),
        else_=Empenho.valor
    )
    top_contratos = db.session.query(
        Empenho.codContrato,
        func.sum(vlr_calc).label('total')
    ).filter(
        Empenho.codigoUG == UG_CODE,
        Empenho.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
        Empenho.dataEmissao >= date(ano, 1, 1),
        Empenho.dataEmissao <= date(ano, 12, 31),
    ).group_by(Empenho.codContrato
    ).order_by(func.sum(vlr_calc).desc()
    ).limit(15).all()

    # Batch: buscar contratos e liquidações de uma vez
    cod_list = [str(c) for c, _ in top_contratos if c]

    # Contratos em batch
    contratos_map = {}
    if cod_list:
        all_contratos = Contrato.query.filter(
            func.replace(func.replace(Contrato.codigo, '.', ''), '/', '').in_(cod_list)
        ).all()
        for c in all_contratos:
            contratos_map[c.codigo.replace('.', '').replace('/', '')] = c

    # Liquidações em batch (uma query para todos os contratos)
    vlr_liq = case(
        (Liquidacao.statusDocumento == 'ANULADO', Liquidacao.valor * -1),
        else_=Liquidacao.valor
    )
    liq_map = {}
    if cod_list:
        liq_rows = db.session.query(
            Liquidacao.codContrato,
            func.sum(vlr_liq).label('total')
        ).filter(
            Liquidacao.codigoUG == UG_CODE,
            Liquidacao.codContrato.in_(cod_list),
            Liquidacao.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
            Liquidacao.dataEmissao >= date(ano, 1, 1),
            Liquidacao.dataEmissao <= date(ano, 12, 31),
        ).group_by(Liquidacao.codContrato).all()
        for cod, total in liq_rows:
            liq_map[str(cod)] = float(total or 0)

    categories = []
    empenhados = []
    liquidados = []

    for cod_contrato, total_emp in top_contratos:
        if not cod_contrato:
            continue
        cod_str = str(cod_contrato)
        contrato = contratos_map.get(cod_str)
        nome = (contrato.nomeContratadoResumido if contrato else f'Contrato {cod_contrato}') or ''

        categories.append(nome[:25])
        empenhados.append(round(float(total_emp), 2))
        liquidados.append(round(float(liq_map.get(cod_str, 0)), 2))

    return jsonify({
        'categories': categories,
        'series': [
            {'name': 'Empenhado', 'data': empenhados},
            {'name': 'Liquidado', 'data': liquidados},
        ]
    })


@dashboards_bp.route('/api/evolucao-empenhos')
@login_required
@requires_permission('dashboards')
def api_evolucao_empenhos():
    """Line chart: empenhos por mes."""
    ano = request.args.get('ano', datetime.now().year, type=int)

    vlr_calc = case(
        (Empenho.statusDocumento == 'ANULADO', Empenho.valor * -1),
        else_=Empenho.valor
    )
    result = db.session.query(
        extract('month', Empenho.dataEmissao).label('mes'),
        func.sum(vlr_calc).label('total')
    ).filter(
        Empenho.codigoUG == UG_CODE,
        Empenho.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
        extract('year', Empenho.dataEmissao) == ano
    ).group_by('mes').order_by('mes').all()

    meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
             'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    data = [0.0] * 12
    for mes, total in result:
        if mes:
            data[int(mes) - 1] = round(float(total or 0), 2)

    return jsonify({
        'categories': meses,
        'series': [{'name': f'Empenhos {ano}', 'data': data}]
    })


@dashboards_bp.route('/api/saldo-por-contrato')
@login_required
@requires_permission('dashboards')
def api_saldo_por_contrato():
    """Horizontal bar: saldo disponivel por contrato (top 15)."""
    ano = request.args.get('ano', datetime.now().year, type=int)

    # Top contratos por empenhado
    vlr_emp = case(
        (Empenho.statusDocumento == 'ANULADO', Empenho.valor * -1),
        else_=Empenho.valor
    )
    top_contratos = db.session.query(
        Empenho.codContrato,
        func.sum(vlr_emp).label('total_emp')
    ).filter(
        Empenho.codigoUG == UG_CODE,
        Empenho.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
        Empenho.dataEmissao >= date(ano, 1, 1),
        Empenho.dataEmissao <= date(ano, 12, 31),
    ).group_by(Empenho.codContrato
    ).order_by(func.sum(vlr_emp).desc()
    ).limit(15).all()

    # Batch: buscar contratos e liquidações de uma vez
    cod_list = [str(c) for c, _ in top_contratos if c]

    contratos_map = {}
    if cod_list:
        all_contratos = Contrato.query.filter(
            func.replace(func.replace(Contrato.codigo, '.', ''), '/', '').in_(cod_list)
        ).all()
        for c in all_contratos:
            contratos_map[c.codigo.replace('.', '').replace('/', '')] = c

    vlr_liq = case(
        (Liquidacao.statusDocumento == 'ANULADO', Liquidacao.valor * -1),
        else_=Liquidacao.valor
    )
    liq_map = {}
    if cod_list:
        liq_rows = db.session.query(
            Liquidacao.codContrato,
            func.sum(vlr_liq).label('total')
        ).filter(
            Liquidacao.codigoUG == UG_CODE,
            Liquidacao.codContrato.in_(cod_list),
            Liquidacao.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
            Liquidacao.dataEmissao >= date(ano, 1, 1),
            Liquidacao.dataEmissao <= date(ano, 12, 31),
        ).group_by(Liquidacao.codContrato).all()
        for cod, total in liq_rows:
            liq_map[str(cod)] = float(total or 0)

    categories = []
    saldos = []

    for cod_contrato, total_emp in top_contratos:
        if not cod_contrato:
            continue
        cod_str = str(cod_contrato)
        contrato = contratos_map.get(cod_str)
        nome = (contrato.nomeContratadoResumido if contrato else f'Contrato {cod_contrato}') or ''

        saldo = max(0.0, float(total_emp) - float(liq_map.get(cod_str, 0)))
        categories.append(nome[:25])
        saldos.append(round(saldo, 2))

    return jsonify({
        'categories': categories,
        'series': [{'name': 'Saldo (R$)', 'data': saldos}]
    })


# ---------------------------------------------------------------------------
# Contratos
# ---------------------------------------------------------------------------

@dashboards_bp.route('/api/contratos-por-situacao')
@login_required
@requires_permission('dashboards')
def api_contratos_por_situacao():
    """Donut: contratos por situacao."""
    result = db.session.query(
        Contrato.situacao, func.count(Contrato.codigo)
    ).filter(
        Contrato.situacao.isnot(None)
    ).group_by(Contrato.situacao).all()

    labels = [s for s, _ in result]
    series = [c for _, c in result]
    return jsonify({'labels': labels, 'series': series})


@dashboards_bp.route('/api/valor-por-contratado')
@login_required
@requires_permission('dashboards')
def api_valor_por_contratado():
    """Horizontal bar: valor total por contratado (top 15)."""
    result = db.session.query(
        Contrato.nomeContratadoResumido,
        func.sum(Contrato.valorTotal).label('total')
    ).filter(
        Contrato.nomeContratadoResumido.isnot(None),
        Contrato.valorTotal.isnot(None),
    ).group_by(Contrato.nomeContratadoResumido
    ).order_by(func.sum(Contrato.valorTotal).desc()
    ).limit(15).all()

    categories = [(n or 'N/A')[:30] for n, _ in result]
    values = [round(float(v or 0), 2) for _, v in result]
    return jsonify({
        'categories': categories,
        'series': [{'name': 'Valor Total (R$)', 'data': values}]
    })


@dashboards_bp.route('/api/contratos-gantt')
@login_required
@requires_permission('dashboards')
def api_contratos_gantt():
    """RangeBar: vigencia dos contratos ativos."""
    contratos = Contrato.query.filter(
        Contrato.dataInicioVigencia.isnot(None),
        Contrato.dataFimVigencia.isnot(None),
        Contrato.situacao.in_(['ATIVO', 'VIGENTE', 'EM EXECUÇÃO']),
    ).order_by(Contrato.dataInicioVigencia).limit(20).all()

    data = []
    for c in contratos:
        nome = (c.nomeContratadoResumido or c.codigo)[:30]
        # Converter datas para timestamps em milissegundos (compativel com Windows)
        ts_inicio = int(calendar.timegm(c.dataInicioVigencia.timetuple())) * 1000
        ts_fim = int(calendar.timegm(c.dataFimVigencia.timetuple())) * 1000
        data.append({
            'x': nome,
            'y': [ts_inicio, ts_fim],
        })

    return jsonify({'series': [{'name': 'Vigencia', 'data': data}]})


@dashboards_bp.route('/api/contratos-vigencia-proxima')
@login_required
@requires_permission('dashboards')
def api_contratos_vigencia_proxima():
    """Tabela: contratos com vigencia proxima do fim (60 dias)."""
    hoje = date.today()
    limite = hoje + timedelta(days=60)
    contratos = Contrato.query.filter(
        Contrato.dataFimVigencia.isnot(None),
        Contrato.dataFimVigencia >= hoje,
        Contrato.dataFimVigencia <= limite,
        Contrato.situacao.in_(['ATIVO', 'VIGENTE', 'EM EXECUÇÃO']),
    ).order_by(Contrato.dataFimVigencia).all()

    data = [{
        'codigo': c.codigo,
        'contratado': c.nomeContratadoResumido or c.nomeContratado,
        'fim_vigencia': c.dataFimVigencia.strftime('%d/%m/%Y'),
        'dias_restantes': (c.dataFimVigencia - hoje).days,
        'valor': round(float(c.valorTotal or 0), 2),
    } for c in contratos]

    return jsonify({'contratos': data})


# ---------------------------------------------------------------------------
# KPI endpoints (dados para React SPA)
# ---------------------------------------------------------------------------

def _calcular_total(modelo, ano):
    """Calcula total empenhado ou liquidado global para o ano."""
    vlr_calculado = case(
        (modelo.statusDocumento == 'ANULADO', modelo.valor * -1),
        else_=modelo.valor
    )
    result = db.session.query(func.sum(vlr_calculado)).filter(
        modelo.codigoUG == UG_CODE,
        modelo.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
        modelo.dataEmissao >= date(ano, 1, 1),
        modelo.dataEmissao <= date(ano, 12, 31),
    ).scalar()
    return float(result) if result else 0.0


@dashboards_bp.route('/api/kpis-consolidado')
@login_required
@requires_permission('dashboards')
def api_kpis_consolidado():
    """KPIs do dashboard consolidado."""
    ano = request.args.get('ano', datetime.now().year, type=int)

    total_solicitacoes = Solicitacao.query.count()
    status_counts = SolicitacaoRepository.contar_por_status()
    contratos_ativos = Contrato.query.filter(
        Contrato.situacao.in_(['ATIVO', 'VIGENTE', 'EM EXECUÇÃO'])
    ).count()
    total_contratos = Contrato.query.count()
    nes_pendentes = SolicitacaoEmpenho.query.filter(
        SolicitacaoEmpenho.ne.is_(None)
    ).count()
    total_empenhado = _calcular_total(Empenho, ano)
    total_liquidado = _calcular_total(Liquidacao, ano)
    saldo_global = max(0.0, total_empenhado - total_liquidado)

    return jsonify({
        'total_solicitacoes': total_solicitacoes,
        'status_counts': status_counts,
        'contratos_ativos': contratos_ativos,
        'total_contratos': total_contratos,
        'nes_pendentes': nes_pendentes,
        'total_empenhado': round(total_empenhado, 2),
        'total_liquidado': round(total_liquidado, 2),
        'saldo_global': round(saldo_global, 2),
    })


@dashboards_bp.route('/api/kpis-financeiro')
@login_required
@requires_permission('dashboards')
def api_kpis_financeiro():
    """KPIs do dashboard financeiro."""
    ano = request.args.get('ano', datetime.now().year, type=int)

    total_empenhado = _calcular_total(Empenho, ano)
    total_liquidado = _calcular_total(Liquidacao, ano)
    saldo_total = max(0.0, total_empenhado - total_liquidado)
    nes_pendentes = SolicitacaoEmpenho.query.filter(
        SolicitacaoEmpenho.ne.is_(None)
    ).count()

    return jsonify({
        'total_empenhado': round(total_empenhado, 2),
        'total_liquidado': round(total_liquidado, 2),
        'saldo_total': round(saldo_total, 2),
        'nes_pendentes': nes_pendentes,
    })


@dashboards_bp.route('/api/kpis-contratos')
@login_required
@requires_permission('dashboards')
def api_kpis_contratos():
    """KPIs do dashboard de contratos."""
    total_contratos = Contrato.query.count()
    contratos_ativos = Contrato.query.filter(
        Contrato.situacao.in_(['ATIVO', 'VIGENTE', 'EM EXECUÇÃO'])
    ).count()
    contratos_encerrados = Contrato.query.filter(
        Contrato.situacao == 'ENCERRADO'
    ).count()
    valor_total = db.session.query(func.sum(Contrato.valorTotal)).scalar()
    valor_total = float(valor_total) if valor_total else 0.0

    return jsonify({
        'total_contratos': total_contratos,
        'contratos_ativos': contratos_ativos,
        'contratos_encerrados': contratos_encerrados,
        'valor_total': round(valor_total, 2),
    })


# ---------------------------------------------------------------------------
# Consolidado Orçamentário (novo dashboard)
# ---------------------------------------------------------------------------

_UG = '210101'
_UGS_PADRAO = ('210101', '210102')

_NOMES_MESES = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
}

# Helpers para extração de ação a partir de codClassificacao
_SQL_ACAO_RESERVA = "SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(codClassificacao, ' ', ''), '.', 7), '.', -1)"
_SQL_ACAO_CLASS = "SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(codClassificacao, ' ', ''), '.', 8), '.', -1)"


def _build_date_range(ano, mes=None):
    """Retorna (dt_ini, dt_fim) string para range de datas."""
    if mes:
        dt_ini = f"{ano}-{mes:02d}-01"
        dt_fim = f"{ano + 1}-01-01" if mes == 12 else f"{ano}-{mes + 1:02d}-01"
    else:
        dt_ini = f"{ano}-01-01"
        dt_fim = f"{ano + 1}-01-01"
    return dt_ini, dt_fim


def _exec_sum(sql_str, params):
    """Executa SQL e retorna float."""
    try:
        r = db.session.execute(text(sql_str), params).scalar()
        return float(r or 0)
    except Exception:
        return 0.0


def _get_filtros():
    """Extrai filtros de mes, acao, natureza, fonte, ug do request. Suporta múltiplos valores separados por vírgula."""
    mes = request.args.get('mes', type=int)
    acao = request.args.get('acao', '')
    natureza = request.args.get('natureza', '')
    fonte = request.args.get('fonte', '')
    ug = request.args.get('ug', '')
    return mes, acao, natureza, fonte, ug


def _build_ug_filter(ug, col='codigoUG', prefix='ug'):
    """Gera (frag_sql, params) para filtrar por codigoUG.

    - ug vazio: filtra pelas UGs padrão (210101, 210102).
    - ug com csv (ex: '210101' ou '210101,210102'): filtra pelas selecionadas.
    Aceita `col` com alias (ex: 'e.codigoUG').
    """
    params = {}
    if not ug:
        placeholders = []
        for i, u in enumerate(_UGS_PADRAO):
            key = f"{prefix}_def_{i}"
            params[key] = u
            placeholders.append(f":{key}")
        return f"AND {col} IN ({','.join(placeholders)})", params
    frag = _multi_in(col, ug, prefix, params)
    return frag, params


def _multi_in(col, values_str, param_prefix, params_dict):
    """Gera cláusula IN para múltiplos valores separados por vírgula."""
    vals = [v.strip() for v in values_str.split(',') if v.strip()]
    if len(vals) == 1:
        params_dict[param_prefix] = vals[0]
        return f"AND {col} = :{param_prefix}"
    placeholders = []
    for i, v in enumerate(vals):
        key = f"{param_prefix}_{i}"
        params_dict[key] = v
        placeholders.append(f":{key}")
    return f"AND {col} IN ({','.join(placeholders)})"


def _build_exec_filters(fonte, natureza, acao, for_table=''):
    """Gera fragmentos SQL de filtro + params. Suporta múltiplos valores (vírgula)."""
    frags = []
    params = {}
    if fonte:
        frags.append(_multi_in('codFonte', fonte, 'fonte', params))
    if natureza:
        frags.append(_multi_in('codNatureza', natureza, 'nat', params))
    if acao:
        if for_table == 'reserva':
            col = _SQL_ACAO_RESERVA
        elif for_table == 'empenho':
            col = 'codAcao'
        else:
            col = _SQL_ACAO_CLASS
        frags.append(_multi_in(col, acao, 'acao', params))
    return ' '.join(frags), params


@dashboards_bp.route('/api/filtros-orcamentario')
@login_required
@requires_permission('dashboards')
def api_filtros_orcamentario():
    """Opções de filtro do dashboard orçamentário com cascata Ação→Fonte→Natureza.

    Aceita `acao` e `fonte` (csv de códigos) para restringir as listas filhas
    apenas às combinações que coexistem em `loa`:
      - Ações: sempre o universo do ano (raiz).
      - Fontes: filtradas por `acao`, se informada.
      - Naturezas: filtradas por `acao` e/ou `fonte`, se informados.
    """
    ano = request.args.get('ano', datetime.now().year, type=int)
    acao_sel = request.args.get('acao', '')
    fonte_sel = request.args.get('fonte', '')
    ug_sel = request.args.get('ug', '')
    try:
        # UGs — apenas 210101 e 210102 com codigo + titulo da tabela `ug`
        ugs_rows = db.session.execute(text(
            "SELECT codigo, titulo FROM ug WHERE codigo IN ('210101', '210102') ORDER BY codigo"
        )).fetchall()
        ugs = [{'codigo': str(r[0]), 'descricao': r[1] or ''} for r in ugs_rows]

        # Filtro UG comum para queries em loa
        ug_frag_m, ug_params_m = _build_ug_filter(ug_sel, prefix='ugm')

        # Meses
        sql_m = text(f"SELECT DISTINCT mes FROM loa WHERE ano = :ano {ug_frag_m} ORDER BY mes")
        meses = [{'valor': r[0], 'label': _NOMES_MESES.get(r[0], str(r[0]))}
                 for r in db.session.execute(sql_m, {'ano': ano, **ug_params_m}) if r[0]]

        # Ações — universo do ano (raiz da hierarquia, nunca filtrada por ação/fonte/natureza, mas pode ser por UG)
        ug_frag_a, ug_params_a = _build_ug_filter(ug_sel, prefix='uga')
        sql_a = text(f"SELECT DISTINCT codAcao FROM loa WHERE ano = :ano AND codAcao IS NOT NULL {ug_frag_a} ORDER BY codAcao")
        cod_acoes = [r[0] for r in db.session.execute(sql_a, {'ano': ano, **ug_params_a}) if r[0]]
        desc_a = {str(r[0]): r[1] for r in db.session.execute(text("SELECT codigo, titulo FROM acao")).fetchall() if r[0] and r[1]}
        acoes = [{'codigo': c, 'descricao': desc_a.get(str(c), '')} for c in cod_acoes]
        acoes.sort(key=lambda x: x['descricao'].lower())

        # Fontes — restritas pela Ação selecionada (se houver) e UG
        params_f = {'ano': ano}
        frag_acao_f = ''
        if acao_sel:
            frag_acao_f = _multi_in('codAcao', acao_sel, 'af', params_f)
        ug_frag_f, ug_params_f = _build_ug_filter(ug_sel, prefix='ugf')
        params_f.update(ug_params_f)
        sql_f = text(
            f"SELECT DISTINCT codFonte FROM loa "
            f"WHERE ano = :ano AND codFonte IS NOT NULL {frag_acao_f} {ug_frag_f} "
            f"ORDER BY codFonte"
        )
        cod_fontes = [r[0] for r in db.session.execute(sql_f, params_f) if r[0]]
        try:
            desc_f = {str(r[0]): r[1] for r in db.session.execute(text("SELECT codigo, titulo FROM fonterecurso")).fetchall() if r[0] and r[1]}
        except Exception:
            desc_f = {}
        fontes = [{'codigo': c, 'descricao': desc_f.get(str(c), str(c))} for c in cod_fontes]
        fontes.sort(key=lambda x: str(x['codigo']))

        # Naturezas — restritas por Ação, Fonte e UG selecionadas (se houver)
        params_n = {'ano': ano}
        frag_acao_n = _multi_in('codAcao', acao_sel, 'an', params_n) if acao_sel else ''
        frag_fonte_n = _multi_in('codFonte', fonte_sel, 'fn', params_n) if fonte_sel else ''
        ug_frag_n, ug_params_n = _build_ug_filter(ug_sel, prefix='ugn')
        params_n.update(ug_params_n)
        sql_n = text(
            f"SELECT DISTINCT codNatureza FROM loa "
            f"WHERE ano = :ano AND codNatureza IS NOT NULL {frag_acao_n} {frag_fonte_n} {ug_frag_n} "
            f"ORDER BY codNatureza"
        )
        cod_nats = [r[0] for r in db.session.execute(sql_n, params_n) if r[0]]
        desc_n = {str(r[0]): r[1] for r in db.session.execute(text("SELECT codigo, titulo FROM natdespesas")).fetchall() if r[0] and r[1]}
        naturezas = [{'codigo': c, 'descricao': desc_n.get(str(c), '')} for c in cod_nats]
        naturezas.sort(key=lambda x: x['descricao'].lower())
    except Exception:
        meses, acoes, naturezas, fontes, ugs = [], [], [], [], []

    return jsonify({'meses': meses, 'acoes': acoes, 'naturezas': naturezas, 'fontes': fontes, 'ugs': ugs})


@dashboards_bp.route('/api/kpis-orcamentario')
@login_required
@requires_permission('dashboards')
def api_kpis_orcamentario():
    """KPIs orçamentários com filtros: mes, acao, natureza, fonte, ug."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    mes, acao, natureza, fonte, ug = _get_filtros()
    dt_ini, dt_fim = _build_date_range(ano, mes)
    p = {'dt_ini': dt_ini, 'dt_fim': dt_fim}

    f_res, fp_res = _build_exec_filters(fonte, natureza, acao, 'reserva')
    f_emp, fp_emp = _build_exec_filters(fonte, natureza, acao, 'empenho')
    f_cls, fp_cls = _build_exec_filters(fonte, natureza, acao, 'class')

    ug_res, ugp_res = _build_ug_filter(ug, prefix='ugres')
    ug_emp, ugp_emp = _build_ug_filter(ug, prefix='ugemp')
    ug_liq, ugp_liq = _build_ug_filter(ug, prefix='ugliq')
    ug_pd, ugp_pd = _build_ug_filter(ug, prefix='ugpd')
    ug_pda, ugp_pda = _build_ug_filter(ug, prefix='ugpda')
    ug_ob, ugp_ob = _build_ug_filter(ug, prefix='ugob')

    reservado = _exec_sum(f"""
        SELECT COALESCE(SUM(CASE WHEN tipoAlteracao='ANULACAO' THEN -valor ELSE valor END),0)
        FROM reserva WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_res} {f_res}
    """, {**p, **ugp_res, **fp_res})

    empenhado = _exec_sum(f"""
        SELECT COALESCE(SUM(CASE WHEN tipoAlteracaoNE='ANULACAO' THEN -valor ELSE valor END),0)
        FROM empenho WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_emp} {f_emp}
    """, {**p, **ugp_emp, **fp_emp})

    liquidado = _exec_sum(f"""
        SELECT COALESCE(SUM(CASE WHEN tipoAlteracao='ANULACAO' THEN -valor ELSE valor END),0)
        FROM liquidacao WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_liq} {f_cls}
    """, {**p, **ugp_liq, **fp_cls})

    pd_val = _exec_sum(f"""
        SELECT COALESCE(SUM(valor),0) FROM pd
        WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_pd} {f_cls}
    """, {**p, **ugp_pd, **fp_cls})

    pd_aberto = _exec_sum(f"""
        SELECT COALESCE(SUM(valor),0) FROM pd
        WHERE statusDocumento='CONTABILIZADO' AND statusExecucao='STATUS_DISPONIVEL'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_pda} {f_cls}
    """, {**p, **ugp_pda, **fp_cls})

    pago = _exec_sum(f"""
        SELECT COALESCE(SUM(valor),0) FROM ob
        WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_ob} {f_cls}
    """, {**p, **ugp_ob, **fp_cls})

    dotacao = 0.0
    try:
        f_loa = []
        p_loa = {'ano': ano}
        if fonte:
            f_loa.append(_multi_in('codFonte', fonte, 'lf', p_loa))
        if natureza:
            f_loa.append(_multi_in('codNatureza', natureza, 'ln', p_loa))
        if acao:
            f_loa.append(_multi_in('codAcao', acao, 'la', p_loa))
        filt_loa = ' '.join(f_loa)
        ug_loa, ugp_loa = _build_ug_filter(ug, prefix='ugloa')
        ug_loa_sub, ugp_loa_sub = _build_ug_filter(ug, prefix='ugloasub')
        p_loa.update(ugp_loa)
        p_loa.update(ugp_loa_sub)
        r = db.session.execute(text(f"""
            SELECT COALESCE(SUM(saldo),0) FROM loa
            WHERE ano = :ano AND id = '622110101'
              AND mes = (SELECT MAX(mes) FROM loa WHERE ano = :ano {ug_loa_sub})
              {ug_loa}
              {filt_loa}
        """), p_loa).scalar()
        cred_disp = float(r or 0)
        dotacao = cred_disp + reservado + empenhado
        if dotacao <= 0:
            dotacao = empenhado if empenhado > 0 else 1
    except Exception:
        dotacao = empenhado if empenhado > 0 else 1

    return jsonify({
        'reservado': round(reservado, 2),
        'empenhado': round(empenhado, 2),
        'liquidado': round(liquidado, 2),
        'pd': round(pd_val, 2),
        'pd_aberto': round(pd_aberto, 2),
        'pago': round(pago, 2),
        'dotacao': round(dotacao, 2),
    })


@dashboards_bp.route('/api/evolucao-orcamentaria')
@login_required
@requires_permission('dashboards')
def api_evolucao_orcamentaria():
    """Evolução mensal com filtros."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    _, acao, natureza, fonte, ug = _get_filtros()
    meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
             'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']

    def _mensal(tabela, campo_anulacao, for_table, prefix):
        ug_frag, ug_params = _build_ug_filter(ug, prefix=prefix)
        filt, fp = _build_exec_filters(fonte, natureza, acao, for_table)
        sql = f"""
            SELECT MONTH(dataEmissao) as m,
                   COALESCE(SUM(CASE WHEN {campo_anulacao} THEN -valor ELSE valor END),0) as v
            FROM {tabela}
            WHERE statusDocumento='CONTABILIZADO'
              AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_frag} {filt}
            GROUP BY MONTH(dataEmissao)
        """
        rows = db.session.execute(text(sql), {
            'dt_ini': f'{ano}-01-01', 'dt_fim': f'{ano + 1}-01-01', **ug_params, **fp
        }).fetchall()
        data = [0.0] * 12
        for m, v in rows:
            if m:
                data[int(m) - 1] = round(float(v or 0), 2)
        return data

    def _mensal_simples(tabela, for_table, prefix):
        ug_frag, ug_params = _build_ug_filter(ug, prefix=prefix)
        filt, fp = _build_exec_filters(fonte, natureza, acao, for_table)
        sql = f"""
            SELECT MONTH(dataEmissao) as m, COALESCE(SUM(valor),0) as v
            FROM {tabela}
            WHERE statusDocumento='CONTABILIZADO'
              AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_frag} {filt}
            GROUP BY MONTH(dataEmissao)
        """
        rows = db.session.execute(text(sql), {
            'dt_ini': f'{ano}-01-01', 'dt_fim': f'{ano + 1}-01-01', **ug_params, **fp
        }).fetchall()
        data = [0.0] * 12
        for m, v in rows:
            if m:
                data[int(m) - 1] = round(float(v or 0), 2)
        return data

    return jsonify({
        'categories': meses,
        'series': [
            {'name': 'Reserva', 'data': _mensal('reserva', "tipoAlteracao='ANULACAO'", 'reserva', 'ugevr')},
            {'name': 'Empenho', 'data': _mensal('empenho', "tipoAlteracaoNE='ANULACAO'", 'empenho', 'ugeve')},
            {'name': 'Liquidação', 'data': _mensal('liquidacao', "tipoAlteracao='ANULACAO'", 'class', 'ugevl')},
            {'name': 'PD', 'data': _mensal_simples('pd', 'class', 'ugevpd')},
            {'name': 'OB', 'data': _mensal_simples('ob', 'class', 'ugevob')},
        ]
    })


@dashboards_bp.route('/api/pd-por-status')
@login_required
@requires_permission('dashboards')
def api_pd_por_status():
    """PD agrupado por statusExecucao (para gráfico de pizza)."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    mes, acao, natureza, fonte, ug = _get_filtros()
    dt_ini, dt_fim = _build_date_range(ano, mes)
    p = {'dt_ini': dt_ini, 'dt_fim': dt_fim}

    f_cls, fp_cls = _build_exec_filters(fonte, natureza, acao, 'class')
    ug_frag, ug_params = _build_ug_filter(ug, prefix='ugpst')

    sql = text(f"""
        SELECT statusExecucao,
               COALESCE(SUM(valor),0) as total,
               COUNT(*) as qtd_pds,
               COUNT(DISTINCT codigoCredor) as qtd_credores
        FROM pd WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_frag}
          {f_cls}
        GROUP BY statusExecucao ORDER BY total DESC
    """)
    rows = db.session.execute(sql, {**p, **ug_params, **fp_cls}).fetchall()

    # Mapear nomes amigáveis
    label_map = {
        'STATUS_EXECUTADA': 'Executada',
        'STATUS_DISPONIVEL': 'Em Aberto',
        'STATUS_ERRO': 'Com Erro',
        'STATUS_CANCELADA': 'Cancelada',
    }
    color_map = {
        'STATUS_EXECUTADA': '#198754',
        'STATUS_DISPONIVEL': '#fd7e14',
        'STATUS_ERRO': '#dc3545',
        'STATUS_CANCELADA': '#6c757d',
    }

    labels = []
    values = []
    colors = []
    qtd_pds = []
    qtd_credores = []
    for r in rows:
        status = r[0] or 'Sem status'
        labels.append(label_map.get(status, status))
        values.append(round(float(r[1] or 0), 2))
        colors.append(color_map.get(status, '#adb5bd'))
        qtd_pds.append(int(r[2] or 0))
        qtd_credores.append(int(r[3] or 0))

    return jsonify({
        'labels': labels,
        'series': values,
        'colors': colors,
        'qtd_pds': qtd_pds,
        'qtd_credores': qtd_credores,
    })


@dashboards_bp.route('/api/listagem-pd')
@login_required
@requires_permission('dashboards')
def api_listagem_pd():
    """Listagem de PDs com filtros."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    mes, acao, natureza, fonte, ug = _get_filtros()
    dt_ini, dt_fim = _build_date_range(ano, mes)
    p = {'dt_ini': dt_ini, 'dt_fim': dt_fim}

    f_cls, fp_cls = _build_exec_filters(fonte, natureza, acao, 'class')
    ug_frag, ug_params = _build_ug_filter(ug, col='p.codigoUG', prefix='uglst')

    status_execucao = request.args.get('status_execucao', '')
    f_status = ''
    if status_execucao:
        f_status = f"AND p.statusExecucao = :status_exec"
        fp_cls['status_exec'] = status_execucao

    sql = text(f"""
        SELECT p.codigo, p.statusDocumento, p.codNatureza, p.codigoCredor,
               p.nomeCredor, p.valor, p.statusExecucao, p.competencia
        FROM pd p
        WHERE p.statusDocumento='CONTABILIZADO'
          AND p.dataEmissao >= :dt_ini AND p.dataEmissao < :dt_fim {ug_frag}
          {f_cls} {f_status}
        ORDER BY p.valor DESC
    """)
    rows = db.session.execute(sql, {**p, **ug_params, **fp_cls}).fetchall()

    status_label = {
        'STATUS_EXECUTADA': 'Executada',
        'STATUS_DISPONIVEL': 'Em Aberto',
        'STATUS_ERRO': 'Com Erro',
        'STATUS_CANCELADA': 'Cancelada',
    }

    result = []
    for r in rows:
        result.append({
            'codigo': r[0] or '',
            'statusDocumento': r[1] or '',
            'codNatureza': r[2] or '',
            'credor': f"{r[3] or ''} - {r[4] or ''}",
            'valor': round(float(r[5] or 0), 2),
            'statusExecucao': status_label.get(r[6] or '', r[6] or ''),
            'statusExecucaoRaw': r[6] or '',
            'competencia': r[7] or '',
        })

    return jsonify({'rows': result})


@dashboards_bp.route('/api/pd-por-natureza')
@login_required
@requires_permission('dashboards')
def api_pd_por_natureza():
    """PD total e PD em aberto agrupados por natureza de despesa."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    mes, acao, natureza, fonte, ug = _get_filtros()
    dt_ini, dt_fim = _build_date_range(ano, mes)
    p = {'dt_ini': dt_ini, 'dt_fim': dt_fim}

    f_cls, fp_cls = _build_exec_filters(fonte, natureza, acao, 'class')
    ug_t, ugp_t = _build_ug_filter(ug, col='p.codigoUG', prefix='ugpnt')
    ug_a, ugp_a = _build_ug_filter(ug, col='p.codigoUG', prefix='ugpna')

    # PD executada por natureza (apenas STATUS_EXECUTADA)
    sql_total = text(f"""
        SELECT p.codNatureza, n.titulo, COALESCE(SUM(p.valor),0) as total
        FROM pd p
        LEFT JOIN natdespesas n ON n.codigo = p.codNatureza
        WHERE p.statusDocumento='CONTABILIZADO' AND p.statusExecucao='STATUS_EXECUTADA'
          AND p.dataEmissao >= :dt_ini AND p.dataEmissao < :dt_fim {ug_t}
          {f_cls}
        GROUP BY p.codNatureza, n.titulo HAVING total > 0 ORDER BY total DESC LIMIT 15
    """)
    rows_total = db.session.execute(sql_total, {**p, **ugp_t, **fp_cls}).fetchall()

    # PD em aberto por natureza
    sql_aberto = text(f"""
        SELECT p.codNatureza, COALESCE(SUM(p.valor),0) as total
        FROM pd p
        WHERE p.statusDocumento='CONTABILIZADO' AND p.statusExecucao='STATUS_DISPONIVEL'
          AND p.dataEmissao >= :dt_ini AND p.dataEmissao < :dt_fim {ug_a}
          {f_cls}
        GROUP BY p.codNatureza
    """)
    aberto_map = {str(r[0]): float(r[1] or 0) for r in db.session.execute(sql_aberto, {**p, **ugp_a, **fp_cls})}

    codes = []
    labels = []
    pd_values = []
    pd_aberto_values = []
    for r in rows_total:
        cod = str(r[0] or '')
        codes.append(cod)
        labels.append(f"{cod} - {(r[1] or 'Sem natureza')}")
        pd_values.append(round(float(r[2] or 0), 2))
        pd_aberto_values.append(round(aberto_map.get(cod, 0), 2))

    return jsonify({
        'categories': codes,
        'labels': labels,
        'series': [
            {'name': 'PD Executada', 'data': pd_values},
            {'name': 'PD (em aberto)', 'data': pd_aberto_values},
        ],
    })


@dashboards_bp.route('/api/execucao-por-natureza')
@login_required
@requires_permission('dashboards')
def api_execucao_por_natureza():
    """Barras por natureza de despesa com filtros."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    metrica = request.args.get('metrica', 'empenho')
    mes, acao, natureza, fonte, ug = _get_filtros()
    dt_ini, dt_fim = _build_date_range(ano, mes)

    # Filtros de fonte e natureza
    fp = {}
    if fonte:
        f_fonte = _multi_in('r.codFonte', fonte, 'fr', fp)
        fp2 = {}; f_fonte_e = _multi_in('e.codFonte', fonte, 'fe', fp2); fp.update(fp2)
        fp3 = {}; f_fonte_l = _multi_in('l.codFonte', fonte, 'fl', fp3); fp.update(fp3)
        fp4 = {}; f_fonte_p = _multi_in('p.codFonte', fonte, 'fp', fp4); fp.update(fp4)
        fp5 = {}; f_fonte_o = _multi_in('o.codFonte', fonte, 'fo', fp5); fp.update(fp5)
    else:
        f_fonte = f_fonte_e = f_fonte_l = f_fonte_p = f_fonte_o = ""

    # Filtro de natureza (filtra quais naturezas aparecem)
    if natureza:
        f_nat = _multi_in('r.codNatureza', natureza, 'nr', fp)
        fp_ne = {}; f_nat_e = _multi_in('e.codNatureza', natureza, 'ne', fp_ne); fp.update(fp_ne)
        fp_nl = {}; f_nat_l = _multi_in('l.codNatureza', natureza, 'nl', fp_nl); fp.update(fp_nl)
        fp_np = {}; f_nat_p = _multi_in('p.codNatureza', natureza, 'np', fp_np); fp.update(fp_np)
        fp_no = {}; f_nat_o = _multi_in('o.codNatureza', natureza, 'no', fp_no); fp.update(fp_no)
    else:
        f_nat = f_nat_e = f_nat_l = f_nat_p = f_nat_o = ""

    # Filtro UG por alias
    ug_r, ugp_r = _build_ug_filter(ug, col='r.codigoUG', prefix='ugenr')
    ug_e, ugp_e = _build_ug_filter(ug, col='e.codigoUG', prefix='ugene')
    ug_l, ugp_l = _build_ug_filter(ug, col='l.codigoUG', prefix='ugenl')
    ug_p, ugp_p = _build_ug_filter(ug, col='p.codigoUG', prefix='ugenp')
    ug_o, ugp_o = _build_ug_filter(ug, col='o.codigoUG', prefix='ugeno')
    fp.update(ugp_r); fp.update(ugp_e); fp.update(ugp_l); fp.update(ugp_p); fp.update(ugp_o)

    # Todas as queries retornam: codNatureza, titulo, total
    tbl_map = {
        'reserva': ('reserva r', 'r', "CASE WHEN r.tipoAlteracao='ANULACAO' THEN -r.valor ELSE r.valor END", ug_r, f_fonte, f_nat),
        'liquidacao': ('liquidacao l', 'l', "CASE WHEN l.tipoAlteracao='ANULACAO' THEN -l.valor ELSE l.valor END", ug_l, f_fonte_l, f_nat_l),
        'pd': ('pd p', 'p', 'p.valor', ug_p, f_fonte_p, f_nat_p),
        'ob': ('ob o', 'o', 'o.valor', ug_o, f_fonte_o, f_nat_o),
        'empenho': ('empenho e', 'e', "CASE WHEN e.tipoAlteracaoNE='ANULACAO' THEN -e.valor ELSE e.valor END", ug_e, f_fonte_e, f_nat_e),
    }
    tbl, alias, val_expr, ug_filt, flt_fonte, flt_nat = tbl_map.get(metrica, tbl_map['empenho'])

    sql = f"""
        SELECT {alias}.codNatureza, n.titulo, COALESCE(SUM({val_expr}),0) as total
        FROM {tbl}
        LEFT JOIN natdespesas n ON n.codigo = {alias}.codNatureza
        WHERE {alias}.statusDocumento='CONTABILIZADO'
          AND {alias}.dataEmissao >= :dt_ini AND {alias}.dataEmissao < :dt_fim {ug_filt} {flt_fonte} {flt_nat}
        GROUP BY {alias}.codNatureza, n.titulo HAVING total > 0 ORDER BY total DESC LIMIT 15
    """

    rows = db.session.execute(text(sql), {'dt_ini': dt_ini, 'dt_fim': dt_fim, **fp}).fetchall()

    codes = [str(r[0] or '') for r in rows]
    labels = [f"{r[0] or ''} - {(r[1] or 'Sem natureza')}" for r in rows]
    values = [round(float(r[2] or 0), 2) for r in rows]

    return jsonify({
        'categories': codes,
        'labels': labels,
        'series': [{'name': metrica.capitalize(), 'data': values}]
    })


@dashboards_bp.route('/api/tabela-contratos-orcamentario')
@login_required
@requires_permission('dashboards')
def api_tabela_contratos_orcamentario():
    """Tabela com filtros: mes, acao, natureza, fonte, ug."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    mes, acao, natureza, fonte, ug = _get_filtros()
    dt_ini, dt_fim = _build_date_range(ano, mes)
    p = {'dt_ini': dt_ini, 'dt_fim': dt_fim}

    f_emp, fp_emp = _build_exec_filters(fonte, natureza, acao, 'empenho')
    f_res, fp_res = _build_exec_filters(fonte, natureza, acao, 'reserva')
    f_cls, fp_cls = _build_exec_filters(fonte, natureza, acao, 'class')

    ug_e, ugp_e = _build_ug_filter(ug, prefix='ugtbe')
    ug_l, ugp_l = _build_ug_filter(ug, prefix='ugtbl')
    ug_r, ugp_r = _build_ug_filter(ug, prefix='ugtbr')
    ug_pd, ugp_pd = _build_ug_filter(ug, prefix='ugtbpd')
    ug_pda, ugp_pda = _build_ug_filter(ug, prefix='ugtbpda')
    ug_pdad, ugp_pdad = _build_ug_filter(ug, prefix='ugtbpdad')
    ug_ob, ugp_ob = _build_ug_filter(ug, prefix='ugtbob')

    emp_sql = text(f"""
        SELECT codContrato, SUM(CASE WHEN tipoAlteracaoNE='ANULACAO' THEN -valor ELSE valor END) as total
        FROM empenho WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_e}
          AND codContrato IS NOT NULL {f_emp}
        GROUP BY codContrato
    """)
    emp_map = {str(r[0]): float(r[1] or 0) for r in db.session.execute(emp_sql, {**p, **ugp_e, **fp_emp})}

    liq_sql = text(f"""
        SELECT codContrato, SUM(CASE WHEN tipoAlteracao='ANULACAO' THEN -valor ELSE valor END) as total
        FROM liquidacao WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_l}
          AND codContrato IS NOT NULL {f_cls}
        GROUP BY codContrato
    """)
    liq_map = {str(r[0]): float(r[1] or 0) for r in db.session.execute(liq_sql, {**p, **ugp_l, **fp_cls})}

    res_sql = text(f"""
        SELECT codContrato, SUM(CASE WHEN tipoAlteracao='ANULACAO' THEN -valor ELSE valor END) as total
        FROM reserva WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_r}
          AND codContrato IS NOT NULL {f_res}
        GROUP BY codContrato
    """)
    res_map = {str(r[0]): float(r[1] or 0) for r in db.session.execute(res_sql, {**p, **ugp_r, **fp_res})}

    pd_sql = text(f"""
        SELECT codContrato, SUM(valor) as total
        FROM pd WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_pd}
          AND codContrato IS NOT NULL {f_cls}
        GROUP BY codContrato
    """)
    pd_map = {str(r[0]): float(r[1] or 0) for r in db.session.execute(pd_sql, {**p, **ugp_pd, **fp_cls})}

    pd_aberto_sql = text(f"""
        SELECT codContrato, SUM(valor) as total
        FROM pd WHERE statusDocumento='CONTABILIZADO' AND statusExecucao='STATUS_DISPONIVEL'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_pda}
          AND codContrato IS NOT NULL {f_cls}
        GROUP BY codContrato
    """)
    pd_aberto_map = {str(r[0]): float(r[1] or 0) for r in db.session.execute(pd_aberto_sql, {**p, **ugp_pda, **fp_cls})}

    # Detalhe PDs em aberto por contrato (codigo, competencia, valor)
    pd_aberto_detail_sql = text(f"""
        SELECT codContrato, codigo, competencia, valor
        FROM pd WHERE statusDocumento='CONTABILIZADO' AND statusExecucao='STATUS_DISPONIVEL'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_pdad}
          AND codContrato IS NOT NULL {f_cls}
        ORDER BY codContrato, valor DESC
    """)
    pd_aberto_detail = {}  # codContrato -> [{codigo, competencia, valor}]
    for r in db.session.execute(pd_aberto_detail_sql, {**p, **ugp_pdad, **fp_cls}):
        key = str(r[0])
        if key not in pd_aberto_detail:
            pd_aberto_detail[key] = []
        pd_aberto_detail[key].append({
            'codigo': r[1] or '',
            'competencia': r[2] or '',
            'valor': round(float(r[3] or 0), 2),
        })

    ob_sql = text(f"""
        SELECT codContrato, SUM(valor) as total
        FROM ob WHERE statusDocumento='CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim {ug_ob}
          AND codContrato IS NOT NULL {f_cls}
        GROUP BY codContrato
    """)
    ob_map = {str(r[0]): float(r[1] or 0) for r in db.session.execute(ob_sql, {**p, **ugp_ob, **fp_cls})}

    # Union de todos os códigos de contrato
    all_cods = set()
    for m in [res_map, emp_map, liq_map, pd_map, pd_aberto_map, ob_map]:
        all_cods.update(m.keys())

    # Buscar nomes dos contratos em batch
    contratos_map = {}
    if all_cods:
        all_contratos = Contrato.query.filter(
            func.replace(func.replace(Contrato.codigo, '.', ''), '/', '').in_(list(all_cods))
        ).all()
        for c in all_contratos:
            key = c.codigo.replace('.', '').replace('/', '')
            contratos_map[key] = c

    # Montar tabela
    rows = []
    for cod in sorted(all_cods):
        contrato = contratos_map.get(cod)
        nome = (contrato.nomeContratado or contrato.nomeContratadoResumido) if contrato else ''
        codigo_fmt = contrato.codigo if contrato else cod
        rows.append({
            'contrato': codigo_fmt,
            'credor': nome or f'Contrato {cod}',
            'objeto': (contrato.objeto or '') if contrato else '',
            'reserva': round(res_map.get(cod, 0), 2),
            'empenho': round(emp_map.get(cod, 0), 2),
            'liquidacao': round(liq_map.get(cod, 0), 2),
            'pd': round(pd_map.get(cod, 0), 2),
            'pd_aberto': round(pd_aberto_map.get(cod, 0), 2),
            'pd_aberto_pds': pd_aberto_detail.get(cod, []),
            'ob': round(ob_map.get(cod, 0), 2),
        })

    # Ordenar por empenho desc
    rows.sort(key=lambda r: r['empenho'], reverse=True)

    return jsonify({'rows': rows})
