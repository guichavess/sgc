"""
API endpoints para graficos dos Dashboards.
Retornam JSON consumido pelo ApexCharts no frontend.
Acesso restrito a administradores (is_admin=True).
"""
from datetime import datetime, date, timedelta
import calendar

from flask import jsonify, request
from flask_login import login_required
from sqlalchemy import func, case, extract

from app.dashboards.routes import dashboards_bp
from app.models import (
    Solicitacao, Contrato, SolicitacaoEmpenho,
    Empenho, Liquidacao, Etapa, HistoricoMovimentacao,
)
from app.extensions import db
from app.repositories import SolicitacaoRepository, EmpenhoRepository, LiquidacaoRepository
from app.utils.permissions import requires_admin
from app.constants import UG_CODE


# ---------------------------------------------------------------------------
# Consolidado
# ---------------------------------------------------------------------------

@dashboards_bp.route('/api/status-solicitacoes')
@login_required
@requires_admin
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
@requires_admin
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
@requires_admin
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

    categories = []
    values = []
    for cod_contrato, total in result:
        if not cod_contrato:
            continue
        cod_str = str(cod_contrato)
        contrato = Contrato.query.filter(
            func.replace(func.replace(Contrato.codigo, '.', ''), '/', '') == cod_str
        ).first()
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
@requires_admin
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
@requires_admin
def api_tempo_medio_etapa():
    """Bar chart: tempo medio em dias por etapa."""
    etapas = Etapa.query.order_by(Etapa.ordem).all()
    etapa_map = {e.id: (e.alias or e.nome) for e in etapas}
    etapa_ordem = {e.id: e.ordem for e in etapas}

    # Calcular tempo medio por etapa via historico
    # Agrupa movimentacoes por solicitacao, calcula diff entre entrada e saida de cada etapa
    subq = db.session.query(
        HistoricoMovimentacao.id_solicitacao,
        HistoricoMovimentacao.id_etapa_nova,
        HistoricoMovimentacao.data_movimentacao,
    ).order_by(
        HistoricoMovimentacao.id_solicitacao,
        HistoricoMovimentacao.data_movimentacao,
    ).all()

    # Agrupar por solicitacao
    from collections import defaultdict
    sol_movs = defaultdict(list)
    for id_sol, id_etapa, data_mov in subq:
        sol_movs[id_sol].append((id_etapa, data_mov))

    # Calcular tempo por etapa
    etapa_tempos = defaultdict(list)
    for id_sol, movs in sol_movs.items():
        for i, (id_etapa, data_entrada) in enumerate(movs):
            if i + 1 < len(movs):
                _, data_saida = movs[i + 1]
                dias = (data_saida - data_entrada).total_seconds() / 86400
                if dias >= 0:
                    etapa_tempos[id_etapa].append(dias)

    categories = []
    data = []
    for etapa_id in sorted(etapa_tempos.keys(), key=lambda x: etapa_ordem.get(x, 999)):
        tempos = etapa_tempos[etapa_id]
        if tempos:
            media = sum(tempos) / len(tempos)
            nome = etapa_map.get(etapa_id, f'Etapa {etapa_id}')
            categories.append(nome)
            data.append(round(media, 1))

    return jsonify({'categories': categories, 'data': data})


@dashboards_bp.route('/api/distribuicao-competencia')
@login_required
@requires_admin
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
@requires_admin
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

    categories = []
    empenhados = []
    liquidados = []

    for cod_contrato, total_emp in top_contratos:
        if not cod_contrato:
            continue
        cod_str = str(cod_contrato)
        contrato = Contrato.query.filter(
            func.replace(func.replace(Contrato.codigo, '.', ''), '/', '') == cod_str
        ).first()
        nome = (contrato.nomeContratadoResumido if contrato else f'Contrato {cod_contrato}') or ''

        # Liquidado para o mesmo contrato
        vlr_liq = case(
            (Liquidacao.statusDocumento == 'ANULADO', Liquidacao.valor * -1),
            else_=Liquidacao.valor
        )
        total_liq = db.session.query(func.sum(vlr_liq)).filter(
            Liquidacao.codigoUG == UG_CODE,
            Liquidacao.codContrato == cod_contrato,
            Liquidacao.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
            Liquidacao.dataEmissao >= date(ano, 1, 1),
            Liquidacao.dataEmissao <= date(ano, 12, 31),
        ).scalar()

        categories.append(nome[:25])
        empenhados.append(round(float(total_emp), 2))
        liquidados.append(round(float(total_liq or 0), 2))

    return jsonify({
        'categories': categories,
        'series': [
            {'name': 'Empenhado', 'data': empenhados},
            {'name': 'Liquidado', 'data': liquidados},
        ]
    })


@dashboards_bp.route('/api/evolucao-empenhos')
@login_required
@requires_admin
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
@requires_admin
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

    categories = []
    saldos = []

    for cod_contrato, total_emp in top_contratos:
        if not cod_contrato:
            continue
        cod_str = str(cod_contrato)
        contrato = Contrato.query.filter(
            func.replace(func.replace(Contrato.codigo, '.', ''), '/', '') == cod_str
        ).first()
        nome = (contrato.nomeContratadoResumido if contrato else f'Contrato {cod_contrato}') or ''

        vlr_liq = case(
            (Liquidacao.statusDocumento == 'ANULADO', Liquidacao.valor * -1),
            else_=Liquidacao.valor
        )
        total_liq = db.session.query(func.sum(vlr_liq)).filter(
            Liquidacao.codigoUG == UG_CODE,
            Liquidacao.codContrato == cod_contrato,
            Liquidacao.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
            Liquidacao.dataEmissao >= date(ano, 1, 1),
            Liquidacao.dataEmissao <= date(ano, 12, 31),
        ).scalar()

        saldo = max(0.0, float(total_emp) - float(total_liq or 0))
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
@requires_admin
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
@requires_admin
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
@requires_admin
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
@requires_admin
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
@requires_admin
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
@requires_admin
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
@requires_admin
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
