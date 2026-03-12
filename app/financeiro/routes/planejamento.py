"""
Rotas do Planejamento Orçamentário (Financeiro).
Páginas: Lançar Planejamento e Relatório de Execução.
"""
from functools import wraps
from datetime import datetime
from decimal import Decimal
from flask import render_template, request, jsonify, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import func, case, extract, text

from app.financeiro.routes import financeiro_bp
from app.extensions import db
from app.models.empenho import Empenho
from app.models.empenho_item import EmpenhoItem, ClassSubItemDespesa
from app.models.execucao_orcamentaria import ExecucaoOrcamentaria
from app.models.planejamento_orcamentario import PlanejamentoOrcamentario
from app.services.prestacao_contrato_service import PrestacaoContratoService


# ── Controle de acesso ──────────────────────────────────────────────────────

def _requires_admin_or_pedro(f):
    """Permite acesso a admins gerais e ao Pedro Alexandre."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        is_pedro = current_user.nome and 'PEDRO ALEXANDRE' in current_user.nome.upper()
        if not current_user.is_admin and not is_pedro:
            flash('Acesso restrito.', 'danger')
            return redirect(url_for('hub'))
        return f(*args, **kwargs)
    return decorated


# ── Helpers ─────────────────────────────────────────────────────────────────

ZERO = Decimal('0')
UG = '210101'

MESES = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
NOMES_MESES = {
    '01': 'Jan', '02': 'Fev', '03': 'Mar', '04': 'Abr',
    '05': 'Mai', '06': 'Jun', '07': 'Jul', '08': 'Ago',
    '09': 'Set', '10': 'Out', '11': 'Nov', '12': 'Dez',
}
NOMES_MESES_COMPLETO = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
}
ANOS_DISPONIVEIS = [2024, 2025, 2026]

# "De para" de labels de situação
SITUACAO_LABELS = {
    'A_CONTRATAR': 'A Contratar',
    'EM_VIGOR': 'Em Vigor',
    'ENCERRADO': 'Encerrado',
    'LICITADO': 'Licitado',
}


def _format_brl(val):
    """Formata valor como R$ 1.234,56."""
    if val is None:
        return 'R$ 0,00'
    v = Decimal(str(val))
    sinal = '-' if v < 0 else ''
    v = abs(v)
    inteiro = int(v)
    centavos = int(round((v - inteiro) * 100))
    parte_int = f'{inteiro:,}'.replace(',', '.')
    return f'{sinal}R$ {parte_int},{centavos:02d}'


def _parse_brl(val_str):
    """Converte '1.234,56' ou '-' para Decimal."""
    if not val_str or val_str.strip() == '-':
        return Decimal('0')
    # Remove pontos de milhar e troca vírgula por ponto
    limpo = val_str.strip().replace('.', '').replace(',', '.')
    try:
        return Decimal(limpo)
    except Exception:
        return Decimal('0')


def _format_num(val):
    """Formata valor como 1.234,56 (sem R$)."""
    if val is None or val == 0:
        return '-'
    v = Decimal(str(val))
    sinal = '-' if v < 0 else ''
    v = abs(v)
    inteiro = int(v)
    centavos = int(round((v - inteiro) * 100))
    parte_int = f'{inteiro:,}'.replace(',', '.')
    return f'{sinal}{parte_int},{centavos:02d}'


def _format_num_inteiro(val):
    """Formata valor como 1.234.567 (sem decimais, sem R$)."""
    if val is None or val == 0:
        return '0'
    v = Decimal(str(val))
    sinal = '-' if v < 0 else ''
    inteiro = int(abs(v))
    return f'{sinal}{inteiro:,}'.replace(',', '.')


def _pct(parte, total):
    """Calcula percentual seguro."""
    if not total or total == 0:
        return ZERO
    return (Decimal(str(parte)) / Decimal(str(total)) * 100).quantize(Decimal('0.01'))


def _decimal(val):
    """Converte para Decimal seguro."""
    if val is None:
        return ZERO
    try:
        return Decimal(str(val))
    except Exception:
        return ZERO


# ── Página: Lançar Planejamento ─────────────────────────────────────────────

@financeiro_bp.route('/planejamento')
@login_required
@_requires_admin_or_pedro
def planejamento_index():
    """Página principal — Lançar Planejamento Orçamentário."""
    ano = datetime.now().year

    # ── Filtros (mesmos da página de contratos) — todos multi-select ──
    filtro_codigo = request.args.get('codigo', '').strip()
    filtro_contratado = request.args.get('contratado', '').strip()
    filtro_situacao = [v.strip() for v in request.args.getlist('situacao') if v.strip()]
    filtro_natureza = [int(v) for v in request.args.getlist('natureza') if v.strip()]
    filtro_tipo_execucao = [int(v) for v in request.args.getlist('tipo_execucao') if v.strip()]
    filtro_centro_custo = [int(v) for v in request.args.getlist('centro_custo') if v.strip()]
    filtro_tipo_contrato = [v.strip() for v in request.args.getlist('tipo_contrato') if v.strip()]
    filtro_pdm = [int(v) for v in request.args.getlist('pdm') if v.strip()]
    filtro_subitem = [v.strip() for v in request.args.getlist('subitem_despesa') if v.strip()]
    filtro_tipo_patrimonial = [v.strip() for v in request.args.getlist('tipo_patrimonial') if v.strip()]
    page = request.args.get('page', 1, type=int)

    # ── Contratos paginados com filtros ──
    pagination = PrestacaoContratoService.listar_contratos_paginado(
        codigo=filtro_codigo or None,
        contratado=filtro_contratado or None,
        situacao=filtro_situacao or None,
        natureza_codigo=filtro_natureza or None,
        tipo_execucao_id=filtro_tipo_execucao or None,
        centro_de_custo_id=filtro_centro_custo or None,
        tipo_contrato=filtro_tipo_contrato or None,
        pdm_id=filtro_pdm or None,
        subitem_despesa=filtro_subitem or None,
        tipo_patrimonial=filtro_tipo_patrimonial or None,
        page=page,
        per_page=20
    )

    codigos_pagina = [c.codigo for c in pagination.items]

    # ── Valores mensais (execuções orçamentárias do ano corrente) ──
    exec_data = {}
    if codigos_pagina:
        rows = db.session.query(
            ExecucaoOrcamentaria.cod_contrato,
            ExecucaoOrcamentaria.competencia,
            func.coalesce(func.sum(ExecucaoOrcamentaria.valor), 0).label('total_valor')
        ).filter(
            ExecucaoOrcamentaria.cod_contrato.in_(codigos_pagina),
            ExecucaoOrcamentaria.competencia.like(f'%/{ano}')
        ).group_by(
            ExecucaoOrcamentaria.cod_contrato,
            ExecucaoOrcamentaria.competencia
        ).all()

        for row in rows:
            cod = row.cod_contrato
            if cod not in exec_data:
                exec_data[cod] = {}
            mm = row.competencia[:2] if row.competencia else None
            if mm:
                exec_data[cod][mm] = float(row.total_valor or 0)

    # ── Planejamentos já salvos ──
    plan_data = {}
    if codigos_pagina:
        plans = PlanejamentoOrcamentario.query.filter(
            PlanejamentoOrcamentario.cod_contrato.in_(codigos_pagina)
        ).all()
        for p in plans:
            cod = p.cod_contrato
            if cod not in plan_data:
                plan_data[cod] = {
                    'planejamento_inicial': p.planejamento_inicial,
                    'repactuacao_prorrogacao': p.repactuacao_prorrogacao,
                    'valores': {},
                }
            # Extrair mês da competência (MM/YYYY → MM)
            if p.competencia and '/' in p.competencia:
                mm = p.competencia.split('/')[0]
                plan_data[cod]['valores'][mm] = float(p.valor or 0)
            # Atualizar booleans com o registro mais recente
            plan_data[cod]['planejamento_inicial'] = p.planejamento_inicial
            plan_data[cod]['repactuacao_prorrogacao'] = p.repactuacao_prorrogacao

    # ── Montar dados da tabela ──
    contratos_table = []
    for contrato in pagination.items:
        cod = contrato.codigo
        plan = plan_data.get(cod)
        meses_valores = {}
        for m in MESES:
            # Priorizar valores já salvos no planejamento; senão usar execução
            if plan and m in plan.get('valores', {}):
                meses_valores[m] = plan['valores'][m]
            else:
                meses_valores[m] = exec_data.get(cod, {}).get(m, 0)
        contratos_table.append({
            'codigo': cod,
            'nomeContratante': contrato.nomeContratado or contrato.nomeContratadoResumido or '',
            'meses': meses_valores,
            'planejamento': plan,
        })

    # ── Dados para selects de filtro ──
    todas_situacoes = PrestacaoContratoService.listar_situacoes()
    todos_tipos_execucao = PrestacaoContratoService.listar_tipos_execucao()
    todos_centros_custo = PrestacaoContratoService.listar_centros_de_custo()
    todos_pdms = PrestacaoContratoService.listar_pdms_utilizados()
    todas_naturezas = PrestacaoContratoService.listar_naturezas_utilizadas()
    todos_subitens = PrestacaoContratoService.listar_subitens_utilizados()
    todos_tipos_patrimoniais = PrestacaoContratoService.listar_tipos_patrimoniais_utilizados()

    tem_filtro = any([filtro_codigo, filtro_contratado, filtro_situacao,
                      filtro_natureza, filtro_tipo_execucao, filtro_centro_custo,
                      filtro_tipo_contrato, filtro_pdm, filtro_subitem,
                      filtro_tipo_patrimonial])

    return render_template(
        'financeiro/planejamento.html',
        contratos=contratos_table,
        pagination=pagination,
        ano=ano,
        meses=MESES,
        nomes_meses=NOMES_MESES,
        format_num=_format_num,
        situacao_labels=SITUACAO_LABELS,
        # Dados dos filtros
        todas_situacoes=todas_situacoes,
        todos_tipos_execucao=todos_tipos_execucao,
        todos_centros_custo=todos_centros_custo,
        todos_pdms=todos_pdms,
        todas_naturezas=todas_naturezas,
        todos_subitens=todos_subitens,
        todos_tipos_patrimoniais=todos_tipos_patrimoniais,
        # Valores selecionados nos filtros
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
        tem_filtro=tem_filtro,
    )


# ── Helpers internos do relatório ──────────────────────────────────────────

def _parse_filtros_contrato():
    """Extrai filtros de contrato do request.args (comuns entre as páginas, todos multi-select)."""
    return {
        'codigo': request.args.get('codigo', '').strip() or None,
        'contratado': request.args.get('contratado', '').strip() or None,
        'situacao': [v.strip() for v in request.args.getlist('situacao') if v.strip()] or None,
        'natureza_codigo': [int(v) for v in request.args.getlist('natureza') if v.strip()] or None,
        'tipo_execucao_id': [int(v) for v in request.args.getlist('tipo_execucao') if v.strip()] or None,
        'centro_de_custo_id': [int(v) for v in request.args.getlist('centro_custo') if v.strip()] or None,
        'tipo_contrato': [v.strip() for v in request.args.getlist('tipo_contrato') if v.strip()] or None,
        'pdm_id': [int(v) for v in request.args.getlist('pdm') if v.strip()] or None,
        'subitem_despesa': [v.strip() for v in request.args.getlist('subitem_despesa') if v.strip()] or None,
        'tipo_patrimonial': [v.strip() for v in request.args.getlist('tipo_patrimonial') if v.strip()] or None,
    }


def _build_cod_maps(contratos):
    """Constrói mapeamentos de códigos formatados ↔ numéricos."""
    num_to_cod = {}
    num_codes_int = []
    for c in contratos:
        num = c.codigo.replace('.', '').replace('/', '')
        num_to_cod[num] = c.codigo
        try:
            num_codes_int.append(int(num))
        except ValueError:
            pass
    return num_to_cod, num_codes_int


def _query_planejado(codigos, ano, mes=None):
    """Retorna planejado total, por mês, por contrato e flag planejamento_inicial."""
    total = ZERO
    by_month = {}
    by_contract = {}
    ini_by_contract = {}

    if not codigos:
        return total, by_month, by_contract, ini_by_contract

    q = PlanejamentoOrcamentario.query.filter(
        PlanejamentoOrcamentario.cod_contrato.in_(codigos),
        PlanejamentoOrcamentario.competencia.like(f'%/{ano}')
    )
    if mes:
        q = q.filter(PlanejamentoOrcamentario.competencia == f'{mes:02d}/{ano}')

    for p in q.all():
        val = _decimal(p.valor)
        total += val
        by_contract[p.cod_contrato] = by_contract.get(p.cod_contrato, ZERO) + val
        ini_by_contract[p.cod_contrato] = p.planejamento_inicial
        if p.competencia and '/' in p.competencia:
            mm = p.competencia.split('/')[0]
            by_month[mm] = by_month.get(mm, ZERO) + val

    return total, by_month, by_contract, ini_by_contract


def _query_empenhado(num_codes_int, num_to_cod, dt_ini, dt_fim):
    """Retorna empenhado total, por mês e por contrato (código formatado).
    Usado para a tabela por contrato."""
    total = ZERO
    by_month = {}
    by_contract = {}

    if not num_codes_int:
        return total, by_month, by_contract

    emp_value = case(
        (Empenho.tipoAlteracaoNE == 'ANULACAO', -Empenho.valor),
        else_=Empenho.valor
    )
    rows = db.session.query(
        Empenho.codContrato,
        extract('month', Empenho.dataEmissao).label('mes'),
        func.sum(emp_value).label('total')
    ).filter(
        Empenho.statusDocumento == 'CONTABILIZADO',
        Empenho.codigoUG == UG,
        Empenho.dataEmissao >= dt_ini,
        Empenho.dataEmissao < dt_fim,
        Empenho.codContrato.in_(num_codes_int)
    ).group_by(
        Empenho.codContrato, extract('month', Empenho.dataEmissao)
    ).all()

    for row in rows:
        cod_num = str(row.codContrato)
        cod_fmt = num_to_cod.get(cod_num, cod_num)
        val = _decimal(row.total)
        total += val
        by_contract[cod_fmt] = by_contract.get(cod_fmt, ZERO) + val
        mm = f'{int(row.mes):02d}'
        by_month[mm] = by_month.get(mm, ZERO) + val

    return total, by_month, by_contract


def _query_empenhado_geral(dt_ini, dt_fim, filtro_natureza=None):
    """Retorna empenhado GERAL (sem filtro por contrato), agrupado por natureza e mês.
    Usado para KPIs e tabela por natureza."""
    total = ZERO
    by_month = {}
    by_natureza = {}

    params = {'dt_ini': dt_ini, 'dt_fim': dt_fim}
    filtro_nat = ''
    if filtro_natureza:
        placeholders = ','.join([f':n{i}' for i in range(len(filtro_natureza))])
        for i, n in enumerate(filtro_natureza):
            params[f'n{i}'] = n
        filtro_nat = f'AND codNatureza IN ({placeholders})'

    sql = text(f"""
        SELECT codNatureza, MONTH(dataEmissao) AS mes,
               SUM(CASE WHEN tipoAlteracaoNE = 'ANULACAO' THEN -valor ELSE valor END) AS total
        FROM empenho
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          {filtro_nat}
        GROUP BY codNatureza, MONTH(dataEmissao)
    """)

    try:
        for row in db.session.execute(sql, params).fetchall():
            nat = str(row[0]) if row[0] else 'Sem Natureza'
            val = _decimal(row[2])
            total += val
            by_natureza[nat] = by_natureza.get(nat, ZERO) + val
            mm = f'{int(row[1]):02d}'
            by_month[mm] = by_month.get(mm, ZERO) + val
    except Exception:
        pass

    return total, by_month, by_natureza


def _query_liquidado(num_codes_int, num_to_cod, dt_ini, dt_fim):
    """Retorna liquidado total, por mês e por contrato via raw SQL.
    Usado para a tabela por contrato."""
    total = ZERO
    by_month = {}
    by_contract = {}

    if not num_codes_int:
        return total, by_month, by_contract

    placeholders = ','.join([f':c{i}' for i in range(len(num_codes_int))])
    params = {f'c{i}': v for i, v in enumerate(num_codes_int)}
    params['dt_ini'] = dt_ini
    params['dt_fim'] = dt_fim

    sql = text(f"""
        SELECT codContrato, MONTH(dataEmissao) AS mes,
               SUM(CASE WHEN tipoAlteracao = 'ANULACAO' THEN -valor ELSE valor END) AS total
        FROM liquidacao
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codContrato IN ({placeholders})
        GROUP BY codContrato, MONTH(dataEmissao)
    """)

    try:
        for row in db.session.execute(sql, params).fetchall():
            cod_num = str(row[0])
            cod_fmt = num_to_cod.get(cod_num, cod_num)
            val = _decimal(row[2])
            total += val
            by_contract[cod_fmt] = by_contract.get(cod_fmt, ZERO) + val
            mm = f'{int(row[1]):02d}'
            by_month[mm] = by_month.get(mm, ZERO) + val
    except Exception:
        pass

    return total, by_month, by_contract


def _query_liquidado_geral(dt_ini, dt_fim, filtro_natureza=None):
    """Retorna liquidado GERAL (sem filtro por contrato), agrupado por natureza e mês.
    Usado para KPIs e tabela por natureza."""
    total = ZERO
    by_month = {}
    by_natureza = {}

    params = {'dt_ini': dt_ini, 'dt_fim': dt_fim}
    filtro_nat = ''
    if filtro_natureza:
        placeholders = ','.join([f':n{i}' for i in range(len(filtro_natureza))])
        for i, n in enumerate(filtro_natureza):
            params[f'n{i}'] = n
        filtro_nat = f'AND codNatureza IN ({placeholders})'

    sql = text(f"""
        SELECT codNatureza, MONTH(dataEmissao) AS mes,
               SUM(CASE WHEN tipoAlteracao = 'ANULACAO' THEN -valor ELSE valor END) AS total
        FROM liquidacao
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          {filtro_nat}
        GROUP BY codNatureza, MONTH(dataEmissao)
    """)

    try:
        for row in db.session.execute(sql, params).fetchall():
            nat = str(row[0]) if row[0] else 'Sem Natureza'
            val = _decimal(row[2])
            total += val
            by_natureza[nat] = by_natureza.get(nat, ZERO) + val
            mm = f'{int(row[1]):02d}'
            by_month[mm] = by_month.get(mm, ZERO) + val
    except Exception:
        pass

    return total, by_month, by_natureza


def _query_planejado_por_natureza(ano, mes=None, filtro_natureza=None):
    """Retorna planejado agrupado por cod_natureza. Para a tabela por natureza."""
    total = ZERO
    by_natureza = {}
    by_month = {}

    q = db.session.query(
        PlanejamentoOrcamentario.cod_natureza,
        PlanejamentoOrcamentario.competencia,
        func.sum(PlanejamentoOrcamentario.valor).label('total')
    ).filter(
        PlanejamentoOrcamentario.competencia.like(f'%/{ano}')
    ).group_by(
        PlanejamentoOrcamentario.cod_natureza,
        PlanejamentoOrcamentario.competencia
    )

    if mes:
        q = q.filter(PlanejamentoOrcamentario.competencia == f'{mes:02d}/{ano}')

    if filtro_natureza:
        str_nats = [str(n) for n in filtro_natureza]
        q = q.filter(PlanejamentoOrcamentario.cod_natureza.in_(str_nats))

    for row in q.all():
        nat = str(row.cod_natureza) if row.cod_natureza else 'Sem Natureza'
        val = _decimal(row.total)
        total += val
        by_natureza[nat] = by_natureza.get(nat, ZERO) + val
        if row.competencia and '/' in row.competencia:
            mm = row.competencia.split('/')[0]
            by_month[mm] = by_month.get(mm, ZERO) + val

    return total, by_month, by_natureza


def _query_dotacao_atualizada(ano, filtro_natureza=None):
    """Retorna dotação atualizada do LOA (mesma fórmula da tela Orçamentária)."""
    CONTA_INICIAL = '522110101'
    CONTA_SUPLEMENTAR = '522120101'
    CONTA_ESPECIAIS_ABERTO = '522120201'
    CONTA_CANCELADO = '522190401'
    CONTA_BLOQUEADO = '622120106'

    params = {'ano': ano}
    filtro_nat = ''
    if filtro_natureza:
        if isinstance(filtro_natureza, list):
            placeholders = ','.join([f':nat{i}' for i in range(len(filtro_natureza))])
            for i, n in enumerate(filtro_natureza):
                params[f'nat{i}'] = n
            filtro_nat = f'AND codNatureza IN ({placeholders})'
        else:
            filtro_nat = 'AND codNatureza = :nat'
            params['nat'] = filtro_natureza

    # Dotação Inicial (sempre mês 1)
    sql_ini = text(f"""
        SELECT COALESCE(SUM(saldo), 0) FROM loa
        WHERE ano = :ano AND id = :conta AND mes = 1 {filtro_nat}
    """)
    try:
        dot_inicial = _decimal(db.session.execute(sql_ini, {**params, 'conta': CONTA_INICIAL}).scalar() or 0)
    except Exception:
        dot_inicial = ZERO

    # Demais contas: MAX(mes) do ano (snapshot mais recente)
    sql_max = text(f"""
        SELECT id, COALESCE(SUM(saldo), 0) as total FROM loa
        WHERE ano = :ano
          AND mes = (SELECT MAX(mes) FROM loa WHERE ano = :ano)
          {filtro_nat}
        GROUP BY id
    """)
    try:
        rows = db.session.execute(sql_max, params).fetchall()
        totais = {r[0]: _decimal(r[1]) for r in rows}
    except Exception:
        totais = {}

    dot_suplementar = totais.get(CONTA_SUPLEMENTAR, ZERO)
    dot_especiais = totais.get(CONTA_ESPECIAIS_ABERTO, ZERO)
    dot_cancelado = totais.get(CONTA_CANCELADO, ZERO)
    dot_bloqueado = totais.get(CONTA_BLOQUEADO, ZERO)

    return dot_inicial + dot_suplementar + dot_especiais - abs(dot_cancelado) - abs(dot_bloqueado)


# ── Página: Relatório de Execução ─────────────────────────────────────────

@financeiro_bp.route('/planejamento/relatorio')
@login_required
@_requires_admin_or_pedro
def planejamento_relatorio():
    """Dashboard — Relatório Planejado vs. Liquidado (estilo Power BI)."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    filtro_mes_rel = request.args.get('mes', type=int) or None
    page = request.args.get('page', 1, type=int)

    # ── Filtros de contrato ──
    f = _parse_filtros_contrato()

    # Filtros brutos (para repassar ao template) — todos multi-select
    filtro_codigo = request.args.get('codigo', '').strip()
    filtro_contratado = request.args.get('contratado', '').strip()
    filtro_situacao = [v.strip() for v in request.args.getlist('situacao') if v.strip()]
    filtro_natureza = [int(v) for v in request.args.getlist('natureza') if v.strip()]
    filtro_tipo_execucao = [int(v) for v in request.args.getlist('tipo_execucao') if v.strip()]
    filtro_centro_custo = [int(v) for v in request.args.getlist('centro_custo') if v.strip()]
    filtro_tipo_contrato = [v.strip() for v in request.args.getlist('tipo_contrato') if v.strip()]
    filtro_pdm = [int(v) for v in request.args.getlist('pdm') if v.strip()]
    filtro_subitem = [v.strip() for v in request.args.getlist('subitem_despesa') if v.strip()]
    filtro_tipo_patrimonial = [v.strip() for v in request.args.getlist('tipo_patrimonial') if v.strip()]

    # ── Todos os contratos filtrados (sem paginação, para KPIs/chart) ──
    all_pg = PrestacaoContratoService.listar_contratos_paginado(**f, page=1, per_page=10000)
    all_contracts = all_pg.items
    codigos = [c.codigo for c in all_contracts]

    # ── Paginação para tabela de detalhes ──
    pagination = PrestacaoContratoService.listar_contratos_paginado(**f, page=page, per_page=20)

    # ── Mapeamento de códigos ──
    num_to_cod, num_codes_int = _build_cod_maps(all_contracts)

    # ── Date range ──
    if filtro_mes_rel:
        dt_ini = f'{ano}-{filtro_mes_rel:02d}-01'
        dt_fim = f'{ano}-{filtro_mes_rel + 1:02d}-01' if filtro_mes_rel < 12 else f'{ano + 1}-01-01'
    else:
        dt_ini = f'{ano}-01-01'
        dt_fim = f'{ano + 1}-01-01'

    # ══════════════════════════════════════════════════════════════════
    # QUERIES DE DADOS
    # ══════════════════════════════════════════════════════════════════
    # Planejado por contrato (para tabela de contratos)
    plan_total_contratos, plan_by_month_contratos, plan_by_contract, plan_ini = _query_planejado(codigos, ano, filtro_mes_rel)
    # Empenhado/Liquidado por contrato (para tabela de contratos)
    _, _, emp_by_contract = _query_empenhado(num_codes_int, num_to_cod, dt_ini, dt_fim)
    _, _, liq_by_contract = _query_liquidado(num_codes_int, num_to_cod, dt_ini, dt_fim)

    # Empenhado/Liquidado GERAL (todos empenhos da UG, sem filtro contrato) — para KPIs e nat_table
    emp_total, emp_by_month, emp_by_natureza = _query_empenhado_geral(dt_ini, dt_fim, filtro_natureza or None)
    liq_total, liq_by_month, liq_by_natureza = _query_liquidado_geral(dt_ini, dt_fim, filtro_natureza or None)

    # Planejado por natureza (para nat_table)
    plan_total, plan_by_month, plan_by_natureza = _query_planejado_por_natureza(ano, filtro_mes_rel, filtro_natureza or None)

    # Dotação atualizada
    dot_atualizada = _query_dotacao_atualizada(ano, filtro_natureza or None)

    # ══════════════════════════════════════════════════════════════════
    # KPIs
    # ══════════════════════════════════════════════════════════════════
    kpi = {
        'dotacao_atualizada': dot_atualizada,
        'planejado': plan_total,
        'empenhado': emp_total,
        'dif_plan_emp': emp_total - plan_total,
        'pct_emp': _pct(emp_total, plan_total),
        'liquidado': liq_total,
        'pct_liq': _pct(liq_total, plan_total),
    }

    # ══════════════════════════════════════════════════════════════════
    # CHART — Planejado vs Liquidado por mês
    # ══════════════════════════════════════════════════════════════════
    chart_meses = []
    chart_planejado = []
    chart_liquidado = []
    for m in MESES:
        chart_meses.append(NOMES_MESES[m])
        chart_planejado.append(float(plan_by_month.get(m, 0)))
        chart_liquidado.append(float(liq_by_month.get(m, 0)))

    # ══════════════════════════════════════════════════════════════════
    # TABELA 1 — Por Natureza de Despesa (dados gerais, sem filtro contrato)
    # ══════════════════════════════════════════════════════════════════

    # Buscar nomes das naturezas
    from app.models.nat_despesa import NatDespesa
    all_nat_codes = set()
    for k in emp_by_natureza:
        all_nat_codes.add(k)
    for k in liq_by_natureza:
        all_nat_codes.add(k)
    for k in plan_by_natureza:
        all_nat_codes.add(k)

    nat_names = {}
    int_codes = []
    for c in all_nat_codes:
        try:
            int_codes.append(int(c))
        except (ValueError, TypeError):
            pass
    if int_codes:
        for nd in NatDespesa.query.filter(NatDespesa.codigo.in_(int_codes)).all():
            nat_names[str(nd.codigo)] = f'{nd.codigo}-{nd.titulo}'

    # Merge empenhado, liquidado e planejado por natureza
    all_nats = set(emp_by_natureza.keys()) | set(liq_by_natureza.keys()) | set(plan_by_natureza.keys())

    # Sub-itens por natureza (via empenho_itens) — agora sem filtro por contrato
    EXCLUDE_NATUREZA = {'339092', '449092'}
    subitem_rows = db.session.query(
        EmpenhoItem.Natureza,
        EmpenhoItem.SubItemDespesa,
    ).filter(
        EmpenhoItem.Natureza.isnot(None),
        EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
    ).distinct().all()

    # Lookup de descrição do sub-item
    subitem_desc = {}
    for sid in db.session.query(ClassSubItemDespesa).all():
        codigo = f'{sid.valoresClassificador1}.{sid.valoresClassificador2}'
        subitem_desc[codigo] = sid.nomeClassificador or codigo

    # {nat_code: set(subitem_label)}
    subitem_map = {}
    for r in subitem_rows:
        nat = r.Natureza or ''
        raw_sub = r.SubItemDespesa or 'Sem Sub-item'
        desc = subitem_desc.get(raw_sub)
        sub_label = desc if desc else raw_sub
        subitem_map.setdefault(nat, set()).add(sub_label)

    # Query empenhado/liquidado por natureza+subitem para detalhamento
    subitem_values = {}
    params_sub = {'dt_ini': dt_ini, 'dt_fim': dt_fim}
    filtro_nat_sub = ''
    if filtro_natureza:
        ph = ','.join([f':sn{i}' for i in range(len(filtro_natureza))])
        for i, n in enumerate(filtro_natureza):
            params_sub[f'sn{i}'] = n
        filtro_nat_sub = f'AND e.codNatureza IN ({ph})'

    sql_sub_emp = text(f"""
        SELECT ei.Natureza, ei.SubItemDespesa,
               SUM(CASE WHEN e.tipoAlteracaoNE = 'ANULACAO' THEN -e.valor ELSE e.valor END) AS total
        FROM empenho e
        JOIN empenho_itens ei ON e.codigo = ei.codigo AND e.codigoUG = ei.codigoUG AND e.codContrato = ei.CodContrato
        WHERE e.statusDocumento = 'CONTABILIZADO'
          AND e.codigoUG = '210101'
          AND e.dataEmissao >= :dt_ini AND e.dataEmissao < :dt_fim
          {filtro_nat_sub}
        GROUP BY ei.Natureza, ei.SubItemDespesa
    """)
    try:
        for row in db.session.execute(sql_sub_emp, params_sub).fetchall():
            nat = str(row[0]) if row[0] else ''
            sub = str(row[1]) if row[1] else 'Sem Sub-item'
            desc = subitem_desc.get(sub, sub)
            subitem_values.setdefault(nat, {}).setdefault(desc, {'emp': ZERO, 'liq': ZERO})
            subitem_values[nat][desc]['emp'] += _decimal(row[2])
    except Exception:
        pass

    # Planejado por natureza+subitem (para detalhamento nos sub-itens)
    plan_by_nat_sub = {}
    q_plan_sub = db.session.query(
        PlanejamentoOrcamentario.cod_natureza,
        PlanejamentoOrcamentario.cod_subitem,
        func.sum(PlanejamentoOrcamentario.valor).label('total')
    ).filter(
        PlanejamentoOrcamentario.competencia.like(f'%/{ano}')
    ).group_by(
        PlanejamentoOrcamentario.cod_natureza,
        PlanejamentoOrcamentario.cod_subitem
    )
    if filtro_mes_rel:
        q_plan_sub = q_plan_sub.filter(
            PlanejamentoOrcamentario.competencia == f'{filtro_mes_rel:02d}/{ano}'
        )
    if filtro_natureza:
        str_nats = [str(n) for n in filtro_natureza]
        q_plan_sub = q_plan_sub.filter(PlanejamentoOrcamentario.cod_natureza.in_(str_nats))
    for row in q_plan_sub.all():
        nat = str(row.cod_natureza) if row.cod_natureza else ''
        sub_code = str(row.cod_subitem) if row.cod_subitem else ''
        val = _decimal(row.total)
        plan_by_nat_sub.setdefault(nat, {})[sub_code] = val

    nat_table = []
    for nat_code in sorted(all_nats):
        p = plan_by_natureza.get(nat_code, ZERO)
        e = emp_by_natureza.get(nat_code, ZERO)
        l = liq_by_natureza.get(nat_code, ZERO)

        if p == 0 and e == 0 and l == 0:
            continue

        nat_name = nat_names.get(nat_code, nat_code)

        # Sub-itens
        subitems = []
        if nat_code in subitem_values:
            plan_subs = plan_by_nat_sub.get(nat_code, {})
            for sub_name, sub_vals in subitem_values[nat_code].items():
                se = sub_vals['emp']
                sl = sub_vals['liq']
                # Encontrar planejado: sub_name é "51 - SERVICOS TECNICOS..." - extrair código
                sp = ZERO
                for plan_sub_code, plan_val in plan_subs.items():
                    if plan_sub_code and sub_name.startswith(plan_sub_code):
                        sp = plan_val
                        break
                if se or sl or sp:
                    subitems.append({
                        'subitem': sub_name,
                        'planejado': sp,
                        'empenhado': se,
                        'liquidado': sl,
                        'pct_liq': float(_pct(sl, sp)) if sp else 0.0,
                        'pct_emp': float(_pct(se, sp)) if sp else 0.0,
                    })
            subitems.sort(key=lambda x: x['empenhado'], reverse=True)

        nat_table.append({
            'natcompleta': nat_name,
            'planejado': p,
            'empenhado': e,
            'liquidado': l,
            'pct_liq': float(_pct(l, p)),
            'pct_emp': float(_pct(e, p)),
            'subitems': subitems,
        })
    nat_table.sort(key=lambda x: x['empenhado'], reverse=True)

    # ══════════════════════════════════════════════════════════════════
    # TABELA 2 — Por Contrato (paginada)
    # ══════════════════════════════════════════════════════════════════
    contratos_table = []
    for c in pagination.items:
        cod = c.codigo
        p = plan_by_contract.get(cod, ZERO)
        e = emp_by_contract.get(cod, ZERO)
        l = liq_by_contract.get(cod, ZERO)

        cc = ''
        if hasattr(c, 'centro_de_custo') and c.centro_de_custo:
            cc = c.centro_de_custo.descricao

        nat = ''
        if hasattr(c, 'nat_despesa') and c.nat_despesa:
            nat = f'{c.nat_despesa.codigo}-{c.nat_despesa.titulo}'

        contratos_table.append({
            'codigo': cod,
            'credor': c.nomeContratado or c.nomeContratadoResumido or '',
            'planejado': p,
            'liquidado': l,
            'centro_custo': cc,
            'pct': float(_pct(l, p)),
            'empenhado': e,
            'natcompleta': nat,
            'plan_inicial': 'SIM' if plan_ini.get(cod) else 'NAO',
        })

    # ── Dados para selects de filtro ──
    todas_situacoes = PrestacaoContratoService.listar_situacoes()
    todos_tipos_execucao = PrestacaoContratoService.listar_tipos_execucao()
    todos_centros_custo = PrestacaoContratoService.listar_centros_de_custo()
    todos_pdms = PrestacaoContratoService.listar_pdms_utilizados()
    todas_naturezas = PrestacaoContratoService.listar_naturezas_utilizadas()
    todos_subitens = PrestacaoContratoService.listar_subitens_utilizados()
    todos_tipos_patrimoniais = PrestacaoContratoService.listar_tipos_patrimoniais_utilizados()

    tem_filtro = any([filtro_codigo, filtro_contratado, filtro_situacao,
                      filtro_natureza, filtro_tipo_execucao, filtro_centro_custo,
                      filtro_tipo_contrato, filtro_pdm, filtro_subitem,
                      filtro_tipo_patrimonial, filtro_mes_rel,
                      ano != datetime.now().year])

    return render_template(
        'financeiro/planejamento_relatorio.html',
        ano=ano,
        filtro_mes_rel=filtro_mes_rel,
        kpi=kpi,
        chart_meses=chart_meses,
        chart_planejado=chart_planejado,
        chart_liquidado=chart_liquidado,
        nat_table=nat_table,
        contratos=contratos_table,
        pagination=pagination,
        format_brl=_format_brl,
        format_num=_format_num,
        format_num_inteiro=_format_num_inteiro,
        situacao_labels=SITUACAO_LABELS,
        # Filtros
        todas_situacoes=todas_situacoes,
        todos_tipos_execucao=todos_tipos_execucao,
        todos_centros_custo=todos_centros_custo,
        todos_pdms=todos_pdms,
        todas_naturezas=todas_naturezas,
        todos_subitens=todos_subitens,
        todos_tipos_patrimoniais=todos_tipos_patrimoniais,
        anos_disponiveis=ANOS_DISPONIVEIS,
        nomes_meses_completo=NOMES_MESES_COMPLETO,
        # Valores selecionados
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
        tem_filtro=tem_filtro,
    )


# ── API: Salvar respostas do modal ──────────────────────────────────────────

@financeiro_bp.route('/api/planejamento/salvar', methods=['POST'])
@login_required
@_requires_admin_or_pedro
def api_planejamento_salvar():
    """Salva respostas do planejamento orçamentário para um contrato."""
    data = request.get_json() or {}
    cod_contrato = data.get('cod_contrato', '').strip()
    planejamento_inicial = data.get('planejamento_inicial')
    repactuacao_prorrogacao = data.get('repactuacao_prorrogacao')
    valores = data.get('valores', {})

    if not cod_contrato:
        return jsonify({'sucesso': False, 'msg': 'Código do contrato obrigatório.'}), 400

    if planejamento_inicial is None or repactuacao_prorrogacao is None:
        return jsonify({'sucesso': False, 'msg': 'Ambas as perguntas devem ser respondidas.'}), 400

    try:
        ano = datetime.now().year
        agora = datetime.now()
        pi = bool(planejamento_inicial)
        rp = bool(repactuacao_prorrogacao)

        # Auto-detectar natureza e subitem do contrato via empenho_itens
        cod_natureza = None
        cod_subitem = None
        cod_numerico = cod_contrato.replace('.', '').replace('/', '')
        try:
            cod_int = int(cod_numerico)
            ei_row = db.session.query(
                EmpenhoItem.Natureza, EmpenhoItem.SubItemDespesa
            ).filter(
                EmpenhoItem.CodContrato == cod_int,
                EmpenhoItem.Natureza.isnot(None),
                EmpenhoItem.Natureza != ''
            ).first()
            if ei_row:
                cod_natureza = str(ei_row[0])
                if ei_row[1]:
                    raw_sub = str(ei_row[1])
                    cod_subitem = raw_sub.split('.')[-1] if '.' in raw_sub else raw_sub
        except (ValueError, TypeError):
            pass

        for mes_num, valor_str in valores.items():
            # Converter formato BRL (1.234,56) para Decimal
            valor_decimal = _parse_brl(valor_str)
            competencia = f'{mes_num}/{ano}'

            existente = PlanejamentoOrcamentario.query.filter_by(
                cod_contrato=cod_contrato,
                competencia=competencia
            ).first()

            if existente:
                existente.valor = valor_decimal
                existente.planejamento_inicial = pi
                existente.repactuacao_prorrogacao = rp
                existente.dt_lancamento = agora
                existente.usuario = current_user.id
                existente.cod_natureza = cod_natureza
                existente.cod_subitem = cod_subitem
            else:
                novo = PlanejamentoOrcamentario(
                    cod_contrato=cod_contrato,
                    competencia=competencia,
                    valor=valor_decimal,
                    cod_natureza=cod_natureza,
                    cod_subitem=cod_subitem,
                    usuario=current_user.id,
                    planejamento_inicial=pi,
                    repactuacao_prorrogacao=rp,
                )
                db.session.add(novo)

        db.session.commit()
        return jsonify({'sucesso': True, 'msg': 'Planejamento salvo com sucesso.'})

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'msg': f'Erro ao salvar: {str(e)}'}), 500
