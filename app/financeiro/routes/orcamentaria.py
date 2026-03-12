"""
Rotas do módulo Orçamentário (Financeiro).
Tela principal de gerenciamento orçamentário — admins + Pedro Alexandre.
"""
from functools import wraps
from decimal import Decimal
from flask import render_template, request, jsonify, flash, redirect, url_for
from sqlalchemy import text
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp
from app.extensions import db


def requires_admin_or_pedro(f):
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


# =============================================================================
# Constantes de contas LOA
# =============================================================================
CONTA_INICIAL = '522110101'
CONTA_SUPLEMENTAR = '522120101'
CONTAS_ESPECIAL = ('522120201', '522120202', '522120203')
CONTAS_EXTRAORDINARIA = ('522120301', '522120302', '522120303')
CONTA_CONTINGENCIADO = '622120106'
CONTA_ANULADA = '522190401'
CONTA_CREDITO_DISPONIVEL = '622110101'

ZERO = Decimal('0')

NOMES_MESES = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
}


def _decimal(val):
    """Converte para Decimal seguro."""
    if val is None:
        return ZERO
    try:
        return Decimal(str(val))
    except Exception:
        return ZERO


def _format_brl(val):
    """Formata valor para padrão BRL com R$."""
    if val is None:
        val = ZERO
    return f'R$ {val:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def _format_num(val):
    """Formata valor numérico sem R$ (para tabelas)."""
    if val is None:
        val = ZERO
    return f'{val:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def _pct(parte, total):
    """Calcula percentual seguro."""
    if not total or total == 0:
        return Decimal('0')
    return (parte / total * 100).quantize(Decimal('0.01'))


# =============================================================================
# Tela principal Orçamentária
# =============================================================================
@financeiro_bp.route('/orcamentaria')
@login_required
@requires_admin_or_pedro
def orcamentaria():
    ano = request.args.get('ano', 2026, type=int)
    mes = request.args.get('mes', type=int)
    fonte_filtro = request.args.get('fonte', '')
    natureza_filtro = request.args.get('natureza', '')
    acao_filtro = request.args.get('acao', '')

    # --- KPI Cards: totais gerais (respeitam todos os filtros) ---
    kpi = _calcular_kpis(ano, mes, fonte_filtro, natureza_filtro, acao_filtro)

    # --- Tabela: Execução por Ação ---
    acoes = _calcular_acoes(ano, mes, fonte_filtro, natureza_filtro, acao_filtro)

    # --- Pré-carrega naturezas de todas as ações (evita AJAX lento) ---
    naturezas_por_acao = _calcular_todas_naturezas(ano, mes, fonte_filtro, natureza_filtro, acao_filtro)

    # --- Listas para filtros (dependentes dos outros filtros selecionados) ---
    fontes = _listar_fontes_filtradas(ano, mes, acao_filtro, natureza_filtro, '')
    meses_disponiveis = _listar_meses_filtrados(ano, '', acao_filtro, natureza_filtro, fonte_filtro)
    naturezas_disponiveis = _listar_naturezas_filtradas(ano, mes, acao_filtro, '', fonte_filtro)
    acoes_disponiveis = _listar_acoes_filtradas(ano, mes, '', natureza_filtro, fonte_filtro)

    return render_template(
        'financeiro/orcamentaria.html',
        ano=ano,
        mes=mes,
        fonte_filtro=fonte_filtro,
        natureza_filtro=natureza_filtro,
        acao_filtro=acao_filtro,
        kpi=kpi,
        acoes=acoes,
        naturezas_json=naturezas_por_acao,
        fontes=fontes,
        meses_disponiveis=meses_disponiveis,
        naturezas_disponiveis=naturezas_disponiveis,
        acoes_disponiveis=acoes_disponiveis,
        nomes_meses=NOMES_MESES,
        format_brl=_format_brl,
        format_num=_format_num,
    )


def _calcular_kpis(ano, mes, fonte, natureza='', acao=''):
    """Calcula valores dos KPI cards."""
    filtro_mes = "AND mes = :mes" if mes else ""
    filtro_fonte = ""
    filtro_natureza = ""
    filtro_acao = ""
    params = {"ano": ano}
    if mes:
        params["mes"] = mes

    # Aplica filtro de fonte (coluna codFonte)
    if fonte:
        filtro_fonte = "AND codFonte = :fonte"
        params["fonte"] = fonte
    if natureza:
        filtro_natureza = "AND codNatureza = :natureza"
        params["natureza"] = natureza
    if acao:
        filtro_acao = "AND codAcao = :acao"
        params["acao"] = acao

    # Range de datas para usar índices (evita YEAR() que impede index seek)
    # Se mes informado, filtra apenas aquele mês
    if mes:
        data_inicio = f"{ano}-{mes:02d}-01"
        if mes == 12:
            data_fim = f"{ano + 1}-01-01"
        else:
            data_fim = f"{ano}-{mes + 1:02d}-01"
    else:
        data_inicio = f"{ano}-01-01"
        data_fim = f"{ano + 1}-01-01"
    params_data = {"dt_ini": data_inicio, "dt_fim": data_fim}

    # Filtros para tabelas de execução (reserva, empenho, liquidacao, pd, ob)
    filtro_exec_fonte = ""
    filtro_exec_natureza = ""
    if fonte:
        filtro_exec_fonte = "AND codFonte = :fonte"
        params_data["fonte"] = fonte
    if natureza:
        filtro_exec_natureza = "AND codNatureza = :natureza"
        params_data["natureza"] = natureza

    # Filtro de ação: reserva usa extração de codClassificacao, empenho usa codAcao
    filtro_exec_acao_reserva = ""
    filtro_exec_acao_empenho = ""
    filtro_exec_acao_class = ""
    if acao:
        filtro_exec_acao_reserva = f"AND {_SQL_ACAO_FROM_CLASS_RESERVA} = :acao"
        filtro_exec_acao_empenho = "AND codAcao = :acao"
        filtro_exec_acao_class = f"AND {_SQL_ACAO_FROM_CLASS} = :acao"
        params_data["acao"] = acao

    # Crédito Disponível (LOA)
    sql_credito = text(f"""
        SELECT COALESCE(SUM(saldo), 0) FROM loa_2026
        WHERE ano = :ano AND id = :conta {filtro_mes} {filtro_fonte} {filtro_natureza} {filtro_acao}
    """)
    credito_disp = db.session.execute(
        sql_credito, {**params, "conta": CONTA_CREDITO_DISPONIVEL}
    ).scalar() or 0

    # Reservado (só CONTABILIZADO; ANULACAO inverte sinal)
    sql_reservado = text(f"""
        SELECT COALESCE(SUM(
            CASE WHEN tipoAlteracao = 'ANULACAO' THEN -valor ELSE valor END
        ), 0) FROM reserva
        WHERE statusDocumento = 'CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          {filtro_exec_fonte} {filtro_exec_natureza} {filtro_exec_acao_reserva}
    """)
    reservado = db.session.execute(sql_reservado, params_data).scalar() or 0

    # Empenhado (CONTABILIZADO; tipoAlteracaoNE = ANULACAO inverte sinal)
    sql_empenhado = text(f"""
        SELECT COALESCE(SUM(
            CASE WHEN tipoAlteracaoNE = 'ANULACAO' THEN -valor ELSE valor END
        ), 0) FROM empenho
        WHERE statusDocumento = 'CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
          {filtro_exec_fonte} {filtro_exec_natureza} {filtro_exec_acao_empenho}
    """)
    empenhado = db.session.execute(sql_empenhado, params_data).scalar() or 0

    # Liquidado (CONTABILIZADO; tipoAlteracao = ANULACAO inverte sinal)
    sql_liquidado = text(f"""
        SELECT COALESCE(SUM(
            CASE WHEN tipoAlteracao = 'ANULACAO' THEN -valor ELSE valor END
        ), 0) FROM liquidacao
        WHERE statusDocumento = 'CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
          {filtro_exec_fonte} {filtro_exec_natureza} {filtro_exec_acao_class}
    """)
    try:
        liquidado = db.session.execute(sql_liquidado, params_data).scalar() or 0
    except Exception:
        liquidado = 0

    # PDs a Pagar (CONTABILIZADO)
    sql_pd = text(f"""
        SELECT COALESCE(SUM(valor), 0) FROM pd
        WHERE statusDocumento = 'CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
          {filtro_exec_fonte} {filtro_exec_natureza} {filtro_exec_acao_class}
    """)
    try:
        pd_val = db.session.execute(sql_pd, params_data).scalar() or 0
    except Exception:
        pd_val = 0

    # Pago (OB) (CONTABILIZADO)
    sql_pago = text(f"""
        SELECT COALESCE(SUM(valor), 0) FROM ob
        WHERE statusDocumento = 'CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
          {filtro_exec_fonte} {filtro_exec_natureza} {filtro_exec_acao_class}
    """)
    try:
        pago = db.session.execute(sql_pago, params_data).scalar() or 0
    except Exception:
        pago = 0

    # Dotação Inicial — sempre fixa em janeiro, sem filtro de mês
    params_dot_ini = {"ano": ano}
    filtros_dot_ini = ""
    if fonte:
        filtros_dot_ini += " AND codFonte = :fonte"
        params_dot_ini["fonte"] = fonte
    if natureza:
        filtros_dot_ini += " AND codNatureza = :natureza"
        params_dot_ini["natureza"] = natureza
    if acao:
        filtros_dot_ini += " AND codAcao = :acao"
        params_dot_ini["acao"] = acao
    sql_dot_ini = text(f"""
        SELECT COALESCE(SUM(saldo), 0) FROM loa_2026
        WHERE ano = :ano AND id = :conta AND mes = 1 {filtros_dot_ini}
    """)
    dot_inicial = _decimal(
        db.session.execute(sql_dot_ini, {**params_dot_ini, "conta": CONTA_INICIAL}).scalar() or 0
    )

    # Demais contas LOA (respeitam filtro de mês)
    sql_dotacao = text(f"""
        SELECT id, COALESCE(SUM(saldo), 0) as total FROM loa_2026
        WHERE ano = :ano {filtro_mes} {filtro_fonte} {filtro_natureza} {filtro_acao}
        GROUP BY id
    """)
    rows = db.session.execute(sql_dotacao, params).fetchall()
    totais_conta = {r[0]: _decimal(r[1]) for r in rows}
    dot_suplementar = totais_conta.get(CONTA_SUPLEMENTAR, ZERO)
    dot_especial = sum(totais_conta.get(c, ZERO) for c in CONTAS_ESPECIAL)
    dot_extra = sum(totais_conta.get(c, ZERO) for c in CONTAS_EXTRAORDINARIA)
    dot_anulada = totais_conta.get(CONTA_ANULADA, ZERO)
    dot_atualizada = dot_inicial + dot_suplementar + dot_especial + dot_extra - abs(dot_anulada)

    return {
        'credito_disponivel': _decimal(credito_disp),
        'reservado': _decimal(reservado),
        'empenhado': _decimal(empenhado),
        'liquidado': _decimal(liquidado),
        'pds_pagar': _decimal(pd_val),
        'pago': _decimal(pago),
        'dotacao_atualizada': dot_atualizada,
        'pct_disponivel': _pct(_decimal(credito_disp), dot_atualizada),
        'pct_reservado': _pct(_decimal(reservado), dot_atualizada),
        'pct_empenhado': _pct(_decimal(empenhado), dot_atualizada),
        'pct_liquidado': _pct(_decimal(liquidado), dot_atualizada),
        'pct_pds': _pct(_decimal(pd_val), dot_atualizada),
        'pct_pago': _pct(_decimal(pago), dot_atualizada),
    }


def _calcular_acoes(ano, mes, fonte, natureza='', acao=''):
    """Calcula dados da tabela Execução por Ação."""
    filtro_mes = "AND mes = :mes" if mes else ""
    filtro_fonte = ""
    filtro_natureza = ""
    filtro_acao = ""
    params = {"ano": ano}
    if mes:
        params["mes"] = mes
    if fonte:
        filtro_fonte = "AND codFonte = :fonte"
        params["fonte"] = fonte
    if natureza:
        filtro_natureza = "AND codNatureza = :natureza"
        params["natureza"] = natureza
    if acao:
        filtro_acao = "AND codAcao = :acao"
        params["acao"] = acao

    # Dotação Inicial — sempre fixa em janeiro (mes=1), sem filtro de mês
    params_dot_ini = {"ano": ano}
    filtros_dot_ini = ""
    if fonte:
        filtros_dot_ini += " AND codFonte = :fonte"
        params_dot_ini["fonte"] = fonte
    if natureza:
        filtros_dot_ini += " AND codNatureza = :natureza"
        params_dot_ini["natureza"] = natureza
    if acao:
        filtros_dot_ini += " AND codAcao = :acao"
        params_dot_ini["acao"] = acao
    sql_dot_ini = text(f"""
        SELECT codAcao as acao, COALESCE(SUM(saldo), 0) as total
        FROM loa_2026
        WHERE ano = :ano AND id = :conta AND mes = 1 {filtros_dot_ini}
        GROUP BY acao
    """)
    dot_ini_rows = db.session.execute(sql_dot_ini, {**params_dot_ini, "conta": CONTA_INICIAL}).fetchall()
    dot_ini_por_acao = {r[0]: _decimal(r[1]) for r in dot_ini_rows if r[0]}

    # Demais contas LOA (respeitam filtro de mês)
    sql = text(f"""
        SELECT
            codAcao as acao,
            id as conta,
            COALESCE(SUM(saldo), 0) as total
        FROM loa_2026
        WHERE ano = :ano {filtro_mes} {filtro_fonte} {filtro_natureza} {filtro_acao}
        GROUP BY acao, conta
        ORDER BY acao
    """)

    rows = db.session.execute(sql, params).fetchall()

    # Agrupa por ação
    acoes_dict = {}
    for acao_cod, conta, total in rows:
        if not acao_cod:
            continue
        if acao_cod not in acoes_dict:
            acoes_dict[acao_cod] = {}
        acoes_dict[acao_cod][conta] = _decimal(total)

    # Garante que ações com dotação inicial apareçam mesmo sem dados no mês filtrado
    for acao_cod in dot_ini_por_acao:
        if acao_cod not in acoes_dict:
            acoes_dict[acao_cod] = {}

    # Busca títulos das ações na tabela 'acao'
    sql_desc = text("SELECT codigo, titulo FROM acao")
    desc_rows = db.session.execute(sql_desc).fetchall()
    desc_map = {}
    for codigo, titulo in desc_rows:
        if codigo and titulo:
            desc_map[str(codigo)] = titulo

    # Execução financeira por ação (empenho/liquidação/PD/OB)
    exec_por_acao = _calcular_execucao(ano, nivel='acao', mes=mes, fonte=fonte, natureza=natureza, acao=acao)

    # Execução Gerencial (soma de execucoes_orcamentarias por ação)
    exec_gerencial_por_acao = _calcular_exec_gerencial(nivel='acao')

    resultado = []
    totais = {
        'dot_inicial': ZERO, 'dot_suplementar': ZERO, 'dot_especial': ZERO,
        'dot_extraordinaria': ZERO, 'contingenciado': ZERO, 'dot_anulada': ZERO,
        'dot_atualizada': ZERO, 'credito_disponivel': ZERO, 'analise': ZERO,
        'reservado': ZERO, 'empenhado': ZERO, 'liquidado': ZERO, 'pd': ZERO, 'pago': ZERO,
        'exec_gerencial': ZERO,
    }

    for acao_cod in sorted(acoes_dict.keys()):
        contas = acoes_dict[acao_cod]

        dot_inicial = dot_ini_por_acao.get(acao_cod, ZERO)
        dot_suplementar = contas.get(CONTA_SUPLEMENTAR, ZERO)
        dot_especial = sum(contas.get(c, ZERO) for c in CONTAS_ESPECIAL)
        dot_extra = sum(contas.get(c, ZERO) for c in CONTAS_EXTRAORDINARIA)
        contingenciado = contas.get(CONTA_CONTINGENCIADO, ZERO)
        dot_anulada = contas.get(CONTA_ANULADA, ZERO)
        credito_disp = contas.get(CONTA_CREDITO_DISPONIVEL, ZERO)

        dot_atualizada = dot_inicial + dot_suplementar + dot_especial + dot_extra - abs(dot_anulada)
        analise = dot_atualizada - abs(credito_disp) if credito_disp else ZERO
        pct_exec = _pct(analise, dot_atualizada)

        # Dados de execução financeira
        ex = exec_por_acao.get(str(acao_cod), {})
        reservado = _decimal(ex.get('res', 0))
        empenhado = _decimal(ex.get('emp', 0))
        liquidado = _decimal(ex.get('liq', 0))
        pd_val = _decimal(ex.get('pd', 0))
        pago = _decimal(ex.get('ob', 0))

        # Execução Gerencial
        exec_ger = _decimal(exec_gerencial_por_acao.get(str(acao_cod), 0))

        item = {
            'codigo': acao_cod,
            'descricao': desc_map.get(acao_cod, ''),
            'desc_acao': f'{acao_cod} - {desc_map.get(acao_cod, "")}',
            'dot_inicial': dot_inicial,
            'dot_suplementar': dot_suplementar,
            'dot_especial': dot_especial,
            'dot_extraordinaria': dot_extra,
            'contingenciado': contingenciado,
            'dot_anulada': dot_anulada,
            'dot_atualizada': dot_atualizada,
            'credito_disponivel': credito_disp,
            'analise': analise,
            'pct_exec': pct_exec,
            'reservado': reservado,
            'pct_res': _pct(reservado, dot_atualizada),
            'empenhado': empenhado,
            'pct_emp': _pct(empenhado, dot_atualizada),
            'liquidado': liquidado,
            'pct_liq': _pct(liquidado, dot_atualizada),
            'pd': pd_val,
            'pct_pd': _pct(pd_val, dot_atualizada),
            'pago': pago,
            'pct_pago': _pct(pago, dot_atualizada),
            'exec_gerencial': exec_ger,
        }
        resultado.append(item)

        for key in totais:
            totais[key] += item.get(key, ZERO)

    totais['pct_exec'] = _pct(totais['analise'], totais['dot_atualizada'])
    totais['pct_res'] = _pct(totais['reservado'], totais['dot_atualizada'])
    totais['pct_emp'] = _pct(totais['empenhado'], totais['dot_atualizada'])
    totais['pct_liq'] = _pct(totais['liquidado'], totais['dot_atualizada'])
    totais['pct_pd'] = _pct(totais['pd'], totais['dot_atualizada'])
    totais['pct_pago'] = _pct(totais['pago'], totais['dot_atualizada'])

    return {'itens': resultado, 'totais': totais}


# Expressão SQL para extrair codAcao de codClassificacao (segmento 8 após remover espaços)
# liquidacao/pd/ob: formato "1.21.101.1.04.122.0010.2000..." → ação = segmento 8
_SQL_ACAO_FROM_CLASS = "SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(codClassificacao, ' ', ''), '.', 8), '.', -1)"
# reserva: formato "21.101.1.04.122.0010.2000..." (sem prefixo) → ação = segmento 7
_SQL_ACAO_FROM_CLASS_RESERVA = "SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(codClassificacao, ' ', ''), '.', 7), '.', -1)"


def _calcular_execucao(ano, nivel='acao', mes=None, fonte='', natureza='', acao=''):
    """Calcula valores de empenho/liquidação/PD/OB agrupados por ação (ou ação+natureza).

    Args:
        ano: Ano fiscal.
        nivel: 'acao' retorna {acao: {emp, liq, pd, ob}},
               'natureza' retorna {(acao, nat): {emp, liq, pd, ob}}.
        mes: Mês para filtrar (opcional).
        fonte: Código da fonte para filtrar (opcional).
        natureza: Código da natureza para filtrar (opcional).
        acao: Código da ação para filtrar (opcional).
    """
    # Range de datas: se mes informado, filtra apenas aquele mês
    if mes:
        dt_ini = f"{ano}-{mes:02d}-01"
        if mes == 12:
            dt_fim = f"{ano + 1}-01-01"
        else:
            dt_fim = f"{ano}-{mes + 1:02d}-01"
    else:
        dt_ini = f"{ano}-01-01"
        dt_fim = f"{ano + 1}-01-01"
    params = {"dt_ini": dt_ini, "dt_fim": dt_fim}

    # Filtros comuns de execução
    filtro_fonte = ""
    filtro_natureza_sql = ""
    if fonte:
        filtro_fonte = "AND codFonte = :fonte"
        params["fonte"] = fonte
    if natureza:
        filtro_natureza_sql = "AND codNatureza = :natureza"
        params["natureza"] = natureza

    # Filtro de ação (cada tabela usa extração diferente)
    filtro_acao_reserva = ""
    filtro_acao_empenho = ""
    filtro_acao_class = ""
    if acao:
        filtro_acao_reserva = f"AND {_SQL_ACAO_FROM_CLASS_RESERVA} = :acao"
        filtro_acao_empenho = "AND codAcao = :acao"
        filtro_acao_class = f"AND {_SQL_ACAO_FROM_CLASS} = :acao"
        params["acao"] = acao

    group_nat = ", codNatureza" if nivel == 'natureza' else ""
    select_nat = ", codNatureza" if nivel == 'natureza' else ""

    resultado = {}

    # --- Reservado (extrair ação do codClassificacao, só CONTABILIZADO, ANULACAO inverte sinal) ---
    sql_res = text(f"""
        SELECT {_SQL_ACAO_FROM_CLASS_RESERVA} as acao {select_nat},
               COALESCE(SUM(
                   CASE WHEN tipoAlteracao = 'ANULACAO' THEN -valor ELSE valor END
               ), 0) as total
        FROM reserva
        WHERE statusDocumento = 'CONTABILIZADO'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          {filtro_fonte} {filtro_natureza_sql} {filtro_acao_reserva}
        GROUP BY acao {group_nat}
    """)
    try:
        for r in db.session.execute(sql_res, params).fetchall():
            key = (str(r[0]), str(r[1])) if nivel == 'natureza' else str(r[0])
            val = float(r[-1])
            resultado.setdefault(key, {'res': 0, 'emp': 0, 'liq': 0, 'pd': 0, 'ob': 0})
            resultado[key]['res'] += val
    except Exception:
        pass

    # --- Empenho (tem codAcao direto, CONTABILIZADO, ANULACAO inverte sinal) ---
    sql_emp = text(f"""
        SELECT codAcao as acao {select_nat},
               COALESCE(SUM(
                   CASE WHEN tipoAlteracaoNE = 'ANULACAO' THEN -valor ELSE valor END
               ), 0) as total
        FROM empenho
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          {filtro_fonte} {filtro_natureza_sql} {filtro_acao_empenho}
        GROUP BY acao {group_nat}
    """)
    for r in db.session.execute(sql_emp, params).fetchall():
        key = (str(r[0]), str(r[1])) if nivel == 'natureza' else str(r[0])
        val = float(r[-1])
        resultado.setdefault(key, {'res': 0, 'emp': 0, 'liq': 0, 'pd': 0, 'ob': 0})
        resultado[key]['emp'] += val

    # --- Liquidação (CONTABILIZADO, ANULACAO inverte sinal) ---
    sql_liq = text(f"""
        SELECT {_SQL_ACAO_FROM_CLASS} as acao {select_nat},
               COALESCE(SUM(
                   CASE WHEN tipoAlteracao = 'ANULACAO' THEN -valor ELSE valor END
               ), 0) as total
        FROM liquidacao
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          {filtro_fonte} {filtro_natureza_sql} {filtro_acao_class}
        GROUP BY acao {group_nat}
    """)
    try:
        for r in db.session.execute(sql_liq, params).fetchall():
            key = (str(r[0]), str(r[1])) if nivel == 'natureza' else str(r[0])
            val = float(r[-1])
            resultado.setdefault(key, {'res': 0, 'emp': 0, 'liq': 0, 'pd': 0, 'ob': 0})
            resultado[key]['liq'] += val
    except Exception:
        pass

    # --- PD, OB (CONTABILIZADO, sem tipoAlteracao) ---
    tabelas = [('pd', 'pd'), ('ob', 'ob')]
    for tabela, campo in tabelas:
        sql = text(f"""
            SELECT {_SQL_ACAO_FROM_CLASS} as acao {select_nat},
                   COALESCE(SUM(valor), 0) as total
            FROM {tabela}
            WHERE statusDocumento = 'CONTABILIZADO'
              AND codigoUG = '210101'
              AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
              {filtro_fonte} {filtro_natureza_sql} {filtro_acao_class}
            GROUP BY acao {group_nat}
        """)
        try:
            for r in db.session.execute(sql, params).fetchall():
                key = (str(r[0]), str(r[1])) if nivel == 'natureza' else str(r[0])
                val = float(r[-1])
                resultado.setdefault(key, {'res': 0, 'emp': 0, 'liq': 0, 'pd': 0, 'ob': 0})
                resultado[key][campo] += val
        except Exception:
            pass  # Tabela pode não existir

    return resultado


def _calcular_exec_gerencial(nivel='acao'):
    """Calcula soma de execucoes_orcamentarias agrupado por ação (ou ação+natureza).

    Args:
        nivel: 'acao' retorna {acao: total}, 'natureza' retorna {(acao,nat): total}.
    """
    group_nat = ", e.natureza" if nivel == 'natureza' else ""
    select_nat = ", e.natureza" if nivel == 'natureza' else ""

    sql = text(f"""
        SELECT e.acao {select_nat}, COALESCE(SUM(e.valor), 0) as total
        FROM execucoes_orcamentarias e
        WHERE e.acao IS NOT NULL AND e.acao != ''
        GROUP BY e.acao {group_nat}
    """)

    resultado = {}
    try:
        for r in db.session.execute(sql).fetchall():
            if nivel == 'natureza':
                key = (str(r[0]), str(r[1]))
            else:
                key = str(r[0])
            resultado[key] = float(r[-1])
    except Exception:
        pass
    return resultado


def _carregar_descricoes():
    """Carrega descrições de natureza (natdespesas) e fonte (class_fonte)."""
    nat_rows = db.session.execute(
        text("SELECT codigo, titulo FROM natdespesas")
    ).fetchall()
    nat_map = {str(r[0]): r[1] for r in nat_rows if r[0] and r[1]}

    fonte_rows = db.session.execute(
        text("SELECT codigo, descricao FROM class_fonte")
    ).fetchall()
    fonte_map = {str(r[0]): r[1] for r in fonte_rows if r[0] and r[1]}

    return nat_map, fonte_map


def _calcular_todas_naturezas(ano, mes, fonte, natureza='', acao=''):
    """Pré-carrega naturezas de TODAS as ações em uma única query.
    Retorna dict {acao: [{natureza, desc_natureza, dot_inicial, ...}]} para embutir como JSON."""
    filtro_mes = "AND mes = :mes" if mes else ""
    filtro_fonte = ""
    filtro_natureza = ""
    filtro_acao = ""
    params = {"ano": ano}
    if mes:
        params["mes"] = mes
    if fonte:
        filtro_fonte = "AND codFonte = :fonte"
        params["fonte"] = fonte
    if natureza:
        filtro_natureza = "AND codNatureza = :natureza"
        params["natureza"] = natureza
    if acao:
        filtro_acao = "AND codAcao = :acao"
        params["acao"] = acao

    # Dotação Inicial por ação+natureza — sempre fixa em janeiro (mes=1)
    params_dot_ini = {"ano": ano, "conta": CONTA_INICIAL}
    filtros_dot_ini = ""
    if fonte:
        filtros_dot_ini += " AND codFonte = :fonte"
        params_dot_ini["fonte"] = fonte
    if natureza:
        filtros_dot_ini += " AND codNatureza = :natureza"
        params_dot_ini["natureza"] = natureza
    if acao:
        filtros_dot_ini += " AND codAcao = :acao"
        params_dot_ini["acao"] = acao
    sql_dot_ini = text(f"""
        SELECT codAcao, codNatureza, COALESCE(SUM(saldo), 0) as total
        FROM loa_2026
        WHERE ano = :ano AND id = :conta AND mes = 1 AND codNatureza IS NOT NULL {filtros_dot_ini}
        GROUP BY codAcao, codNatureza
    """)
    dot_ini_por_nat = {}
    for r in db.session.execute(sql_dot_ini, params_dot_ini).fetchall():
        if r[0] and r[1]:
            dot_ini_por_nat[(r[0], r[1])] = float(_decimal(r[2]))

    # Demais contas LOA (respeitam filtro de mês)
    sql = text(f"""
        SELECT codAcao, codNatureza, id as conta, COALESCE(SUM(saldo), 0) as total
        FROM loa_2026
        WHERE ano = :ano AND codNatureza IS NOT NULL
          {filtro_mes} {filtro_fonte} {filtro_natureza} {filtro_acao}
        GROUP BY codAcao, codNatureza, conta
        ORDER BY codAcao, codNatureza
    """)
    rows = db.session.execute(sql, params).fetchall()

    # Descrições de natureza
    nat_map, _ = _carregar_descricoes()

    # Execução financeira por ação+natureza (empenho/liquidação/PD/OB)
    exec_por_nat = _calcular_execucao(ano, nivel='natureza', mes=mes, fonte=fonte, natureza=natureza, acao=acao)

    # Execução Gerencial por ação+natureza
    exec_ger_por_nat = _calcular_exec_gerencial(nivel='natureza')

    # Agrupa: acao -> natureza -> conta -> total
    tree = {}
    for acao_v, nat, conta, total in rows:
        if not acao_v or not nat:
            continue
        tree.setdefault(acao_v, {}).setdefault(nat, {})[conta] = float(_decimal(total))

    # Garante que ações+naturezas com dotação inicial apareçam
    for (acao_v, nat) in dot_ini_por_nat:
        tree.setdefault(acao_v, {}).setdefault(nat, {})

    # Calcula dotações por natureza em cada ação
    resultado = {}
    for acao_v, nats in tree.items():
        lista = []
        for nat in sorted(nats.keys()):
            contas = nats[nat]
            dot_inicial = dot_ini_por_nat.get((acao_v, nat), 0)
            dot_suplementar = contas.get(CONTA_SUPLEMENTAR, 0)
            dot_especial = sum(contas.get(c, 0) for c in CONTAS_ESPECIAL)
            dot_extra = sum(contas.get(c, 0) for c in CONTAS_EXTRAORDINARIA)
            contingenciado = contas.get(CONTA_CONTINGENCIADO, 0)
            dot_anulada = contas.get(CONTA_ANULADA, 0)
            credito_disp = contas.get(CONTA_CREDITO_DISPONIVEL, 0)

            dot_atualizada = dot_inicial + dot_suplementar + dot_especial + dot_extra - abs(dot_anulada)
            analise = dot_atualizada - abs(credito_disp) if credito_disp else 0
            pct_exec = round((analise / dot_atualizada * 100), 2) if dot_atualizada else 0

            # Dados de execução financeira para esta ação+natureza
            ex = exec_por_nat.get((str(acao_v), str(nat)), {})
            res = float(ex.get('res', 0))
            emp = float(ex.get('emp', 0))
            liq = float(ex.get('liq', 0))
            pd_val = float(ex.get('pd', 0))
            ob = float(ex.get('ob', 0))

            # Execução Gerencial
            exec_ger = exec_ger_por_nat.get((str(acao_v), str(nat)), 0)

            desc = nat_map.get(str(nat), '')
            lista.append({
                'natureza': nat,
                'desc_natureza': f'{nat} - {desc}' if desc else nat,
                'dot_inicial': dot_inicial,
                'dot_suplementar': dot_suplementar,
                'dot_especial': dot_especial,
                'dot_extraordinaria': dot_extra,
                'contingenciado': contingenciado,
                'dot_anulada': dot_anulada,
                'dot_atualizada': dot_atualizada,
                'credito_disponivel': credito_disp,
                'analise': analise,
                'pct_exec': pct_exec,
                'reservado': res,
                'pct_res': round((res / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'empenhado': emp,
                'pct_emp': round((emp / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'liquidado': liq,
                'pct_liq': round((liq / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'pd': pd_val,
                'pct_pd': round((pd_val / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'pago': ob,
                'pct_pago': round((ob / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'exec_gerencial': exec_ger,
            })
        resultado[acao_v] = lista

    return resultado


def _listar_fontes(ano):
    """Lista fontes distintas para filtro, com descrição."""
    sql = text("""
        SELECT DISTINCT codFonte as fonte
        FROM loa_2026
        WHERE ano = :ano AND codFonte IS NOT NULL
        ORDER BY fonte
    """)
    rows = db.session.execute(sql, {"ano": ano}).fetchall()
    codigos = [r[0] for r in rows if r[0]]

    # Busca descrições da tabela class_fonte
    _, fonte_map = _carregar_descricoes()
    resultado = [{'codigo': c, 'descricao': fonte_map.get(str(c), '')} for c in codigos]
    resultado.sort(key=lambda x: x['descricao'].lower())
    return resultado


def _listar_naturezas(ano):
    """Lista naturezas distintas para filtro, com descrição."""
    sql = text("""
        SELECT DISTINCT codNatureza as nat
        FROM loa_2026
        WHERE ano = :ano AND codNatureza IS NOT NULL
        ORDER BY nat
    """)
    rows = db.session.execute(sql, {"ano": ano}).fetchall()
    codigos = [r[0] for r in rows if r[0]]

    nat_map, _ = _carregar_descricoes()
    resultado = [{'codigo': c, 'descricao': nat_map.get(str(c), '')} for c in codigos]
    resultado.sort(key=lambda x: x['descricao'].lower())
    return resultado


def _listar_meses(ano):
    """Lista meses disponíveis."""
    sql = text("""
        SELECT DISTINCT mes FROM loa_2026
        WHERE ano = :ano ORDER BY mes
    """)
    rows = db.session.execute(sql, {"ano": ano}).fetchall()
    return [r[0] for r in rows]


def _listar_acoes(ano):
    """Lista ações distintas para filtro, com descrição."""
    sql = text("""
        SELECT DISTINCT codAcao as acao
        FROM loa_2026
        WHERE ano = :ano AND codAcao IS NOT NULL
        ORDER BY acao
    """)
    rows = db.session.execute(sql, {"ano": ano}).fetchall()
    codigos = [r[0] for r in rows if r[0]]

    # Busca descrições da tabela acao
    sql_desc = text("SELECT codigo, titulo FROM acao")
    desc_rows = db.session.execute(sql_desc).fetchall()
    desc_map = {str(r[0]): r[1] for r in desc_rows if r[0] and r[1]}

    resultado = [{'codigo': c, 'descricao': desc_map.get(str(c), '')} for c in codigos]
    resultado.sort(key=lambda x: x['descricao'].lower())
    return resultado


# --- Versões filtradas (dependentes) dos listadores de filtros ---

def _build_filtro_sql(ano, mes, acao, natureza, fonte, excluir):
    """Constrói WHERE + params para listagem de filtros dependentes.

    `excluir` indica qual filtro NÃO incluir (para não filtrar por si mesmo).
    """
    where = "WHERE ano = :ano"
    params = {"ano": ano}
    if mes and excluir != 'mes':
        where += " AND mes = :mes"
        params["mes"] = int(mes)
    if acao and excluir != 'acao':
        where += " AND codAcao = :acao"
        params["acao"] = acao
    if natureza and excluir != 'natureza':
        where += " AND codNatureza = :natureza"
        params["natureza"] = natureza
    if fonte and excluir != 'fonte':
        where += " AND codFonte = :fonte"
        params["fonte"] = fonte
    return where, params


def _listar_meses_filtrados(ano, mes, acao, natureza, fonte):
    where, params = _build_filtro_sql(ano, mes, acao, natureza, fonte, 'mes')
    sql = text(f"SELECT DISTINCT mes FROM loa_2026 {where} ORDER BY mes")
    return [r[0] for r in db.session.execute(sql, params).fetchall() if r[0] is not None]


def _listar_acoes_filtradas(ano, mes, acao, natureza, fonte):
    where, params = _build_filtro_sql(ano, mes, acao, natureza, fonte, 'acao')
    sql = text(f"SELECT DISTINCT codAcao FROM loa_2026 {where} AND codAcao IS NOT NULL ORDER BY codAcao")
    codigos = [r[0] for r in db.session.execute(sql, params).fetchall() if r[0]]
    sql_desc = text("SELECT codigo, titulo FROM acao")
    desc_map = {str(r[0]): r[1] for r in db.session.execute(sql_desc).fetchall() if r[0] and r[1]}
    resultado = [{'codigo': c, 'descricao': desc_map.get(str(c), '')} for c in codigos]
    resultado.sort(key=lambda x: x['descricao'].lower())
    return resultado


def _listar_naturezas_filtradas(ano, mes, acao, natureza, fonte):
    where, params = _build_filtro_sql(ano, mes, acao, natureza, fonte, 'natureza')
    sql = text(f"SELECT DISTINCT codNatureza FROM loa_2026 {where} AND codNatureza IS NOT NULL ORDER BY codNatureza")
    codigos = [r[0] for r in db.session.execute(sql, params).fetchall() if r[0]]
    nat_map, _ = _carregar_descricoes()
    resultado = [{'codigo': c, 'descricao': nat_map.get(str(c), '')} for c in codigos]
    resultado.sort(key=lambda x: x['descricao'].lower())
    return resultado


def _listar_fontes_filtradas(ano, mes, acao, natureza, fonte):
    where, params = _build_filtro_sql(ano, mes, acao, natureza, fonte, 'fonte')
    sql = text(f"SELECT DISTINCT codFonte FROM loa_2026 {where} AND codFonte IS NOT NULL ORDER BY codFonte")
    codigos = [r[0] for r in db.session.execute(sql, params).fetchall() if r[0]]
    _, fonte_map = _carregar_descricoes()
    resultado = [{'codigo': c, 'descricao': fonte_map.get(str(c), '')} for c in codigos]
    resultado.sort(key=lambda x: x['descricao'].lower())
    return resultado


# =============================================================================
# API: Filtros dinâmicos por ano
# =============================================================================
@financeiro_bp.route('/api/orcamentaria/filtros/<int:ano>')
@login_required
@requires_admin_or_pedro
def api_orcamentaria_filtros(ano):
    """Retorna opções de filtro disponíveis para um dado ano."""
    meses = _listar_meses(ano)
    naturezas = _listar_naturezas(ano)
    fontes = _listar_fontes(ano)
    acoes = _listar_acoes(ano)

    return jsonify({
        'meses': [{'valor': m, 'label': NOMES_MESES.get(m, str(m))} for m in meses],
        'naturezas': [{'codigo': n['codigo'], 'descricao': n['descricao']} for n in naturezas],
        'fontes': [{'codigo': f['codigo'], 'descricao': f['descricao']} for f in fontes],
        'acoes': [{'codigo': a['codigo'], 'descricao': a['descricao']} for a in acoes],
    })


# =============================================================================
# API: Filtros dependentes (cascata)
# =============================================================================
@financeiro_bp.route('/api/orcamentaria/filtros-dependentes')
@login_required
@requires_admin_or_pedro
def api_filtros_dependentes():
    """Retorna opções disponíveis para cada filtro, filtradas pelos OUTROS filtros selecionados."""
    try:
        ano = request.args.get('ano', 2026, type=int)
        mes_raw = request.args.get('mes', '')
        acao_raw = request.args.get('acao', '')
        natureza_raw = request.args.get('natureza', '')
        fonte_raw = request.args.get('fonte', '')

        # Suporta valores múltiplos separados por vírgula
        mes_vals = [v.strip() for v in mes_raw.split(',') if v.strip()] if mes_raw else []
        acao_vals = [v.strip() for v in acao_raw.split(',') if v.strip()] if acao_raw else []
        nat_vals = [v.strip() for v in natureza_raw.split(',') if v.strip()] if natureza_raw else []
        fonte_vals = [v.strip() for v in fonte_raw.split(',') if v.strip()] if fonte_raw else []

        base = "FROM loa_2026 WHERE ano = :ano"

        def _build_query(select_col, excluir):
            """Constrói SQL e params excluindo o filtro indicado."""
            frags = ""
            params = {"ano": ano}
            if mes_vals and excluir != 'mes':
                placeholders = ', '.join(f':mes_{i}' for i in range(len(mes_vals)))
                frags += f" AND mes IN ({placeholders})"
                for i, v in enumerate(mes_vals):
                    params[f'mes_{i}'] = int(v)
            if acao_vals and excluir != 'acao':
                placeholders = ', '.join(f':acao_{i}' for i in range(len(acao_vals)))
                frags += f" AND codAcao IN ({placeholders})"
                for i, v in enumerate(acao_vals):
                    params[f'acao_{i}'] = v
            if nat_vals and excluir != 'natureza':
                placeholders = ', '.join(f':nat_{i}' for i in range(len(nat_vals)))
                frags += f" AND codNatureza IN ({placeholders})"
                for i, v in enumerate(nat_vals):
                    params[f'nat_{i}'] = v
            if fonte_vals and excluir != 'fonte':
                placeholders = ', '.join(f':fonte_{i}' for i in range(len(fonte_vals)))
                frags += f" AND codFonte IN ({placeholders})"
                for i, v in enumerate(fonte_vals):
                    params[f'fonte_{i}'] = v
            not_null = f" AND {select_col} IS NOT NULL" if select_col != 'mes' else ""
            sql = f"SELECT DISTINCT {select_col} {base}{frags}{not_null} ORDER BY {select_col}"
            return text(sql), params

        # Meses disponíveis (filtrado por acao, natureza, fonte — NÃO por mes)
        sql_m, p_m = _build_query('mes', 'mes')
        meses = [r[0] for r in db.session.execute(sql_m, p_m).fetchall() if r[0] is not None]

        # Ações disponíveis (filtrado por mes, natureza, fonte — NÃO por acao)
        sql_a, p_a = _build_query('codAcao', 'acao')
        acoes_vals = [r[0] for r in db.session.execute(sql_a, p_a).fetchall() if r[0]]

        # Naturezas disponíveis (filtrado por mes, acao, fonte — NÃO por natureza)
        sql_n, p_n = _build_query('codNatureza', 'natureza')
        nats_vals = [r[0] for r in db.session.execute(sql_n, p_n).fetchall() if r[0]]

        # Fontes disponíveis (filtrado por mes, acao, natureza — NÃO por fonte)
        sql_f, p_f = _build_query('codFonte', 'fonte')
        fontes_vals = [r[0] for r in db.session.execute(sql_f, p_f).fetchall() if r[0]]

        # Descrições
        nat_map, fonte_map = _carregar_descricoes()
        sql_desc_acao = text("SELECT codigo, titulo FROM acao")
        acao_map = {str(r[0]): r[1] for r in db.session.execute(sql_desc_acao).fetchall() if r[0] and r[1]}

        return jsonify({
            'meses': [{'valor': m, 'label': NOMES_MESES.get(m, str(m))} for m in meses],
            'acoes': [{'codigo': str(a), 'descricao': acao_map.get(str(a), '')} for a in acoes_vals],
            'naturezas': [{'codigo': str(n), 'descricao': nat_map.get(str(n), '')} for n in nats_vals],
            'fontes': [{'codigo': str(f), 'descricao': fonte_map.get(str(f), '')} for f in fontes_vals],
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# =============================================================================
# APIs AJAX para expansão de linhas
# =============================================================================
@financeiro_bp.route('/api/orcamentaria/naturezas/<acao>')
@login_required
@requires_admin_or_pedro
def api_orcamentaria_naturezas(acao):
    """Retorna naturezas de despesa agrupadas para uma ação."""
    ano = request.args.get('ano', 2026, type=int)
    mes = request.args.get('mes', type=int)
    fonte = request.args.get('fonte', '')

    filtro_mes = "AND mes = :mes" if mes else ""
    filtro_fonte = ""
    params = {"ano": ano, "acao": acao}
    if mes:
        params["mes"] = mes
    if fonte:
        filtro_fonte = "AND codFonte = :fonte"
        params["fonte"] = fonte

    sql = text(f"""
        SELECT
            codNatureza as natureza,
            id as conta,
            COALESCE(SUM(saldo), 0) as total
        FROM loa_2026
        WHERE ano = :ano
          AND codAcao = :acao
          {filtro_mes} {filtro_fonte}
        GROUP BY natureza, conta
        ORDER BY natureza
    """)

    rows = db.session.execute(sql, params).fetchall()

    nat_dict = {}
    for nat, conta, total in rows:
        if not nat:
            continue
        if nat not in nat_dict:
            nat_dict[nat] = {}
        nat_dict[nat][conta] = float(_decimal(total))

    resultado = []
    for nat in sorted(nat_dict.keys()):
        contas = nat_dict[nat]
        dot_inicial = contas.get(CONTA_INICIAL, 0)
        dot_suplementar = contas.get(CONTA_SUPLEMENTAR, 0)
        dot_especial = sum(contas.get(c, 0) for c in CONTAS_ESPECIAL)
        dot_extra = sum(contas.get(c, 0) for c in CONTAS_EXTRAORDINARIA)
        contingenciado = contas.get(CONTA_CONTINGENCIADO, 0)
        dot_anulada = contas.get(CONTA_ANULADA, 0)
        credito_disp = contas.get(CONTA_CREDITO_DISPONIVEL, 0)

        dot_atualizada = dot_inicial + dot_suplementar + dot_especial + dot_extra - abs(dot_anulada)
        analise = dot_atualizada - abs(credito_disp) if credito_disp else 0
        pct_exec = round((analise / dot_atualizada * 100), 2) if dot_atualizada else 0

        resultado.append({
            'natureza': nat,
            'dot_inicial': dot_inicial,
            'dot_suplementar': dot_suplementar,
            'dot_especial': dot_especial,
            'dot_extraordinaria': dot_extra,
            'contingenciado': contingenciado,
            'dot_anulada': dot_anulada,
            'dot_atualizada': dot_atualizada,
            'credito_disponivel': credito_disp,
            'analise': analise,
            'pct_exec': pct_exec,
        })

    return jsonify(resultado)


@financeiro_bp.route('/api/orcamentaria/contratos/<acao>/<natureza>')
@login_required
@requires_admin_or_pedro
def api_orcamentaria_contratos(acao, natureza):
    """Retorna contratos filtrados por ação + natureza + fonte,
    com valores de execução (empenho/liquidação/PD/OB)."""
    ano = request.args.get('ano', 2026, type=int)
    mes = request.args.get('mes', type=int)
    fonte = request.args.get('fonte', '')

    # Date range
    if mes:
        dt_ini = f"{ano}-{mes:02d}-01"
        if mes == 12:
            dt_fim = f"{ano + 1}-01-01"
        else:
            dt_fim = f"{ano}-{mes + 1:02d}-01"
    else:
        dt_ini = f"{ano}-01-01"
        dt_fim = f"{ano + 1}-01-01"

    # Filtros condicionais de fonte
    filtro_fonte_reserva = "AND CAST(r.codFonte AS CHAR) = :fonte" if fonte else ""
    filtro_fonte = "AND CAST(codFonte AS CHAR) = :fonte" if fonte else ""

    base_params = {"dt_ini": dt_ini, "dt_fim": dt_fim, "natureza": natureza, "acao": acao}
    if fonte:
        base_params["fonte"] = fonte

    # --- Reservas (ação via codClassificacao segmento 7) ---
    sql_reserva = text(f"""
        SELECT
            r.codContrato,
            c.nomeContratado,
            COALESCE(SUM(
                CASE WHEN r.tipoAlteracao = 'ANULACAO' THEN -r.valor ELSE r.valor END
            ), 0) as total_reservado,
            COUNT(*) as qtd_reservas
        FROM reserva r
        LEFT JOIN contratos c ON r.codContrato = c.codigo
        WHERE r.statusDocumento = 'CONTABILIZADO'
          AND r.dataEmissao >= :dt_ini AND r.dataEmissao < :dt_fim
          AND r.codContrato IS NOT NULL
          AND r.codContrato != ''
          AND CAST(r.codNatureza AS CHAR) = :natureza
          AND {_SQL_ACAO_FROM_CLASS_RESERVA} = :acao
          {filtro_fonte_reserva}
        GROUP BY r.codContrato, c.nomeContratado
        ORDER BY total_reservado DESC
    """)
    rows = db.session.execute(sql_reserva, base_params).fetchall()

    # --- Execução por contrato (empenho, liquidação, PD, OB) ---
    exec_maps = {'emp': {}, 'liq': {}, 'pd': {}, 'ob': {}}

    # Empenho (tem codAcao direto)
    sql_emp = text(f"""
        SELECT CAST(codContrato AS CHAR),
               COALESCE(SUM(
                   CASE WHEN tipoAlteracaoNE = 'ANULACAO' THEN -valor ELSE valor END
               ), 0)
        FROM empenho
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND CAST(codNatureza AS CHAR) = :natureza
          AND CAST(codAcao AS CHAR) = :acao
          AND codContrato IS NOT NULL AND codContrato != 0
          {filtro_fonte}
        GROUP BY codContrato
    """)
    try:
        for r in db.session.execute(sql_emp, base_params).fetchall():
            exec_maps['emp'][str(r[0]).strip()] = float(r[1])
    except Exception:
        pass

    # Liquidação (tipoAlteracao, ação via codClassificacao segmento 8)
    sql_liq = text(f"""
        SELECT CAST(codContrato AS CHAR),
               COALESCE(SUM(
                   CASE WHEN tipoAlteracao = 'ANULACAO' THEN -valor ELSE valor END
               ), 0)
        FROM liquidacao
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND CAST(codNatureza AS CHAR) = :natureza
          AND {_SQL_ACAO_FROM_CLASS} = :acao
          AND codContrato IS NOT NULL
          AND CAST(codContrato AS CHAR) NOT IN ('', '0')
          {filtro_fonte}
        GROUP BY codContrato
    """)
    try:
        for r in db.session.execute(sql_liq, base_params).fetchall():
            exec_maps['liq'][str(r[0]).strip()] = float(r[1])
    except Exception:
        pass

    # PD (sem tipoAlteracao, ação via codClassificacao segmento 8)
    sql_pd = text(f"""
        SELECT CAST(codContrato AS CHAR), COALESCE(SUM(valor), 0)
        FROM pd
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND CAST(codNatureza AS CHAR) = :natureza
          AND {_SQL_ACAO_FROM_CLASS} = :acao
          AND codContrato IS NOT NULL
          AND CAST(codContrato AS CHAR) NOT IN ('', '0')
          {filtro_fonte}
        GROUP BY codContrato
    """)
    try:
        for r in db.session.execute(sql_pd, base_params).fetchall():
            exec_maps['pd'][str(r[0]).strip()] = float(r[1])
    except Exception:
        pass

    # OB (sem tipoAlteracao, ação via codClassificacao segmento 8)
    sql_ob = text(f"""
        SELECT CAST(codContrato AS CHAR), COALESCE(SUM(valor), 0)
        FROM ob
        WHERE statusDocumento = 'CONTABILIZADO'
          AND codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND CAST(codNatureza AS CHAR) = :natureza
          AND {_SQL_ACAO_FROM_CLASS} = :acao
          AND codContrato IS NOT NULL
          AND CAST(codContrato AS CHAR) NOT IN ('', '0')
          {filtro_fonte}
        GROUP BY codContrato
    """)
    try:
        for r in db.session.execute(sql_ob, base_params).fetchall():
            exec_maps['ob'][str(r[0]).strip()] = float(r[1])
    except Exception:
        pass

    resultado = []
    for cod_contrato, nome_contratado, total, qtd in rows:
        key = str(cod_contrato).strip()
        resultado.append({
            'codContrato': cod_contrato,
            'nomeContratado': nome_contratado or '',
            'totalReservado': float(total),
            'qtdReservas': qtd,
            'empenhado': exec_maps['emp'].get(key, 0),
            'liquidado': exec_maps['liq'].get(key, 0),
            'pd': exec_maps['pd'].get(key, 0),
            'pago': exec_maps['ob'].get(key, 0),
        })

    # --- Execuções financeiras sem contrato (fornecedores cadastrados) ---
    sql_exec = text("""
        SELECT
            f.cnpj,
            f.descricao as fornecedor_desc,
            COALESCE(SUM(e.valor), 0) as total_valor,
            COUNT(*) as qtd
        FROM execucoes_orcamentarias e
        JOIN fornecedores_sem_contrato f ON f.id = e.fornecedor_id
        WHERE e.acao = :acao
          AND e.natureza = :natureza
          AND (e.cod_contrato IS NULL OR e.cod_contrato = '')
        GROUP BY f.id, f.cnpj, f.descricao
        ORDER BY f.descricao
    """)
    try:
        exec_rows = db.session.execute(sql_exec, {
            "acao": acao, "natureza": natureza
        }).fetchall()
        for cnpj, desc, total_val, qtd_exec in exec_rows:
            digitos = ''.join(c for c in (cnpj or '') if c.isdigit())
            cnpj_fmt = f'{digitos[:2]}.{digitos[2:5]}.{digitos[5:8]}/{digitos[8:12]}-{digitos[12:14]}' if len(digitos) == 14 else cnpj
            resultado.append({
                'codContrato': None,
                'nomeContratado': desc or '',
                'cnpj': cnpj_fmt,
                'totalReservado': 0,
                'totalValor': float(total_val),
                'qtdExecucoes': qtd_exec,
                'empenhado': 0,
                'liquidado': 0,
                'pd': 0,
                'pago': 0,
                'semContrato': True,
            })
    except Exception:
        pass

    return jsonify(resultado)
