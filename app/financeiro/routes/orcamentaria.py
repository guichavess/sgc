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
    """Formata valor para padrão BRL."""
    if val is None:
        val = ZERO
    return f'R$ {val:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


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

    # --- Listas para filtros ---
    fontes = _listar_fontes(ano)
    meses_disponiveis = _listar_meses(ano)
    naturezas_disponiveis = _listar_naturezas(ano)
    acoes_disponiveis = _listar_acoes(ano)

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
    data_inicio = f"{ano}-01-01"
    data_fim = f"{ano + 1}-01-01"
    params_data = {"dt_ini": data_inicio, "dt_fim": data_fim}

    # Crédito Disponível (LOA)
    sql_credito = text(f"""
        SELECT COALESCE(SUM(saldo), 0) FROM loa_2026
        WHERE ano = :ano AND id = :conta {filtro_mes} {filtro_fonte} {filtro_natureza} {filtro_acao}
    """)
    credito_disp = db.session.execute(
        sql_credito, {**params, "conta": CONTA_CREDITO_DISPONIVEL}
    ).scalar() or 0

    # Reservado
    sql_reservado = text("""
        SELECT COALESCE(SUM(valor), 0) FROM reserva
        WHERE dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
    """)
    reservado = db.session.execute(sql_reservado, params_data).scalar() or 0

    # Empenhado (tabela empenho existente)
    sql_empenhado = text("""
        SELECT COALESCE(SUM(valor), 0) FROM empenho
        WHERE dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
    """)
    empenhado = db.session.execute(sql_empenhado, params_data).scalar() or 0

    # Liquidado
    sql_liquidado = text("""
        SELECT COALESCE(SUM(valor), 0) FROM liquidacao
        WHERE dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
    """)
    try:
        liquidado = db.session.execute(sql_liquidado, params_data).scalar() or 0
    except Exception:
        liquidado = 0

    # PDs a Pagar
    sql_pd = text("""
        SELECT COALESCE(SUM(valor), 0) FROM pd
        WHERE dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
    """)
    try:
        pd_val = db.session.execute(sql_pd, params_data).scalar() or 0
    except Exception:
        pd_val = 0

    # Pago (OB)
    sql_pago = text("""
        SELECT COALESCE(SUM(valor), 0) FROM ob
        WHERE dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND codigoUG = '210101'
    """)
    try:
        pago = db.session.execute(sql_pago, params_data).scalar() or 0
    except Exception:
        pago = 0

    # Dotação Atualizada total (para % execução)
    sql_dotacao = text(f"""
        SELECT id, COALESCE(SUM(saldo), 0) as total FROM loa_2026
        WHERE ano = :ano {filtro_mes} {filtro_fonte} {filtro_natureza} {filtro_acao}
        GROUP BY id
    """)
    rows = db.session.execute(sql_dotacao, params).fetchall()
    totais_conta = {r[0]: _decimal(r[1]) for r in rows}

    dot_inicial = totais_conta.get(CONTA_INICIAL, ZERO)
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

    # Busca títulos das ações na tabela 'acao'
    sql_desc = text("SELECT codigo, titulo FROM acao")
    desc_rows = db.session.execute(sql_desc).fetchall()
    desc_map = {}
    for codigo, titulo in desc_rows:
        if codigo and titulo:
            desc_map[codigo] = titulo

    # Execução financeira por ação (empenho/liquidação/PD/OB)
    exec_por_acao = _calcular_execucao(ano, nivel='acao')

    resultado = []
    totais = {
        'dot_inicial': ZERO, 'dot_suplementar': ZERO, 'dot_especial': ZERO,
        'dot_extraordinaria': ZERO, 'contingenciado': ZERO, 'dot_anulada': ZERO,
        'dot_atualizada': ZERO, 'credito_disponivel': ZERO, 'analise': ZERO,
        'empenhado': ZERO, 'liquidado': ZERO, 'pd': ZERO, 'pago': ZERO,
    }

    for acao_cod in sorted(acoes_dict.keys()):
        contas = acoes_dict[acao_cod]

        dot_inicial = contas.get(CONTA_INICIAL, ZERO)
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
        empenhado = _decimal(ex.get('emp', 0))
        liquidado = _decimal(ex.get('liq', 0))
        pd_val = _decimal(ex.get('pd', 0))
        pago = _decimal(ex.get('ob', 0))

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
            'empenhado': empenhado,
            'pct_emp': _pct(empenhado, dot_atualizada),
            'liquidado': liquidado,
            'pct_liq': _pct(liquidado, dot_atualizada),
            'pd': pd_val,
            'pct_pd': _pct(pd_val, dot_atualizada),
            'pago': pago,
            'pct_pago': _pct(pago, dot_atualizada),
        }
        resultado.append(item)

        for key in totais:
            totais[key] += item.get(key, ZERO)

    totais['pct_exec'] = _pct(totais['analise'], totais['dot_atualizada'])
    totais['pct_emp'] = _pct(totais['empenhado'], totais['dot_atualizada'])
    totais['pct_liq'] = _pct(totais['liquidado'], totais['dot_atualizada'])
    totais['pct_pd'] = _pct(totais['pd'], totais['dot_atualizada'])
    totais['pct_pago'] = _pct(totais['pago'], totais['dot_atualizada'])

    return {'itens': resultado, 'totais': totais}


# Expressão SQL para extrair codAcao de codClassificacao (segmento 8 após remover espaços)
_SQL_ACAO_FROM_CLASS = "SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(codClassificacao, ' ', ''), '.', 8), '.', -1)"


def _calcular_execucao(ano, nivel='acao'):
    """Calcula valores de empenho/liquidação/PD/OB agrupados por ação (ou ação+natureza).

    Args:
        ano: Ano fiscal.
        nivel: 'acao' retorna {acao: {emp, liq, pd, ob}},
               'natureza' retorna {(acao, nat): {emp, liq, pd, ob}}.
    """
    dt_ini = f"{ano}-01-01"
    dt_fim = f"{ano + 1}-01-01"
    params = {"dt_ini": dt_ini, "dt_fim": dt_fim}

    group_nat = ", codNatureza" if nivel == 'natureza' else ""
    select_nat = ", codNatureza" if nivel == 'natureza' else ""

    resultado = {}

    # --- Empenho (tem codAcao direto) ---
    sql_emp = text(f"""
        SELECT codAcao as acao {select_nat}, COALESCE(SUM(valor), 0) as total
        FROM empenho
        WHERE codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
        GROUP BY acao {group_nat}
    """)
    for r in db.session.execute(sql_emp, params).fetchall():
        key = (str(r[0]), str(r[1])) if nivel == 'natureza' else str(r[0])
        val = float(r[-1])
        resultado.setdefault(key, {'emp': 0, 'liq': 0, 'pd': 0, 'ob': 0})
        resultado[key]['emp'] += val

    # --- Liquidação, PD, OB (extrair ação do codClassificacao) ---
    tabelas = [('liquidacao', 'liq'), ('pd', 'pd'), ('ob', 'ob')]
    for tabela, campo in tabelas:
        sql = text(f"""
            SELECT {_SQL_ACAO_FROM_CLASS} as acao {select_nat},
                   COALESCE(SUM(valor), 0) as total
            FROM {tabela}
            WHERE codigoUG = '210101'
              AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
            GROUP BY acao {group_nat}
        """)
        try:
            for r in db.session.execute(sql, params).fetchall():
                key = (str(r[0]), str(r[1])) if nivel == 'natureza' else str(r[0])
                val = float(r[-1])
                resultado.setdefault(key, {'emp': 0, 'liq': 0, 'pd': 0, 'ob': 0})
                resultado[key][campo] += val
        except Exception:
            pass  # Tabela pode não existir

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
    exec_por_nat = _calcular_execucao(ano, nivel='natureza')

    # Agrupa: acao -> natureza -> conta -> total
    tree = {}
    for acao, nat, conta, total in rows:
        if not acao or not nat:
            continue
        tree.setdefault(acao, {}).setdefault(nat, {})[conta] = float(_decimal(total))

    # Calcula dotações por natureza em cada ação
    resultado = {}
    for acao, nats in tree.items():
        lista = []
        for nat in sorted(nats.keys()):
            contas = nats[nat]
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

            # Dados de execução financeira para esta ação+natureza
            ex = exec_por_nat.get((str(acao), str(nat)), {})
            emp = float(ex.get('emp', 0))
            liq = float(ex.get('liq', 0))
            pd_val = float(ex.get('pd', 0))
            ob = float(ex.get('ob', 0))

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
                'empenhado': emp,
                'pct_emp': round((emp / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'liquidado': liq,
                'pct_liq': round((liq / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'pd': pd_val,
                'pct_pd': round((pd_val / dot_atualizada * 100), 2) if dot_atualizada else 0,
                'pago': ob,
                'pct_pago': round((ob / dot_atualizada * 100), 2) if dot_atualizada else 0,
            })
        resultado[acao] = lista

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
    return [{'codigo': c, 'descricao': fonte_map.get(str(c), '')} for c in codigos]


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
    return [{'codigo': c, 'descricao': nat_map.get(str(c), '')} for c in codigos]


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

    return [{'codigo': c, 'descricao': desc_map.get(str(c), '')} for c in codigos]


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
    """Retorna contratos da tabela reserva filtrados por ação + natureza,
    com valores de execução (empenho/liquidação/PD/OB)."""
    ano = request.args.get('ano', 2026, type=int)

    dt_ini = f"{ano}-01-01"
    dt_fim = f"{ano + 1}-01-01"
    base_params = {"dt_ini": dt_ini, "dt_fim": dt_fim, "natureza": natureza}

    # --- Reservas ---
    sql_reserva = text("""
        SELECT
            r.codContrato,
            c.nomeContratado,
            COALESCE(SUM(r.valor), 0) as total_reservado,
            COUNT(*) as qtd_reservas
        FROM reserva r
        LEFT JOIN contratos c ON r.codContrato = c.codigo
        WHERE r.dataEmissao >= :dt_ini AND r.dataEmissao < :dt_fim
          AND r.codContrato IS NOT NULL
          AND r.codContrato != ''
          AND CAST(r.codNatureza AS CHAR) = :natureza
        GROUP BY r.codContrato, c.nomeContratado
        ORDER BY total_reservado DESC
    """)
    rows = db.session.execute(sql_reserva, base_params).fetchall()

    # --- Execução por contrato (empenho, liquidação, PD, OB) ---
    exec_maps = {'emp': {}, 'liq': {}, 'pd': {}, 'ob': {}}

    # Empenho (codContrato é BigInteger)
    sql_emp = text("""
        SELECT CAST(codContrato AS CHAR), COALESCE(SUM(valor), 0)
        FROM empenho
        WHERE codigoUG = '210101'
          AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
          AND CAST(codNatureza AS CHAR) = :natureza
          AND codContrato IS NOT NULL AND codContrato != 0
        GROUP BY codContrato
    """)
    try:
        for r in db.session.execute(sql_emp, base_params).fetchall():
            exec_maps['emp'][str(r[0]).strip()] = float(r[1])
    except Exception:
        pass

    # Liquidação, PD, OB
    for tabela, campo in [('liquidacao', 'liq'), ('pd', 'pd'), ('ob', 'ob')]:
        sql = text(f"""
            SELECT CAST(codContrato AS CHAR), COALESCE(SUM(valor), 0)
            FROM {tabela}
            WHERE codigoUG = '210101'
              AND dataEmissao >= :dt_ini AND dataEmissao < :dt_fim
              AND CAST(codNatureza AS CHAR) = :natureza
              AND codContrato IS NOT NULL
              AND CAST(codContrato AS CHAR) NOT IN ('', '0')
            GROUP BY codContrato
        """)
        try:
            for r in db.session.execute(sql, base_params).fetchall():
                exec_maps[campo][str(r[0]).strip()] = float(r[1])
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

    return jsonify(resultado)
