"""
Rotas do Dashboard.
"""
from datetime import datetime
from flask import render_template, session, redirect, url_for, request
from flask_login import login_required, current_user
from sqlalchemy import func

from app.solicitacoes.routes import solicitacoes_bp
from app.models import Solicitacao, Contrato, Etapa, TipoPagamento, StatusEmpenho, SolicitacaoEmpenho
from app.models.saldo import SaldoEmpenho
from app.models.historico import HistoricoMovimentacao
from app.extensions import db
from app.repositories import SolicitacaoRepository
from app.constants import MESES_PT_BR, normalizar_competencia, NUMERO_PARA_MES
from app.utils.permissions import requires_permission


@solicitacoes_bp.route('/dashboard')
@login_required
@requires_permission('solicitacoes.visualizar')
def dashboard():
    """Exibe o dashboard principal com listagem de solicitações."""
    # Parâmetros de filtro (nomes coincidem com o template)
    filtro_busca = request.args.get('q', '').strip()
    filtro_competencias_raw = request.args.getlist('filtro_competencia')  # multi-checkbox
    filtro_etapas = [int(e) for e in request.args.getlist('filtro_etapa') if e.isdigit()]
    filtro_tipos = [int(t) for t in request.args.getlist('filtro_tipo') if t.isdigit()]
    filtro_status_empenho = [int(s) for s in request.args.getlist('filtro_status_empenho') if s.isdigit()]
    filtro_data_inicio = request.args.get('data_inicio', '').strip()
    filtro_data_fim = request.args.get('data_fim', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 20

    # Converte datas se informadas
    data_inicio = None
    data_fim = None
    if filtro_data_inicio:
        try:
            data_inicio = datetime.strptime(filtro_data_inicio, '%Y-%m-%d')
        except ValueError:
            pass
    if filtro_data_fim:
        try:
            data_fim = datetime.strptime(filtro_data_fim, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        except ValueError:
            pass

    # Competências: normaliza e monta mapeamento nome_exibição → valores reais no banco
    todas_competencias_raw = SolicitacaoRepository.listar_competencias_distintas()
    mapa_comp = {}  # normalizado → set de valores originais do banco
    for c in todas_competencias_raw:
        norm = normalizar_competencia(c)
        mapa_comp.setdefault(norm, set()).add(c)

    # Expande filtro de competência: "Fevereiro/2025" → ["Fevereiro/2025", "02/2025"]
    filtro_competencias_expandido = []
    for fc in filtro_competencias_raw:
        if fc in mapa_comp:
            filtro_competencias_expandido.extend(mapa_comp[fc])
        else:
            filtro_competencias_expandido.append(fc)

    # Busca solicitações com filtros
    pagination = SolicitacaoRepository.listar_com_filtros(
        busca=filtro_busca or None,
        competencias=filtro_competencias_expandido or None,
        etapa_ids=filtro_etapas or None,
        tipo_pagamento_ids=filtro_tipos or None,
        status_empenho_ids=filtro_status_empenho or None,
        data_inicio=data_inicio,
        data_fim=data_fim,
        page=page,
        per_page=per_page
    )

    solicitacoes = pagination.items

    # =========================================================================
    # PRE-LOAD: buscar dados que as properties N+1 buscam individualmente
    # Evita dezenas de queries extras por página (saldo, empenho, tempo, natureza)
    # =========================================================================
    sol_ids = [s.id for s in solicitacoes]

    # 1) Saldos por (contrato, competência) — usado em saldo_atual_contrato
    saldos_map = {}  # (cod_contrato, competencia) → SaldoEmpenho
    if solicitacoes:
        pares = list({(s.codigo_contrato, s.competencia) for s in solicitacoes if s.competencia})
        if pares:
            # Subconsulta para pegar o saldo mais recente por (contrato, competência)
            sub = db.session.query(
                SaldoEmpenho.cod_contrato,
                SaldoEmpenho.competencia,
                func.max(SaldoEmpenho.data).label('max_data')
            ).filter(
                db.tuple_(SaldoEmpenho.cod_contrato, SaldoEmpenho.competencia).in_(pares)
            ).group_by(SaldoEmpenho.cod_contrato, SaldoEmpenho.competencia).subquery()

            saldos_rows = SaldoEmpenho.query.join(
                sub,
                db.and_(
                    SaldoEmpenho.cod_contrato == sub.c.cod_contrato,
                    SaldoEmpenho.competencia == sub.c.competencia,
                    SaldoEmpenho.data == sub.c.max_data,
                )
            ).all()
            for s in saldos_rows:
                saldos_map[(s.cod_contrato, s.competencia)] = s

    # 2) Valor empenho solicitado — SolicitacaoEmpenho.valor por id_solicitacao
    empenho_vals = {}
    if sol_ids:
        emp_rows = SolicitacaoEmpenho.query.filter(
            SolicitacaoEmpenho.id_solicitacao.in_(sol_ids)
        ).all()
        for e in emp_rows:
            empenho_vals[e.id_solicitacao] = e.valor

    # 3) Primeira movimentação (para tempo_decorrido_visual em processos abertos)
    primeiro_hist = {}
    if sol_ids:
        sub_hist = db.session.query(
            HistoricoMovimentacao.id_solicitacao,
            func.min(HistoricoMovimentacao.data_movimentacao).label('min_data')
        ).filter(
            HistoricoMovimentacao.id_solicitacao.in_(sol_ids)
        ).group_by(HistoricoMovimentacao.id_solicitacao).all()
        for sol_id, min_data in sub_hist:
            primeiro_hist[sol_id] = min_data

    # Montar dicts para o template acessar sem N+1
    saldos_preloaded = {}
    tempos_preloaded = {}
    for s in solicitacoes:
        saldos_preloaded[s.id] = saldos_map.get((s.codigo_contrato, s.competencia))

        # Calcular tempo decorrido visual
        if s.tempo_total:
            tempos_preloaded[s.id] = s.tempo_total
        else:
            data_inicio_proc = primeiro_hist.get(s.id) or s.data_solicitacao
            if data_inicio_proc:
                diff = datetime.now() - data_inicio_proc
                days = diff.days
                total_seconds = int(diff.total_seconds())
                hours = (total_seconds % 86400) // 3600
                minutes = (total_seconds % 3600) // 60
                if days > 0:
                    tempos_preloaded[s.id] = f"{days} dias"
                elif hours > 0:
                    tempos_preloaded[s.id] = f"{hours} horas"
                elif minutes > 0:
                    tempos_preloaded[s.id] = f"{minutes} min"
                else:
                    tempos_preloaded[s.id] = "recentemente"
            else:
                tempos_preloaded[s.id] = "--"

    # Busca etapas para filtro
    todas_etapas = Etapa.query.order_by(Etapa.ordem).all()

    # Busca tipos de pagamento para filtro
    todos_tipos = TipoPagamento.query.order_by(TipoPagamento.id).all()

    # Busca status de empenho para filtro
    todos_status_empenho = StatusEmpenho.query.order_by(StatusEmpenho.id).all()

    # Lista normalizada de competências (sem duplicatas)
    todas_competencias = list(mapa_comp.keys())

    # Ordena competências
    def chave_ordenacao(comp_str):
        try:
            partes = comp_str.split('/')
            if len(partes) == 2:
                mes_nome, ano = partes
                return (int(ano), MESES_PT_BR.get(mes_nome.capitalize(), 0))
        except (ValueError, IndexError, AttributeError):
            pass
        return (0, 0)

    todas_competencias.sort(key=chave_ordenacao, reverse=True)

    # Contagens para os filtros — query única retornando todas as dimensões
    contagem_rows = db.session.query(
        Solicitacao.competencia,
        Solicitacao.etapa_atual_id,
        Solicitacao.id_tipo_pagamento,
        Solicitacao.status_empenho_id,
        func.count(Solicitacao.id)
    ).group_by(
        Solicitacao.competencia,
        Solicitacao.etapa_atual_id,
        Solicitacao.id_tipo_pagamento,
        Solicitacao.status_empenho_id,
    ).all()

    contagem_comp_raw = {}
    contagem_etapas = {}
    contagem_tipos = {}
    contagem_status_empenho = {}
    for comp, etapa, tipo, st_emp, cnt in contagem_rows:
        contagem_comp_raw[comp] = contagem_comp_raw.get(comp, 0) + cnt
        contagem_etapas[etapa] = contagem_etapas.get(etapa, 0) + cnt
        contagem_tipos[tipo] = contagem_tipos.get(tipo, 0) + cnt
        contagem_status_empenho[st_emp] = contagem_status_empenho.get(st_emp, 0) + cnt

    contagem_competencias = {}
    for norm, originais in mapa_comp.items():
        contagem_competencias[norm] = sum(contagem_comp_raw.get(o, 0) for o in originais)

    return render_template(
        'solicitacoes/dashboard.html',
        solicitacoes=solicitacoes,
        pagination=pagination,
        todas_etapas=todas_etapas,
        todos_tipos=todos_tipos,
        todos_status_empenho=todos_status_empenho,
        todas_competencias=todas_competencias,
        filtro_etapas=filtro_etapas,
        filtro_tipos=filtro_tipos,
        filtro_status_empenho=filtro_status_empenho,
        contagem_competencias=contagem_competencias,
        contagem_etapas=contagem_etapas,
        contagem_tipos=contagem_tipos,
        contagem_status_empenho=contagem_status_empenho,
        # Dados pré-carregados (evita N+1 no template)
        saldos_preloaded=saldos_preloaded,
        empenho_vals=empenho_vals,
        tempos_preloaded=tempos_preloaded,
    )
