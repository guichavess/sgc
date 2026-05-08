"""
Rotas administrativas do módulo de Diárias (gerenciar agências, cargos/valores, administração).
"""
import json
from datetime import datetime
from flask import render_template, request, redirect, url_for, flash, abort, current_app, jsonify, session
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission, usuario_tem_caixa, CAIXA_APOIOSGA, CAIXA_NCI
from app.services.diaria_service import DiariaService
from app.services.sei_auth import autenticar_usuario_sei
from app.services.sei_integration import assinar_documento
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, gerar_despacho_sga, gerar_despacho_sga_negacao,
    gerar_analise_pagamento,
    gerar_despacho_nci, enviar_procedimento, sincronizar_documentos_diaria,
    UNIDADE_APOIOSGA, UNIDADE_NCI, PERGUNTAS_ANALISE_PAGAMENTO,
)
from app.models.diaria import (
    DiariasValorCargo, DiariasCargo,
    DiariasItinerario, DiariasItemItinerario,
)
from app.services.vincular_processo_diaria import (
    verificar_protocolo_sei,
    vincular_processo_sei,
    importar_processo_sei_como_novo,
    sincronizar_processos_bloco_diarias,
    prever_processos_bloco_diarias,
)
from app.constants import DiariasEtapaID
from app.extensions import db
from app.services.diarias_notification import DiariasNotifier


# ── Agências (leitura a partir dos contratos com Natureza 339033) ────────────

@diarias_bp.route('/agencias')
@login_required
@requires_permission('diarias.aprovar')
def agencias():
    """Lista agências de viagem vinculadas a contratos com Natureza 339033."""
    return render_template('diarias/agencias.html',
        agencias=DiariaService.get_agencias(),
    )


# ── Cargos e Valores ────────────────────────────────────────────────────────

@diarias_bp.route('/cargos')
@login_required
@requires_permission('diarias.aprovar')
def cargos():
    """Lista cargos e valores de diárias."""
    return render_template('diarias/cargos.html',
        cargos=DiariasCargo.query.order_by(DiariasCargo.nome).all(),
        valores=DiariasValorCargo.query.order_by(
            DiariasValorCargo.cargo_id,
            DiariasValorCargo.tipo_itinerario_id,
        ).all(),
    )


@diarias_bp.route('/cargos/salvar-valor', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def salvar_valor_cargo():
    """Cria ou atualiza um valor de diária."""
    vc_id = request.form.get('id')
    cargo_id = request.form.get('cargo_id', type=int)
    tipo_id = request.form.get('tipo_itinerario_id', type=int)
    valor = request.form.get('valor', type=float)

    if cargo_id is None or tipo_id is None or valor is None:
        flash('Todos os campos são obrigatórios.', 'danger')
        return redirect(url_for('diarias.cargos'))

    if valor <= 0:
        flash('O valor da diária deve ser maior que zero.', 'danger')
        return redirect(url_for('diarias.cargos'))

    if vc_id:
        vc = DiariasValorCargo.query.get(int(vc_id))
        if vc:
            vc.cargo_id = cargo_id
            vc.tipo_itinerario_id = tipo_id
            vc.valor = valor
        else:
            flash('Registro não encontrado.', 'danger')
            return redirect(url_for('diarias.cargos'))
    else:
        vc = DiariasValorCargo(cargo_id=cargo_id, tipo_itinerario_id=tipo_id, valor=valor)
        db.session.add(vc)

    db.session.commit()
    flash('Valor salvo com sucesso!', 'success')
    return redirect(url_for('diarias.cargos'))


# ── Administração / Acompanhamento ────────────────────────────────────────

@diarias_bp.route('/administracao/vincular-processo', methods=['GET'])
@login_required
@requires_permission('diarias.aprovar')
def vincular_processo_pagina():
    """Página dedicada para vincular/importar um processo SEI como nova solicitação."""
    from app.models.diaria import DiariasEtapa
    etapas = DiariasEtapa.query.order_by(DiariasEtapa.ordem).all()
    return render_template('diarias/vincular_processo.html', etapas=etapas)


@diarias_bp.route('/administracao/importar-processo', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def importar_processo():
    """
    Importa um processo SEI já existente como nova solicitação no sistema.

    Recebe protocolo_sei + etapa_id, consulta o SEI, parseia a Requisição de
    Diárias, cria um novo DiariasItinerario com os dados encontrados e redireciona
    para o detalhe do novo itinerário.
    """
    protocolo_sei = request.form.get('protocolo_sei', '').strip()
    etapa_id_raw = request.form.get('etapa_id', '').strip()

    if not protocolo_sei:
        flash('Informe o número do processo SEI.', 'danger')
        return redirect(url_for('diarias.vincular_processo_pagina'))

    if not etapa_id_raw:
        flash('Selecione a etapa atual do processo.', 'danger')
        return redirect(url_for('diarias.vincular_processo_pagina'))

    try:
        etapa_id = int(etapa_id_raw)
    except (ValueError, TypeError):
        flash('Etapa inválida.', 'danger')
        return redirect(url_for('diarias.vincular_processo_pagina'))

    resultado = importar_processo_sei_como_novo(
        protocolo_sei=protocolo_sei,
        etapa_id=etapa_id,
        usuario_id=current_user.id,
        usuario_gerador=current_user.sigla_login or 'importacao',
    )

    if resultado['sucesso']:
        db.session.commit()
        novo_id = resultado['itinerario_id']
        flash(f'Processo {protocolo_sei} importado com sucesso.', 'success')
        return redirect(url_for('diarias.administracao_detalhe', id=novo_id))
    else:
        db.session.rollback()
        flash(
            f'Erro ao importar processo: {resultado.get("erro", "Erro desconhecido.")}',
            'danger',
        )
        return redirect(url_for('diarias.vincular_processo_pagina'))


@diarias_bp.route('/administracao/sincronizar-processos', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def sincronizar_processos():
    """Sincroniza processos do bloco SEI oficial de diárias."""
    wants_json = (
        request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        or request.accept_mimetypes.best == 'application/json'
    )
    token = session.get('sei_token')
    if not token:
        msg = 'Token SEI do usuário não encontrado. Faça login novamente.'
        if wants_json:
            return jsonify({'sucesso': False, 'erro': msg}), 401
        flash(msg, 'danger')
        return redirect(url_for('diarias.administracao'))

    if request.form.get('acao') == 'precheck':
        resultado_precheck = prever_processos_bloco_diarias(token=token)
        status = 200 if resultado_precheck.get('sucesso') else 500
        return jsonify(resultado_precheck), status

    try:
        resultado = sincronizar_processos_bloco_diarias(
            token=token,
            usuario_id=current_user.id,
            usuario_gerador=current_user.sigla_login or current_user.nome or 'sincronizacao',
        )
        if resultado.get('sucesso'):
            db.session.commit()
            if wants_json:
                return jsonify(resultado)
            flash(
                f"Sincronização concluída: {resultado.get('criados', 0)} processo(s) criado(s), "
                f"{resultado.get('existentes', 0)} já existente(s).",
                'success',
            )
            if resultado.get('erros'):
                flash(f"{len(resultado['erros'])} processo(s) não puderam ser sincronizados.", 'warning')
        else:
            db.session.rollback()
            if wants_json:
                return jsonify(resultado), 500
            flash(resultado.get('erro') or 'Não foi possível sincronizar o bloco SEI.', 'danger')
    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception('[DIARIAS] Erro inesperado ao sincronizar processos do bloco')
        if wants_json:
            return jsonify({'sucesso': False, 'erro': f'Erro ao sincronizar processos: {exc}'}), 500
        flash(f'Erro ao sincronizar processos: {exc}', 'danger')

    return redirect(url_for('diarias.administracao'))


@diarias_bp.route('/administracao')
@login_required
@requires_permission('diarias.aprovar')
def administracao():
    """
    Painel de administração: lista todas as solicitações com dados resumidos
    e progresso na timeline para o gestor acompanhar.
    """
    busca = request.args.get('q', '').strip()
    filtro_etapas = request.args.getlist('filtro_etapa', type=int)
    filtro_tipos = request.args.getlist('filtro_tipo', type=int)
    filtro_solicitacao = request.args.getlist('filtro_solicitacao', type=int)
    filtro_negados = request.args.get('negados', '').strip()
    try:
        page = max(1, int(request.args.get('page', 1) or 1))
    except (ValueError, TypeError):
        page = 1

    query = DiariasItinerario.query

    # Filtro de processos negados: negados=1 mostra só negados; padrão oculta negados
    if filtro_negados == '1':
        query = query.filter(DiariasItinerario.processo_negado.is_(True))
    else:
        query = query.filter(
            db.or_(
                DiariasItinerario.processo_negado.is_(False),
                DiariasItinerario.processo_negado.is_(None),
            )
        )

    if busca:
        query = query.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    if filtro_etapas:
        query = query.filter(DiariasItinerario.etapa_atual_id.in_(filtro_etapas))

    if filtro_tipos:
        query = query.filter(DiariasItinerario.tipo_itinerario.in_(filtro_tipos))

    if filtro_solicitacao:
        query = query.filter(DiariasItinerario.tipo_solicitacao_id.in_(filtro_solicitacao))

    pagination = query.order_by(
        DiariasItinerario.data_solicitacao.desc()
    ).paginate(page=page, per_page=20, error_out=False)

    itinerarios = pagination.items

    # Conta pessoas por itinerário
    pessoas_count = {}
    if itinerarios:
        ids = [it.id for it in itinerarios]
        counts = db.session.query(
            DiariasItemItinerario.id_itinerario,
            db.func.count(DiariasItemItinerario.id)
        ).filter(
            DiariasItemItinerario.id_itinerario.in_(ids)
        ).group_by(DiariasItemItinerario.id_itinerario).all()
        pessoas_count = {row[0]: row[1] for row in counts}

    from app.models.diaria import DiariasEtapa
    etapas = DiariasEtapa.query.order_by(DiariasEtapa.ordem).all()

    # ── Contagens dinâmicas (cruzadas entre filtros) ──────────────────────
    # Base query com busca textual + filtro de negados (comum a todos os filtros)
    base_q = DiariasItinerario.query
    if filtro_negados == '1':
        base_q = base_q.filter(DiariasItinerario.processo_negado.is_(True))
    else:
        base_q = base_q.filter(
            db.or_(
                DiariasItinerario.processo_negado.is_(False),
                DiariasItinerario.processo_negado.is_(None),
            )
        )
    if busca:
        base_q = base_q.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    # Contagem de etapas: aplica filtros de tipo + solicitação
    etapa_count_q = base_q
    if filtro_tipos:
        etapa_count_q = etapa_count_q.filter(DiariasItinerario.tipo_itinerario.in_(filtro_tipos))
    if filtro_solicitacao:
        etapa_count_q = etapa_count_q.filter(DiariasItinerario.tipo_solicitacao_id.in_(filtro_solicitacao))
    etapa_counts = {
        row[0]: row[1] for row in etapa_count_q.with_entities(
            DiariasItinerario.etapa_atual_id,
            db.func.count(DiariasItinerario.id)
        ).group_by(DiariasItinerario.etapa_atual_id).all()
    }

    # Contagem de tipos itinerário: aplica filtros de etapa + solicitação
    tipo_count_q = base_q
    if filtro_etapas:
        tipo_count_q = tipo_count_q.filter(DiariasItinerario.etapa_atual_id.in_(filtro_etapas))
    if filtro_solicitacao:
        tipo_count_q = tipo_count_q.filter(DiariasItinerario.tipo_solicitacao_id.in_(filtro_solicitacao))
    tipo_counts = {
        row[0]: row[1] for row in tipo_count_q.with_entities(
            DiariasItinerario.tipo_itinerario,
            db.func.count(DiariasItinerario.id)
        ).group_by(DiariasItinerario.tipo_itinerario).all()
    }

    # Contagem de tipos solicitação: aplica filtros de etapa + tipo itinerário
    sol_count_q = base_q
    if filtro_etapas:
        sol_count_q = sol_count_q.filter(DiariasItinerario.etapa_atual_id.in_(filtro_etapas))
    if filtro_tipos:
        sol_count_q = sol_count_q.filter(DiariasItinerario.tipo_itinerario.in_(filtro_tipos))
    sol_counts = {
        row[0]: row[1] for row in sol_count_q.with_entities(
            DiariasItinerario.tipo_solicitacao_id,
            db.func.count(DiariasItinerario.id)
        ).group_by(DiariasItinerario.tipo_solicitacao_id).all()
    }

    # Tipos de itinerário fixos (1=Estadual, 2=Nacional, 3=Internacional)
    tipos_itinerario = [
        {'id': 1, 'nome': 'Estadual', 'cor': '#0dcaf0', 'icone': 'bi-geo-alt'},
        {'id': 2, 'nome': 'Nacional', 'cor': '#ffc107', 'icone': 'bi-airplane'},
        {'id': 3, 'nome': 'Internacional', 'cor': '#dc3545', 'icone': 'bi-globe'},
    ]

    # Tipos de solicitação (1=Apenas Diárias, 2=Diárias+Passagens, 3=Apenas Passagens)
    tipos_solicitacao = [
        {'id': 1, 'nome': 'Apenas Diárias', 'cor': '#198754', 'icone': 'bi-cash-stack'},
        {'id': 2, 'nome': 'Diárias + Passagens', 'cor': '#6f42c1', 'icone': 'bi-ticket-perforated'},
        {'id': 3, 'nome': 'Apenas Passagens', 'cor': '#0d6efd', 'icone': 'bi-airplane-engines'},
    ]

    # ── KPIs globais (toda a query filtrada, não só a página atual) ───────────
    from app.constants import DiariasEtapaID

    kpi_row = query.with_entities(
        db.func.count(DiariasItinerario.id),
        db.func.coalesce(db.func.sum(DiariasItinerario.valor_total), 0),
    ).one()

    # Em andamento = não estão na etapa final (Prestação de Contas)
    em_andamento_kpi = query.filter(
        DiariasItinerario.etapa_atual_id != DiariasEtapaID.PRESTACAO_CONTAS.value
    ).count()

    total_pessoas_kpi = db.session.query(
        db.func.count(DiariasItemItinerario.id)
    ).join(
        DiariasItinerario,
        DiariasItemItinerario.id_itinerario == DiariasItinerario.id
    ).filter(
        DiariasItinerario.id.in_(query.with_entities(DiariasItinerario.id))
    ).scalar() or 0

    kpis = {
        'total_solicitacoes': kpi_row[0],
        'valor_total': kpi_row[1],
        'em_andamento': em_andamento_kpi,
        'total_pessoas': total_pessoas_kpi,
    }

    return render_template(
        'diarias/administracao.html',
        itinerarios=itinerarios,
        pagination=pagination,
        pessoas_count=pessoas_count,
        etapas=etapas,
        etapa_counts=etapa_counts,
        filtro_etapas=filtro_etapas,
        tipos_itinerario=tipos_itinerario,
        tipo_counts=tipo_counts,
        filtro_tipos=filtro_tipos,
        tipos_solicitacao=tipos_solicitacao,
        sol_counts=sol_counts,
        filtro_solicitacao=filtro_solicitacao,
        kpis=kpis,
        filtro_negados=filtro_negados,
    )


@diarias_bp.route('/aprovar')
@login_required
def aprovar_solicitacoes():
    """
    Lista solicitações na etapa 1 pendentes de assinatura do Superintendente.
    Visível apenas para superintendentes. Filtra solicitações cujo solicitante
    pertence à mesma superintendência, identificada via Sigla SEI
    (current_user.superintendencia_sigla) preenchida no login.
    """
    eh_secretario = current_user.is_secretario
    eh_super = current_user.is_superintendente

    if not (eh_super or eh_secretario):
        flash('Acesso restrito a Superintendentes e Secretário.', 'warning')
        return redirect(url_for('diarias.dashboard'))

    # Superintendente precisa ter superintendencia_sigla; Secretário vê tudo
    minha_super = current_user.superintendencia_sigla
    if eh_super and not minha_super:
        flash(
            'Sua unidade SEI ainda não foi identificada. '
            'Faça logout e login novamente para atualizar seus dados.',
            'warning',
        )
        return redirect(url_for('diarias.dashboard'))

    # Super da SGA (SGACG) autoriza solicitações de TODAS as superintendências
    from app.utils.unidade_sei import SUPER_SGA_SIGLA
    eh_super_sga = eh_super and (minha_super == SUPER_SGA_SIGLA)

    # Parâmetros de filtro (mesmos da rota administracao)
    busca = request.args.get('q', '').strip()
    filtro_tipos = request.args.getlist('filtro_tipo', type=int)
    filtro_solicitacao = request.args.getlist('filtro_solicitacao', type=int)
    try:
        page = max(1, int(request.args.get('page', 1) or 1))
    except (ValueError, TypeError):
        page = 1

    # Filtra por papel
    from app.models.usuario import Usuario

    query = DiariasItinerario.query.filter(
        DiariasItinerario.etapa_atual_id == DiariasEtapaID.SOLICITACAO_INICIAL,
        db.or_(
            DiariasItinerario.processo_negado.is_(False),
            DiariasItinerario.processo_negado.is_(None),
        ),
    )

    if eh_secretario:
        # Secretário: solicitações já assinadas pelo Super, aguardando autorização
        query = query.filter(DiariasItinerario.superintendente_assinou == True)  # noqa: E712
    else:
        # Super: solicitações ainda NÃO assinadas
        query = query.filter(DiariasItinerario.superintendente_assinou == False)  # noqa: E712
        if not eh_super_sga:
            # Super comum: filtra pela própria superintendência
            query = (
                query
                .join(Usuario, Usuario.sigla_login == DiariasItinerario.usuario_gerador)
                .filter(Usuario.superintendencia_sigla == minha_super)
            )

    if busca:
        query = query.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    if filtro_tipos:
        query = query.filter(DiariasItinerario.tipo_itinerario.in_(filtro_tipos))

    if filtro_solicitacao:
        query = query.filter(DiariasItinerario.tipo_solicitacao_id.in_(filtro_solicitacao))

    query = query.order_by(DiariasItinerario.data_solicitacao.desc(), DiariasItinerario.id.desc())

    pagination = query.paginate(page=page, per_page=20, error_out=False)
    itinerarios = pagination.items

    # Pré-carrega contagem de pessoas (batch query, evita N+1)
    pessoas_count = {}
    if itinerarios:
        ids = [it.id for it in itinerarios]
        counts = db.session.query(
            DiariasItemItinerario.id_itinerario,
            db.func.count(DiariasItemItinerario.id)
        ).filter(
            DiariasItemItinerario.id_itinerario.in_(ids)
        ).group_by(DiariasItemItinerario.id_itinerario).all()
        pessoas_count = {row[0]: row[1] for row in counts}

    # Contagens cruzadas (aplicam mesmos filtros da query principal por papel)
    base_q = DiariasItinerario.query.filter(
        DiariasItinerario.etapa_atual_id == DiariasEtapaID.SOLICITACAO_INICIAL,
        db.or_(
            DiariasItinerario.processo_negado.is_(False),
            DiariasItinerario.processo_negado.is_(None),
        ),
    )
    if eh_secretario:
        base_q = base_q.filter(DiariasItinerario.superintendente_assinou == True)  # noqa: E712
    else:
        base_q = base_q.filter(DiariasItinerario.superintendente_assinou == False)  # noqa: E712
        if not eh_super_sga:
            base_q = (
                base_q
                .join(Usuario, Usuario.sigla_login == DiariasItinerario.usuario_gerador)
                .filter(Usuario.superintendencia_sigla == minha_super)
            )

    if busca:
        base_q = base_q.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    tipo_count_q = base_q
    if filtro_solicitacao:
        tipo_count_q = tipo_count_q.filter(DiariasItinerario.tipo_solicitacao_id.in_(filtro_solicitacao))
    tipo_counts = {
        row[0]: row[1] for row in tipo_count_q.with_entities(
            DiariasItinerario.tipo_itinerario,
            db.func.count(DiariasItinerario.id)
        ).group_by(DiariasItinerario.tipo_itinerario).all()
    }

    sol_count_q = base_q
    if filtro_tipos:
        sol_count_q = sol_count_q.filter(DiariasItinerario.tipo_itinerario.in_(filtro_tipos))
    sol_counts = {
        row[0]: row[1] for row in sol_count_q.with_entities(
            DiariasItinerario.tipo_solicitacao_id,
            db.func.count(DiariasItinerario.id)
        ).group_by(DiariasItinerario.tipo_solicitacao_id).all()
    }

    tipos_itinerario = [
        {'id': 1, 'nome': 'Estadual', 'cor': '#0dcaf0', 'icone': 'bi-geo-alt'},
        {'id': 2, 'nome': 'Nacional', 'cor': '#ffc107', 'icone': 'bi-airplane'},
        {'id': 3, 'nome': 'Internacional', 'cor': '#dc3545', 'icone': 'bi-globe'},
    ]
    tipos_solicitacao = [
        {'id': 1, 'nome': 'Apenas Diárias', 'cor': '#198754', 'icone': 'bi-cash-stack'},
        {'id': 2, 'nome': 'Diárias + Passagens', 'cor': '#6f42c1', 'icone': 'bi-ticket-perforated'},
        {'id': 3, 'nome': 'Apenas Passagens', 'cor': '#0d6efd', 'icone': 'bi-airplane-engines'},
    ]

    return render_template(
        'diarias/aprovar_solicitacoes.html',
        itinerarios=itinerarios,
        pagination=pagination,
        pessoas_count=pessoas_count,
        tipos_itinerario=tipos_itinerario,
        tipo_counts=tipo_counts,
        filtro_tipos=filtro_tipos,
        tipos_solicitacao=tipos_solicitacao,
        sol_counts=sol_counts,
        filtro_solicitacao=filtro_solicitacao,
        eh_secretario=eh_secretario,
        eh_super_sga=eh_super_sga,
    )


@diarias_bp.route('/api/verificar-processo-sei')
@login_required
def api_verificar_processo_sei():
    """
    AJAX — Verifica se um processo SEI existe.

    Query param: protocolo (string)

    Returns:
        JSON {sucesso, protocolo_formatado, id_procedimento,
              link_acesso, especificacao, erro}
    """
    protocolo = request.args.get('protocolo', '').strip()
    resultado = verificar_protocolo_sei(protocolo)
    return jsonify(resultado)


@diarias_bp.route('/administracao/<int:id>/vincular-processo', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def vincular_processo(id):
    """
    POST — Vincula um processo SEI existente à solicitação de diárias.

    Form params:
        protocolo_sei (str) — número do processo SEI
        etapa_id      (int) — etapa para a qual o itinerário avança

    Redireciona para /diarias/administracao/<id> com flash de resultado.
    """
    protocolo_sei = request.form.get('protocolo_sei', '').strip()
    etapa_id_raw = request.form.get('etapa_id', '').strip()

    if not protocolo_sei:
        flash('Informe o número do processo SEI.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    if not etapa_id_raw:
        flash('Selecione a etapa para vinculação.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    try:
        etapa_id = int(etapa_id_raw)
    except (ValueError, TypeError):
        flash('Etapa inválida.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    resultado = vincular_processo_sei(
        itinerario_id=id,
        protocolo_sei=protocolo_sei,
        etapa_id=etapa_id,
        usuario_id=current_user.id,
    )

    if resultado['sucesso']:
        db.session.commit()
        flash('Processo SEI vinculado com sucesso.', 'success')
        for msg in resultado.get('msgs', []):
            current_app.logger.info(f'[DIARIAS] Vincular processo {id}: {msg}')
    else:
        db.session.rollback()
        flash(f'Erro ao vincular processo: {resultado.get("erro", "Erro desconhecido.")}', 'danger')

    return redirect(url_for('diarias.administracao_detalhe', id=id))


@diarias_bp.route('/administracao/<int:id>')
@login_required
def administracao_detalhe(id):
    """
    Detalhe administrativo de uma solicitação: timeline completa, info geral,
    upload de cotação ao SEI e visualização da NR do financeiro.

    Acesso:
      - Usuários com permissão 'diarias.aprovar' (admin/financeiro)
      - Superintendentes (quando a solicitação pertencer à sua superintendência)
    """
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        abort(404)

    itinerario = dados['itinerario']

    # Guard de acesso
    pode_aprovar = current_user.tem_permissao('diarias', 'aprovar')
    eh_super_do_setor = False
    from app.utils.unidade_sei import SUPER_SGA_SIGLA
    if current_user.is_superintendente and current_user.superintendencia_sigla:
        # Super da SGA vê o detalhe de qualquer solicitação
        if current_user.superintendencia_sigla == SUPER_SGA_SIGLA:
            eh_super_do_setor = True
        else:
            from app.models.usuario import Usuario
            solicitante = Usuario.query.filter_by(sigla_login=itinerario.usuario_gerador).first()
            if solicitante and solicitante.superintendencia_sigla == current_user.superintendencia_sigla:
                eh_super_do_setor = True

    # Secretário tem acesso a qualquer solicitação
    eh_secretario = current_user.is_secretario

    if not (pode_aprovar or eh_super_do_setor or eh_secretario):
        flash('Acesso restrito.', 'warning')
        return redirect(url_for('diarias.dashboard'))
    timeline_data = DiariaService.obter_timeline(itinerario)
    aba = request.args.get('aba', 'resumo')

    # Hierarquia de autorização (Etapa 1) — 1 query + 1 iteração
    nivel_autorizacao = None
    super_dispensado = False
    if itinerario.etapa_atual_id == 1:
        from app.services.diarias_autorizacao import get_estado_etapa1
        _estado_e1 = get_estado_etapa1(itinerario)
        nivel_autorizacao = _estado_e1['nivel_autorizacao']
        super_dispensado = _estado_e1['super_dispensado']

    # Contexto extra para modais de edição admin
    from app.models.diaria import (
        DiariasCargo, DiariasNatureza, DiariasEtapa,
        Estado, DiariasTipoSolicitacao, DiariasTipoItinerario,
    )

    return render_template(
        'diarias/administracao_detalhe.html',
        itinerario=itinerario,
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        cotacoes_voos=dados.get('cotacoes_voos', []),
        timeline_data=timeline_data,
        agencias=DiariaService.get_agencias(),
        aba=aba,
        # Dados para modais de edição
        cargos=DiariasCargo.query.order_by(DiariasCargo.nome).all(),
        naturezas=DiariasNatureza.query.all(),
        etapas_lista=DiariasEtapa.query.order_by(DiariasEtapa.ordem).all(),
        estados=Estado.query.order_by(Estado.nome).all(),
        tipos_solicitacao=DiariasTipoSolicitacao.query.all(),
        tipos_itinerario_list=DiariasTipoItinerario.query.all(),
        nivel_autorizacao=nivel_autorizacao,
        super_dispensado=super_dispensado,
    )


@diarias_bp.route('/administracao/<int:id>/upload-cotacao', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def upload_cotacao(id):
    """
    Faz upload de um documento de cotação de passagem ao processo SEI
    usando a série "Cotação" (IdSerie 272).
    """
    from app.services.diarias_sei_integration import (
        gerar_token_sei_admin, adicionar_documento_externo, ID_SERIE_COTACAO,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_protocolo:
        flash('Esta solicitação não possui processo SEI para enviar o documento.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    arquivo = request.files.get('arquivo_cotacao')
    descricao_cotacao = request.form.get('descricao_cotacao', '').strip()

    if not arquivo or not arquivo.filename:
        flash('Selecione um arquivo para enviar.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    # Validação de extensão
    EXTENSOES_PERMITIDAS = {'.pdf', '.png', '.jpg', '.jpeg', '.doc', '.docx', '.xls', '.xlsx'}
    ext = '.' + arquivo.filename.rsplit('.', 1)[-1].lower() if '.' in arquivo.filename else ''
    if ext not in EXTENSOES_PERMITIDAS:
        flash(f'Extensão "{ext}" não permitida. Aceitas: {", ".join(sorted(EXTENSOES_PERMITIDAS))}', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    try:
        arquivo_bytes = arquivo.read()
        if len(arquivo_bytes) == 0:
            flash('O arquivo está vazio.', 'danger')
            return redirect(url_for('diarias.administracao_detalhe', id=id))

        token = gerar_token_sei_admin()
        if not token:
            flash('Falha na autenticação com o SEI.', 'danger')
            return redirect(url_for('diarias.administracao_detalhe', id=id))

        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=itinerario.sei_protocolo,
            arquivo_bytes=arquivo_bytes,
            nome_arquivo=arquivo.filename,
            descricao=descricao_cotacao or 'Cotação de passagens',
            id_serie=ID_SERIE_COTACAO,
            numero=descricao_cotacao or None,
        )

        if retorno:
            doc_fmt = retorno.get('DocumentoFormatado', '')
            flash(
                f'Cotação enviada ao SEI com sucesso! Documento: {doc_fmt}',
                'success',
            )
        else:
            flash('Erro ao enviar cotação ao SEI. Tente novamente.', 'danger')

    except Exception as e:
        flash(f'Erro ao processar upload: {str(e)}', 'danger')

    return redirect(url_for('diarias.administracao_detalhe', id=id))


# ── Gerar Documento de Cotações (interno) ──────────────────────────────────

@diarias_bp.route('/administracao/<int:id>/gerar-cotacoes-sei', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def gerar_cotacoes_sei(id):
    """
    Gera documento interno de cotações (série 272) no processo SEI
    com tabela formatada de todos os DiariasCotacaoVoo do itinerário.
    """
    from app.services.diarias_sei_integration import gerar_documento_cotacoes
    from app.models.diaria import DiariasCotacaoVoo

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_protocolo:
        flash('Esta solicitação não possui processo SEI.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    if not itinerario.sei_id_procedimento:
        flash('Processo SEI sem ID de procedimento.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    cotacoes = DiariasCotacaoVoo.query.filter_by(itinerario_id=id).all()
    if not cotacoes:
        flash('Não há cotações cadastradas para gerar o documento.', 'warning')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    try:
        token = gerar_token_sei_admin()
        if not token:
            flash('Falha na autenticação com o SEI.', 'danger')
            return redirect(url_for('diarias.administracao_detalhe', id=id))

        retorno = gerar_documento_cotacoes(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=itinerario.sei_protocolo,
            cotacoes_voos=cotacoes,
        )

        if retorno:
            doc_fmt = retorno.get('DocumentoFormatado', '')
            itinerario.set_doc(
                'memorando_cotacoes',
                sei_id=retorno.get('IdDocumento', ''),
                sei_formatado=doc_fmt,
            )
            db.session.commit()
            flash(
                f'Documento de cotações gerado no SEI com sucesso! Documento: {doc_fmt}',
                'success',
            )
        else:
            flash('Erro ao gerar documento de cotações no SEI. Tente novamente.', 'danger')

    except Exception as e:
        current_app.logger.error(f"[DIARIAS] Erro ao gerar cotações SEI: {e}")
        flash(f'Erro ao gerar documento: {str(e)}', 'danger')

    return redirect(url_for('diarias.administracao_detalhe', id=id))


# ── Escolha de Passagens ────────────────────────────────────────────────────

@diarias_bp.route('/administracao/<int:id>/escolha-passagens', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def escolha_passagens(id):
    """Salva escolha de passagens (IDA + VOLTA) e gera documento SEI."""
    from app.models.diaria import DiariasCotacaoVoo
    from app.services.diarias_sei_integration import (
        gerar_token_sei_admin, gerar_escolha_passagens,
        gerar_memorando_cotacoes, consultar_documentos_procedimento,
        ID_SERIE_COTACAO,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: só para Nacional/Internacional com passagens
    if itinerario.tipo_itinerario not in [2, 3]:
        flash('Escolha de passagens só se aplica a viagens nacionais/internacionais.', 'warning')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    # Guard: não pode resubmeter
    if itinerario.escolha_voo_ida_id:
        flash('A escolha de passagens já foi realizada para esta solicitação.', 'warning')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    # Parse form
    voo_ida_id = request.form.get('escolha_voo_ida', type=int)
    voo_volta_id = request.form.get('escolha_voo_volta', type=int)

    if not voo_ida_id or not voo_volta_id:
        flash('Selecione um voo de IDA e um voo de VOLTA.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    # Valida que os IDs pertencem a este itinerário
    voo_ida = DiariasCotacaoVoo.query.get(voo_ida_id)
    voo_volta = DiariasCotacaoVoo.query.get(voo_volta_id)

    if not voo_ida or voo_ida.itinerario_id != id or voo_ida.tipo_trecho != 'ida':
        flash('Voo de IDA inválido.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    if not voo_volta or voo_volta.itinerario_id != id or voo_volta.tipo_trecho != 'volta':
        flash('Voo de VOLTA inválido.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    # Detecta se o mais barato foi selecionado (server-side)
    all_ida = DiariasCotacaoVoo.query.filter_by(
        itinerario_id=id, tipo_trecho='ida'
    ).order_by(DiariasCotacaoVoo.valor.asc()).all()

    all_volta = DiariasCotacaoVoo.query.filter_by(
        itinerario_id=id, tipo_trecho='volta'
    ).order_by(DiariasCotacaoVoo.valor.asc()).all()

    menor_ida = all_ida[0].valor if all_ida else None
    menor_volta = all_volta[0].valor if all_volta else None

    is_cheapest = (voo_ida.valor <= menor_ida and voo_volta.valor <= menor_volta)

    # Parse justificativa (só se NÃO é o mais barato)
    justificativa_codigos = []
    justificativa_outros = None

    if not is_cheapest:
        for code in ['J1', 'J2', 'J3', 'J4', 'J5']:
            if request.form.get(f'justificativa_{code}'):
                justificativa_codigos.append(code)
        justificativa_outros = request.form.get('justificativa_outros_texto', '').strip() or None

    declaracao = bool(request.form.get('declaracao_responsabilidade'))

    # Salva no banco
    itinerario.escolha_voo_ida_id = voo_ida_id
    itinerario.escolha_voo_volta_id = voo_volta_id
    itinerario.escolha_menor_valor = is_cheapest
    itinerario.escolha_justificativa_codigos = ','.join(justificativa_codigos) if justificativa_codigos else None
    itinerario.escolha_justificativa_outros = justificativa_outros
    itinerario.escolha_declaracao_responsabilidade = declaracao

    # Gera documentos SEI: SEAD_ESCOLHA_PASSAGENS + 2º SEAD_MEMORANDO_SGA
    sei_ok = False
    if itinerario.sei_id_procedimento:
        try:
            token = gerar_token_sei_admin()
            if token:
                sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

                # 1) SEAD_ESCOLHA_PASSAGENS
                retorno = gerar_escolha_passagens(
                    token=token,
                    id_procedimento=itinerario.sei_id_procedimento,
                    dados_escolha={
                        'voos_ida': all_ida,
                        'voos_volta': all_volta,
                        'escolha_ida_id': voo_ida_id,
                        'escolha_volta_id': voo_volta_id,
                        'menor_valor': is_cheapest,
                        'justificativa_codigos': justificativa_codigos,
                        'justificativa_outros_texto': justificativa_outros,
                        'declaracao': declaracao,
                    },
                    sei_protocolo=sei_protocolo,
                )
                if retorno:
                    itinerario.set_doc('escolha_passagens',
                        sei_id=str(retorno.get('IdDocumento', '')),
                        sei_formatado=retorno.get('DocumentoFormatado', ''))
                    sei_ok = True

                    # 2) 2º SEAD_MEMORANDO_SGA — Encaminhamento de Cotações
                    # Busca IDs das cotações (IdSerie 272) no processo SEI
                    ref_cotacoes = ''
                    if sei_protocolo:
                        resp_docs = consultar_documentos_procedimento(sei_protocolo)
                        if resp_docs.get('sucesso'):
                            ids_cotacao = [
                                d.get('DocumentoFormatado', '')
                                for d in resp_docs['documentos']
                                if str(d.get('Serie', {}).get('IdSerie', '')) == ID_SERIE_COTACAO
                            ]
                            ref_cotacoes = ', '.join(ids_cotacao) if ids_cotacao else ''

                    doc_req_pass = itinerario.get_doc('requisicao_passagens')
                    ref_req_passagens = (doc_req_pass.sei_formatado if doc_req_pass else '') or ''

                    if ref_cotacoes and ref_req_passagens:
                        ret_memo = gerar_memorando_cotacoes(
                            token=token,
                            id_procedimento=itinerario.sei_id_procedimento,
                            sei_protocolo=sei_protocolo,
                            ref_cotacoes_fmt=ref_cotacoes,
                            ref_requisicao_passagens_fmt=ref_req_passagens,
                        )
                        if ret_memo:
                            itinerario.set_doc('memorando_cotacoes',
                                sei_id=str(ret_memo.get('IdDocumento', '')),
                                sei_formatado=ret_memo.get('DocumentoFormatado', ''))
                        else:
                            current_app.logger.warning("SEI: Escolha OK mas memorando cotações falhou.")
                    else:
                        current_app.logger.warning(
                            f"SEI: Sem ref cotações ({ref_cotacoes!r}) ou req passagens ({ref_req_passagens!r}), "
                            "memorando cotações não gerado."
                        )
                else:
                    flash('Aviso: Escolha salva, mas geração do documento SEI falhou.', 'warning')
            else:
                flash('Aviso: Escolha salva, mas não foi possível autenticar no SEI.', 'warning')
        except Exception as e:
            flash(f'Aviso: Escolha salva, mas erro na integração SEI: {e}', 'warning')

    # Escolha de passagens concluída → avança para Análise 2ª Parte (etapa 6)
    if itinerario.etapa_atual_id == DiariasEtapaID.ESCOLHA_VOO:
        DiariaService.registrar_movimentacao(
            id_itinerario=id,
            etapa_nova_id=DiariasEtapaID.ANALISE_SOLICITACAO_2,
            usuario_id=current_user.id if current_user else None,
            comentario='Escolha de passagens registrada. Avançando para Análise 2ª Parte.',
            auto_commit=False,
        )
    db.session.commit()

    if sei_ok:
        flash('Escolha de passagens registrada e documento gerado no SEI com sucesso!', 'success')
    elif not itinerario.sei_id_procedimento:
        flash('Escolha de passagens registrada com sucesso!', 'success')

    return redirect(url_for('diarias.administracao_detalhe', id=id))


# ── Assinatura do Superintendente nas Requisições ────────────────────────

@diarias_bp.route('/administracao/<int:id>/assinar-superintendente', methods=['POST'])
@login_required
def assinar_superintendente(id):
    """
    Superintendente assina os documentos de Requisição de Diárias
    (e Requisição de Passagens, se aplicável) no SEI.
    Deve ocorrer ANTES da autorização do Secretário.
    """
    from app.services.sei_auth import autenticar_usuario_sei
    from app.services.sei_integration import assinar_documento

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: somente o superintendente pode assinar
    if not current_user.is_superintendente:
        return jsonify({
            'sucesso': False,
            'erro': 'Apenas o Superintendente pode assinar as requisições.'
        }), 403

    # Guard: só permite na etapa 1 (Solicitação Iniciada)
    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIAL:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação não está na etapa de autorização (etapa 1).'
        }), 400

    if itinerario.processo_negado:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação foi negada e não pode mais ser assinada.'
        }), 400

    # Guard: precisa de processo SEI e requisição gerada
    if not itinerario.sei_id_procedimento or not itinerario.has_doc('requisicao'):
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação não possui processo SEI ou requisição de diárias vinculada.'
        }), 400

    # Guard: já assinou
    if itinerario.superintendente_assinou:
        return jsonify({
            'sucesso': False,
            'erro': 'O Superintendente já assinou as requisições desta solicitação.'
        }), 400

    # Dispensa de assinatura quando o próprio superintendente é integrante da viagem.
    # Nesse caso, não exige credenciais SEI — marca automaticamente.
    from app.services.diarias_autorizacao import superintendente_dispensado
    if superintendente_dispensado(itinerario):
        itinerario.superintendente_assinou = True
        itinerario.superintendente_assinou_data = datetime.now()
        itinerario.superintendente_assinou_nome = current_user.nome or 'Superintendente (dispensado)'
        db.session.commit()
        current_app.logger.info(
            f"[DIARIAS] Superintendente dispensado (é integrante) — itinerario={itinerario.id}"
        )
        return jsonify({
            'sucesso': True,
            'dispensado': True,
            'mensagem': 'Superintendente é integrante da solicitação — assinatura dispensada automaticamente.',
        })

    # Parse JSON
    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não informados.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip()

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Usuário e senha do SEI são obrigatórios.'}), 400

    # Protocolo do processo para suporte a bypass por protocolo específico
    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo

    # 1. Autentica superintendente no SEI
    auth_result = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
    if not auth_result:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha na autenticação no SEI. Verifique usuário e senha.'
        }), 401

    token_super = auth_result['token']
    id_usuario = auth_result['id_usuario']
    id_login = auth_result['id_login']
    # Sempre usa o cargo real do SEI (UltimoCargoAssinatura); formulário é apenas último recurso
    cargo = auth_result.get('cargo') or cargo or 'Superintendente de Gestão Administrativa'

    # Usa a unidade onde o documento foi criado (APOIOSGA) para assinatura.
    unidade_assinatura = UNIDADE_APOIOSGA

    current_app.logger.info(
        f"[DIARIAS] Assinatura superintendente - usuario={sei_usuario} "
        f"id_usuario={id_usuario!r} id_login={id_login!r} "
        f"unidade_assinatura={unidade_assinatura!r} "
        f"protocolo_doc={(itinerario.get_doc('requisicao').sei_formatado if itinerario.has_doc('requisicao') else '')!r}"
    )

    documentos_assinados = []

    # 2. Assinar Requisição de Diárias
    doc_requisicao = itinerario.get_doc('requisicao')
    dados_assinatura = {
        'protocolo_doc': doc_requisicao.sei_id if doc_requisicao else '',
        'orgao': 'SEAD-PI',
        'cargo': cargo,
        'id_login': id_login,
        'id_usuario': id_usuario,
        'senha': sei_senha,
    }

    ret = assinar_documento(
        token=token_super,
        unidade_id=unidade_assinatura,
        dados_assinatura=dados_assinatura,
        protocolo_proc=protocolo_proc,
    )

    if not ret or not ret.get('sucesso'):
        erro = ret.get('erro', 'Erro desconhecido') if ret else 'Sem resposta'
        return jsonify({
            'sucesso': False,
            'erro': f'Falha ao assinar Requisição de Diárias: {erro}'
        }), 500

    documentos_assinados.append((doc_requisicao.sei_formatado if doc_requisicao else '') or 'Req. Diárias')

    # Superintendente assina APENAS a Requisição de Diárias (532).
    # A Requisição de Passagens (2975) não requer assinatura do superintendente.

    # 4. Salvar no banco — busca timestamp real do SEI ao invés de datetime.now()
    from app.services.diarias_autorizacao import verificar_assinatura_superintendente_sei
    data_assinatura = None
    nome_assinante = current_user.nome or 'Superintendente'
    try:
        check = verificar_assinatura_superintendente_sei(itinerario)
        if check.get('assinada'):
            if check.get('data_hora_assinatura'):
                data_assinatura = check['data_hora_assinatura']
            if check.get('assinante_nome'):
                nome_assinante = check['assinante_nome']
    except Exception as exc:
        current_app.logger.warning(
            f'[DIARIAS] assinar_superintendente: falha ao obter timestamp SEI '
            f'itinerario={itinerario.id}: {exc}'
        )

    itinerario.superintendente_assinou = True
    itinerario.superintendente_assinou_data = data_assinatura or datetime.now()
    itinerario.superintendente_assinou_nome = nome_assinante
    db.session.commit()

    # 5. Notificar que Superintendente assinou
    try:
        DiariasNotifier.notificar_etapa(itinerario, 'assinatura_superintendente', current_user.id)
    except Exception as exc_notif:
        current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')

    return jsonify({
        'sucesso': True,
        'documentos_assinados': documentos_assinados,
        'mensagem': f'Requisições assinadas com sucesso: {", ".join(documentos_assinados)}',
    })


# ── Negar solicitação (Superintendente) ──────────────────────────────────

@diarias_bp.route('/administracao/<int:id>/negar', methods=['POST'])
@login_required
def negar_solicitacao(id):
    """
    Superintendente nega uma solicitação de diárias na etapa 1.

    Fluxo:
      1. Autentica no SEI
      2. Cria despacho SEAD_DESPACHO_SGA (IdSerie 2987) com justificativa
      3. Assina o despacho
      4. Encaminha processo SEI para a unidade solicitante
      5. Marca processo_negado=True no banco
      6. Registra movimentação/timeline
    """
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not current_user.is_superintendente:
        return jsonify({
            'sucesso': False,
            'erro': 'Apenas o Superintendente pode negar solicitações.'
        }), 403

    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIAL:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação não está na etapa de autorização (etapa 1).'
        }), 400

    if not itinerario.sei_id_procedimento:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação não possui processo SEI vinculado.'
        }), 400

    if not itinerario.unidade_geradora_id or not itinerario.unidade_geradora_descricao:
        return jsonify({
            'sucesso': False,
            'erro': 'Descrição da unidade solicitante não encontrada nesta solicitação.'
        }), 400

    if itinerario.processo_negado:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação já foi negada.'
        }), 400

    if itinerario.superintendente_assinou:
        return jsonify({
            'sucesso': False,
            'erro': 'Não é possível negar uma solicitação já assinada pelo Superintendente.'
        }), 400

    if itinerario.secretario_assinou:
        return jsonify({
            'sucesso': False,
            'erro': 'Não é possível negar uma solicitação já autorizada pelo Secretário.'
        }), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não informados.'}), 400

    justificativa = (dados.get('justificativa') or '').strip()
    sei_usuario = (dados.get('sei_usuario') or '').strip()
    sei_senha = (dados.get('sei_senha') or '').strip()

    if not justificativa:
        return jsonify({'sucesso': False, 'erro': 'Justificativa é obrigatória.'}), 400

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Usuário e senha do SEI são obrigatórios.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo

    # 1. Autentica no SEI
    auth_result = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
    if not auth_result:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha na autenticação no SEI. Verifique usuário e senha.'
        }), 401

    token_super = auth_result['token']
    id_usuario = auth_result['id_usuario']
    id_login = auth_result['id_login']
    cargo = auth_result.get('cargo') or 'Superintendente de Gestão Administrativa'

    # 2. Criar despacho de negação no SEI
    doc_despacho = gerar_despacho_sga_negacao(
        token=token_super,
        id_procedimento=itinerario.sei_id_procedimento,
        sei_protocolo=protocolo_proc,
        justificativa=justificativa,
        unidade_geradora_descricao=itinerario.unidade_geradora_descricao,
    )

    if not doc_despacho:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha ao criar despacho de negação no SEI.'
        }), 500

    id_documento = doc_despacho.get('IdDocumento', '')
    doc_formatado = doc_despacho.get('DocumentoFormatado', '')

    # 3. Assinar o despacho
    dados_assinatura = {
        'protocolo_doc': id_documento,
        'orgao': 'SEAD-PI',
        'cargo': cargo,
        'id_login': id_login,
        'id_usuario': id_usuario,
        'senha': sei_senha,
    }
    ret_assinatura = assinar_documento(
        token=token_super,
        unidade_id=UNIDADE_APOIOSGA,
        dados_assinatura=dados_assinatura,
        protocolo_proc=protocolo_proc,
    )
    if not ret_assinatura or not ret_assinatura.get('sucesso'):
        erro = ret_assinatura.get('erro', 'Erro desconhecido') if ret_assinatura else 'Sem resposta'
        return jsonify({
            'sucesso': False,
            'erro': f'Falha ao assinar despacho de negação: {erro}'
        }), 500

    # 4. Encaminhar processo para unidade solicitante antes de marcar no banco
    try:
        envio = enviar_procedimento(
            token=token_super,
            protocolo_procedimento=protocolo_proc,
            unidades_destino=[itinerario.unidade_geradora_id],
            manter_aberto=True,
            unidade_origem=UNIDADE_APOIOSGA,
        )
    except Exception as exc_envio:
        current_app.logger.warning(
            f'[DIARIAS] negar_solicitacao: falha ao encaminhar processo '
            f'para {itinerario.unidade_geradora_id}: {exc_envio}'
        )
        return jsonify({
            'sucesso': False,
            'erro': 'Despacho assinado, mas houve falha ao encaminhar o processo para a unidade solicitante.'
        }), 500

    if not envio or not envio.get('sucesso'):
        erro_envio = envio.get('erro', 'Erro desconhecido') if envio else 'Sem resposta'
        current_app.logger.warning(
            f'[DIARIAS] negar_solicitacao: SEI recusou encaminhamento '
            f'para {itinerario.unidade_geradora_id}: {erro_envio}'
        )
        return jsonify({
            'sucesso': False,
            'erro': f'Despacho assinado, mas houve falha ao encaminhar o processo: {erro_envio}'
        }), 500

    # 5. Marcar como negado no banco (commit único ao final)
    itinerario.processo_negado = True
    itinerario.processo_negado_data = datetime.now()
    itinerario.processo_negado_por_id = current_user.id
    itinerario.processo_negado_por_nome = current_user.nome or 'Superintendente'
    itinerario.processo_negado_justificativa = justificativa
    itinerario.processo_negado_doc_sei_id = id_documento
    itinerario.processo_negado_doc_sei_formatado = doc_formatado

    # 6. Registrar movimentação
    DiariaService.registrar_movimentacao(
        itinerario.id,
        DiariasEtapaID.SOLICITACAO_INICIAL,
        current_user.id,
        f'Solicitação negada pelo Superintendente. Justificativa: {justificativa[:200]}',
        auto_commit=False,
    )

    db.session.commit()

    current_app.logger.info(
        f'[DIARIAS] Solicitação {itinerario.id} negada por {current_user.nome} '
        f'(doc SEI: {doc_formatado})'
    )

    return jsonify({
        'sucesso': True,
        'mensagem': f'Solicitação negada com sucesso. Despacho SEI: {doc_formatado}',
        'doc_sei_formatado': doc_formatado,
    })


# ── Verificação automática de assinaturas (sem credenciais) ──────────────

@diarias_bp.route('/administracao/<int:id>/verificar-assinaturas-super', methods=['POST'])
@login_required
def verificar_assinaturas_super(id):
    """
    Verifica se Requisição de Diárias (e Passagens, se aplicável) já estão
    assinadas no banco local. Se sim, marca superintendente_assinou = True
    sem exigir credenciais SEI — útil quando as assinaturas foram feitas
    diretamente no SEI fora do sistema.
    """
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not (current_user.is_superintendente or current_user.is_admin):
        return jsonify({'sucesso': False, 'erro': 'Acesso restrito.'}), 403

    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIAL:
        return jsonify({'sucesso': False, 'erro': 'Solicitação não está na etapa de autorização.'}), 400

    if itinerario.processo_negado:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação foi negada e não pode ter assinatura sincronizada.'
        }), 400

    if itinerario.superintendente_assinou:
        return jsonify({'sucesso': True, 'mensagem': 'Superintendente já havia assinado.', 'ja_assinado': True})

    # Superintendente assina APENAS a Requisição de Diárias (532).
    # Verificação dinâmica: consulta o SEI e casa o assinante pela Sigla/CPF
    # contra o usuário cadastrado com cargo_gestao='superintendente'. Se o
    # admin trocar quem é o Superintendente no módulo de usuários, o sistema
    # passa a esperar pela assinatura do novo automaticamente.
    from app.services.diarias_autorizacao import verificar_assinatura_superintendente_sei
    resultado = verificar_assinatura_superintendente_sei(itinerario)

    if not resultado.get('assinada'):
        # Atualiza referência local do doc se o SEI já o expôs (mesmo sem assinatura)
        if resultado.get('doc_sei_id'):
            itinerario.set_doc(
                'requisicao',
                sei_id=resultado['doc_sei_id'],
                sei_formatado=resultado.get('doc_sei_formatado'),
            )
            db.session.commit()

        msg = resultado.get('erro') or (
            'Requisição de Diárias localizada no SEI, mas ainda não foi '
            'assinada pelo Superintendente cadastrado no sistema.'
        )
        return jsonify({
            'sucesso': False,
            'pendente': True,
            'mensagem': msg,
        })

    # Assinatura encontrada — atualiza flag local do doc + libera passo do Secretário
    doc_req = itinerario.get_doc('requisicao')
    if doc_req:
        doc_req.assinado = True
    else:
        itinerario.set_doc(
            'requisicao',
            sei_id=resultado.get('doc_sei_id'),
            sei_formatado=resultado.get('doc_sei_formatado'),
            assinado=True,
        )

    itinerario.superintendente_assinou = True
    sei_ts = resultado.get('data_hora_assinatura')
    if not sei_ts:
        current_app.logger.warning(
            f'[DIARIAS] verificar_assinaturas_super: data_hora_assinatura is None '
            f'para itinerario={id}, usando datetime.now()'
        )
    itinerario.superintendente_assinou_data = sei_ts or datetime.now()
    itinerario.superintendente_assinou_nome = resultado.get('assinante_nome')
    db.session.commit()

    assinante_nome = resultado.get('assinante_nome') or 'Superintendente'
    current_app.logger.info(
        f'[DIARIAS] verificar_assinaturas_super: superintendente_assinou=True '
        f'via verificacao SEI (itinerario={id}, assinante={assinante_nome!r})'
    )

    try:
        DiariasNotifier.notificar_etapa(itinerario, 'assinatura_superintendente', current_user.id)
    except Exception as exc_notif:
        current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')

    return jsonify({
        'sucesso': True,
        'mensagem': f'Requisição de Diárias assinada por {assinante_nome}. Etapa liberada para autorização do Secretário.',
        'assinante': assinante_nome,
    })


# ── Autorização do Secretário ─────────────────────────────────────────────

@diarias_bp.route('/administracao/<int:id>/autorizar', methods=['POST'])
@login_required
def autorizar_solicitacao(id):
    """
    Gera autorização do secretário no SEI e avança a etapa automaticamente.

    Fluxo:
    1. Autentica secretário com credenciais SEI informadas
    2. Cria documento SEAD_AUTORIZACAO_DO_SECRETARIO no processo SEI
    3. Assina o documento com as credenciais do secretário
    4. Avança etapa para FINANCEIRO
    5. Encaminha processo para DFIN/APOIO
    6. Gera despacho DFIN
    """
    from flask import jsonify
    from app.services.sei_auth import autenticar_usuario_sei
    from app.services.diarias_sei_integration import (
        gerar_token_sei_admin, gerar_autorizacao_secretario,
        enviar_procedimento, gerar_despacho_dfin,
        UNIDADE_DFIN_APOIO, consultar_documentos_procedimento,
        ID_SERIE_REQUISICAO_DIARIAS,
    )
    from app.services.sei_integration import assinar_documento

    def _assinante_ja_no_doc(docs_sei, id_serie, sigla):
        """Retorna True se sigla (login SEI) já consta nas assinaturas do doc da série indicada."""
        for doc in docs_sei:
            if str(doc.get('Serie', {}).get('IdSerie', '')) == str(id_serie):
                for ass in (doc.get('Assinaturas') or []):
                    if ass.get('Sigla', '').lower() == sigla.lower():
                        return True
        return False

    itinerario = DiariasItinerario.query.get_or_404(id)

    if itinerario.processo_negado:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação foi negada e não pode mais ser autorizada.'
        }), 400

    # Parse JSON antes dos guards: override_nivel pode alterar a checagem de hierarquia
    dados = request.get_json() or {}
    override_nivel = dados.get('override_nivel')
    motivo_override = (dados.get('motivo_override') or '').strip()

    # Guard: hierarquia de autorização (3 níveis)
    from app.services.diarias_autorizacao import get_nivel_autorizacao
    nivel_info = get_nivel_autorizacao(itinerario)
    nivel_computado = nivel_info['nivel']
    autorizador_ids = {u.id for u in nivel_info['autorizadores']}

    # Override Nível 3: Superintendente assume quando Sec. em Exercício
    # está indisponível (viagem, licença, etc.) e o nível computado seria 2.
    if override_nivel == 3:
        if (current_user.cargo_gestao or '').strip() != 'superintendente':
            return jsonify({
                'sucesso': False,
                'erro': 'Apenas usuário com cargo de Superintendente pode acionar o override Nível 3.'
            }), 403
        if nivel_computado != 2:
            return jsonify({
                'sucesso': False,
                'erro': (
                    f'Override Nível 3 só é válido quando o nível computado é 2 '
                    f'(atual: {nivel_computado}).'
                )
            }), 400
        if not motivo_override or len(motivo_override) < 10:
            return jsonify({
                'sucesso': False,
                'erro': 'Motivo da indisponibilidade do Secretário em Exercício é obrigatório (mín. 10 caracteres).'
            }), 400
        nivel_atual = 3
        current_app.logger.warning(
            f'[DIARIAS] OVERRIDE_N3 itinerario={itinerario.id} '
            f'super_id={current_user.id} motivo={motivo_override!r}'
        )
    elif current_user.id not in autorizador_ids:
        # Caminho normal: usuário precisa estar entre os autorizadores computados.
        nivel = nivel_info['nivel']
        motivo = nivel_info['motivo_escalada']
        msg = f'Autorização deve ser feita pelo Nível {nivel} da hierarquia.'
        if motivo:
            msg += f' Motivo: {motivo}.'
        return jsonify({'sucesso': False, 'erro': msg}), 403
    else:
        nivel_atual = nivel_computado

    # Guard: só permite na etapa 1 (Solicitação Iniciada)
    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIAL:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação não está na etapa de autorização (etapa 1).'
        }), 400

    # Guard: precisa de processo SEI
    if not itinerario.sei_id_procedimento:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação não possui processo SEI vinculado.'
        }), 400

    # Guard: Superintendente precisa ter assinado antes — exceto no Nível 3
    # (quando o próprio superintendente autoriza, a pré-assinatura é incorporada na ação única)
    if not itinerario.superintendente_assinou and nivel_atual != 3:
        return jsonify({
            'sucesso': False,
            'erro': 'O Superintendente precisa assinar as requisições antes da autorização do Secretário.'
        }), 400

    sei_usuario = (dados.get('sei_usuario') or '').strip()
    sei_senha = (dados.get('sei_senha') or '').strip()
    cargo = (dados.get('cargo') or '').strip()

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Usuário e senha do SEI são obrigatórios.'}), 400

    # Protocolo do processo para bypass por protocolo específico
    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo

    # 1. Autentica secretário no SEI
    auth_result = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
    if not auth_result:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha na autenticação no SEI. Verifique usuário e senha.'
        }), 401

    token_secretario = auth_result['token']
    id_usuario = auth_result['id_usuario']
    id_login = auth_result['id_login']
    # Sempre usa o cargo real do SEI (UltimoCargoAssinatura); formulário é apenas último recurso
    cargo_default = (
        'Superintendente de Gestão Administrativa'
        if nivel_atual == 3
        else 'Secretário de Administração do Estado do Piauí'
    )
    cargo = auth_result.get('cargo') or cargo or cargo_default

    # Unidade de assinatura: preferir unidade do usuario (sessao), fallback APOIOSGA
    from flask import session
    unidade_assinatura_sec = session.get('unidade_atual_id') or current_user.unidade_padrao_id or UNIDADE_APOIOSGA

    current_app.logger.info(
        f"[DIARIAS] Autorização nível {nivel_atual} - usuario={sei_usuario} "
        f"unidade_assinatura={unidade_assinatura_sec!r}"
    )

    # Nível 3 — ação única: superintendente assume ambos os papéis.
    # Marca a pré-assinatura e assina a Requisição de Diárias antes de autorizar.
    if nivel_atual == 3 and not itinerario.superintendente_assinou:
        doc_req_n3 = itinerario.get_doc('requisicao')
        if doc_req_n3 and doc_req_n3.sei_id:
            ret_n3 = assinar_documento(
                token=token_secretario,
                unidade_id=unidade_assinatura_sec,
                dados_assinatura={
                    'protocolo_doc': doc_req_n3.sei_id,
                    'orgao': 'SEAD-PI',
                    'cargo': cargo,
                    'id_login': id_login,
                    'id_usuario': id_usuario,
                    'senha': sei_senha,
                },
                protocolo_proc=protocolo_proc,
            )
            if ret_n3 and ret_n3.get('sucesso'):
                itinerario.superintendente_assinou = True
                # Busca timestamp real do SEI
                data_n3 = None
                nome_n3 = current_user.nome or 'Superintendente'
                try:
                    from app.services.diarias_autorizacao import verificar_assinatura_superintendente_sei
                    check_n3 = verificar_assinatura_superintendente_sei(itinerario)
                    if check_n3.get('assinada'):
                        if check_n3.get('data_hora_assinatura'):
                            data_n3 = check_n3['data_hora_assinatura']
                        if check_n3.get('assinante_nome'):
                            nome_n3 = check_n3['assinante_nome']
                except Exception as exc_n3_ts:
                    current_app.logger.warning(
                        f'[DIARIAS] Nível 3: falha ao obter timestamp SEI '
                        f'itinerario={itinerario.id}: {exc_n3_ts}'
                    )
                itinerario.superintendente_assinou_data = data_n3 or datetime.now()
                itinerario.superintendente_assinou_nome = nome_n3
                current_app.logger.info(
                    f"[DIARIAS] Nível 3 — superintendente assinou Req. Diárias como ação única "
                    f"itinerario={itinerario.id}"
                )
            else:
                erro_n3 = (ret_n3.get('erro', 'Erro desconhecido') if ret_n3 else 'Sem resposta')
                current_app.logger.warning(
                    f"[DIARIAS] Nível 3 — falha ao assinar Req. Diárias: {erro_n3}"
                )

    # Consulta documentos do processo no SEI para verificar assinaturas existentes
    _sei_docs = []
    try:
        _resp_docs = consultar_documentos_procedimento(protocolo_proc)
        if _resp_docs.get('sucesso'):
            _sei_docs = _resp_docs['documentos']
    except Exception as _e_docs:
        current_app.logger.warning(
            f"[DIARIAS] Falha ao listar docs SEI para verificação de assinatura: {_e_docs}"
        )

    # 1.5 Assina Requisição de Diárias apenas se o Secretário ainda não assinou
    doc_req_sec = itinerario.get_doc('requisicao')
    if doc_req_sec and doc_req_sec.sei_id:
        if _assinante_ja_no_doc(_sei_docs, ID_SERIE_REQUISICAO_DIARIAS, sei_usuario):
            current_app.logger.info(
                f"[DIARIAS] Requisição de Diárias já assinada pelo Secretário "
                f"({sei_usuario}) — pulando assinatura."
            )
        else:
            ret_req = assinar_documento(
                token=token_secretario,
                unidade_id=unidade_assinatura_sec,
                dados_assinatura={
                    'protocolo_doc': doc_req_sec.sei_id,
                    'orgao': 'SEAD-PI',
                    'cargo': cargo,
                    'id_login': id_login,
                    'id_usuario': id_usuario,
                    'senha': sei_senha,
                },
                protocolo_proc=protocolo_proc,
            )
            if not ret_req or not ret_req.get('sucesso'):
                erro = ret_req.get('erro', 'Erro desconhecido') if ret_req else 'Sem resposta'
                current_app.logger.warning(
                    f"[DIARIAS] Secretário falhou ao assinar Req. Diárias: {erro}"
                )

    # A Requisição de Passagens não requer assinatura do Secretário —
    # basta a assinatura de qualquer membro da solicitação para ser válida.

    # 2. Obtem token admin para operações SEI
    token_admin = gerar_token_sei_admin()
    if not token_admin:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha ao obter token administrativo do SEI.'
        }), 500

    sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''
    apenas_diarias = (itinerario.tipo_solicitacao_id == 1)

    if apenas_diarias:
        # ── Tipo 1 (Apenas Diárias): autorização via dupla assinatura na Requisição ──
        # Não gera doc 574 separado. A assinatura do Secretário na Requisição de
        # Diárias (532) + assinatura prévia do Superintendente = autorização completa.
        doc_req = itinerario.get_doc('requisicao')
        if not doc_req or not doc_req.sei_id:
            return jsonify({
                'sucesso': False,
                'erro': 'Requisição de Diárias não encontrada no processo SEI.'
            }), 400

        doc_formatado = doc_req.sei_formatado or doc_req.sei_id
        id_documento = doc_req.sei_id

        comentario_etapa = f'Requisição de Diárias ({doc_formatado}) assinada pelo Secretário — autorização concedida'
        if override_nivel == 3:
            comentario_etapa += f' [OVERRIDE Nível 3 — Sec. Exercício indisponível: {motivo_override}]'
    else:
        # ── Tipos 2,3 (com passagens): gera doc SEAD_AUTORIZAÇÃO_DO_SECRETÁRIO (574) ──
        retorno_doc = gerar_autorizacao_secretario(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            tipo_solicitacao_id=itinerario.tipo_solicitacao_id,
            sei_protocolo=sei_protocolo,
            nome_assinante=current_user.nome.upper() if current_user.nome else None,
            cargo_assinante=cargo,
        )

        if not retorno_doc:
            return jsonify({
                'sucesso': False,
                'erro': 'Falha ao criar documento de autorização no SEI.'
            }), 500

        doc_formatado = retorno_doc.get('DocumentoFormatado', '')
        id_documento = str(retorno_doc.get('IdDocumento', ''))

        # Assina o documento de autorização com credenciais do secretário
        dados_assinatura = {
            'protocolo_doc': id_documento,
            'orgao': 'SEAD-PI',
            'cargo': cargo,
            'id_login': id_login,
            'id_usuario': id_usuario,
            'senha': sei_senha,
        }

        ret_assinatura = assinar_documento(
            token=token_secretario,
            unidade_id=unidade_assinatura_sec,
            dados_assinatura=dados_assinatura,
            protocolo_proc=protocolo_proc,
        )

        if not ret_assinatura or not ret_assinatura.get('sucesso'):
            erro_assinatura = ret_assinatura.get('erro', 'Erro desconhecido') if ret_assinatura else 'Sem resposta'
            return jsonify({
                'sucesso': False,
                'erro': f'Documento criado, mas falha ao assinar: {erro_assinatura}'
            }), 500

        # Salva referência do doc 574 no banco
        itinerario.set_doc('autorizacao',
            sei_id=id_documento,
            sei_formatado=doc_formatado)

        comentario_etapa = f'Autorização do Secretário ({doc_formatado}) gerada e assinada pelo sistema'
        if override_nivel == 3:
            comentario_etapa += f' [OVERRIDE Nível 3 — Sec. Exercício indisponível: {motivo_override}]'

    # Registra assinatura do secretário
    itinerario.secretario_assinou = True
    itinerario.secretario_assinou_data = datetime.now()
    itinerario.secretario_assinou_nome = current_user.nome or 'Secretário'

    # Avança etapa: sempre vai para Análise da Solicitação (etapa 3)
    DiariaService.registrar_movimentacao(
        id_itinerario=id,
        etapa_nova_id=DiariasEtapaID.ANALISE_SOLICITACAO,
        usuario_id=current_user.id if current_user else None,
        comentario=comentario_etapa,
    )

    # Commit critico: persiste doc + etapa ANTES de operações SEI subsequentes
    db.session.commit()

    resultado = {
        'sucesso': True,
        'documento_formatado': doc_formatado,
        'id_documento': id_documento,
    }

    # 7. Encaminha processo para DFIN/APOIO (caixa do Diretor)
    #    Após o Diretor despachar, o processo será enviado à GPO.
    envio_ok = False
    try:
        envio = enviar_procedimento(
            token_admin,
            sei_protocolo,
            [UNIDADE_DFIN_APOIO],
            manter_aberto=True,
        )
        resultado['envio_procedimento'] = envio
        envio_ok = envio.get('sucesso', False)

        if envio_ok:
            current_app.logger.info(
                f"SEI Diárias: Processo {sei_protocolo} encaminhado ao DFIN/APOIO ({UNIDADE_DFIN_APOIO})."
            )
            resultado['envio_mensagem'] = 'Processo encaminhado para DFIN/APOIO com sucesso!'
        else:
            current_app.logger.warning(
                f"SEI Diárias: Falha ao encaminhar procedimento para GPO: {envio.get('erro')}"
            )
            resultado['envio_mensagem'] = f'Aviso: falha ao encaminhar para GPO: {envio.get("erro", "")}'
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao encaminhar procedimento: {e}")
        resultado['envio_mensagem'] = f'Aviso: erro ao encaminhar para GPO: {str(e)}'

    # 8. Notifica o Diretor DFIN que há nova solicitação de diária
    try:
        from app.services.diarias_sei_integration import _resolver_titular_por_cargo
        from app.models.usuario import Usuario
        diretor = Usuario.query.filter_by(cargo_gestao='diretor_dfin', ativo=True).first()
        if diretor:
            from app.services.notification_engine import NotificationEngine
            processo = itinerario.sei_protocolo or itinerario.n_processo or f'Diaria #{itinerario.id}'
            NotificationEngine.notificar(
                tipo_codigo='diarias.autorizacao_secretario',
                destinatarios=[diretor.id],
                titulo='Nova Solicitação de Diária para Despacho',
                mensagem=f'A solicitação {processo} foi autorizada pelo Secretário e encaminhada à GPO. Acesse a aba de Diárias para despachar.',
                ref_modulo='diarias',
                ref_id=str(itinerario.id),
                ref_url='/financeiro/diarias',
            )
    except Exception as e:
        current_app.logger.warning(f'[DIARIAS] Falha ao notificar Diretor DFIN: {e}')

    try:
        # MED-08: Evento correto para autorização do secretário
        DiariasNotifier.notificar_etapa(itinerario, 'autorizacao_secretario', current_user.id)
    except Exception as exc_notif:
        current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')

    return jsonify(resultado)


# ── Ciência Superintendente + Despacho SGA → NCI (Passo 2 pós-NE) ──────────


@diarias_bp.route('/administracao/<int:id>/ciencia-sga', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def ciencia_sga(id):
    """Ciência do Superintendente + Despacho SGA (idSerie 2987) → NCI.

    O Superintendente (usuário com acesso à caixa APOIOSGA) lê o despacho CCDP,
    marca ciência e gera o despacho SGA assinado, encaminhando ao NCI.
    """
    # Verifica acesso à caixa APOIOSGA
    if not usuario_tem_caixa(CAIXA_APOIOSGA):
        return jsonify({'sucesso': False, 'erro': 'Você não tem acesso à caixa APOIOSGA.'}), 403

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guards
    if not itinerario.has_doc('despacho_ccdp'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho CCDP ainda não foi gerado.'}), 400

    if itinerario.has_doc('despacho_sga'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho SGA já foi gerado.'}), 400

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Não há processo SEI vinculado.'}), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não fornecidos.'}), 400

    # Verifica ciência
    if not dados.get('ciencia'):
        return jsonify({'sucesso': False, 'erro': 'É necessário confirmar a ciência do despacho.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Superintendente'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI são obrigatórias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        # 1. Autenticar superintendente no SEI
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI.'}), 401

        # 2. Token admin
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        # 3. Gerar Despacho SGA (série 2987)
        doc_despacho_ccdp = itinerario.get_doc('despacho_ccdp')
        retorno_doc = gerar_despacho_sga(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
            ref_despacho_ccdp_id=doc_despacho_ccdp.sei_id if doc_despacho_ccdp else '',
            ref_despacho_ccdp_formatado=doc_despacho_ccdp.sei_formatado if doc_despacho_ccdp else '',
            nome_assinante=current_user.nome.upper() if current_user and current_user.nome else None,
            cargo_assinante=cargo or 'Superintendente de Gestão Administrativa – SEAD',
        )

        if not retorno_doc:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar Despacho SGA no SEI.'}), 500

        doc_id = str(retorno_doc.get('IdDocumento', ''))
        doc_formatado = retorno_doc.get('DocumentoFormatado', '')

        # 4. Assinar com credenciais do superintendente
        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_APOIOSGA,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )

        aviso = None
        if not resultado_assinatura or not resultado_assinatura.get('sucesso'):
            erro_txt = resultado_assinatura.get('erro', 'Erro desconhecido') if resultado_assinatura else 'Sem resposta'
            aviso = f'Documento gerado mas assinatura falhou: {erro_txt}'

        # 5. Enviar procedimento para NCI
        envio = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=itinerario.sei_protocolo,
            unidades_destino=[UNIDADE_NCI],
            unidade_origem=UNIDADE_APOIOSGA,
        )

        # 6. Salvar no banco
        itinerario.ciencia_superintendente = True
        itinerario.ciencia_superintendente_data = datetime.now()
        itinerario.set_doc('despacho_sga',
            sei_id=doc_id,
            sei_formatado=doc_formatado)

        # 7. Ação interna (Análise da Solicitação) — não avança etapa principal
        # Despacho SGA é sub-ação dentro da etapa "Análise da Solicitação"

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'ciencia_sga', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')

        resultado = {
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'envio_procedimento': envio,
        }
        if aviso:
            resultado['aviso'] = aviso
        return jsonify(resultado)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# ── Análise de Pagamento NCI + Despacho NCI (Passo 3 pós-NE) ──────────────


@diarias_bp.route('/administracao/<int:id>/analise-nci', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def analise_nci(id):
    """Análise de Pagamento (idSerie 461) + Despacho NCI (idSerie 5).

    O usuário do NCI preenche o formulário de 21 perguntas S/N,
    gera ambos os documentos já assinados no SEI.
    """
    # Verifica acesso à caixa NCI
    if not usuario_tem_caixa(CAIXA_NCI):
        return jsonify({'sucesso': False, 'erro': 'Você não tem acesso à caixa NCI.'}), 403

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guards
    if not itinerario.has_doc('despacho_sga'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho SGA ainda não foi gerado.'}), 400

    if itinerario.has_doc('analise_pagamento'):
        return jsonify({'sucesso': False, 'erro': 'A Análise de Pagamento já foi gerada.'}), 400

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Não há processo SEI vinculado.'}), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não fornecidos.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Assessora Técnica'
    respostas = dados.get('respostas', {})
    observacoes = dados.get('observacoes', '')

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI são obrigatórias.'}), 400

    if not respostas:
        return jsonify({'sucesso': False, 'erro': 'As respostas da análise são obrigatórias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        # 1. Autenticar usuário NCI no SEI
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI.'}), 401

        # 2. Token admin
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        # 3. Gerar Análise de Pagamento (série 461)
        retorno_analise = gerar_analise_pagamento(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            respostas=respostas,
            observacoes=observacoes,
        )

        if not retorno_analise:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar Análise de Pagamento no SEI.'}), 500

        analise_id = str(retorno_analise.get('IdDocumento', ''))
        analise_formatado = retorno_analise.get('DocumentoFormatado', '')

        # 4. Assinar a Análise com credenciais do NCI
        ret_assinatura_analise = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_NCI,
            dados_assinatura={
                'protocolo_doc': analise_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )
        aviso_assinatura = None
        if not ret_assinatura_analise or not ret_assinatura_analise.get('sucesso'):
            aviso_assinatura = 'Análise gerada mas assinatura falhou.'

        # 5. Gerar Despacho NCI (série 5) — assinatura dinâmica
        retorno_despacho = gerar_despacho_nci(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            ref_analise_formatado=analise_formatado,
            nome_assinante=current_user.nome.upper() if current_user.nome else None,
            cargo_assinante=cargo,
        )

        despacho_id = ''
        despacho_formatado = ''
        if retorno_despacho:
            despacho_id = str(retorno_despacho.get('IdDocumento', ''))
            despacho_formatado = retorno_despacho.get('DocumentoFormatado', '')

            # 6. Assinar o Despacho NCI
            ret_assinatura_despacho = assinar_documento(
                token=auth['token'],
                unidade_id=UNIDADE_NCI,
                dados_assinatura={
                    'protocolo_doc': despacho_id,
                    'orgao': 'SEAD-PI',
                    'cargo': cargo,
                    'id_login': auth['id_login'],
                    'id_usuario': auth['id_usuario'],
                    'senha': sei_senha,
                }
            )
            if not ret_assinatura_despacho or not ret_assinatura_despacho.get('sucesso'):
                aviso_assinatura = (aviso_assinatura or '') + ' Despacho NCI gerado mas assinatura falhou.'

        # 7. Salvar no banco
        itinerario.ciencia_nci = True
        itinerario.ciencia_nci_data = datetime.now()
        itinerario.analise_pagamento_respostas = json.dumps(respostas, ensure_ascii=False)
        itinerario.analise_pagamento_observacoes = observacoes
        itinerario.set_doc('analise_pagamento',
            sei_id=analise_id,
            sei_formatado=analise_formatado)
        itinerario.set_doc('despacho_nci',
            sei_id=despacho_id,
            sei_formatado=despacho_formatado)

        # 8. Análise NCI é o último sub-item da Análise 2ª Parte → avança para Concessão das Diárias
        DiariaService.registrar_movimentacao(
            id_itinerario=id,
            etapa_nova_id=DiariasEtapaID.CONCESSAO_DIARIAS,
            usuario_id=current_user.id if current_user else None,
            comentario=f'Análise NCI ({analise_formatado}) e Despacho NCI ({despacho_formatado}) gerados.',
        )

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'analise_nci', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')

        resposta = {
            'sucesso': True,
            'analise_formatado': analise_formatado,
            'analise_id': analise_id,
            'despacho_formatado': despacho_formatado,
            'despacho_id': despacho_id,
        }
        if aviso_assinatura:
            resposta['aviso'] = aviso_assinatura
        return jsonify(resposta)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# =============================================================================
# ATUALIZAR ETAPAS INDIVIDUAL (sincronizar documentos do SEI)
# =============================================================================

@diarias_bp.route('/api/atualizar-individual/<int:id_itinerario>', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_atualizar_individual(id_itinerario):
    """Sincroniza documentos do SEI e recalcula etapa de um itinerário."""
    if not current_user.is_admin:
        return jsonify({'sucesso': False, 'msg': 'Acesso restrito a administradores.'}), 403

    itinerario = DiariasItinerario.query.get(id_itinerario)
    if not itinerario:
        return jsonify({'sucesso': False, 'msg': 'Itinerário não encontrado.'}), 404

    # Flag para forçar reimportação de cotações (remove existentes e reimporta)
    force_cotacoes = False
    if request.is_json and request.get_json(silent=True):
        force_cotacoes = bool(request.get_json(silent=True).get('force_cotacoes', False))

    try:
        resultado = sincronizar_documentos_diaria(itinerario, force_cotacoes=force_cotacoes)
        if resultado['sucesso']:
            # Busca nome da etapa nova para exibir no frontend
            from app.models.diaria import DiariasEtapa
            etapa_obj = DiariasEtapa.query.get(resultado['etapa_nova'])
            return jsonify({
                'sucesso': True,
                'msg': ' | '.join(resultado['msgs']),
                'msgs': resultado['msgs'],
                'docs_encontrados': resultado.get('docs_encontrados', []),
                'docs_atualizados': resultado['docs_atualizados'],
                'cotacoes_importadas': resultado.get('cotacoes_importadas', 0),
                'etapa_anterior': resultado['etapa_anterior'],
                'etapa_nova': resultado['etapa_nova'],
                'etapa_nova_nome': etapa_obj.nome if etapa_obj else str(resultado['etapa_nova']),
                'autorizacao': resultado.get('autorizacao'),
            })
        else:
            return jsonify({'sucesso': False, 'msg': resultado.get('erro', 'Erro desconhecido.')}), 500
    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'msg': f'Erro: {str(e)}'}), 500


# ── Auditoria de Dados ─────────────────────────────────────────────────────

@diarias_bp.route('/auditoria')
@login_required
@requires_permission('diarias.aprovar')
def auditoria():
    """
    Página de auditoria — identifica informações faltantes em cada processo de diária.
    Verifica dados básicos, pessoas, documentos SEI (por etapa) e quadro orçamentário.
    """
    from app.models.diaria import (
        DiariasEtapa, DiariasDocumentoSei, DiariasQuadroOrcamentario,
        DiariasMovimentacao,
    )
    from app.constants import DIARIAS_SUBITENS, TIPOS_COM_PASSAGENS
    from app.services.diarias_sei_integration import SERIE_TIPO_DOCUMENTO_MAP

    # Filtros SQL — aplicados antes de carregar para reduzir volume
    filtro_etapa = request.args.getlist('filtro_etapa')
    filtro_tipo = request.args.getlist('filtro_tipo')
    filtro_pendencia = request.args.get('filtro_pendencia', '')
    filtro_faltando = request.args.getlist('filtro_faltando')

    # PERF-02: Eager load documentos_sei para evitar N+1 no has_doc()
    from sqlalchemy.orm import joinedload
    query = (
        DiariasItinerario.query
        .options(joinedload(DiariasItinerario.documentos_sei))
        .order_by(DiariasItinerario.data_solicitacao.desc(), DiariasItinerario.id.desc())
    )
    if filtro_etapa:
        filtro_etapa_int = [int(e) for e in filtro_etapa]
        query = query.filter(DiariasItinerario.etapa_atual_id.in_(filtro_etapa_int))
    if filtro_tipo:
        filtro_tipo_int = [int(t) for t in filtro_tipo]
        query = query.filter(DiariasItinerario.tipo_itinerario.in_(filtro_tipo_int))

    itinerarios = query.all()

    # Pré-carregar contagem de pessoas
    all_ids = [it.id for it in itinerarios]
    pessoas_count = {}
    if all_ids:
        counts = db.session.query(
            DiariasItemItinerario.id_itinerario,
            db.func.count(DiariasItemItinerario.id)
        ).filter(
            DiariasItemItinerario.id_itinerario.in_(all_ids)
        ).group_by(DiariasItemItinerario.id_itinerario).all()
        pessoas_count = {row[0]: row[1] for row in counts}

    # Pré-carregar quadros
    quadros_existentes = set()
    if all_ids:
        q_ids = db.session.query(
            DiariasQuadroOrcamentario.itinerario_id
        ).filter(
            DiariasQuadroOrcamentario.itinerario_id.in_(all_ids),
            DiariasQuadroOrcamentario.ug.isnot(None),
        ).all()
        quadros_existentes = {row[0] for row in q_ids}

    # PERF-01: Filtrar movimentações apenas dos protocolos relevantes (não ALL)
    mov_docs_por_protocolo = {}  # protocolo → set de doc_tipos
    protocolos_relevantes = {it.n_processo or it.sei_protocolo for it in itinerarios
                             if it.n_processo or it.sei_protocolo}
    if protocolos_relevantes:
        all_movs = DiariasMovimentacao.query.filter(
            DiariasMovimentacao.protocolo_procedimento.in_(protocolos_relevantes)
        ).all()
    else:
        all_movs = []
    for mov in all_movs:
        proto = mov.protocolo_procedimento or ''
        id_serie_str = str(mov.id_serie) if mov.id_serie else ''
        tipo_doc = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie_str)

        # Refinamento: IdSerie 264 com numero "PRESTAÇÃO SCDP" → prestacao_scdp
        if id_serie_str == '264':
            numero_upper = (mov.numero or '').upper()
            tem_prestacao = 'PRESTAÇÃO' in numero_upper or 'PRESTACAO' in numero_upper
            if tem_prestacao and 'SCDP' in numero_upper:
                tipo_doc = 'prestacao_scdp'

        if tipo_doc:
            if proto not in mov_docs_por_protocolo:
                mov_docs_por_protocolo[proto] = set()
            mov_docs_por_protocolo[proto].add(tipo_doc)

    etapas = DiariasEtapa.query.order_by(DiariasEtapa.ordem).all()

    resultados = []
    contadores = {'total': 0, 'completos': 0}

    for it in itinerarios:
        contadores['total'] += 1
        faltando = []
        tem_passagens = it.tipo_solicitacao_id in TIPOS_COM_PASSAGENS

        # Tipos de doc encontrados na movimentacao para este processo
        protocolo_mov = it.n_processo or it.sei_protocolo or ''
        tipos_mov = mov_docs_por_protocolo.get(protocolo_mov, set())

        # ── Dados básicos ──
        tem_protocolo = bool(it.sei_protocolo)
        tem_pessoas = pessoas_count.get(it.id, 0) > 0
        tem_valor = bool(it.valor_total and float(it.valor_total) > 0)
        tem_objetivo = bool(it.objetivo and it.objetivo.strip())
        tem_datas = bool(it.data_viagem and it.data_retorno)
        tem_quadro = it.id in quadros_existentes

        if not tem_protocolo:
            faltando.append('Processo SEI')
        if not tem_pessoas:
            faltando.append('Pessoas')
        if not tem_valor:
            faltando.append('Valor Total')
        if not tem_objetivo:
            faltando.append('Objetivo')
        if not tem_datas:
            faltando.append('Datas')

        # ── Documentos por etapa (via DIARIAS_SUBITENS) ──
        docs_status = {}
        tem_ob = it.has_doc('ob') or 'ob' in tipos_mov
        for etapa_id, subitens in DIARIAS_SUBITENS.items():
            for sub in subitens:
                # Pular condicionais que não se aplicam
                if sub.get('condicional') == 'passagens' and not tem_passagens:
                    continue
                # Pular etapa 2 (Escolha Voo) para processos sem passagens
                if etapa_id == DiariasEtapaID.ESCOLHA_VOO and not tem_passagens:
                    continue

                doc_tipo = sub['doc_tipo']
                # Verificar em AMBAS as fontes: DiariasDocumentoSei + diarias_movimentacao
                tem_doc = it.has_doc(doc_tipo) or doc_tipo in tipos_mov

                # Auto-preencher SCDP: se tem OB, SCDP já foi cumprido
                if not tem_doc and tem_ob and doc_tipo in ('autorizacao_scdp', 'prestacao_scdp'):
                    tem_doc = True

                # Nota: para tipo 1 (Apenas Diárias), o sub-item 'autorizacao'
                # nem é iterado (filtrado via condicional='passagens' no topo do loop).

                docs_status[doc_tipo] = tem_doc
                if not tem_doc and not sub.get('opcional'):
                    faltando.append(sub['nome'])

        # Quadro: considerar presente se tem dados estruturados OU doc SEI/movimentacao
        if not tem_quadro:
            tem_quadro = it.has_doc('quadro_orcamentario') or 'quadro_orcamentario' in tipos_mov
        if not tem_quadro:
            faltando.append('Quadro Orcamentario')

        if not faltando:
            contadores['completos'] += 1

        resultados.append({
            'itinerario': it,
            'qtd_pessoas': pessoas_count.get(it.id, 0),
            'tem_protocolo': tem_protocolo,
            'tem_pessoas': tem_pessoas,
            'tem_valor': tem_valor,
            'tem_objetivo': tem_objetivo,
            'tem_datas': tem_datas,
            'tem_quadro': tem_quadro,
            'docs_status': docs_status,
            'faltando': faltando,
            'qtd_faltando': len(faltando),
        })

    # Coletar todos os itens de pendencia distintos (para filtro)
    todos_itens_faltando = sorted({item for r in resultados for item in r['faltando']})

    # Filtros Python (pendência e faltando não são SQL-filtráveis)
    if filtro_pendencia == 'completos':
        resultados = [r for r in resultados if r['qtd_faltando'] == 0]
    elif filtro_pendencia == 'incompletos':
        resultados = [r for r in resultados if r['qtd_faltando'] > 0]
    if filtro_faltando:
        resultados = [r for r in resultados if any(f in r['faltando'] for f in filtro_faltando)]

    return render_template(
        'diarias/auditoria.html',
        resultados=resultados,
        contadores=contadores,
        itens_faltando=todos_itens_faltando,
        etapas=etapas,
    )
