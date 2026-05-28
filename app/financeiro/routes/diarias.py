"""
Rotas de Diárias - Módulo Financeiro.
Lista solicitações de diárias despachadas para DFIN e permite inserção de Nota de Reserva,
Quadro Orçamentário, upload de Autorização SCDP e criação de Nota de Empenho.
"""
from datetime import datetime
from decimal import Decimal, InvalidOperation
from flask import render_template, request, flash, redirect, url_for, abort, jsonify, current_app
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp
from app.models.diaria import DiariasItinerario, DiariasItemItinerario, DiariasQuadroOrcamentario, DiariasDocumentoSei
from app.extensions import db
from app.constants import DiariasEtapaID, etapa_diaria_em_ou_apos
from app.services.diaria_service import DiariaService
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, adicionar_documento_externo, gerar_quadro_orcamentario,
    gerar_despacho_ccdp, enviar_procedimento,
    gerar_despacho_diretor, gerar_despacho_geo,
    gerar_despacho_final_ccdp,
    ID_SERIE_AUTORIZACAO_SCDP, ID_SERIE_PRESTACAO_SCDP,
    ID_SERIE_NL, ID_SERIE_PD, ID_SERIE_OB, ID_SERIE_NP,
    UNIDADE_CCDP, UNIDADE_APOIOSGA, UNIDADE_DFIN_APOIO, UNIDADE_GEO,
)
from app.services.sei_auth import autenticar_usuario_sei
from app.services.sei_integration import assinar_documento
from app.utils.permissions import (
    requires_permission, usuario_tem_caixa,
    CAIXA_CCDP, CAIXA_DFIN_APOIO, CAIXA_GEO, CAIXA_GPO,
)
from app.services.diarias_notification import DiariasNotifier

EXTENSOES_UPLOAD_PERMITIDAS = {'.pdf', '.png', '.jpg', '.jpeg', '.doc', '.docx', '.xls', '.xlsx'}

# Tipos fixos com metadados visuais (ícone/cor) para os filtros dropdown
TIPOS_ITINERARIO = [
    {'id': 1, 'nome': 'Estadual', 'cor': '#0dcaf0', 'icone': 'bi-geo-alt'},
    {'id': 2, 'nome': 'Nacional', 'cor': '#ffc107', 'icone': 'bi-airplane'},
    {'id': 3, 'nome': 'Internacional', 'cor': '#dc3545', 'icone': 'bi-globe'},
]
TIPOS_SOLICITACAO = [
    {'id': 1, 'nome': 'Apenas Diárias', 'cor': '#198754', 'icone': 'bi-cash-stack'},
    {'id': 2, 'nome': 'Diárias + Passagens', 'cor': '#6f42c1', 'icone': 'bi-ticket-perforated'},
    {'id': 3, 'nome': 'Apenas Passagens', 'cor': '#0d6efd', 'icone': 'bi-airplane-engines'},
]


def _validar_extensao_arquivo(arquivo):
    """Retorna erro string se extensão inválida, None se ok."""
    if not arquivo or not arquivo.filename:
        return None
    ext = '.' + arquivo.filename.rsplit('.', 1)[-1].lower() if '.' in arquivo.filename else ''
    if ext not in EXTENSOES_UPLOAD_PERMITIDAS:
        return f'Extensão "{ext}" não permitida. Aceitas: {", ".join(sorted(EXTENSOES_UPLOAD_PERMITIDAS))}'
    return None


def _contar_pessoas(itinerarios):
    """Retorna dict {itinerario_id: count} para lista de itinerários."""
    if not itinerarios:
        return {}
    ids = [it.id for it in itinerarios]
    counts = db.session.query(
        DiariasItemItinerario.id_itinerario,
        db.func.count(DiariasItemItinerario.id)
    ).filter(
        DiariasItemItinerario.id_itinerario.in_(ids)
    ).group_by(DiariasItemItinerario.id_itinerario).all()
    return {row[0]: row[1] for row in counts}


def _aplicar_filtros_diarias(query, busca, filtro_tipos, filtro_solicitacao):
    """Aplica filtros comuns de busca, tipo e solicitação à query de diárias."""
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
    return query


def _contadores_filtro(query_base):
    """Retorna dicts de contagem por tipo_itinerario e tipo_solicitacao_id."""
    tipo_counts = dict(
        db.session.query(
            DiariasItinerario.tipo_itinerario, db.func.count()
        ).filter(
            DiariasItinerario.id.in_(query_base.with_entities(DiariasItinerario.id))
        ).group_by(DiariasItinerario.tipo_itinerario).all()
    )
    sol_counts = dict(
        db.session.query(
            DiariasItinerario.tipo_solicitacao_id, db.func.count()
        ).filter(
            DiariasItinerario.id.in_(query_base.with_entities(DiariasItinerario.id))
        ).group_by(DiariasItinerario.tipo_solicitacao_id).all()
    )
    return tipo_counts, sol_counts


@financeiro_bp.route('/diarias')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_lista():
    """Lista solicitações de diárias na etapa financeira ou posterior (etapa >= 2)."""
    busca = request.args.get('q', '').strip()
    filtro_status = request.args.getlist('filtro_status')
    filtro_tipos = [int(x) for x in request.args.getlist('filtro_tipo') if x.isdigit()]
    filtro_solicitacao = [int(x) for x in request.args.getlist('filtro_solicitacao') if x.isdigit()]
    page = request.args.get('page', 1, type=int)

    # Query base: processos autorizados (etapa >= Análise 1ª Parte)
    query_base = DiariasItinerario.query.filter(
        DiariasItinerario.etapa_atual_id >= int(DiariasEtapaID.ANALISE_SOLICITACAO)
    )

    # Contadores para badges (antes de aplicar filtros)
    tipo_counts, sol_counts = _contadores_filtro(query_base)

    # Status NR counts
    nr_pendente_q = query_base.filter(
        ~DiariasItinerario.documentos_sei.any(
            (DiariasDocumentoSei.tipo_documento == 'nota_reserva') & (DiariasDocumentoSei.codigo.isnot(None))
        ),
        DiariasItinerario.etapa_atual_id == int(DiariasEtapaID.ANALISE_SOLICITACAO),
    )
    nr_inserida_q = query_base.filter(
        DiariasItinerario.documentos_sei.any(
            (DiariasDocumentoSei.tipo_documento == 'nota_reserva') & (DiariasDocumentoSei.codigo.isnot(None))
        ),
    )
    status_counts = {
        'pendente': nr_pendente_q.count(),
        'inserida': nr_inserida_q.count(),
    }

    # Aplica filtros
    query = _aplicar_filtros_diarias(query_base, busca, filtro_tipos, filtro_solicitacao)

    if 'pendente' in filtro_status and 'inserida' not in filtro_status:
        query = query.filter(
            ~DiariasItinerario.documentos_sei.any(
                (DiariasDocumentoSei.tipo_documento == 'nota_reserva') & (DiariasDocumentoSei.codigo.isnot(None))
            ),
            DiariasItinerario.etapa_atual_id == int(DiariasEtapaID.ANALISE_SOLICITACAO),
        )
    elif 'inserida' in filtro_status and 'pendente' not in filtro_status:
        query = query.filter(
            DiariasItinerario.documentos_sei.any(
                (DiariasDocumentoSei.tipo_documento == 'nota_reserva') & (DiariasDocumentoSei.codigo.isnot(None))
            ),
        )

    pagination = query.order_by(
        DiariasItinerario.data_solicitacao.desc(),
        DiariasItinerario.id.desc(),
    ).paginate(page=page, per_page=20, error_out=False)

    return render_template(
        'financeiro/diarias_lista.html',
        itinerarios=pagination.items,
        pagination=pagination,
        pessoas_count=_contar_pessoas(pagination.items),
        filtro_status=filtro_status,
        filtro_tipos=filtro_tipos,
        filtro_solicitacao=filtro_solicitacao,
        tipo_counts=tipo_counts,
        sol_counts=sol_counts,
        status_counts=status_counts,
        tipos_itinerario=TIPOS_ITINERARIO,
        tipos_solicitacao=TIPOS_SOLICITACAO,
    )


def _pode_ver_despachos():
    """Verifica se o usuário pode acessar a aba Despachos e Autorizações.
    Acesso: Admin, Diretor DFIN, Superintendente SGA."""
    if current_user.is_admin:
        return True
    if getattr(current_user, 'cargo_gestao', None) == 'diretor_dfin':
        return True
    if current_user.is_superintendente and getattr(current_user, 'superintendencia_sigla', None) == 'SGACG':
        return True
    return False


@financeiro_bp.route('/diarias/despachos')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_despachos():
    """Lista processos de diárias pendentes de despacho do Diretor DFIN."""
    if not _pode_ver_despachos():
        flash('Acesso restrito ao Diretor DFIN, Superintendente SGA e administradores.', 'warning')
        return redirect(url_for('financeiro.diarias_lista'))

    busca = request.args.get('q', '').strip()
    filtro_tipos = [int(x) for x in request.args.getlist('filtro_tipo') if x.isdigit()]
    filtro_solicitacao = [int(x) for x in request.args.getlist('filtro_solicitacao') if x.isdigit()]
    page = request.args.get('page', 1, type=int)

    # Query base: autorizados (etapa >= 3), sem despacho DFIN, somente processos nativos do SGC
    query_base = DiariasItinerario.query.filter(
        DiariasItinerario.etapa_atual_id >= int(DiariasEtapaID.ANALISE_SOLICITACAO),
        DiariasItinerario.usuario_gerador != 'IMPORTACAO_SEI',
        ~DiariasItinerario.documentos_sei.any(
            DiariasDocumentoSei.tipo_documento == 'despacho_dfin'
        ),
    )

    tipo_counts, sol_counts = _contadores_filtro(query_base)

    query = _aplicar_filtros_diarias(query_base, busca, filtro_tipos, filtro_solicitacao)

    pagination = query.order_by(
        DiariasItinerario.data_solicitacao.desc(),
        DiariasItinerario.id.desc(),
    ).paginate(page=page, per_page=20, error_out=False)

    return render_template(
        'financeiro/diarias_despachos.html',
        itinerarios=pagination.items,
        pagination=pagination,
        pessoas_count=_contar_pessoas(pagination.items),
        filtro_tipos=filtro_tipos,
        filtro_solicitacao=filtro_solicitacao,
        tipo_counts=tipo_counts,
        sol_counts=sol_counts,
        tipos_itinerario=TIPOS_ITINERARIO,
        tipos_solicitacao=TIPOS_SOLICITACAO,
    )


@financeiro_bp.route('/diarias/<int:id>/despachar-dfin', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despachar_dfin(id):
    """Diretor DFIN gera e assina o despacho no SEI — gate para inserção de NR.

    Recebe JSON com {sei_usuario, sei_senha} para assinatura.
    Com DIARIAS_BYPASS_ASSINATURAS=True, credenciais são opcionais.
    """
    from app.services.sei_integration import assinar_documento
    from app.services.diarias_sei_integration import (
        gerar_despacho_dfin, UNIDADE_GPO, UNIDADE_DFIN_APOIO,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: apenas Diretor DFIN, Superintendente SGA ou Admin
    if not _pode_ver_despachos():
        return jsonify({'sucesso': False, 'erro': 'Apenas o Diretor DFIN, Superintendente SGA ou administradores podem despachar.'}), 403

    # Guard: já despachado
    if itinerario.has_doc('despacho_dfin'):
        doc = itinerario.get_doc('despacho_dfin')
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': f'Despacho já gerado: {doc.sei_formatado or doc.sei_id}',
        })

    # Guard: precisa ter sido autorizado (etapa >= Análise 1ª Parte)
    if itinerario.etapa_atual_id < DiariasEtapaID.ANALISE_SOLICITACAO:
        return jsonify({'sucesso': False, 'erro': 'Solicitação ainda não foi autorizada.'}), 400

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Sem processo SEI vinculado.'}), 400

    # Credenciais SEI do Diretor (para assinatura)
    dados = request.get_json() or {}
    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()

    # Protocolo usado para detectar bypass por processo específico
    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    # Autenticar Diretor no SEI (para assinatura)
    auth = autenticar_usuario_sei(
        sei_usuario or 'bypass', sei_senha or 'bypass',
        protocolo_bypass=protocolo_proc,
    )
    if not auth:
        return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI.'}), 401

    cargo = auth.get('cargo') or current_user.cargo or 'Diretor de Planejamento e Finanças da SEAD-PI'

    try:
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = protocolo_proc

        # 1. Gerar despacho DFIN
        itens = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).all()
        nomes = [i.nome_pessoa for i in itens if i.nome_pessoa]

        # nome_assinante/cargo_assinante omitidos — a função resolve o titular
        # DFIN automaticamente (cargo_gestao='diretor_dfin'), garantindo que o
        # documento seja sempre assinado pelo Diretor, não pelo usuário logado.
        ret = gerar_despacho_dfin(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            interessados=nomes,
        )

        if not ret:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar despacho no SEI.'}), 500

        doc_id = str(ret.get('IdDocumento', ''))
        doc_fmt = ret.get('DocumentoFormatado', '')

        # 2. Assinar o despacho com credenciais do Diretor
        ret_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_DFIN_APOIO,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha or 'bypass',
            },
            protocolo_proc=protocolo_proc,
        )

        aviso_assinatura = ''
        if not ret_assinatura or not ret_assinatura.get('sucesso'):
            aviso_assinatura = f' (Aviso: assinatura: {ret_assinatura.get("erro", "") if ret_assinatura else "sem resposta"})'

        # 3. Salvar no banco
        itinerario.set_doc('despacho_dfin', sei_id=doc_id, sei_formatado=doc_fmt)
        db.session.commit()

        # 4. Enviar processo para GPO
        envio_gpo = enviar_procedimento(token, sei_protocolo, [UNIDADE_GPO], manter_aberto=True)
        envio_msg = ''
        if envio_gpo.get('sucesso'):
            envio_msg = ' Processo encaminhado à GPO.'
            current_app.logger.info(f"SEI Diárias: Processo {sei_protocolo} encaminhado à GPO.")
        else:
            envio_msg = f' Aviso: falha ao encaminhar à GPO: {envio_gpo.get("erro", "")}'

        # 5. Notificar equipe financeira
        aviso_notif = ''
        try:
            DiariasNotifier.notificar_etapa(itinerario, 'nota_reserva', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')
            aviso_notif = ' Notificação não enviada — informe a equipe manualmente.'

        resp = {
            'sucesso': True,
            'documento_formatado': doc_fmt,
            'mensagem': f'Despacho gerado: {doc_fmt}.{envio_msg}{aviso_assinatura}',
        }
        if aviso_notif:
            resp['aviso'] = aviso_notif
        return jsonify(resp)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro: {str(e)}'}), 500


@financeiro_bp.route('/diarias/<int:id>')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_detalhe(id):
    """Exibe detalhes de uma solicitação de diária para o financeiro."""
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        abort(404)

    itinerario = dados['itinerario']

    # Só mostra se já passou da etapa 1 (autorizado)
    if itinerario.etapa_atual_id < DiariasEtapaID.ANALISE_SOLICITACAO:
        abort(404)

    # Auto-varredura: tenta extrair NR do SEI se ainda não preenchida
    doc_nr = itinerario.get_doc('nota_reserva')
    if not (doc_nr and doc_nr.codigo) and itinerario.sei_protocolo:
        try:
            from app.services.diarias_sei_integration import varrer_nota_reserva
            resultado_nr = varrer_nota_reserva(itinerario)
            if resultado_nr.get('sucesso'):
                db.session.commit()
                current_app.logger.info(
                    f"[DIARIAS FIN] NR auto-detectada: {resultado_nr['nr_codigo']} "
                    f"para itinerário {itinerario.id}"
                )
        except Exception as e:
            current_app.logger.warning(f"[DIARIAS FIN] Erro na auto-varredura NR: {e}")

    timeline_data = DiariaService.obter_timeline(itinerario)

    # NRs cadastradas por servidor (para painel de múltiplas NRs)
    from app.models.diaria import DiariasNotaReserva, DiariasNotaEmpenho
    nrs_por_item = {
        nr.item_itinerario_id: nr
        for nr in DiariasNotaReserva.query.filter_by(itinerario_id=itinerario.id).all()
    }
    total_servidores = len(dados['itens'])
    total_nrs = DiariasNotaReserva.query.filter_by(itinerario_id=itinerario.id).filter(
        DiariasNotaReserva.sei_id.isnot(None)
    ).count()
    todas_nrs_ok = total_servidores > 0 and total_nrs >= total_servidores

    # NEs cadastradas por servidor (mesmo padrão da NR: 1 por servidor)
    nes_por_item = {
        ne.item_itinerario_id: ne
        for ne in DiariasNotaEmpenho.query.filter_by(itinerario_id=itinerario.id).all()
    }
    total_nes = DiariasNotaEmpenho.query.filter_by(itinerario_id=itinerario.id).filter(
        DiariasNotaEmpenho.sei_id.isnot(None)
    ).count()
    todas_nes_ok = total_servidores > 0 and total_nes >= total_servidores

    # NL/PD/OB cadastrados por servidor (mesmo padrao de NR/NE)
    from app.models.diaria import (
        DiariasNotaLiquidacao, DiariasProgramacaoDesembolso, DiariasOrdemBancaria,
        DiariasNotaPatrimonial,
    )
    nls_por_item = {
        r.item_itinerario_id: r
        for r in DiariasNotaLiquidacao.query.filter_by(itinerario_id=itinerario.id).all()
    }
    pds_por_item = {
        r.item_itinerario_id: r
        for r in DiariasProgramacaoDesembolso.query.filter_by(itinerario_id=itinerario.id).all()
    }
    obs_por_item = {
        r.item_itinerario_id: r
        for r in DiariasOrdemBancaria.query.filter_by(itinerario_id=itinerario.id).all()
    }
    total_nls = len(nls_por_item)
    total_pds = len(pds_por_item)
    total_obs = len(obs_por_item)
    todas_nls_ok = total_servidores > 0 and total_nls >= total_servidores
    todas_pds_ok = total_servidores > 0 and total_pds >= total_servidores
    todas_obs_ok = total_servidores > 0 and total_obs >= total_servidores

    # NPs cadastradas por servidor (Prestacao de Contas)
    nps_por_item = {
        r.item_itinerario_id: r
        for r in DiariasNotaPatrimonial.query.filter_by(itinerario_id=itinerario.id).all()
    }
    total_nps = len(nps_por_item)
    todas_nps_ok = total_servidores > 0 and total_nps >= total_servidores

    return render_template(
        'financeiro/diarias_detalhe.html',
        itinerario=itinerario,
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        timeline_data=timeline_data,
        nrs_por_item=nrs_por_item,
        total_servidores=total_servidores,
        total_nrs=total_nrs,
        todas_nrs_ok=todas_nrs_ok,
        nes_por_item=nes_por_item,
        total_nes=total_nes,
        todas_nes_ok=todas_nes_ok,
        nls_por_item=nls_por_item,
        pds_por_item=pds_por_item,
        obs_por_item=obs_por_item,
        total_nls=total_nls,
        total_pds=total_pds,
        total_obs=total_obs,
        todas_nls_ok=todas_nls_ok,
        todas_pds_ok=todas_pds_ok,
        todas_obs_ok=todas_obs_ok,
        nps_por_item=nps_por_item,
        total_nps=total_nps,
        todas_nps_ok=todas_nps_ok,
    )


@financeiro_bp.route('/diarias/<int:id>/inserir-nr', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_nr(id):
    """Insere Nota de Reserva individual para um servidor da solicitação.

    Form espera:
      - item_itinerario_id: ID do servidor (diarias_itens_itinerario.id)
      - nota_reserva: código da NR (ex: '2026NR00123')
      - valor_nr (opcional): valor reservado
      - arquivo_nr (opcional): PDF da NR para upload ao SEI
    """
    from app.models.diaria import DiariasNotaReserva, DiariasItemItinerario

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: só permite inserção na etapa 3 (Análise 1ª Parte)
    if itinerario.etapa_atual_id != DiariasEtapaID.ANALISE_SOLICITACAO:
        flash('Esta solicitação não está na etapa correta para inserção de NR.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Guard: Diretor DFIN precisa ter despachado antes
    if not itinerario.has_doc('despacho_dfin'):
        flash('O Diretor DFIN precisa despachar esta solicitação antes de inserir a NR.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Guard: somente usuários com acesso à caixa GPO podem inserir NR
    if not usuario_tem_caixa(CAIXA_GPO):
        flash('Apenas usuários da GPO (Gerência de Planejamento e Orçamento) podem inserir NR.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    item_id_raw = request.form.get('item_itinerario_id', '').strip()
    nr_code = request.form.get('nota_reserva', '').strip()
    valor_raw = request.form.get('valor_nr', '').strip()
    arquivo = request.files.get('arquivo_nr')

    if not item_id_raw:
        flash('Servidor obrigatório para inserir NR.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))
    if not nr_code:
        flash('O código da Nota de Reserva é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    try:
        item_id = int(item_id_raw)
    except ValueError:
        flash('Servidor inválido.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Valida que o servidor pertence a este itinerário
    item = DiariasItemItinerario.query.filter_by(
        id=item_id, id_itinerario=itinerario.id
    ).first()
    if not item:
        flash('Servidor não pertence a esta solicitação.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Verifica se já existe NR para este servidor (UPSERT)
    nr = DiariasNotaReserva.query.filter_by(
        itinerario_id=itinerario.id, item_itinerario_id=item_id
    ).first()

    # Captura se a NR já tinha documento SEI vinculado ANTES de qualquer mudança.
    # Usado para prevenir re-upload do PDF em duplo-clique.
    ja_tinha_sei_id = bool(nr and nr.sei_id)

    valor_decimal = _parse_valor_brl(valor_raw)
    if nr:
        nr.codigo = nr_code
        if valor_decimal is not None:
            nr.valor = valor_decimal
    else:
        nr = DiariasNotaReserva(
            itinerario_id=itinerario.id,
            item_itinerario_id=item_id,
            codigo=nr_code,
            valor=valor_decimal,
        )
        db.session.add(nr)

    db.session.commit()

    # Upload do PDF ao SEI (se arquivo fornecido, processo SEI existe,
    # e ainda não há sei_id — evita duplo-upload em duplo-clique/retentativa).
    sei_upload_ok = False
    if ja_tinha_sei_id and arquivo and arquivo.filename:
        flash(
            f'Aviso: já existe documento SEI vinculado a esta NR ({nr.sei_formatado}). '
            'O arquivo enviado foi ignorado para evitar duplicata.',
            'info',
        )
    if arquivo and arquivo.filename and itinerario.sei_protocolo and not ja_tinha_sei_id:
        try:
            arquivo_bytes = arquivo.read()
            if len(arquivo_bytes) > 0:
                token = gerar_token_sei_admin()
                if token:
                    from app.services.diarias_sei_integration import ID_SERIE_NOTA_RESERVA, UNIDADE_GPO
                    enviar_procedimento(token, itinerario.sei_protocolo, [UNIDADE_GPO], manter_aberto=True)
                    retorno = adicionar_documento_externo(
                        token=token,
                        protocolo_formatado=itinerario.sei_protocolo,
                        arquivo_bytes=arquivo_bytes,
                        nome_arquivo=arquivo.filename,
                        descricao=f'Nota de Reserva {nr_code} — {item.nome_pessoa or ""}',
                        id_serie=ID_SERIE_NOTA_RESERVA,
                        unidade_id=UNIDADE_GPO,
                    )
                    if retorno:
                        nr.sei_id = str(retorno.get('IdDocumento', ''))
                        nr.sei_formatado = retorno.get('DocumentoFormatado', '')
                        db.session.commit()
                        sei_upload_ok = True
                    else:
                        flash('Aviso: NR salva, mas o upload do documento ao SEI falhou.', 'warning')
        except Exception as e:
            flash(f'Aviso: NR salva, mas erro ao enviar documento ao SEI: {e}', 'warning')

    # Marcador agregado: só dispara quando TODOS os servidores têm NR com sei_id confirmado.
    total_servidores = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
    total_nrs = DiariasNotaReserva.query.filter_by(itinerario_id=itinerario.id).filter(
        DiariasNotaReserva.sei_id.isnot(None)
    ).count()
    if total_nrs >= total_servidores > 0:
        itinerario.set_doc('nota_reserva', codigo=nr_code)
        db.session.commit()

    msg_sei = ' e documento enviado ao SEI' if sei_upload_ok else ''
    flash(
        f'NR {nr_code} inserida para {item.nome_pessoa or "servidor"}{msg_sei}. '
        f'({total_nrs}/{total_servidores} servidores com NR)',
        'success' if total_nrs < total_servidores else 'success'
    )

    try:
        DiariasNotifier.notificar_etapa(itinerario, 'nota_reserva', current_user.id)
    except Exception as exc_notif:
        current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


def _parse_valor_brl(valor_str):
    """Converte string de valor BR (1.234,56 ou 1234.56) para Decimal."""
    if not valor_str:
        return None
    valor_str = valor_str.strip().replace('R$', '').strip()
    # Formato brasileiro: 1.234,56 → 1234.56
    if ',' in valor_str:
        valor_str = valor_str.replace('.', '').replace(',', '.')
    try:
        return Decimal(valor_str)
    except (InvalidOperation, ValueError):
        return None


@financeiro_bp.route('/diarias/<int:id>/inserir-quadro-orcamentario', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_quadro_orcamentario(id):
    """Insere Quadro Orçamentário em uma solicitação de diária.

    Só libera quando TODOS os servidores da solicitação já têm NR cadastrada
    (1 NR por servidor).
    """
    from app.models.diaria import DiariasNotaReserva, DiariasItemItinerario

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: somente usuários com acesso à caixa GPO podem inserir Quadro Orçamentário
    if not usuario_tem_caixa(CAIXA_GPO):
        flash('Apenas usuários da GPO (Gerência de Planejamento e Orçamento) podem inserir o Quadro Orçamentário.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Gate: exige 1 NR por servidor antes do quadro
    total_servidores = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
    total_nrs = DiariasNotaReserva.query.filter_by(itinerario_id=itinerario.id).count()
    if total_servidores == 0 or total_nrs < total_servidores:
        flash(
            f'É necessário cadastrar uma Nota de Reserva por servidor antes do Quadro Orçamentário '
            f'({total_nrs}/{total_servidores} NRs inseridas).',
            'warning'
        )
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if itinerario.quadro_orcamentario and itinerario.quadro_orcamentario.ug:
        flash('O Quadro Orçamentário já foi inserido para esta solicitação.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Coleta campos do formulário
    ug = request.form.get('quadro_ug', '').strip()
    funcao = request.form.get('quadro_funcao', '').strip()
    subfuncao = request.form.get('quadro_subfuncao', '').strip()
    programa = request.form.get('quadro_programa', '').strip()
    plano_interno = request.form.get('quadro_plano_interno', '').strip()
    fonte_recursos = request.form.get('quadro_fonte_recursos', '').strip()
    natureza_despesa = request.form.get('quadro_natureza_despesa', '').strip()
    valor_inicial_nr = _parse_valor_brl(request.form.get('quadro_valor_inicial_nr', ''))
    saldo_nr = _parse_valor_brl(request.form.get('quadro_saldo_nr', ''))
    valor_despesa = _parse_valor_brl(request.form.get('quadro_valor_despesa', ''))
    saldo_atual_nr = _parse_valor_brl(request.form.get('quadro_saldo_atual_nr', ''))

    # Validação básica
    if not ug or not natureza_despesa:
        flash('UG e Natureza da Despesa são obrigatórios.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if valor_despesa is None:
        flash('Valor da Despesa é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Salva no banco (via relacionamento quadro_orcamentario)
    if not itinerario.quadro_orcamentario:
        itinerario.quadro_orcamentario = DiariasQuadroOrcamentario(itinerario_id=itinerario.id)
    itinerario.quadro_orcamentario.ug = ug
    itinerario.quadro_orcamentario.funcao = funcao
    itinerario.quadro_orcamentario.subfuncao = subfuncao
    itinerario.quadro_orcamentario.programa = programa
    itinerario.quadro_orcamentario.plano_interno = plano_interno
    itinerario.quadro_orcamentario.fonte_recursos = fonte_recursos
    itinerario.quadro_orcamentario.natureza_despesa = natureza_despesa
    itinerario.quadro_orcamentario.valor_inicial_nr = valor_inicial_nr
    itinerario.quadro_orcamentario.saldo_nr = saldo_nr
    itinerario.quadro_orcamentario.valor_despesa = valor_despesa
    itinerario.quadro_orcamentario.saldo_atual_nr = saldo_atual_nr

    # Credenciais SEI para assinatura
    sei_usuario = request.form.get('sei_usuario', '').strip()
    sei_senha = request.form.get('sei_senha', '').strip()

    # Gera documento no SEI (se processo SEI existe)
    sei_ok = False
    if itinerario.sei_id_procedimento:
        try:
            token = gerar_token_sei_admin()
            if token:
                retorno = gerar_quadro_orcamentario(
                    token=token,
                    id_procedimento=itinerario.sei_id_procedimento,
                    dados_quadro={
                        'ug': ug,
                        'funcao': funcao,
                        'subfuncao': subfuncao,
                        'programa': programa,
                        'plano_interno': plano_interno,
                        'fonte_recursos': fonte_recursos,
                        'natureza_despesa': natureza_despesa,
                        'valor_inicial_nr': valor_inicial_nr,
                        'saldo_nr': saldo_nr,
                        'valor_despesa': valor_despesa,
                        'saldo_atual_nr': saldo_atual_nr,
                    },
                    sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
                )
                if retorno:
                    doc_id = str(retorno.get('IdDocumento', ''))
                    itinerario.set_doc('quadro_orcamentario',
                                       sei_id=doc_id,
                                       sei_formatado=retorno.get('DocumentoFormatado', ''))
                    sei_ok = True

                    # Assinar o documento com credenciais do usuário
                    if sei_usuario and sei_senha:
                        from app.services.sei_integration import assinar_documento
                        from app.services.diarias_sei_integration import UNIDADE_GPO
                        protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo
                        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
                        if auth and auth.get('token'):
                            cargo_assinatura = auth.get('cargo') or current_user.cargo or ''
                            ret_ass = assinar_documento(
                                token=auth['token'],
                                unidade_id=UNIDADE_GPO,
                                dados_assinatura={
                                    'protocolo_doc': doc_id,
                                    'orgao': 'SEAD-PI',
                                    'cargo': cargo_assinatura,
                                    'id_login': auth['id_login'],
                                    'id_usuario': auth['id_usuario'],
                                    'senha': sei_senha,
                                },
                                protocolo_proc=protocolo_proc,
                            )
                            if ret_ass and ret_ass.get('sucesso'):
                                flash('Documento assinado com sucesso!', 'success')
                            else:
                                flash(f'Documento gerado mas assinatura falhou: {ret_ass.get("erro", "") if ret_ass else ""}', 'warning')
                        else:
                            flash('Documento gerado mas autenticação SEI falhou.', 'warning')
                else:
                    flash('Aviso: Quadro salvo, mas a geração do documento no SEI falhou.', 'warning')
            else:
                flash('Aviso: Quadro salvo, mas não foi possível autenticar no SEI.', 'warning')
        except Exception as e:
            flash(f'Aviso: Quadro salvo, mas erro ao gerar documento no SEI: {e}', 'warning')

    db.session.commit()

    if sei_ok:
        flash('Quadro Orçamentário inserido e documento gerado no SEI!', 'success')
    elif not itinerario.sei_id_procedimento:
        flash('Quadro Orçamentário inserido com sucesso!', 'success')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


@financeiro_bp.route('/diarias/<int:id>/despacho-geo-quadro', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despacho_geo_quadro(id):
    """Gerente GEO gera despacho após quadro orçamentário — gate para SCDP.

    Cria o documento SEAD_DESPACHO (série 754) na unidade GEO,
    assina com credenciais do Gerente e envia o processo para GEO.
    """
    from app.services.sei_integration import assinar_documento
    from app.services.diarias_sei_integration import (
        gerar_despacho_geo, UNIDADE_GEO,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: quadro orçamentário deve existir
    if not itinerario.quadro_orcamentario or not itinerario.quadro_orcamentario.ug:
        return jsonify({'sucesso': False, 'erro': 'O Quadro Orçamentário deve ser inserido antes do despacho.'}), 400

    # Guard: já tem despacho GEO do quadro
    if itinerario.has_doc('despacho_geo_quadro'):
        doc = itinerario.get_doc('despacho_geo_quadro')
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': f'Despacho já gerado: {doc.sei_formatado or doc.sei_id}',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Sem processo SEI vinculado.'}), 400

    dados = request.get_json() or {}
    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()

    # Protocolo para bypass de assinatura (processos de teste)
    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    auth = autenticar_usuario_sei(
        sei_usuario or 'bypass', sei_senha or 'bypass',
        protocolo_bypass=protocolo_proc,
    )
    if not auth:
        return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI.'}), 401

    cargo = auth.get('cargo') or current_user.cargo or 'Gerente de Execução Orçamentária - SEAD-PI'

    try:
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = protocolo_proc

        # Enviar processo para GEO (se ainda não está lá)
        enviar_procedimento(token, sei_protocolo, [UNIDADE_GEO], manter_aberto=True)

        # Gerar despacho GEO — a função resolve o titular da GEO internamente
        # (ignora nome_assinante/cargo_assinante para garantir que o documento
        # seja sempre assinado pelo Gerente GEO, não pelo usuário logado).
        ret = gerar_despacho_geo(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            itinerario=itinerario,
        )

        if not ret:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar despacho no SEI.'}), 500

        doc_id = str(ret.get('IdDocumento', ''))
        doc_fmt = ret.get('DocumentoFormatado', '')

        # Assinar
        ret_ass = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_GEO,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha or 'bypass',
            },
            protocolo_proc=protocolo_proc,
        )

        aviso = ''
        if not ret_ass or not ret_ass.get('sucesso'):
            aviso = f' (assinatura: {ret_ass.get("erro", "") if ret_ass else "sem resposta"})'

        itinerario.set_doc('despacho_geo_quadro', sei_id=doc_id, sei_formatado=doc_fmt)

        # Avançar etapa SOMENTE se TODOS os sub-itens da Análise 1ª Parte
        # estiverem cumpridos (NR de todos os servidores + Quadro + Análise
        # de Habilitação). Caso contrário, mantém o processo na etapa 3 até
        # que o Análise de Habilitação seja gerada.
        from app.constants import TIPOS_COM_PASSAGENS
        if itinerario.etapa_atual_id == DiariasEtapaID.ANALISE_SOLICITACAO:
            tem_habilitacao = itinerario.has_doc('analise_habilitacao')
            if tem_habilitacao:
                proxima = DiariasEtapaID.ANALISE_SOLICITACAO_2
                DiariaService.registrar_movimentacao(
                    id_itinerario=id,
                    etapa_nova_id=proxima,
                    usuario_id=current_user.id,
                    comentario=f'Despacho GEO ({doc_fmt}) gerado e Análise 1ª Parte completa. Etapa avançada.',
                    auto_commit=False,
                )
                aviso += ' Etapa avançada para próxima fase.'
            else:
                aviso += (
                    ' Aviso: Análise de Habilitação ainda pendente — '
                    'a etapa permanecerá em Análise 1ª Parte até a habilitação ser gerada.'
                )

        db.session.commit()

        return jsonify({
            'sucesso': True,
            'documento_formatado': doc_fmt,
            'mensagem': f'Despacho GEO gerado: {doc_fmt}.{aviso}',
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro: {str(e)}'}), 500


@financeiro_bp.route('/diarias/<int:id>/gerar-analise-habilitacao', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def gerar_analise_habilitacao(id):
    """Gera documento de Análise de Habilitação (IdSerie 7) no SEI.

    Verifica elegibilidade de cada servidor nas tabelas:
    - diarias_controle_prestacao
    - diarias_controle_servidores
    - diarias_controle_viagens

    Regras: Decreto 14.910/2012
    - Art. 7º: limite 180 diárias/ano
    - Art. 12, §2º: prestação de contas aprovada
    """
    from app.services.sei_integration import assinar_documento
    from app.services.diarias_sei_integration import (
        gerar_analise_diarias, verificar_elegibilidade_servidor,
        UNIDADE_CCDP,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: despacho GEO deve existir
    if not itinerario.has_doc('despacho_geo_quadro'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho GEO deve ser gerado antes da análise.'}), 400

    # Guard: análise já gerada
    if itinerario.has_doc('analise_habilitacao'):
        doc = itinerario.get_doc('analise_habilitacao')
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': f'Análise já gerada: {doc.sei_formatado or doc.sei_id}',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Sem processo SEI vinculado.'}), 400

    dados = request.get_json() or {}
    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''
    auth = autenticar_usuario_sei(
        sei_usuario or 'bypass', sei_senha or 'bypass',
        protocolo_bypass=protocolo_proc,
    )
    if not auth:
        return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI.'}), 401

    cargo = auth.get('cargo') or current_user.cargo or ''

    try:
        # 1. Verificar elegibilidade de cada pessoa do itinerário
        itens = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).all()
        servidores_analise = []

        for item in itens:
            cpf = item.cpf_pessoa
            if not cpf:
                continue
            resultado_elegib = verificar_elegibilidade_servidor(cpf)
            servidores_analise.append({
                'nome': item.nome_pessoa or cpf,
                'cpf': cpf,
                'acumulado': resultado_elegib.get('acumulado', 0),
                'prestacao_status': resultado_elegib.get('prestacao_status', 'N/A'),
                'apto': resultado_elegib.get('apto', True),
                'motivo_bloqueio': resultado_elegib.get('motivo_bloqueio'),
            })

        # 2. Gerar documento no SEI
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        # Enviar processo para CCDP antes de criar documento de análise
        enviar_procedimento(token, sei_protocolo, [UNIDADE_CCDP], manter_aberto=True)

        ret = gerar_analise_diarias(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            servidores_analise=servidores_analise,
        )

        if not ret:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar documento de análise no SEI.'}), 500

        doc_id = str(ret.get('IdDocumento', ''))
        doc_fmt = ret.get('DocumentoFormatado', '')

        # 3. Assinar
        ret_ass = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_CCDP,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha or 'bypass',
            },
        )

        aviso = ''
        if not ret_ass or not ret_ass.get('sucesso'):
            aviso = f' (assinatura: {ret_ass.get("erro", "") if ret_ass else "sem resposta"})'

        # 4. Salvar no banco
        itinerario.set_doc('analise_habilitacao', sei_id=doc_id, sei_formatado=doc_fmt)

        # Se o despacho GEO já existe, agora a Análise 1ª Parte está completa
        # e pode-se avançar a etapa. Isso cobre o caso em que a Habilitação
        # é gerada DEPOIS do despacho GEO (ordem inversa).
        from app.constants import TIPOS_COM_PASSAGENS
        avanco_msg = ''
        if (itinerario.etapa_atual_id == DiariasEtapaID.ANALISE_SOLICITACAO
                and itinerario.has_doc('despacho_geo_quadro')):
            proxima = DiariasEtapaID.ANALISE_SOLICITACAO_2
            DiariaService.registrar_movimentacao(
                id_itinerario=id,
                etapa_nova_id=proxima,
                usuario_id=current_user.id,
                comentario=f'Análise de Habilitação ({doc_fmt}) gerada — Análise 1ª Parte completa.',
                auto_commit=False,
            )
            avanco_msg = ' Etapa avançada para próxima fase.'

        db.session.commit()

        # Resultado com dados da análise para exibição
        todos_aptos = all(s['apto'] for s in servidores_analise)

        return jsonify({
            'sucesso': True,
            'documento_formatado': doc_fmt,
            'todos_aptos': todos_aptos,
            'servidores': servidores_analise,
            'mensagem': f'Análise gerada: {doc_fmt}. {"Todos aptos." if todos_aptos else "ATENÇÃO: servidor(es) inapto(s)."}{aviso}{avanco_msg}',
        })

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'[DIARIAS-FIN] Erro ao gerar análise: {e}')
        return jsonify({'sucesso': False, 'erro': f'Erro: {str(e)}'}), 500


@financeiro_bp.route('/diarias/<int:id>/upload-scdp', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def upload_autorizacao_scdp(id):
    """Upload do PDF 'Autorização SOLICITAÇÃO APROVADA SCDP' ao processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: análise de habilitação deve existir
    if not itinerario.has_doc('analise_habilitacao'):
        flash('A Análise de Habilitação deve ser gerada antes do SCDP.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Guard: precisa ter processo SEI
    if not itinerario.sei_protocolo:
        flash('Esta solicitação não possui processo SEI vinculado.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if itinerario.has_doc('autorizacao_scdp'):
        flash('A Autorização SCDP já foi enviada para esta solicitação.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    arquivo = request.files.get('arquivo_scdp')
    numero_scdp = request.form.get('numero_scdp', '').strip()

    if not arquivo or not arquivo.filename:
        flash('O arquivo PDF da Autorização SCDP é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    ext_erro = _validar_extensao_arquivo(arquivo)
    if ext_erro:
        flash(ext_erro, 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    try:
        arquivo_bytes = arquivo.read()
        if len(arquivo_bytes) == 0:
            flash('O arquivo está vazio.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        token = gerar_token_sei_admin()
        if not token:
            flash('Não foi possível autenticar no SEI.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=itinerario.sei_protocolo,
            arquivo_bytes=arquivo_bytes,
            nome_arquivo=arquivo.filename,
            descricao=f'Autorização SOLICITAÇÃO APROVADA SCDP{" (" + numero_scdp + ")" if numero_scdp else ""}',
            id_serie=ID_SERIE_AUTORIZACAO_SCDP,
            numero=numero_scdp or None,
        )

        if retorno:
            itinerario.set_doc('autorizacao_scdp',
                               sei_id=str(retorno.get('IdDocumento', '')),
                               sei_formatado=retorno.get('DocumentoFormatado', ''))
            db.session.commit()
            flash('Autorização SCDP enviada ao SEI com sucesso!', 'success')
        else:
            flash('Erro ao enviar documento ao SEI.', 'danger')

    except Exception as e:
        flash(f'Erro ao enviar Autorização SCDP: {e}', 'danger')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


@financeiro_bp.route('/diarias/<int:id>/inserir-nota-empenho', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_nota_empenho(id):
    """Insere Nota de Empenho individual para um servidor da solicitação.

    Form espera:
      - item_itinerario_id: ID do servidor (diarias_itens_itinerario.id)
      - nota_empenho_codigo: código da NE (ex: '210101-2026NE00456')
      - valor_ne (opcional): valor empenhado
      - arquivo_ne (opcional): PDF da NE para upload ao SEI

    Cada servidor recebe sua própria NE — mesmo padrão da NR. Só quando
    TODOS os servidores têm NE cadastrada é que o marcador agregado
    `nota_empenho` em DiariasDocumentoSei é gravado (para compat com
    timeline/auditoria/guards existentes).
    """
    from app.models.diaria import DiariasNotaEmpenho, DiariasItemItinerario

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: precisa ter processo SEI
    if not itinerario.sei_id_procedimento:
        flash('Esta solicitação não possui processo SEI vinculado.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Guard: etapa mínima — NE só pode ser inserida a partir da Análise 2ª Parte.
    # Usa ordem cronológica real: ANALISE_SOLICITACAO_2(ID=6) tem ordem=3.
    if not etapa_diaria_em_ou_apos(itinerario.etapa_atual_id, DiariasEtapaID.ANALISE_SOLICITACAO_2):
        flash(
            'A Nota de Empenho só pode ser inserida a partir da Análise 2ª Parte (Etapa 6).',
            'warning',
        )
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Guard: despacho_sga deve estar gerado antes da NE
    if not itinerario.has_doc('despacho_sga'):
        flash(
            'O Despacho SGA deve ser gerado antes da Nota de Empenho.',
            'warning',
        )
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    item_id_raw = request.form.get('item_itinerario_id', '').strip()
    codigo_ne = request.form.get('nota_empenho_codigo', '').strip()
    valor_raw = request.form.get('valor_ne', '').strip()
    arquivo = request.files.get('arquivo_ne')

    if not item_id_raw:
        flash('Servidor obrigatório para inserir NE.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))
    if not codigo_ne:
        flash('O código da Nota de Empenho é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    try:
        item_id = int(item_id_raw)
    except ValueError:
        flash('Servidor inválido.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Valida que o servidor pertence a este itinerário
    item = DiariasItemItinerario.query.filter_by(
        id=item_id, id_itinerario=itinerario.id
    ).first()
    if not item:
        flash('Servidor não pertence a esta solicitação.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # UPSERT da NE por servidor
    ne = DiariasNotaEmpenho.query.filter_by(
        itinerario_id=itinerario.id, item_itinerario_id=item_id
    ).first()

    # Captura se a NE já tinha documento SEI vinculado ANTES de qualquer mudança.
    # Usado para prevenir re-upload do PDF em duplo-clique.
    ja_tinha_sei_id = bool(ne and ne.sei_id)

    valor_decimal = _parse_valor_brl(valor_raw)
    if ne:
        ne.codigo = codigo_ne
        if valor_decimal is not None:
            ne.valor = valor_decimal
    else:
        ne = DiariasNotaEmpenho(
            itinerario_id=itinerario.id,
            item_itinerario_id=item_id,
            codigo=codigo_ne,
            valor=valor_decimal,
        )
        db.session.add(ne)

    db.session.commit()

    # Upload ao SEI (se arquivo fornecido e ainda não há sei_id — evita duplo-upload).
    sei_upload_ok = False
    if ja_tinha_sei_id and arquivo and arquivo.filename:
        flash(
            f'Aviso: já existe documento SEI vinculado a esta NE ({ne.sei_formatado}). '
            'O arquivo enviado foi ignorado para evitar duplicata.',
            'info',
        )
    try:
        token = gerar_token_sei_admin()
        if not token:
            flash('Não foi possível autenticar no SEI.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        if not ja_tinha_sei_id:
            from app.services.diarias_sei_integration import UNIDADE_CCDP, ID_SERIE_NOTA_EMPENHO
            # Enviar processo para CCDP antes de criar NE (idempotente para múltiplas NEs)
            enviar_procedimento(token, sei_protocolo, [UNIDADE_CCDP], manter_aberto=True)

            if arquivo and arquivo.filename:
                arquivo_bytes = arquivo.read()
                nome_arquivo = arquivo.filename
            else:
                # Fallback: gera HTML mínimo se nenhum arquivo foi enviado
                html = f'<html><body><p>Nota de Empenho: {codigo_ne}</p></body></html>'
                arquivo_bytes = html.encode('utf-8')
                # UG_CODE (210101) vai só no nome do arquivo — o campo `codigo`
                # da NE armazena apenas o código bruto (ex: '2026NE00456').
                from app.constants import UG_CODE
                nome_arquivo = f'{UG_CODE}_NE_{codigo_ne}.html'

            retorno = adicionar_documento_externo(
                token=token,
                protocolo_formatado=sei_protocolo,
                arquivo_bytes=arquivo_bytes,
                nome_arquivo=nome_arquivo,
                descricao=f'Nota de Empenho {codigo_ne} — {item.nome_pessoa or ""}',
                id_serie=ID_SERIE_NOTA_EMPENHO,
                numero=codigo_ne,
                unidade_id=UNIDADE_CCDP,
            )

            if retorno:
                ne.sei_id = str(retorno.get('IdDocumento', ''))
                ne.sei_formatado = retorno.get('DocumentoFormatado', '')
                db.session.commit()
                sei_upload_ok = True
            else:
                flash('Aviso: NE salva, mas o envio ao SEI falhou.', 'warning')
    except Exception as e:
        flash(f'Aviso: NE salva, mas erro ao enviar documento ao SEI: {e}', 'warning')

    # Marcador agregado: só dispara quando TODOS os servidores têm NE com sei_id confirmado.
    total_servidores = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
    total_nes = DiariasNotaEmpenho.query.filter_by(itinerario_id=itinerario.id).filter(
        DiariasNotaEmpenho.sei_id.isnot(None)
    ).count()
    if total_nes >= total_servidores > 0:
        itinerario.set_doc('nota_empenho', codigo=codigo_ne)
        db.session.commit()

    msg_sei = ' e documento enviado ao SEI' if sei_upload_ok else ''
    flash(
        f'NE {codigo_ne} inserida para {item.nome_pessoa or "servidor"}{msg_sei}. '
        f'({total_nes}/{total_servidores} servidores com NE)',
        'success',
    )

    try:
        DiariasNotifier.notificar_etapa(itinerario, 'nota_empenho', current_user.id)
    except Exception as exc_notif:
        current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


@financeiro_bp.route('/diarias/<int:id>/despacho-ccdp', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despacho_ccdp(id):
    """Gera Despacho CCDP (idSerie 754) após Nota de Empenho — envia para SGA.

    O usuário fornece suas credenciais SEI para assinar o documento.
    O despacho é criado na unidade CCDP e assinado pelo usuário financeiro,
    depois o processo é enviado para a caixa APOIOSGA.
    """
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guards — exige 1 NE por servidor com upload confirmado no SEI (sei_id preenchido).
    from app.models.diaria import DiariasNotaEmpenho, DiariasItemItinerario
    total_servidores_g = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
    total_nes_g = DiariasNotaEmpenho.query.filter_by(itinerario_id=itinerario.id).filter(
        DiariasNotaEmpenho.sei_id.isnot(None)
    ).count()
    if total_servidores_g == 0 or total_nes_g < total_servidores_g:
        return jsonify({
            'sucesso': False,
            'erro': f'Todas as Notas de Empenho precisam ser inseridas antes do despacho '
                    f'({total_nes_g}/{total_servidores_g} servidores com NE).',
        }), 400

    doc_ccdp = itinerario.get_doc('despacho_ccdp')
    if doc_ccdp and doc_ccdp.sei_id:
        if doc_ccdp.assinado:
            # Doc criado + assinado — double-click, apenas informa
            return jsonify({
                'sucesso': True, 'ja_existe': True,
                'mensagem': 'O Despacho CCDP já foi gerado e assinado.',
                'documento_formatado': doc_ccdp.sei_formatado or '',
            })
        # Doc criado mas assinatura falhou — redireciona para retry
        return jsonify({
            'sucesso': True,
            'pendente_assinatura': True,
            'documento_formatado': doc_ccdp.sei_formatado or '',
            'id_documento': doc_ccdp.sei_id,
            'mensagem': 'Despacho já criado mas pendente de assinatura. Use o botão "Realizar Assinatura".',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Não há processo SEI vinculado.'}), 400

    # Parse credenciais SEI do body JSON
    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não fornecidos.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or current_user.cargo or ''

    # Autenticar (bypass ativo aceita vazio) — passa protocolo para bypass por processo
    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''
    auth = autenticar_usuario_sei(
        sei_usuario or 'bypass', sei_senha or 'bypass',
        protocolo_bypass=protocolo_proc,
    )

    try:
        # Token admin para criar o documento
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        # Enviar processo para CCDP
        enviar_procedimento(token_admin, itinerario.sei_protocolo or itinerario.n_processo or '', [UNIDADE_CCDP], manter_aberto=True)

        # 3. Gerar o Despacho CCDP (série 754) — assinatura dinâmica do usuário logado
        retorno_doc = gerar_despacho_ccdp(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
            itinerario=itinerario,
            nome_assinante=current_user.nome.upper() if current_user.nome else None,
            cargo_assinante=cargo,
        )

        if not retorno_doc:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar documento de despacho no SEI.'}), 500

        doc_id = str(retorno_doc.get('IdDocumento', ''))
        doc_formatado = retorno_doc.get('DocumentoFormatado', '')

        # 4. Assinar o documento com credenciais do usuário
        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_CCDP,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )

        if not resultado_assinatura or not resultado_assinatura.get('sucesso'):
            # Doc criado mas não assinado — salvar referência para permitir retry
            erro_assinatura = resultado_assinatura.get('erro', 'Erro desconhecido') if resultado_assinatura else 'Sem resposta'
            itinerario.set_doc('despacho_ccdp', sei_id=doc_id, sei_formatado=doc_formatado)
            db.session.commit()
            # NÃO notifica — o processo ainda não avançou
            return jsonify({
                'sucesso': True,
                'pendente_assinatura': True,
                'documento_formatado': doc_formatado,
                'id_documento': doc_id,
                'aviso': f'Documento gerado mas assinatura falhou: {erro_assinatura}. Use o botão "Realizar Assinatura" para completar.',
            })

        # 5. Enviar procedimento para APOIOSGA
        envio = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=itinerario.sei_protocolo,
            unidades_destino=[UNIDADE_APOIOSGA],
            unidade_origem=UNIDADE_CCDP,
        )

        # 6. Salvar referências no banco (assinado=True pois assinatura OK)
        itinerario.set_doc('despacho_ccdp', sei_id=doc_id, sei_formatado=doc_formatado, assinado=True)

        # 7. Despacho CCDP é ação interna da etapa Análise — não avança etapa principal

        db.session.commit()

        aviso_notif = ''
        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_ccdp', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')
            aviso_notif = 'Notificação não enviada — informe a equipe manualmente.'

        resp = {
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'envio_procedimento': envio,
        }
        if aviso_notif:
            resp['aviso'] = aviso_notif
        return jsonify(resp)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# ── Despacho APOIO/DFIN (Superintendente, pós Análise NCI) ─────────────────


@financeiro_bp.route('/diarias/<int:id>/confirmar-analise-nci', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def confirmar_analise_nci(id):
    """Etapa Análise NCI: VERIFICA documentos no SEI e gera despachos automáticos.

    Verificações no processo SEI:
    1. Existe documento IdSerie 461 (SINCIN Análise de Pagamento)?
    2. O SINCIN tem 2+ assinaturas (Agente Técnico + Coordenador)?
    3. Existe documento IdSerie 5 (Despacho NCI)?

    Se OK → gera automaticamente:
    - Despacho APOIO→DFIN (754) na DFIN/APOIO
    - Despacho DFIN→GEO (754) na DFIN/APOIO
    - Despacho GEO→CCDP (754) na GEO
    - Envia processo para CCDP
    """
    from app.services.diarias_sei_integration import (
        consultar_documentos_procedimento,
        gerar_despacho_apoio, gerar_despacho_diretor, gerar_despacho_geo,
        UNIDADE_CCDP, UNIDADE_GEO,
        ID_SERIE_ANALISE_PAGAMENTO, ID_SERIE_DESPACHO_NCI,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.has_doc('despacho_sga'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho SGA→NCI deve ser gerado primeiro.'}), 400

    if itinerario.has_doc('analise_pagamento') and itinerario.has_doc('despacho_diretor'):
        return jsonify({'sucesso': True, 'ja_existe': True, 'mensagem': 'Análise NCI já verificada e despachos gerados.'})

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Sem processo SEI vinculado.'}), 400

    sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        # 1. Consultar documentos do processo no SEI
        resp_docs = consultar_documentos_procedimento(sei_protocolo)
        if not resp_docs.get('sucesso'):
            return jsonify({'sucesso': False, 'erro': f'Erro ao consultar SEI: {resp_docs.get("erro", "")}'}), 500

        documentos = resp_docs.get('documentos', [])

        # 2. Verificar SINCIN (IdSerie 461)
        doc_sincin = None
        for doc in documentos:
            if str(doc.get('Serie', {}).get('IdSerie', '')) == ID_SERIE_ANALISE_PAGAMENTO:
                doc_sincin = doc
                break

        if not doc_sincin:
            return jsonify({
                'sucesso': False,
                'erro': 'Documento SINCIN Análise de Pagamento (IdSerie 461) não encontrado no processo SEI. O NCI deve criar este documento no SEI primeiro.',
            }), 400

        # 3. Verificar assinaturas do SINCIN (mínimo 2)
        assinaturas = doc_sincin.get('Assinaturas', [])
        if len(assinaturas) < 2:
            nomes = [a.get('Nome', '?') for a in assinaturas]
            return jsonify({
                'sucesso': False,
                'erro': f'O SINCIN possui apenas {len(assinaturas)} assinatura(s): {", ".join(nomes)}. São necessárias 2 (Agente Técnico + Coordenador NCI).',
            }), 400

        # 4. Verificar Despacho NCI (IdSerie 5)
        doc_despacho_nci = None
        for doc in documentos:
            if str(doc.get('Serie', {}).get('IdSerie', '')) == ID_SERIE_DESPACHO_NCI:
                doc_despacho_nci = doc
                break

        if not doc_despacho_nci:
            return jsonify({
                'sucesso': False,
                'erro': 'Despacho NCI (IdSerie 5) não encontrado no processo SEI. O NCI deve gerar o despacho no SEI primeiro.',
            }), 400

        # ══ TODAS AS VERIFICAÇÕES OK — salvar docs e gerar despachos ══

        # Salvar SINCIN e Despacho NCI no banco
        itinerario.set_doc('analise_pagamento',
            sei_id=str(doc_sincin.get('IdDocumento', '')),
            sei_formatado=doc_sincin.get('DocumentoFormatado', ''))

        itinerario.set_doc('despacho_nci',
            sei_id=str(doc_despacho_nci.get('IdDocumento', '')),
            sei_formatado=doc_despacho_nci.get('DocumentoFormatado', ''))

        sincin_fmt = doc_sincin.get('DocumentoFormatado', '')
        docs_gerados = [f'SINCIN verificado: {sincin_fmt}', f'Despacho NCI verificado: {doc_despacho_nci.get("DocumentoFormatado", "")}']

        # Gerar 3 despachos automáticos
        token = gerar_token_sei_admin()
        if token:
            # Despacho APOIO→DFIN
            enviar_procedimento(token, sei_protocolo, [UNIDADE_DFIN_APOIO], manter_aberto=True)
            r_apoio = gerar_despacho_apoio(
                token=token, id_procedimento=itinerario.sei_id_procedimento,
                sei_protocolo=sei_protocolo, ref_analise_nci_id=sincin_fmt, itinerario=itinerario)
            if r_apoio:
                docs_gerados.append(f'Despacho APOIO: {r_apoio.get("DocumentoFormatado", "")}')

            # Despacho DFIN→GEO
            r_dir = gerar_despacho_diretor(
                token=token, id_procedimento=itinerario.sei_id_procedimento,
                sei_protocolo=sei_protocolo,
                ref_despacho_apoio_id=r_apoio.get('DocumentoFormatado', '') if r_apoio else '',
                itinerario=itinerario)
            if r_dir:
                itinerario.set_doc('despacho_diretor',
                    sei_id=str(r_dir.get('IdDocumento', '')),
                    sei_formatado=r_dir.get('DocumentoFormatado', ''))
                docs_gerados.append(f'Despacho Diretor: {r_dir.get("DocumentoFormatado", "")}')

            # Despacho GEO→CCDP
            enviar_procedimento(token, sei_protocolo, [UNIDADE_GEO], manter_aberto=True)
            r_geo = gerar_despacho_geo(
                token=token, id_procedimento=itinerario.sei_id_procedimento,
                sei_protocolo=sei_protocolo, itinerario=itinerario)
            if r_geo:
                itinerario.set_doc('despacho_geo',
                    sei_id=str(r_geo.get('IdDocumento', '')),
                    sei_formatado=r_geo.get('DocumentoFormatado', ''))
                docs_gerados.append(f'Despacho GEO: {r_geo.get("DocumentoFormatado", "")}')

            # Enviar para CCDP
            enviar_procedimento(token, sei_protocolo, [UNIDADE_CCDP], manter_aberto=True)

        # Avançar etapa → Concessão de Diárias (todos os despachos NCI concluídos)
        if itinerario.etapa_atual_id == DiariasEtapaID.ANALISE_SOLICITACAO_2:
            DiariaService.registrar_movimentacao(
                id_itinerario=id,
                etapa_nova_id=DiariasEtapaID.CONCESSAO_DIARIAS,
                usuario_id=current_user.id,
                comentario='Análise NCI concluída e despachos gerados. Etapa avançada para Concessão de Diárias.',
                auto_commit=False,
            )

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'analise_nci', current_user.id)
        except Exception:
            pass

        assinantes = [f'{a.get("Nome", "?")} ({a.get("CargoFuncao", "")})' for a in assinaturas]
        return jsonify({
            'sucesso': True,
            'assinantes': assinantes,
            'mensagem': f'Análise NCI verificada com sucesso! Assinantes: {", ".join(assinantes)}. {len(docs_gerados)} documentos processados.',
        })

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'[DIARIAS-FIN] Erro na análise NCI: {e}')
        return jsonify({'sucesso': False, 'erro': f'Erro: {str(e)}'}), 500


@financeiro_bp.route('/diarias/<int:id>/despacho-apoio', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despacho_apoio(id):
    """Gera Despacho SGA (idSerie 2987) — encaminha ao NCI.

    Referencia o despacho CCDP e encaminha os autos para análise do NCI.
    Criado na unidade APOIOSGA, enviado para NCI.
    """
    from app.services.diarias_sei_integration import gerar_despacho_sga, UNIDADE_APOIOSGA, UNIDADE_NCI

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guards
    if not itinerario.has_doc('despacho_ccdp'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho CCDP deve ser gerado primeiro.'}), 400

    doc_apoio = itinerario.get_doc('despacho_sga')
    if doc_apoio and doc_apoio.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho SGA já foi gerado.',
            'documento_formatado': doc_apoio.sei_formatado or '',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Não há processo SEI vinculado.'}), 400

    dados = request.get_json() or {}

    try:
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        # Enviar processo para APOIOSGA
        enviar_procedimento(token_admin, sei_protocolo, [UNIDADE_APOIOSGA], manter_aberto=True)

        # Referência ao despacho CCDP
        doc_ccdp = itinerario.get_doc('despacho_ccdp')
        ref_ccdp_id = (doc_ccdp.sei_id or '') if doc_ccdp else ''
        ref_ccdp_fmt = (doc_ccdp.sei_formatado or '') if doc_ccdp else ''

        retorno_doc = gerar_despacho_sga(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            ref_despacho_ccdp_id=ref_ccdp_id,
            ref_despacho_ccdp_formatado=ref_ccdp_fmt,
            nome_assinante=current_user.nome.upper() if current_user.nome else None,
            cargo_assinante=current_user.cargo or 'Superintendente de Gestão Administrativa – SEAD',
            itinerario=itinerario,
        )

        if not retorno_doc:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar Despacho SGA no SEI.'}), 500

        doc_id = str(retorno_doc.get('IdDocumento', ''))
        doc_formatado = retorno_doc.get('DocumentoFormatado', '')

        # Enviar processo para NCI (origem = APOIOSGA, onde o despacho foi criado).
        # CRÍTICO: sem unidade_origem correta, o NCI nao recebe o processo na sua
        # caixa e nao consegue criar o SINCIN/Despacho NCI exigidos pela etapa
        # 'Analise NCI' (que faz consultar_documentos_procedimento).
        envio_nci = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=sei_protocolo,
            unidades_destino=[UNIDADE_NCI],
            unidade_origem=UNIDADE_APOIOSGA,
            manter_aberto=True,
        )
        if not envio_nci.get('sucesso'):
            current_app.logger.warning(
                f'[DIARIAS-FIN] Falha ao enviar processo para NCI: {envio_nci.get("erro")}'
            )

        # Salvar como 'despacho_sga' — nome canonico (IdSerie 2987).
        # O label 'despacho_apoio' no banco e reservado para o despacho APOIO/DFIN
        # pos-NCI (IdSerie 754), salvo automaticamente na etapa Analise NCI.
        itinerario.set_doc('despacho_sga', sei_id=doc_id, sei_formatado=doc_formatado)
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_sga', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'mensagem': f'Despacho SGA gerado ({doc_formatado}) e processo encaminhado ao NCI.',
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# ── Despacho Diretor DFIN → GEO ───────────────────────────────────────────


@financeiro_bp.route('/diarias/<int:id>/despacho-diretor', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despacho_diretor(id):
    """Gera Despacho do Diretor DFIN (idSerie 754) — encaminha para GEO.

    O Diretor de Planejamento e Financas ve o despacho do Superintendente,
    confirma ciencia e gera seu despacho encaminhando a GEO.
    """
    if not usuario_tem_caixa(CAIXA_DFIN_APOIO):
        return jsonify({'sucesso': False, 'erro': 'Voce nao tem acesso a caixa APOIO/DFIN.'}), 403

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guards — precisa do despacho SGA (que foi enviado ao NCI) antes do Diretor
    if not itinerario.has_doc('despacho_sga'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho SGA ainda nao foi gerado.'}), 400

    doc_diretor = itinerario.get_doc('despacho_diretor')
    if doc_diretor and doc_diretor.sei_id and doc_diretor.assinado:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho do Diretor já foi gerado.',
            'documento_formatado': doc_diretor.sei_formatado or '',
        })
    if doc_diretor and doc_diretor.sei_id and not doc_diretor.assinado:
        return jsonify({
            'sucesso': True,
            'pendente_assinatura': True,
            'documento_formatado': doc_diretor.sei_formatado or '',
            'id_documento': doc_diretor.sei_id,
            'mensagem': 'Despacho já criado mas pendente de assinatura. Use o botão "Realizar Assinatura".',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Nao ha processo SEI vinculado.'}), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados nao fornecidos.'}), 400

    if not dados.get('ciencia'):
        return jsonify({'sucesso': False, 'erro': 'E necessario confirmar a ciencia do despacho.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Diretor de Planejamento e Financas'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI sao obrigatorias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticacao SEI.'}), 401

        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        doc_apoio_ref = itinerario.get_doc('despacho_sga')
        ref_apoio = (doc_apoio_ref.sei_formatado or doc_apoio_ref.sei_id or '') if doc_apoio_ref else ''

        retorno_doc = gerar_despacho_diretor(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            ref_despacho_apoio_id=ref_apoio,
            itinerario=itinerario,
            nome_assinante=current_user.nome.upper() if current_user.nome else None,
            cargo_assinante=cargo,
        )

        if not retorno_doc:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar Despacho do Diretor no SEI.'}), 500

        doc_id = str(retorno_doc.get('IdDocumento', ''))
        doc_formatado = retorno_doc.get('DocumentoFormatado', '')

        # Assinar
        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_DFIN_APOIO,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )

        if not resultado_assinatura or not resultado_assinatura.get('sucesso'):
            # Doc criado mas não assinado — salvar referência para permitir retry.
            # ciencia_diretor permanece False até a assinatura ser confirmada.
            erro_assinatura = resultado_assinatura.get('erro', 'Erro desconhecido') if resultado_assinatura else 'Sem resposta'
            itinerario.set_doc('despacho_diretor', sei_id=doc_id, sei_formatado=doc_formatado, assinado=False)
            db.session.commit()
            return jsonify({
                'sucesso': True,
                'pendente_assinatura': True,
                'documento_formatado': doc_formatado,
                'id_documento': doc_id,
                'aviso': f'Documento gerado mas assinatura falhou: {erro_assinatura}. Use o botão "Realizar Assinatura" para completar.',
            })

        # Enviar para GEO
        envio = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=itinerario.sei_protocolo,
            unidades_destino=[UNIDADE_GEO],
            unidade_origem=UNIDADE_DFIN_APOIO,
        )

        # Salvar
        itinerario.ciencia_diretor = True
        itinerario.ciencia_diretor_data = datetime.now()
        itinerario.set_doc('despacho_diretor', sei_id=doc_id, sei_formatado=doc_formatado, assinado=True)

        # Despacho Diretor é ação interna — não avança etapa principal

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_diretor', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        resultado = {
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'envio_procedimento': envio,
        }
        return jsonify(resultado)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# ── Despacho GEO → CCDP ──────────────────────────────────────────────────


@financeiro_bp.route('/diarias/<int:id>/despacho-geo', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despacho_geo(id):
    """Gera Despacho GEO (idSerie 754) — encaminha para CCDP.

    O Gerente de Execucao Orcamentaria ve o despacho do Diretor,
    confirma ciencia, gera o despacho e encaminha a CCDP.
    Apos isso, libera os inputs de NL, PD e OB sequencialmente.
    """
    if not usuario_tem_caixa(CAIXA_GEO):
        return jsonify({'sucesso': False, 'erro': 'Voce nao tem acesso a caixa GEO.'}), 403

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guards
    if not itinerario.has_doc('despacho_diretor'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho do Diretor ainda nao foi gerado.'}), 400

    doc_geo = itinerario.get_doc('despacho_geo')
    if doc_geo and doc_geo.sei_id and doc_geo.assinado:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho GEO já foi gerado.',
            'documento_formatado': doc_geo.sei_formatado or '',
        })
    if doc_geo and doc_geo.sei_id and not doc_geo.assinado:
        return jsonify({
            'sucesso': True,
            'pendente_assinatura': True,
            'documento_formatado': doc_geo.sei_formatado or '',
            'id_documento': doc_geo.sei_id,
            'mensagem': 'Despacho já criado mas pendente de assinatura. Use o botão "Realizar Assinatura".',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Nao ha processo SEI vinculado.'}), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados nao fornecidos.'}), 400

    if not dados.get('ciencia'):
        return jsonify({'sucesso': False, 'erro': 'E necessario confirmar a ciencia do despacho.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Gerente de Execucao Orcamentaria'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI sao obrigatorias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticacao SEI.'}), 401

        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        retorno_doc = gerar_despacho_geo(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            itinerario=itinerario,
            nome_assinante=current_user.nome.upper() if current_user.nome else None,
            cargo_assinante=cargo,
        )

        if not retorno_doc:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar Despacho GEO no SEI.'}), 500

        doc_id = str(retorno_doc.get('IdDocumento', ''))
        doc_formatado = retorno_doc.get('DocumentoFormatado', '')

        # Assinar
        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_GEO,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )

        if not resultado_assinatura or not resultado_assinatura.get('sucesso'):
            # Doc criado mas não assinado — salvar referência para permitir retry.
            # ciencia_geo permanece False até a assinatura ser confirmada.
            erro_assinatura = resultado_assinatura.get('erro', 'Erro desconhecido') if resultado_assinatura else 'Sem resposta'
            itinerario.set_doc('despacho_geo', sei_id=doc_id, sei_formatado=doc_formatado, assinado=False)
            db.session.commit()
            return jsonify({
                'sucesso': True,
                'pendente_assinatura': True,
                'documento_formatado': doc_formatado,
                'id_documento': doc_id,
                'aviso': f'Documento gerado mas assinatura falhou: {erro_assinatura}. Use o botão "Realizar Assinatura" para completar.',
            })

        # Enviar para CCDP
        envio = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=itinerario.sei_protocolo,
            unidades_destino=[UNIDADE_CCDP],
            unidade_origem=UNIDADE_GEO,
        )

        # Salvar
        itinerario.ciencia_geo = True
        itinerario.ciencia_geo_data = datetime.now()
        itinerario.set_doc('despacho_geo', sei_id=doc_id, sei_formatado=doc_formatado, assinado=True)

        # Despacho GEO é ação interna — não avança etapa principal

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_geo', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        resultado = {
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'envio_procedimento': envio,
        }
        return jsonify(resultado)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# ── NL / PD / OB (inserção sequencial após Despacho GEO) ─────────────────


def _inserir_doc_financeiro_servidor(itinerario, modelo, tipo_agregado, id_serie,
                                       descricao_prefixo, notificacao_evento,
                                       on_completo=None):
    """Helper compartilhado: UPSERT de NL/PD/OB por servidor.

    Cada servidor (DiariasItemItinerario) tem sua propria NL/PD/OB no banco e
    no SEI. Mesmo padrao de inserir_nr/inserir_nota_empenho.

    Args:
        itinerario: objeto DiariasItinerario.
        modelo: classe do model (DiariasNotaLiquidacao/etc).
        tipo_agregado: 'nl' | 'pd' | 'ob' (label em DiariasDocumentoSei para
                       marcar agregado quando todos servidores estao OK).
        id_serie: IdSerie SEI do documento (ID_SERIE_NL/PD/OB).
        descricao_prefixo: prefixo da Descricao SEI (ex: 'NL').
        notificacao_evento: evento passado a DiariasNotifier.

    Returns:
        flask Response (redirect com flash).
    """
    # DiariasItemItinerario ja importado no topo do modulo (linha 12).
    # Import local redundante causava UnboundLocalError por shadowing de escopo.

    item_id_raw = request.form.get('item_itinerario_id', '').strip()
    codigo = request.form.get('codigo', '').strip()
    valor_raw = request.form.get('valor', '').strip()
    arquivo = request.files.get('arquivo')

    redirect_url = url_for('financeiro.diarias_detalhe', id=itinerario.id)

    if not item_id_raw:
        flash('Servidor obrigatorio.', 'danger')
        return redirect(redirect_url)
    if not codigo:
        flash(f'O codigo da {descricao_prefixo} e obrigatorio.', 'danger')
        return redirect(redirect_url)
    if not arquivo or not arquivo.filename:
        flash(f'O arquivo PDF da {descricao_prefixo} e obrigatorio.', 'danger')
        return redirect(redirect_url)

    try:
        item_id = int(item_id_raw)
    except ValueError:
        flash('Servidor invalido.', 'danger')
        return redirect(redirect_url)

    item = DiariasItemItinerario.query.filter_by(
        id=item_id, id_itinerario=itinerario.id
    ).first()
    if not item:
        flash('Servidor nao pertence a esta solicitacao.', 'danger')
        return redirect(redirect_url)

    # UPSERT
    registro = modelo.query.filter_by(
        itinerario_id=itinerario.id, item_itinerario_id=item_id
    ).first()
    ja_tinha_sei_id = bool(registro and registro.sei_id)

    valor_decimal = _parse_valor_brl(valor_raw)
    if registro:
        registro.codigo = codigo
        if valor_decimal is not None:
            registro.valor = valor_decimal
    else:
        registro = modelo(
            itinerario_id=itinerario.id,
            item_itinerario_id=item_id,
            codigo=codigo,
            valor=valor_decimal,
        )
        db.session.add(registro)

    db.session.commit()

    if ja_tinha_sei_id:
        flash(
            f'Aviso: ja existe documento SEI vinculado a esta {descricao_prefixo} '
            f'({registro.sei_formatado}). Arquivo enviado foi ignorado.',
            'info',
        )
        return redirect(redirect_url)

    # Upload ao SEI
    try:
        token = gerar_token_sei_admin()
        if not token:
            flash('Falha ao autenticar no SEI.', 'danger')
            return redirect(redirect_url)

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''
        arquivo_bytes = arquivo.read()

        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=sei_protocolo,
            arquivo_bytes=arquivo_bytes,
            nome_arquivo=arquivo.filename,
            descricao=f'{descricao_prefixo} {codigo} - {item.nome_pessoa or ""}',
            id_serie=id_serie,
            numero=codigo,
            unidade_id=UNIDADE_CCDP,
        )

        if not retorno:
            flash(f'Erro ao gerar {descricao_prefixo} no SEI.', 'danger')
            return redirect(redirect_url)

        registro.sei_id = str(retorno.get('IdDocumento', ''))
        registro.sei_formatado = retorno.get('DocumentoFormatado', '')
        db.session.commit()

        # Marcador agregado em DiariasDocumentoSei: so presente quando TODOS servidores OK
        total_serv = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
        total = modelo.query.filter_by(itinerario_id=itinerario.id).filter(
            modelo.sei_id.isnot(None)
        ).count()
        if total >= total_serv > 0:
            itinerario.set_doc(tipo_agregado, codigo=codigo)
            if on_completo:
                on_completo(itinerario)
            db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, notificacao_evento, current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        flash(
            f'{descricao_prefixo} {codigo} enviada ao SEI para {item.nome_pessoa or "servidor"} '
            f'({retorno.get("DocumentoFormatado", "")}). '
            f'({total}/{total_serv} servidores com {descricao_prefixo})',
            'success',
        )
        return redirect(redirect_url)

    except Exception as e:
        db.session.rollback()
        flash(f'Erro inesperado: {str(e)}', 'danger')
        return redirect(redirect_url)


@financeiro_bp.route('/diarias/<int:id>/inserir-nl', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_nl(id):
    """Insere NL - Nota de Liquidacao (idSerie 420) no processo SEI, 1 por servidor."""
    from app.models.diaria import DiariasNotaLiquidacao

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Esta solicitação não possui processo SEI vinculado.'}), 400

    # Guard: ordem cronológica real — NL só a partir de CONCESSAO_DIARIAS (ordem=4).
    # CONCESSAO_DIARIAS(ID=4) vem depois de ANALISE_SOLICITACAO_2(ID=6) na ordem real.
    if not etapa_diaria_em_ou_apos(itinerario.etapa_atual_id, DiariasEtapaID.CONCESSAO_DIARIAS):
        return jsonify({'sucesso': False, 'erro': 'A solicitação ainda não está na etapa de Concessão de Diárias.'}), 400

    if not itinerario.has_doc('despacho_geo'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho GEO deve ser gerado antes da NL.'}), 400

    return _inserir_doc_financeiro_servidor(
        itinerario=itinerario,
        modelo=DiariasNotaLiquidacao,
        tipo_agregado='nl',
        id_serie=ID_SERIE_NL,
        descricao_prefixo='NL',
        notificacao_evento='nl_inserida',
    )


@financeiro_bp.route('/diarias/<int:id>/inserir-pd', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_pd(id):
    """Insere PD - Programacao de Desembolso (idSerie 421) no processo SEI, 1 por servidor."""
    from app.models.diaria import DiariasProgramacaoDesembolso, DiariasNotaLiquidacao, DiariasItemItinerario

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Esta solicitação não possui processo SEI vinculado.'}), 400

    # Guard: ordem cronológica real — PD só a partir de CONCESSAO_DIARIAS (ordem=4).
    if not etapa_diaria_em_ou_apos(itinerario.etapa_atual_id, DiariasEtapaID.CONCESSAO_DIARIAS):
        return jsonify({
            'sucesso': False,
            'erro': 'A PD só pode ser inserida a partir da etapa de Concessão de Diárias.',
        }), 400

    # Guard: despacho_geo deve estar gerado antes da PD
    if not itinerario.has_doc('despacho_geo'):
        return jsonify({
            'sucesso': False,
            'erro': 'O Despacho GEO deve ser gerado antes da PD.',
        }), 400

    # Gate: todos servidores precisam ter NL com upload SEI confirmado (sei_id preenchido)
    total_serv = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
    total_nl = DiariasNotaLiquidacao.query.filter_by(itinerario_id=itinerario.id).filter(
        DiariasNotaLiquidacao.sei_id.isnot(None)
    ).count()
    if total_serv == 0 or total_nl < total_serv:
        return jsonify({
            'sucesso': False,
            'erro': f'Todas as NLs precisam ser inseridas antes das PDs ({total_nl}/{total_serv}).',
        }), 400

    return _inserir_doc_financeiro_servidor(
        itinerario=itinerario,
        modelo=DiariasProgramacaoDesembolso,
        tipo_agregado='pd',
        id_serie=ID_SERIE_PD,
        descricao_prefixo='PD',
        notificacao_evento='pd_inserida',
    )


@financeiro_bp.route('/diarias/<int:id>/inserir-ob', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_ob(id):
    """Insere OB - Ordem Bancaria (idSerie 422) no processo SEI, 1 por servidor.

    Quando a ultima OB e inserida (todos servidores OK), avanca etapa para
    Prestacao de Contas.
    """
    from app.models.diaria import (
        DiariasOrdemBancaria, DiariasProgramacaoDesembolso, DiariasItemItinerario,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Esta solicitação não possui processo SEI vinculado.'}), 400

    # Guard: ordem cronológica real — OB só a partir de CONCESSAO_DIARIAS (ordem=4).
    if not etapa_diaria_em_ou_apos(itinerario.etapa_atual_id, DiariasEtapaID.CONCESSAO_DIARIAS):
        return jsonify({
            'sucesso': False,
            'erro': 'A OB só pode ser inserida a partir da etapa de Concessão de Diárias.',
        }), 400

    # Guard: despacho_geo deve estar gerado antes da OB
    if not itinerario.has_doc('despacho_geo'):
        return jsonify({
            'sucesso': False,
            'erro': 'O Despacho GEO deve ser gerado antes da OB.',
        }), 400

    # Gate: todos servidores precisam ter PD com upload SEI confirmado (sei_id preenchido)
    total_serv = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
    total_pd = DiariasProgramacaoDesembolso.query.filter_by(itinerario_id=itinerario.id).filter(
        DiariasProgramacaoDesembolso.sei_id.isnot(None)
    ).count()
    if total_serv == 0 or total_pd < total_serv:
        return jsonify({
            'sucesso': False,
            'erro': f'Todas as PDs precisam ser inseridas antes das OBs ({total_pd}/{total_serv}).',
        }), 400

    def _avancar_etapa(itin):
        """Callback disparado quando todos servidores tem OB: avanca para
        PRESTACAO_CONTAS.

        Aceita como ponto de partida tanto CONCESSAO_DIARIAS (fluxo normal)
        quanto ANALISE_SOLICITACAO_2 (quando o avanco intermediario apos
        despachos pos-NCI nao ocorreu — por ex. geracao via script/bypass).
        Sem esse fallback, a etapa Concessao fica sem marca de 'atual' nem
        'concluida' na timeline, apesar de NL/PD/OB terem sido inseridos.
        """
        if itin.etapa_atual_id in (
            DiariasEtapaID.CONCESSAO_DIARIAS,
            DiariasEtapaID.ANALISE_SOLICITACAO_2,
        ):
            DiariaService.registrar_movimentacao(
                id_itinerario=itin.id,
                etapa_nova_id=DiariasEtapaID.PRESTACAO_CONTAS,
                usuario_id=current_user.id if current_user else None,
                comentario='Ultima OB inserida. Avancando para Prestacao de Contas.',
            )

    return _inserir_doc_financeiro_servidor(
        itinerario=itinerario,
        modelo=DiariasOrdemBancaria,
        tipo_agregado='ob',
        id_serie=ID_SERIE_OB,
        descricao_prefixo='OB',
        notificacao_evento='ob_inserida',
        on_completo=_avancar_etapa,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 11 - Prestação de Contas CCDP (NP + Prestação SCDP + Despacho Final)
# ══════════════════════════════════════════════════════════════════════════════

@financeiro_bp.route('/diarias/<int:id>/inserir-np', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_np(id):
    """Insere NP - Nota Patrimonial (idSerie 423) no processo SEI, 1 por servidor."""
    from app.models.diaria import DiariasNotaPatrimonial

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not usuario_tem_caixa(CAIXA_CCDP):
        flash('Acesso restrito a usuarios da CCDP.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if not itinerario.has_doc('comprovante_viagem'):
        flash('O comprovante de viagem deve ser enviado antes da NP.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    return _inserir_doc_financeiro_servidor(
        itinerario=itinerario,
        modelo=DiariasNotaPatrimonial,
        tipo_agregado='np',
        id_serie=ID_SERIE_NP,
        descricao_prefixo='NP',
        notificacao_evento='np_inserida',
    )


@financeiro_bp.route('/diarias/<int:id>/upload-prestacao-scdp', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def upload_prestacao_scdp(id):
    """Upload do Documento de Prestação SCDP (idSerie 264, externo) ao processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not usuario_tem_caixa(CAIXA_CCDP):
        flash('Acesso restrito a usuarios da CCDP.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if not itinerario.has_doc('np'):
        flash('A Nota Patrimonial (NP) deve ser inserida antes.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if itinerario.has_doc('prestacao_scdp'):
        flash('O Documento de Prestação SCDP já foi enviado.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    arquivo = request.files.get('arquivo_prestacao_scdp')
    if not arquivo or not arquivo.filename:
        flash('Selecione um arquivo PDF para enviar.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    ext_erro = _validar_extensao_arquivo(arquivo)
    if ext_erro:
        flash(ext_erro, 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    try:
        arquivo_bytes = arquivo.read()
        if not arquivo_bytes:
            flash('Arquivo vazio.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        token = gerar_token_sei_admin()
        if not token:
            flash('Falha na autenticação SEI.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=itinerario.sei_protocolo,
            arquivo_bytes=arquivo_bytes,
            nome_arquivo=arquivo.filename,
            descricao='Documento de Prestação SCDP',
            id_serie=ID_SERIE_PRESTACAO_SCDP,
        )

        if retorno:
            itinerario.set_doc('prestacao_scdp',
                               sei_id=str(retorno.get('IdDocumento', '')),
                               sei_formatado=retorno.get('DocumentoFormatado', ''))
            db.session.commit()
            try:
                DiariasNotifier.notificar_etapa(itinerario, 'prestacao_scdp', current_user.id)
            except Exception as exc_notif:
                current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')
            flash('Documento de Prestação SCDP enviado ao SEI com sucesso!', 'success')
        else:
            flash('Erro ao enviar documento ao SEI.', 'danger')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao enviar documento: {str(e)}', 'danger')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


@financeiro_bp.route('/diarias/<int:id>/despacho-final', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despacho_final(id):
    """Gera Despacho Final CCDP (idSerie 754) - 'Processo pago e concluído nesta unidade.'"""
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not usuario_tem_caixa(CAIXA_CCDP):
        return jsonify({'sucesso': False, 'erro': 'Acesso restrito a usuarios da CCDP.'}), 403

    if not itinerario.has_doc('prestacao_scdp'):
        return jsonify({'sucesso': False, 'erro': 'O Documento de Prestacao SCDP deve ser enviado antes.'}), 400

    doc_final = itinerario.get_doc('despacho_final')
    if doc_final and doc_final.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho Final já foi gerado.',
            'documento_formatado': doc_final.sei_formatado or '',
        })

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados nao fornecidos.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Auxiliar Técnica'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI sao obrigatorias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticacao SEI.'}), 401

        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        retorno_doc = gerar_despacho_final_ccdp(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            itinerario=itinerario,
            nome_assinante=current_user.nome.upper() if current_user.nome else None,
            cargo_assinante=cargo,
        )

        if not retorno_doc:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar Despacho Final no SEI.'}), 500

        doc_id = str(retorno_doc.get('IdDocumento', ''))
        doc_formatado = retorno_doc.get('DocumentoFormatado', '')

        # Assinar com credenciais do usuario
        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_CCDP,
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
        if not resultado_assinatura.get('sucesso'):
            aviso = f'Documento gerado mas assinatura falhou: {resultado_assinatura.get("erro", "")}'

        # Salvar
        itinerario.set_doc('despacho_final', sei_id=doc_id, sei_formatado=doc_formatado)

        # Despacho final — processo concluído (já na etapa 5 - Prestação de Contas)

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'processo_concluido', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        resp = {
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
        }
        if aviso:
            resp['aviso'] = aviso
        return jsonify(resp)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro ao gerar despacho final: {str(e)}'}), 500


# ── Retry assinatura Despacho CCDP (quando doc criado mas assinatura falhou) ──


@financeiro_bp.route('/diarias/<int:id>/assinar-despacho-ccdp', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def assinar_despacho_ccdp(id):
    """Retry de assinatura do Despacho CCDP quando o documento já foi criado no SEI
    mas a assinatura falhou anteriormente.

    Após assinatura bem-sucedida:
    1. Assina o documento existente
    2. Envia o procedimento para APOIOSGA
    3. Avança a etapa
    """
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: doc deve existir mas etapa não deve ter avançado
    doc_ccdp_retry = itinerario.get_doc('despacho_ccdp')
    if not doc_ccdp_retry or not doc_ccdp_retry.sei_id:
        return jsonify({'sucesso': False, 'erro': 'Nenhum despacho CCDP encontrado para assinar.'}), 400

    if doc_ccdp_retry.assinado:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho CCDP já foi assinado.',
            'documento_formatado': doc_ccdp_retry.sei_formatado or '',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Não há processo SEI vinculado.'}), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não fornecidos.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Auxiliar Técnica'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI são obrigatórias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        # 1. Autenticar usuário no SEI
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI. Verifique suas credenciais.'}), 401

        # 2. Assinar o documento existente
        doc_id = doc_ccdp_retry.sei_id
        doc_formatado = doc_ccdp_retry.sei_formatado or ''

        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_CCDP,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )

        if not resultado_assinatura or not resultado_assinatura.get('sucesso'):
            erro_txt = resultado_assinatura.get('erro', 'Erro desconhecido') if resultado_assinatura else 'Sem resposta'
            return jsonify({
                'sucesso': False,
                'erro': f'Assinatura falhou novamente: {erro_txt}',
            }), 500

        # 3. Token admin para enviar procedimento
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        # 4. Enviar procedimento para APOIOSGA
        envio = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=itinerario.sei_protocolo,
            unidades_destino=[UNIDADE_APOIOSGA],
            unidade_origem=UNIDADE_CCDP,
        )

        # 5. Marcar assinatura como concluída
        doc_ccdp_retry.assinado = True
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_ccdp', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'envio_procedimento': envio,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# ── Retry de assinatura: Diretor e GEO ───────────────────────────────────────


@financeiro_bp.route('/diarias/<int:id>/assinar-despacho-diretor', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def assinar_despacho_diretor(id):
    """Retry de assinatura do Despacho do Diretor quando o documento já foi criado
    no SEI mas a assinatura falhou anteriormente."""
    if not usuario_tem_caixa(CAIXA_DFIN_APOIO):
        return jsonify({'sucesso': False, 'erro': 'Voce nao tem acesso a caixa APOIO/DFIN.'}), 403

    itinerario = DiariasItinerario.query.get_or_404(id)

    doc_retry = itinerario.get_doc('despacho_diretor')
    if not doc_retry or not doc_retry.sei_id:
        return jsonify({'sucesso': False, 'erro': 'Nenhum despacho do Diretor encontrado para assinar.'}), 400

    if doc_retry.assinado:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho do Diretor já foi assinado.',
            'documento_formatado': doc_retry.sei_formatado or '',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Não há processo SEI vinculado.'}), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não fornecidos.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Diretor de Planejamento e Financas'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI são obrigatórias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI. Verifique suas credenciais.'}), 401

        doc_id = doc_retry.sei_id
        doc_formatado = doc_retry.sei_formatado or ''

        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_DFIN_APOIO,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )

        if not resultado_assinatura or not resultado_assinatura.get('sucesso'):
            erro_txt = resultado_assinatura.get('erro', 'Erro desconhecido') if resultado_assinatura else 'Sem resposta'
            return jsonify({'sucesso': False, 'erro': f'Assinatura falhou novamente: {erro_txt}'}), 500

        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        envio = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=itinerario.sei_protocolo,
            unidades_destino=[UNIDADE_GEO],
            unidade_origem=UNIDADE_DFIN_APOIO,
        )

        doc_retry.assinado = True
        itinerario.ciencia_diretor = True
        itinerario.ciencia_diretor_data = datetime.now()
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_diretor', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'envio_procedimento': envio,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


@financeiro_bp.route('/diarias/<int:id>/assinar-despacho-geo', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def assinar_despacho_geo(id):
    """Retry de assinatura do Despacho GEO quando o documento já foi criado
    no SEI mas a assinatura falhou anteriormente."""
    if not usuario_tem_caixa(CAIXA_GEO):
        return jsonify({'sucesso': False, 'erro': 'Voce nao tem acesso a caixa GEO.'}), 403

    itinerario = DiariasItinerario.query.get_or_404(id)

    doc_retry = itinerario.get_doc('despacho_geo')
    if not doc_retry or not doc_retry.sei_id:
        return jsonify({'sucesso': False, 'erro': 'Nenhum despacho GEO encontrado para assinar.'}), 400

    if doc_retry.assinado:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho GEO já foi assinado.',
            'documento_formatado': doc_retry.sei_formatado or '',
        })

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Não há processo SEI vinculado.'}), 400

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não fornecidos.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip() or 'Gerente de Execucao Orcamentaria'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI são obrigatórias.'}), 400

    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI. Verifique suas credenciais.'}), 401

        doc_id = doc_retry.sei_id
        doc_formatado = doc_retry.sei_formatado or ''

        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_GEO,
            dados_assinatura={
                'protocolo_doc': doc_id,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': sei_senha,
            }
        )

        if not resultado_assinatura or not resultado_assinatura.get('sucesso'):
            erro_txt = resultado_assinatura.get('erro', 'Erro desconhecido') if resultado_assinatura else 'Sem resposta'
            return jsonify({'sucesso': False, 'erro': f'Assinatura falhou novamente: {erro_txt}'}), 500

        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        envio = enviar_procedimento(
            token=token_admin,
            protocolo_procedimento=itinerario.sei_protocolo,
            unidades_destino=[UNIDADE_CCDP],
            unidade_origem=UNIDADE_GEO,
        )

        doc_retry.assinado = True
        itinerario.ciencia_geo = True
        itinerario.ciencia_geo_data = datetime.now()
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_geo', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
            'envio_procedimento': envio,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500
