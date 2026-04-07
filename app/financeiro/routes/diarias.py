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
from app.constants import DiariasEtapaID
from app.services.diaria_service import DiariaService
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, adicionar_documento_externo, gerar_quadro_orcamentario,
    gerar_nota_empenho, gerar_despacho_ccdp, enviar_procedimento,
    gerar_despacho_apoio, gerar_despacho_diretor, gerar_despacho_geo,
    gerar_nl, gerar_pd, gerar_ob, gerar_np, gerar_despacho_final_ccdp,
    ID_SERIE_AUTORIZACAO_SCDP, ID_SERIE_PRESTACAO_SCDP,
    UNIDADE_CCDP, UNIDADE_APOIOSGA, UNIDADE_DFIN_APOIO, UNIDADE_GEO,
)
from app.services.sei_auth import autenticar_usuario_sei
from app.services.sei_integration import assinar_documento
from app.utils.permissions import (
    requires_permission, usuario_tem_caixa,
    CAIXA_CCDP, CAIXA_DFIN_APOIO, CAIXA_GEO,
)
from app.services.diarias_notification import DiariasNotifier


@financeiro_bp.route('/diarias')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_lista():
    """Lista solicitações de diárias na etapa financeira ou posterior (etapa >= 2)."""
    busca = request.args.get('q', '').strip()
    filtro_status = request.args.get('status_nr', '')
    page = request.args.get('page', 1, type=int)

    query = DiariasItinerario.query.filter(
        DiariasItinerario.etapa_atual_id >= int(DiariasEtapaID.ANALISE_SOLICITACAO)
    )

    # Filtro de busca por processo SEI ou usuário gerador
    if busca:
        query = query.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    # Filtro por status da NR (via documentos_sei relationship)
    if filtro_status == 'pendente':
        query = query.filter(
            ~DiariasItinerario.documentos_sei.any(
                (DiariasDocumentoSei.tipo == 'nota_reserva') & (DiariasDocumentoSei.codigo.isnot(None))
            ),
            DiariasItinerario.etapa_atual_id == int(DiariasEtapaID.ANALISE_SOLICITACAO),
        )
    elif filtro_status == 'inserida':
        query = query.filter(
            DiariasItinerario.documentos_sei.any(
                (DiariasDocumentoSei.tipo == 'nota_reserva') & (DiariasDocumentoSei.codigo.isnot(None))
            ),
        )

    pagination = query.order_by(
        DiariasItinerario.data_solicitacao.desc()
    ).paginate(page=page, per_page=20, error_out=False)

    # Conta pessoas por itinerário para exibição
    itinerarios = pagination.items
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

    return render_template(
        'financeiro/diarias_lista.html',
        itinerarios=itinerarios,
        pagination=pagination,
        pessoas_count=pessoas_count,
        filtro_status=filtro_status,
    )


@financeiro_bp.route('/diarias/<int:id>')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_detalhe(id):
    """Exibe detalhes de uma solicitação de diária para o financeiro."""
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        abort(404)

    itinerario = dados['itinerario']

    # Só mostra se já chegou na análise (etapa >= 3)
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

    return render_template(
        'financeiro/diarias_detalhe.html',
        itinerario=itinerario,
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        timeline_data=timeline_data,
    )


@financeiro_bp.route('/diarias/<int:id>/inserir-nr', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_nr(id):
    """Insere Nota de Reserva em uma solicitação de diária."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: só permite inserção na etapa de Análise da Solicitação
    if itinerario.etapa_atual_id != DiariasEtapaID.ANALISE_SOLICITACAO:
        flash('Esta solicitação já possui Nota de Reserva ou não está na etapa correta.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    nr_code = request.form.get('nota_reserva', '').strip()
    arquivo = request.files.get('arquivo_nr')

    if not nr_code:
        flash('O código da Nota de Reserva é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Salva o código da NR
    itinerario.set_doc('nota_reserva', codigo=nr_code)

    # Upload do PDF ao SEI (se arquivo fornecido e processo SEI existe)
    sei_upload_ok = False
    if arquivo and arquivo.filename and itinerario.sei_protocolo:
        try:
            arquivo_bytes = arquivo.read()
            if len(arquivo_bytes) > 0:
                token = gerar_token_sei_admin()
                if token:
                    retorno = adicionar_documento_externo(
                        token=token,
                        protocolo_formatado=itinerario.sei_protocolo,
                        arquivo_bytes=arquivo_bytes,
                        nome_arquivo=arquivo.filename,
                        descricao=f'Nota de Reserva {nr_code}',
                    )
                    if retorno:
                        itinerario.set_doc('nota_reserva',
                                           sei_id=str(retorno.get('IdDocumento', '')),
                                           sei_formatado=retorno.get('DocumentoFormatado', ''),
                                           codigo=nr_code)
                        sei_upload_ok = True
                    else:
                        flash('Aviso: NR salva, mas o upload do documento ao SEI falhou.', 'warning')
                else:
                    flash('Aviso: NR salva, mas não foi possível autenticar no SEI.', 'warning')
        except Exception as e:
            flash(f'Aviso: NR salva, mas erro ao enviar documento ao SEI: {e}', 'warning')

    # NR é sub-item da etapa 3 (Análise da Solicitação) — não avança etapa principal

    if sei_upload_ok:
        flash(f'Nota de Reserva {nr_code} inserida e documento enviado ao SEI com sucesso!', 'success')
    elif not arquivo or not arquivo.filename:
        flash(f'Nota de Reserva {nr_code} inserida com sucesso!', 'success')

    try:
        DiariasNotifier.notificar_etapa(itinerario, 'nota_reserva', current_user.id)
    except Exception as exc_notif:
        current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

    return redirect(url_for('financeiro.diarias_lista'))


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
    """Insere Quadro Orçamentário em uma solicitação de diária (após NR)."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: NR deve estar inserida e quadro ainda não preenchido
    doc_nr = itinerario.get_doc('nota_reserva')
    if not doc_nr or not doc_nr.codigo:
        flash('A Nota de Reserva deve ser inserida antes do Quadro Orçamentário.', 'warning')
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
                    itinerario.set_doc('quadro_orcamentario',
                                       sei_id=str(retorno.get('IdDocumento', '')),
                                       sei_formatado=retorno.get('DocumentoFormatado', ''))
                    sei_ok = True
                else:
                    flash('Aviso: Quadro salvo, mas a geração do documento no SEI falhou.', 'warning')
            else:
                flash('Aviso: Quadro salvo, mas não foi possível autenticar no SEI.', 'warning')
        except Exception as e:
            flash(f'Aviso: Quadro salvo, mas erro ao gerar documento no SEI: {e}', 'warning')

    db.session.commit()

    if sei_ok:
        flash('Quadro Orçamentário inserido e documento gerado no SEI com sucesso!', 'success')
    elif not itinerario.sei_id_procedimento:
        flash('Quadro Orçamentário inserido com sucesso!', 'success')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


@financeiro_bp.route('/diarias/<int:id>/upload-scdp', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def upload_autorizacao_scdp(id):
    """Upload do PDF 'Autorização SOLICITAÇÃO APROVADA SCDP' ao processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

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
    """Cria documento Nota de Empenho (idSerie 419) no processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: precisa ter processo SEI
    if not itinerario.sei_id_procedimento:
        flash('Esta solicitação não possui processo SEI vinculado.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if itinerario.has_doc('nota_empenho'):
        flash('A Nota de Empenho já foi inserida para esta solicitação.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    codigo_ne = request.form.get('nota_empenho_codigo', '').strip()
    if not codigo_ne:
        flash('O código da Nota de Empenho é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Dados opcionais para enriquecer o documento
    dados_empenho = {
        'valor': request.form.get('ne_valor', '').strip() or None,
        'natureza_despesa': request.form.get('ne_natureza_despesa', '').strip() or None,
        'fonte_recursos': request.form.get('ne_fonte_recursos', '').strip() or None,
        'favorecido': request.form.get('ne_favorecido', '').strip() or None,
        'objeto': request.form.get('ne_objeto', '').strip() or None,
    }

    try:
        token = gerar_token_sei_admin()
        if not token:
            flash('Não foi possível autenticar no SEI.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        retorno = gerar_nota_empenho(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
            codigo_ne=codigo_ne,
            dados_empenho=dados_empenho,
        )

        if retorno:
            itinerario.set_doc('nota_empenho',
                               sei_id=str(retorno.get('IdDocumento', '')),
                               sei_formatado=retorno.get('DocumentoFormatado', ''),
                               codigo=codigo_ne)
            db.session.commit()
            try:
                DiariasNotifier.notificar_etapa(itinerario, 'nota_empenho', current_user.id)
            except Exception as exc_notif:
                current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')
            flash(f'Nota de Empenho {codigo_ne} inserida e documento gerado no SEI!', 'success')
        else:
            flash('Erro ao gerar documento de Nota de Empenho no SEI.', 'danger')

    except Exception as e:
        flash(f'Erro ao inserir Nota de Empenho: {e}', 'danger')

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

    # Guards
    if not itinerario.has_doc('nota_empenho'):
        return jsonify({'sucesso': False, 'erro': 'A Nota de Empenho deve ser inserida antes do despacho.'}), 400

    doc_ccdp = itinerario.get_doc('despacho_ccdp')
    if doc_ccdp and doc_ccdp.sei_id:
        # Se etapa já avançou, é double-click — apenas informa
        if itinerario.has_doc('despacho_ccdp'):
            return jsonify({
                'sucesso': True, 'ja_existe': True,
                'mensagem': 'O Despacho CCDP já foi gerado.',
                'documento_formatado': doc_ccdp.sei_formatado or '',
            })
        # Se doc existe MAS etapa não avançou → dead state (assinatura falhou antes)
        # Redireciona para retry
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
    cargo = dados.get('cargo', '').strip() or 'Auxiliar Técnica'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI são obrigatórias.'}), 400

    try:
        # 1. Autenticar usuário no SEI
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI. Verifique suas credenciais.'}), 401

        # 2. Token admin para criar o documento
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        # 3. Gerar o Despacho CCDP (série 754)
        retorno_doc = gerar_despacho_ccdp(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
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

        # 6. Salvar referências no banco
        itinerario.set_doc('despacho_ccdp', sei_id=doc_id, sei_formatado=doc_formatado)

        # 7. Despacho CCDP é ação interna da etapa Análise — não avança etapa principal

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


# ── Despacho APOIO/DFIN (Superintendente, pós Análise NCI) ─────────────────


@financeiro_bp.route('/diarias/<int:id>/despacho-apoio', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def despacho_apoio(id):
    """Gera Despacho APOIO/DFIN (idSerie 754) — Superintendente encaminha para DFIN.

    Após a Análise NCI, o Superintendente (caixa DFIN/APOIO) dá ciência,
    gera o despacho referenciando a análise NCI e encaminha ao Diretor DFIN.
    """
    if not usuario_tem_caixa(CAIXA_DFIN_APOIO):
        return jsonify({'sucesso': False, 'erro': 'Voce nao tem acesso a caixa APOIO/DFIN.'}), 403

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guards
    if not itinerario.has_doc('analise_pagamento'):
        return jsonify({'sucesso': False, 'erro': 'A Analise NCI ainda nao foi gerada.'}), 400

    doc_apoio = itinerario.get_doc('despacho_apoio')
    if doc_apoio and doc_apoio.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho APOIO já foi gerado.',
            'documento_formatado': doc_apoio.sei_formatado or '',
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
    cargo = dados.get('cargo', '').strip() or 'Superintendente de Gestao Administrativa'

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Credenciais SEI sao obrigatorias.'}), 400

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticacao SEI.'}), 401

        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        # Referencia a analise NCI
        doc_nci = itinerario.get_doc('analise_pagamento')
        ref_nci = (doc_nci.sei_formatado or doc_nci.sei_id or '') if doc_nci else ''

        retorno_doc = gerar_despacho_apoio(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            ref_analise_nci_id=ref_nci,
        )

        if not retorno_doc:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar Despacho APOIO no SEI.'}), 500

        doc_id = str(retorno_doc.get('IdDocumento', ''))
        doc_formatado = retorno_doc.get('DocumentoFormatado', '')

        # Assinar com credenciais do usuario
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

        aviso = None
        if not resultado_assinatura.get('sucesso'):
            aviso = f'Documento gerado mas assinatura falhou: {resultado_assinatura.get("erro", "")}'

        # Salvar
        itinerario.ciencia_apoio = True

        itinerario.ciencia_apoio_data = datetime.now()
        itinerario.set_doc('despacho_apoio', sei_id=doc_id, sei_formatado=doc_formatado)

        # Despacho APOIO é ação interna — não avança etapa principal

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'despacho_apoio', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        resultado = {
            'sucesso': True,
            'documento_formatado': doc_formatado,
            'id_documento': doc_id,
        }
        if aviso:
            resultado['aviso'] = aviso
        return jsonify(resultado)

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

    # Guards
    if not itinerario.has_doc('despacho_apoio'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho APOIO ainda nao foi gerado.'}), 400

    doc_diretor = itinerario.get_doc('despacho_diretor')
    if doc_diretor and doc_diretor.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho do Diretor já foi gerado.',
            'documento_formatado': doc_diretor.sei_formatado or '',
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

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticacao SEI.'}), 401

        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        doc_apoio_ref = itinerario.get_doc('despacho_apoio')
        ref_apoio = (doc_apoio_ref.sei_formatado or doc_apoio_ref.sei_id or '') if doc_apoio_ref else ''

        retorno_doc = gerar_despacho_diretor(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            ref_despacho_apoio_id=ref_apoio,
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

        aviso = None
        if not resultado_assinatura.get('sucesso'):
            aviso = f'Documento gerado mas assinatura falhou: {resultado_assinatura.get("erro", "")}'

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
        itinerario.set_doc('despacho_diretor', sei_id=doc_id, sei_formatado=doc_formatado)

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
        if aviso:
            resultado['aviso'] = aviso
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
    if doc_geo and doc_geo.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho GEO já foi gerado.',
            'documento_formatado': doc_geo.sei_formatado or '',
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

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
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

        aviso = None
        if not resultado_assinatura.get('sucesso'):
            aviso = f'Documento gerado mas assinatura falhou: {resultado_assinatura.get("erro", "")}'

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
        itinerario.set_doc('despacho_geo', sei_id=doc_id, sei_formatado=doc_formatado)

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
        if aviso:
            resultado['aviso'] = aviso
        return jsonify(resultado)

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


# ── NL / PD / OB (inserção sequencial após Despacho GEO) ─────────────────


@financeiro_bp.route('/diarias/<int:id>/inserir-nl', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_nl(id):
    """Insere NL - Nota de Liquidacao (idSerie 420) no processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Esta solicitação não possui processo SEI vinculado.'}), 400

    if not itinerario.has_doc('despacho_geo'):
        return jsonify({'sucesso': False, 'erro': 'O Despacho GEO deve ser gerado antes da NL.'}), 400

    doc_nl = itinerario.get_doc('nl')
    if doc_nl and doc_nl.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'A NL já foi inserida.',
            'documento_formatado': doc_nl.sei_formatado or '',
        })

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados nao fornecidos.'}), 400

    codigo = dados.get('codigo', '').strip()
    if not codigo:
        return jsonify({'sucesso': False, 'erro': 'O codigo da NL e obrigatorio.'}), 400

    try:
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha ao autenticar no SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        retorno = gerar_nl(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            codigo_nl=codigo,
        )

        if not retorno:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar NL no SEI.'}), 500

        itinerario.set_doc('nl',
                           sei_id=str(retorno.get('IdDocumento', '')),
                           sei_formatado=retorno.get('DocumentoFormatado', ''),
                           codigo=codigo)
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'nl_inserida', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': retorno.get('DocumentoFormatado', ''),
            'id_documento': str(retorno.get('IdDocumento', '')),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


@financeiro_bp.route('/diarias/<int:id>/inserir-pd', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_pd(id):
    """Insere PD - Programacao de Desembolso (idSerie 421) no processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Esta solicitação não possui processo SEI vinculado.'}), 400

    if not itinerario.has_doc('nl'):
        return jsonify({'sucesso': False, 'erro': 'A NL deve ser inserida antes da PD.'}), 400

    doc_pd = itinerario.get_doc('pd')
    if doc_pd and doc_pd.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'A PD já foi inserida.',
            'documento_formatado': doc_pd.sei_formatado or '',
        })

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados nao fornecidos.'}), 400

    codigo = dados.get('codigo', '').strip()
    if not codigo:
        return jsonify({'sucesso': False, 'erro': 'O codigo da PD e obrigatorio.'}), 400

    try:
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha ao autenticar no SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        retorno = gerar_pd(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            codigo_pd=codigo,
        )

        if not retorno:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar PD no SEI.'}), 500

        itinerario.set_doc('pd',
                           sei_id=str(retorno.get('IdDocumento', '')),
                           sei_formatado=retorno.get('DocumentoFormatado', ''),
                           codigo=codigo)
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'pd_inserida', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': retorno.get('DocumentoFormatado', ''),
            'id_documento': str(retorno.get('IdDocumento', '')),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}), 500


@financeiro_bp.route('/diarias/<int:id>/inserir-ob', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_ob(id):
    """Insere OB - Ordem Bancaria (idSerie 422) no processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_id_procedimento:
        return jsonify({'sucesso': False, 'erro': 'Esta solicitação não possui processo SEI vinculado.'}), 400

    if not itinerario.has_doc('pd'):
        return jsonify({'sucesso': False, 'erro': 'A PD deve ser inserida antes da OB.'}), 400

    doc_ob = itinerario.get_doc('ob')
    if doc_ob and doc_ob.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'A OB já foi inserida.',
            'documento_formatado': doc_ob.sei_formatado or '',
        })

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados nao fornecidos.'}), 400

    codigo = dados.get('codigo', '').strip()
    if not codigo:
        return jsonify({'sucesso': False, 'erro': 'O codigo da OB e obrigatorio.'}), 400

    try:
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha ao autenticar no SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        retorno = gerar_ob(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            codigo_ob=codigo,
        )

        if not retorno:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar OB no SEI.'}), 500

        itinerario.set_doc('ob',
                           sei_id=str(retorno.get('IdDocumento', '')),
                           sei_formatado=retorno.get('DocumentoFormatado', ''),
                           codigo=codigo)

        # OB é o último sub-item da Concessão → avança para Prestação de Contas
        if itinerario.etapa_atual_id == DiariasEtapaID.CONCESSAO_DIARIAS:
            DiariaService.registrar_movimentacao(
                id_itinerario=id,
                etapa_nova_id=DiariasEtapaID.PRESTACAO_CONTAS,
                usuario_id=current_user.id if current_user else None,
                comentario=f'OB {codigo} inserida. Avançando para Prestação de Contas.',
            )

        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'ob_inserida', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': retorno.get('DocumentoFormatado', ''),
            'id_documento': str(retorno.get('IdDocumento', '')),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro inesperado OB: {str(e)}'}), 500


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 11 - Prestação de Contas CCDP (NP + Prestação SCDP + Despacho Final)
# ══════════════════════════════════════════════════════════════════════════════

@financeiro_bp.route('/diarias/<int:id>/inserir-np', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_np(id):
    """Insere NP - Nota Patrimonial (idSerie 423) no processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    if not usuario_tem_caixa(CAIXA_CCDP):
        return jsonify({'sucesso': False, 'erro': 'Acesso restrito a usuarios da CCDP.'}), 403

    if not itinerario.has_doc('comprovante_viagem'):
        return jsonify({'sucesso': False, 'erro': 'O comprovante de viagem deve ser enviado antes da NP.'}), 400

    doc_np = itinerario.get_doc('np')
    if doc_np and doc_np.sei_id:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'A NP já foi inserida.',
            'documento_formatado': doc_np.sei_formatado or '',
        })

    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados nao fornecidos.'}), 400

    codigo = dados.get('codigo', '').strip()
    if not codigo:
        return jsonify({'sucesso': False, 'erro': 'O codigo da NP e obrigatorio.'}), 400

    try:
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha ao autenticar no SEI.'}), 500

        sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

        retorno = gerar_np(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            codigo_np=codigo,
        )

        if not retorno:
            return jsonify({'sucesso': False, 'erro': 'Erro ao gerar NP no SEI.'}), 500

        itinerario.set_doc('np',
                           sei_id=str(retorno.get('IdDocumento', '')),
                           sei_formatado=retorno.get('DocumentoFormatado', ''),
                           codigo=codigo)
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'np_inserida', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS-FIN] Falha ao enviar notificacao: {exc_notif}')

        return jsonify({
            'sucesso': True,
            'documento_formatado': retorno.get('DocumentoFormatado', ''),
            'id_documento': str(retorno.get('IdDocumento', '')),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'erro': f'Erro ao inserir NP: {str(e)}'}), 500


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

    try:
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
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

    if itinerario.has_doc('despacho_ccdp') and itinerario.etapa_atual_id >= DiariasEtapaID.CONCESSAO_DIARIAS:
        return jsonify({
            'sucesso': True, 'ja_existe': True,
            'mensagem': 'O Despacho CCDP já foi assinado e a etapa já avançou.',
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

    try:
        # 1. Autenticar usuário no SEI
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
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

        # 5. Despacho CCDP é ação interna — não avança etapa principal

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
