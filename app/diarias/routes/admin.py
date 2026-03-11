"""
Rotas administrativas do módulo de Diárias (gerenciar agências, cargos/valores, administração).
"""
from flask import render_template, request, redirect, url_for, flash, abort, current_app
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission
from app.services.diaria_service import DiariaService
from app.models.diaria import (
    DiariasValorCargo, DiariasCargo,
    DiariasItinerario, DiariasItemItinerario,
)
from app.constants import DiariasEtapaID
from app.extensions import db


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

    if not all([cargo_id, tipo_id, valor is not None]):
        flash('Todos os campos são obrigatórios.', 'danger')
        return redirect(url_for('diarias.cargos'))

    if vc_id:
        vc = DiariasValorCargo.query.get(int(vc_id))
        if vc:
            vc.cargo_id = cargo_id
            vc.tipo_itinerario_id = tipo_id
            vc.valor = valor
    else:
        vc = DiariasValorCargo(cargo_id=cargo_id, tipo_itinerario_id=tipo_id, valor=valor)
        db.session.add(vc)

    db.session.commit()
    flash('Valor salvo com sucesso!', 'success')
    return redirect(url_for('diarias.cargos'))


# ── Administração / Acompanhamento ────────────────────────────────────────

@diarias_bp.route('/administracao')
@login_required
@requires_permission('diarias.aprovar')
def administracao():
    """
    Painel de administração: lista todas as solicitações com dados resumidos
    e progresso na timeline para o gestor acompanhar.
    """
    busca = request.args.get('q', '').strip()
    filtro_etapa = request.args.get('etapa', '')
    page = request.args.get('page', 1, type=int)

    query = DiariasItinerario.query

    if busca:
        query = query.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    if filtro_etapa:
        query = query.filter(DiariasItinerario.etapa_atual_id == int(filtro_etapa))

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

    return render_template(
        'diarias/administracao.html',
        itinerarios=itinerarios,
        pagination=pagination,
        pessoas_count=pessoas_count,
        etapas=etapas,
        filtro_etapa=filtro_etapa,
    )


@diarias_bp.route('/administracao/<int:id>')
@login_required
@requires_permission('diarias.aprovar')
def administracao_detalhe(id):
    """
    Detalhe administrativo de uma solicitação: timeline completa, info geral,
    upload de cotação ao SEI e visualização da NR do financeiro.
    """
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        abort(404)

    itinerario = dados['itinerario']
    timeline_data = DiariaService.obter_timeline(itinerario)

    return render_template(
        'diarias/administracao_detalhe.html',
        itinerario=itinerario,
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        cotacoes_voos=dados.get('cotacoes_voos', []),
        timeline_data=timeline_data,
        agencias=DiariaService.get_agencias(),
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
                    itinerario.sei_id_escolha_passagens = str(retorno.get('IdDocumento', ''))
                    itinerario.sei_escolha_passagens_formatado = retorno.get('DocumentoFormatado', '')
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

                    ref_req_passagens = itinerario.sei_requisicao_passagens_formatado or ''

                    if ref_cotacoes and ref_req_passagens:
                        ret_memo = gerar_memorando_cotacoes(
                            token=token,
                            id_procedimento=itinerario.sei_id_procedimento,
                            sei_protocolo=sei_protocolo,
                            ref_cotacoes_fmt=ref_cotacoes,
                            ref_requisicao_passagens_fmt=ref_req_passagens,
                        )
                        if ret_memo:
                            itinerario.sei_id_memorando_cotacoes = str(ret_memo.get('IdDocumento', ''))
                            itinerario.sei_memorando_cotacoes_formatado = ret_memo.get('DocumentoFormatado', '')
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

    db.session.commit()

    if sei_ok:
        flash('Escolha de passagens registrada e documento gerado no SEI com sucesso!', 'success')
    elif not itinerario.sei_id_procedimento:
        flash('Escolha de passagens registrada com sucesso!', 'success')

    return redirect(url_for('diarias.administracao_detalhe', id=id))


# ── Autorização do Secretário ─────────────────────────────────────────────

@diarias_bp.route('/administracao/<int:id>/autorizar', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
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
        UNIDADE_SEAD, UNIDADE_DFIN_APOIO,
    )
    from app.services.sei_integration import assinar_documento

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: somente o secretário pode autorizar
    if not current_user.is_secretario:
        return jsonify({
            'sucesso': False,
            'erro': 'Apenas o Secretário pode autorizar solicitações.'
        }), 403

    # Guard: só permite na etapa 1 (Solicitação Iniciada)
    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIADA:
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

    # Parse JSON
    dados = request.get_json()
    if not dados:
        return jsonify({'sucesso': False, 'erro': 'Dados não informados.'}), 400

    sei_usuario = dados.get('sei_usuario', '').strip()
    sei_senha = dados.get('sei_senha', '').strip()
    cargo = dados.get('cargo', '').strip()

    if not sei_usuario or not sei_senha:
        return jsonify({'sucesso': False, 'erro': 'Usuário e senha do SEI são obrigatórios.'}), 400

    if not cargo:
        cargo = 'Secretário de Administração do Estado do Piauí'

    # 1. Autentica secretário no SEI
    auth_result = autenticar_usuario_sei(sei_usuario, sei_senha)
    if not auth_result:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha na autenticação no SEI. Verifique usuário e senha.'
        }), 401

    token_secretario = auth_result['token']
    id_usuario = auth_result['id_usuario']
    id_login = auth_result['id_login']

    # 2. Obtem token admin para criar documento
    token_admin = gerar_token_sei_admin()
    if not token_admin:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha ao obter token administrativo do SEI.'
        }), 500

    sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

    # 3. Cria documento de autorização
    retorno_doc = gerar_autorizacao_secretario(
        token=token_admin,
        id_procedimento=itinerario.sei_id_procedimento,
        tipo_solicitacao_id=itinerario.tipo_solicitacao_id,
        sei_protocolo=sei_protocolo,
    )

    if not retorno_doc:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha ao criar documento de autorização no SEI.'
        }), 500

    doc_formatado = retorno_doc.get('DocumentoFormatado', '')
    id_documento = str(retorno_doc.get('IdDocumento', ''))

    # 4. Assina o documento com credenciais do secretário
    dados_assinatura = {
        'protocolo_doc': doc_formatado,
        'orgao': 'SEAD-PI',
        'cargo': cargo,
        'id_login': id_login,
        'id_usuario': id_usuario,
        'senha': sei_senha,
    }

    ret_assinatura = assinar_documento(
        token=token_secretario,
        unidade_id=UNIDADE_SEAD,
        dados_assinatura=dados_assinatura,
    )

    if not ret_assinatura or not ret_assinatura.get('sucesso'):
        erro_assinatura = ret_assinatura.get('erro', 'Erro desconhecido') if ret_assinatura else 'Sem resposta'
        return jsonify({
            'sucesso': False,
            'erro': f'Documento criado, mas falha ao assinar: {erro_assinatura}'
        }), 500

    # 5. Salva referência no banco
    itinerario.sei_id_autorizacao = id_documento
    itinerario.sei_autorizacao_formatado = doc_formatado

    # 6. Avança etapa para FINANCEIRO
    DiariaService.registrar_movimentacao(
        id_itinerario=id,
        etapa_nova_id=DiariasEtapaID.FINANCEIRO,
        usuario_id=current_user.id if current_user else None,
        comentario=f'Autorização do Secretário ({doc_formatado}) gerada e assinada pelo sistema',
    )

    resultado = {
        'sucesso': True,
        'documento_formatado': doc_formatado,
        'id_documento': id_documento,
    }

    # 7. Encaminha processo para DFIN/APOIO
    try:
        envio = enviar_procedimento(
            token_admin,
            sei_protocolo,
            [UNIDADE_DFIN_APOIO],
            manter_aberto=True,
        )
        resultado['envio_procedimento'] = envio

        if envio.get('sucesso'):
            # 8. Gera despacho DFIN
            try:
                itens = DiariasItemItinerario.query.filter_by(
                    id_itinerario=itinerario.id
                ).all()
                nomes_interessados = [
                    item.nome_pessoa for item in itens if item.nome_pessoa
                ]

                despacho_ret = gerar_despacho_dfin(
                    token=token_admin,
                    id_procedimento=itinerario.sei_id_procedimento,
                    sei_protocolo=sei_protocolo,
                    interessados=nomes_interessados,
                )
                if despacho_ret:
                    itinerario.sei_id_despacho_dfin = str(despacho_ret.get('IdDocumento', ''))
                    itinerario.sei_despacho_dfin_formatado = despacho_ret.get('DocumentoFormatado', '')
                    resultado['despacho_dfin'] = despacho_ret.get('DocumentoFormatado', '')
            except Exception as e:
                current_app.logger.error(f"SEI Diárias: Erro ao gerar despacho DFIN: {e}")
        else:
            current_app.logger.warning(
                f"SEI Diárias: Falha ao encaminhar procedimento: {envio.get('erro')}"
            )
    except Exception as e:
        current_app.logger.error(f"SEI Diárias: Erro ao encaminhar procedimento: {e}")

    db.session.commit()

    return jsonify(resultado)
