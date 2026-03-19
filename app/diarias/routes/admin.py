"""
Rotas administrativas do módulo de Diárias (gerenciar agências, cargos/valores, administração).
"""
import json
from datetime import datetime
from flask import render_template, request, redirect, url_for, flash, abort, current_app, jsonify
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission, usuario_tem_caixa, CAIXA_APOIOSGA, CAIXA_NCI
from app.services.diaria_service import DiariaService
from app.services.sei_auth import autenticar_usuario_sei
from app.services.sei_integration import assinar_documento
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, gerar_despacho_sga, gerar_analise_pagamento,
    gerar_despacho_nci, enviar_procedimento,
    UNIDADE_APOIOSGA, UNIDADE_NCI, PERGUNTAS_ANALISE_PAGAMENTO,
)
from app.models.diaria import (
    DiariasValorCargo, DiariasCargo,
    DiariasItinerario, DiariasItemItinerario,
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


# ── Assinatura do Superintendente nas Requisições ────────────────────────

@diarias_bp.route('/administracao/<int:id>/assinar-superintendente', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def assinar_superintendente(id):
    """
    Superintendente assina os documentos de Requisição de Diárias
    (e Requisição de Passagens, se aplicável) no SEI.
    Deve ocorrer ANTES da autorização do Secretário.
    """
    from app.services.sei_auth import autenticar_usuario_sei
    from app.services.diarias_sei_integration import UNIDADE_SEAD
    from app.services.sei_integration import assinar_documento

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: somente o superintendente pode assinar
    if not current_user.is_superintendente:
        return jsonify({
            'sucesso': False,
            'erro': 'Apenas o Superintendente pode assinar as requisições.'
        }), 403

    # Guard: só permite na etapa 1 (Solicitação Iniciada)
    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIADA:
        return jsonify({
            'sucesso': False,
            'erro': 'Esta solicitação não está na etapa de autorização (etapa 1).'
        }), 400

    # Guard: precisa de processo SEI e requisição gerada
    if not itinerario.sei_id_procedimento or not itinerario.sei_id_requisicao:
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
        cargo = 'Superintendente de Gestão Administrativa'

    # 1. Autentica superintendente no SEI
    auth_result = autenticar_usuario_sei(sei_usuario, sei_senha)
    if not auth_result:
        return jsonify({
            'sucesso': False,
            'erro': 'Falha na autenticação no SEI. Verifique usuário e senha.'
        }), 401

    token_super = auth_result['token']
    id_usuario = auth_result['id_usuario']
    id_login = auth_result['id_login']

    documentos_assinados = []

    # 2. Assinar Requisição de Diárias
    dados_assinatura = {
        'protocolo_doc': itinerario.sei_id_requisicao,
        'orgao': 'SEAD-PI',
        'cargo': cargo,
        'id_login': id_login,
        'id_usuario': id_usuario,
        'senha': sei_senha,
    }

    ret = assinar_documento(
        token=token_super,
        unidade_id=UNIDADE_SEAD,
        dados_assinatura=dados_assinatura,
    )

    if not ret or not ret.get('sucesso'):
        erro = ret.get('erro', 'Erro desconhecido') if ret else 'Sem resposta'
        return jsonify({
            'sucesso': False,
            'erro': f'Falha ao assinar Requisição de Diárias: {erro}'
        }), 500

    documentos_assinados.append(itinerario.sei_requisicao_formatado or 'Req. Diárias')

    # 3. Assinar Requisição de Passagens (se existir)
    if itinerario.sei_id_requisicao_passagens:
        dados_assinatura_pass = {
            'protocolo_doc': itinerario.sei_id_requisicao_passagens,
            'orgao': 'SEAD-PI',
            'cargo': cargo,
            'id_login': id_login,
            'id_usuario': id_usuario,
            'senha': sei_senha,
        }

        ret_pass = assinar_documento(
            token=token_super,
            unidade_id=UNIDADE_SEAD,
            dados_assinatura=dados_assinatura_pass,
        )

        if ret_pass and ret_pass.get('sucesso'):
            documentos_assinados.append(itinerario.sei_requisicao_passagens_formatado or 'Req. Passagens')
        else:
            current_app.logger.warning(
                f"SEI Diárias: Superintendente assinou Req. Diárias mas falhou na Req. Passagens: "
                f"{ret_pass.get('erro') if ret_pass else 'Sem resposta'}"
            )

    # 4. Salvar no banco
    itinerario.superintendente_assinou = True
    itinerario.superintendente_assinou_data = datetime.now()
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

    # Guard: Superintendente precisa ter assinado antes
    if not itinerario.superintendente_assinou:
        return jsonify({
            'sucesso': False,
            'erro': 'O Superintendente precisa assinar as requisições antes da autorização do Secretário.'
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

    # 1.5 Secretário assina as Requisições (Diárias + Passagens)
    if itinerario.sei_id_requisicao:
        ret_req = assinar_documento(
            token=token_secretario,
            unidade_id=UNIDADE_SEAD,
            dados_assinatura={
                'protocolo_doc': itinerario.sei_id_requisicao,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': id_login,
                'id_usuario': id_usuario,
                'senha': sei_senha,
            },
        )
        if not ret_req or not ret_req.get('sucesso'):
            erro = ret_req.get('erro', 'Erro desconhecido') if ret_req else 'Sem resposta'
            current_app.logger.warning(f"SEI: Secretário falhou ao assinar Req. Diárias: {erro}")

    if itinerario.sei_id_requisicao_passagens:
        ret_req_pass = assinar_documento(
            token=token_secretario,
            unidade_id=UNIDADE_SEAD,
            dados_assinatura={
                'protocolo_doc': itinerario.sei_id_requisicao_passagens,
                'orgao': 'SEAD-PI',
                'cargo': cargo,
                'id_login': id_login,
                'id_usuario': id_usuario,
                'senha': sei_senha,
            },
        )
        if not ret_req_pass or not ret_req_pass.get('sucesso'):
            erro = ret_req_pass.get('erro', 'Erro desconhecido') if ret_req_pass else 'Sem resposta'
            current_app.logger.warning(f"SEI: Secretário falhou ao assinar Req. Passagens: {erro}")

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

    try:
        DiariasNotifier.notificar_etapa(itinerario, 'nova_solicitacao', current_user.id)
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
    if not itinerario.sei_id_despacho_ccdp:
        return jsonify({'sucesso': False, 'erro': 'O Despacho CCDP ainda não foi gerado.'}), 400

    if itinerario.sei_id_despacho_sga:
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

    try:
        # 1. Autenticar superintendente no SEI
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
        if not auth or not auth.get('token'):
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI.'}), 401

        # 2. Token admin
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'sucesso': False, 'erro': 'Falha ao obter token administrativo SEI.'}), 500

        # 3. Gerar Despacho SGA (série 2987)
        retorno_doc = gerar_despacho_sga(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
            ref_despacho_ccdp_id=itinerario.sei_id_despacho_ccdp,
            ref_despacho_ccdp_formatado=itinerario.sei_despacho_ccdp_formatado,
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
        itinerario.sei_id_despacho_sga = doc_id
        itinerario.sei_despacho_sga_formatado = doc_formatado

        # 7. Avançar etapa
        DiariaService.registrar_movimentacao(
            id_itinerario=id,
            etapa_nova_id=DiariasEtapaID.CIENCIA_SGA,
            usuario_id=current_user.id if current_user else None,
            comentario=f'Ciência Superintendente. Despacho SGA ({doc_formatado}) enviado ao NCI.',
        )

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
    if not itinerario.sei_id_despacho_sga:
        return jsonify({'sucesso': False, 'erro': 'O Despacho SGA ainda não foi gerado.'}), 400

    if itinerario.sei_id_analise_pagamento:
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

    try:
        # 1. Autenticar usuário NCI no SEI
        auth = autenticar_usuario_sei(sei_usuario, sei_senha)
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

        # 5. Gerar Despacho NCI (série 5)
        retorno_despacho = gerar_despacho_nci(
            token=token_admin,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=sei_protocolo,
            ref_analise_formatado=analise_formatado,
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
        itinerario.sei_id_analise_pagamento = analise_id
        itinerario.sei_analise_pagamento_formatado = analise_formatado
        itinerario.sei_id_despacho_nci = despacho_id
        itinerario.sei_despacho_nci_formatado = despacho_formatado

        # 8. Avançar etapa
        DiariaService.registrar_movimentacao(
            id_itinerario=id,
            etapa_nova_id=DiariasEtapaID.ANALISE_NCI,
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
