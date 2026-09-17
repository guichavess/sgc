"""
Rotas de CRUD - Criar, visualizar e editar solicitações.
"""
from flask import (
    render_template, session, redirect, url_for,
    request, flash, jsonify, current_app
)
from flask_login import login_required, current_user
from datetime import datetime
import requests as http_requests
import time

from app.solicitacoes.routes import solicitacoes_bp
from app.models import Solicitacao, Contrato, Etapa, HistoricoMovimentacao, SeiMovimentacao, SolicitacaoEmpenho, TipoPagamento
from app.extensions import db
from app.services import SolicitacaoService, SaldoService
from app.repositories import SolicitacaoRepository, ContratoRepository
from app.services.sei_integration import (
    criar_procedimento_pagamento,
    gerar_documento_pagamento,
    assinar_documento,
    consultar_procedimento_sei,
    listar_documentos_procedimento_sei,
    descrever_falha_criacao,
    UNIDADE_SEAD
)
from app.services.sei_auth import gerar_token_sei_admin
from app.constants import SerieDocumentoSEI
from app.utils.permissions import requires_permission
from app.utils.sei_datas import data_primeiro_documento_sei


@solicitacoes_bp.route('/nova-lote')
@login_required
@requires_permission('solicitacoes.criar')
def nova_lote():
    """Página para criação de solicitações de pagamento em lote."""
    lista_unidades = session.get('unidades', [])
    tipos_pagamento = TipoPagamento.query.order_by(TipoPagamento.id).all()
    return render_template(
        'solicitacoes/nova_lote.html',
        unidades=lista_unidades,
        tipos_pagamento=tipos_pagamento
    )


@solicitacoes_bp.route('/nova', methods=['GET', 'POST'])
@login_required
@requires_permission('solicitacoes.criar')
def nova_solicitacao():
    """Cria uma nova solicitação de pagamento com integração SEI."""
    lista_unidades = session.get('unidades', [])

    # Variáveis de controle do Modal de Assinatura
    modal_abrir = False
    doc_protocolo = ""
    unidade_atual = ""
    processo_sei = None
    alerta = None

    # Busca tipos de pagamento para o dropdown
    tipos_pagamento = TipoPagamento.query.order_by(TipoPagamento.id).all()

    if request.method == 'POST':
        codigo_contrato = request.form.get('contrato_selecionado')
        competencia = request.form.get('competencia')
        unidade_id = request.form.get('unidade_procedimento')
        id_tipo_pagamento = request.form.get('id_tipo_pagamento', type=int)

        # Validação básica
        if not codigo_contrato or not competencia or not unidade_id:
            flash('Preencha todos os campos obrigatórios.', 'danger')
            return redirect(url_for('solicitacoes.nova_solicitacao'))

        # Verifica se contrato existe
        contrato = ContratoRepository.buscar_por_codigo(codigo_contrato)
        if not contrato:
            flash('Contrato não encontrado.', 'danger')
            return redirect(url_for('solicitacoes.nova_solicitacao'))

        # Falhas a partir daqui re-renderizam a tela com o alerta e os dados preenchidos
        reenvio = {
            'contrato': _contrato_para_tela(contrato),
            'competencia': competencia,
            'id_tipo_pagamento': str(id_tipo_pagamento or ''),
            'unidade_procedimento': unidade_id,
        }

        token_sei = session.get('sei_token') or gerar_token_sei_admin()
        if not token_sei:
            current_app.logger.warning('[SOLICITACOES] Nova solicitação sem token SEI (usuário %s)', current_user.id)
            return _render_nova(tipos_pagamento, reenvio=reenvio, alerta={
                'tipo': 'erro',
                'titulo': 'Não foi possível autenticar no SEI',
                'mensagem': 'O SGC não conseguiu acesso ao SEI para abrir o processo.',
                'dica': 'Saia do SGC e entre novamente para renovar o acesso ao SEI.',
                'acao': {'rotulo': 'Sair e entrar novamente', 'href': url_for('auth.logout')},
            })

        proc_criado = None
        try:
            # Monta dados do contrato para a API SEI
            dados_contrato_api = {
                'numeroOriginal': contrato.numeroOriginal,
                'nomeContratado': contrato.nomeContratado,
                'codigo': contrato.codigo,
                'nomeContratadoResumido': contrato.nomeContratadoResumido
            }

            # 1. Cria Processo no SEI
            detalhe_erro = {}
            proc_criado = criar_procedimento_pagamento(
                token_sei, unidade_id, dados_contrato_api, competencia, detalhe_erro=detalhe_erro
            )

            if not proc_criado:
                current_app.logger.warning(
                    '[SOLICITACOES] SEI não criou o processo (contrato %s, unidade %s): %s',
                    codigo_contrato, unidade_id, detalhe_erro,
                )
                alerta = dict(descrever_falha_criacao(detalhe_erro), tipo='erro')
                if detalhe_erro.get('status') == 401:
                    alerta['acao'] = {'rotulo': 'Sair e entrar novamente', 'href': url_for('auth.logout')}
                else:
                    alerta['acao'] = {'rotulo': 'Tentar novamente', 'form': 'form-solicitacao'}
                return _render_nova(tipos_pagamento, reenvio=reenvio, alerta=alerta)

            # 2. Gera Documento (Requerimento) vinculado ao processo
            ctx_doc = {
                'num_contrato': contrato.numeroOriginal,
                'empresa': contrato.nomeContratado,
                'competencia': competencia,
                'usuario_nome': session.get('usuario_nome', current_user.nome if hasattr(current_user, 'nome') else ''),
                'usuario_cargo': session.get('usuario_cargo', 'Colaborador'),
                'objeto': contrato.objeto or 'Objeto não informado'
            }
            detalhe_doc = {}
            doc_criado = gerar_documento_pagamento(
                token_sei, unidade_id, proc_criado['IdProcedimento'], ctx_doc, detalhe_erro=detalhe_doc
            )

            # 3. Salva solicitação no banco
            nova_sol = Solicitacao(
                codigo_contrato=codigo_contrato,
                id_usuario_solicitante=current_user.id,
                protocolo_gerado_sei=proc_criado.get('ProcedimentoFormatado'),
                id_procedimento_sei=proc_criado.get('IdProcedimento'),
                link_processo_sei=proc_criado.get('LinkAcesso'),
                especificacao=proc_criado.get('EspecificacaoGerada'),
                competencia=competencia,
                id_caixa_sei=unidade_id,
                id_tipo_pagamento=id_tipo_pagamento,
                descricao=f'Solicitação de Pagamento - {competencia}',
                status_geral='AGUARDANDO_ASSINATURA',
                data_solicitacao=datetime.now()
            )
            db.session.add(nova_sol)
            db.session.commit()

            # 4. Prepara o Modal de Assinatura (não redireciona)
            doc_protocolo = (doc_criado or {}).get('DocumentoFormatado', '')
            proc_formatado = proc_criado.get('ProcedimentoFormatado', '')
            if not doc_protocolo:
                # Sem documento não há o que assinar: o modal abriria com "—" e a assinatura falharia
                current_app.logger.warning(
                    '[SOLICITACOES] Processo %s criado sem requisição de pagamento: %s',
                    proc_formatado, detalhe_doc or 'resposta do SEI sem DocumentoFormatado',
                )
                return _render_nova(tipos_pagamento, alerta=_alerta_documento_nao_gerado(proc_criado, detalhe_doc))

            modal_abrir = True
            unidade_atual = unidade_id
            processo_sei = _processo_para_tela(proc_criado)
            alerta = {
                'tipo': 'sucesso',
                'titulo': 'Processo aberto no SEI',
                'mensagem': 'Assine a requisição de pagamento com sua senha do SEI para concluir.',
                'processo': processo_sei,
            }

        except Exception:
            db.session.rollback()
            current_app.logger.exception(
                '[SOLICITACOES] Falha ao criar solicitação (contrato %s, unidade %s, processo %s)',
                codigo_contrato, unidade_id, (proc_criado or {}).get('ProcedimentoFormatado'),
            )
            if proc_criado:
                # O processo já existe no SEI: tentar de novo abriria um segundo processo
                return _render_nova(tipos_pagamento, alerta={
                    'tipo': 'erro',
                    'titulo': 'Processo aberto no SEI, mas a solicitação não foi registrada',
                    'mensagem': 'O processo foi criado no SEI, mas houve uma falha ao gerar o documento ou salvar a solicitação.',
                    'processo': _processo_para_tela(proc_criado),
                    'dica': 'Não crie de novo: vincule o processo já aberto para não duplicar.',
                    'acao': {'rotulo': 'Vincular processo', 'href': url_for('solicitacoes.vincular_solicitacao')},
                })
            return _render_nova(tipos_pagamento, reenvio=reenvio, alerta={
                'tipo': 'erro',
                'titulo': 'Não foi possível criar a solicitação',
                'mensagem': 'Ocorreu uma falha inesperada antes de abrir o processo no SEI.',
                'dica': 'Seus dados continuam aqui. Tente novamente; se continuar, avise o suporte.',
                'acao': {'rotulo': 'Tentar novamente', 'form': 'form-solicitacao'},
            })

    return render_template(
        'solicitacoes/nova.html',
        unidades=lista_unidades,
        modal_abrir=modal_abrir,
        doc_protocolo=doc_protocolo,
        unidade_atual=unidade_atual,
        processo_sei=processo_sei,
        alerta=alerta,
        tipos_pagamento=tipos_pagamento
    )


def _alerta_documento_nao_gerado(proc_criado, detalhe_doc):
    """Alerta de processo aberto (e solicitação salva) sem a requisição de pagamento gerada."""
    mensagem = 'O processo foi aberto no SEI e a solicitação foi registrada, mas o SEI não gerou o documento de requisição.'
    do_sei = (detalhe_doc.get('mensagem') or '').strip()
    if do_sei:
        mensagem = f'{mensagem} Resposta do SEI: “{do_sei}”'
    link = proc_criado.get('LinkAcesso')
    acao = ({'rotulo': 'Abrir processo no SEI', 'href': link, 'externo': True} if link
            else {'rotulo': 'Ir para as solicitações', 'href': url_for('solicitacoes.dashboard')})
    return {
        'tipo': 'aviso',
        'titulo': 'Processo criado, mas a requisição de pagamento não foi gerada',
        'mensagem': mensagem,
        'dica': 'Não crie de novo: inclua a requisição de pagamento direto no processo pelo SEI.',
        'codigo': descrever_falha_criacao(detalhe_doc)['codigo'],
        'acao': acao,
        'processo': _processo_para_tela(proc_criado),
    }


def _processo_para_tela(proc_criado):
    """Número e link do processo criado, para exibir com link direto ao SEI."""
    return {'numero': proc_criado.get('ProcedimentoFormatado') or 'sem número',
            'link': proc_criado.get('LinkAcesso') or ''}


def _contrato_para_tela(contrato):
    """Contrato no mesmo formato da busca (api_buscar_contratos), para refazer o passo 1."""
    objeto = contrato.objeto or 'Sem objeto'
    return {
        'codigo': contrato.codigo,
        'numeroOriginal': contrato.numeroOriginal,
        'nomeContratado': contrato.nomeContratado,
        'objeto': objeto[:100] + '...' if len(objeto) > 100 else objeto,
        'numProcesso': contrato.numProcesso,
    }


def _render_nova(tipos_pagamento, alerta, reenvio=None):
    """Re-renderiza a Nova solicitação com o alerta da página (sem redirect: mantém os dados)."""
    return render_template(
        'solicitacoes/nova.html',
        unidades=session.get('unidades', []),
        modal_abrir=False,
        doc_protocolo='',
        unidade_atual='',
        tipos_pagamento=tipos_pagamento,
        alerta=alerta,
        reenvio=reenvio,
    )


@solicitacoes_bp.route('/vincular', methods=['GET', 'POST'])
@login_required
@requires_permission('solicitacoes.criar')
def vincular_solicitacao():
    """Vincula um processo SEI existente a uma nova solicitação."""
    # Busca tipos de pagamento para o dropdown
    tipos_pagamento = TipoPagamento.query.order_by(TipoPagamento.id).all()

    if request.method == 'POST':
        protocolo_sei = request.form.get('protocolo_sei', '').strip()
        codigo_contrato = request.form.get('contrato_selecionado', '').strip()
        competencia = request.form.get('competencia', '').strip()
        id_tipo_pagamento = request.form.get('id_tipo_pagamento', type=int)
        valor_empenho_raw = request.form.get('valor_empenho', '').strip()

        # Validação básica
        if not protocolo_sei or not codigo_contrato or not competencia:
            flash('Preencha todos os campos obrigatórios.', 'danger')
            return redirect(url_for('solicitacoes.vincular_solicitacao'))

        # Verifica duplicata
        existente = Solicitacao.query.filter_by(
            protocolo_gerado_sei=protocolo_sei
        ).first()
        if existente:
            flash(f'Este processo já está vinculado à solicitação #{existente.id}.', 'warning')
            return redirect(url_for('solicitacoes.vincular_solicitacao'))

        # Verifica contrato
        contrato = ContratoRepository.buscar_por_codigo(codigo_contrato)
        if not contrato:
            flash('Contrato não encontrado.', 'danger')
            return redirect(url_for('solicitacoes.vincular_solicitacao'))

        # Consulta processo no SEI
        token_sei = session.get('sei_token') or gerar_token_sei_admin()
        if not token_sei:
            flash('Não foi possível autenticar no SEI.', 'danger')
            return redirect(url_for('solicitacoes.vincular_solicitacao'))

        resultado_sei = consultar_procedimento_sei(token_sei, protocolo_sei)
        if not resultado_sei['sucesso']:
            flash(f'Processo não encontrado no SEI: {resultado_sei["erro"]}', 'danger')
            return redirect(url_for('solicitacoes.vincular_solicitacao'))

        try:
            # Documentos do processo: o início real é a data do 1º documento SEI,
            # não a data da vinculação (fallback: agora, se nenhum doc tiver data)
            protocolo_formatado = resultado_sei['protocolo_formatado']
            resultado_docs = listar_documentos_procedimento_sei(token_sei, protocolo_formatado)
            documentos_sei = resultado_docs.get('documentos', [])
            data_inicio = data_primeiro_documento_sei(documentos_sei) or datetime.now()

            # 1. Cria a solicitação no banco
            nova_sol = Solicitacao(
                codigo_contrato=codigo_contrato,
                id_usuario_solicitante=current_user.id,
                protocolo_gerado_sei=resultado_sei['protocolo_formatado'],
                id_procedimento_sei=resultado_sei['id_procedimento'],
                link_processo_sei=resultado_sei['link_acesso'],
                especificacao=resultado_sei.get('especificacao', ''),
                competencia=competencia,
                id_caixa_sei=UNIDADE_SEAD,
                id_tipo_pagamento=id_tipo_pagamento,
                descricao=f'Processo vinculado - {competencia}',
                status_geral='ABERTO',
                data_solicitacao=data_inicio
            )
            db.session.add(nova_sol)
            db.session.flush()

            # 2. Registra histórico inicial (etapa 1 = início do processo)
            historico = HistoricoMovimentacao(
                id_solicitacao=nova_sol.id,
                id_etapa_nova=1,
                id_usuario_responsavel=current_user.id,
                data_movimentacao=data_inicio,
                comentario='Processo SEI existente vinculado ao sistema'
            )
            db.session.add(historico)
            db.session.commit()

            # 3. Baixa documentos do SEI para tabela seimovimentacao
            if documentos_sei:
                _baixar_documentos_sei(
                    token_sei, protocolo_formatado, documentos_sei
                )

            # 4. Detecta etapa atual com base nos documentos
            from app.solicitacoes.routes.api import processar_item_sei
            app_obj = current_app._get_current_object()
            processar_item_sei(
                app_obj, nova_sol.id, token_sei,
                current_user.id, {e.id: e.ordem for e in Etapa.query.all()}
            )

            # 5. Registra valor do empenho (se informado)
            if valor_empenho_raw:
                try:
                    # Converte formato brasileiro (1.234,56) para float
                    valor_float = float(
                        valor_empenho_raw.replace('.', '').replace(',', '.')
                    )
                    if valor_float > 0:
                        SaldoService.registrar_e_atualizar_saldo(
                            nova_sol, current_user.id, valor_float,
                            validar_saldo=False
                        )
                except (ValueError, TypeError):
                    pass  # Valor inválido — ignora silenciosamente

            flash(
                f'Processo {protocolo_formatado} vinculado com sucesso! '
                f'Etapas detectadas automaticamente.',
                'success'
            )
            return redirect(url_for(
                'solicitacoes.detalhes_solicitacao',
                id_solicitacao=nova_sol.id
            ))

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f'Erro ao vincular processo: {e}', exc_info=True)
            flash('Erro ao vincular processo. Tente novamente.', 'danger')
            return redirect(url_for('solicitacoes.vincular_solicitacao'))

    # GET
    return render_template('solicitacoes/vincular.html', tipos_pagamento=tipos_pagamento)


def _baixar_documentos_sei(token_sei, protocolo, documentos):
    """Salva documentos do SEI na tabela seimovimentacao (uso síncrono)."""
    start_time = time.time()
    tempo_total = 0

    try:
        for doc in documentos:
            serie = doc.get('Serie', {})
            unidade = doc.get('UnidadeElaboradora', {})
            tempo_total = round(time.time() - start_time, 3)

            novo_mov = SeiMovimentacao(
                id_documento=str(doc.get('IdDocumento', '')),
                protocolo_procedimento=protocolo,
                id_procedimento=str(doc.get('IdProcedimento', '')),
                procedimento_formatado=str(doc.get('ProcedimentoFormatado', '')),
                documento_formatado=str(doc.get('DocumentoFormatado', '')),
                link_acesso=doc.get('LinkAcesso'),
                descricao=doc.get('Descricao'),
                data=doc.get('Data') or doc.get('DataGeracao'),
                numero=doc.get('Numero'),
                id_serie=int(serie.get('IdSerie')) if serie.get('IdSerie') and str(serie.get('IdSerie')).isdigit() else None,
                serie_nome=serie.get('Nome'),
                serie_aplicabilidade=serie.get('Aplicabilidade'),
                unidade_id=str(unidade.get('IdUnidade', '')),
                unidade_sigla=unidade.get('Sigla'),
                unidade_descricao=unidade.get('Descricao'),
                tempo_execucao=tempo_total,
                obs='Vinculado manualmente'
            )
            db.session.add(novo_mov)

        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'Erro ao salvar documentos SEI: {e}', exc_info=True)


@solicitacoes_bp.route('/solicitacao/<int:id_solicitacao>', methods=['GET', 'POST'])
@login_required
@requires_permission('solicitacoes.visualizar')
def detalhes_solicitacao(id_solicitacao):
    """Exibe detalhes de uma solicitação e permite ações."""
    solicitacao = SolicitacaoRepository.get_com_contrato(id_solicitacao)

    if not solicitacao:
        flash('Solicitação não encontrada.', 'danger')
        return redirect(url_for('solicitacoes.dashboard'))

    if request.method == 'POST':
        acao = request.form.get('acao')

        if acao == 'avancar_etapa':
            nova_etapa_id = request.form.get('nova_etapa_id', type=int)
            comentario = request.form.get('comentario', '')

            if nova_etapa_id:
                if SolicitacaoService.avancar_etapa(
                    solicitacao,
                    nova_etapa_id,
                    current_user.id,
                    comentario
                ):
                    flash('Etapa atualizada com sucesso!', 'success')
                else:
                    flash('Não foi possível avançar para esta etapa.', 'warning')

        elif acao == 'solicitar_empenho':
            valor = request.form.get('valor', type=float, default=0.0)

            resultado = SaldoService.registrar_e_atualizar_saldo(
                solicitacao,
                current_user.id,
                valor,
                validar_saldo=False
            )

            if resultado['sucesso']:
                flash(resultado['msg'], 'success')
            else:
                flash(resultado['msg'], 'danger')

        return redirect(url_for('solicitacoes.detalhes_solicitacao', id_solicitacao=id_solicitacao))

    # GET - Monta dados para exibição
    todas_etapas = Etapa.query.order_by(Etapa.ordem).all()
    timeline_data = SolicitacaoService.obter_timeline(solicitacao)

    # Histórico de movimentações
    historico = HistoricoMovimentacao.query.filter_by(
        id_solicitacao=id_solicitacao
    ).order_by(HistoricoMovimentacao.data_movimentacao.desc()).all()

    # Informações de saldo
    saldo_info = SaldoService.obter_resumo_saldo(
        solicitacao.codigo_contrato,
        solicitacao.competencia
    )

    # Busca documentos financeiros do SEI (NE, NL, PD, OB)
    mov_ne = None
    mov_nl = None
    mov_pd = None
    mov_ob = None
    if solicitacao.protocolo_gerado_sei:
        docs_sei = SeiMovimentacao.query.filter_by(
            protocolo_procedimento=solicitacao.protocolo_gerado_sei
        ).all()
        mov_ne = next((d for d in docs_sei if str(d.id_serie) == SerieDocumentoSEI.NOTA_EMPENHO), None)
        mov_nl = next((d for d in docs_sei if str(d.id_serie) == SerieDocumentoSEI.LIQUIDACAO), None)
        mov_pd = next((d for d in docs_sei if str(d.id_serie) == SerieDocumentoSEI.PD), None)
        mov_ob = next((d for d in docs_sei if str(d.id_serie) == SerieDocumentoSEI.OB), None)

    # Último empenho solicitado
    ultimo_empenho = SolicitacaoEmpenho.query.filter_by(
        id_solicitacao=id_solicitacao
    ).order_by(SolicitacaoEmpenho.data.desc()).first()

    return render_template(
        'solicitacoes/detalhes.html',
        solicitacao=solicitacao,
        todas_etapas=todas_etapas,
        timeline_data=timeline_data,
        historico=historico,
        saldo_info=saldo_info,
        mov_ne=mov_ne,
        mov_nl=mov_nl,
        mov_pd=mov_pd,
        mov_ob=mov_ob,
        ultimo_empenho=ultimo_empenho
    )
