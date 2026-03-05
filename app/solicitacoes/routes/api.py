"""
Rotas de API - Endpoints AJAX/JSON.
"""
from flask import request, jsonify, session, redirect, url_for, Response, stream_with_context, current_app
from flask_login import login_required, current_user
from sqlalchemy import or_
from datetime import datetime
import json
import time
import requests as http_requests
import concurrent.futures

from app.solicitacoes.routes import solicitacoes_bp
from app.models import (
    Solicitacao, SolicitacaoEmpenho, Contrato,
    HistoricoMovimentacao, Etapa, SeiMovimentacao
)
from app.extensions import db
from app.repositories import ContratoRepository, SolicitacaoRepository
from app.services import SolicitacaoService, SaldoService
from app.services.sei_auth import gerar_token_sei_admin
from app.services.sei_integration import (
    assinar_documento, consultar_procedimento_sei,
    criar_procedimento_pagamento, gerar_documento_pagamento
)
from app.constants import SerieDocumentoSEI
from app.utils.permissions import requires_permission


# =============================================================================
# ENDPOINTS DE BUSCA
# =============================================================================

@solicitacoes_bp.route('/api/buscar-contrato')
@login_required
@requires_permission('solicitacoes.visualizar')
def api_buscar_contrato():
    """Busca contratos por termo (autocomplete)."""
    termo = request.args.get('q', '').strip()

    if len(termo) < 2:
        return jsonify({'contratos': []})

    contratos = ContratoRepository.buscar_por_termo(termo, limite=10)

    return jsonify({
        'contratos': [ContratoRepository.to_dict(c) for c in contratos]
    })


@solicitacoes_bp.route('/api/contratos')
@login_required
@requires_permission('solicitacoes.visualizar')
def api_buscar_contratos():
    """Busca contratos para autocomplete (formato Select2)."""
    termo = request.args.get('q', '').strip()

    # Se parâmetro todos=1, retorna todos os contratos (para seleção em lote)
    if request.args.get('todos') == '1':
        resultados = Contrato.query.order_by(Contrato.codigo).all()
    else:
        if not termo or len(termo) < 3:
            return jsonify({'results': []})

        query = Contrato.query.filter(
            or_(
                Contrato.codigo.like(f'%{termo}%'),
                Contrato.numeroOriginal.like(f'%{termo}%'),
                Contrato.nomeContratado.like(f'%{termo}%'),
                Contrato.numProcesso.like(f'%{termo}%')
            )
        )

        resultados = query.limit(20).all()

    dados = [{
        'codigo': c.codigo,
        'numeroOriginal': c.numeroOriginal,
        'nomeContratado': c.nomeContratado,
        'objeto': c.objeto[:100] + '...' if c.objeto and len(c.objeto) > 100 else (c.objeto or 'Sem objeto'),
        'numProcesso': c.numProcesso
    } for c in resultados]

    return jsonify({'results': dados})


# =============================================================================
# ENDPOINT DE VALIDAÇÃO SEI
# =============================================================================

@solicitacoes_bp.route('/api/validar-protocolo-sei')
@login_required
@requires_permission('solicitacoes.criar')
def api_validar_protocolo_sei():
    """Valida se um protocolo SEI existe e retorna seus dados."""
    protocolo = request.args.get('protocolo', '').strip()

    if not protocolo or len(protocolo) < 5:
        return jsonify({'valido': False, 'erro': 'Informe um protocolo válido.'})

    # Verifica se já existe solicitação com esse protocolo
    existente = Solicitacao.query.filter_by(
        protocolo_gerado_sei=protocolo
    ).first()
    if not existente:
        # Tenta com protocolo formatado (pode ter sido salvo com formato diferente)
        protocolo_limpo = "".join(filter(str.isdigit, protocolo))
        existente = Solicitacao.query.filter(
            Solicitacao.protocolo_gerado_sei.like(f'%{protocolo_limpo}%')
        ).first()

    if existente:
        return jsonify({
            'valido': False,
            'erro': f'Este processo já está vinculado à solicitação #{existente.id}.'
        })

    # Consulta o SEI
    token_sei = session.get('sei_token') or gerar_token_sei_admin()
    if not token_sei:
        return jsonify({'valido': False, 'erro': 'Não foi possível autenticar no SEI.'})

    resultado = consultar_procedimento_sei(token_sei, protocolo)

    if not resultado['sucesso']:
        return jsonify({'valido': False, 'erro': resultado['erro']})

    return jsonify({
        'valido': True,
        'protocolo_formatado': resultado['protocolo_formatado'],
        'id_procedimento': resultado['id_procedimento'],
        'link_acesso': resultado['link_acesso'],
        'especificacao': resultado['especificacao']
    })


# =============================================================================
# ENDPOINTS DE ASSINATURA E NE
# =============================================================================

@solicitacoes_bp.route('/api/assinar_ajax', methods=['POST'])
@login_required
@requires_permission('solicitacoes.editar')
def assinar_ajax():
    """Assina documento no SEI via AJAX."""
    dados = request.json
    senha = dados.get('senha')
    protocolo_doc = dados.get('protocolo')
    unidade = dados.get('unidade')

    if not senha:
        return jsonify({'sucesso': False, 'erro': 'Senha obrigatória'})

    # Recupera a solicitação pendente de assinatura do usuário logado
    solicitacao = Solicitacao.query.filter_by(
        id_usuario_solicitante=current_user.id,
        status_geral='AGUARDANDO_ASSINATURA'
    ).order_by(Solicitacao.id.desc()).first()

    if not solicitacao:
        return jsonify({'sucesso': False, 'erro': 'Solicitação não encontrada para registrar histórico.'})

    # Monta dados para o serviço de assinatura
    dados_ass = {
        'protocolo_doc': protocolo_doc,
        'orgao': session.get('usuario_orgao', 'SEAD-PI'),
        'cargo': session.get('usuario_cargo', 'Colaborador'),
        'id_login': session.get('usuario_sei_id_login'),
        'id_usuario': session.get('usuario_sei_id'),
        'senha': senha
    }

    token = session.get('sei_token')

    # Tenta assinar via API SEI
    resultado = assinar_documento(token, unidade, dados_ass)

    if resultado.get('sucesso'):
        try:
            # Atualiza Status da Solicitação
            if solicitacao.status_geral == 'AGUARDANDO_ASSINATURA':
                solicitacao.status_geral = 'ABERTO'

            # Cria o Histórico de Movimentação
            novo_historico = HistoricoMovimentacao(
                id_solicitacao=solicitacao.id,
                id_etapa_anterior=None,
                id_etapa_nova=1,
                id_usuario_responsavel=current_user.id,
                data_movimentacao=datetime.now(),
                comentario=f"Processo criado e documento {protocolo_doc} assinado com sucesso."
            )

            SaldoService.registrar_e_atualizar_saldo(solicitacao, current_user.id, valor_solicitado=0.0, validar_saldo=False)
            db.session.add(novo_historico)
            db.session.commit()

            return jsonify({'sucesso': True})

        except Exception as e:
            db.session.rollback()
            return jsonify({'sucesso': False, 'erro': "Assinado, mas erro ao salvar histórico."})
    else:
        return jsonify({'sucesso': False, 'erro': resultado.get('erro', 'Erro desconhecido')})


@solicitacoes_bp.route('/api/solicitar-empenho', methods=['POST'])
@login_required
@requires_permission('solicitacoes.editar')
def api_solicitar_empenho():
    """Registra solicitação de empenho e notifica financeiro."""
    data = request.get_json() or {}
    solicitacao_id = data.get('solicitacao_id')
    valor = data.get('valor', 0.0)

    current_app.logger.info(f"[EMPENHO] Recebido: solicitacao_id={solicitacao_id}, valor={valor}")

    if not solicitacao_id:
        return jsonify({'sucesso': False, 'msg': 'ID da solicitação não informado'})

    solicitacao = Solicitacao.query.get(solicitacao_id)
    if not solicitacao:
        return jsonify({'sucesso': False, 'msg': 'Solicitação não encontrada'})

    # Converte valor formatado "1.234,56" para float
    if isinstance(valor, str):
        valor = valor.replace('.', '').replace(',', '.')
    valor_float = float(valor)

    current_app.logger.info(f"[EMPENHO] valor_float={valor_float}, contrato={solicitacao.codigo_contrato}")

    resultado = SaldoService.registrar_e_atualizar_saldo(
        solicitacao,
        current_user.id,
        valor_float,
        validar_saldo=False  # Solicitação é um pedido, não exige saldo prévio
    )

    current_app.logger.info(f"[EMPENHO] Resultado SaldoService: {resultado}")

    if resultado.get('sucesso'):
        # Atualiza status para "Solicitado" (id=1)
        solicitacao.status_empenho_id = 1
        db.session.commit()
        current_app.logger.info(f"[EMPENHO] status_empenho_id atualizado para 1")

        # Notifica usuarios do modulo financeiro
        try:
            from app.services.notification_engine import NotificationEngine
            dest = NotificationEngine.resolver_destinatarios(
                'financeiro.nova_solicitacao',
                codigo_contrato=solicitacao.codigo_contrato,
            )
            current_app.logger.info(f"[EMPENHO] Destinatarios notificacao: {dest}")
            if dest:
                valor_fmt = f"R$ {valor_float:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                contratado = solicitacao.contrato.nomeContratadoResumido or solicitacao.contrato.nomeContratado if solicitacao.contrato else solicitacao.codigo_contrato
                NotificationEngine.notificar(
                    tipo_codigo='financeiro.nova_solicitacao',
                    destinatarios=dest,
                    titulo='Novo Empenho Solicitado',
                    mensagem=f'{contratado} — {valor_fmt} (Comp. {solicitacao.competencia})',
                    ref_modulo='financeiro',
                    ref_id=str(solicitacao.id),
                    ref_url='/financeiro/pendencias_ne',
                )
                current_app.logger.info(f"[EMPENHO] Notificacao enviada com sucesso")
        except Exception as e:
            current_app.logger.error(f"[EMPENHO] Erro ao notificar: {e}")

    return jsonify(resultado)


@solicitacoes_bp.route('/api/obter-saldo')
@login_required
@requires_permission('solicitacoes.visualizar')
def api_obter_saldo():
    """Obtém saldo de um contrato."""
    codigo_contrato = request.args.get('contrato', '').strip()
    competencia = request.args.get('competencia', '').strip()

    if not codigo_contrato or not competencia:
        return jsonify({'sucesso': False, 'msg': 'Parâmetros incompletos'})

    saldo_info = SaldoService.obter_resumo_saldo(codigo_contrato, competencia)

    return jsonify({
        'sucesso': True,
        **saldo_info
    })


# =============================================================================
# FUNÇÕES AUXILIARES DE SINCRONIZAÇÃO
# =============================================================================

def baixar_documentos_thread(app_obj, protocolo, token_sei, base_url):
    """
    Baixa documentos da API SEI para um protocolo e popula a tabela seimovimentacao.
    Executa em thread separada com contexto Flask próprio.
    """
    with app_obj.app_context():
        start_time = time.time()
        protocolo_limpo = "".join(filter(str.isdigit, protocolo))

        headers = {'token': token_sei, 'Accept': 'application/json'}
        params = {
            "protocolo_procedimento": protocolo_limpo,
            "pagina": 1,
            "quantidade": 1000,
            "sinal_completo": "N"
        }

        try:
            resp = http_requests.get(base_url, headers=headers, params=params, timeout=120)
            tempo_total = round(time.time() - start_time, 3)

            if resp.status_code == 200:
                data_json = resp.json()

                # Busca a lista de documentos
                items_found = []
                if isinstance(data_json, dict):
                    items_found = data_json.get('Documentos', [])
                    if not items_found and 'resultados' in data_json:
                        items_found = data_json['resultados']
                elif isinstance(data_json, list):
                    items_found = data_json

                if items_found:
                    for doc in items_found:
                        serie = doc.get('Serie', {})
                        unidade = doc.get('UnidadeElaboradora', {})

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
                            obs=""
                        )
                        db.session.add(novo_mov)

                    db.session.commit()
                    return (True, f"OK: {protocolo}")
                else:
                    return (True, f"Vazio: {protocolo}")
            else:
                app_obj.logger.warning(f"API SEI retornou status {resp.status_code} para o protocolo {protocolo}")
                return (False, f"Erro API {protocolo}: {resp.status_code}")

        except Exception as e:
            db.session.rollback()
            app_obj.logger.error(f"ERRO DOWNLOAD SEI (Protocolo {protocolo}): {str(e)}", exc_info=True)
            return (False, f"Erro {protocolo}: {str(e)}")


def processar_item_sei(app_obj, sol_id, token_sei, usuario_id, mapa_ordem):
    """
    Lê a tabela seimovimentacao e aplica as regras de negócio
    para avançar etapas na timeline de cada solicitação.
    """
    with app_obj.app_context():
        sol = Solicitacao.query.get(sol_id)
        if not sol or not sol.protocolo_gerado_sei:
            return None

        # ==============================================================================
        # 1. CONSTANTES
        # ==============================================================================
        SERIE_SOLICITACAO = SerieDocumentoSEI.SOLICITACAO
        SERIE_EMAIL = SerieDocumentoSEI.EMAIL
        SERIE_REQUERIMENTO = SerieDocumentoSEI.REQUERIMENTO
        SERIE_ATESTO_FISCAL = SerieDocumentoSEI.ATESTO_FISCAL
        SERIE_ATESTO_GESTOR = SerieDocumentoSEI.ATESTO_GESTOR
        SERIE_NOTA_EMPENHO = SerieDocumentoSEI.NOTA_EMPENHO
        SERIE_LIQUIDACAO = SerieDocumentoSEI.LIQUIDACAO
        SERIE_PD = SerieDocumentoSEI.PD
        SERIE_OB = SerieDocumentoSEI.OB

        MAPA_ORDEM_LOCAL = {
            1: 1,    # Solicitação Criada
            2: 2,    # Documentação Solicitada
            8: 3,    # Documentação Recebida
            15: 4,   # Solicitação da NF
            12: 5,   # Fiscais Notificados
            13: 6,   # Contrato Fiscalizado
            14: 7,   # Atestado pelo Controle Interno
            11: 8,   # NF Atestada
            5: 9,    # Liquidado
            6: 10    # Pago
        }

        mudou = False

        # ==============================================================================
        # 2. FUNÇÕES AUXILIARES INTERNAS
        # ==============================================================================
        def extrair_data_segura(doc_obj):
            if not doc_obj or not doc_obj.data:
                return None
            try:
                raw = str(doc_obj.data).strip().replace("'", "").replace('"', "")
                data_str = raw.split(' ')[0]
                for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
                    try:
                        return datetime.strptime(data_str, fmt)
                    except ValueError:
                        continue
                return None
            except (AttributeError, TypeError):
                return None

        def registrar_historico_forcado(id_etapa, data_doc, msg):
            if not data_doc:
                return False
            hist = HistoricoMovimentacao.query.filter_by(
                id_solicitacao=sol.id, id_etapa_nova=id_etapa
            ).first()
            if hist:
                data_banco = hist.data_movimentacao
                if data_banco is None or data_banco.date() != data_doc.date():
                    hist.data_movimentacao = data_doc
                    hist.comentario = msg
                    return True
                return False
            novo = HistoricoMovimentacao(
                id_solicitacao=sol.id,
                id_etapa_anterior=sol.etapa_atual_id,
                id_etapa_nova=id_etapa,
                id_usuario_responsavel=usuario_id,
                data_movimentacao=data_doc,
                comentario=msg
            )
            db.session.add(novo)
            return True

        def tentar_avancar_status(id_nova_etapa):
            ordem_atual = MAPA_ORDEM_LOCAL.get(sol.etapa_atual_id, 0)
            ordem_nova = MAPA_ORDEM_LOCAL.get(id_nova_etapa, 0)
            if ordem_nova > ordem_atual:
                sol.etapa_atual_id = id_nova_etapa
                return True
            return False

        def calcular_tempo_total(data_fim):
            if not data_fim:
                return
            if sol.tempo_total:
                return
            try:
                # Usa a data da PRIMEIRA movimentação histórica como início real do processo
                primeiro_hist = HistoricoMovimentacao.query.filter_by(
                    id_solicitacao=sol.id
                ).order_by(HistoricoMovimentacao.data_movimentacao.asc()).first()

                if primeiro_hist and primeiro_hist.data_movimentacao:
                    data_inicio = primeiro_hist.data_movimentacao
                else:
                    data_inicio = sol.data_solicitacao

                if not data_inicio:
                    return

                diferenca = data_fim - data_inicio
                dias = max(diferenca.days, 0)
                sol.tempo_total = f"{dias} dias"
            except (TypeError, AttributeError):
                pass

        # ==============================================================================
        # 3. CARREGAMENTO DOS DADOS
        # ==============================================================================
        try:
            docs = SeiMovimentacao.query.filter_by(
                protocolo_procedimento=sol.protocolo_gerado_sei
            ).order_by(SeiMovimentacao.id_documento.asc()).all()

            if not docs:
                return None

            ids_series = [str(d.id_serie) for d in docs]
            emails = [d for d in docs if str(d.id_serie) == SERIE_EMAIL]

            # ==================================================================
            # 4. LÓGICA DE NEGÓCIO
            # ==================================================================

            # --- A. ETAPA 8 e 12: DOCUMENTAÇÃO RECEBIDA (Requerimento - 64) ---
            if SERIE_REQUERIMENTO in ids_series:
                doc_req = next((d for d in docs if str(d.id_serie) == SERIE_REQUERIMENTO), None)
                dt = extrair_data_segura(doc_req)
                if registrar_historico_forcado(8, dt, "Documentação Recebida (Req. 64)"):
                    mudou = True
                if registrar_historico_forcado(12, dt, "Fiscais Notificados (Automático)"):
                    mudou = True
                if tentar_avancar_status(12):
                    mudou = True
                elif tentar_avancar_status(8):
                    mudou = True

            # --- B. ETAPA 11: NF ATESTADA ---
            if len(emails) >= 2:
                try:
                    segundo_email = emails[1]
                    idx_2_email = docs.index(segundo_email)
                    doc_alvo = None
                    for i in range(idx_2_email + 1, len(docs)):
                        if str(docs[i].id_serie) != SERIE_EMAIL:
                            doc_alvo = docs[i]
                            break
                    if doc_alvo:
                        dt = extrair_data_segura(doc_alvo)
                        if registrar_historico_forcado(11, dt, f"NF Atestada (Doc {doc_alvo.serie_nome})"):
                            mudou = True
                            tentar_avancar_status(11)
                except ValueError:
                    pass

            # --- C. FINANCEIRO (OB > PD > NL > NE) ---
            doc_ne = next((d for d in docs if str(d.id_serie) == SERIE_NOTA_EMPENHO), None)
            doc_nl = next((d for d in docs if str(d.id_serie) == SERIE_LIQUIDACAO), None)
            doc_pd = next((d for d in docs if str(d.id_serie) == SERIE_PD), None)
            doc_ob = next((d for d in docs if str(d.id_serie) == SERIE_OB), None)

            # Atualização dos números (NE, NL, PD, OB)
            if doc_ne and doc_ne.numero and sol.num_ne != str(doc_ne.numero):
                sol.num_ne = str(doc_ne.numero)
                # Atualiza status_empenho para Atendido (2) se tiver NE no SEI
                if sol.status_empenho_id != 2:
                    sol.status_empenho_id = 2
                mudou = True

            if doc_nl and doc_nl.numero and sol.num_nl != str(doc_nl.numero):
                sol.num_nl = str(doc_nl.numero)
                mudou = True

            if doc_pd and doc_pd.numero and sol.num_pd != str(doc_pd.numero):
                sol.num_pd = str(doc_pd.numero)
                mudou = True

            if doc_ob and doc_ob.numero and sol.num_ob != str(doc_ob.numero):
                sol.num_ob = str(doc_ob.numero)
                mudou = True

            # Lógica de Avanço Financeiro
            fin_etapa = None
            fin_doc = None
            msg_fin = ""

            if doc_ob:
                fin_etapa = 6
                fin_doc = doc_ob
                msg_fin = f"Pago (OB {sol.num_ob})"
                if sol.status_geral != 'PAGO':
                    sol.status_geral = 'PAGO'
                    mudou = True
                dt_ob = extrair_data_segura(doc_ob)
                if registrar_historico_forcado(5, dt_ob, "Liquidado (Via Pagamento)"):
                    mudou = True
                calcular_tempo_total(dt_ob)

            elif doc_pd:
                fin_etapa = 5
                fin_doc = doc_pd
                msg_fin = f"Programado (PD {sol.num_pd})"
                if sol.status_geral != 'EM LIQUIDAÇÃO':
                    sol.status_geral = 'EM LIQUIDAÇÃO'
                    mudou = True

            elif doc_nl:
                fin_etapa = 5
                fin_doc = doc_nl
                msg_fin = f"Liquidado (NL {sol.num_nl})"
                if sol.status_geral != 'EM LIQUIDAÇÃO':
                    sol.status_geral = 'EM LIQUIDAÇÃO'
                    mudou = True

            if fin_etapa:
                dt = extrair_data_segura(fin_doc)
                if registrar_historico_forcado(fin_etapa, dt, msg_fin):
                    mudou = True
                    tentar_avancar_status(fin_etapa)

            # --- D. OUTRAS ETAPAS ---
            if SERIE_SOLICITACAO in ids_series:
                d = next(d for d in docs if str(d.id_serie) == SERIE_SOLICITACAO)
                if registrar_historico_forcado(1, extrair_data_segura(d), "Solicitação Criada"):
                    mudou = True
                    tentar_avancar_status(1)

            if len(emails) >= 1:
                if registrar_historico_forcado(2, extrair_data_segura(emails[0]), "Doc Solicitada"):
                    mudou = True
                    tentar_avancar_status(2)

            if len(emails) >= 2:
                if registrar_historico_forcado(15, extrair_data_segura(emails[1]), "Solicitação NF"):
                    mudou = True
                    tentar_avancar_status(15)

            if SERIE_ATESTO_GESTOR in ids_series:
                d = next(d for d in docs if str(d.id_serie) == SERIE_ATESTO_GESTOR)
                if registrar_historico_forcado(13, extrair_data_segura(d), "Atesto Gestor"):
                    mudou = True
                    tentar_avancar_status(13)

            if SERIE_ATESTO_FISCAL in ids_series:
                d = next(d for d in docs if str(d.id_serie) == SERIE_ATESTO_FISCAL)
                if registrar_historico_forcado(14, extrair_data_segura(d), "Atesto Fiscal"):
                    mudou = True
                    tentar_avancar_status(14)

            # ==================================================================
            # 5. FINALIZAÇÃO
            # ==================================================================
            if mudou:
                db.session.commit()

                # Hook: notificacoes de avanco via SEI
                try:
                    from app.services.notification_engine import NotificationEngine

                    ref_url = f'/solicitacoes/solicitacao/{sol.id}'
                    contrato_cod = sol.codigo_contrato or ''

                    from app.models import Etapa as EtapaModel
                    etapa = EtapaModel.query.get(sol.etapa_atual_id)
                    nome_etapa = etapa.nome if etapa else f'Etapa {sol.etapa_atual_id}'

                    dest = NotificationEngine.resolver_destinatarios(
                        'solicitacao.etapa_avancou',
                        codigo_contrato=contrato_cod,
                        usuario_criador_id=sol.id_usuario_solicitante,
                    )
                    if dest:
                        NotificationEngine.notificar(
                            tipo_codigo='solicitacao.etapa_avancou',
                            destinatarios=dest,
                            titulo=f'Etapa atualizada: {nome_etapa}',
                            mensagem=f'Solicitacao #{sol.id} - Contrato {contrato_cod} (via sync SEI)',
                            ref_modulo='solicitacoes',
                            ref_id=str(sol.id),
                            ref_url=ref_url,
                        )

                    # Se PAGO, notificacao especifica
                    if sol.etapa_atual_id == 6:
                        dest_pago = NotificationEngine.resolver_destinatarios(
                            'solicitacao.paga',
                            codigo_contrato=contrato_cod,
                            usuario_criador_id=sol.id_usuario_solicitante,
                        )
                        if dest_pago:
                            NotificationEngine.notificar(
                                tipo_codigo='solicitacao.paga',
                                destinatarios=dest_pago,
                                titulo='Pagamento efetuado',
                                mensagem=f'Solicitacao #{sol.id} - Contrato {contrato_cod} foi paga.',
                                ref_modulo='solicitacoes',
                                ref_id=str(sol.id),
                                ref_url=ref_url,
                            )
                except Exception as notif_err:
                    print(f"Aviso: erro notificacao SEI {sol_id}: {notif_err}")

                return sol.protocolo_gerado_sei

            return None

        except Exception as e:
            db.session.rollback()
            print(f"Erro processamento {sol_id}: {e}")
            return None


# =============================================================================
# FASE 1: DOWNLOAD DE DOCUMENTOS DO SEI → TABELA seimovimentacao
# =============================================================================

@solicitacoes_bp.route('/api/sincronizar-documentos', methods=['GET', 'POST'])
@login_required
@requires_permission('solicitacoes.aprovar')
def api_sincronizar_documentos():
    """
    FASE 1: Baixa documentos da API SEI e popula a tabela seimovimentacao.
    - Filtra solicitações pendentes (etapa != 6, não canceladas, com protocolo)
    - Limpa movimentações antigas desses protocolos
    - Baixa documentos atualizados da API SEI via threads paralelas
    """
    app_real = current_app._get_current_object()

    def generate():
        with app_real.app_context():
            yield f"data: {json.dumps({'msg': 'Autenticando...', 'progresso': 5})}\n\n"

            token_sei = session.get('sei_token') or gerar_token_sei_admin()

            if not token_sei:
                yield f"data: {json.dumps({'msg': 'Erro: Token SEI não disponível', 'progresso': 0})}\n\n"
                return

            # Busca solicitações pendentes com protocolo válido
            solicitacoes_pendentes = Solicitacao.query.filter(
                Solicitacao.protocolo_gerado_sei.isnot(None),
                Solicitacao.etapa_atual_id != 6,
                Solicitacao.status_geral != 'CANCELADO'
            ).all()

            total = len(solicitacoes_pendentes)

            if total == 0:
                yield f"data: {json.dumps({'msg': 'Todos os processos já estão concluídos. Nada a sincronizar.', 'progresso': 100, 'concluido': True})}\n\n"
                return

            # Extrai protocolos para limpeza seletiva
            lista_protocolos = [s.protocolo_gerado_sei for s in solicitacoes_pendentes]

            # Limpeza seletiva: deleta movimentações dos processos que serão atualizados
            yield f"data: {json.dumps({'msg': f'Limpando dados de {total} processos em andamento...', 'progresso': 10})}\n\n"
            try:
                db.session.query(SeiMovimentacao).filter(
                    SeiMovimentacao.protocolo_procedimento.in_(lista_protocolos)
                ).delete(synchronize_session=False)
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                yield f"data: {json.dumps({'msg': f'Erro ao limpar banco: {str(e)}', 'progresso': 0})}\n\n"
                return

            base_url = "https://api.sei.pi.gov.br/v1/unidades/110006213/procedimentos/documentos"

            # Download via threads paralelas
            yield f"data: {json.dumps({'msg': 'Iniciando download dos documentos...', 'progresso': 15})}\n\n"

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_prot = {
                    executor.submit(
                        baixar_documentos_thread,
                        app_real,
                        s.protocolo_gerado_sei,
                        token_sei,
                        base_url
                    ): s.protocolo_gerado_sei
                    for s in solicitacoes_pendentes
                }

                completed = 0
                sucessos = 0

                for future in concurrent.futures.as_completed(future_to_prot):
                    protocolo = future_to_prot[future]
                    completed += 1
                    percentual = 15 + int((completed / total) * 85)

                    try:
                        sucesso, mensagem = future.result()

                        if sucesso:
                            sucessos += 1
                            if completed % 5 == 0:
                                yield f"data: {json.dumps({'progresso': percentual, 'msg': f'Baixando... ({completed}/{total})'})}\n\n"
                        else:
                            yield f"data: {json.dumps({'progresso': percentual, 'msg': f'[ALERTA] {mensagem}'})}\n\n"

                    except Exception as exc:
                        yield f"data: {json.dumps({'progresso': percentual, 'msg': f'Erro thread {protocolo}: {str(exc)}'})}\n\n"

            yield f"data: {json.dumps({'msg': f'Download concluído! {sucessos}/{total} protocolos atualizados.', 'progresso': 100, 'concluido': True})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


# =============================================================================
# FASE 2: LÊ seimovimentacao E APLICA REGRAS DE NEGÓCIO (AVANÇA ETAPAS)
# =============================================================================

@solicitacoes_bp.route('/api/atualizar-etapas-sei', methods=['GET', 'POST'])
@login_required
@requires_permission('solicitacoes.aprovar')
def api_atualizar_etapas():
    """
    FASE 2: Lê a tabela seimovimentacao e aplica regras de negócio
    para avançar os cards na timeline.
    - Ignora processos com etapa_atual_id == 6 (PAGO/OB)
    - Usa threads paralelas para performance
    """
    app_real = current_app._get_current_object()
    usuario_id = current_user.id

    def generate():
        with app_real.app_context():
            # Mapa de ordem das etapas
            todas_etapas = Etapa.query.all()
            mapa_ordem = {e.id: e.ordem for e in todas_etapas}

            # Filtra solicitações pendentes
            ids_para_processar = [
                s.id for s in Solicitacao.query.filter(
                    Solicitacao.etapa_atual_id != 6,
                    Solicitacao.status_geral != 'CANCELADO'
                ).all()
            ]

            total = len(ids_para_processar)

            if total == 0:
                yield f"data: {json.dumps({'progresso': 100, 'msg': 'Todos os processos já estão atualizados.', 'concluido': True})}\n\n"
                return

            yield f"data: {json.dumps({'progresso': 0, 'msg': f'Iniciando análise de {total} processos pendentes...'})}\n\n"

            token_sei = session.get('sei_token')

            # Threads paralelas para processar etapas
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                future_to_id = {
                    executor.submit(
                        processar_item_sei,
                        app_real,
                        sid,
                        token_sei,
                        usuario_id,
                        mapa_ordem
                    ): sid for sid in ids_para_processar
                }

                completed_count = 0
                atualizados_count = 0

                for future in concurrent.futures.as_completed(future_to_id):
                    completed_count += 1
                    percentual = int((completed_count / total) * 100)

                    try:
                        resultado = future.result()
                        if resultado:
                            atualizados_count += 1
                            msg = f"Movimentado: {resultado}"
                            yield f"data: {json.dumps({'progresso': percentual, 'msg': msg})}\n\n"
                        else:
                            if completed_count % 5 == 0:
                                yield f"data: {json.dumps({'progresso': percentual, 'msg': f'Analisando... {completed_count}/{total}'})}\n\n"

                    except Exception as exc:
                        print(f"Erro na thread: {exc}")
                        yield f"data: {json.dumps({'progresso': percentual, 'msg': 'Erro ao processar um item.'})}\n\n"

            yield f"data: {json.dumps({'progresso': 100, 'msg': f'Concluído! {atualizados_count} processos avançaram de etapa.', 'concluido': True})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


# =============================================================================
# FASE 3: ATUALIZAÇÃO DE SALDOS SIAFE
# =============================================================================

@solicitacoes_bp.route('/api/atualizar-todos-saldos', methods=['POST'])
@login_required
@requires_permission('solicitacoes.aprovar')
def api_atualizar_todos_saldos():
    """Atualiza saldos de todos os contratos."""
    def generate():
        yield f"data: {json.dumps({'msg': 'Atualizando saldos...', 'progresso': 5})}\n\n"

        try:
            # Busca combinações únicas de contrato/competência
            combinacoes = db.session.query(
                Solicitacao.codigo_contrato,
                Solicitacao.competencia
            ).distinct().filter(
                Solicitacao.competencia.isnot(None)
            ).all()

            total = len(combinacoes)
            atualizados = 0
            erros = 0

            if total == 0:
                yield f"data: {json.dumps({'msg': 'Nenhum saldo para atualizar.', 'progresso': 100, 'concluido': True})}\n\n"
                return

            yield f"data: {json.dumps({'msg': f'Calculando saldos de {total} combinações...', 'progresso': 10})}\n\n"

            for i, (contrato, competencia) in enumerate(combinacoes):
                try:
                    SaldoService.atualizar_saldo_contrato(contrato, competencia)
                    atualizados += 1
                except Exception:
                    erros += 1

                # Atualiza progresso a cada 10 itens para não sobrecarregar o SSE
                if (i + 1) % 10 == 0 or (i + 1) == total:
                    progresso = 10 + int(((i + 1) / total) * 85)
                    yield f"data: {json.dumps({'msg': f'Processados {i+1}/{total}...', 'progresso': progresso})}\n\n"

            msg_final = f'Concluído! {atualizados} saldos atualizados.'
            if erros:
                msg_final += f' ({erros} erros)'
            yield f"data: {json.dumps({'msg': msg_final, 'progresso': 100, 'concluido': True})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'msg': f'Erro: {str(e)}', 'progresso': 0})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


# =============================================================================
# ATUALIZAÇÃO INDIVIDUAL (ETAPA + SALDO)
# =============================================================================

@solicitacoes_bp.route('/api/atualizar-individual/<int:id_solicitacao>', methods=['POST'])
@login_required
@requires_permission('solicitacoes.aprovar')
def api_atualizar_individual(id_solicitacao):
    """Atualiza etapas SEI e saldo de empenho de uma única solicitação."""
    from app.utils.permissions import requires_admin  # noqa: F811
    if not current_user.is_admin:
        return jsonify({'sucesso': False, 'msg': 'Acesso restrito a administradores.'}), 403

    sol = Solicitacao.query.get(id_solicitacao)
    if not sol:
        return jsonify({'sucesso': False, 'msg': 'Solicitação não encontrada.'}), 404

    app_real = current_app._get_current_object()
    usuario_id = current_user.id
    token_sei = session.get('sei_token')
    todas_etapas = Etapa.query.all()
    mapa_ordem = {e.id: e.ordem for e in todas_etapas}

    msgs = []

    # 1. Atualizar etapas via SEI
    try:
        resultado = processar_item_sei(app_real, sol.id, token_sei, usuario_id, mapa_ordem)
        if resultado:
            msgs.append(f'Etapa atualizada: {resultado}')
        else:
            msgs.append('Etapas: sem alterações.')
    except Exception as e:
        msgs.append(f'Erro ao atualizar etapas: {str(e)}')

    # 2. Atualizar saldo de empenho
    try:
        if sol.codigo_contrato and sol.competencia:
            SaldoService.atualizar_saldo_contrato(sol.codigo_contrato, sol.competencia)
            msgs.append('Saldo de empenho atualizado.')
        else:
            msgs.append('Saldo: dados insuficientes (contrato/competência).')
    except Exception as e:
        msgs.append(f'Erro ao atualizar saldo: {str(e)}')

    return jsonify({'sucesso': True, 'msg': ' | '.join(msgs)})


# =============================================================================
# CRIAÇÃO EM LOTE
# =============================================================================

@solicitacoes_bp.route('/api/criar-lote', methods=['POST'])
@login_required
@requires_permission('solicitacoes.criar')
def api_criar_lote():
    """
    Cria e assina solicitações de pagamento em lote via SSE.
    Para cada contrato: cria processo SEI + documento + assina + salva no banco com status ABERTO.
    """
    dados = request.get_json() or {}
    codigos_contratos = dados.get('contratos', [])
    competencia = dados.get('competencia', '').strip()
    id_tipo_pagamento = dados.get('id_tipo_pagamento')
    unidade_id = dados.get('unidade_id', '').strip()

    if not codigos_contratos or not competencia or not unidade_id:
        return jsonify({'sucesso': False, 'erro': 'Dados incompletos.'}), 400

    app_real = current_app._get_current_object()
    usuario_id = current_user.id
    usuario_nome = session.get('usuario_nome', current_user.nome if hasattr(current_user, 'nome') else '')
    usuario_cargo = session.get('usuario_cargo', 'Colaborador')
    sei_token = session.get('sei_token') or gerar_token_sei_admin()

    def generate():
        with app_real.app_context():
            if not sei_token:
                yield f"data: {json.dumps({'msg': 'Erro: Não foi possível autenticar no SEI.', 'progresso': 0, 'erro_fatal': True})}\n\n"
                return

            total = len(codigos_contratos)
            total_criados = 0
            erros_count = 0

            yield f"data: {json.dumps({'msg': f'Iniciando criação de {total} processos...', 'progresso': 5})}\n\n"

            for i, codigo in enumerate(codigos_contratos):
                progresso = 5 + int(((i + 1) / total) * 90)

                try:
                    # 1. Busca contrato
                    contrato = ContratoRepository.buscar_por_codigo(codigo)
                    if not contrato:
                        erros_count += 1
                        yield f"data: {json.dumps({'msg': f'❌ Contrato {codigo} não encontrado.', 'progresso': progresso, 'contrato': codigo, 'sucesso': False})}\n\n"
                        continue

                    # 2. Cria Processo no SEI
                    dados_contrato_api = {
                        'numeroOriginal': contrato.numeroOriginal,
                        'nomeContratado': contrato.nomeContratado,
                        'codigo': contrato.codigo,
                        'nomeContratadoResumido': contrato.nomeContratadoResumido
                    }
                    proc_criado = criar_procedimento_pagamento(
                        sei_token, unidade_id, dados_contrato_api, competencia
                    )

                    if not proc_criado:
                        erros_count += 1
                        yield f"data: {json.dumps({'msg': f'❌ Erro ao criar processo SEI para {codigo}.', 'progresso': progresso, 'contrato': codigo, 'sucesso': False})}\n\n"
                        continue

                    # 3. Gera Documento (Requerimento)
                    ctx_doc = {
                        'num_contrato': contrato.numeroOriginal,
                        'empresa': contrato.nomeContratado,
                        'competencia': competencia,
                        'usuario_nome': usuario_nome,
                        'usuario_cargo': usuario_cargo,
                        'objeto': contrato.objeto or 'Objeto não informado'
                    }
                    doc_criado = gerar_documento_pagamento(
                        sei_token, unidade_id, proc_criado['IdProcedimento'], ctx_doc
                    )

                    protocolo_formatado = proc_criado.get('ProcedimentoFormatado', '')

                    # 4. Salva Solicitação no banco
                    nova_sol = Solicitacao(
                        codigo_contrato=codigo,
                        id_usuario_solicitante=usuario_id,
                        protocolo_gerado_sei=protocolo_formatado,
                        id_procedimento_sei=proc_criado.get('IdProcedimento'),
                        link_processo_sei=proc_criado.get('LinkAcesso'),
                        especificacao=proc_criado.get('EspecificacaoGerada'),
                        competencia=competencia,
                        id_caixa_sei=unidade_id,
                        id_tipo_pagamento=id_tipo_pagamento,
                        descricao=f'Solicitação de Pagamento - {competencia}',
                        status_geral='ABERTO',
                        criado_em_lote=True,
                        data_solicitacao=datetime.now()
                    )
                    db.session.add(nova_sol)
                    db.session.flush()

                    # 5. Cria histórico e saldo
                    novo_historico = HistoricoMovimentacao(
                        id_solicitacao=nova_sol.id,
                        id_etapa_anterior=None,
                        id_etapa_nova=1,
                        id_usuario_responsavel=usuario_id,
                        data_movimentacao=datetime.now(),
                        comentario=f"Processo criado em lote."
                    )
                    db.session.add(novo_historico)
                    SaldoService.registrar_e_atualizar_saldo(nova_sol, usuario_id, valor_solicitado=0.0, validar_saldo=False)

                    db.session.commit()
                    total_criados += 1

                    link_sei = proc_criado.get('LinkAcesso', '')
                    yield f"data: {json.dumps({'msg': f'✅ {codigo} → {protocolo_formatado}', 'progresso': progresso, 'contrato': codigo, 'protocolo': protocolo_formatado, 'contratado': contrato.nomeContratado, 'link_sei': link_sei, 'sucesso': True})}\n\n"

                except Exception as e:
                    db.session.rollback()
                    erros_count += 1
                    app_real.logger.error(f'Erro lote contrato {codigo}: {e}', exc_info=True)
                    yield f"data: {json.dumps({'msg': f'❌ Erro em {codigo}: {str(e)[:80]}', 'progresso': progresso, 'contrato': codigo, 'sucesso': False})}\n\n"

            # Resumo final
            msg_final = f'Concluído: {total_criados}/{total} processos criados.'
            if erros_count:
                msg_final += f' ({erros_count} com erro)'

            yield f"data: {json.dumps({'msg': msg_final, 'progresso': 100, 'concluido': True, 'total_criados': total_criados, 'total_erros': erros_count})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


