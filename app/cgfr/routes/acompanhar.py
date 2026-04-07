"""
Rota de acompanhamento de processo CGFR com timeline.
Inclui sync individual e bulk (paralelo) de documentos SEI.

Usa sei_integration.listar_documentos_procedimento_sei() (compartilhada)
para chamada API + retry. Persiste em CgfrMovimentacao (tabela propria do CGFR).
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as http_requests
import urllib3
from flask import render_template, abort, jsonify, current_app, Response, stream_with_context
from flask_login import login_required

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from app.cgfr.routes import cgfr_bp
from app.cgfr.models import CgfrProcessoEnviado, CgfrMovimentacao
from app.cgfr.services.processo_service import obter_timeline_acompanhamento
from app.extensions import db
from app.services.sei_auth import gerar_token_sei_admin
from app.services.sei_integration import consultar_procedimento_sei, listar_documentos_procedimento_sei
from app.utils.permissions import requires_permission

logger = logging.getLogger(__name__)

# Constantes
_SEI_TIMEOUT = 60
_MAX_RETRIES = 3
_MAX_WORKERS = 8


# ---------------------------------------------------------------------------
# Helpers compartilhados
# ---------------------------------------------------------------------------

def _salvar_docs_cgfr(protocolo, documentos):
    """Persiste lista de dicts de documentos SEI em CgfrMovimentacao.

    Args:
        protocolo: processo_formatado (PK do processo).
        documentos: lista de dicts retornada por listar_documentos_procedimento_sei().

    Returns:
        (success: bool, msg: str)
    """
    try:
        CgfrMovimentacao.query.filter_by(protocolo_procedimento=protocolo).delete()

        for doc in documentos:
            serie = doc.get('Serie', {})
            unidade = doc.get('UnidadeElaboradora', {})
            id_serie_val = None
            if serie.get('IdSerie') and str(serie.get('IdSerie')).isdigit():
                id_serie_val = int(serie['IdSerie'])

            mov = CgfrMovimentacao(
                id_documento=str(doc.get('IdDocumento', '')),
                protocolo_procedimento=protocolo,
                id_procedimento=str(doc.get('IdProcedimento', '')),
                procedimento_formatado=str(doc.get('ProcedimentoFormatado', '')),
                documento_formatado=str(doc.get('DocumentoFormatado', '')),
                link_acesso=doc.get('LinkAcesso'),
                descricao=doc.get('Descricao'),
                data=doc.get('Data') or doc.get('DataGeracao'),
                numero=doc.get('Numero'),
                id_serie=id_serie_val,
                serie_nome=serie.get('Nome'),
                serie_aplicabilidade=serie.get('Aplicabilidade'),
                unidade_id=str(unidade.get('IdUnidade', '')),
                unidade_sigla=unidade.get('Sigla'),
                unidade_descricao=unidade.get('Descricao'),
                obs='',
            )
            db.session.add(mov)

        db.session.commit()
        return (True, f'{len(documentos)} documentos sincronizados')
    except Exception as e:
        db.session.rollback()
        return (False, f'Erro ao salvar documentos: {e}')


def _update_processo_from_sei(protocolo, token_sei, app_obj):
    """Consulta API SEI procedimento e atualiza campos sync do CgfrProcessoEnviado.
    Preenche link_acesso e outros campos que o sync Trino pode trazer vazios.
    """
    resultado = consultar_procedimento_sei(token_sei, protocolo)
    if not resultado.get('sucesso'):
        logger.warning(f"[{protocolo}] Consulta procedimento falhou: {resultado.get('erro')}")
        return

    data = resultado.get('dados_procedimento', {})
    if not data:
        return

    with app_obj.app_context():
        try:
            processo = CgfrProcessoEnviado.query.get(protocolo)
            if not processo:
                return

            if data.get('LinkAcesso'):
                processo.link_acesso = data['LinkAcesso']
            if data.get('Especificacao'):
                processo.especificacao = data['Especificacao']
            if data.get('TipoProcedimento', {}).get('Nome'):
                processo.tipo_processo = data['TipoProcedimento']['Nome']

            geracao = data.get('AndamentoGeracao', {})
            if geracao:
                unidade_g = geracao.get('Unidade', {})
                usuario_g = geracao.get('Usuario', {})
                if unidade_g.get('IdUnidade'):
                    processo.id_unidade_geradora = str(unidade_g['IdUnidade'])
                if unidade_g.get('Sigla'):
                    processo.geracao_sigla = unidade_g['Sigla']
                if geracao.get('Descricao'):
                    processo.geracao_descricao = geracao['Descricao']
                if usuario_g.get('Nome'):
                    processo.usuario_gerador = usuario_g['Nome']

            ultimo = data.get('UltimoAndamento', {})
            if ultimo:
                unidade_u = ultimo.get('Unidade', {})
                usuario_u = ultimo.get('Usuario', {})
                if unidade_u.get('Sigla'):
                    processo.ultimo_andamento_sigla = unidade_u['Sigla']
                if ultimo.get('Descricao'):
                    processo.ultimo_andamento_descricao = ultimo['Descricao']
                if usuario_u.get('Nome'):
                    processo.ultimo_andamento_usuario = usuario_u['Nome']

            db.session.commit()
            logger.info(f"[{protocolo}] Campos sync do processo atualizados via API SEI")
        except Exception as e:
            db.session.rollback()
            logger.error(f"[{protocolo}] Erro ao atualizar processo: {e}")
        finally:
            db.session.remove()


def _fetch_and_save_docs(protocolo, token_sei, app_obj):
    """Busca documentos via sei_integration (compartilhada) e salva em CgfrMovimentacao.

    Executada em thread separada para bulk sync — por isso recebe app_obj
    e gerencia o app_context manualmente.

    Returns:
        (success: bool, msg: str)
    """
    resultado = listar_documentos_procedimento_sei(
        token_sei, protocolo, max_retries=_MAX_RETRIES, timeout=_SEI_TIMEOUT
    )

    if not resultado['sucesso']:
        return (False, resultado['erro'])

    documentos = resultado['documentos']

    with app_obj.app_context():
        try:
            return _salvar_docs_cgfr(protocolo, documentos)
        finally:
            db.session.remove()


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@cgfr_bp.route('/acompanhar/<path:protocolo>')
@login_required
@requires_permission('cgfr')
def acompanhar(protocolo):
    """Pagina de acompanhamento de processo CGFR com timeline."""
    dados = obter_timeline_acompanhamento(protocolo)
    if not dados:
        abort(404)

    return render_template(
        'cgfr/acompanhar.html',
        processo=dados['processo'],
        timeline_data=dados['timeline_data'],
        mov_cgfr=dados['mov_cgfr'],
        mov_nr=dados['mov_nr'],
        tem_orcamento=dados['tem_orcamento'],
    )


@cgfr_bp.route('/acompanhar/<path:protocolo>/sync', methods=['POST'])
@login_required
@requires_permission('cgfr')
def sync_documentos(protocolo):
    """Sincroniza documentos SEI de um processo CGFR via API SEI."""
    processo = CgfrProcessoEnviado.query.get(protocolo)
    if not processo:
        return jsonify({'success': False, 'error': 'Processo nao encontrado'}), 404

    try:
        token_sei = gerar_token_sei_admin()
        if not token_sei:
            return jsonify({'success': False, 'error': 'Falha ao obter token SEI'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erro autenticacao SEI: {str(e)}'}), 500

    app_obj = current_app._get_current_object()

    # 1. Atualiza campos sync do processo (link_acesso, especificacao, etc.)
    _update_processo_from_sei(protocolo, token_sei, app_obj)

    # 2. Sincroniza documentos (usa sei_integration compartilhada)
    success, msg = _fetch_and_save_docs(protocolo, token_sei, app_obj)

    if success:
        return jsonify({'success': True, 'message': msg})
    else:
        return jsonify({'success': False, 'error': msg}), 500


@cgfr_bp.route('/acompanhar/sync-bulk')
@login_required
@requires_permission('cgfr')
def sync_documentos_bulk():
    """Sincroniza documentos SEI de TODOS os processos via SSE para progresso em tempo real."""
    try:
        token_sei = gerar_token_sei_admin()
        if not token_sei:
            return jsonify({'success': False, 'error': 'Falha ao obter token SEI'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erro autenticacao SEI: {str(e)}'}), 500

    all_processos = CgfrProcessoEnviado.query.filter(
        CgfrProcessoEnviado.processo_formatado.like(f'{CgfrProcessoEnviado.PREFIXO_SEAD}%')
    ).all()
    protocolos = [p.processo_formatado for p in all_processos]

    if not protocolos:
        return jsonify({'success': True, 'message': 'Nenhum processo encontrado', 'total': 0, 'ok': 0, 'erros': 0})

    app_obj = current_app._get_current_object()

    def generate():
        total = len(protocolos)
        done = 0
        erros = 0

        yield f"data: {json.dumps({'msg': f'Iniciando sync de {total} processos...', 'progresso': 0})}\n\n"

        def _sync_full(proto, token, app):
            _update_processo_from_sei(proto, token, app)
            return _fetch_and_save_docs(proto, token, app)

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {
                executor.submit(_sync_full, proto, token_sei, app_obj): proto
                for proto in protocolos
            }

            for future in as_completed(futures):
                done += 1
                success, msg = future.result()
                proto = futures[future]
                if not success:
                    erros += 1

                progresso = int((done / total) * 100)
                status = 'OK' if success else 'ERRO'
                yield f"data: {json.dumps({'msg': f'[{status}] {proto} ({done}/{total})', 'progresso': progresso})}\n\n"

        yield f"data: {json.dumps({'msg': f'Concluido! {done - erros}/{total} sincronizados, {erros} erros', 'progresso': 100, 'concluido': True, 'total': total, 'ok': done - erros, 'erros': erros})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    )
