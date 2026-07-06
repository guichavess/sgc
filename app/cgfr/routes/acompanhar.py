"""
Rota de acompanhamento de processo CGFR com timeline.
Inclui sync individual e bulk (paralelo) de documentos SEI.

Usa sei_integration.listar_documentos_procedimento_sei() (compartilhada)
para chamada API + retry. Persiste em CgfrMovimentacao (tabela propria do CGFR).
"""
import json
import logging
import time
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
# Timeout por tentativa na listagem de docs SEI. Generoso de proposito:
# processos grandes (ate 1000 docs numa tacada) levam >60s pra responder e,
# como o sync individual agora e SSE com heartbeats, a conexao sobrevive alem
# do idle timeout do ALB (60s) — nao ha mais 504 por causa da espera.
_SEI_TIMEOUT = 180
_MAX_RETRIES = 3
_MAX_WORKERS = 8
# Intervalo (s) entre heartbeats do SSE enquanto a API SEI processa.
# Precisa ser < idle timeout do proxy/ALB (60s) para manter a conexao viva.
_SSE_HEARTBEAT = 15


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
        mov_cgfr_extra=dados.get('mov_cgfr_extra'),
        mov_nr=dados['mov_nr'],
        tem_orcamento=dados['tem_orcamento'],
    )


@cgfr_bp.route('/acompanhar/<path:protocolo>/sync', methods=['GET'])
@login_required
@requires_permission('cgfr')
def sync_documentos(protocolo):
    """Sincroniza documentos SEI de um processo CGFR via SSE (nao-bloqueante).

    A API SEI pode demorar minutos para responder. Se a requisicao ficar
    bloqueada esperando, o idle timeout do proxy/ALB (60s) corta a conexao e
    o navegador recebe 504 — mesmo que o Gunicorn conclua e salve os docs.

    Solucao: rodar a sincronizacao numa thread e transmitir o progresso via
    Server-Sent Events, com heartbeats periodicos (< 60s) que mantem a
    conexao viva enquanto o SEI processa. A resposta HTTP sai na hora.
    """
    processo = CgfrProcessoEnviado.query.get(protocolo)
    if not processo:
        return jsonify({'success': False, 'error': 'Processo nao encontrado'}), 404

    app_obj = current_app._get_current_object()

    def _sync_full():
        """Executado em thread separada; gerencia o proprio app_context."""
        with app_obj.app_context():
            token_sei = gerar_token_sei_admin()
        if not token_sei:
            return (False, 'Falha ao obter token SEI')
        # 1. Atualiza campos sync do processo (link_acesso, especificacao, etc.)
        _update_processo_from_sei(protocolo, token_sei, app_obj)
        # 2. Sincroniza documentos (usa sei_integration compartilhada)
        return _fetch_and_save_docs(protocolo, token_sei, app_obj)

    def generate():
        yield f"data: {json.dumps({'msg': 'Conectando ao SEI...', 'progresso': 5})}\n\n"

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_sync_full)

            # Heartbeats mantem a conexao viva enquanto o SEI processa,
            # evitando o idle timeout do ALB sem bloquear ate o fim da chamada.
            while not future.done():
                yield (
                    "data: "
                    + json.dumps({
                        'msg': 'Baixando documentos do SEI...',
                        'progresso': 50,
                        'heartbeat': True,
                    })
                    + "\n\n"
                )
                time.sleep(_SSE_HEARTBEAT)

            try:
                success, msg = future.result()
            except Exception as e:
                logger.exception('[CGFR] Erro inesperado ao sincronizar documentos')
                yield (
                    "data: "
                    + json.dumps({
                        'success': False,
                        'concluido': True,
                        'error': f'Erro inesperado ao sincronizar documentos: {e}',
                    })
                    + "\n\n"
                )
                return

        if success:
            yield (
                "data: "
                + json.dumps({
                    'success': True,
                    'concluido': True,
                    'progresso': 100,
                    'message': msg,
                })
                + "\n\n"
            )
        else:
            yield (
                "data: "
                + json.dumps({'success': False, 'concluido': True, 'error': msg})
                + "\n\n"
            )

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


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
        CgfrProcessoEnviado.filtro_visiveis()
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
