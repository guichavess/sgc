"""
Rota de acompanhamento de processo CGFR com timeline.
Inclui sync individual e bulk (paralelo) de documentos SEI.
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
from app.utils.permissions import requires_admin

logger = logging.getLogger(__name__)

# Constantes
_SEI_BASE_URL = "https://api.sei.pi.gov.br/v1/unidades/110006213/procedimentos/documentos"
_SEI_TIMEOUT = 60   # 60s — API SEI é lenta
_MAX_RETRIES = 3    # 3 tentativas com backoff progressivo
_MAX_WORKERS = 8    # threads paralelas para bulk sync


@cgfr_bp.route('/acompanhar/<path:protocolo>')
@login_required
@requires_admin
def acompanhar(protocolo):
    """Página de acompanhamento de processo CGFR com timeline."""
    dados = obter_timeline_acompanhamento(protocolo)
    if not dados:
        abort(404)

    return render_template(
        'cgfr/acompanhar.html',
        processo=dados['processo'],
        timeline_data=dados['timeline_data'],
        mov_nr=dados['mov_nr'],
        tem_orcamento=dados['tem_orcamento'],
    )


def _fetch_and_save_docs(protocolo, token_sei, app_obj):
    """Busca documentos de um processo na API SEI e salva em cgfrmovimentacao.
    Executada em thread separada para paralelismo.
    Usa mesma lógica robusta do script sync_cgfr_prioritarios.py:
      - timeout 60s, 3 retries com backoff progressivo, verify=False
    Retorna (success: bool, msg: str).
    """
    protocolo_limpo = "".join(filter(str.isdigit, protocolo))
    headers = {'token': token_sei, 'Accept': 'application/json'}
    params = {
        "protocolo_procedimento": protocolo_limpo,
        "pagina": 1, "quantidade": 1000, "sinal_completo": "N"
    }

    # 3 tentativas com backoff progressivo (5s, 10s, 15s)
    resp = None
    for tentativa in range(1, _MAX_RETRIES + 1):
        try:
            resp = http_requests.get(
                _SEI_BASE_URL, headers=headers, params=params,
                timeout=_SEI_TIMEOUT, verify=False,
            )
            if resp.status_code == 200:
                break
            logger.warning(f"[{protocolo}] API retornou {resp.status_code} (tentativa {tentativa}/{_MAX_RETRIES})")
        except http_requests.exceptions.ReadTimeout:
            logger.warning(f"[{protocolo}] Timeout (tentativa {tentativa}/{_MAX_RETRIES})")
        except (http_requests.exceptions.SSLError,
                http_requests.exceptions.ConnectionError) as e:
            logger.warning(f"[{protocolo}] Conexão: {e} (tentativa {tentativa}/{_MAX_RETRIES})")

        if tentativa < _MAX_RETRIES:
            time.sleep(tentativa * 5)
        else:
            return (False, f'API SEI não respondeu após {_MAX_RETRIES} tentativas: {protocolo}')

    if not resp or resp.status_code != 200:
        return (False, f'API {resp.status_code if resp else "?"}: {protocolo}')

    try:
        data_json = resp.json()
    except Exception:
        return (False, f'JSON inválido: {protocolo}')

    items_found = []
    if isinstance(data_json, dict):
        items_found = data_json.get('Documentos', [])
        if not items_found and 'resultados' in data_json:
            items_found = data_json['resultados']
    elif isinstance(data_json, list):
        items_found = data_json

    # Salvar no banco (precisa de app context em thread)
    with app_obj.app_context():
        try:
            CgfrMovimentacao.query.filter_by(protocolo_procedimento=protocolo).delete()

            for doc in items_found:
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
        except Exception as e:
            db.session.rollback()
            return (False, f'DB erro: {protocolo}: {e}')
        finally:
            db.session.remove()

    return (True, f'{len(items_found)} documentos sincronizados')


@cgfr_bp.route('/acompanhar/<path:protocolo>/sync', methods=['POST'])
@login_required
@requires_admin
def sync_documentos(protocolo):
    """Sincroniza documentos SEI de um processo CGFR via API SEI."""
    processo = CgfrProcessoEnviado.query.get(protocolo)
    if not processo:
        return jsonify({'success': False, 'error': 'Processo não encontrado'}), 404

    try:
        token_sei = gerar_token_sei_admin()
        if not token_sei:
            return jsonify({'success': False, 'error': 'Falha ao obter token SEI'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erro autenticação SEI: {str(e)}'}), 500

    app_obj = current_app._get_current_object()
    success, msg = _fetch_and_save_docs(protocolo, token_sei, app_obj)

    return jsonify({'success': success, 'message': msg})


@cgfr_bp.route('/acompanhar/sync-bulk')
@login_required
@requires_admin
def sync_documentos_bulk():
    """Sincroniza documentos SEI de TODOS os processos via SSE para progresso em tempo real.
    Usa mesma lógica robusta do script sync_cgfr_prioritarios.py (60s timeout, 3 retries, verify=False).
    """
    try:
        token_sei = gerar_token_sei_admin()
        if not token_sei:
            return jsonify({'success': False, 'error': 'Falha ao obter token SEI'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erro autenticação SEI: {str(e)}'}), 500

    # Todos os processos
    all_processos = CgfrProcessoEnviado.query.all()
    protocolos = [p.processo_formatado for p in all_processos]

    if not protocolos:
        return jsonify({'success': True, 'message': 'Nenhum processo encontrado', 'total': 0, 'ok': 0, 'erros': 0})

    app_obj = current_app._get_current_object()

    def generate():
        total = len(protocolos)
        done = 0
        erros = 0

        yield f"data: {json.dumps({'msg': f'Iniciando sync de {total} processos...', 'progresso': 0})}\n\n"

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {
                executor.submit(_fetch_and_save_docs, proto, token_sei, app_obj): proto
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

        yield f"data: {json.dumps({'msg': f'Concluído! {done - erros}/{total} sincronizados, {erros} erros', 'progresso': 100, 'concluido': True, 'total': total, 'ok': done - erros, 'erros': erros})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    )
