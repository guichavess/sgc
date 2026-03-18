"""
Rota de acompanhamento de processo CGFR com timeline.
"""
import logging
import time

import requests as http_requests
from flask import render_template, abort, jsonify, current_app
from flask_login import login_required

from app.cgfr.routes import cgfr_bp
from app.cgfr.models import CgfrProcessoEnviado, CgfrMovimentacao
from app.cgfr.services.processo_service import obter_timeline_acompanhamento
from app.extensions import db
from app.services.sei_auth import gerar_token_sei_admin
from app.utils.permissions import requires_admin

logger = logging.getLogger(__name__)

# Session reutilizável para conexões SEI
_sei_session = http_requests.Session()
_sei_session.verify = False


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
        logger.exception('Erro ao gerar token SEI')
        return jsonify({'success': False, 'error': f'Erro autenticação SEI: {str(e)}'}), 500

    # Limpar protocolo para chamada API
    protocolo_limpo = "".join(filter(str.isdigit, protocolo))

    base_url = "https://api.sei.pi.gov.br/v1/unidades/110006213/procedimentos/documentos"
    headers = {'token': token_sei, 'Accept': 'application/json'}
    params = {
        "protocolo_procedimento": protocolo_limpo,
        "pagina": 1,
        "quantidade": 1000,
        "sinal_completo": "N"
    }

    # Tentar até 3 vezes
    resp = None
    for tentativa in range(1, 4):
        try:
            resp = _sei_session.get(base_url, headers=headers, params=params, timeout=60)
            break
        except (http_requests.exceptions.SSLError,
                http_requests.exceptions.ConnectionError) as e_retry:
            if tentativa < 3:
                time.sleep(tentativa * 5)
            else:
                logger.error(f'Erro SEI após 3 tentativas para {protocolo}: {e_retry}')
                return jsonify({'success': False, 'error': f'Erro de conexão SEI: {str(e_retry)}'}), 500

    if not resp or resp.status_code != 200:
        status = resp.status_code if resp else 'sem resposta'
        return jsonify({'success': False, 'error': f'API SEI retornou status {status}'}), 500

    try:
        data_json = resp.json()
    except Exception:
        return jsonify({'success': False, 'error': 'Resposta inválida da API SEI'}), 500

    # Extrair lista de documentos
    items_found = []
    if isinstance(data_json, dict):
        items_found = data_json.get('Documentos', [])
        if not items_found and 'resultados' in data_json:
            items_found = data_json['resultados']
    elif isinstance(data_json, list):
        items_found = data_json

    if not items_found:
        return jsonify({'success': True, 'message': 'Nenhum documento encontrado', 'count': 0})

    # Limpar documentos antigos deste processo
    CgfrMovimentacao.query.filter_by(protocolo_procedimento=protocolo).delete()

    # Inserir documentos
    count = 0
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
        count += 1

    db.session.commit()
    logger.info(f'Sync CGFR docs: {count} documentos para {protocolo}')

    return jsonify({'success': True, 'message': f'{count} documentos sincronizados', 'count': count})
