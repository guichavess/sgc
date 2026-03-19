"""
Rota de vinculação manual de processo CGFR.
Permite vincular processos SEI que não aparecem no Data Lake (sync Trino).
Fluxo: consulta API SEI → preview → usuário completa campos → salva.
"""
import logging
from datetime import datetime

import requests as http_requests
import urllib3
from flask import render_template, request, jsonify, redirect, url_for, flash, current_app
from flask_login import login_required

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from app.cgfr.routes import cgfr_bp
from app.cgfr.models import CgfrProcessoEnviado
from app.cgfr.routes.acompanhar import _fetch_and_save_docs
from app.extensions import db
from app.services.sei_auth import gerar_token_sei_admin
from app.utils.permissions import requires_admin

logger = logging.getLogger(__name__)

# Constantes SEI
_SEI_BASE = "https://api.sei.pi.gov.br/v1/unidades/110006213"
_SEI_TIMEOUT = 60


def _consultar_procedimento_completo(token, protocolo):
    """Consulta procedimento SEI com sinal_completo=S para trazer andamentos."""
    protocolo_limpo = "".join(filter(str.isdigit, protocolo))
    if not protocolo_limpo:
        return {'sucesso': False, 'erro': 'Protocolo inválido.'}

    url = f"{_SEI_BASE}/procedimentos/consulta"
    headers = {'token': token, 'Accept': 'application/json'}
    params = {'protocolo_procedimento': protocolo_limpo, 'sinal_completo': 'S'}

    try:
        resp = http_requests.get(url, params=params, headers=headers,
                                 timeout=_SEI_TIMEOUT, verify=False)
        if resp.status_code != 200:
            return {'sucesso': False, 'erro': f'Processo não encontrado no SEI (HTTP {resp.status_code}).'}

        data = resp.json()
        return {'sucesso': True, 'dados': data}

    except http_requests.exceptions.Timeout:
        return {'sucesso': False, 'erro': 'Timeout ao consultar SEI. Tente novamente.'}
    except Exception as e:
        return {'sucesso': False, 'erro': f'Erro ao consultar SEI: {str(e)}'}


@cgfr_bp.route('/vincular', methods=['GET'])
@login_required
@requires_admin
def vincular_processo():
    """Página de vinculação manual de processo CGFR."""
    return render_template('cgfr/vincular.html')


@cgfr_bp.route('/api/consultar-sei', methods=['POST'])
@login_required
@requires_admin
def api_consultar_sei():
    """AJAX: Consulta processo no SEI e retorna preview dos dados."""
    protocolo = (request.json or {}).get('protocolo', '').strip()
    if not protocolo:
        return jsonify({'sucesso': False, 'erro': 'Protocolo não informado.'}), 400

    # Verifica duplicata
    existente = CgfrProcessoEnviado.query.get(protocolo)
    if not existente:
        # Tenta com formato limpo (só dígitos) — PK pode estar formatado
        protocolo_limpo = "".join(filter(str.isdigit, protocolo))
        existente = CgfrProcessoEnviado.query.filter(
            CgfrProcessoEnviado.processo_formatado.like(f'%{protocolo_limpo[-8:]}%')
        ).first() if len(protocolo_limpo) >= 8 else None

    if existente:
        return jsonify({
            'sucesso': False,
            'erro': f'Este processo já está vinculado: {existente.processo_formatado}',
            'duplicata': True
        })

    # Autentica no SEI
    try:
        token = gerar_token_sei_admin()
        if not token:
            return jsonify({'sucesso': False, 'erro': 'Falha na autenticação SEI.'}), 500
    except Exception as e:
        return jsonify({'sucesso': False, 'erro': f'Erro autenticação SEI: {str(e)}'}), 500

    # Consulta procedimento com dados completos
    resultado = _consultar_procedimento_completo(token, protocolo)
    if not resultado['sucesso']:
        return jsonify(resultado)

    data = resultado['dados']
    andamento_geracao = data.get('AndamentoGeracao', {}) or {}
    ultimo_andamento = data.get('UltimoAndamento', {}) or {}
    tipo_proc = data.get('TipoProcedimento', {}) or {}
    unidade_geracao = andamento_geracao.get('Unidade', {}) or {}
    usuario_geracao = andamento_geracao.get('Usuario', {}) or {}
    unidade_ult = ultimo_andamento.get('Unidade', {}) or {}
    usuario_ult = ultimo_andamento.get('Usuario', {}) or {}

    # Monta preview para o frontend
    preview = {
        'processo_formatado': data.get('ProcedimentoFormatado', ''),
        'link_acesso': data.get('LinkAcesso', ''),
        'especificacao': data.get('Especificacao', ''),
        'tipo_processo': tipo_proc.get('Nome', ''),
        'data_hora_processo': data.get('DataAutuacao', ''),
        # Geração
        'id_unidade_geradora': unidade_geracao.get('IdUnidade', ''),
        'geracao_sigla': unidade_geracao.get('Sigla', ''),
        'geracao_data': andamento_geracao.get('DataHora', ''),
        'geracao_descricao': andamento_geracao.get('Descricao', ''),
        'usuario_gerador': usuario_geracao.get('Nome', ''),
        # Último andamento
        'ultimo_andamento_sigla': unidade_ult.get('Sigla', ''),
        'ultimo_andamento_descricao': ultimo_andamento.get('Descricao', ''),
        'ultimo_andamento_data': ultimo_andamento.get('DataHora', ''),
        'ultimo_andamento_usuario': usuario_ult.get('Nome', ''),
        # Interessados
        'interessados': ', '.join(
            i.get('Nome', '') for i in (data.get('Interessados', []) or [])
        ),
    }

    return jsonify({'sucesso': True, 'preview': preview})


@cgfr_bp.route('/vincular', methods=['POST'])
@login_required
@requires_admin
def vincular_processo_post():
    """Salva processo CGFR vinculado manualmente + sincroniza documentos SEI."""
    protocolo = request.form.get('processo_formatado', '').strip()
    if not protocolo:
        flash('Protocolo não informado.', 'danger')
        return redirect(url_for('cgfr.vincular_processo'))

    # Verifica duplicata
    if CgfrProcessoEnviado.query.get(protocolo):
        flash(f'Este processo já está vinculado: {protocolo}', 'warning')
        return redirect(url_for('cgfr.vincular_processo'))

    try:
        # Autentica no SEI para buscar dados completos
        token = gerar_token_sei_admin()
        if not token:
            flash('Falha na autenticação SEI.', 'danger')
            return redirect(url_for('cgfr.vincular_processo'))

        # Consulta procedimento completo
        resultado = _consultar_procedimento_completo(token, protocolo)
        if not resultado['sucesso']:
            flash(f'Erro ao consultar SEI: {resultado["erro"]}', 'danger')
            return redirect(url_for('cgfr.vincular_processo'))

        data = resultado['dados']
        andamento_geracao = data.get('AndamentoGeracao', {}) or {}
        ultimo_andamento = data.get('UltimoAndamento', {}) or {}
        tipo_proc = data.get('TipoProcedimento', {}) or {}
        unidade_geracao = andamento_geracao.get('Unidade', {}) or {}
        usuario_geracao = andamento_geracao.get('Usuario', {}) or {}
        unidade_ult = ultimo_andamento.get('Unidade', {}) or {}
        usuario_ult = ultimo_andamento.get('Usuario', {}) or {}

        # Parse datas SEI (formato: "dd/mm/yyyy HH:MM:SS" ou "dd/mm/yyyy")
        def parse_sei_datetime(val):
            if not val:
                return None
            for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    return datetime.strptime(val, fmt)
                except (ValueError, TypeError):
                    continue
            return None

        def parse_sei_date(val):
            dt = parse_sei_datetime(val)
            return dt.date() if dt else None

        # Parse valores monetários (formato BR: "1.234,56")
        def parse_valor(raw):
            if not raw:
                return None
            try:
                return float(raw.replace('.', '').replace(',', '.'))
            except (ValueError, TypeError):
                return None

        # Cria registro
        processo = CgfrProcessoEnviado(
            processo_formatado=data.get('ProcedimentoFormatado', protocolo),
            link_acesso=data.get('LinkAcesso', ''),
            especificacao=data.get('Especificacao', ''),
            tipo_processo=tipo_proc.get('Nome', ''),
            data_hora_processo=parse_sei_datetime(data.get('DataAutuacao')),
            id_unidade_geradora=str(unidade_geracao.get('IdUnidade', '')),
            geracao_sigla=unidade_geracao.get('Sigla', ''),
            geracao_data=parse_sei_datetime(andamento_geracao.get('DataHora')),
            geracao_descricao=andamento_geracao.get('Descricao', ''),
            usuario_gerador=usuario_geracao.get('Nome', ''),
            ultimo_andamento_sigla=unidade_ult.get('Sigla', ''),
            ultimo_andamento_descricao=ultimo_andamento.get('Descricao', ''),
            ultimo_andamento_data=parse_sei_datetime(ultimo_andamento.get('DataHora')),
            ultimo_andamento_usuario=usuario_ult.get('Nome', ''),
            # Campos manuais do formulário
            fornecedor=request.form.get('fornecedor', '').strip() or None,
            objeto_do_pedido=request.form.get('objeto_do_pedido', '').strip() or None,
            necessidade=request.form.get('necessidade', '').strip() or None,
            deliberacao=request.form.get('deliberacao', '').strip() or None,
            tipo_despesa=request.form.get('tipo_despesa', '').strip() or None,
            valor_solicitado=parse_valor(request.form.get('valor_solicitado', '')),
            valor_aprovado=parse_valor(request.form.get('valor_aprovado', '')),
            data_da_reuniao=parse_sei_date(request.form.get('data_da_reuniao', '')),
            observacao=request.form.get('observacao', '').strip() or None,
            possui_reserva=1 if request.form.get('possui_reserva') == '1' else 0,
            valor_reserva=request.form.get('valor_reserva', '').strip() or None,
            nivel_prioridade=request.form.get('nivel_prioridade', '').strip() or None,
            data_inclusao=datetime.now(),
        )
        db.session.add(processo)
        db.session.commit()

        # Sincroniza documentos SEI → cgfrmovimentacao
        protocolo_fmt = processo.processo_formatado
        app_obj = current_app._get_current_object()
        success, msg = _fetch_and_save_docs(protocolo_fmt, token, app_obj)
        if success:
            logger.info(f'Documentos sincronizados para {protocolo_fmt}: {msg}')
        else:
            logger.warning(f'Falha ao sincronizar documentos de {protocolo_fmt}: {msg}')

        flash(
            f'Processo {protocolo_fmt} vinculado com sucesso! '
            f'Documentos SEI sincronizados.',
            'success'
        )
        return redirect(url_for('cgfr.dashboard'))

    except Exception as e:
        db.session.rollback()
        logger.error(f'Erro ao vincular processo CGFR: {e}', exc_info=True)
        flash('Erro ao vincular processo. Tente novamente.', 'danger')
        return redirect(url_for('cgfr.vincular_processo'))
