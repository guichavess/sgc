import io
import os
import uuid
from datetime import datetime
from flask import render_template, request, jsonify, send_file, current_app
from flask_login import current_user
from app.utils.permissions import requires_permission
from werkzeug.utils import secure_filename
from sqlalchemy import inspect as sa_inspect
from app.extensions import db
from app.models.identidade_visual import (
    IdentidadeVisualLocal, IdentidadeVisualArquivo, IdentidadeVisualLog,
    MunicipioPiaui, TIPOS_LOCAL,
)
from app.identidade_visual.routes import identidade_visual_bp

UPLOAD_FOLDER = os.path.join('app', 'static', 'uploads', 'identidade_visual')
ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'heic'}
PER_PAGE = 15


def _allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def _registrar_log(acao, descricao, local_id=None):
    """Adiciona um registro de auditoria à sessão (commit fica a cargo do chamador)."""
    db.session.add(IdentidadeVisualLog(
        local_id=local_id,
        acao=acao,
        descricao=descricao,
        usuario_id=getattr(current_user, 'id', None),
        usuario_nome=getattr(current_user, 'nome', None),
    ))


@identidade_visual_bp.route('/')
@requires_permission('identidade_visual.visualizar')
def dashboard():
    # Filtros multi-seleção (estilo Pagamentos): cada um aceita vários valores.
    cidades_sel = [c for c in request.args.getlist('cidade') if c]
    tipos_sel = [t for t in request.args.getlist('tipo_local') if t]
    bairros_sel = [b for b in request.args.getlist('bairro') if b]
    status_sel = [s for s in request.args.getlist('status') if s]
    page = max(1, int(request.args.get('page', 1) or 1))

    query = IdentidadeVisualLocal.query

    if cidades_sel:
        query = query.filter(IdentidadeVisualLocal.cidade.in_(cidades_sel))
    if tipos_sel:
        query = query.filter(IdentidadeVisualLocal.tipo_local.in_(tipos_sel))
    if bairros_sel:
        query = query.filter(IdentidadeVisualLocal.bairro.in_(bairros_sel))

    query = query.order_by(IdentidadeVisualLocal.cidade)

    # Prioridade de exibição: PENDENTES primeiro, depois por cidade.
    # status é calculado em Python (depende de data_acao + arquivos), por isso
    # a ordenação e o filtro de status são feitos sobre a lista materializada.
    def _ordem(l):
        return (0 if l.status == 'PENDENTE' else 1, (l.cidade or '').lower())

    all_filtered = query.all()
    if status_sel:
        all_filtered = [l for l in all_filtered if l.status in status_sel]
    all_filtered.sort(key=_ordem)

    total = len(all_filtered)
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = min(page, total_pages)
    locais = all_filtered[(page - 1) * PER_PAGE: page * PER_PAGE]
    todos_para_kpi = all_filtered

    realizados = sum(1 for l in todos_para_kpi if l.status == 'REALIZADO')
    pendentes = total - realizados
    custo_total = sum(float(l.custo) for l in todos_para_kpi if l.custo)

    # ── Contagens globais para os badges dos filtros (todos os registros) ──
    # Uma query leve por colunas + 1 query distinta de arquivos evita N+1.
    rows = db.session.query(
        IdentidadeVisualLocal.id,
        IdentidadeVisualLocal.cidade,
        IdentidadeVisualLocal.tipo_local,
        IdentidadeVisualLocal.bairro,
        IdentidadeVisualLocal.data_acao,
    ).all()
    ids_com_arquivo = {
        r[0] for r in db.session.query(IdentidadeVisualArquivo.local_id).distinct()
    }

    contagem_cidades, contagem_tipos, contagem_bairros = {}, {}, {}
    contagem_status = {'PENDENTE': 0, 'REALIZADO': 0}
    for rid, rcidade, rtipo, rbairro, rdata in rows:
        if rcidade:
            contagem_cidades[rcidade] = contagem_cidades.get(rcidade, 0) + 1
        if rtipo:
            contagem_tipos[rtipo] = contagem_tipos.get(rtipo, 0) + 1
        if rbairro:
            contagem_bairros[rbairro] = contagem_bairros.get(rbairro, 0) + 1
        st = 'REALIZADO' if (rdata is not None and rid in ids_com_arquivo) else 'PENDENTE'
        contagem_status[st] += 1

    cidades = sorted(contagem_cidades.keys())
    tipos_local_existentes = sorted(contagem_tipos.keys())
    bairros = sorted(contagem_bairros.keys())

    if 'municipios_pi' in sa_inspect(db.engine).get_table_names():
        municipios = MunicipioPiaui.query.order_by(MunicipioPiaui.nome).all()
    else:
        municipios = []

    return render_template(
        'identidade_visual/dashboard.html',
        locais=locais,
        cidades=cidades,
        tipos_local=tipos_local_existentes,
        tipos_local_opcoes=TIPOS_LOCAL,
        bairros=bairros,
        contagem_cidades=contagem_cidades,
        contagem_tipos=contagem_tipos,
        contagem_bairros=contagem_bairros,
        contagem_status=contagem_status,
        filtro_cidades=cidades_sel,
        filtro_tipos=tipos_sel,
        filtro_bairros=bairros_sel,
        filtro_status_sel=status_sel,
        page=page,
        total_pages=total_pages,
        total=total,
        realizados=realizados,
        pendentes=pendentes,
        custo_total=custo_total,
        municipios=municipios,
        pode_excluir=current_user.tem_permissao('identidade_visual', 'excluir'),
    )


@identidade_visual_bp.route('/api/salvar-acao/<int:local_id>', methods=['POST'])
@requires_permission('identidade_visual.editar')
def salvar_acao(local_id):
    local = IdentidadeVisualLocal.query.get(local_id)
    if not local:
        return jsonify({'erro': 'Local não encontrado'}), 404

    data_acao = request.form.get('data_acao', '').strip()
    if not data_acao:
        return jsonify({'erro': 'Data e hora são obrigatórios'}), 400
    try:
        local.data_acao = datetime.strptime(data_acao, '%Y-%m-%dT%H:%M')
    except ValueError:
        return jsonify({'erro': 'Formato de data inválido'}), 400

    arquivos = request.files.getlist('arquivos')
    tem_arquivo_novo = any(a and a.filename for a in arquivos)
    tem_arquivo_existente = local.arquivos.count() > 0

    if not tem_arquivo_novo and not tem_arquivo_existente:
        return jsonify({'erro': 'É necessário anexar pelo menos um arquivo'}), 400

    custo_raw = request.form.get('custo', '').strip()
    if custo_raw:
        custo_raw = custo_raw.replace('R$', '').replace('.', '').replace(',', '.').strip()
        try:
            local.custo = float(custo_raw)
        except ValueError:
            return jsonify({'erro': 'Valor de custo inválido'}), 400
    else:
        local.custo = None

    for arquivo in arquivos:
        if not arquivo or not arquivo.filename:
            continue
        if not _allowed_file(arquivo.filename):
            return jsonify({'erro': f'Tipo de arquivo não permitido: {arquivo.filename}'}), 400

        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
        nome_seguro = secure_filename(arquivo.filename)
        extensao = nome_seguro.rsplit('.', 1)[1].lower()
        nome_unico = f"{uuid.uuid4().hex[:8]}_{nome_seguro}"
        caminho = os.path.join(UPLOAD_FOLDER, nome_unico)
        arquivo.save(caminho)

        novo_arquivo = IdentidadeVisualArquivo(
            local_id=local.id,
            nome_original=arquivo.filename,
            nome_servidor=nome_unico,
            tipo=extensao,
        )
        db.session.add(novo_arquivo)

    local.atualizado_por_id = getattr(current_user, 'id', None)
    _registrar_log('EDITAR', f'Registrou ação/anexos em {local.cidade} — {local.tipo_local}', local_id=local.id)

    db.session.commit()
    return jsonify({'ok': True, 'status': local.status})


@identidade_visual_bp.route('/api/remover-arquivo/<int:arquivo_id>', methods=['POST'])
@requires_permission('identidade_visual.editar')
def remover_arquivo(arquivo_id):
    arquivo = IdentidadeVisualArquivo.query.get(arquivo_id)
    if not arquivo:
        return jsonify({'erro': 'Arquivo não encontrado'}), 404

    caminho = os.path.join(UPLOAD_FOLDER, arquivo.nome_servidor)
    if os.path.exists(caminho):
        os.remove(caminho)

    local = arquivo.local_ref
    db.session.delete(arquivo)
    db.session.commit()
    return jsonify({'ok': True, 'status': local.status})


@identidade_visual_bp.route('/api/remover-custo/<int:local_id>', methods=['POST'])
@requires_permission('identidade_visual.editar')
def remover_custo(local_id):
    local = IdentidadeVisualLocal.query.get(local_id)
    if not local:
        return jsonify({'erro': 'Local não encontrado'}), 404

    local.custo = None
    db.session.commit()
    return jsonify({'ok': True, 'status': local.status})


@identidade_visual_bp.route('/api/arquivos/<int:local_id>')
@requires_permission('identidade_visual.visualizar')
def listar_arquivos(local_id):
    local = IdentidadeVisualLocal.query.get(local_id)
    if not local:
        return jsonify({'erro': 'Local não encontrado'}), 404

    arquivos = [{
        'id': a.id,
        'nome': a.nome_original,
        'tipo': a.tipo,
        'url': f'/static/uploads/identidade_visual/{a.nome_servidor}',
        'data': a.created_at.strftime('%d/%m/%Y %H:%M') if a.created_at else '',
    } for a in local.arquivos.all()]

    try:
        mid = local.municipio_id
    except Exception:
        mid = None

    return jsonify({
        'arquivos': arquivos,
        'custo': str(local.custo) if local.custo else None,
        'data_acao': local.data_acao.strftime('%Y-%m-%dT%H:%M') if local.data_acao else None,
        'cidade': local.cidade,
        'municipio_id': mid,
        'tipo_local': local.tipo_local,
        'endereco': local.endereco or '',
        'bairro': local.bairro or '',
        'cep': local.cep or '',
    })


def _validar_local(data):
    """Valida campos comuns de criação/edição. Retorna (erro_msg, status_code) ou None."""
    municipio_id = data.get('municipio_id')
    tipo_local = (data.get('tipo_local') or '').strip()
    endereco = (data.get('endereco') or '').strip()
    bairro = (data.get('bairro') or '').strip()
    cep = (data.get('cep') or '').strip()

    if not municipio_id:
        return 'Cidade é obrigatória', 400
    if not tipo_local:
        return 'Tipo Local é obrigatório', 400
    if not endereco:
        return 'Endereço é obrigatório', 400
    if not bairro:
        return 'Bairro é obrigatório', 400
    if not cep:
        return 'CEP é obrigatório', 400
    if tipo_local not in TIPOS_LOCAL:
        return 'Tipo Local inválido', 400
    return None


@identidade_visual_bp.route('/api/criar-local', methods=['POST'])
@requires_permission('identidade_visual.editar')
def criar_local():
    data = request.get_json() or {}

    erro = _validar_local(data)
    if erro:
        return jsonify({'erro': erro[0]}), erro[1]

    municipio = db.session.get(MunicipioPiaui, data['municipio_id'])
    if not municipio:
        return jsonify({'erro': 'Município não encontrado'}), 400

    uid = getattr(current_user, 'id', None)
    local = IdentidadeVisualLocal(
        cidade=municipio.nome,
        municipio_id=municipio.id,
        tipo_local=data['tipo_local'].strip(),
        endereco=data['endereco'].strip(),
        bairro=data['bairro'].strip(),
        cep=data['cep'].strip(),
        criado_por_id=uid,
        atualizado_por_id=uid,
    )
    db.session.add(local)
    db.session.flush()
    _registrar_log('CRIAR', f'Criou o local {local.cidade} — {local.tipo_local}', local_id=local.id)
    db.session.commit()

    return jsonify({'ok': True, 'id': local.id})


@identidade_visual_bp.route('/api/editar-local/<int:local_id>', methods=['POST'])
@requires_permission('identidade_visual.editar')
def editar_local(local_id):
    local = IdentidadeVisualLocal.query.get(local_id)
    if not local:
        return jsonify({'erro': 'Local não encontrado'}), 404

    data = request.get_json() or {}

    erro = _validar_local(data)
    if erro:
        return jsonify({'erro': erro[0]}), erro[1]

    municipio = db.session.get(MunicipioPiaui, data['municipio_id'])
    if not municipio:
        return jsonify({'erro': 'Município não encontrado'}), 400

    local.cidade = municipio.nome
    local.municipio_id = municipio.id
    local.tipo_local = data['tipo_local'].strip()
    local.endereco = data['endereco'].strip()
    local.bairro = data['bairro'].strip()
    local.cep = data['cep'].strip()
    local.atualizado_por_id = getattr(current_user, 'id', None)

    _registrar_log('EDITAR', f'Editou o local {local.cidade} — {local.tipo_local}', local_id=local.id)
    db.session.commit()

    return jsonify({'ok': True, 'id': local.id})


@identidade_visual_bp.route('/api/excluir-local/<int:local_id>', methods=['POST'])
@requires_permission('identidade_visual.excluir')
def excluir_local(local_id):
    """Exclui um local. Restrito a usuários com acesso FULL ao módulo
    (permissão `identidade_visual.excluir` ou is_admin). A ação é auditada."""
    local = IdentidadeVisualLocal.query.get(local_id)
    if not local:
        return jsonify({'erro': 'Local não encontrado'}), 404

    # Remove arquivos físicos antes de apagar os registros filhos (cascade)
    for arquivo in local.arquivos.all():
        caminho = os.path.join(UPLOAD_FOLDER, arquivo.nome_servidor)
        if os.path.exists(caminho):
            try:
                os.remove(caminho)
            except OSError:
                current_app.logger.warning(
                    f'[IDENTIDADE_VISUAL] Falha ao remover arquivo {caminho}')

    descricao = f'Excluiu o local {local.cidade} — {local.tipo_local} (#{local.id})'
    _registrar_log('EXCLUIR', descricao, local_id=local.id)

    db.session.delete(local)
    db.session.commit()
    current_app.logger.info(f'[IDENTIDADE_VISUAL] {descricao} por '
                            f'{getattr(current_user, "nome", "?")}')
    return jsonify({'ok': True})


@identidade_visual_bp.route('/exportar-excel')
@requires_permission('identidade_visual.visualizar')
def exportar_excel():
    import openpyxl
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

    locais = IdentidadeVisualLocal.query.order_by(IdentidadeVisualLocal.cidade).all()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Identidade Visual'

    headers = ['Cidade', 'Tipo Local', 'Endereço', 'Bairro', 'CEP', 'Status', 'Custo (R$)', 'Data/Hora', 'Qtd Arquivos']
    header_fill = PatternFill(start_color='0891B2', end_color='0891B2', fill_type='solid')
    header_font = Font(bold=True, color='FFFFFF', size=11)
    thin_border = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'), bottom=Side(style='thin')
    )

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')
        cell.border = thin_border

    for row_idx, l in enumerate(locais, 2):
        valores = [
            l.cidade,
            l.tipo_local,
            l.endereco or '',
            l.bairro or '',
            l.cep or '',
            l.status,
            float(l.custo) if l.custo else None,
            l.data_acao.strftime('%d/%m/%Y %H:%M') if l.data_acao else '',
            l.arquivos.count(),
        ]
        for col, v in enumerate(valores, 1):
            cell = ws.cell(row=row_idx, column=col, value=v)
            cell.border = thin_border
            if col == 7 and v is not None:
                cell.number_format = '#,##0.00'

    col_widths = [22, 30, 40, 18, 12, 12, 15, 18, 14]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = w

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'Relatorio_Fachadas_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
    )
