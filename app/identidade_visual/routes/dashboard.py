import io
import os
import uuid
from datetime import datetime
from flask import render_template, request, jsonify, send_file
from app.utils.permissions import requires_permission
from werkzeug.utils import secure_filename
from app.extensions import db
from app.models.identidade_visual import IdentidadeVisualLocal, IdentidadeVisualArquivo
from app.identidade_visual.routes import identidade_visual_bp

UPLOAD_FOLDER = os.path.join('app', 'static', 'uploads', 'identidade_visual')
ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'heic'}
PER_PAGE = 15


def _allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@identidade_visual_bp.route('/')
@requires_permission('identidade_visual.visualizar')
def dashboard():
    cidade = request.args.get('cidade', '')
    tipo_local = request.args.get('tipo_local', '')
    bairro = request.args.get('bairro', '')
    status = request.args.get('status', '')
    page = max(1, int(request.args.get('page', 1) or 1))

    query = IdentidadeVisualLocal.query

    if cidade:
        query = query.filter(IdentidadeVisualLocal.cidade == cidade)
    if tipo_local:
        query = query.filter(IdentidadeVisualLocal.tipo_local == tipo_local)
    if bairro:
        query = query.filter(IdentidadeVisualLocal.bairro == bairro)

    query = query.order_by(IdentidadeVisualLocal.cidade)

    if status:
        all_filtered = query.all()
        if status == 'REALIZADO':
            all_filtered = [l for l in all_filtered if l.status == 'REALIZADO']
        elif status == 'PENDENTE':
            all_filtered = [l for l in all_filtered if l.status == 'PENDENTE']
        total = len(all_filtered)
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page = min(page, total_pages)
        locais = all_filtered[(page - 1) * PER_PAGE: page * PER_PAGE]
    else:
        pagination = query.paginate(page=page, per_page=PER_PAGE, error_out=False)
        locais = pagination.items
        total = pagination.total
        total_pages = pagination.pages

    cidades = db.session.query(IdentidadeVisualLocal.cidade).distinct().order_by(IdentidadeVisualLocal.cidade).all()
    cidades = [c[0] for c in cidades]

    tipos_local = db.session.query(IdentidadeVisualLocal.tipo_local).distinct().order_by(IdentidadeVisualLocal.tipo_local).all()
    tipos_local = [t[0] for t in tipos_local]

    bairros = db.session.query(IdentidadeVisualLocal.bairro).filter(
        IdentidadeVisualLocal.bairro.isnot(None),
        IdentidadeVisualLocal.bairro != ''
    ).distinct().order_by(IdentidadeVisualLocal.bairro).all()
    bairros = [b[0] for b in bairros]

    return render_template(
        'identidade_visual/dashboard.html',
        locais=locais,
        cidades=cidades,
        tipos_local=tipos_local,
        bairros=bairros,
        filtro_cidade=cidade,
        filtro_tipo_local=tipo_local,
        filtro_bairro=bairro,
        filtro_status=status,
        page=page,
        total_pages=total_pages,
        total=total,
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

    return jsonify({
        'arquivos': arquivos,
        'custo': str(local.custo) if local.custo else None,
        'data_acao': local.data_acao.strftime('%Y-%m-%dT%H:%M') if local.data_acao else None,
    })


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
