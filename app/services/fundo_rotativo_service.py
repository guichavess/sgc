"""
Service Layer do módulo Gestão do Fundo Rotativo.

Regras de negócio para registros de saldo (CRUD da aba "Saldo").
"""
from datetime import datetime
from decimal import Decimal, InvalidOperation

from flask import abort, current_app
from sqlalchemy import case, extract, func

from app.constants import FUNDO_ROTATIVO_EXERCICIOS_MAP, UG_FUNDO_ROTATIVO
from app.extensions import db
from app.models.class_fonte import ClassFonte
from app.models.contrato import Contrato
from app.models.empenho import Empenho
from app.models.fundo_rotativo import FundoRotativoSaldo
from app.models.liquidacao import Liquidacao
from app.models.loa import Loa
from app.models.nat_despesa import NatDespesa
from app.models.ob import OB
from app.models.pd import PD
from app.models.reserva import Reserva


STATUS_CONTABILIZADO = 'CONTABILIZADO'
STATUS_PD_ABERTO = 'STATUS_DISPONIVEL'


def _parse_valor(valor):
    if isinstance(valor, Decimal):
        return valor
    if valor is None or valor == '':
        raise ValueError('Valor é obrigatório.')
    s = str(valor).strip().replace('R$', '').replace(' ', '')
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        raise ValueError('Valor inválido.')


def _validar(valor, data, fonte_codigo, id_exercicio):
    if data is None or data == '':
        raise ValueError('Data é obrigatória.')
    if not fonte_codigo:
        raise ValueError('Fonte é obrigatória.')
    if id_exercicio not in FUNDO_ROTATIVO_EXERCICIOS_MAP:
        raise ValueError(
            f'Exercício inválido. Valores aceitos: '
            f'{", ".join(FUNDO_ROTATIVO_EXERCICIOS_MAP.keys())}.'
        )
    valor_decimal = _parse_valor(valor)
    if valor_decimal <= 0:
        raise ValueError('Valor deve ser maior que zero.')
    fonte = ClassFonte.query.filter_by(codigo=fonte_codigo).first()
    if not fonte:
        raise ValueError(f'Fonte "{fonte_codigo}" não encontrada.')
    return valor_decimal


def criar_saldo(valor, data, fonte_codigo, id_exercicio, usuario_id):
    """Cria um novo registro de saldo do Fundo Rotativo."""
    valor_decimal = _validar(valor, data, fonte_codigo, id_exercicio)

    saldo = FundoRotativoSaldo(
        valor=valor_decimal,
        data=data,
        fonte_codigo=fonte_codigo,
        id_exercicio=id_exercicio,
        criado_por=usuario_id,
    )
    db.session.add(saldo)
    db.session.commit()
    current_app.logger.info(
        f'[FUNDO_ROTATIVO] Saldo criado id={saldo.id} valor={valor_decimal} '
        f'fonte={fonte_codigo} exerc={id_exercicio}'
    )
    return saldo


def listar_saldos(
    page=1,
    busca=None,
    fonte_codigo=None,
    id_exercicio=None,
    ano=None,
    natureza=None,
    per_page=20,
):
    """Lista saldos paginados, ordenados por data desc + id desc."""
    query = FundoRotativoSaldo.query

    fontes = [str(v).strip() for v in _as_list(fonte_codigo) if str(v).strip()]
    if fontes:
        query = query.filter(FundoRotativoSaldo.fonte_codigo.in_(fontes))

    exercicios = [str(v).strip() for v in _as_list(id_exercicio) if str(v).strip()]
    if exercicios:
        query = query.filter(FundoRotativoSaldo.id_exercicio.in_(exercicios))

    anos = []
    for v in _as_list(ano):
        try:
            anos.append(int(str(v)))
        except (TypeError, ValueError):
            pass
    if anos:
        query = query.filter(extract('year', FundoRotativoSaldo.data).in_(anos))

    naturezas = [str(v).strip() for v in _as_list(natureza) if str(v).strip()]
    if naturezas:
        query = query.filter(FundoRotativoSaldo.natureza.in_(naturezas))
    if busca:
        filtro = f'%{busca}%'
        query = query.outerjoin(
            ClassFonte,
            FundoRotativoSaldo.fonte_codigo == ClassFonte.codigo,
        ).filter(
            db.or_(
                FundoRotativoSaldo.fonte_codigo.ilike(filtro),
                ClassFonte.descricao.ilike(filtro),
                FundoRotativoSaldo.natureza.ilike(filtro),
            )
        )

    soma_total = _as_float(
        query.with_entities(func.coalesce(func.sum(FundoRotativoSaldo.valor), 0)).scalar()
    )

    query = query.order_by(
        FundoRotativoSaldo.data.desc(),
        FundoRotativoSaldo.id.desc(),
    )
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    return pagination, soma_total


def atualizar_saldo(saldo_id, valor, data, fonte_codigo, id_exercicio):
    """Atualiza um saldo existente."""
    saldo = FundoRotativoSaldo.query.get(saldo_id)
    if not saldo:
        abort(404)

    valor_decimal = _validar(valor, data, fonte_codigo, id_exercicio)

    saldo.valor = valor_decimal
    saldo.data = data
    saldo.fonte_codigo = fonte_codigo
    saldo.id_exercicio = id_exercicio
    db.session.commit()
    current_app.logger.info(
        f'[FUNDO_ROTATIVO] Saldo atualizado id={saldo.id} valor={valor_decimal} '
        f'fonte={fonte_codigo} exerc={id_exercicio}'
    )
    return saldo


def excluir_saldo(saldo_id):
    """Exclui um saldo existente."""
    saldo = FundoRotativoSaldo.query.get(saldo_id)
    if not saldo:
        abort(404)
    db.session.delete(saldo)
    db.session.commit()
    current_app.logger.info(f'[FUNDO_ROTATIVO] Saldo excluído id={saldo_id}')


def listar_naturezas_disponiveis():
    """Naturezas distintas conhecidas (a partir da LOA), com titulo quando houver."""
    rows = (
        db.session.query(Loa.codNatureza)
        .filter(Loa.codNatureza.isnot(None), Loa.codNatureza != '')
        .distinct()
        .order_by(Loa.codNatureza.asc())
        .all()
    )
    codigos = [str(r[0]) for r in rows if r[0]]
    codigos_int = []
    for codigo in codigos:
        try:
            codigos_int.append(int(codigo))
        except (TypeError, ValueError):
            continue

    titulos = {}
    if codigos_int:
        naturezas = NatDespesa.query.filter(NatDespesa.codigo.in_(codigos_int)).all()
        titulos = {
            str(n.codigo): (n.titulo or '').strip()
            for n in naturezas
            if n.codigo is not None
        }

    return [
        {
            'codigo': codigo,
            'titulo': titulos.get(codigo, ''),
            'label': f'{codigo} - {titulos[codigo]}' if titulos.get(codigo) else codigo,
        }
        for codigo in codigos
    ]


def listar_anos_disponiveis():
    """Anos distintos presentes nos registros de saldo (desc)."""
    rows = (
        db.session.query(extract('year', FundoRotativoSaldo.data))
        .distinct()
        .order_by(extract('year', FundoRotativoSaldo.data).desc())
        .all()
    )
    return [int(r[0]) for r in rows if r[0] is not None]


def _parse_ano(ano):
    if not ano:
        return None
    try:
        ano_int = int(str(ano))
    except (TypeError, ValueError):
        return None
    return ano_int if ano_int > 0 else None


def _parse_int_filter(valor):
    if not valor:
        return None
    try:
        return int(str(valor))
    except (TypeError, ValueError):
        return valor


def _as_list(valor):
    if valor is None or valor == '':
        return []
    if isinstance(valor, (list, tuple, set)):
        return [v for v in valor if v not in (None, '')]
    return [valor]


def _normalizar_filtros_dashboard(ano=None, fonte_codigo=None, natureza=None):
    anos = []
    for v in _as_list(ano):
        parsed = _parse_ano(v)
        if parsed:
            anos.append(parsed)

    fontes = []
    for v in _as_list(fonte_codigo):
        s = str(v).strip()
        if s:
            fontes.append(s)

    naturezas = []
    for v in _as_list(natureza):
        s = str(v).strip()
        if s:
            naturezas.append(s)

    fontes_int = []
    for v in fontes:
        try:
            fontes_int.append(int(v))
        except (TypeError, ValueError):
            pass

    naturezas_int = []
    for v in naturezas:
        try:
            naturezas_int.append(int(v))
        except (TypeError, ValueError):
            pass

    return {
        'anos': anos,
        'fontes': fontes,
        'fontes_int': fontes_int,
        'naturezas': naturezas,
        'naturezas_int': naturezas_int,
    }


def _aplicar_filtros_execucao(query, model, filtros):
    if not filtros:
        return query

    if filtros.get('anos'):
        query = query.filter(extract('year', model.dataEmissao).in_(filtros['anos']))
    if filtros.get('fontes_int'):
        query = query.filter(model.codFonte.in_(filtros['fontes_int']))
    if filtros.get('naturezas_int'):
        query = query.filter(model.codNatureza.in_(filtros['naturezas_int']))
    return query


def _saldo_total_dashboard(filtros):
    query = db.session.query(func.coalesce(func.sum(FundoRotativoSaldo.valor), 0))

    if filtros.get('fontes'):
        query = query.filter(FundoRotativoSaldo.fonte_codigo.in_(filtros['fontes']))
    if filtros.get('naturezas'):
        query = query.filter(FundoRotativoSaldo.natureza.in_(filtros['naturezas']))
    if filtros.get('anos'):
        query = query.filter(extract('year', FundoRotativoSaldo.data).in_(filtros['anos']))

    return _as_float(query.scalar())


def listar_anos_dashboard_disponiveis():
    """Anos disponiveis para filtro do dashboard do Fundo Rotativo."""
    ano_atual = datetime.now().year
    anos = {ano_atual}

    rows = (
        db.session.query(extract('year', FundoRotativoSaldo.data))
        .filter(FundoRotativoSaldo.data.isnot(None))
        .distinct()
        .all()
    )
    anos.update(int(r[0]) for r in rows if r[0] is not None)

    for model in (Reserva, Empenho, Liquidacao, PD, OB):
        rows = (
            db.session.query(extract('year', model.dataEmissao))
            .filter(
                model.codigoUG == UG_FUNDO_ROTATIVO,
                model.dataEmissao.isnot(None),
            )
            .distinct()
            .all()
        )
        anos.update(int(r[0]) for r in rows if r[0] is not None)

    return sorted(anos, reverse=True)


def _as_float(valor):
    return float(valor or 0)


def _normalizar_codigo_contrato(codigo):
    if codigo is None:
        return ''
    return str(codigo).strip().replace('.', '').replace('/', '')


def _soma_sinalizada(model, tipo_alteracao_attr):
    valor_col = model.valor
    tipo_col = getattr(model, tipo_alteracao_attr)
    return func.coalesce(
        func.sum(case((tipo_col == 'ANULACAO', -valor_col), else_=valor_col)),
        0,
    )


def _soma_ug(model, sinalizado_por=None, filtros=None):
    expr = (
        _soma_sinalizada(model, sinalizado_por)
        if sinalizado_por
        else func.coalesce(func.sum(model.valor), 0)
    )
    query = (
        db.session.query(expr)
        .filter(
            model.codigoUG == UG_FUNDO_ROTATIVO,
            model.statusDocumento == STATUS_CONTABILIZADO,
        )
    )
    query = _aplicar_filtros_execucao(query, model, filtros)
    return _as_float(query.scalar())


def _mapa_execucao_por_contrato(model, sinalizado_por=None, status_execucao=None, filtros=None):
    expr = (
        _soma_sinalizada(model, sinalizado_por)
        if sinalizado_por
        else func.coalesce(func.sum(model.valor), 0)
    )
    query = (
        db.session.query(model.codContrato, expr)
        .filter(
            model.codigoUG == UG_FUNDO_ROTATIVO,
            model.statusDocumento == STATUS_CONTABILIZADO,
            model.codContrato.isnot(None),
        )
    )
    if status_execucao:
        query = query.filter(model.statusExecucao == status_execucao)
    query = _aplicar_filtros_execucao(query, model, filtros)

    rows = query.group_by(model.codContrato).all()
    mapa = {}
    for codigo, total in rows:
        chave = _normalizar_codigo_contrato(codigo)
        if chave:
            mapa[chave] = _as_float(total)
    return mapa


def _detalhar_pds_aberto_por_contrato(filtros=None):
    query = (
        db.session.query(PD.codContrato, PD.codigo, PD.valor)
        .filter(
            PD.codigoUG == UG_FUNDO_ROTATIVO,
            PD.statusDocumento == STATUS_CONTABILIZADO,
            PD.statusExecucao == STATUS_PD_ABERTO,
            PD.codContrato.isnot(None),
        )
        .order_by(PD.codContrato.asc(), PD.valor.desc())
    )
    query = _aplicar_filtros_execucao(query, PD, filtros)
    rows = query.all()
    detalhes = {}
    for cod_contrato, codigo_pd, valor in rows:
        chave = _normalizar_codigo_contrato(cod_contrato)
        if not chave:
            continue
        detalhes.setdefault(chave, []).append({
            'codigo': codigo_pd or '',
            'competencia': '',
            'valor': round(_as_float(valor), 2),
        })
    return detalhes


def obter_dashboard_fundo_rotativo(ano=None, fonte_codigo=None, natureza=None):
    """Retorna KPIs e tabela de execução por contrato da UG do Fundo Rotativo."""
    filtros = _normalizar_filtros_dashboard(
        ano=ano,
        fonte_codigo=fonte_codigo,
        natureza=natureza,
    )
    saldo_total = _saldo_total_dashboard(filtros)

    reserva_map = _mapa_execucao_por_contrato(
        Reserva,
        sinalizado_por='tipoAlteracao',
        filtros=filtros,
    )
    empenho_map = _mapa_execucao_por_contrato(
        Empenho,
        sinalizado_por='tipoAlteracaoNE',
        filtros=filtros,
    )
    liquidacao_map = _mapa_execucao_por_contrato(
        Liquidacao,
        sinalizado_por='tipoAlteracao',
        filtros=filtros,
    )
    pd_map = _mapa_execucao_por_contrato(PD, filtros=filtros)
    pd_aberto_map = _mapa_execucao_por_contrato(
        PD,
        status_execucao=STATUS_PD_ABERTO,
        filtros=filtros,
    )
    ob_map = _mapa_execucao_por_contrato(OB, filtros=filtros)
    pd_aberto_detail = _detalhar_pds_aberto_por_contrato(filtros=filtros)

    contratos = (
        Contrato.query
        .filter(Contrato.codigoUG == UG_FUNDO_ROTATIVO)
        .order_by(Contrato.codigo.asc())
        .all()
    )
    contratos_map = {
        _normalizar_codigo_contrato(contrato.codigo): contrato
        for contrato in contratos
        if _normalizar_codigo_contrato(contrato.codigo)
    }

    codigos = set(contratos_map.keys())
    for mapa in (reserva_map, empenho_map, liquidacao_map, pd_map, pd_aberto_map, ob_map):
        codigos.update(mapa.keys())

    rows = []
    for codigo in sorted(codigos):
        contrato = contratos_map.get(codigo)
        rows.append({
            'contrato': contrato.codigo if contrato else codigo,
            'credor': (
                contrato.nomeContratadoResumido
                or contrato.nomeContratado
                or f'Contrato {codigo}'
            ) if contrato else f'Contrato {codigo}',
            'objeto': (contrato.objeto or '') if contrato else '',
            'reserva': round(reserva_map.get(codigo, 0), 2),
            'empenho': round(empenho_map.get(codigo, 0), 2),
            'liquidacao': round(liquidacao_map.get(codigo, 0), 2),
            'pd': round(pd_map.get(codigo, 0), 2),
            'pd_aberto': round(pd_aberto_map.get(codigo, 0), 2),
            'pd_aberto_pds': pd_aberto_detail.get(codigo, []),
            'ob': round(ob_map.get(codigo, 0), 2),
        })

    rows.sort(key=lambda row: row['empenho'], reverse=True)

    reservado = _soma_ug(Reserva, sinalizado_por='tipoAlteracao', filtros=filtros)

    return {
        'kpis': {
            'saldo_total': round(saldo_total, 2),
            'reservado': round(reservado, 2),
            'disponivel': round(saldo_total - reservado, 2),
            'liquidado': round(
                _soma_ug(Liquidacao, sinalizado_por='tipoAlteracao', filtros=filtros),
                2,
            ),
            'pago': round(_soma_ug(OB, filtros=filtros), 2),
        },
        'rows': rows,
    }
