"""
Service Layer do modulo Gestao do Fundo Rotativo.

Regras de negocio para snapshots de saldo via SIAFE e dashboard financeiro.
"""
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json

import requests
from flask import current_app
from requests.adapters import HTTPAdapter
from sqlalchemy import case, extract, func
from urllib3.util.retry import Retry

from app.constants import UG_FUNDO_ROTATIVO
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
CONTA_CONTABIL_FUNDO_ROTATIVO = '111111901'
SIAFE_BASE_URL_DEFAULT = 'https://tesouro.sefaz.pi.gov.br/siafe-api'


def _parse_decimal(valor):
    if isinstance(valor, Decimal):
        return valor
    if valor is None or valor == '':
        return None
    s = str(valor).strip().replace('R$', '').replace(' ', '')
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        raise ValueError(f'Valor numerico invalido: {valor}')


def _normalizar_codigo_fonte(valor):
    if valor is None:
        return None
    digits = ''.join(ch for ch in str(valor) if ch.isdigit())
    return digits or None


def _normalizar_id_exercicio(valor):
    if valor is None or valor == '':
        return '00'
    digits = ''.join(ch for ch in str(valor) if ch.isdigit())
    if not digits:
        return '00'
    return digits.zfill(2) if len(digits) <= 2 else digits


def _classificacao_por_codigo(classificacoes, codigo):
    for item in classificacoes or []:
        try:
            codigo_item = int(item.get('codigoTipoClassificador'))
        except (TypeError, ValueError):
            continue
        if codigo_item == codigo:
            return item
    return {}


def _nome_classificador(classificacoes, codigo):
    item = _classificacao_por_codigo(classificacoes, codigo)
    nome = item.get('nomeClassificador')
    return str(nome).strip() if nome not in (None, '') else None


def _valores_classificador(classificacoes, codigo):
    item = _classificacao_por_codigo(classificacoes, codigo)
    valores = item.get('valoresClassificador') or []
    return [str(v).strip() for v in valores if v not in (None, '')]


def _parse_int(valor, default=None):
    try:
        return int(str(valor))
    except (TypeError, ValueError):
        return default


def _as_float(valor):
    return float(valor or 0)


def _as_list(valor):
    if valor is None or valor == '':
        return []
    if isinstance(valor, (list, tuple, set)):
        return [v for v in valor if v not in (None, '')]
    return [valor]


def _make_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET', 'POST']),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


class SiafeSaldoContabilClient:
    """Cliente minimo para autenticar e consultar saldo contabil no SIAFE."""

    def __init__(self, base_url, usuario, senha, session=None):
        self.base_url = (base_url or SIAFE_BASE_URL_DEFAULT).rstrip('/')
        self.usuario = usuario
        self.senha = senha
        self.session = session or _make_session()
        self._token = None

    @classmethod
    def from_config(cls):
        return cls(
            current_app.config.get('SIAFE_URL') or SIAFE_BASE_URL_DEFAULT,
            current_app.config.get('SIAFE_USUARIO'),
            current_app.config.get('SIAFE_SENHA'),
        )

    def _obter_token(self):
        if self._token:
            return self._token

        if not self.usuario or not self.senha:
            raise RuntimeError('Credenciais SIAFE_USUARIO/SIAFE_SENHA nao configuradas.')

        url = f'{self.base_url}/auth'
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'SGC-Pagamentos/FundoRotativo',
        }
        payload = {'usuario': self.usuario, 'senha': self.senha}
        current_app.logger.info(f'[FUNDO_ROTATIVO] Autenticando no SIAFE base_url={self.base_url}')

        try:
            response = self.session.post(
                url,
                json=payload,
                headers=headers,
                timeout=(5, 60),
                verify=False,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f'Falha de comunicacao na autenticacao SIAFE: {exc}') from exc

        if response.status_code != 200:
            raise RuntimeError(f'Erro de autenticacao SIAFE: HTTP {response.status_code}')

        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError('Resposta de autenticacao SIAFE nao e JSON valido.') from exc

        token = data.get('token')
        if not token:
            raise RuntimeError('SIAFE autenticou, mas nao retornou token.')

        self._token = token
        return token

    def consultar_saldo_contabil(self, ano, mes, conta_contabil, codigo_ug):
        token = self._obter_token()
        url = f'{self.base_url}/saldo-contabil/{int(ano)}/{int(mes)}/{conta_contabil}/{codigo_ug}'
        headers = {
            'accept': '*/*',
            'Authorization': f'Bearer {token}',
        }
        current_app.logger.info(
            f'[FUNDO_ROTATIVO] Consultando saldo SIAFE ano={ano} mes={mes} '
            f'conta={conta_contabil} ug={codigo_ug}'
        )

        try:
            response = self.session.get(url, headers=headers, timeout=(5, 60), verify=False)
        except requests.RequestException as exc:
            raise RuntimeError(f'Falha de comunicacao na consulta SIAFE: {exc}') from exc

        if response.status_code != 200:
            raise RuntimeError(f'Erro na consulta SIAFE saldo-contabil: HTTP {response.status_code}')

        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError('Resposta de saldo-contabil SIAFE nao e JSON valido.') from exc

        if data is None:
            return []
        if not isinstance(data, list):
            raise RuntimeError('Resposta de saldo-contabil SIAFE nao e uma lista.')
        return data


def normalizar_linha_saldo_siafe(row, ano, mes, conta_contabil):
    """Transforma uma linha bruta da API em colunas planilhadas."""
    classificacoes = row.get('classificacao') or []
    fonte_formatada = _nome_classificador(classificacoes, 28)
    fonte_codigo = _normalizar_codigo_fonte(fonte_formatada)

    domicilio = _nome_classificador(classificacoes, 101)
    domicilio_partes = _valores_classificador(classificacoes, 101)
    banco = domicilio_partes[0] if len(domicilio_partes) > 0 else None
    agencia = domicilio_partes[1] if len(domicilio_partes) > 1 else None
    conta_bancaria = domicilio_partes[2] if len(domicilio_partes) > 2 else None

    identificador_exercicio = _nome_classificador(classificacoes, 23)
    ano_int = _parse_int(row.get('ano'), _parse_int(ano))
    mes_int = _parse_int(row.get('mes'), _parse_int(mes))

    valor = _parse_decimal(row.get('saldo')) or Decimal('0')

    return {
        'valor': valor,
        'data': datetime(ano_int, mes_int, 1),
        'codigoUG': str(row.get('codigoUG') or UG_FUNDO_ROTATIVO),
        'conta_contabil': str(conta_contabil),
        'ano': ano_int,
        'mes': mes_int,
        'conta_corrente': str(row.get('contaCorrente') or ''),
        'classificacaoStr': str(row.get('classificacaoStr') or ''),
        'saldoAnterior': _parse_decimal(row.get('saldoAnterior')),
        'valorCredito': _parse_decimal(row.get('valorCredito')),
        'valorDebito': _parse_decimal(row.get('valorDebito')),
        'fonte_codigo': fonte_codigo,
        'fonte_formatada': fonte_formatada,
        'id_exercicio': _normalizar_id_exercicio(identificador_exercicio),
        'identificador_exercicio_fonte': identificador_exercicio,
        'marcador_fonte': _nome_classificador(classificacoes, 24),
        'detalhamento_fonte': _nome_classificador(classificacoes, 159),
        'tipo_detalhamento_fonte': _nome_classificador(classificacoes, 186),
        'domicilio_bancario': domicilio,
        'banco': banco,
        'agencia': agencia,
        'conta_bancaria': conta_bancaria,
        'classificacao_json': json.dumps(classificacoes, ensure_ascii=False),
    }


def _normalizar_periodos(periodos):
    normalizados = []
    vistos = set()
    for ano, mes in periodos:
        ano_int = _parse_int(ano)
        mes_int = _parse_int(mes)
        if not ano_int or not mes_int or not 1 <= mes_int <= 12:
            raise ValueError(f'Periodo invalido: {ano}/{mes}')
        chave = (ano_int, mes_int)
        if chave not in vistos:
            vistos.add(chave)
            normalizados.append(chave)
    return normalizados


def _deduplicar_linhas_snapshot(rows):
    por_chave = {}
    for values in rows:
        chave = (
            values.get('codigoUG'),
            values.get('conta_contabil'),
            values.get('ano'),
            values.get('mes'),
            values.get('conta_corrente') or '',
            values.get('classificacaoStr') or '',
        )
        por_chave[chave] = values
    return list(por_chave.values())


def sincronizar_saldos_periodos(periodos, usuario_id, siafe_client=None):
    """Busca todos os periodos no SIAFE e substitui snapshots em um commit."""
    periodos = _normalizar_periodos(periodos)
    client = siafe_client or SiafeSaldoContabilClient.from_config()

    resultados = []
    total_registros = 0
    for ano, mes in periodos:
        rows_api = client.consultar_saldo_contabil(
            ano,
            mes,
            CONTA_CONTABIL_FUNDO_ROTATIVO,
            UG_FUNDO_ROTATIVO,
        )
        rows = [
            normalizar_linha_saldo_siafe(row, ano, mes, CONTA_CONTABIL_FUNDO_ROTATIVO)
            for row in rows_api
        ]
        rows = _deduplicar_linhas_snapshot(rows)
        resultados.append((ano, mes, rows))
        total_registros += len(rows)

    sincronizado_em = datetime.utcnow()
    try:
        for ano, mes, rows in resultados:
            (
                FundoRotativoSaldo.query
                .filter(
                    FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
                    FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
                    FundoRotativoSaldo.ano == ano,
                    FundoRotativoSaldo.mes == mes,
                )
                .delete(synchronize_session=False)
            )
            for values in rows:
                values['criado_por'] = usuario_id
                values['sincronizado_por'] = usuario_id
                values['data_sincronizacao'] = sincronizado_em
                db.session.add(FundoRotativoSaldo(**values))

        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    current_app.logger.info(
        f'[FUNDO_ROTATIVO] Sincronizacao concluida periodos={len(periodos)} '
        f'registros={total_registros}'
    )
    return {
        'periodos': len(periodos),
        'registros': total_registros,
    }


def sincronizar_saldos_inicial(usuario_id, siafe_client=None):
    periodos = [(2025, mes) for mes in range(1, 13)]
    periodos.extend((2026, mes) for mes in range(1, 6))
    return sincronizar_saldos_periodos(periodos, usuario_id, siafe_client=siafe_client)


def sincronizar_saldos_mes_atual(usuario_id, siafe_client=None):
    now = datetime.now()
    mes = now.month
    ano = now.year
    # purgar snapshots gerados com mes=12 hardcoded antes da correcao
    if mes != 12:
        try:
            FundoRotativoSaldo.query.filter(
                FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
                FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
                FundoRotativoSaldo.ano == ano,
                FundoRotativoSaldo.mes == 12,
            ).delete(synchronize_session=False)
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
    return sincronizar_saldos_periodos([(ano, mes)], usuario_id, siafe_client=siafe_client)


def _parse_ano(ano):
    if not ano:
        return None
    try:
        ano_int = int(str(ano))
    except (TypeError, ValueError):
        return None
    return ano_int if ano_int > 0 else None


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


def _filtro_ano_saldo(query, anos):
    if not anos:
        return query
    return query.filter(
        db.or_(
            FundoRotativoSaldo.ano.in_(anos),
            extract('year', FundoRotativoSaldo.data).in_(anos),
        )
    )


def listar_saldos(
    page=1,
    busca=None,
    fonte_codigo=None,
    id_exercicio=None,
    ano=None,
    mes=None,
    conta_bancaria=None,
    natureza=None,
    per_page=20,
):
    """Lista snapshots paginados, ordenados por periodo desc."""
    query = FundoRotativoSaldo.query.filter(
        FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
        FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
    )

    fontes = [str(v).strip() for v in _as_list(fonte_codigo) if str(v).strip()]
    if fontes:
        query = query.filter(FundoRotativoSaldo.fonte_codigo.in_(fontes))

    exercicios = [str(v).strip() for v in _as_list(id_exercicio) if str(v).strip()]
    if exercicios:
        query = query.filter(FundoRotativoSaldo.id_exercicio.in_(exercicios))

    anos = []
    for v in _as_list(ano):
        parsed = _parse_ano(v)
        if parsed:
            anos.append(parsed)
    query = _filtro_ano_saldo(query, anos)

    meses = []
    for v in _as_list(mes):
        try:
            m = int(str(v))
        except (TypeError, ValueError):
            continue
        if 1 <= m <= 12:
            meses.append(m)
    if meses:
        query = query.filter(
            db.or_(
                FundoRotativoSaldo.mes.in_(meses),
                extract('month', FundoRotativoSaldo.data).in_(meses),
            )
        )

    contas = [str(v).strip() for v in _as_list(conta_bancaria) if str(v).strip()]
    if contas:
        query = query.filter(FundoRotativoSaldo.conta_bancaria.in_(contas))

    if busca:
        filtro = f'%{busca}%'
        query = query.outerjoin(
            ClassFonte,
            FundoRotativoSaldo.fonte_codigo == ClassFonte.codigo,
        ).filter(
            db.or_(
                FundoRotativoSaldo.fonte_codigo.ilike(filtro),
                FundoRotativoSaldo.fonte_formatada.ilike(filtro),
                FundoRotativoSaldo.conta_corrente.ilike(filtro),
                FundoRotativoSaldo.classificacaoStr.ilike(filtro),
                FundoRotativoSaldo.domicilio_bancario.ilike(filtro),
                FundoRotativoSaldo.banco.ilike(filtro),
                FundoRotativoSaldo.agencia.ilike(filtro),
                FundoRotativoSaldo.conta_bancaria.ilike(filtro),
                ClassFonte.descricao.ilike(filtro),
            )
        )

    soma_total = _as_float(
        query.with_entities(func.coalesce(func.sum(FundoRotativoSaldo.valor), 0)).scalar()
    )

    query = query.order_by(
        FundoRotativoSaldo.ano.desc(),
        FundoRotativoSaldo.mes.desc(),
        FundoRotativoSaldo.fonte_codigo.is_(None).asc(),
        FundoRotativoSaldo.fonte_codigo.asc(),
        FundoRotativoSaldo.id.desc(),
    )
    pagination = query.paginate(page=max(1, page or 1), per_page=per_page, error_out=False)
    return pagination, soma_total


def listar_fontes_saldo_disponiveis():
    """Fontes presentes nos saldos, enriquecidas com class_fonte quando houver."""
    rows = (
        db.session.query(FundoRotativoSaldo.fonte_codigo, FundoRotativoSaldo.fonte_formatada)
        .filter(
            FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
            FundoRotativoSaldo.fonte_codigo.isnot(None),
            FundoRotativoSaldo.fonte_codigo != '',
        )
        .distinct()
        .order_by(FundoRotativoSaldo.fonte_codigo.asc())
        .all()
    )
    fontes_por_codigo = {}
    for codigo, fonte_formatada in rows:
        if codigo and str(codigo) not in fontes_por_codigo:
            fontes_por_codigo[str(codigo)] = fonte_formatada

    codigos = list(fontes_por_codigo.keys())
    descricoes = {}
    if codigos:
        fontes = ClassFonte.query.filter(ClassFonte.codigo.in_(codigos)).all()
        descricoes = {f.codigo: f.descricao for f in fontes}

    return [
        {
            'codigo': codigo,
            'label': (
                f'{codigo} - {descricoes[codigo]}'
                if descricoes.get(codigo)
                else f'{codigo} ({fonte_formatada})' if fonte_formatada else codigo
            ),
        }
        for codigo, fonte_formatada in fontes_por_codigo.items()
    ]


def listar_anos_disponiveis():
    """Anos distintos presentes nos registros de saldo (desc)."""
    anos = set()
    rows = (
        db.session.query(FundoRotativoSaldo.ano)
        .filter(
            FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
            FundoRotativoSaldo.ano.isnot(None),
        )
        .distinct()
        .all()
    )
    anos.update(int(r[0]) for r in rows if r[0] is not None)

    legacy_rows = (
        db.session.query(extract('year', FundoRotativoSaldo.data))
        .filter(
            FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
            FundoRotativoSaldo.data.isnot(None),
        )
        .distinct()
        .all()
    )
    anos.update(int(r[0]) for r in legacy_rows if r[0] is not None)
    return sorted(anos, reverse=True)


def listar_contas_disponiveis():
    """Numeros de conta bancaria distintos presentes nos saldos (asc)."""
    rows = (
        db.session.query(FundoRotativoSaldo.conta_bancaria)
        .filter(
            FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_bancaria.isnot(None),
            FundoRotativoSaldo.conta_bancaria != '',
        )
        .distinct()
        .order_by(FundoRotativoSaldo.conta_bancaria.asc())
        .all()
    )
    return [{'codigo': c, 'label': c} for (c,) in rows]


def listar_meses_disponiveis():
    """Meses distintos (1-12) presentes nos registros de saldo, asc."""
    meses = set()
    rows = (
        db.session.query(FundoRotativoSaldo.mes)
        .filter(
            FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
            FundoRotativoSaldo.mes.isnot(None),
        )
        .distinct()
        .all()
    )
    meses.update(int(r[0]) for r in rows if r[0] is not None)

    legacy_rows = (
        db.session.query(extract('month', FundoRotativoSaldo.data))
        .filter(
            FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
            FundoRotativoSaldo.data.isnot(None),
        )
        .distinct()
        .all()
    )
    meses.update(int(r[0]) for r in legacy_rows if r[0] is not None)
    return sorted(m for m in meses if 1 <= m <= 12)


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
    query = (
        db.session.query(func.coalesce(func.sum(FundoRotativoSaldo.valor), 0))
        .filter(
            FundoRotativoSaldo.codigoUG == UG_FUNDO_ROTATIVO,
            FundoRotativoSaldo.conta_contabil == CONTA_CONTABIL_FUNDO_ROTATIVO,
        )
    )

    if filtros.get('fontes'):
        query = query.filter(FundoRotativoSaldo.fonte_codigo.in_(filtros['fontes']))
    query = _filtro_ano_saldo(query, filtros.get('anos'))

    return _as_float(query.scalar())


def listar_anos_dashboard_disponiveis():
    """Anos disponiveis para filtro do dashboard do Fundo Rotativo."""
    ano_atual = datetime.now().year
    anos = {ano_atual}
    anos.update(listar_anos_disponiveis())

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
    """Retorna KPIs e tabela de execucao por contrato da UG do Fundo Rotativo."""
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
