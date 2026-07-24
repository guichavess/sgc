"""
Testes da sincronização do módulo de Pagamentos (SEI + Etapas + Saldos).

Cobre:
- atualizar_saldos_em_lote: não cascateia erro entre combinações (bug da Fase 3)
- registrar_log / obter_ultima_sincronizacao: fonte da "última atualização"
- endpoint /api/registrar-sincronizacao: grava log (200 logado, 401 anônimo)
- dashboard: exibe a data da última sincronização de forma confiável
"""
from datetime import datetime, timedelta
import uuid

import pytest


def _uid():
    return uuid.uuid4().hex[:10]


def _login_admin(client, db_session):
    from app.models.usuario import Usuario

    uid = _uid()
    usuario = Usuario(
        id_usuario_sei=f'sync_admin_{uid}',
        nome='ADMIN SYNC',
        sigla_login=f'sync_admin_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True

    return usuario


# ──────────────────────────────────────────────────────────────────────────────
# Fase 3 — atualizar_saldos_em_lote (correção da cascata de erros)
# ──────────────────────────────────────────────────────────────────────────────

def test_atualizar_saldos_em_lote_nao_cascateia(app, db_session, monkeypatch):
    """Falha de UMA combinação não impede as demais de serem atualizadas."""
    from app.services import sincronizacao_pagamentos_service as svc
    from app.services.saldo_service import SaldoService
    from app.models import SaldoEmpenho

    combinacoes = [('C1', '01/2026'), ('C2', '02/2026'), ('C3', '03/2026')]

    def fake_calc(codigo_contrato, competencia, ano=None):
        if codigo_contrato == 'C1':
            raise RuntimeError('SIAFE indisponível')
        return 100.0

    monkeypatch.setattr(
        SaldoService, 'calcular_saldo_disponivel', staticmethod(fake_calc)
    )

    atualizados, erros, lista_erros = svc.atualizar_saldos_em_lote(combinacoes)

    assert erros == 1
    assert atualizados == 2
    assert len(lista_erros) == 1
    # C2 e C3 persistidos mesmo após a falha de C1 (sem envenenar a sessão)
    assert SaldoEmpenho.get_saldo_atual('C2', '02/2026') is not None
    assert SaldoEmpenho.get_saldo_atual('C3', '03/2026') is not None
    assert SaldoEmpenho.get_saldo_atual('C1', '01/2026') is None


def test_atualizar_saldos_em_lote_deduplica_por_contrato_ano(app, db_session, monkeypatch):
    """
    O saldo depende apenas de (contrato, ano). Várias competências do mesmo
    contrato/ano NÃO devem recalcular os totais SIAFE repetidamente — o cálculo
    pesado roda 1x por (contrato, ano) e é reaproveitado (correção da lentidão).
    """
    from app.services import sincronizacao_pagamentos_service as svc
    from app.services.saldo_service import SaldoService
    from app.models import SaldoEmpenho

    chamadas = []

    def fake_calc(codigo_contrato, competencia, ano=None):
        chamadas.append((codigo_contrato, competencia))
        return 500.0

    monkeypatch.setattr(
        SaldoService, 'calcular_saldo_disponivel', staticmethod(fake_calc)
    )

    # 3 competências do MESMO contrato/ano + 1 de outro ano
    combinacoes = [
        ('C7', '01/2026'), ('C7', '02/2026'), ('C7', '03/2026'),
        ('C7', '01/2025'),
    ]

    atualizados, erros, _ = svc.atualizar_saldos_em_lote(combinacoes)

    # Todas as 4 combinações são persistidas...
    assert atualizados == 4
    assert erros == 0
    # ...mas o cálculo pesado rodou só 2x (C7/2026 e C7/2025), não 4x.
    assert len(chamadas) == 2
    for comp in ('01/2026', '02/2026', '03/2026', '01/2025'):
        assert float(SaldoEmpenho.get_saldo_atual('C7', comp).saldo) == 500.0


def test_gravar_saldos_calculados_upsert(app, db_session):
    """gravar_saldos_calculados persiste a lista de calculados em 1 commit."""
    from app.services import sincronizacao_pagamentos_service as svc
    from app.models import SaldoEmpenho

    existente = SaldoEmpenho(
        cod_contrato='C4', competencia='07/2026',
        saldo=1, data=datetime.now() - timedelta(days=2),
    )
    db_session.add(existente)
    db_session.commit()

    calculados = [('C4', '07/2026', 999.0), ('C5', '07/2026', 123.0)]
    atualizados, lista_erros = svc.gravar_saldos_calculados(calculados)

    assert atualizados == 2
    assert lista_erros == []
    # Atualiza o existente (sem duplicar) e cria o novo
    linhas_c4 = SaldoEmpenho.query.filter_by(cod_contrato='C4', competencia='07/2026').all()
    assert len(linhas_c4) == 1
    assert float(linhas_c4[0].saldo) == 999.0
    assert float(SaldoEmpenho.get_saldo_atual('C5', '07/2026').saldo) == 123.0


def test_calcular_saldo_item_usa_cache(app, db_session, monkeypatch):
    """calcular_saldo_item reaproveita o cache por (contrato, ano)."""
    from app.services import sincronizacao_pagamentos_service as svc
    from app.services.saldo_service import SaldoService

    chamadas = []

    def fake_calc(codigo_contrato, competencia, ano=None):
        chamadas.append(competencia)
        return 42.0

    monkeypatch.setattr(
        SaldoService, 'calcular_saldo_disponivel', staticmethod(fake_calc)
    )

    cache = {}
    s1, e1 = svc.calcular_saldo_item('C1', '01/2026', cache)
    s2, e2 = svc.calcular_saldo_item('C1', '11/2026', cache)  # mesmo ano → cache

    assert (s1, e1) == (42.0, None)
    assert (s2, e2) == (42.0, None)
    assert len(chamadas) == 1  # só a primeira competência chamou o cálculo pesado


def test_atualizar_saldos_em_lote_atualiza_registro_existente(app, db_session, monkeypatch):
    """Combinação já existente é atualizada (não duplica linha)."""
    from app.services import sincronizacao_pagamentos_service as svc
    from app.services.saldo_service import SaldoService
    from app.models import SaldoEmpenho

    existente = SaldoEmpenho(
        cod_contrato='C9', competencia='05/2026',
        saldo=10, data=datetime.now() - timedelta(days=1),
    )
    db_session.add(existente)
    db_session.commit()

    monkeypatch.setattr(
        SaldoService, 'calcular_saldo_disponivel',
        staticmethod(lambda codigo_contrato, competencia, ano=None: 777.0),
    )

    atualizados, erros, _ = svc.atualizar_saldos_em_lote([('C9', '05/2026')])

    assert atualizados == 1
    assert erros == 0
    linhas = SaldoEmpenho.query.filter_by(cod_contrato='C9', competencia='05/2026').all()
    assert len(linhas) == 1
    assert float(linhas[0].saldo) == 777.0


# ──────────────────────────────────────────────────────────────────────────────
# Log de sincronização — fonte da "última atualização"
# ──────────────────────────────────────────────────────────────────────────────

def test_registrar_e_obter_ultima_sincronizacao(app, db_session):
    from app.services import sincronizacao_pagamentos_service as svc

    log = svc.registrar_log(
        origem='manual', status='sucesso',
        docs=5, etapas=3, saldos=10, erros=0, usuario_id=1,
    )
    assert log.id is not None
    assert log.finalizado_em is not None
    assert log.saldos_atualizados == 10

    ultima = svc.obter_ultima_sincronizacao()
    assert ultima is not None
    assert ultima.id == log.id


def test_obter_ultima_retorna_a_mais_recente(app, db_session):
    from app.services import sincronizacao_pagamentos_service as svc
    from app.models import SincronizacaoLog

    antigo = SincronizacaoLog(
        origem='agendada', status='sucesso',
        finalizado_em=datetime.now() - timedelta(hours=6),
    )
    db_session.add(antigo)
    db_session.commit()

    recente = svc.registrar_log(origem='manual', status='sucesso')

    ultima = svc.obter_ultima_sincronizacao()
    assert ultima.id == recente.id


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint /api/registrar-sincronizacao
# ──────────────────────────────────────────────────────────────────────────────

def test_endpoint_registrar_sincronizacao_logado(client, db_session):
    _login_admin(client, db_session)
    from app.models import SincronizacaoLog

    resp = client.post(
        '/solicitacoes/api/registrar-sincronizacao',
        json={'docs': 4, 'etapas': 2, 'saldos': 7, 'erros': 1, 'status': 'parcial'},
    )
    assert resp.status_code == 200

    log = SincronizacaoLog.query.order_by(SincronizacaoLog.id.desc()).first()
    assert log is not None
    assert log.origem == 'manual'
    assert log.saldos_atualizados == 7
    assert log.status == 'parcial'
    assert log.finalizado_em is not None


def test_endpoint_registrar_sincronizacao_anonimo(client, db_session):
    resp = client.post(
        '/solicitacoes/api/registrar-sincronizacao',
        json={'docs': 1, 'etapas': 1, 'saldos': 1, 'erros': 0},
    )
    assert resp.status_code in (401, 302)


# ──────────────────────────────────────────────────────────────────────────────
# Dashboard exibe a última atualização
# ──────────────────────────────────────────────────────────────────────────────

def test_dashboard_exibe_ultima_sincronizacao(client, db_session):
    _login_admin(client, db_session)
    from app.services import sincronizacao_pagamentos_service as svc

    log = svc.registrar_log(origem='manual', status='sucesso', saldos=3)

    resp = client.get('/solicitacoes/dashboard')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert log.finalizado_em.strftime('%d/%m/%Y') in html
