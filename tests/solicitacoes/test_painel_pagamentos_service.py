"""
Service do Painel de Pagamentos (indicadores).

Lógica movida de `app/solicitacoes/routes/reports.py` para
`app/services/painel_pagamentos_service.py` sem alteração de regra:
filtros globais, contagem por checkpoint (CHECKPOINTS_RELATORIO) e
tempo médio por fase a partir do histórico de movimentações.
"""
from datetime import datetime, timedelta

import pytest
from werkzeug.datastructures import MultiDict

from app.services import painel_pagamentos_service as painel


# ──────────────────────────────────────────────────────────────────────────────
# Leitura de filtros
# ──────────────────────────────────────────────────────────────────────────────

def test_ler_filtros_converte_query_string():
    args = MultiDict([
        ('competencia', ' 08/2026 '),
        ('contratado', ' ACME '),
        ('data_inicio', '2026-02-01'),
        ('data_fim', '2026-02-28'),
        ('filtro_tipo', '1'),
        ('filtro_tipo', 'x'),
        ('filtro_tipo', '3'),
    ])
    f = painel.ler_filtros(args)

    assert f.competencia == '08/2026'
    assert f.contratado == 'ACME'
    assert f.data_inicio == datetime(2026, 2, 1)
    assert f.data_fim == datetime(2026, 2, 28, 23, 59, 59)
    assert f.tipo_pagamento_ids == [1, 3]


def test_ler_filtros_vazios_viram_none():
    f = painel.ler_filtros(MultiDict([('competencia', '  '), ('data_inicio', 'lixo')]))

    assert f.competencia is None
    assert f.contratado is None
    assert f.data_inicio is None
    assert f.data_fim is None
    assert f.tipo_pagamento_ids is None


@pytest.mark.parametrize('delta,esperado', [
    (timedelta(days=-1), '0d'),
    (timedelta(hours=2), '1d'),
    (timedelta(minutes=30), '0d'),
    (timedelta(days=4, hours=5), '4d'),
])
def test_formatar_delta_str(delta, esperado):
    assert painel.formatar_delta_str(delta) == esperado


# ──────────────────────────────────────────────────────────────────────────────
# Cenário com dados
# ──────────────────────────────────────────────────────────────────────────────

D0 = datetime(2026, 2, 10, 9, 0)


@pytest.fixture()
def cenario(db_session):
    """
    Etapas com nomes neutros (não caem no mapa por nome de ids_adicionais).
      s1: etapa 1  · 08/2026 · ACME   · tipo 1 · 10/02
      s2: etapa 12 · 08/2026 · BETA   · tipo 2 · 20/02
      s3: etapa 13 · 09/2026 · ACME   · tipo 1 · 05/03
      s4: etapa 5  · 09/2026 · GAMA   · tipo 2 · 10/02  (finalizado, com histórico)
    """
    from app.models import Solicitacao, Contrato, Etapa, HistoricoMovimentacao, TipoPagamento

    for eid in (1, 2, 5, 12, 13):
        db_session.add(Etapa(id=eid, nome=f'Etapa teste {eid}', alias=f'e{eid}', ordem=eid))
    db_session.add(TipoPagamento(id=1, nome='Regular'))
    db_session.add(TipoPagamento(id=2, nome='DEA'))
    db_session.add(Contrato(codigo='C1', nomeContratado='ACME SERVICOS LTDA'))
    db_session.add(Contrato(codigo='C2', nomeContratado='BETA LTDA'))
    db_session.add(Contrato(codigo='C3', nomeContratado='GAMA SA'))

    def sol(sid, contrato, etapa, comp, tipo, data, status='ABERTO'):
        db_session.add(Solicitacao(
            id=sid, codigo_contrato=contrato, id_usuario_solicitante=1,
            etapa_atual_id=etapa, competencia=comp, id_tipo_pagamento=tipo,
            data_solicitacao=data, status_geral=status,
        ))

    sol(1, 'C1', 1, '08/2026', 1, datetime(2026, 2, 10))
    sol(2, 'C2', 12, '08/2026', 2, datetime(2026, 2, 20))
    sol(3, 'C1', 13, '09/2026', 1, datetime(2026, 3, 5))
    sol(4, 'C3', 5, '09/2026', 2, D0, status='PAGO')
    db_session.flush()

    for hid, etapa, quando in ((1, 1, D0), (2, 2, D0 + timedelta(days=3)), (3, 5, D0 + timedelta(days=10))):
        db_session.add(HistoricoMovimentacao(
            id=hid, id_solicitacao=4, id_etapa_nova=etapa, data_movimentacao=quando,
        ))
    db_session.commit()


def _ids_filtrados(**kw):
    from app.models import Solicitacao, Contrato

    q = Solicitacao.query.join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
    q = painel.aplicar_filtros(q, painel.FiltrosPainel(**kw))
    return sorted(s.id for s in q.all())


def test_aplicar_filtros_sem_filtro_retorna_todos(cenario):
    assert _ids_filtrados() == [1, 2, 3, 4]


def test_aplicar_filtros_competencia(cenario):
    assert _ids_filtrados(competencia='08/2026') == [1, 2]


def test_aplicar_filtros_contratado_parcial(cenario):
    assert _ids_filtrados(contratado='acme') == [1, 3]


def test_aplicar_filtros_periodo(cenario):
    assert _ids_filtrados(
        data_inicio=datetime(2026, 2, 15), data_fim=datetime(2026, 2, 28, 23, 59, 59)
    ) == [2]


def test_aplicar_filtros_tipo_pagamento(cenario):
    assert _ids_filtrados(tipo_pagamento_ids=[2]) == [2, 4]


def test_resumo_por_fase_conta_checkpoints_e_grupos(cenario):
    resumo = painel.resumo_por_fase(painel.FiltrosPainel())
    por_nome = {r['nome']: r for r in resumo}

    assert por_nome['Solicitação Criada']['qtd'] == 1
    assert por_nome['Atesto e Fiscalização']['qtd'] == 2   # etapas 12 + 13
    assert por_nome['Atesto e Fiscalização']['eh_grupo'] is True
    assert por_nome['Financeiro']['qtd'] == 1
    assert por_nome['NF Atestada']['qtd'] == 0


def test_resumo_por_fase_respeita_filtro(cenario):
    resumo = painel.resumo_por_fase(painel.FiltrosPainel(competencia='09/2026'))
    por_nome = {r['nome']: r['qtd'] for r in resumo}

    assert por_nome['Solicitação Criada'] == 0
    assert por_nome['Atesto e Fiscalização'] == 1
    assert por_nome['Financeiro'] == 1


def test_dados_visao_geral_pagina_e_conta_por_etapa(cenario):
    resumo, detalhes, pagination, contagem_etapa = painel.dados_visao_geral(
        painel.FiltrosPainel(), page=1, per_page=2,
    )

    assert len(resumo) == 7
    assert pagination.total == 4
    assert len(detalhes) == 2
    assert contagem_etapa['Etapa teste 12'] == 1


def test_dados_metricas_media_de_tempo_por_fase(cenario):
    timeline, matriz, colunas, pagination = painel.dados_metricas(
        painel.FiltrosPainel(competencia='09/2026', contratado='GAMA'), page_matriz=1,
    )
    medias = {t['nome']: (t['media_entrada'], t['qtd_base']) for t in timeline}

    assert medias['Solicitação Criada'] == ('3d', 1)
    assert medias['Documentação Solicitada'] == ('7d', 1)
    assert medias['Financeiro'] == ('0d', 1)       # finalizado: fase final não acumula
    assert medias['NF Atestada'] == ('--', 0)
    assert len(colunas) == 7
    assert pagination.total == 1
    assert matriz[0]['tempos'] == {0: '3d', 1: '7d', 6: '0d'}


def test_listar_detalhes_sem_paginacao(cenario):
    detalhes = painel.listar_detalhes(painel.FiltrosPainel(tipo_pagamento_ids=[1]))
    assert sorted(s.id for s in detalhes) == [1, 3]
