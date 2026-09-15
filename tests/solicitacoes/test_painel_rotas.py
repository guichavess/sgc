"""
Reorganização das telas de Pagamentos: Solicitações · Painel · Relatórios.

- /solicitacoes/painel         → indicadores (abas Visão Geral / Métricas)
- /solicitacoes/relatorios     → central de relatórios (impressão/auditoria)
  · com `aba_ativa` (links antigos) → 302 para o Painel preservando filtros
- /solicitacoes/api/painel/<indicador> → JSON dos gráficos do Painel
"""
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import pytest

PERM_VISUALIZAR = [('solicitacoes', '', 'visualizar')]


@pytest.fixture()
def dados(db_session):
    from app.models import Solicitacao, Contrato, Etapa, TipoPagamento

    db_session.add(Etapa(id=1, nome='Etapa teste 1', alias='e1', ordem=1, cor_hex='#0d6efd'))
    db_session.add(Etapa(id=12, nome='Etapa teste 12', alias='e12', ordem=12, cor_hex='#fd7e14'))
    db_session.add(TipoPagamento(id=1, nome='Regular'))
    db_session.add(Contrato(codigo='C1', nomeContratado='ACME SERVICOS LTDA'))
    db_session.add(Contrato(codigo='C2', nomeContratado='BETA LTDA'))
    db_session.add(Solicitacao(
        id=1, codigo_contrato='C1', id_usuario_solicitante=1, etapa_atual_id=1,
        competencia='08/2026', id_tipo_pagamento=1, data_solicitacao=datetime(2026, 2, 10),
        protocolo_gerado_sei='00002.000001/2026-01',
    ))
    db_session.add(Solicitacao(
        id=2, codigo_contrato='C2', id_usuario_solicitante=1, etapa_atual_id=12,
        competencia='09/2026', id_tipo_pagamento=1, data_solicitacao=datetime(2026, 3, 1),
        protocolo_gerado_sei='00002.000002/2026-02',
    ))
    db_session.commit()


@pytest.fixture()
def cliente_leitor(dados, usuario_com_permissoes, logar):
    return logar(usuario_com_permissoes(7101, permissoes=PERM_VISUALIZAR))


@pytest.fixture()
def cliente_admin(dados, usuario_com_permissoes, logar):
    return logar(usuario_com_permissoes(7102, is_admin=True))


@pytest.fixture()
def cliente_sem_permissao(dados, usuario_com_permissoes, logar):
    return logar(usuario_com_permissoes(7103))


# ──────────────────────────────────────────────────────────────────────────────
# Painel
# ──────────────────────────────────────────────────────────────────────────────

def test_painel_exige_login(client, dados):
    resp = client.get('/solicitacoes/painel')
    assert resp.status_code == 302
    assert '/auth' in resp.headers['Location']


def test_painel_sem_permissao_redireciona(cliente_sem_permissao):
    resp = cliente_sem_permissao.get('/solicitacoes/painel')
    assert resp.status_code == 302
    assert '/solicitacoes/painel' not in resp.headers['Location']


def test_painel_visao_geral(cliente_leitor):
    resp = cliente_leitor.get('/solicitacoes/painel')
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert 'Solicitação Criada' in html
    assert '00002.000001/2026-01' in html
    assert 'Matriz de Tempos por Fase' not in html   # aba métricas não é calculada


def test_painel_visao_geral_respeita_filtro(cliente_leitor):
    resp = cliente_leitor.get('/solicitacoes/painel?competencia=09/2026')
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert '00002.000002/2026-02' in html
    assert '00002.000001/2026-01' not in html


def test_painel_aba_metricas_renderiza_matriz(cliente_leitor):
    resp = cliente_leitor.get('/solicitacoes/painel?aba=metricas')
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert 'Matriz de Tempos por Fase' in html
    assert '00002.000001/2026-01' in html


def test_sidebar_tem_solicitacoes_painel_relatorios(cliente_leitor):
    html = cliente_leitor.get('/solicitacoes/painel').get_data(as_text=True)

    assert 'href="/solicitacoes/dashboard"' in html
    assert 'href="/solicitacoes/painel"' in html
    assert 'href="/solicitacoes/relatorios"' in html
    assert 'Solicitações' in html
    assert 'Painel' in html
    assert 'Relatórios' in html
    assert '> Dashboard' not in html


def test_painel_grafico_por_fase_em_barras_horizontais_com_escala_log(cliente_leitor):
    html = cliente_leitor.get('/solicitacoes/painel').get_data(as_text=True)

    assert 'data-indicador="por-fase" data-tipo="barras_log"' in html
    assert 'escala logarítmica' in html
    assert 'horizontal: true' in html


# ──────────────────────────────────────────────────────────────────────────────
# Relatórios (central)
# ──────────────────────────────────────────────────────────────────────────────

def test_relatorios_link_antigo_redireciona_para_painel_com_filtros(cliente_leitor):
    resp = cliente_leitor.get(
        '/solicitacoes/relatorios?aba_ativa=metricas&competencia=08/2026'
        '&filtro_tipo=1&filtro_tipo=2&page_matriz=3'
    )
    assert resp.status_code == 302

    destino = urlparse(resp.headers['Location'])
    qs = parse_qs(destino.query)
    assert destino.path == '/solicitacoes/painel'
    assert qs['aba'] == ['metricas']
    assert qs['competencia'] == ['08/2026']
    assert qs['filtro_tipo'] == ['1', '2']
    assert qs['page_matriz'] == ['3']
    assert 'aba_ativa' not in qs


def test_relatorios_central_renderiza_cards(cliente_leitor):
    resp = cliente_leitor.get('/solicitacoes/relatorios')
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert 'Relatório Geral' in html
    assert 'Métricas e Performance' in html
    assert '/solicitacoes/relatorios/imprimir' in html
    assert '/solicitacoes/relatorios/auditoria' not in html


def test_relatorios_central_mostra_auditoria_para_admin(cliente_admin):
    html = cliente_admin.get('/solicitacoes/relatorios').get_data(as_text=True)

    assert '/solicitacoes/relatorios/auditoria' in html
    assert '/solicitacoes/relatorios/auditoria/csv' in html


def test_relatorios_exige_login(client, dados):
    resp = client.get('/solicitacoes/relatorios')
    assert resp.status_code == 302
    assert '/auth' in resp.headers['Location']


@pytest.mark.parametrize('aba,marcador', [
    ('geral', 'Relatório Geral de Processos'),
    ('metricas', 'Relatório de Métricas e Performance'),
])
def test_relatorios_imprimir_continua_funcionando(cliente_leitor, aba, marcador):
    resp = cliente_leitor.get(f'/solicitacoes/relatorios/imprimir?aba_ativa={aba}&competencia=08/2026')
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert marcador in html
    assert '00002.000001/2026-01' in html
    assert '00002.000002/2026-02' not in html


def test_auditoria_admin_200(cliente_admin):
    assert cliente_admin.get('/solicitacoes/relatorios/auditoria').status_code == 200
    csv = cliente_admin.get('/solicitacoes/relatorios/auditoria/csv')
    assert csv.status_code == 200
    assert '00002.000001/2026-01' in csv.get_data(as_text=True)


def test_auditoria_nao_admin_redireciona(cliente_leitor):
    resp = cliente_leitor.get('/solicitacoes/relatorios/auditoria')
    assert resp.status_code == 302


# ──────────────────────────────────────────────────────────────────────────────
# API do Painel (Fase 3 — esqueleto)
# ──────────────────────────────────────────────────────────────────────────────

def test_api_painel_por_fase_json(cliente_leitor):
    resp = cliente_leitor.get('/solicitacoes/api/painel/por-fase')
    assert resp.status_code == 200

    dados_json = resp.get_json()
    por_nome = {s['nome']: s['qtd'] for s in dados_json['series']}
    assert por_nome['Solicitação Criada'] == 1
    assert por_nome['Atesto e Fiscalização'] == 1
    assert dados_json['total'] == 2


def test_api_painel_por_fase_respeita_filtro(cliente_leitor):
    dados_json = cliente_leitor.get('/solicitacoes/api/painel/por-fase?competencia=09/2026').get_json()
    por_nome = {s['nome']: s['qtd'] for s in dados_json['series']}

    assert por_nome['Solicitação Criada'] == 0
    assert por_nome['Atesto e Fiscalização'] == 1
    assert dados_json['total'] == 1


def test_api_painel_exige_login(client, dados):
    resp = client.get('/solicitacoes/api/painel/por-fase')
    assert resp.status_code == 302


def test_api_painel_indicador_inexistente_404(cliente_leitor):
    assert cliente_leitor.get('/solicitacoes/api/painel/nao-existe').status_code == 404
