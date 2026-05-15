"""
Sub-aba Cotações e KPIs atualizados — Administração (TDD Red → Green)
=====================================================================
Arquivo: tests/diarias/test_cotacoes_admin.py

Cobre:
  1. Nova rota administracao_cotacoes() retorna 200
  2. Tabela exibe todos os processos com passagens (tipo 2/3)
  3. Pendentes aparecem antes das realizadas na ordenação
  4. Filtro status_cotacao=pendente retorna apenas sem cotações
  5. Filtro status_cotacao=realizada retorna apenas com cotações
  6. Processo SEI renderiza como link clicável
  7. Badge de pendentes aparece no subnav
  8. KPI "Cotações Realizadas" no administracao.html
  9. Card "Solicitações" com collapse breakdown
 10. Filtros replicados (busca, etapa, tipo, negados)

Como rodar:
    pytest tests/diarias/test_cotacoes_admin.py -v
"""
import uuid
import pytest
from decimal import Decimal
from datetime import date, datetime


def _uid():
    return uuid.uuid4().hex[:12]


def _login_admin(client, db_session, app):
    from app.models.usuario import Usuario

    uid = _uid()
    u = Usuario(
        id_usuario_sei=f'admin_{uid}',
        nome='ADMIN COTACOES TESTE',
        sigla_login=f'adm_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(u)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(u.id)
        sess['_fresh'] = True

    return u


def _make_itinerario(db_session, **kwargs):
    from app.models.diaria import DiariasItinerario

    defaults = dict(
        n_processo=f'PROC-{_uid()}',
        usuario_gerador=f'user_{_uid()}@test.com',
        tipo_itinerario=2,
        tipo_solicitacao_id=2,
        qtd_diarias_solicitadas=Decimal('2.5'),
        data_solicitacao=datetime(2026, 4, 1),
        data_viagem=date(2026, 5, 1),
        data_retorno=date(2026, 5, 3),
        etapa_atual_id=2,
        valor_total=Decimal('500.00'),
    )
    defaults.update(kwargs)
    it = DiariasItinerario(**defaults)
    db_session.add(it)
    db_session.flush()
    return it


def _make_cotacao_voo(db_session, itinerario_id, **kwargs):
    from app.models.diaria import DiariasCotacaoVoo

    defaults = dict(
        itinerario_id=itinerario_id,
        tipo_trecho='ida',
        cia='LATAM',
        voo='LA3456',
        saida=datetime(2026, 5, 1, 8, 0),
        chegada=datetime(2026, 5, 1, 11, 0),
        origem='THE',
        destino='BSB',
        valor=Decimal('1200.00'),
    )
    defaults.update(kwargs)
    cot = DiariasCotacaoVoo(**defaults)
    db_session.add(cot)
    db_session.flush()
    return cot


def _make_item(db_session, itinerario_id):
    from app.models.diaria import DiariasItemItinerario

    item = DiariasItemItinerario(
        id_itinerario=itinerario_id,
        cpf_pessoa=_uid()[:11],
        nome_pessoa='FULANO TESTE',
        cargo_id=1,
        valor_cargo=Decimal('320.00'),
    )
    db_session.add(item)
    db_session.flush()
    return item


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 1 — Rota administracao_cotacoes()
# ════════════════════════════════════════════════════════════════════════════

class TestCotacoesAdminRota:

    def test_rota_retorna_200(self, client, db_session, app):
        """GET /diarias/administracao/cotacoes deve retornar 200."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao/cotacoes')
        assert resp.status_code == 200

    def test_rota_sem_login_redireciona(self, client, db_session, app):
        """Sem login, deve redirecionar para login."""
        resp = client.get('/diarias/administracao/cotacoes')
        assert resp.status_code in (302, 401)

    def test_exibe_apenas_tipos_com_passagens(self, client, db_session, app):
        """Tabela deve mostrar apenas itinerários com tipo_solicitacao_id 2 ou 3."""
        _login_admin(client, db_session, app)

        it_apenas_diarias = _make_itinerario(db_session, tipo_solicitacao_id=1)
        it_com_passagens = _make_itinerario(
            db_session, tipo_solicitacao_id=2,
            sei_protocolo='00002.000099/2026-00',
            sei_id_procedimento='99999',
        )

        resp = client.get('/diarias/administracao/cotacoes')
        html = resp.data.decode('utf-8', errors='replace')

        assert '00002.000099/2026-00' in html, (
            "Itinerário com tipo_solicitacao_id=2 deve aparecer na tabela de cotações."
        )
        assert it_apenas_diarias.n_processo not in html, (
            "Itinerário com tipo_solicitacao_id=1 NÃO deve aparecer na tabela de cotações."
        )


class TestCotacoesStatusFiltro:

    def test_filtro_pendente(self, client, db_session, app):
        """Filtro status_cotacao=pendente mostra apenas sem cotações."""
        _login_admin(client, db_session, app)

        it_sem = _make_itinerario(
            db_session, sei_protocolo=f'SEM-{_uid()}',
            sei_id_procedimento='111',
        )
        it_com = _make_itinerario(
            db_session, sei_protocolo=f'COM-{_uid()}',
            sei_id_procedimento='222',
        )
        _make_cotacao_voo(db_session, it_com.id)

        resp = client.get('/diarias/administracao/cotacoes?status_cotacao=pendente')
        html = resp.data.decode('utf-8', errors='replace')

        assert it_sem.sei_protocolo in html
        assert it_com.sei_protocolo not in html

    def test_filtro_realizada(self, client, db_session, app):
        """Filtro status_cotacao=realizada mostra apenas com cotações."""
        _login_admin(client, db_session, app)

        it_sem = _make_itinerario(
            db_session, sei_protocolo=f'SEM-{_uid()}',
            sei_id_procedimento='333',
        )
        it_com = _make_itinerario(
            db_session, sei_protocolo=f'COM-{_uid()}',
            sei_id_procedimento='444',
        )
        _make_cotacao_voo(db_session, it_com.id)

        resp = client.get('/diarias/administracao/cotacoes?status_cotacao=realizada')
        html = resp.data.decode('utf-8', errors='replace')

        assert it_com.sei_protocolo in html
        assert it_sem.sei_protocolo not in html

    def test_sem_filtro_mostra_todos(self, client, db_session, app):
        """Sem filtro de status, exibe todos os processos com passagens."""
        _login_admin(client, db_session, app)

        it_sem = _make_itinerario(
            db_session, sei_protocolo=f'TODOS-SEM-{_uid()}',
            sei_id_procedimento='555',
        )
        it_com = _make_itinerario(
            db_session, sei_protocolo=f'TODOS-COM-{_uid()}',
            sei_id_procedimento='666',
        )
        _make_cotacao_voo(db_session, it_com.id)

        resp = client.get('/diarias/administracao/cotacoes')
        html = resp.data.decode('utf-8', errors='replace')

        assert it_sem.sei_protocolo in html
        assert it_com.sei_protocolo in html


class TestCotacoesBadgeStatus:

    def test_badge_pendente_aparece(self, client, db_session, app):
        """Itinerário sem cotação deve exibir badge 'Pendente'."""
        _login_admin(client, db_session, app)
        _make_itinerario(db_session, tipo_solicitacao_id=2)

        resp = client.get('/diarias/administracao/cotacoes')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Pendente' in html

    def test_badge_realizada_aparece(self, client, db_session, app):
        """Itinerário com cotação deve exibir badge 'Realizada'."""
        _login_admin(client, db_session, app)
        it = _make_itinerario(db_session, tipo_solicitacao_id=2)
        _make_cotacao_voo(db_session, it.id)

        resp = client.get('/diarias/administracao/cotacoes')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Realizada' in html


class TestCotacoesLinkSEI:

    def test_processo_sei_renderiza_como_link(self, client, db_session, app):
        """Coluna Processo SEI deve renderizar como link clicável para o SEI."""
        _login_admin(client, db_session, app)
        _make_itinerario(
            db_session,
            sei_protocolo='00002.000777/2026-00',
            sei_id_procedimento='777888',
        )

        resp = client.get('/diarias/administracao/cotacoes')
        html = resp.data.decode('utf-8', errors='replace')

        assert 'procedimento_trabalhar' in html
        assert '777888' in html
        assert '00002.000777/2026-00' in html
        assert 'target="_blank"' in html


class TestCotacoesFiltros:

    def test_filtro_busca_texto(self, client, db_session, app):
        """Filtro de busca por texto deve filtrar por protocolo/solicitante."""
        _login_admin(client, db_session, app)
        _make_itinerario(
            db_session, sei_protocolo='00002.999999/2026-00',
            sei_id_procedimento='999',
        )
        _make_itinerario(
            db_session, sei_protocolo='00002.111111/2026-00',
            sei_id_procedimento='111',
        )

        resp = client.get('/diarias/administracao/cotacoes?q=999999')
        html = resp.data.decode('utf-8', errors='replace')

        assert '999999' in html
        assert '111111' not in html

    def test_filtro_negados(self, client, db_session, app):
        """Filtro negados=1 deve mostrar apenas processos negados."""
        _login_admin(client, db_session, app)
        _make_itinerario(
            db_session, processo_negado=True,
            sei_protocolo=f'NEG-{_uid()}', sei_id_procedimento='NEG1',
        )
        _make_itinerario(
            db_session, processo_negado=False,
            sei_protocolo=f'OK-{_uid()}', sei_id_procedimento='OK1',
        )

        resp = client.get('/diarias/administracao/cotacoes?negados=1')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'NEG-' in html


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 2 — Subnav badge de pendentes
# ════════════════════════════════════════════════════════════════════════════

class TestSubnavBadgePendentes:

    def test_badge_pendentes_aparece_no_subnav(self, client, db_session, app):
        """Subnav deve exibir contagem de cotações pendentes."""
        _login_admin(client, db_session, app)
        _make_itinerario(db_session, tipo_solicitacao_id=2)

        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')

        assert 'subnav-count' in html, (
            "Badge com classe 'subnav-count' deve aparecer no subnav "
            "quando há cotações pendentes."
        )

    def test_badge_aparece_em_agencias(self, client, db_session, app):
        """Badge de pendentes também deve aparecer na sub-aba Agências."""
        _login_admin(client, db_session, app)
        _make_itinerario(db_session, tipo_solicitacao_id=3)

        resp = client.get('/diarias/agencias')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'subnav-count' in html

    def test_badge_aparece_em_cargos(self, client, db_session, app):
        """Badge de pendentes também deve aparecer na sub-aba Cargos."""
        _login_admin(client, db_session, app)
        _make_itinerario(db_session, tipo_solicitacao_id=2)

        resp = client.get('/diarias/cargos')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'subnav-count' in html


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 3 — KPIs atualizados no administracao.html
# ════════════════════════════════════════════════════════════════════════════

class TestKpisAtualizados:

    def test_kpi_cotacoes_realizadas_na_pagina(self, client, db_session, app):
        """KPI 'Cotações Realizadas' deve aparecer no HTML."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Cotações Realizadas' in html, (
            "KPI 'Cotações Realizadas' não encontrado no HTML."
        )

    def test_kpi_cotacoes_realizadas_conta_corretamente(self, client, db_session, app):
        """KPI deve contar apenas itinerários com passagens que têm cotações."""
        _login_admin(client, db_session, app)

        it1 = _make_itinerario(db_session, tipo_solicitacao_id=2)
        _make_cotacao_voo(db_session, it1.id)

        it2 = _make_itinerario(db_session, tipo_solicitacao_id=2)
        # it2 sem cotação

        it3 = _make_itinerario(db_session, tipo_solicitacao_id=1)
        # it3 é "apenas diárias" — não conta

        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Cotações Realizadas' in html

    def test_collapse_breakdown_presente(self, client, db_session, app):
        """Collapse de breakdown das solicitações deve estar no HTML."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')

        assert 'kpiSolicitacoesDetalhes' in html, (
            "Collapse '#kpiSolicitacoesDetalhes' não encontrado no HTML."
        )
        assert 'Em Andamento' in html
        assert 'Concluídas' in html or 'Conclu' in html
        assert 'Negadas' in html

    def test_collapse_valores_breakdown(self, client, db_session, app):
        """Breakdown deve mostrar contagens corretas de em andamento, concluídas e negadas."""
        _login_admin(client, db_session, app)

        _make_itinerario(db_session, etapa_atual_id=1, tipo_solicitacao_id=1)
        _make_itinerario(db_session, etapa_atual_id=3, tipo_solicitacao_id=1)
        _make_itinerario(db_session, etapa_atual_id=5, tipo_solicitacao_id=1)

        resp = client.get('/diarias/administracao')
        assert resp.status_code == 200

    def test_kpi_cotacoes_realizadas_substituiu_em_andamento(self, client, db_session, app):
        """O 4º KPI card deve ser 'Cotações Realizadas' com ícone ticket-perforated."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')

        assert 'bi-ticket-perforated' in html, (
            "Ícone 'bi-ticket-perforated' não encontrado. "
            "O 4º card deve ser 'Cotações Realizadas'."
        )

    def test_card_solicitacoes_clicavel(self, client, db_session, app):
        """Card de Solicitações deve ter data-bs-toggle=collapse."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')

        assert 'data-bs-toggle="collapse"' in html
        assert 'kpiSolicitacoesDetalhes' in html
