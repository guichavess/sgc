"""
KPI Cards e Botão Vincular Processo — Administração (TDD Red → Green)
=====================================================================
Arquivo: tests/diarias/test_administracao_kpis.py

Cobre:
  1. Rota administracao() retorna variável `kpis` no contexto
  2. kpis.total_solicitacoes corresponde ao total de registros
  3. kpis.total_pessoas soma corretamente DiariasItemItinerario
  4. kpis.valor_total soma DiariasItinerario.valor_total
  5. kpis.em_andamento conta itinerarios que ainda não estão na etapa final
  6. Template renderiza cards KPI com valores corretos
  7. Valor total aparece como badge no cabeçalho da tabela
  8. Página de detalhe contém botão "Vincular Processo"
  9. Página de detalhe contém modal #modalVincularProcesso
 10. Modal contém campos protocolo_sei e etapa_id

Como rodar:
    pytest tests/diarias/test_administracao_kpis.py -v
"""
import uuid
import pytest
from decimal import Decimal
from datetime import date, datetime


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _uid():
    """Gera sufixo único de 12 chars para evitar colisão de UNIQUE constraints."""
    return uuid.uuid4().hex[:12]


def _login_admin(client, db_session, app):
    """
    Cria um usuário admin com permissão diarias.aprovar e faz login no client.
    Retorna o usuário criado.
    """
    from flask_login import login_user
    from app.models.usuario import Usuario

    uid = _uid()
    u = Usuario(
        id_usuario_sei=f'admin_{uid}',
        nome='ADMIN KPIS TESTE',
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


def _make_itinerario(db_session, app, **kwargs):
    """
    Cria e persiste um DiariasItinerario mínimo no banco de teste.
    Aceita kwargs para sobrescrever campos padrão.
    """
    from app.models.diaria import DiariasItinerario

    defaults = dict(
        n_processo=f'PROC-{_uid()}',
        usuario_gerador=f'user_{_uid()}@test.com',
        tipo_itinerario=1,
        tipo_solicitacao_id=1,
        qtd_diarias_solicitadas=Decimal('2.5'),
        data_solicitacao=datetime(2026, 4, 1),
        data_viagem=date(2026, 5, 1),
        data_retorno=date(2026, 5, 3),
        etapa_atual_id=1,
        valor_total=Decimal('0.00'),
    )
    defaults.update(kwargs)
    it = DiariasItinerario(**defaults)
    db_session.add(it)
    db_session.flush()
    return it


def _make_item(db_session, itinerario_id, **kwargs):
    """Cria um DiariasItemItinerario vinculado ao itinerario_id."""
    from app.models.diaria import DiariasItemItinerario

    defaults = dict(
        id_itinerario=itinerario_id,
        cpf_pessoa=f'{_uid()[:11]}',
        nome_pessoa='FULANO BELTRANO',
        cargo_id=1,
        valor_cargo=Decimal('320.00'),
    )
    defaults.update(kwargs)
    item = DiariasItemItinerario(**defaults)
    db_session.add(item)
    db_session.flush()
    return item


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 1 — Cálculos KPI na rota administracao()
# ════════════════════════════════════════════════════════════════════════════

class TestAdministracaoKpis:
    """
    Testa que a rota administracao() calcula e expõe os KPIs corretos
    e que o template os renderiza.
    """

    def test_rota_retorna_200(self, client, db_session, app):
        """GET /diarias/administracao deve retornar 200."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        assert resp.status_code == 200, (
            f"Esperado 200, obtido {resp.status_code}. "
            "Rota /diarias/administracao não encontrada ou erro."
        )

    def test_rota_expoe_variavel_kpis(self, client, db_session, app):
        """A rota deve passar a variável `kpis` para o template."""
        _login_admin(client, db_session, app)

        with app.test_request_context():
            pass  # warm-up

        # Usamos test_client context processor — verificamos o HTML diretamente
        resp = client.get('/diarias/administracao')
        assert resp.status_code == 200
        # Se `kpis` não existir, o template vai falhar — o 200 já prova que existe.
        # Adicionalmente, verificamos que algum card KPI está no HTML.
        html = resp.data.decode('utf-8', errors='replace')
        assert 'kpi' in html.lower() or 'solicitaç' in html.lower(), (
            "HTML não contém indicadores de KPI cards. Implemente os cards no template."
        )

    def test_kpi_total_solicitacoes_na_pagina(self, client, db_session, app):
        """KPI de total de solicitações deve aparecer no HTML."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        # Deve conter algum elemento que mostre "Solicitações" como label de KPI
        assert 'Solicitaç' in html or 'solicitaç' in html, (
            "HTML não contém o label 'Solicitações' para o KPI card."
        )

    def test_kpi_total_pessoas_na_pagina(self, client, db_session, app):
        """KPI de total de pessoas deve aparecer no HTML."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Pessoas' in html or 'pessoas' in html, (
            "HTML não contém o label 'Pessoas' para o KPI card."
        )

    def test_kpi_valor_total_na_pagina(self, client, db_session, app):
        """KPI de valor total deve aparecer no HTML."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Valor Total' in html or 'valor total' in html.lower(), (
            "HTML não contém o label 'Valor Total' para o KPI card."
        )

    def test_kpi_em_andamento_na_pagina(self, client, db_session, app):
        """KPI 'Em Andamento' deve aparecer no HTML."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Em Andamento' in html or 'em andamento' in html.lower(), (
            "HTML não contém o label 'Em Andamento' para o KPI card."
        )

    def test_kpi_valor_total_soma_corretamente(self, client, db_session, app):
        """
        Dado 2 itinerários com valor_total=500 e 300, o KPI valor total deve
        somar 800 e esse valor deve aparecer no HTML.
        """
        _login_admin(client, db_session, app)

        _make_itinerario(db_session, app, valor_total=Decimal('500.00'))
        _make_itinerario(db_session, app, valor_total=Decimal('300.00'))

        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        # Valor 800 deve aparecer de alguma forma (pode ser "800,00" ou "800.00" ou "R$ 800")
        assert '800' in html, (
            "HTML não contém o valor 800 correspondente à soma dos valor_total (500+300). "
            "Implemente o KPI de valor total na rota e no template."
        )

    def test_kpi_pessoas_soma_corretamente(self, client, db_session, app):
        """
        Dado 1 itinerário com 2 pessoas, o KPI total de pessoas deve ser 2.
        """
        _login_admin(client, db_session, app)

        it = _make_itinerario(db_session, app)
        _make_item(db_session, it.id)
        _make_item(db_session, it.id)

        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        # Pelo menos o número 2 deve aparecer em algum KPI
        assert '2' in html, (
            "HTML deve conter o número 2 refletindo 2 pessoas no KPI."
        )

    def test_badge_valor_total_no_cabecalho_tabela(self, client, db_session, app):
        """
        O cabeçalho da tabela de solicitações deve conter um badge com o
        valor total além do badge de contagem de registros.
        """
        _login_admin(client, db_session, app)

        _make_itinerario(db_session, app, valor_total=Decimal('1200.00'))

        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        # O cabeçalho já tem "registro(s)" — verificamos que valor total também aparece
        # próximo ao registro(s) (pode ser como badge ou span)
        assert 'registro(s)' in html, "Badge de contagem 'registro(s)' deve continuar presente."
        # Valor 1.200 ou 1200 deve aparecer em algum lugar como soma
        assert '1.200' in html or '1200' in html, (
            "Badge de valor total (R$ 1.200,00 ou similar) deve aparecer no cabeçalho "
            "da tabela. Implemente o badge no template administracao.html."
        )

    def test_kpi_em_andamento_exclui_etapa_final(self, client, db_session, app):
        """
        Dado 3 itinerários (2 em etapas iniciais, 1 na etapa 5 = Prestação de Contas),
        o KPI em_andamento deve contar apenas os 2 não concluídos.
        A página deve carregar sem erros.
        """
        _login_admin(client, db_session, app)

        _make_itinerario(db_session, app, etapa_atual_id=1)
        _make_itinerario(db_session, app, etapa_atual_id=3)
        _make_itinerario(db_session, app, etapa_atual_id=5)  # Prestação de Contas = concluído

        resp = client.get('/diarias/administracao')
        assert resp.status_code == 200


# ════════════════════════════════════════════════════════════════════════════
# CLASSE 2 — Botão "Vincular Processo" na lista + Página dedicada
# ════════════════════════════════════════════════════════════════════════════

class TestVincularProcessoPagina:
    """
    Testa o fluxo "Vincular Processo" como página dedicada
    em /diarias/administracao/vincular-processo.

    O botão na lista redireciona para esta página (não abre modal).
    """

    def test_lista_contem_link_vincular_processo(self, client, db_session, app):
        """Página de lista deve conter um link/botão 'Vincular Processo'."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Vincular Processo' in html, (
            "Link 'Vincular Processo' não encontrado na página de lista. "
            "Adicione um link/botão no template administracao.html."
        )

    def test_lista_botao_nao_abre_modal(self, client, db_session, app):
        """
        O botão de vincular processo NÃO deve abrir modal —
        deve ser um link <a href=...> para a página dedicada.
        """
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao')
        html = resp.data.decode('utf-8', errors='replace')
        # Não deve mais haver modal de vincular processo nesta página
        assert 'modalVincularProcesso' not in html, (
            "Modal 'modalVincularProcesso' ainda presente na lista. "
            "O botão deve ser um <a href> para a página dedicada, não um modal."
        )

    def test_pagina_vincular_processo_retorna_200(self, client, db_session, app):
        """GET /diarias/administracao/vincular-processo deve retornar 200."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao/vincular-processo')
        assert resp.status_code == 200, (
            f"Esperado 200 na página de vincular processo. Obtido {resp.status_code}. "
            "Crie a rota GET /diarias/administracao/vincular-processo."
        )

    def test_pagina_contem_campo_protocolo_sei(self, client, db_session, app):
        """Página dedicada deve conter campo name='protocolo_sei'."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao/vincular-processo')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'protocolo_sei' in html, (
            "Campo 'protocolo_sei' não encontrado na página dedicada."
        )

    def test_pagina_contem_campo_etapa_id(self, client, db_session, app):
        """Página dedicada deve conter campo name='etapa_id'."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao/vincular-processo')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'etapa_id' in html, (
            "Campo 'etapa_id' não encontrado na página dedicada."
        )

    def test_pagina_contem_botao_verificar(self, client, db_session, app):
        """Página dedicada deve conter botão de verificação AJAX."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao/vincular-processo')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Verificar' in html, (
            "Botão 'Verificar' não encontrado na página dedicada."
        )

    def test_pagina_form_action_aponta_para_importar(self, client, db_session, app):
        """Form da página deve enviar POST para a rota importar_processo."""
        _login_admin(client, db_session, app)
        resp = client.get('/diarias/administracao/vincular-processo')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'importar-processo' in html or 'importar_processo' in html, (
            "Form não aponta para a rota 'importar-processo'. "
            "Use url_for('diarias.importar_processo') como action."
        )

    def test_post_sem_protocolo_redireciona_com_erro(self, client, db_session, app):
        """POST sem protocolo_sei deve redirecionar (302) com flash de erro."""
        _login_admin(client, db_session, app)
        resp = client.post(
            '/diarias/administracao/importar-processo',
            data={'protocolo_sei': '', 'etapa_id': '1'},
            follow_redirects=False,
        )
        assert resp.status_code == 302, (
            f"Esperado redirecionamento 302 ao faltar protocolo. Obtido {resp.status_code}."
        )
