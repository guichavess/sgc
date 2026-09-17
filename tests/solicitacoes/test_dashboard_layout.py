"""
Rebranding da tela Solicitações (/solicitacoes/dashboard) — pontos tirados do
mockup Stitch "Workspace Editorial" (docs/ux/mockups/stitch/pagamentos/1-dashboard-editorial.*):

1. Assinatura verde/amarelo/azul sob o título
2. Bolinha de status da atualização geral (verde = em dia, vermelha = falhou)
3. Chips de filtro com respiro até a seta e estado "ativo" (nome + contador)
4. Contadores dos itens de filtro com largura fixa
5. Tags da tabela no estilo suave do mockup (fundo tingido + borda)
6. Linha com barra de destaque à esquerda no hover
7. Botões do cabeçalho com animação de preenchimento (motion button)
"""
import re
from datetime import datetime

import pytest

PERM_CRIAR = [('solicitacoes', '', 'criar')]


@pytest.fixture()
def dados(db_session):
    from app.models import Solicitacao, Contrato, Etapa, TipoPagamento, StatusEmpenho

    db_session.add(Etapa(id=1, nome='Criada', alias='criada', ordem=1, cor_hex='#64748B'))
    db_session.add(Etapa(id=2, nome='Em análise', alias='analise', ordem=2, cor_hex='#2563EB'))
    db_session.add(TipoPagamento(id=1, nome='Regular'))
    db_session.add(TipoPagamento(id=2, nome='DEA: Indenizatório'))
    db_session.add(StatusEmpenho(id=1, nome='Empenho solicitado', cor_badge='warning'))
    db_session.add(Contrato(codigo='22026000123', nomeContratado='LIDERANCA LIMPEZA LTDA'))
    db_session.add(Contrato(codigo='22026000188', nomeContratado='EQUATORIAL PIAUI'))
    db_session.add(Solicitacao(
        id=1, codigo_contrato='22026000123', id_usuario_solicitante=1, etapa_atual_id=1,
        competencia='08/2026', id_tipo_pagamento=1, data_solicitacao=datetime(2026, 8, 10),
        protocolo_gerado_sei='00002.004512/2026-31',
    ))
    db_session.add(Solicitacao(
        id=2, codigo_contrato='22026000188', id_usuario_solicitante=1, etapa_atual_id=2,
        competencia='08/2026', id_tipo_pagamento=2, data_solicitacao=datetime(2026, 8, 12),
        protocolo_gerado_sei='00002.003984/2026-12', status_empenho_id=1,
    ))
    db_session.commit()


@pytest.fixture()
def cliente(dados, usuario_com_permissoes, logar):
    return logar(usuario_com_permissoes(7201, permissoes=PERM_CRIAR))


@pytest.fixture()
def cliente_admin(dados, usuario_com_permissoes, logar):
    return logar(usuario_com_permissoes(7202, is_admin=True))


def _html(cliente, url='/solicitacoes/dashboard'):
    resp = cliente.get(url)
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def _tbody(html):
    return html.split('<tbody>', 1)[1].split('</tbody>', 1)[0]


def test_dashboard_exige_login(client, dados):
    resp = client.get('/solicitacoes/dashboard')
    assert resp.status_code in (302, 401)


def test_carrega_css_da_tela(cliente):
    assert 'css/components/solicitacoes-dashboard.css' in _html(cliente)


# 1 ─ título e assinatura
def test_titulo_da_pagina(cliente):
    assert '<h3 class="sol-title">Solicitações de Pagamentos</h3>' in _html(cliente)


def test_assinatura_sob_o_titulo(cliente):
    html = _html(cliente)
    assert re.search(
        r'class="sol-signature"[^>]*>\s*<span></span>\s*<span></span>\s*<span></span>', html
    )


# 2 ─ bolinha de status da atualização geral
def test_status_sync_verde_quando_em_dia(cliente):
    from app.services import sincronizacao_pagamentos_service as svc
    svc.registrar_log(origem='manual', status='sucesso')

    html = _html(cliente)
    assert 'sol-sync-dot is-ok' in html
    assert 'sol-sync-dot is-erro' not in html


def test_status_sync_vermelho_quando_falhou(cliente):
    from app.services import sincronizacao_pagamentos_service as svc
    svc.registrar_log(origem='agendada', status='erro')

    html = _html(cliente)
    assert 'sol-sync-dot is-erro' in html
    assert 'falhou' in html


def test_status_sync_vermelho_quando_terminou_com_erros(cliente):
    from app.services import sincronizacao_pagamentos_service as svc
    svc.registrar_log(origem='agendada', status='sucesso')
    svc.registrar_log(origem='agendada', status='parcial', erros=3)

    html = _html(cliente)
    assert 'sol-sync-dot is-erro' in html
    assert 'sol-sync-dot is-ok' not in html
    assert '3 erro(s)' in html


def test_status_sync_volta_ao_verde_depois_de_sucesso(cliente):
    from app.services import sincronizacao_pagamentos_service as svc
    svc.registrar_log(origem='agendada', status='erro')
    svc.registrar_log(origem='manual', status='sucesso')

    html = _html(cliente)
    assert 'sol-sync-dot is-ok' in html
    assert 'sol-sync-dot is-erro' not in html


def test_status_sync_neutro_sem_log(cliente):
    html = _html(cliente)
    assert 'sol-sync-dot is-ok' not in html
    assert 'sol-sync-dot is-erro' not in html


# 3 ─ chips de filtro
def test_chips_de_filtro_inativos_sem_selecao(cliente):
    html = _html(cliente)
    chips = re.findall(r'<button class="btn sol-filtro-chip dropdown-toggle([^"]*)"', html)
    assert len(chips) == 4
    assert all('is-ativo' not in c for c in chips)


def test_chip_etapa_ativo_mantem_nome_e_mostra_contador(cliente):
    html = _html(cliente, '/solicitacoes/dashboard?filtro_etapa=1&filtro_etapa=2')
    chip = re.search(r'<button class="btn sol-filtro-chip dropdown-toggle is-ativo"[^>]*id="dropdownEtapas".*?</button>', html, re.S)
    assert chip, 'chip de Etapa deveria vir ativo do servidor'
    assert '<span id="labelEtapas">Etapa</span>' in chip.group(0)
    assert re.search(r'id="badgeEtapas"[^>]*>2<', chip.group(0))
    assert 'Selecionada' not in html


# 4 ─ contadores de largura fixa
def test_contadores_dos_itens_de_filtro(cliente):
    html = _html(cliente)
    # 1 competência + 2 etapas + 2 tipos + 1 status de empenho
    assert html.count('class="sol-filtro-contagem"') == 6
    assert 'badge bg-light text-muted border ms-2' not in html


# 5 ─ tags da tabela
def test_etapa_como_tag_suave_na_cor_da_etapa(cliente):
    tbody = _tbody(_html(cliente))
    assert '<span class="sol-tag" style="--tag-cor: #64748B">Criada</span>' in tbody
    assert '<span class="sol-tag" style="--tag-cor: #2563EB">Em análise</span>' in tbody
    assert not re.search(r'<span class="badge[^"]*"[^>]*>\s*(Criada|Em análise)', tbody)


def test_tempo_total_mantem_estilo_anterior_em_pilula(cliente):
    tbody = _tbody(_html(cliente))
    # em andamento → pílula azul original com relógio
    assert tbody.count('<span class="badge rounded-pill border border-primary text-primary bg-light px-3 py-2"') == 2
    assert 'bi bi-clock me-1' in tbody
    assert 'sol-tempo' not in tbody


def test_contrato_em_mono(cliente):
    tbody = _tbody(_html(cliente))
    assert '<span class="sol-mono">22026000123</span>' in tbody


def test_tipo_pagamento_como_tag_igual_ao_status_atual(cliente):
    tbody = _tbody(_html(cliente))
    # Regular → neutro · DEA (inclusive "DEA: Indenizatório") → vermelho
    assert '<span class="sol-tag sol-tag-tipo" style="--tag-cor: #64748B">Regular</span>' in tbody
    assert '<span class="sol-tag sol-tag-tipo" style="--tag-cor: #C5221F">DEA: Indenizatório</span>' in tbody
    assert 'class="sol-tipo' not in tbody


def test_status_empenho_como_tag_semantica(cliente):
    tbody = _tbody(_html(cliente))
    assert '<span class="sol-tag" style="--tag-cor: #D97706">Empenho solicitado</span>' in tbody


# 6 ─ hover da linha
def test_linhas_com_classe_de_destaque(cliente):
    tbody = _tbody(_html(cliente))
    assert tbody.count('<tr class="sol-row"') == 2


# 7 ─ botões animados do cabeçalho
def test_botoes_do_cabecalho_animados(cliente_admin):
    html = _html(cliente_admin)
    assert re.search(r'id="btnSyncUnificado"[^>]*class="sgc-mbtn sgc-mbtn--secondary"', html)
    assert re.search(r'class="sgc-mbtn sgc-mbtn--secondary"[^>]*href="/solicitacoes/vincular', html) or \
        re.search(r'href="/solicitacoes/vincular[^"]*"[^>]*class="sgc-mbtn sgc-mbtn--secondary"', html)
    assert re.search(r'href="/solicitacoes/nova[^"]*"[^>]*class="sgc-mbtn sgc-mbtn--novo"', html)
    cabecalho = html.split('<div class="sol-head-acoes">', 1)[1].split('</header>', 1)[0]
    assert cabecalho.count('class="sgc-mbtn__fill"') == 3


def test_icones_dos_botoes_com_animacao_propria(cliente_admin):
    html = _html(cliente_admin)
    assert 'class="bi bi-arrow-repeat sgc-mbtn__icon sgc-mbtn__icon--girar"' in html
    assert 'class="bi bi-link-45deg sgc-mbtn__icon sgc-mbtn__icon--conectar"' in html
    assert 'class="bi bi-plus-lg sgc-mbtn__icon sgc-mbtn__icon--somar"' in html


def _css_dashboard():
    from pathlib import Path
    return (Path(__file__).resolve().parents[2] / 'app/static/css/components/solicitacoes-dashboard.css').read_text(encoding='utf-8')


def _css_ui():
    """Componentes globais (sol-head, sol-signature, sgc-mbtn, sol-tag) — importados por main.css."""
    from pathlib import Path
    return (Path(__file__).resolve().parents[2] / 'app/static/css/components/sgc-ui.css').read_text(encoding='utf-8')


def test_css_animacoes_dos_icones_e_botao_verde():
    css = _css_ui()
    assert re.search(r'\.sgc-mbtn__icon--girar \{[^}]*rotate\(360deg\)', css)
    assert re.search(r'\.sgc-mbtn__icon--conectar \{[^}]*rotate\(45deg\)', css)
    assert re.search(r'\.sgc-mbtn__icon--somar \{[^}]*rotate\(90deg\)', css)
    regra_verde = re.search(r'\.sgc-mbtn--sucesso \{(.*?)\}', css, re.S).group(1)
    assert '--pi-green' in regra_verde
    # "Nova solicitação": botão branco, só o círculo do ícone verde; o verde toma o botão no hover
    regra_novo = re.search(r'\.sgc-mbtn--novo \{(.*?)\}', css, re.S).group(1)
    assert '--mbtn-fill: var(--pi-green)' in regra_novo
    assert '--mbtn-bg' not in regra_novo and '--mbtn-label:' not in regra_novo  # fundo branco e texto escuro do padrão
    assert 'rgba(0, 132, 61' in regra_novo
    # reduced motion desliga os giros
    reduzido = css.split('@media (prefers-reduced-motion: reduce)', 1)[1]
    assert '--mbtn-icon-giro: none' in reduzido


def test_botoes_e_status_no_bloco_alinhado_a_direita(cliente_admin):
    html = _html(cliente_admin)
    bloco = html.split('<div class="sol-head-acoes">', 1)[1].split('</header>', 1)[0]
    assert 'class="sol-sync-status"' in bloco
    assert 'id="btnSyncUnificado"' in bloco


def test_css_botao_em_pilula_e_acoes_a_direita():
    css = _css_ui()
    regra_btn = re.search(r'\.sgc-mbtn \{(.*?)\}', css, re.S).group(1)
    assert 'border-radius: 999px' in regra_btn
    regra_fill = re.search(r'\.sgc-mbtn__fill \{(.*?)\}', css, re.S).group(1)
    assert 'border-radius: 999px' in regra_fill
    regra_acoes = re.search(r'\.sol-head-acoes \{(.*?)\}', css, re.S).group(1)
    assert 'margin-left: auto' in regra_acoes
    assert 'align-items: flex-start' not in css
    assert 'align-items: flex-start' not in _css_dashboard()


def test_contratos_de_js_preservados(cliente_admin):
    html = _html(cliente_admin)
    for marcador in ('onclick="iniciarSincronizacaoUnificada()"', 'id="modalSyncSei"',
                     'id="modalConfirmacao"', 'money-mask', 'function salvarEmpenho'):
        assert marcador in html


def test_css_hover_da_linha_mais_escuro():
    css = _css_dashboard()
    cor = re.search(r'--sol-row-hover:\s*(#[0-9a-fA-F]{6})', _css_ui()).group(1)
    r, g, b = (int(cor[i:i + 2], 16) for i in (1, 3, 5))
    luminancia = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255
    # antes #fafbfc (~0.98): quase invisível sobre o branco
    assert luminancia < 0.95
    assert '.sol-tabela tbody tr.sol-row:hover > td { background-color: var(--sol-row-hover)' in css


# ─ filtros de data: date picker (botão + calendário em popover)
def _periodo(html):
    return html.split('id="formFiltros"', 1)[1].split('<!-- Linha 2', 1)[0]


def test_filtros_de_data_viram_date_picker(cliente):
    html = _html(cliente)
    periodo = _periodo(html)
    assert periodo.count('data-sgc-datepicker') == 2
    assert 'type="date"' not in periodo
    assert '<input type="hidden" name="data_inicio" value="">' in periodo
    assert '<input type="hidden" name="data_fim" value="">' in periodo
    assert periodo.count('class="sgc-datepicker__trigger"') == 2
    assert periodo.count('Selecionar data') == 2
    assert 'aria-haspopup="dialog"' in periodo
    assert 'data-min-input="data_inicio"' in periodo
    assert 'data-max-input="data_fim"' in periodo
    assert 'css/components/sgc-datepicker.css' in html
    assert 'js/sgc-datepicker.js' in html


def test_date_picker_mostra_data_escolhida_em_formato_brasileiro(cliente):
    periodo = _periodo(_html(cliente, '/solicitacoes/dashboard?data_inicio=2026-02-01&data_fim=2026-02-28'))
    assert '<input type="hidden" name="data_inicio" value="2026-02-01">' in periodo
    assert '<input type="hidden" name="data_fim" value="2026-02-28">' in periodo
    assert '01/02/2026' in periodo and '28/02/2026' in periodo
    assert 'Selecionar data' not in periodo
    assert periodo.count('sgc-datepicker__trigger is-preenchido') == 0  # classe vai no wrapper
    assert periodo.count('sgc-datepicker is-preenchido') == 2


def test_date_picker_ignora_data_invalida(cliente):
    periodo = _periodo(_html(cliente, '/solicitacoes/dashboard?data_inicio=lixo'))
    assert '<input type="hidden" name="data_inicio" value="">' in periodo
    assert periodo.count('Selecionar data') == 2


def test_date_picker_js_e_css_locais_sem_react():
    from pathlib import Path
    raiz = Path(__file__).resolve().parents[2] / 'app/static'
    js = (raiz / 'js/sgc-datepicker.js').read_text(encoding='utf-8')
    css = (raiz / 'css/components/sgc-datepicker.css').read_text(encoding='utf-8')
    assert 'http' not in js and 'import ' not in js
    for trecho in ('role="grid"', 'aria-selected', 'Escape', 'ArrowLeft', 'PageUp', "'pt-BR'"):
        assert trecho in js
    for classe in ('.sgc-datepicker__popover', '.sgc-datepicker__dia', '.is-selecionado', '.is-hoje', 'prefers-reduced-motion'):
        assert classe in css


def test_css_tags_da_tabela_em_pilula():
    regra = re.search(r'\n\.sol-tag \{(.*?)\}', _css_ui(), re.S).group(1)
    assert 'border-radius: 999px' in regra
    assert 'padding: 1px 10px' in regra
