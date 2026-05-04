"""
Guard de Pré-requisitos para Cotações de Voos
==============================================
Testa a lógica de template que bloqueia "Gerenciar Cotações e Escolha de Voo"
enquanto a Análise de Habilitação (CCDP) não estiver concluída.

Usa renderização direta de template (render_template em test_request_context)
para testar a lógica Jinja2 sem depender de autenticação HTTP ou banco de dados.

Como rodar:
    pytest tests/diarias/test_cotacoes_prereq.py -v
"""
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock


# ── Mock objects ──────────────────────────────────────────────────────────────

class _DocMock:
    """Simula DiariasDocumentoSei."""
    def __init__(self, sei_id=None, sei_formatado=None, codigo=None, assinado=False):
        self.sei_id = sei_id
        self.sei_formatado = sei_formatado
        self.codigo = codigo
        self.assinado = assinado


class _QuadroMock:
    """Simula DiariasQuadroOrcamentario."""
    def __init__(self, ug=None, natureza_despesa=None):
        self.ug = ug
        self.natureza_despesa = natureza_despesa


class _ItinerarioMock:
    """
    Simula DiariasItinerario com os atributos/métodos mínimos para renderizar
    fluxo_stepper.html e administracao_detalhe.html.
    """
    def __init__(
        self,
        itin_id=1,
        tipo_solicitacao_id=2,   # 2 = Diárias+Passagens
        tipo_itinerario=2,        # 2 = Nacional
        etapa_atual_id=3,         # 3 = ANALISE_SOLICITACAO
        sei_protocolo=None,
        superintendente_assinou=False,
        escolha_voo_ida_id=None,
        escolha_sei_opcoes=None,
        docs=None,
        quadro=None,
    ):
        self.id = itin_id
        self.tipo_solicitacao_id = tipo_solicitacao_id
        self.tipo_itinerario = tipo_itinerario
        self.etapa_atual_id = etapa_atual_id
        self.sei_protocolo = sei_protocolo
        self.superintendente_assinou = superintendente_assinou
        self.escolha_voo_ida_id = escolha_voo_ida_id
        self.escolha_sei_opcoes = escolha_sei_opcoes
        self._docs = docs or {}
        self.quadro_orcamentario = quadro
        # Atributos usados por outros trechos do template
        self.usuario_gerador = 'teste'
        self.objetivo = 'Teste'
        self.n_processo = '00002.000001/2026-00'
        self.sei_id_procedimento = None
        self.valor_total = None
        self.data_viagem = None
        self.data_retorno = None
        self.qtd_diarias_solicitadas = 3
        self.ciencia_nci = False
        self.ciencia_apoio = False
        self.ciencia_diretor = False
        self.ciencia_geo = False
        self.analise_pagamento_respostas = None
        self.analise_pagamento_observacoes = None

        # Relacionamentos usados pelo template
        self.tipo_solicitacao = SimpleNamespace(nome='Diárias + Passagens Aéreas')
        self.etapa_atual = SimpleNamespace(nome='Análise 1ª Parte', id=etapa_atual_id)
        self.status = SimpleNamespace(nome='Ativa')
        self.tipo = SimpleNamespace(nome='Nacional')
        self.itens = []
        self.paradas = []
        self.justificativa = None

    def get_doc(self, tipo):
        return self._docs.get(tipo)

    def has_doc(self, tipo):
        doc = self.get_doc(tipo)
        return doc is not None and (doc.sei_id is not None or doc.codigo is not None)

    def set_doc(self, tipo, **kwargs):
        pass


def _itin_gpo_ok_ccdp_nao(itin_id=1):
    """GPO concluído (NR + Quadro), sem Análise de Habilitação."""
    return _ItinerarioMock(
        itin_id=itin_id,
        etapa_atual_id=3,
        docs={
            'nota_reserva': _DocMock(codigo='NR2026001'),
        },
        quadro=_QuadroMock(ug='210101', natureza_despesa='339014'),
    )


def _itin_ambos_ok(itin_id=2):
    """GPO + CCDP concluídos, etapa 2 (ESCOLHA_VOO)."""
    return _ItinerarioMock(
        itin_id=itin_id,
        etapa_atual_id=2,
        sei_protocolo='00002.000001/2026-00',
        docs={
            'nota_reserva': _DocMock(codigo='NR2026002'),
            'analise_habilitacao': _DocMock(
                sei_id='DOC-HAB-001',
                sei_formatado='0001234-01.2026.8.11.0023',
            ),
        },
        quadro=_QuadroMock(ug='210101', natureza_despesa='339014'),
    )


def _make_mock_user():
    """Cria mock de current_user para templates que usam current_user.*"""
    user = MagicMock()
    user.is_authenticated = True
    user.is_anonymous = False
    user.is_active = True
    user.is_admin = True
    user.is_secretario = False
    user.is_superintendente = False
    user.nome = 'ADMIN TESTE'
    user.sigla_login = 'admin_teste'
    user.tem_permissao.return_value = True
    return user


def _render_stepper(app, itin, cotacoes_voos=None):
    """Renderiza o partial fluxo_stepper.html com o itinerário mock."""
    with app.test_request_context('/diarias/administracao/1?aba=fluxo'):
        from flask import render_template
        return render_template(
            'diarias/partials/fluxo_stepper.html',
            itinerario=itin,
            cotacoes_voos=cotacoes_voos or [],
            agencias=[],
            cargos=[],
            naturezas=[],
            pode_editar=True,
            timeline_data={},
        )


def _render_detalhe(app, itin, aba='passagens', cotacoes_voos=None):
    """Renderiza administracao_detalhe.html com o itinerário mock e current_user mockado."""
    with app.test_request_context(f'/diarias/administracao/{itin.id}?aba={aba}'):
        from flask import g, render_template
        # Flask-Login 0.6.x armazena o usuário logado em g._login_user
        g._login_user = _make_mock_user()
        return render_template(
            'diarias/administracao_detalhe.html',
            itinerario=itin,
            itens=[],
            paradas=[],
            cotacoes=[],
            cotacoes_voos=cotacoes_voos or [],
            timeline_data={},
            agencias=[],
            aba=aba,
            cargos=[],
            naturezas=[],
            etapas=[],
            tipos_solicitacao=[],
            tipos_itinerario=[],
            estados=[],
            pode_editar=True,
            pode_aprovar=True,
        )


# ── Testes do fluxo_stepper.html ──────────────────────────────────────────────

class TestStepperGuardCCDP:
    """Stepper deve bloquear cotações enquanto habilitação estiver pendente."""

    def test_stepper_exibe_lock_quando_ccdp_pendente(self, app):
        """
        RED antes do fix: stepper mostra botão "Gerenciar Cotações" sem
        verificar ccdp_ok, ignorando que a habilitação está pendente.

        APÓS correção: deve aparecer mensagem de lock específica para CCDP.
        """
        itin = _itin_gpo_ok_ccdp_nao()
        html = _render_stepper(app, itin)

        assert 'Aguardando CCDP' in html, (
            "GUARD AUSENTE: com GPO ok mas habilitação pendente, o stepper deveria "
            "exibir 'Aguardando CCDP'. "
            "Fix: adicionar '{% elif not ccdp_ok %}' no painel de Cotações "
            "em fluxo_stepper.html (após o bloco '{% if not analise1_ok %}')."
        )

    def test_stepper_nao_mostra_botao_gerenciar_quando_ccdp_pendente(self, app):
        """Botão de link para cotações NÃO deve aparecer quando ccdp_ok=False."""
        itin = _itin_gpo_ok_ccdp_nao()
        html = _render_stepper(app, itin)

        assert 'Gerenciar Cotações e Escolha de Voo' not in html, (
            "BOTÃO INDEVIDO: botão 'Gerenciar Cotações' não deve aparecer quando "
            "a Análise de Habilitação estiver pendente. "
            "Fix: a condição do botão deve incluir ccdp_ok em fluxo_stepper.html."
        )

    def test_stepper_mostra_botao_quando_ambos_ok(self, app):
        """Com GPO + CCDP concluídos, botão de cotações deve aparecer."""
        itin = _itin_ambos_ok()
        html = _render_stepper(app, itin)

        assert 'Gerenciar Cotações e Escolha de Voo' in html, (
            "Botão 'Gerenciar Cotações' deve aparecer quando GPO e CCDP estiverem ok."
        )
        assert 'Aguardando CCDP' not in html, (
            "Mensagem de lock não deve aparecer quando habilitação está concluída."
        )

    def test_stepper_step2_nao_marcado_done_quando_ccdp_pendente(self, app):
        """
        Indicador do passo 'Análise 1ª Parte' na barra de steps NÃO deve ter
        classe fs-done quando ccdp_ok=False.

        RED: antes do fix, analise1_ok ignora ccdp_ok → step 2 mostra como done.
        APÓS correção: 'done': analise1_ok and ccdp_ok → step 2 deixa de ser done.
        """
        itin = _itin_gpo_ok_ccdp_nao()
        html = _render_stepper(app, itin)

        # A CSS do template contém '.fs-done' como seletor, então verificamos pelo
        # padrão de classe no elemento HTML (espaço antes, não ponto do CSS).
        assert 'fluxo-step-item fs-done' not in html, (
            "STEPPER INCORRETO: nenhum step deve ter classe 'fs-done' quando ccdp_ok=False. "
            "Antes do fix 'analise1_ok' ignora ccdp_ok, marcando o step 2 como done incorretamente. "
            "Fix: alterar 'done': analise1_ok → 'done': analise1_ok and ccdp_ok em fluxo_stepper.html"
        )


# ── Testes da aba passagens (administracao_detalhe.html) ──────────────────────

class TestAbaPassagensGuard:
    """Aba 'passagens' deve exibir card de bloqueio quando pré-requisitos ausentes."""

    def test_aba_passagens_exibe_prereq_quando_habilitacao_pendente(self, app):
        """
        RED antes do fix: aba mostra seção vazia sem mensagem de orientação.

        APÓS correção: deve aparecer card "Pré-requisitos não atendidos".
        """
        itin = _itin_gpo_ok_ccdp_nao()
        html = _render_detalhe(app, itin, aba='passagens')

        assert 'Pré-requisitos não atendidos' in html, (
            "GUARD AUSENTE: sem habilitação concluída, a aba 'passagens' deve exibir "
            "'Pré-requisitos não atendidos'. "
            "Fix: adicionar bloco de guard no início da aba passagens em "
            "administracao_detalhe.html (logo após o botão 'Voltar ao Fluxo')."
        )

    def test_aba_passagens_sem_prereq_nao_mostra_form(self, app):
        """Formulário de cotação NÃO deve aparecer quando pré-requisitos não atendidos."""
        itin = _itin_gpo_ok_ccdp_nao()
        html = _render_detalhe(app, itin, aba='passagens')

        assert 'Cadastrar Opção de Voo' not in html, (
            "Formulário 'Cadastrar Opção de Voo' não deve aparecer quando "
            "a Análise de Habilitação está pendente."
        )

    def test_aba_passagens_mostra_form_quando_prereqs_ok(self, app):
        """Com pré-requisitos atendidos e etapa 2, formulário de cotação deve aparecer."""
        itin = _itin_ambos_ok()
        html = _render_detalhe(app, itin, aba='passagens')

        assert 'Cadastrar Opção de Voo' in html, (
            "Formulário 'Cadastrar Opção de Voo' deve aparecer quando "
            "habilitação está concluída e etapa_atual_id == 2 (ESCOLHA_VOO)."
        )
        assert 'Pré-requisitos não atendidos' not in html, (
            "Mensagem de bloqueio não deve aparecer quando pré-requisitos estão ok."
        )

    def test_aba_passagens_sem_prereq_lista_ccdp_como_pendente(self, app):
        """O card de bloqueio deve mencionar especificamente a Habilitação pendente."""
        itin = _itin_gpo_ok_ccdp_nao()
        html = _render_detalhe(app, itin, aba='passagens')

        # CCDP está pendente → deve aparecer na lista de itens faltantes
        assert 'Análise de Habilitação' in html, (
            "O card de pré-requisitos deve listar 'Análise de Habilitação' como pendente."
        )
