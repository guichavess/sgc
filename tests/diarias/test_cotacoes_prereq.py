"""
Testes: Passagens como Fluxo Independente
==========================================
Verifica que a cotação/escolha de passagens funciona independente das etapas
do fluxo principal (desacoplamento da etapa 2 — ESCOLHA_VOO).

Testa:
- Stepper não exibe passo "Cotações e Voo"
- Aba "Passagens" acessível independente da etapa
- Formulário de cotação visível quando escolha não foi feita

Como rodar:
    pytest tests/diarias/test_cotacoes_prereq.py -v
"""
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock


class _DocMock:
    def __init__(self, sei_id=None, sei_formatado=None, codigo=None, assinado=False):
        self.sei_id = sei_id
        self.sei_formatado = sei_formatado
        self.codigo = codigo
        self.assinado = assinado


class _QuadroMock:
    def __init__(self, ug=None, natureza_despesa=None):
        self.ug = ug
        self.natureza_despesa = natureza_despesa


class _ItinerarioMock:
    def __init__(
        self,
        itin_id=1,
        tipo_solicitacao_id=2,
        tipo_itinerario=2,
        etapa_atual_id=3,
        sei_protocolo=None,
        superintendente_assinou=False,
        escolha_voo_ida_id=None,
        escolha_sei_opcoes=None,
        escolha_voo_ida=None,
        escolha_voo_volta=None,
        escolha_menor_valor=None,
        escolha_justificativa_codigos=None,
        escolha_justificativa_outros=None,
        escolha_via_sei=False,
        docs=None,
        quadro=None,
        processo_negado=False,
        sei_id_procedimento=None,
    ):
        self.id = itin_id
        self.tipo_solicitacao_id = tipo_solicitacao_id
        self.tipo_itinerario = tipo_itinerario
        self.etapa_atual_id = etapa_atual_id
        self.sei_protocolo = sei_protocolo
        self.n_processo = sei_protocolo
        self.superintendente_assinou = superintendente_assinou
        self.escolha_voo_ida_id = escolha_voo_ida_id
        self.escolha_sei_opcoes = escolha_sei_opcoes
        self.escolha_voo_ida = escolha_voo_ida
        self.escolha_voo_volta = escolha_voo_volta
        self.escolha_menor_valor = escolha_menor_valor
        self.escolha_justificativa_codigos = escolha_justificativa_codigos
        self.escolha_justificativa_outros = escolha_justificativa_outros
        self.escolha_via_sei = escolha_via_sei
        self._docs = docs or {}
        self.quadro_orcamentario = quadro
        self.processo_negado = processo_negado
        self.sei_id_procedimento = sei_id_procedimento
        self.objetivo = 'Reunião teste'
        self.data_viagem = None
        self.data_retorno = None
        self.valor_total = 500.0
        self.usuario_gerador = 'teste'
        self.paradas = MagicMock()
        self.paradas.count.return_value = 0
        self.paradas.all.return_value = []

    def get_doc(self, tipo):
        return self._docs.get(tipo)

    def has_doc(self, tipo):
        doc = self._docs.get(tipo)
        return bool(doc and (doc.sei_id or doc.codigo))

    @property
    def etapa_atual(self):
        nomes = {1: 'Solicitação Inicial', 3: 'Análise 1ª Parte', 6: 'Análise 2ª Parte',
                 4: 'Concessão', 5: 'Prestação de Contas'}
        return SimpleNamespace(id=self.etapa_atual_id, nome=nomes.get(self.etapa_atual_id, '?'),
                               ordem=self.etapa_atual_id)


def _itin_etapa3_tipo2(itin_id=1):
    """Tipo 2 (Diárias+Passagens) na etapa 3 (Análise 1ª Parte)."""
    return _ItinerarioMock(
        itin_id=itin_id,
        tipo_solicitacao_id=2,
        etapa_atual_id=3,
        docs={
            'nota_reserva': _DocMock(codigo='NR2026001'),
            'analise_habilitacao': _DocMock(sei_id='DOC-HAB-001'),
        },
        quadro=_QuadroMock(ug='210101'),
    )


def _itin_etapa6_tipo2(itin_id=2):
    """Tipo 2 na etapa 6 (Análise 2ª Parte)."""
    return _ItinerarioMock(
        itin_id=itin_id,
        tipo_solicitacao_id=2,
        etapa_atual_id=6,
        sei_protocolo='00002.000001/2026-00',
        docs={
            'nota_reserva': _DocMock(codigo='NR2026002'),
            'analise_habilitacao': _DocMock(sei_id='DOC-HAB-001'),
        },
        quadro=_QuadroMock(ug='210101'),
    )


def _itin_tipo1(itin_id=3):
    """Tipo 1 (Apenas Diárias) — sem passagens."""
    return _ItinerarioMock(
        itin_id=itin_id,
        tipo_solicitacao_id=1,
        tipo_itinerario=1,
        etapa_atual_id=3,
    )


def _make_mock_user():
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
    with app.test_request_context(f'/diarias/administracao/{itin.id}?aba={aba}'):
        from flask import g, render_template
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


class TestStepperSemEscolhaVoo:
    """Stepper não deve mais exibir a etapa 'Cotações e Voo'."""

    def test_stepper_tipo2_sem_passo_cotacoes(self, app):
        """Para tipo 2, stepper não deve ter passo 'Cotações e Voo'."""
        itin = _itin_etapa3_tipo2()
        html = _render_stepper(app, itin)
        assert 'Cotações e Voo' not in html
        assert 'Cotações e Escolha do Voo' not in html

    def test_stepper_tipo2_tem_5_passos(self, app):
        """Stepper deve ter 5 passos para tipo 2 (igual ao tipo 1)."""
        itin = _itin_etapa3_tipo2()
        html = _render_stepper(app, itin)
        assert 'Autorização' in html
        assert 'Análise 1ª Parte' in html
        assert 'Análise 2ª Parte' in html
        assert 'Concessão' in html
        assert 'Prestação de Contas' in html

    def test_stepper_tipo1_sem_passo_cotacoes(self, app):
        """Para tipo 1, stepper nunca teve cotações — confirma que continua assim."""
        itin = _itin_tipo1()
        html = _render_stepper(app, itin)
        assert 'Cotações e Voo' not in html


class TestAbaPassagensIndependente:
    """Aba 'Passagens' deve ser acessível independente da etapa."""

    def test_aba_passagens_visivel_etapa3(self, app):
        """Na etapa 3, aba passagens deve renderizar sem bloqueio."""
        itin = _itin_etapa3_tipo2()
        html = _render_detalhe(app, itin, aba='passagens')
        assert 'Pré-requisitos não atendidos' not in html

    def test_aba_passagens_visivel_etapa6(self, app):
        """Na etapa 6, aba passagens deve renderizar normalmente."""
        itin = _itin_etapa6_tipo2()
        html = _render_detalhe(app, itin, aba='passagens')
        assert 'Pré-requisitos não atendidos' not in html

    def test_aba_passagens_form_cadastro_visivel_sem_escolha(self, app):
        """Formulário 'Cadastrar Opção de Voo' visível quando escolha não foi feita."""
        itin = _itin_etapa6_tipo2()
        html = _render_detalhe(app, itin, aba='passagens')
        assert 'Cadastrar Opção de Voo' in html

    def test_aba_passagens_tipo1_nao_renderiza(self, app):
        """Para tipo 1, a aba passagens não deve renderizar conteúdo."""
        itin = _itin_tipo1()
        html = _render_detalhe(app, itin, aba='passagens')
        assert 'Cadastrar Opção de Voo' not in html

    def test_tab_passagens_aparece_no_nav(self, app):
        """A aba 'Passagens' deve aparecer como tab top-level para tipo 2."""
        itin = _itin_etapa3_tipo2()
        html = _render_detalhe(app, itin, aba='resumo')
        assert 'bi-airplane' in html
        assert 'Passagens' in html
