"""
Testes unitários — helpers de ordem cronológica das etapas de diárias.
=======================================================================
Arquivo alvo: app/constants.py
Funções: ordem_etapa_diaria, etapa_diaria_em_ou_apos, MAPA_ORDEM_ETAPAS_DIARIAS

O fluxo de diárias tem ordem CRONOLÓGICA diferente do ID numérico da etapa:
    SOLICITACAO_INICIAL (ID=1) → ordem 1
    ANALISE_SOLICITACAO  (ID=3) → ordem 2
    ANALISE_SOLICITACAO_2 (ID=6) → ordem 3  ← ID 6, mas é a 3ª etapa!
    CONCESSAO_DIARIAS    (ID=4) → ordem 4  ← ID 4, mas vem DEPOIS do 6
    PRESTACAO_CONTAS     (ID=5) → ordem 5

Como rodar:
    pytest tests/diarias/test_ordem_etapas.py -v
"""
import pytest


class TestOrdemEtapaDiaria:
    """Testa a função ordem_etapa_diaria."""

    def test_solicitacao_inicial_tem_ordem_1(self):
        from app.constants import ordem_etapa_diaria, DiariasEtapaID
        assert ordem_etapa_diaria(DiariasEtapaID.SOLICITACAO_INICIAL) == 1

    def test_analise_solicitacao_tem_ordem_2(self):
        from app.constants import ordem_etapa_diaria, DiariasEtapaID
        assert ordem_etapa_diaria(DiariasEtapaID.ANALISE_SOLICITACAO) == 2

    def test_analise_solicitacao_2_tem_ordem_3(self):
        from app.constants import ordem_etapa_diaria, DiariasEtapaID
        assert ordem_etapa_diaria(DiariasEtapaID.ANALISE_SOLICITACAO_2) == 3

    def test_concessao_diarias_tem_ordem_4(self):
        from app.constants import ordem_etapa_diaria, DiariasEtapaID
        assert ordem_etapa_diaria(DiariasEtapaID.CONCESSAO_DIARIAS) == 4

    def test_prestacao_contas_tem_ordem_5(self):
        from app.constants import ordem_etapa_diaria, DiariasEtapaID
        assert ordem_etapa_diaria(DiariasEtapaID.PRESTACAO_CONTAS) == 5

    def test_escolha_voo_retorna_0_descontinuada(self):
        from app.constants import ordem_etapa_diaria, DiariasEtapaID
        assert ordem_etapa_diaria(DiariasEtapaID.ESCOLHA_VOO) == 0

    def test_id_inexistente_retorna_0(self):
        from app.constants import ordem_etapa_diaria
        assert ordem_etapa_diaria(999) == 0

    def test_none_retorna_0(self):
        from app.constants import ordem_etapa_diaria
        assert ordem_etapa_diaria(None) == 0

    def test_aceita_int_raw(self):
        """Deve aceitar int cru (ID da etapa) além do IntEnum."""
        from app.constants import ordem_etapa_diaria
        # ID=4 = CONCESSAO_DIARIAS → ordem 4
        assert ordem_etapa_diaria(4) == 4
        # ID=6 = ANALISE_SOLICITACAO_2 → ordem 3
        assert ordem_etapa_diaria(6) == 3

    def test_concessao_id4_tem_ordem_maior_que_analise2_id6(self):
        """
        TESTE CRÍTICO: CONCESSAO_DIARIAS (ID=4) vem DEPOIS de ANALISE_SOLICITACAO_2 (ID=6)
        na ordem cronológica real. ordem(4) > ordem(6).
        """
        from app.constants import ordem_etapa_diaria
        ordem_concessao = ordem_etapa_diaria(4)   # CONCESSAO_DIARIAS
        ordem_analise2 = ordem_etapa_diaria(6)    # ANALISE_SOLICITACAO_2
        assert ordem_concessao > ordem_analise2, (
            f'CONCESSAO_DIARIAS (ID=4, ordem={ordem_concessao}) deve ter ordem maior que '
            f'ANALISE_SOLICITACAO_2 (ID=6, ordem={ordem_analise2}). '
            f'O fluxo real é: ...→ etapa_6 → etapa_4 →... (etapa 6 vem antes de 4).'
        )


class TestEtapaDiariaEmOuApos:
    """Testa a função etapa_diaria_em_ou_apos."""

    def test_etapa_igual_retorna_true(self):
        from app.constants import etapa_diaria_em_ou_apos, DiariasEtapaID
        assert etapa_diaria_em_ou_apos(
            DiariasEtapaID.ANALISE_SOLICITACAO_2,
            DiariasEtapaID.ANALISE_SOLICITACAO_2,
        ) is True

    def test_etapa_posterior_retorna_true(self):
        from app.constants import etapa_diaria_em_ou_apos, DiariasEtapaID
        assert etapa_diaria_em_ou_apos(
            DiariasEtapaID.CONCESSAO_DIARIAS,
            DiariasEtapaID.ANALISE_SOLICITACAO,
        ) is True

    def test_etapa_anterior_retorna_false(self):
        from app.constants import etapa_diaria_em_ou_apos, DiariasEtapaID
        assert etapa_diaria_em_ou_apos(
            DiariasEtapaID.ANALISE_SOLICITACAO,
            DiariasEtapaID.CONCESSAO_DIARIAS,
        ) is False

    def test_avanco_6_para_4_nao_eh_regressao(self):
        """
        TESTE CRÍTICO: avançar de ANALISE_SOLICITACAO_2 (ID=6) para
        CONCESSAO_DIARIAS (ID=4) é um AVANÇO na ordem cronológica real.
        etapa_diaria_em_ou_apos(4, 6) deve ser True (4 está em/após 6 na ordem real).
        """
        from app.constants import etapa_diaria_em_ou_apos, DiariasEtapaID
        result = etapa_diaria_em_ou_apos(
            DiariasEtapaID.CONCESSAO_DIARIAS,    # etapa atual = 4
            DiariasEtapaID.ANALISE_SOLICITACAO_2, # referência = 6
        )
        assert result is True, (
            'etapa_diaria_em_ou_apos(CONCESSAO=4, ANALISE_2=6) deve ser True: '
            'CONCESSAO_DIARIAS (ordem=4) está APÓS ANALISE_SOLICITACAO_2 (ordem=3). '
            'Sem essa correção, o sync trata 6→4 como regressão (BUG).'
        )

    def test_6_nao_esta_em_ou_apos_concessao(self):
        """
        ANALISE_SOLICITACAO_2 (ID=6, ordem=3) NÃO está em/após CONCESSAO_DIARIAS (ID=4, ordem=4).
        Portanto: inserir NL/PD/OB na etapa 6 deve ser BLOQUEADO.
        """
        from app.constants import etapa_diaria_em_ou_apos, DiariasEtapaID
        result = etapa_diaria_em_ou_apos(
            DiariasEtapaID.ANALISE_SOLICITACAO_2, # etapa atual = 6
            DiariasEtapaID.CONCESSAO_DIARIAS,     # referência = 4
        )
        assert result is False, (
            'etapa_diaria_em_ou_apos(ANALISE_2=6, CONCESSAO=4) deve ser False: '
            'ANALISE_SOLICITACAO_2 (ordem=3) vem ANTES de CONCESSAO_DIARIAS (ordem=4). '
            'O guard "not etapa_diaria_em_ou_apos(6, 4)" deve bloquear NL/PD/OB na etapa 6.'
        )

    def test_prestacao_contas_em_ou_apos_concessao(self):
        from app.constants import etapa_diaria_em_ou_apos, DiariasEtapaID
        assert etapa_diaria_em_ou_apos(
            DiariasEtapaID.PRESTACAO_CONTAS,
            DiariasEtapaID.CONCESSAO_DIARIAS,
        ) is True

    def test_solicitacao_inicial_nao_em_ou_apos_analise(self):
        from app.constants import etapa_diaria_em_ou_apos, DiariasEtapaID
        assert etapa_diaria_em_ou_apos(
            DiariasEtapaID.SOLICITACAO_INICIAL,
            DiariasEtapaID.ANALISE_SOLICITACAO,
        ) is False

    def test_aceita_int_raw(self):
        """Deve aceitar int cru além do IntEnum."""
        from app.constants import etapa_diaria_em_ou_apos
        # 4 (CONCESSAO) está após 6 (ANALISE_2) na ordem real
        assert etapa_diaria_em_ou_apos(4, 6) is True
        # 6 (ANALISE_2) NÃO está após 4 (CONCESSAO) na ordem real
        assert etapa_diaria_em_ou_apos(6, 4) is False
