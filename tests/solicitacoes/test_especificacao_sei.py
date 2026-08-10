"""
Testes da especificação gerada ao criar o processo de pagamento no SEI.

Bug original (processo 00002.007489/2026-78): a especificação era montada com
`nomeContratadoResumido[18:]`, um corte pensado para o campo `nomeContratado`
("CNPJ - RAZAO SOCIAL", 17 chars de prefixo) mas aplicado ao campo resumido,
que não tem prefixo de CNPJ. Resultado no SEI:

    TAILANDIA ADMINISTRACAO  ->  "PAGAMENTO DE CONTRATO 09/2018-RACAO-18000602-07/2026"

Nomes com <= 18 chars geravam hífen duplo ("09/2018--25014138-07/2026").
"""
import pytest

from app.services.sei_integration import montar_especificacao_pagamento


# ──────────────────────────────────────────────────────────────────────────────
# Nome do contratado — sem mutilação
# ──────────────────────────────────────────────────────────────────────────────

def test_nome_longo_nao_e_cortado():
    """O bug de origem: 'TAILANDIA ADMINISTRACAO' virava 'RACAO'."""
    esp = montar_especificacao_pagamento(
        {
            'numeroOriginal': '09/2018',
            'codigo': '18000602',
            'nomeContratadoResumido': 'TAILANDIA ADMINISTRACAO',
            'nomeContratado': '10377872000127 - TAILANDIA ADMINISTRACAO E INCORPORACAO S.A',
        },
        '07/2026',
    )

    assert 'TAILANDIA ADMINISTRACAO' in esp
    assert 'RACAO-' not in esp
    assert esp == (
        'PAGAMENTO DE CONTRATO 09/2018 - TAILANDIA ADMINISTRACAO - '
        '18000602 - 07/2026'
    )


@pytest.mark.parametrize('resumido,fragmento', [
    ('EMPREENDIMENTOS IMOBILIÁRIOS', 'OBILIÁRIOS'),
    ('FORTED TELECOMUNICAÇÕES', 'AÇÕES'),
    ('CONCRETIZAR SERVICOS', 'OS'),
    ('CONSTRUTORA ENGEMAX', 'X'),
])
def test_nenhum_nome_vira_fragmento(resumido, fragmento):
    """Nenhum contratado deve aparecer reduzido ao sufixo a partir do char 18."""
    esp = montar_especificacao_pagamento(
        {'numeroOriginal': '01/2020', 'codigo': '20000001',
         'nomeContratadoResumido': resumido},
        '07/2026',
    )

    assert resumido in esp
    assert f'- {fragmento} -' not in esp


def test_usa_nomecontratado_sem_cnpj_quando_resumido_ausente():
    """Sem resumido, cai no nomeContratado descartando o prefixo do CNPJ."""
    esp = montar_especificacao_pagamento(
        {
            'numeroOriginal': '04/2025',
            'codigo': '25014138',
            'nomeContratadoResumido': '',
            'nomeContratado': '07921065000119 - AMC EMPREENDIMENTOS LTDA',
        },
        '07/2026',
    )

    assert esp == (
        'PAGAMENTO DE CONTRATO 04/2025 - AMC EMPREENDIMENTOS LTDA - '
        '25014138 - 07/2026'
    )
    assert '07921065000119' not in esp


# ──────────────────────────────────────────────────────────────────────────────
# Campos ausentes — sem separadores órfãos
# ──────────────────────────────────────────────────────────────────────────────

def test_sem_nome_nao_gera_hifen_duplo():
    """Contrato sem nome de contratado não pode gerar '--' na especificação."""
    esp = montar_especificacao_pagamento(
        {'numeroOriginal': '02/2013', 'codigo': '17000479',
         'nomeContratadoResumido': None, 'nomeContratado': None},
        '07/2026',
    )

    assert '--' not in esp
    assert esp == 'PAGAMENTO DE CONTRATO 02/2013 - 17000479 - 07/2026'


def test_todos_os_campos_vazios():
    """Sem nenhum dado, sobra apenas o prefixo — sem separadores soltos."""
    esp = montar_especificacao_pagamento({}, '')

    assert esp == 'PAGAMENTO DE CONTRATO'


def test_espacos_em_branco_sao_normalizados():
    esp = montar_especificacao_pagamento(
        {'numeroOriginal': '  15/2026 ', 'codigo': ' 26104725',
         'nomeContratadoResumido': '  IMOBILIARIA JJ  '},
        ' 06/2026 ',
    )

    assert esp == (
        'PAGAMENTO DE CONTRATO 15/2026 - IMOBILIARIA JJ - 26104725 - 06/2026'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Limite de tamanho do campo no SEI
# ──────────────────────────────────────────────────────────────────────────────

def test_trunca_em_250_caracteres():
    esp = montar_especificacao_pagamento(
        {'numeroOriginal': '01/2026', 'codigo': '26000001',
         'nomeContratadoResumido': 'A' * 400},
        '07/2026',
    )

    assert len(esp) == 250


# ──────────────────────────────────────────────────────────────────────────────
# Integração com criar_procedimento_pagamento (payload enviado ao SEI)
# ──────────────────────────────────────────────────────────────────────────────

def test_criar_procedimento_envia_especificacao_correta(monkeypatch):
    """A especificação do payload e a devolvida em EspecificacaoGerada batem."""
    from app.services import sei_integration

    capturado = {}

    class FakeResponse:
        status_code = 201

        def raise_for_status(self):
            pass

        def json(self):
            return {'IdProcedimento': '1', 'ProcedimentoFormatado': '00002.000001/2026-01'}

    def fake_post(url, json=None, headers=None, verify=None, **kwargs):
        capturado['payload'] = json
        return FakeResponse()

    monkeypatch.setattr(sei_integration.requests, 'post', fake_post)

    retorno = sei_integration.criar_procedimento_pagamento(
        'token-fake',
        '110006213',
        {'numeroOriginal': '09/2018', 'codigo': '18000602',
         'nomeContratadoResumido': 'TAILANDIA ADMINISTRACAO'},
        '07/2026',
    )

    esperado = (
        'PAGAMENTO DE CONTRATO 09/2018 - TAILANDIA ADMINISTRACAO - '
        '18000602 - 07/2026'
    )
    assert capturado['payload']['procedimento']['Especificacao'] == esperado
    assert retorno['EspecificacaoGerada'] == esperado
