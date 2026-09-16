"""
Catálogo do Hub de módulos (/hub).

Monta, para o usuário logado, os cards do hub já filtrados por permissão —
mesmas regras de `Usuario.tem_permissao` (qualquer ação no módulo libera o
card; em módulos com páginas, qualquer página; `is_admin` libera tudo) — com os
textos exibidos na expansão do card e o rótulo "Seu acesso". As permissões do
perfil são lidas em uma única query (índice do `Perfil`).
"""
from collections import namedtuple

from flask import url_for

from app.models.perfil import HIERARQUIA_ACOES, PAGINAS_MODULO, REGRA_ALTA_GESTAO

SECAO_MODULOS = 'modulos'
SECAO_ADMIN = 'admin'

ROTULO_ADMIN_TOTAL = 'Acesso total'
ROTULO_EXCLUSIVO_ADMIN = 'Exclusivo para administradores'

Recurso = namedtuple('Recurso', 'icone texto')

# `id` = nome do módulo nas permissões (PerfilPermissao.modulo).
# `restrito_admin` = só `is_admin` vê (não é liberável por perfil).
# `resumo` responde "de que este módulo é responsável" e os três `recursos`, "o
# que dá para resolver aqui" — capacidades em uma linha, não os itens do menu.
CATALOGO_HUB = [
    {
        'id': 'solicitacoes', 'secao': SECAO_MODULOS, 'endpoint': 'solicitacoes.dashboard', 'restrito_admin': False,
        'nome': 'Solicitação de Pagamentos', 'descricao': 'Pagamento de contratos, do pedido à ordem bancária',
        'cor': '#343990', 'icone': 'bi-cash-stack',
        'resumo': 'Registra o pedido de pagamento de cada contrato e acompanha a tramitação até a OB — as etapas avançam sozinhas conforme os documentos entram no processo do SEI.',
        'recursos': [
            Recurso('bi-file-earmark-plus', 'Abertura de pedidos por contrato, individual ou em lote'),
            Recurso('bi-receipt', 'Acompanhamento de NE, NL, PD e OB em um só lugar'),
            Recurso('bi-graph-up', 'Estoque por fase, tempo de tramitação e relatórios'),
        ],
    },
    {
        'id': 'financeiro', 'secao': SECAO_MODULOS, 'endpoint': 'financeiro.dashboard', 'restrito_admin': False,
        'nome': 'Gestão Orçamentária/Financeira', 'descricao': 'Orçamento, empenho e fundo rotativo',
        'cor': '#1F3A68', 'icone': 'bi-bank',
        'resumo': 'Onde a equipe financeira controla o orçamento disponível, empenha as despesas e mantém o sistema alimentado com os dados do SIAFE.',
        'recursos': [
            Recurso('bi-wallet2', 'Saldo da LOA por ação, natureza e fonte'),
            Recurso('bi-pencil-square', 'Empenho das despesas e conferência das diárias'),
            Recurso('bi-cash-stack', 'Fundo rotativo, planejamento e despesas sem contrato'),
        ],
    },
    {
        'id': 'prestacoes_contratos', 'secao': SECAO_MODULOS, 'endpoint': 'prestacoes_contratos.dashboard', 'restrito_admin': False,
        'nome': 'Execuções de Contratos', 'descricao': 'Contratos vigentes e o que já foi executado',
        'cor': '#0D7A35', 'icone': 'bi-clipboard-check',
        'resumo': 'Reúne o que foi contratado, como está classificado no catálogo e quanto já foi executado de cada item, contrato a contrato.',
        'recursos': [
            Recurso('bi-file-earmark-text', 'Contratos com situação, vigência e centro de custo'),
            Recurso('bi-tags', 'Classificação dos itens no catálogo CATSERV/CATMAT'),
            Recurso('bi-journal-check', 'Execução por item, com quantidade e valor'),
        ],
    },
    {
        'id': 'diarias', 'secao': SECAO_MODULOS, 'endpoint': 'diarias.dashboard', 'restrito_admin': False,
        'nome': 'Diárias', 'descricao': 'Viagem a serviço, do pedido à prestação de contas',
        'cor': '#E07A24', 'icone': 'bi-airplane-engines',
        'resumo': 'Viagem a serviço do começo ao fim: o servidor pede, as chefias autorizam, a equipe resolve passagem e empenho, e a prestação de contas encerra o processo.',
        'recursos': [
            Recurso('bi-geo-alt', 'Itinerários estaduais e nacionais, com valor por cargo'),
            Recurso('bi-check2-square', 'Autorização das chefias e assinatura no SEI'),
            Recurso('bi-receipt', 'Passagens, empenho e prestação de contas da viagem'),
        ],
    },
    {
        'id': 'identidade_visual', 'secao': SECAO_MODULOS, 'endpoint': 'identidade_visual.dashboard', 'restrito_admin': False,
        'nome': 'Identidade Visual', 'descricao': 'Padronização visual das unidades e da frota',
        'cor': '#0891B2', 'icone': 'bi-shop',
        'resumo': 'Acompanha a aplicação da identidade visual do Governo nos Espaços e Salas da Cidadania, no Justo Acesso e nos veículos da secretaria.',
        'recursos': [
            Recurso('bi-geo-alt', 'Situação de cada unidade por município'),
            Recurso('bi-images', 'Registro fotográfico do antes e depois, com custo'),
            Recurso('bi-car-front', 'Veículos identificados pela placa no DETRAN'),
        ],
    },
    {
        'id': 'cgfr', 'secao': SECAO_MODULOS, 'endpoint': 'cgfr.dashboard', 'restrito_admin': False,
        'nome': 'CGFR', 'descricao': 'Processos analisados pela comissão',
        'cor': '#4F46E5', 'icone': 'bi-file-earmark-bar-graph',
        'resumo': 'Concentra os processos do SEI submetidos à Comissão de Gestão Financeira e Gestão por Resultados e a deliberação dada a cada um.',
        'recursos': [
            Recurso('bi-collection', 'Processos da comissão em uma base única'),
            Recurso('bi-cloud-arrow-down', 'Documentos e andamentos puxados do SEI'),
            Recurso('bi-file-earmark-bar-graph', 'Consolidado por natureza em PDF ou Excel'),
        ],
    },
    {
        'id': 'usuarios', 'secao': SECAO_ADMIN, 'endpoint': 'usuarios.dashboard', 'restrito_admin': True,
        'nome': 'Admin', 'descricao': 'Usuários, perfis e permissões de acesso',
        'cor': '#6f42c1', 'icone': 'bi-people',
        'resumo': 'Define quem entra no SGC e o que cada um enxerga: o usuário recebe um perfil e o perfil libera os módulos e as ações permitidas.',
        'recursos': [
            Recurso('bi-person-plus', 'Cadastro e ativação de servidores'),
            Recurso('bi-shield-lock', 'Perfis reaproveitados entre vários usuários'),
            Recurso('bi-sliders', 'Liberação por módulo, por tela e por ação'),
        ],
    },
    {
        'id': 'dashboards', 'secao': SECAO_ADMIN, 'endpoint': 'dashboards.spa_shell', 'restrito_admin': False,
        'nome': 'Dashboards', 'descricao': 'Execução orçamentária em gráficos',
        'cor': '#1B998B', 'icone': 'bi-speedometer2',
        'resumo': 'Leitura gerencial da execução orçamentária da secretaria por exercício, montada sobre os dados que vêm do SIAFE.',
        'recursos': [
            Recurso('bi-graph-up', 'Reservado, empenhado, liquidado e pago sobre a dotação'),
            Recurso('bi-calendar3', 'Evolução mês a mês e por natureza de despesa'),
            Recurso('bi-cash-stack', 'Programação de desembolso e o que segue em aberto'),
        ],
    },
]

_CHAVES_CARD = ('id', 'nome', 'descricao', 'cor', 'icone', 'resumo', 'recursos')


def _juntar(partes):
    """'a' · 'a e b' · 'a, b e c'."""
    if len(partes) <= 1:
        return ''.join(partes)
    return ', '.join(partes[:-1]) + ' e ' + partes[-1]


def rotulo_acesso(acoes):
    """Rótulo "Seu acesso" a partir das ações concedidas no módulo.

    Segue a hierarquia de `Perfil.tem_permissao`: a maior ação CRUD implica as
    menores e qualquer ação implica visualizar; `aprovar` é independente.
    """
    acoes = set(acoes)
    nivel = max((HIERARQUIA_ACOES.get(a, 0) for a in acoes), default=0)
    aprovar = 'aprovar' in acoes
    if nivel == HIERARQUIA_ACOES['excluir'] and aprovar:
        return ROTULO_ADMIN_TOTAL

    partes = ['Visualizar'] + [a for a in ('criar', 'editar', 'excluir') if HIERARQUIA_ACOES[a] <= nivel]
    if aprovar:
        partes.append('aprovar')
    if len(partes) == 1:
        return 'Somente visualizar'
    return _juntar(partes)


def _rotulo_paginas(usuario, modulo, indice):
    """"Seu acesso" de um módulo com páginas.

    "Acesso total" com todas as páginas completas; "Acesso total, exceto …" quando
    só faltam páginas da alta gestão; senão, página a página na ordem do menu.
    """
    rotulos = []
    for pagina in usuario.paginas_acessiveis(modulo):
        if pagina.regra == REGRA_ALTA_GESTAO:
            rotulo = ROTULO_ADMIN_TOTAL
        else:
            rotulo = rotulo_acesso(indice.get((modulo, pagina.chave), ()))
        rotulos.append((pagina, rotulo))

    completas = {p.chave for p, rotulo in rotulos if rotulo == ROTULO_ADMIN_TOTAL}
    faltando = [p for p in PAGINAS_MODULO[modulo] if p.chave not in completas]
    if not faltando:
        return ROTULO_ADMIN_TOTAL
    if all(p.regra == REGRA_ALTA_GESTAO for p in faltando):
        return f'{ROTULO_ADMIN_TOTAL}, exceto ' + _juntar([p.rotulo for p in faltando])
    return '; '.join(f'{p.rotulo}: {rotulo[0].lower()}{rotulo[1:]}' for p, rotulo in rotulos)


def _permissoes_indexadas(usuario):
    """{(modulo, pagina): {acoes}} do perfil do usuário, em uma query."""
    if not usuario.perfil_id or not usuario.perfil:
        return {}
    return usuario.perfil.permissoes_indexadas()


def montar_hub(usuario):
    """Cards do hub visíveis para o usuário.

    Returns:
        {'modulos': [card], 'admin': [card], 'total': int}, onde card tem
        id, nome, descricao, cor, icone, resumo, recursos, url e acesso.
    """
    indice = {} if usuario.is_admin else _permissoes_indexadas(usuario)

    modulos, admin = [], []
    for item in CATALOGO_HUB:
        if item['restrito_admin']:
            if not usuario.is_admin:
                continue
            acesso = ROTULO_EXCLUSIVO_ADMIN
        elif usuario.is_admin:
            acesso = ROTULO_ADMIN_TOTAL
        elif item['id'] in PAGINAS_MODULO:
            if not usuario.paginas_acessiveis(item['id']):
                continue
            acesso = _rotulo_paginas(usuario, item['id'], indice)
        elif (item['id'], '') in indice:
            acesso = rotulo_acesso(indice[(item['id'], '')])
        else:
            continue

        card = {chave: item[chave] for chave in _CHAVES_CARD}
        card['url'] = url_for(item['endpoint'])
        card['acesso'] = acesso
        (admin if item['secao'] == SECAO_ADMIN else modulos).append(card)

    return {'modulos': modulos, 'admin': admin, 'total': len(modulos) + len(admin)}
