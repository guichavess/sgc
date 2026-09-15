"""
Catálogo do Hub de módulos (/hub).

Monta, para o usuário logado, os cards do hub já filtrados por permissão —
mesmas regras de `Usuario.tem_permissao` (qualquer ação no módulo libera o
card; em módulos com páginas, qualquer página; `is_admin` libera tudo) — com os
textos exibidos na expansão do card e o rótulo "Seu acesso". As permissões do
perfil são lidas em uma única query (índice do `Perfil`).
"""
from flask import url_for

from app.models.perfil import HIERARQUIA_ACOES, PAGINAS_MODULO, REGRA_ALTA_GESTAO

SECAO_MODULOS = 'modulos'
SECAO_ADMIN = 'admin'

ROTULO_ADMIN_TOTAL = 'Acesso total'
ROTULO_EXCLUSIVO_ADMIN = 'Exclusivo para administradores'

# `id` = nome do módulo nas permissões (PerfilPermissao.modulo).
# `restrito_admin` = só `is_admin` vê (não é liberável por perfil).
CATALOGO_HUB = [
    {
        'id': 'solicitacoes', 'secao': SECAO_MODULOS, 'endpoint': 'solicitacoes.dashboard', 'restrito_admin': False,
        'nome': 'Solicitação de Pagamentos', 'descricao': 'NE, NL, PD e OB por contrato',
        'cor': '#343990', 'icone': 'bi-cash-stack',
        'resumo': 'Acompanhe o pagamento de cada contrato do pedido à ordem bancária, com etapas atualizadas automaticamente pelo SEI.',
        'recursos': [('bi-file-earmark-plus', 'Nova solicitação por contrato'),
                     ('bi-arrow-repeat', 'Sincronização com SEI e SIAFE'),
                     ('bi-receipt', 'NE, NL, PD e OB no mesmo lugar')],
        'fluxo': ['Criada', 'Em análise', 'Aguard. NE', 'NE inserida', 'Execução', 'Concluída'],
    },
    {
        'id': 'financeiro', 'secao': SECAO_MODULOS, 'endpoint': 'financeiro.dashboard', 'restrito_admin': False,
        'nome': 'Gestão Orçamentária/Financeira', 'descricao': 'Saldos, LOA e Fundo Rotativo',
        'cor': '#1F3A68', 'icone': 'bi-bank',
        'resumo': 'Execução orçamentária e financeira da secretaria: saldo da LOA por classificação, empenhos do SIAFE e Fundo Rotativo.',
        'recursos': [('bi-pie-chart', 'Saldos por ação, natureza e fonte'),
                     ('bi-journal-check', 'Inserção de notas de empenho'),
                     ('bi-wallet2', 'Fundo Rotativo e fornecedores')],
        'fluxo': None,
    },
    {
        'id': 'prestacoes_contratos', 'secao': SECAO_MODULOS, 'endpoint': 'prestacoes_contratos.dashboard', 'restrito_admin': False,
        'nome': 'Execuções de Contratos', 'descricao': 'Registro e atesto de execuções',
        'cor': '#0D7A35', 'icone': 'bi-clipboard-check',
        'resumo': 'Registre o que foi executado em cada contrato, vinculado aos itens do catálogo CATSERV/CATMAT.',
        'recursos': [('bi-list-check', 'Execuções por item vinculado'),
                     ('bi-tags', 'Tipificação CATSERV/CATMAT'),
                     ('bi-patch-check', 'Atesto e fiscalização')],
        'fluxo': None,
    },
    {
        'id': 'diarias', 'secao': SECAO_MODULOS, 'endpoint': 'diarias.dashboard', 'restrito_admin': False,
        'nome': 'Diárias', 'descricao': 'Itinerários, reservas e prestação de contas',
        'cor': '#E07A24', 'icone': 'bi-airplane-engines',
        'resumo': 'Da solicitação da viagem à prestação de contas, com valores calculados por cargo e tipo de itinerário.',
        'recursos': [('bi-geo-alt', 'Itinerários estaduais e nacionais'),
                     ('bi-person-badge', 'Dados do servidor via SGA'),
                     ('bi-file-earmark-text', 'Nota de reserva e empenho')],
        'fluxo': ['Solicitação', 'Análise', 'Voo', 'NCI', 'Concessão', 'Prestação'],
    },
    {
        'id': 'identidade_visual', 'secao': SECAO_MODULOS, 'endpoint': 'identidade_visual.dashboard', 'restrito_admin': False,
        'nome': 'Identidade Visual', 'descricao': 'Fachadas dos Espaços da Cidadania',
        'cor': '#0891B2', 'icone': 'bi-shop',
        'resumo': 'Cadastro e acompanhamento das fachadas e veículos dos Espaços e Salas da Cidadania, com fotos e histórico.',
        'recursos': [('bi-images', 'Fotos e documentos até 25 MB'),
                     ('bi-car-front', 'Consulta de placa no DETRAN'),
                     ('bi-clock-history', 'Histórico de alterações')],
        'fluxo': None,
    },
    {
        'id': 'cgfr', 'secao': SECAO_MODULOS, 'endpoint': 'cgfr.dashboard', 'restrito_admin': False,
        'nome': 'CGFR', 'descricao': 'Gestão financeira e por resultados',
        'cor': '#4F46E5', 'icone': 'bi-file-earmark-bar-graph',
        'resumo': 'Comissão de Gestão Financeira e Gestão Por Resultados: documentos e processos do SEI organizados para análise.',
        'recursos': [('bi-database', 'Consulta à base do SEI'),
                     ('bi-cloud-arrow-down', 'Sincronização de documentos'),
                     ('bi-bar-chart', 'Relatórios por processo')],
        'fluxo': None,
    },
    {
        'id': 'usuarios', 'secao': SECAO_ADMIN, 'endpoint': 'usuarios.dashboard', 'restrito_admin': True,
        'nome': 'Admin', 'descricao': 'Usuários, perfis e acessos',
        'cor': '#6f42c1', 'icone': 'bi-people',
        'resumo': 'Controle quem acessa o SGC: cadastro de usuários, perfis e as permissões de cada módulo.',
        'recursos': [('bi-person-plus', 'Cadastro e ativação de usuários'),
                     ('bi-shield-lock', 'Perfis com permissões por módulo'),
                     ('bi-diagram-3', 'Níveis: visualizar a excluir')],
        'fluxo': None,
    },
    {
        'id': 'dashboards', 'secao': SECAO_ADMIN, 'endpoint': 'dashboards.spa_shell', 'restrito_admin': False,
        'nome': 'Dashboards', 'descricao': 'Indicadores consolidados',
        'cor': '#1B998B', 'icone': 'bi-speedometer2',
        'resumo': 'Indicadores consolidados da secretaria em painéis: pagamentos, execução financeira e contratos.',
        'recursos': [('bi-graph-up', 'Visão consolidada'),
                     ('bi-cash-coin', 'Pagamentos e financeiro'),
                     ('bi-file-earmark-ruled', 'Contratos e execuções')],
        'fluxo': None,
    },
]

_CHAVES_CARD = ('id', 'nome', 'descricao', 'cor', 'icone', 'resumo', 'recursos', 'fluxo')


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
        id, nome, descricao, cor, icone, resumo, recursos, fluxo, url e acesso.
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
