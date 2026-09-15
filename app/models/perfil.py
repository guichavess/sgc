"""
Modelos de Perfil e Permissões do sistema.

Cada permissão é (módulo, página, ação). Módulos sem páginas usam pagina=''
(o módulo inteiro); módulos com páginas (PAGINAS_MODULO, ex.: Financeiro) têm
uma permissão por página. A regra completa de acesso — admin, alta gestão e
perfil — fica em `Usuario.tem_permissao`.
"""
from collections import namedtuple
from datetime import datetime
from app.extensions import db


class Perfil(db.Model):
    """Perfil de acesso (role) do sistema."""

    __tablename__ = 'perfis'

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(100), nullable=False, unique=True)
    descricao = db.Column(db.String(255))
    ativo = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)

    # Relationships
    permissoes = db.relationship('PerfilPermissao', backref='perfil',
                                 lazy='dynamic', cascade='all, delete-orphan')
    usuarios = db.relationship('Usuario', backref='perfil', lazy='dynamic')

    def permissoes_indexadas(self):
        """{(modulo, pagina): {acoes}} lido uma vez e guardado na instância.

        O usuário (e seu perfil) é recarregado a cada request, então o cache vale
        só para o request. Quem altera as permissões chama
        `limpar_cache_permissoes()`.
        """
        indice = getattr(self, '_cache_permissoes', None)
        if indice is None:
            indice = {}
            if self.id is not None:
                linhas = (
                    db.session.query(PerfilPermissao.modulo, PerfilPermissao.pagina, PerfilPermissao.acao)
                    .filter(PerfilPermissao.perfil_id == self.id)
                    .all()
                )
                for modulo, pagina, acao in linhas:
                    indice.setdefault((modulo, pagina or ''), set()).add(acao)
            self._cache_permissoes = indice
        return indice

    def limpar_cache_permissoes(self):
        self._cache_permissoes = None

    def tem_permissao(self, modulo, acao=None, pagina=None):
        """Verifica se o perfil tem permissão para módulo/ação (e página).

        As ações de CRUD são hierárquicas: uma ação de maior privilégio implica
        as de menor (``excluir`` ⇒ ``editar`` ⇒ ``criar``). Além disso, qualquer
        ação concedida implica poder ``visualizar``. A ação ``aprovar`` é
        ortogonal: concede visualização, mas não escrita, e exige
        correspondência exata. Em módulos com páginas a regra vale por página.

        Args:
            modulo: Nome do módulo (ex: 'solicitacoes', 'financeiro')
            acao: Ação específica (ex: 'visualizar', 'criar'). None = qualquer ação.
            pagina: Página do módulo (ex: 'fundo_rotativo'). Em módulo com
                    páginas, None = basta uma página liberada. Linhas antigas
                    com pagina='' nesses módulos são ignoradas. Páginas da alta
                    gestão nunca são liberadas pelo perfil.
        """
        indice = self.permissoes_indexadas()
        if modulo in PAGINAS_MODULO:
            liberaveis = [p.chave for p in paginas_liberaveis(modulo)]
            if pagina is None:
                return any(acao_concedida(indice.get((modulo, p)), acao) for p in liberaveis)
            if pagina not in liberaveis:
                return False
        return acao_concedida(indice.get((modulo, pagina or '')), acao)

    def listar_permissoes_dict(self):
        """Retorna dict {'modulo' ou 'modulo:pagina': [acao1, acao2, ...]}."""
        resultado = {}
        for perm in self.permissoes.order_by(PerfilPermissao.id).all():
            chave = f'{perm.modulo}:{perm.pagina}' if perm.pagina else perm.modulo
            resultado.setdefault(chave, []).append(perm.acao)
        return resultado

    def __repr__(self):
        return f'<Perfil {self.nome}>'


class PerfilPermissao(db.Model):
    """Permissão granular de um perfil: módulo + página + ação."""

    __tablename__ = 'perfil_permissoes'

    id = db.Column(db.Integer, primary_key=True)
    perfil_id = db.Column(db.Integer, db.ForeignKey('perfis.id'), nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    # '' = módulo inteiro. NOT NULL de propósito: no MySQL a UNIQUE aceitaria
    # várias linhas iguais com NULL.
    pagina = db.Column(db.String(50), nullable=False, default='', server_default='')
    acao = db.Column(db.String(20), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('perfil_id', 'modulo', 'pagina', 'acao',
                            name='uq_perfil_modulo_pagina_acao'),
    )

    def __repr__(self):
        if self.pagina:
            return f'<PerfilPermissao {self.modulo}.{self.pagina}.{self.acao}>'
        return f'<PerfilPermissao {self.modulo}.{self.acao}>'


# Constantes de módulos e ações disponíveis
# Nota: O módulo 'usuarios' NÃO está aqui porque é restrito a admins (is_admin).
# Estes são os módulos que admins podem liberar para outros usuários via perfis.
MODULOS = [
    ('dashboards', 'Dashboards'),
    ('solicitacoes', 'Pagamentos'),
    ('financeiro', 'Financeiro'),
    ('prestacoes_contratos', 'Execuções de Contratos'),
    ('diarias', 'Diárias'),
    ('cgfr', 'CGFR'),
    ('identidade_visual', 'Identidade Visual'),
]

ACOES = [
    ('visualizar', 'Visualizar'),
    ('criar', 'Criar'),
    ('editar', 'Editar'),
    ('excluir', 'Excluir'),
    ('aprovar', 'Aprovar'),
]

# Nível de privilégio das ações de CRUD para a hierarquia de `tem_permissao`:
# uma ação concedida implica todas as de nível menor. `visualizar` (nível 0) é
# concedida por qualquer permissão; `aprovar` fica de fora (ortogonal).
HIERARQUIA_ACOES = {
    'visualizar': 0,
    'criar': 1,
    'editar': 2,
    'excluir': 3,
}

# Regras de acesso de uma página:
#   perfil      — liberada pelo perfil (checkbox no formulário)
#   alta_gestao — só admin ou `Usuario.is_alta_gestao`; não aparece no formulário
REGRA_PERFIL = 'perfil'
REGRA_ALTA_GESTAO = 'alta_gestao'

PaginaModulo = namedtuple('PaginaModulo', 'chave rotulo endpoint icone regra')

# Páginas dos módulos que têm permissão por página. A ordem define a página de
# entrada do módulo (a primeira acessível) e a ordem do menu.
PAGINAS_MODULO = {
    'financeiro': [
        PaginaModulo('orcamento', 'Orçamento', 'financeiro.orcamentaria', 'bi-wallet2', REGRA_ALTA_GESTAO),
        PaginaModulo('insercao_ne', 'Inserir NEs', 'financeiro.pendencias_ne', 'bi-pencil-square', REGRA_PERFIL),
        PaginaModulo('diarias', 'Diárias', 'financeiro.diarias_lista', 'bi-airplane', REGRA_PERFIL),
        PaginaModulo('fornecedores', 'Fornecedores', 'financeiro.fornecedores_lista', 'bi-building', REGRA_PERFIL),
        PaginaModulo('execucoes', 'Execuções', 'financeiro.execucoes_lista', 'bi-clipboard-data', REGRA_PERFIL),
        PaginaModulo('fundo_rotativo', 'Fundo Rotativo', 'financeiro.fundo_rotativo_dashboard', 'bi-cash-stack', REGRA_PERFIL),
        PaginaModulo('planejamento', 'Planejamento', 'financeiro.planejamento_relatorio', 'bi-calendar-check', REGRA_ALTA_GESTAO),
    ],
}


def acao_concedida(concedidas, acao):
    """Aplica a hierarquia de ações a um conjunto de ações concedidas."""
    if not concedidas:
        return False
    # Sem ação específica ou 'visualizar': qualquer permissão basta.
    if acao is None or acao == 'visualizar':
        return True
    # Hierarquia CRUD: basta uma ação concedida de nível >= ao exigido.
    nivel_exigido = HIERARQUIA_ACOES.get(acao)
    if nivel_exigido is not None:
        maior_nivel = max((HIERARQUIA_ACOES.get(a, 0) for a in concedidas), default=0)
        if maior_nivel >= nivel_exigido:
            return True
    # Ações fora da hierarquia (ex.: 'aprovar') exigem correspondência exata.
    return acao in concedidas


def pagina_do_modulo(modulo, pagina):
    """PaginaModulo da chave informada, ou None."""
    return next((p for p in PAGINAS_MODULO.get(modulo, []) if p.chave == pagina), None)


def paginas_liberaveis(modulo):
    """Páginas do módulo que o perfil pode liberar (regra 'perfil')."""
    return [p for p in PAGINAS_MODULO.get(modulo, []) if p.regra == REGRA_PERFIL]


def parse_permissao(texto):
    """'modulo', 'modulo.acao', 'modulo.pagina' ou 'modulo.pagina.acao' → (modulo, pagina, acao).

    Com duas partes, a segunda é página se existir em PAGINAS_MODULO; senão é ação.
    """
    partes = texto.split('.')
    if len(partes) == 1:
        return partes[0], None, None
    if len(partes) == 2:
        modulo, segunda = partes
        if pagina_do_modulo(modulo, segunda):
            return modulo, segunda, None
        return modulo, None, segunda
    if len(partes) == 3:
        return partes[0], partes[1], partes[2]
    raise ValueError(f'Permissão inválida: {texto!r}')


def todas_permissoes_liberaveis():
    """Todas as (modulo, pagina, acao) que um perfil pode receber — usado no seed."""
    resultado = []
    for modulo, _ in MODULOS:
        paginas = [p.chave for p in paginas_liberaveis(modulo)] if modulo in PAGINAS_MODULO else ['']
        for pagina in paginas:
            for acao, _ in ACOES:
                resultado.append((modulo, pagina, acao))
    return resultado
