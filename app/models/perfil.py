"""
Modelos de Perfil e Permissões do sistema.
"""
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

    def tem_permissao(self, modulo, acao=None):
        """Verifica se o perfil tem permissão para módulo/ação.

        As ações de CRUD são hierárquicas: uma ação de maior privilégio implica
        as de menor (``excluir`` ⇒ ``editar`` ⇒ ``criar``). Além disso, qualquer
        ação concedida no módulo implica poder ``visualizar`` — não faz sentido
        operar um módulo sem poder vê-lo. Assim, "acesso full" (perfil com
        ``excluir``) garante acesso a tudo, sem precisar marcar cada ação. A ação
        ``aprovar`` é ortogonal: concede visualização, mas não escrita, e exige
        correspondência exata.

        Args:
            modulo: Nome do módulo (ex: 'solicitacoes', 'financeiro')
            acao: Ação específica (ex: 'visualizar', 'criar', 'editar', 'excluir').
                  Se None, verifica apenas acesso ao módulo.
        """
        concedidas = {p.acao for p in self.permissoes.filter_by(modulo=modulo).all()}
        if not concedidas:
            return False
        if acao is None:
            return True
        # Qualquer permissão no módulo concede a visualização.
        if acao == 'visualizar':
            return True
        # Hierarquia CRUD: basta uma ação concedida de nível >= ao exigido.
        nivel_exigido = HIERARQUIA_ACOES.get(acao)
        if nivel_exigido is not None:
            maior_nivel = max((HIERARQUIA_ACOES.get(a, 0) for a in concedidas), default=0)
            if maior_nivel >= nivel_exigido:
                return True
        # Ações fora da hierarquia (ex.: 'aprovar') exigem correspondência exata.
        return acao in concedidas

    def listar_permissoes_dict(self):
        """Retorna dict {modulo: [acao1, acao2, ...]}."""
        resultado = {}
        for perm in self.permissoes.all():
            if perm.modulo not in resultado:
                resultado[perm.modulo] = []
            resultado[perm.modulo].append(perm.acao)
        return resultado

    def __repr__(self):
        return f'<Perfil {self.nome}>'


class PerfilPermissao(db.Model):
    """Permissão granular de um perfil: módulo + ação."""

    __tablename__ = 'perfil_permissoes'

    id = db.Column(db.Integer, primary_key=True)
    perfil_id = db.Column(db.Integer, db.ForeignKey('perfis.id'), nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    acao = db.Column(db.String(20), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('perfil_id', 'modulo', 'acao',
                            name='uq_perfil_modulo_acao'),
    )

    def __repr__(self):
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
    ('fundo_rotativo', 'Fundo Rotativo'),
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
