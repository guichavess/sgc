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

        Args:
            modulo: Nome do módulo (ex: 'solicitacoes', 'financeiro')
            acao: Ação específica (ex: 'visualizar', 'criar', 'editar', 'excluir').
                  Se None, verifica apenas acesso ao módulo.
        """
        query = self.permissoes.filter_by(modulo=modulo)
        if acao:
            query = query.filter_by(acao=acao)
        return query.count() > 0

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
    ('solicitacoes', 'Pagamentos'),
    ('financeiro', 'Financeiro'),
    ('prestacoes_contratos', 'Execuções de Contratos'),
    ('diarias', 'Diárias'),
]

ACOES = [
    ('visualizar', 'Visualizar'),
    ('criar', 'Criar'),
    ('editar', 'Editar'),
    ('excluir', 'Excluir'),
    ('aprovar', 'Aprovar'),
]
