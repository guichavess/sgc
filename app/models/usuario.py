"""
Modelo de Usuário do sistema.
"""
from datetime import datetime
from flask_login import UserMixin
from app.extensions import db, login_manager


class Usuario(db.Model, UserMixin):
    """Modelo para usuários do sistema."""

    __tablename__ = 'sis_usuarios'

    id = db.Column(db.BigInteger, primary_key=True)
    id_usuario_sei = db.Column(db.String(50), unique=True, nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    sigla_login = db.Column(db.String(100), nullable=False)
    cargo = db.Column(db.String(255))
    email = db.Column(db.String(255), nullable=True)
    telefone = db.Column(db.String(20), nullable=True)
    telegram_chat_id = db.Column(db.String(50), nullable=True)
    cpf = db.Column(db.String(11), nullable=True)
    contato_preenchido = db.Column(db.Boolean, default=False, nullable=False)
    notificacoes_email = db.Column(db.Boolean, default=True, nullable=False)
    notificacoes_telegram = db.Column(db.Boolean, default=True, nullable=False)
    unidade_padrao_id = db.Column(db.String(50))
    ultimo_login = db.Column(db.DateTime, default=datetime.now)
    ativo = db.Column(db.Boolean, default=True)
    perfil_id = db.Column(db.Integer, db.ForeignKey('perfis.id'), nullable=True)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)

    # Cargo de gestão (definido manualmente pelo admin)
    # Valores: 'secretario', 'secretario_exercicio', 'superintendente', 'diretor_dfin', ou NULL
    cargo_gestao = db.Column(db.String(50), nullable=True)
    setor_vinculado = db.Column(db.String(255), nullable=True)   # DEPRECATED: usar setor_id. Mantido para backfill/compatibilidade.

    # Vinculação estruturada ao setor (FK para setores) — mantido como referência
    setor_id = db.Column(db.Integer, db.ForeignKey('setores.id'), nullable=True, index=True)
    setor = db.relationship('SetorSead', foreign_keys=[setor_id], lazy='joined')

    # Unidade SEI atual e superintendência derivada (novo modelo — preenchido no login)
    unidade_sei_id = db.Column(db.String(50), nullable=True, index=True)
    unidade_sei_sigla = db.Column(db.String(255), nullable=True, index=True)
    superintendencia_sigla = db.Column(db.String(50), nullable=True, index=True)

    # Unidades SEI vinculadas (1:N — atualizado no login)
    unidades_sei = db.relationship('UsuarioUnidadeSei', back_populates='usuario',
                                   cascade='all, delete-orphan', lazy='select')

    @property
    def superintendencia(self):
        """Retorna a Superintendência do setor do usuário (ou None).

        Prioriza a superintendência derivada do SEI (campo superintendencia_sigla).
        Fallback: usa a relação setor → SetorSead (legado).
        """
        if self.superintendencia_sigla:
            return self.superintendencia_sigla
        if not self.setor:
            return None
        setor_super = self.setor.superintendencia or (self.setor if self.setor.is_superintendencia else None)
        return setor_super.sigla if setor_super else None

    @property
    def is_active(self):
        """Retorna se o usuário está ativo."""
        return self.ativo

    @property
    def is_secretario_exercicio(self):
        """Secretário em exercício (substituto do titular quando este está impedido)."""
        return self.cargo_gestao == 'secretario_exercicio'

    @property
    def nivel_secretario(self):
        """Nível hierárquico de autorização de diárias (1, 2, 3 ou None).

        1 — Secretário titular
        2 — Secretário em exercício
        3 — Superintendente (autoriza em último recurso, ação única)
        """
        if self.cargo_gestao == 'secretario':
            return 1
        if self.cargo_gestao == 'secretario_exercicio':
            return 2
        if self.cargo_gestao == 'superintendente':
            return 3
        return None

    @property
    def is_secretario(self):
        """Verifica se o usuário é Secretário (titular ou em exercício).

        Prioriza o campo cargo_gestao (definido pelo admin).
        Fallback: text matching no campo cargo do SEI (somente titular).
        """
        if self.cargo_gestao in ('secretario', 'secretario_exercicio'):
            return True
        if not self.cargo:
            return False
        cargo_lower = self.cargo.lower()
        return 'secretário' in cargo_lower or 'secretario' in cargo_lower

    @property
    def is_superintendente(self):
        """Verifica se o usuário é Superintendente.

        Prioriza o campo cargo_gestao (definido pelo admin).
        Fallback: text matching no campo cargo do SEI.
        """
        if self.cargo_gestao == 'superintendente':
            return True
        if not self.cargo:
            return False
        return 'superintendente' in self.cargo.lower()

    def tem_permissao(self, modulo, acao=None):
        """Verifica se o usuário tem permissão para módulo/ação.

        Admins têm acesso total a todos os módulos automaticamente.
        Para os demais, verifica via perfil vinculado.
        """
        if self.is_admin:
            return True
        if not self.perfil:
            return False
        return self.perfil.tem_permissao(modulo, acao)

    def __repr__(self):
        return f'<Usuario {self.nome}>'


class UsuarioUnidadeSei(db.Model):
    """Unidades SEI vinculadas a um usuário (sincronizadas no login)."""

    __tablename__ = 'usuario_unidades_sei'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id', ondelete='CASCADE'), nullable=False, index=True)
    unidade_sei_id = db.Column(db.String(50), nullable=False)
    sigla = db.Column(db.String(255), nullable=False)
    descricao = db.Column(db.String(500), nullable=True)

    usuario = db.relationship('Usuario', back_populates='unidades_sei')

    __table_args__ = (
        db.UniqueConstraint('usuario_id', 'unidade_sei_id', name='uq_usuario_unidade_sei'),
    )

    def __repr__(self):
        return f'<UsuarioUnidadeSei {self.usuario_id} - {self.sigla}>'


@login_manager.user_loader
def load_user(user_id):
    """Callback para carregar usuário na sessão."""
    return Usuario.query.get(int(user_id))
