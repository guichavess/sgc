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

    @property
    def is_active(self):
        """Retorna se o usuário está ativo."""
        return self.ativo

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


@login_manager.user_loader
def load_user(user_id):
    """Callback para carregar usuário na sessão."""
    return Usuario.query.get(int(user_id))
