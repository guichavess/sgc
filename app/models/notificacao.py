"""
Models do sistema de notificacoes.
"""
from datetime import datetime
from app.extensions import db


class NotificacaoTipo(db.Model):
    """Catalogo de tipos de notificacao (seed, nao editavel pelo usuario)."""

    __tablename__ = 'notificacao_tipos'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.String(50), unique=True, nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    nome = db.Column(db.String(150), nullable=False)
    descricao = db.Column(db.Text, nullable=True)
    nivel = db.Column(
        db.Enum('silenciosa', 'lembrete', 'alerta', 'critica', name='nivel_notificacao'),
        nullable=False, default='lembrete'
    )
    canal_in_app = db.Column(db.Boolean, nullable=False, default=True)
    canal_email = db.Column(db.Boolean, nullable=False, default=False)
    canal_telegram = db.Column(db.Boolean, nullable=False, default=False)
    periodicidade = db.Column(db.String(30), nullable=True)
    ativo = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, default=datetime.now)

    def __repr__(self):
        return f'<NotificacaoTipo {self.codigo}>'


class Notificacao(db.Model):
    """Instancia de notificacao enviada a um usuario."""

    __tablename__ = 'notificacoes'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    tipo_id = db.Column(db.Integer, db.ForeignKey('notificacao_tipos.id'), nullable=False)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=False)
    titulo = db.Column(db.String(255), nullable=False)
    mensagem = db.Column(db.Text, nullable=False)
    nivel = db.Column(
        db.Enum('silenciosa', 'lembrete', 'alerta', 'critica', name='nivel_notificacao'),
        nullable=False
    )
    ref_modulo = db.Column(db.String(50), nullable=True)
    ref_id = db.Column(db.String(50), nullable=True)
    ref_url = db.Column(db.String(500), nullable=True)
    lida = db.Column(db.Boolean, nullable=False, default=False)
    lida_em = db.Column(db.DateTime, nullable=True)
    descartada = db.Column(db.Boolean, nullable=False, default=False)
    descartada_em = db.Column(db.DateTime, nullable=True)
    enviada_email = db.Column(db.Boolean, nullable=False, default=False)
    enviada_telegram = db.Column(db.Boolean, nullable=False, default=False)
    erro_envio = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.now)
    expires_at = db.Column(db.DateTime, nullable=True)

    # Relationships
    tipo = db.relationship('NotificacaoTipo', backref='notificacoes', lazy=True)
    usuario = db.relationship('Usuario', backref='notificacoes', lazy=True)

    __table_args__ = (
        db.Index('idx_notificacoes_usuario_lida', 'usuario_id', 'lida', 'created_at'),
        db.Index('idx_notificacoes_ref', 'ref_modulo', 'ref_id'),
    )

    def __repr__(self):
        return f'<Notificacao {self.id} - {self.titulo[:30]}>'

    @property
    def tempo_relativo(self):
        """Retorna tempo decorrido em formato legivel (ex: '2h', '3d')."""
        delta = datetime.now() - self.created_at
        segundos = int(delta.total_seconds())
        if segundos < 60:
            return 'agora'
        elif segundos < 3600:
            return f'{segundos // 60}min'
        elif segundos < 86400:
            return f'{segundos // 3600}h'
        else:
            return f'{segundos // 86400}d'


class NotificacaoCriticaConfirmacao(db.Model):
    """Registro de confirmacao de notificacao critica via CPF."""

    __tablename__ = 'notificacao_critica_confirmacoes'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    notificacao_id = db.Column(db.BigInteger, db.ForeignKey('notificacoes.id'), nullable=False)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=False)
    cpf_informado = db.Column(db.String(11), nullable=False)
    confirmada_em = db.Column(db.DateTime, nullable=False, default=datetime.now)

    # Relationships
    notificacao = db.relationship('Notificacao', backref='confirmacoes', lazy=True)
    usuario = db.relationship('Usuario', backref='confirmacoes_criticas', lazy=True)

    __table_args__ = (
        db.UniqueConstraint('notificacao_id', 'usuario_id', name='uq_critica_usuario'),
    )

    def __repr__(self):
        return f'<ConfirmacaoCritica notif={self.notificacao_id} user={self.usuario_id}>'


class NotificacaoPreferencia(db.Model):
    """Preferencias de notificacao por usuario/tipo (opt-out granular)."""

    __tablename__ = 'notificacao_preferencias'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=False)
    tipo_id = db.Column(db.Integer, db.ForeignKey('notificacao_tipos.id'), nullable=False)
    canal_in_app = db.Column(db.Boolean, nullable=False, default=True)
    canal_email = db.Column(db.Boolean, nullable=False, default=True)
    canal_telegram = db.Column(db.Boolean, nullable=False, default=True)
    silenciado = db.Column(db.Boolean, nullable=False, default=False)

    # Relationships
    usuario = db.relationship('Usuario', backref='preferencias_notificacao', lazy=True)
    tipo = db.relationship('NotificacaoTipo', backref='preferencias', lazy=True)

    __table_args__ = (
        db.UniqueConstraint('usuario_id', 'tipo_id', name='uq_pref_usuario_tipo'),
    )

    def __repr__(self):
        return f'<Preferencia user={self.usuario_id} tipo={self.tipo_id}>'
