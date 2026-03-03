"""
Modelos do Catálogo de Serviços (CATSERV).
Hierarquia: Seção > Divisão > Grupo > Classe > Serviço
Fonte: BASE CATSERV.xlsx (importado via scripts/importar_catserv.py)
"""
from app.extensions import db


class CatservSecao(db.Model):
    __tablename__ = 'catserv_secoes'

    codigo_secao = db.Column(db.Integer, primary_key=True, autoincrement=False)
    nome = db.Column(db.String(255), nullable=False)
    status = db.Column(db.Boolean, default=True)
    data_atualizacao = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    divisoes = db.relationship('CatservDivisao', backref='secao', lazy=True)

    def __repr__(self):
        return f'<CatservSecao {self.codigo_secao} - {self.nome}>'


class CatservDivisao(db.Model):
    __tablename__ = 'catserv_divisoes'

    codigo_divisao = db.Column(db.Integer, primary_key=True, autoincrement=False)
    codigo_secao = db.Column(db.Integer, db.ForeignKey('catserv_secoes.codigo_secao'), nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    grupos = db.relationship('CatservGrupo', backref='divisao', lazy=True)

    def __repr__(self):
        return f'<CatservDivisao {self.codigo_divisao} - {self.nome}>'


class CatservGrupo(db.Model):
    __tablename__ = 'catserv_grupos'

    codigo_grupo = db.Column(db.Integer, primary_key=True, autoincrement=False)
    codigo_divisao = db.Column(db.Integer, db.ForeignKey('catserv_divisoes.codigo_divisao'), nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    status = db.Column(db.Boolean, default=True)
    data_atualizacao = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    classes = db.relationship('CatservClasse', backref='grupo', lazy=True)
    servicos_diretos = db.relationship(
        'CatservServico',
        primaryjoin='and_(CatservServico.codigo_grupo == CatservGrupo.codigo_grupo, '
                    'CatservServico.codigo_classe == None)',
        lazy=True, viewonly=True
    )

    def __repr__(self):
        return f'<CatservGrupo {self.codigo_grupo} - {self.nome}>'


class CatservClasse(db.Model):
    __tablename__ = 'catserv_classes'

    codigo_classe = db.Column(db.Integer, primary_key=True, autoincrement=False)
    codigo_grupo = db.Column(db.Integer, db.ForeignKey('catserv_grupos.codigo_grupo'), nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    servicos = db.relationship('CatservServico', backref='classe', lazy=True,
                               foreign_keys='CatservServico.codigo_classe')

    def __repr__(self):
        return f'<CatservClasse {self.codigo_classe} - {self.nome}>'


class CatservServico(db.Model):
    __tablename__ = 'catserv_servicos'

    codigo_servico = db.Column(db.Integer, primary_key=True, autoincrement=False)
    codigo_classe = db.Column(db.Integer, db.ForeignKey('catserv_classes.codigo_classe'), nullable=True)
    codigo_grupo = db.Column(db.Integer, db.ForeignKey('catserv_grupos.codigo_grupo'), nullable=False)
    nome = db.Column(db.String(500), nullable=False)
    status = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    def __repr__(self):
        return f'<CatservServico {self.codigo_servico} - {self.nome}>'

    @property
    def hierarquia_completa(self):
        """Retorna a hierarquia completa do serviço como string."""
        partes = []
        if self.classe:
            grupo = self.classe.grupo
            divisao = grupo.divisao
            secao = divisao.secao
            partes = [secao.nome, divisao.nome, grupo.nome, self.classe.nome, self.nome]
        elif self.codigo_grupo:
            from app.models.catserv import CatservGrupo
            grupo = db.session.get(CatservGrupo, self.codigo_grupo)
            if grupo:
                divisao = grupo.divisao
                secao = divisao.secao
                partes = [secao.nome, divisao.nome, grupo.nome, self.nome]
        return ' > '.join(partes) if partes else self.nome
