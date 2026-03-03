"""
Modelos do Catálogo de Materiais (CATMAT).
Hierarquia: Grupo > Classe > PDM > Item
Fonte: Banco MySQL remoto (sync via scripts/importar_catmat.py)

Tabelas remotas:
  sol_grupos  -> catmat_grupos
  sol_classes -> catmat_classes
  sol_pdms    -> catmat_pdms
  sol_itens   -> catmat_itens

Nota: Sem ForeignKey constraints no banco (são cópias locais de dados remotos).
Relationships usam primaryjoin explícito ligando por 'codigo'.
"""
from app.extensions import db


class CatmatGrupo(db.Model):
    __tablename__ = 'catmat_grupos'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.Integer, unique=True, nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    status = db.Column(db.Boolean, default=True)
    data_atualizacao = db.Column(db.DateTime, nullable=True)
    hash_row = db.Column(db.String(40), nullable=True)
    last_sync = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    classes = db.relationship(
        'CatmatClasse',
        primaryjoin='CatmatGrupo.codigo == foreign(CatmatClasse.codigo_grupo)',
        backref='grupo', lazy=True, viewonly=True
    )

    def __repr__(self):
        return f'<CatmatGrupo {self.codigo} - {self.nome}>'


class CatmatClasse(db.Model):
    __tablename__ = 'catmat_classes'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.Integer, unique=True, nullable=False)
    codigo_grupo = db.Column(db.Integer, nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    status = db.Column(db.Boolean, default=True)
    data_atualizacao = db.Column(db.DateTime, nullable=True)
    hash_row = db.Column(db.String(40), nullable=True)
    last_sync = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    pdms = db.relationship(
        'CatmatPdm',
        primaryjoin='CatmatClasse.codigo == foreign(CatmatPdm.codigo_classe)',
        backref='classe', lazy=True, viewonly=True
    )

    def __repr__(self):
        return f'<CatmatClasse {self.codigo} - {self.nome}>'


class CatmatPdm(db.Model):
    __tablename__ = 'catmat_pdms'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.Integer, unique=True, nullable=False)
    codigo_classe = db.Column(db.Integer, nullable=False)
    nome = db.Column(db.String(500), nullable=False)
    status = db.Column(db.Boolean, default=True)
    data_atualizacao = db.Column(db.DateTime, nullable=True)
    hash_row = db.Column(db.String(40), nullable=True)
    last_sync = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    itens = db.relationship(
        'CatmatItem',
        primaryjoin='CatmatPdm.codigo == foreign(CatmatItem.codigo_pdm)',
        backref='pdm', lazy=True, viewonly=True
    )

    def __repr__(self):
        return f'<CatmatPdm {self.codigo} - {self.nome}>'


class CatmatItem(db.Model):
    __tablename__ = 'catmat_itens'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.Integer, unique=True, nullable=False)
    codigo_pdm = db.Column(db.Integer, nullable=False)
    descricao = db.Column(db.Text, nullable=False)
    status = db.Column(db.Boolean, default=True)
    codigo_ncm = db.Column(db.String(20), nullable=True)
    descricao_ncm = db.Column(db.String(500), nullable=True)
    item_sustentavel = db.Column(db.Boolean, default=False)
    aplica_margem_preferencia = db.Column(db.Boolean, default=False)
    data_atualizacao = db.Column(db.DateTime, nullable=True)
    hash_row = db.Column(db.String(40), nullable=True)
    last_sync = db.Column(db.TIMESTAMP, server_default=db.text('CURRENT_TIMESTAMP'))

    def __repr__(self):
        return f'<CatmatItem {self.codigo} - {self.descricao[:50] if self.descricao else ""}>'
