"""
Modelos para a estrutura hierárquica de setores/superintendências da SEAD.

Hierarquia:
    Secretaria (132) → Superintendência (nível 8) → Diretoria (9) → Gerência (10) → Coordenação (11)

Casos especiais:
    - Gabinete (96): tratado como "pseudo-superintendência" para abrigar
      entidades sem superintendência (NCI, SE, OS, Assessoria de Comunicação).
"""
from app.extensions import db


# Tipos de entidade reconhecidos (codtipoentidade do CSV)
TIPO_ENTIDADE_SECRETARIA = 6
TIPO_ENTIDADE_SUPERINTENDENCIA = 8
TIPO_ENTIDADE_DIRETORIA = 9
TIPO_ENTIDADE_GERENCIA = 10
TIPO_ENTIDADE_COORDENACAO = 11
TIPO_ENTIDADE_GABINETE = 14
TIPO_ENTIDADE_ASSESSORIA = 24

# ID especial: Gabinete usado como "pseudo-superintendência" para setores
# administrativos diretos (NCI, SE, Ouvidoria, Assessoria de Comunicação).
ID_GABINETE = 96


class TipoEntidade(db.Model):
    """Tipo de entidade organizacional (Superintendência, Diretoria, etc.)."""
    __tablename__ = 'tipos_entidade'

    codtipoentidade = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(100), nullable=False)
    nivel = db.Column(db.Integer, nullable=False, default=0)
    nome2 = db.Column(db.String(100), nullable=True)

    def __repr__(self):
        return f'<TipoEntidade {self.codtipoentidade}: {self.nome}>'


class SetorSead(db.Model):
    """Entidade organizacional (setor/unidade) com hierarquia.

    Mapeamento do CSV entidades.csv:
        id              ← identidade
        nome            ← nome
        sigla           ← sigla
        parent_id       ← codentidadepai (FK self-referencing)
        superintendencia_id ← idsuperintendencia (FK self-referencing, denormalizado)
        tipo_entidade_id ← codtipoentidade

    Campo `superintendencia_id` aceita valor ID_GABINETE (96) para entidades
    sem superintendência mas subordinadas ao Gabinete, o que permite o mesmo
    filtro de dropdown em cascata na interface.
    """
    __tablename__ = 'setores'

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False, index=True)
    sigla = db.Column(db.String(100), nullable=True, index=True)

    parent_id = db.Column(db.Integer,
                          db.ForeignKey('setores.id', ondelete='SET NULL'),
                          nullable=True, index=True)
    superintendencia_id = db.Column(db.Integer,
                                    db.ForeignKey('setores.id', ondelete='SET NULL'),
                                    nullable=True, index=True)
    tipo_entidade_id = db.Column(db.Integer,
                                 db.ForeignKey('tipos_entidade.codtipoentidade'),
                                 nullable=False, index=True)
    orgao_id = db.Column(db.Integer, nullable=True, default=1)
    ativo = db.Column(db.Boolean, default=True, nullable=False, index=True)

    # Relações self-referencing
    parent = db.relationship('SetorSead', remote_side='SetorSead.id',
                             foreign_keys=[parent_id],
                             backref=db.backref('filhos', lazy='dynamic'))
    superintendencia = db.relationship('SetorSead', remote_side='SetorSead.id',
                                       foreign_keys=[superintendencia_id])
    tipo_entidade = db.relationship('TipoEntidade', lazy='joined')

    @property
    def is_superintendencia(self):
        """Verifica se este setor é uma Superintendência."""
        return self.tipo_entidade_id == TIPO_ENTIDADE_SUPERINTENDENCIA

    @property
    def is_gabinete(self):
        """Verifica se este setor é o Gabinete (pseudo-superintendência)."""
        return self.id == ID_GABINETE

    @property
    def nome_exibicao(self):
        """Nome para exibição em dropdowns: 'SIGLA - Nome'."""
        if self.sigla:
            return f'{self.sigla} - {self.nome}'
        return self.nome

    def __repr__(self):
        return f'<Setor {self.id}: {self.sigla or self.nome}>'
