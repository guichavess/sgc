from app.extensions import db
from datetime import datetime
from sqlalchemy.orm import deferred


class MunicipioPiaui(db.Model):
    __tablename__ = 'municipios_pi'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(db.String(100), nullable=False, unique=True)
    codigo_ibge = db.Column(db.String(10), nullable=False, unique=True)
    gentilico = db.Column(db.String(100), nullable=True)


# Categoria especial: registro é um veículo (adesivado), não um endereço físico.
# Fica em 1º na lista → 1ª opção do "Tipo Local" no popup de criação.
TIPO_LOCAL_VEICULO = 'Veículo'

TIPOS_LOCAL = [
    TIPO_LOCAL_VEICULO,
    'Espaço da Cidadania',
    'Sala da Cidadania',
]


class IdentidadeVisualLocal(db.Model):
    __tablename__ = 'identidade_visual_locais'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # nullable: registros do tipo "Veículo" não têm cidade (só placa).
    cidade = db.Column(db.String(100), nullable=True)
    # deferred: coluna não entra no SELECT padrão — seguro antes da migração
    municipio_id = deferred(db.Column(db.Integer, nullable=True))
    tipo_local = db.Column('tipo_local', db.String(200), nullable=False)
    endereco = db.Column(db.String(500), nullable=True)
    bairro = db.Column(db.String(200), nullable=True)
    cep = db.Column(db.String(10), nullable=True)
    # ── Campos exclusivos de "Veículo" (preenchidos via API DETRAN pela placa) ──
    # deferred: ficam fora do SELECT padrão da listagem — seguro rodar antes do
    # ALTER TABLE em produção (só são acessados ao renderizar um veículo, que só
    # passa a existir depois da migração).
    placa = deferred(db.Column(db.String(10), nullable=True))
    tipo_veiculo = deferred(db.Column(db.String(100), nullable=True))
    marca_modelo = deferred(db.Column(db.String(200), nullable=True))
    cor = deferred(db.Column(db.String(60), nullable=True))
    custo = db.Column(db.Numeric(12, 2), nullable=True)
    data_acao = db.Column(db.DateTime, nullable=True)
    arquivo_nome = db.Column(db.String(255), nullable=True)
    arquivo_caminho = db.Column(db.String(500), nullable=True)
    # deferred: colunas de auditoria — ficam fora do SELECT padrão para que a
    # listagem continue funcionando mesmo antes do ALTER TABLE em produção.
    criado_por_id = deferred(db.Column(db.BigInteger, nullable=True))
    atualizado_por_id = deferred(db.Column(db.BigInteger, nullable=True))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    arquivos = db.relationship('IdentidadeVisualArquivo', backref='local_ref',
                               lazy='dynamic', cascade='all, delete-orphan',
                               order_by='IdentidadeVisualArquivo.created_at.desc()')

    @property
    def is_veiculo(self):
        return self.tipo_local == TIPO_LOCAL_VEICULO

    @property
    def status(self):
        if self.data_acao is not None and self.arquivos.count() > 0:
            return 'REALIZADO'
        return 'PENDENTE'


class IdentidadeVisualArquivo(db.Model):
    __tablename__ = 'identidade_visual_arquivos'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    local_id = db.Column(db.Integer, db.ForeignKey('identidade_visual_locais.id'), nullable=False)
    nome_original = db.Column(db.String(255), nullable=False)
    nome_servidor = db.Column(db.String(500), nullable=False)
    tipo = db.Column(db.String(10), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class IdentidadeVisualLog(db.Model):
    """Auditoria de ações do módulo (criar/editar/excluir locais).

    Mantém `local_id` como inteiro simples (sem FK) para que o registro de
    exclusão sobreviva à remoção do local.
    """
    __tablename__ = 'identidade_visual_log'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    local_id = db.Column(db.Integer, nullable=True, index=True)
    acao = db.Column(db.String(20), nullable=False)  # CRIAR | EDITAR | EXCLUIR
    descricao = db.Column(db.String(500), nullable=True)
    usuario_id = db.Column(db.BigInteger, nullable=True, index=True)
    usuario_nome = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
