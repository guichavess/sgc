"""
Modelo EmpenhoItem (tabela empenho_itens - dados normalizados do SIAFE).

Tabela populada pelo script atualizar_empenho.py.
Cada linha = 1 empenho com classificadores como colunas:
Fonte, Natureza, Contrato, TipoPatrimonial, SubItemDespesa, CodContrato.
"""
from app.extensions import db


class EmpenhoItem(db.Model):
    """Empenho normalizado com classificadores em colunas.

    Tabela criada pelo pandas to_sql() — sem coluna id.
    PK composta: (codigo, codigoUG, CodContrato).
    dataEmissao é TEXT no banco (formato ISO do pandas).
    """

    __tablename__ = 'empenho_itens'

    codigo = db.Column(db.Text, primary_key=True)         # Codigo da NE
    codigoUG = db.Column(db.Text, primary_key=True)       # Unidade Gestora
    CodContrato = db.Column(db.BigInteger, primary_key=True)  # Codigo do contrato SIAFE
    dataEmissao = db.Column(db.Text)                       # Data como texto (ISO)
    Fonte = db.Column(db.Text)
    Natureza = db.Column(db.Text)
    Contrato = db.Column(db.Text)
    TipoPatrimonial = db.Column(db.Text)      # Codigo(s) do tipo patrimonial (ex: "40")
    SubItemDespesa = db.Column(db.Text)        # Codigo(s) do sub-item (ex: "2399.01")

    @property
    def data_emissao_formatada(self):
        """Retorna dataEmissao formatada como dd/mm/aaaa."""
        if not self.dataEmissao:
            return None
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(str(self.dataEmissao).replace(' ', 'T').split('.')[0])
            return dt.strftime('%d/%m/%Y')
        except (ValueError, TypeError):
            return str(self.dataEmissao)[:10]

    def __repr__(self):
        return f'<EmpenhoItem {self.codigo}>'


class ClassTipoPatrimonial(db.Model):
    """Classificador de Tipo Patrimonial (codigoTipoClassificador=116)."""

    __tablename__ = 'class_tipopatrimonial'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    ano = db.Column(db.BigInteger)
    codigoTipoClassificador = db.Column(db.BigInteger)
    nomeTipoClassificador = db.Column(db.Text)
    nomeClassificador = db.Column(db.Text)
    valoresClassificador1 = db.Column(db.Text)  # Codigo do classificador (ex: "40")
    valoresClassificador2 = db.Column(db.Text)
    createdAt = db.Column(db.DateTime)

    def __repr__(self):
        return f'<ClassTipoPatrimonial {self.valoresClassificador1} - {self.nomeClassificador}>'


class ClassSubItemDespesa(db.Model):
    """Classificador de Sub-Item da Despesa (codigoTipoClassificador=162)."""

    __tablename__ = 'class_subitemdespesa'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    ano = db.Column(db.BigInteger)
    codigoTipoClassificador = db.Column(db.BigInteger)
    nomeTipoClassificador = db.Column(db.Text)
    nomeClassificador = db.Column(db.Text)
    valoresClassificador1 = db.Column(db.Text)  # Parte1 do codigo (ex: "2399")
    valoresClassificador2 = db.Column(db.Text)  # Parte2 do codigo (ex: "01")
    createdAt = db.Column(db.DateTime)

    def __repr__(self):
        return f'<ClassSubItemDespesa {self.valoresClassificador1}.{self.valoresClassificador2} - {self.nomeClassificador}>'
