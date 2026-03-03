"""
Modelo de Prestacao - Prestações realizadas de contratos.
Usada pelo módulo de Prestações de Contratos.

Cada execução referencia um Serviço (CATSERV) ou Item de Material (CATMAT),
determinado pelo campo 'tipo' ('S' ou 'M').
"""
from app.extensions import db


class Prestacao(db.Model):
    """Modelo para prestações de contratos."""

    __tablename__ = 'execucoes'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    codigo_contrato = db.Column(db.String(50), db.ForeignKey('contratos.codigo'), nullable=False)
    quantidade = db.Column(db.Integer, nullable=False, default=1)
    valor = db.Column(db.Numeric(15, 2), nullable=False)
    data = db.Column(db.Date, nullable=False)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)

    # Tipo da execução: 'S' (Serviço) ou 'M' (Material)
    tipo = db.Column(db.String(1), nullable=False, default='S')

    # FK para CATSERV (preenchido quando tipo='S')
    catserv_servico_id = db.Column(db.Integer, nullable=True)

    # FK para CATMAT (preenchido quando tipo='M')
    catmat_item_id = db.Column(db.Integer, nullable=True)

    # FK para itens_vinculados (vinculação que originou esta execução)
    item_vinculado_id = db.Column(
        db.Integer,
        db.ForeignKey('itens_vinculados.id', ondelete='SET NULL'),
        nullable=True
    )

    # FK legada para itens_contrato (dados originais do sistema PHP)
    itens_contrato_id = db.Column(db.Integer, db.ForeignKey('itens_contrato.id'), nullable=True)

    # Relacionamento com o usuário do sistema
    usuario = db.relationship('Usuario', backref='prestacoes', lazy=True)
    item_vinculado = db.relationship('ItemVinculado', backref='execucoes', lazy=True)
    item_contrato = db.relationship('ItemContrato', lazy=True)

    def __repr__(self):
        return f'<Prestacao {self.id} - Contrato {self.codigo_contrato} - Tipo {self.tipo}>'

    @property
    def tipo_display(self):
        """Retorna o tipo formatado para exibição."""
        return 'Serviço' if self.tipo == 'S' else 'Material'

    @property
    def item_descricao(self):
        """Retorna a descrição do item (serviço ou material).

        Prioridade:
        1. Catálogo CATSERV/CATMAT (via catserv_servico_id / catmat_item_id)
        2. Legado: itens_contrato (via itens_contrato_id do sistema PHP)
        """
        if self.tipo == 'S' and self.catserv_servico_id:
            from app.models.catserv import CatservServico
            servico = db.session.get(CatservServico, self.catserv_servico_id)
            return servico.nome if servico else 'Serviço não encontrado'
        elif self.tipo == 'M' and self.catmat_item_id:
            from app.models.catmat import CatmatItem
            item = db.session.get(CatmatItem, self.catmat_item_id)
            return item.descricao if item else 'Material não encontrado'
        # Fallback: dados legados do PHP (itens_contrato_id)
        if self.item_contrato:
            return self.item_contrato.descricao
        return 'N/A'

    @property
    def item_usuario_descricao(self):
        """Retorna a descrição do item de conhecimento do usuário (itens_contrato).

        Prioridade:
        1. Via vinculação: Prestacao → ItemVinculado → ItemContrato.descricao
        2. Legado: Prestacao → ItemContrato.descricao (itens_contrato_id do PHP)
        3. Catálogo CATSERV/CATMAT
        """
        # Via vinculação nova
        if self.item_vinculado and self.item_vinculado.item_contrato:
            return self.item_vinculado.item_contrato.descricao
        # Legado PHP: itens_contrato_id direto
        if self.item_contrato:
            return self.item_contrato.descricao
        # Fallback: nome do catálogo
        return self.item_descricao

    @property
    def valor_total(self):
        """Calcula o valor total (valor unitário x quantidade)."""
        return self.valor * self.quantidade if self.valor and self.quantidade else 0

    @property
    def valor_formatado(self):
        """Retorna o valor unitário formatado em moeda brasileira."""
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    @property
    def valor_total_formatado(self):
        """Retorna o valor total formatado em moeda brasileira."""
        total = self.valor_total
        return f'R$ {total:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
