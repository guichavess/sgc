"""
Repositório para Prestacao (Execuções).
"""
from app.repositories.base import BaseRepository
from app.models.prestacao import Prestacao
from app.models.contrato import Contrato
from app.models.usuario import Usuario
from app.extensions import db


class PrestacaoRepository(BaseRepository[Prestacao]):
    model = Prestacao

    @classmethod
    def listar_com_detalhes(cls):
        """Lista todas as execuções com contrato e nome do criador."""
        return (
            db.session.query(
                Prestacao,
                Contrato.objeto.label('contrato_objeto'),
                Usuario.nome.label('nome_criador')
            )
            .outerjoin(Contrato, Contrato.codigo == Prestacao.codigo_contrato)
            .outerjoin(Usuario, Usuario.id == Prestacao.usuario_id)
            .order_by(Prestacao.data.desc())
            .all()
        )

    @classmethod
    def listar_com_detalhes_paginado(cls, page=1, per_page=20,
                                     filtro_contrato=None, filtro_competencia=None,
                                     filtro_item=None):
        """Lista execuções paginadas com contrato e nome do criador."""
        query = (
            db.session.query(
                Prestacao,
                Contrato.objeto.label('contrato_objeto'),
                Usuario.nome.label('nome_criador')
            )
            .outerjoin(Contrato, Contrato.codigo == Prestacao.codigo_contrato)
            .outerjoin(Usuario, Usuario.id == Prestacao.usuario_id)
        )
        if filtro_contrato:
            query = query.filter(Prestacao.codigo_contrato == filtro_contrato)
        if filtro_competencia:
            # competencia = MM/YYYY -> extract month/year from Prestacao.data
            parts = filtro_competencia.split('/')
            if len(parts) == 2:
                mes, ano = parts
                query = query.filter(
                    db.extract('month', Prestacao.data) == int(mes),
                    db.extract('year', Prestacao.data) == int(ano)
                )
        if filtro_item:
            query = query.filter(Prestacao.itens_contrato_id == int(filtro_item))
        query = query.order_by(Prestacao.data.desc())
        return query.paginate(page=page, per_page=per_page, error_out=False)

    @classmethod
    def contratos_distintos(cls):
        """Retorna lista de códigos de contrato distintos."""
        rows = db.session.query(Prestacao.codigo_contrato).distinct().order_by(
            Prestacao.codigo_contrato
        ).all()
        return [r[0] for r in rows if r[0]]

    @classmethod
    def competencias_distintas(cls):
        """Retorna lista de competências (MM/YYYY) distintas."""
        rows = db.session.query(
            db.extract('month', Prestacao.data).label('mes'),
            db.extract('year', Prestacao.data).label('ano')
        ).distinct().order_by(
            db.extract('year', Prestacao.data).desc(),
            db.extract('month', Prestacao.data).desc()
        ).all()
        return [f'{int(r.mes):02d}/{int(r.ano)}' for r in rows if r.mes and r.ano]

    @classmethod
    def itens_contrato_distintos(cls):
        """Retorna lista de itens de contrato distintos com descrição."""
        from app.models.item_contrato import ItemContrato
        rows = (
            db.session.query(ItemContrato.id, ItemContrato.descricao)
            .join(Prestacao, Prestacao.itens_contrato_id == ItemContrato.id)
            .distinct()
            .order_by(ItemContrato.descricao)
            .all()
        )
        return [{'id': r.id, 'descricao': r.descricao} for r in rows]
