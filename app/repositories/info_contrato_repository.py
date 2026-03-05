"""
Repositório para Contratos (usado pelo módulo Prestações de Contratos).
Reutiliza o model Contrato existente (tabela 'contratos').
"""
from typing import Optional, List
from sqlalchemy import func

from app.repositories.base import BaseRepository
from app.models.contrato import Contrato
from app.models.nat_despesa import NatDespesa
from app.models.empenho_item import EmpenhoItem
from app.extensions import db


class InfoContratoRepository(BaseRepository[Contrato]):
    model = Contrato

    @classmethod
    def get_by_codigo(cls, codigo):
        """Busca contrato pelo código."""
        return cls.model.query.filter_by(codigo=codigo).first()

    @classmethod
    def listar_com_filtros(
        cls,
        codigo: Optional[str] = None,
        contratado: Optional[str] = None,
        situacao: Optional[str] = None,
        natureza_codigo: Optional[int] = None,
        tipo_execucao_id: Optional[int] = None,
        centro_de_custo_id: Optional[int] = None,
        tipo_contrato: Optional[str] = None,
        pdm_id: Optional[int] = None,
        subitem_despesa: Optional[str] = None,
        tipo_patrimonial: Optional[str] = None,
        page: int = 1,
        per_page: int = 20
    ):
        """Lista contratos paginados com filtros."""
        query = db.session.query(Contrato)

        # Filtro por código do contrato
        if codigo:
            query = query.filter(Contrato.codigo.ilike(f'%{codigo}%'))

        # Filtro por nome do contratado
        if contratado:
            termo = f'%{contratado}%'
            query = query.filter(
                db.or_(
                    Contrato.nomeContratado.ilike(termo),
                    Contrato.nomeContratadoResumido.ilike(termo)
                )
            )

        # Filtro por situação
        if situacao:
            query = query.filter(Contrato.situacao == situacao)

        # Filtro por natureza (via Natureza dos empenho_itens vinculados)
        if natureza_codigo:
            query = query.filter(
                Contrato.codigo.in_(
                    db.session.query(EmpenhoItem.CodContrato)
                    .filter(EmpenhoItem.Natureza == str(natureza_codigo))
                    .distinct()
                )
            )

        # Filtro por tipo de execução
        if tipo_execucao_id:
            query = query.filter(Contrato.tipo_execucao_id == tipo_execucao_id)

        # Filtro por centro de custo
        if centro_de_custo_id:
            query = query.filter(Contrato.centro_de_custo_id == centro_de_custo_id)

        # Filtro por tipo de contrato (derivado da modalidade)
        if tipo_contrato:
            mapa_modalidades = {
                'SERVICO': ('SERVICOS', 'ALUGUEIS_IMOVEIS', 'ALUGUEIS'),
                'MATERIAL': ('FORNECIMENTO_MATERIAIS',),
                'MISTO': ('FORNECIMENTO_BENS',),
            }
            modalidades = mapa_modalidades.get(tipo_contrato)
            if modalidades:
                query = query.filter(Contrato.modalidade.in_(modalidades))

        # Filtro por PDM (CATMAT)
        if pdm_id:
            query = query.filter(Contrato.catmat_pdm_id == pdm_id)

        # Naturezas a excluir (estornos/cancelamentos)
        EXCLUDE_NATUREZA = {'339092', '449092'}

        # Filtro por Sub-Item da Despesa (via empenho_itens) — aceita string ou lista
        if subitem_despesa:
            items = subitem_despesa if isinstance(subitem_despesa, list) else [subitem_despesa]
            sub_conditions = [EmpenhoItem.SubItemDespesa.contains(s) for s in items]
            query = query.filter(
                Contrato.codigo.in_(
                    db.session.query(EmpenhoItem.CodContrato)
                    .filter(
                        db.or_(*sub_conditions),
                        EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
                    )
                    .distinct()
                )
            )

        # Filtro por Tipo Patrimonial (via empenho_itens) — aceita string ou lista
        if tipo_patrimonial:
            items = tipo_patrimonial if isinstance(tipo_patrimonial, list) else [tipo_patrimonial]
            tp_conditions = [EmpenhoItem.TipoPatrimonial.contains(t) for t in items]
            query = query.filter(
                Contrato.codigo.in_(
                    db.session.query(EmpenhoItem.CodContrato)
                    .filter(
                        db.or_(*tp_conditions),
                        EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
                    )
                    .distinct()
                )
            )

        query = query.order_by(Contrato.codigo.desc())

        return query.paginate(page=page, per_page=per_page, error_out=False)

    @classmethod
    def listar_situacoes_distintas(cls) -> List[str]:
        """Retorna lista de situações únicas dos contratos."""
        result = db.session.query(
            Contrato.situacao
        ).distinct().filter(
            Contrato.situacao.isnot(None)
        ).order_by(Contrato.situacao).all()
        return [s[0] for s in result if s[0]]
