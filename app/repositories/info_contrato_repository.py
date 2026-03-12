"""
Repositório para Contratos (usado pelo módulo Prestações de Contratos).
Reutiliza o model Contrato existente (tabela 'contratos').
"""
from typing import Optional, List
from sqlalchemy import func
from sqlalchemy.orm import joinedload

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
        query = db.session.query(Contrato).options(
            joinedload(Contrato.centro_de_custo),
            joinedload(Contrato.nat_despesa),
        )

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

        # Filtro por situação — aceita string ou lista
        if situacao:
            items = situacao if isinstance(situacao, list) else [situacao]
            items = [s for s in items if s]
            if items:
                query = query.filter(Contrato.situacao.in_(items))

        # Filtro por natureza (via Natureza dos empenho_itens vinculados) — aceita int ou lista
        if natureza_codigo:
            items = natureza_codigo if isinstance(natureza_codigo, list) else [natureza_codigo]
            items_str = [str(n) for n in items if n]
            if items_str:
                query = query.filter(
                    Contrato.codigo.in_(
                        db.session.query(EmpenhoItem.CodContrato)
                        .filter(EmpenhoItem.Natureza.in_(items_str))
                        .distinct()
                    )
                )

        # Filtro por tipo de execução — aceita int ou lista
        if tipo_execucao_id:
            items = tipo_execucao_id if isinstance(tipo_execucao_id, list) else [tipo_execucao_id]
            items = [i for i in items if i]
            if items:
                query = query.filter(Contrato.tipo_execucao_id.in_(items))

        # Filtro por centro de custo — aceita int ou lista
        if centro_de_custo_id:
            items = centro_de_custo_id if isinstance(centro_de_custo_id, list) else [centro_de_custo_id]
            items = [i for i in items if i]
            if items:
                query = query.filter(Contrato.centro_de_custo_id.in_(items))

        # Filtro por tipo de contrato (derivado da modalidade) — aceita string ou lista
        if tipo_contrato:
            tipos = tipo_contrato if isinstance(tipo_contrato, list) else [tipo_contrato]
            mapa_modalidades = {
                'SERVICO': ('SERVICOS', 'ALUGUEIS_IMOVEIS', 'ALUGUEIS'),
                'MATERIAL': ('FORNECIMENTO_MATERIAIS',),
                'MISTO': ('FORNECIMENTO_BENS',),
            }
            todas_modalidades = []
            for tc in tipos:
                mods = mapa_modalidades.get(tc, ())
                todas_modalidades.extend(mods)
            if todas_modalidades:
                query = query.filter(Contrato.modalidade.in_(todas_modalidades))

        # Filtro por PDM (CATMAT) — aceita int ou lista
        if pdm_id:
            items = pdm_id if isinstance(pdm_id, list) else [pdm_id]
            items = [i for i in items if i]
            if items:
                query = query.filter(Contrato.catmat_pdm_id.in_(items))

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
    def listar_codigos_filtrados(
        cls,
        codigo=None, contratado=None, situacao=None,
        natureza_codigo=None, tipo_execucao_id=None,
        centro_de_custo_id=None, tipo_contrato=None,
        pdm_id=None, subitem_despesa=None, tipo_patrimonial=None,
        **kwargs
    ):
        """Retorna apenas os códigos dos contratos filtrados (query leve, sem carregar objetos)."""
        query = db.session.query(Contrato.codigo)

        if codigo:
            query = query.filter(Contrato.codigo.ilike(f'%{codigo}%'))
        if contratado:
            termo = f'%{contratado}%'
            query = query.filter(db.or_(
                Contrato.nomeContratado.ilike(termo),
                Contrato.nomeContratadoResumido.ilike(termo)
            ))
        if situacao:
            items = situacao if isinstance(situacao, list) else [situacao]
            items = [s for s in items if s]
            if items:
                query = query.filter(Contrato.situacao.in_(items))
        if natureza_codigo:
            items = natureza_codigo if isinstance(natureza_codigo, list) else [natureza_codigo]
            items_str = [str(n) for n in items if n]
            if items_str:
                query = query.filter(Contrato.codigo.in_(
                    db.session.query(EmpenhoItem.CodContrato)
                    .filter(EmpenhoItem.Natureza.in_(items_str)).distinct()
                ))
        if tipo_execucao_id:
            items = tipo_execucao_id if isinstance(tipo_execucao_id, list) else [tipo_execucao_id]
            items = [i for i in items if i]
            if items:
                query = query.filter(Contrato.tipo_execucao_id.in_(items))
        if centro_de_custo_id:
            items = centro_de_custo_id if isinstance(centro_de_custo_id, list) else [centro_de_custo_id]
            items = [i for i in items if i]
            if items:
                query = query.filter(Contrato.centro_de_custo_id.in_(items))
        if tipo_contrato:
            tipos = tipo_contrato if isinstance(tipo_contrato, list) else [tipo_contrato]
            mapa_modalidades = {
                'SERVICO': ('SERVICOS', 'ALUGUEIS_IMOVEIS', 'ALUGUEIS'),
                'MATERIAL': ('FORNECIMENTO_MATERIAIS',),
                'MISTO': ('FORNECIMENTO_BENS',),
            }
            todas_modalidades = []
            for tc in tipos:
                todas_modalidades.extend(mapa_modalidades.get(tc, ()))
            if todas_modalidades:
                query = query.filter(Contrato.modalidade.in_(todas_modalidades))
        if pdm_id:
            items = pdm_id if isinstance(pdm_id, list) else [pdm_id]
            items = [i for i in items if i]
            if items:
                query = query.filter(Contrato.catmat_pdm_id.in_(items))

        EXCLUDE_NATUREZA = {'339092', '449092'}
        if subitem_despesa:
            items = subitem_despesa if isinstance(subitem_despesa, list) else [subitem_despesa]
            sub_conditions = [EmpenhoItem.SubItemDespesa.contains(s) for s in items]
            query = query.filter(Contrato.codigo.in_(
                db.session.query(EmpenhoItem.CodContrato)
                .filter(db.or_(*sub_conditions), EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA))
                .distinct()
            ))
        if tipo_patrimonial:
            items = tipo_patrimonial if isinstance(tipo_patrimonial, list) else [tipo_patrimonial]
            tp_conditions = [EmpenhoItem.TipoPatrimonial.contains(t) for t in items]
            query = query.filter(Contrato.codigo.in_(
                db.session.query(EmpenhoItem.CodContrato)
                .filter(db.or_(*tp_conditions), EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA))
                .distinct()
            ))

        return [r[0] for r in query.all()]

    @classmethod
    def listar_situacoes_distintas(cls) -> List[str]:
        """Retorna lista de situações únicas dos contratos."""
        result = db.session.query(
            Contrato.situacao
        ).distinct().filter(
            Contrato.situacao.isnot(None)
        ).order_by(Contrato.situacao).all()
        return [s[0] for s in result if s[0]]
