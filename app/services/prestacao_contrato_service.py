"""
Serviço para operações de negócio do módulo Prestações de Contratos.
"""
import re
from datetime import datetime
from app.extensions import db
from app.repositories.info_contrato_repository import InfoContratoRepository
from app.repositories.prestacao_repository import PrestacaoRepository
from app.models.saldo_contrato import SaldoContrato, SaldoContratoItem, MovimentacaoSaldo
# Models manuais mantidos para compatibilidade, mas não mais usados no financeiro
# from app.models.empenho_contrato import EmpenhoContrato, LiquidacaoContrato, PagamentoContrato


class PrestacaoContratoService:
    """Centraliza a lógica de negócio do módulo de prestações."""

    @staticmethod
    def preencher_natureza_contratos():
        """Preenche automaticamente natureza_id dos contratos que ainda não têm,
        buscando via cadeia: contratos.codigo → empenho.codContrato → empenho.codNatureza → natdespesas.codigo.
        Executado em batch antes da listagem."""
        from app.models.contrato import Contrato
        from app.models.empenho import Empenho
        from app.models.nat_despesa import NatDespesa
        from sqlalchemy import func

        # Contratos sem natureza_id
        contratos_sem = Contrato.query.filter(Contrato.natureza_id.is_(None)).all()
        if not contratos_sem:
            return 0

        codigos = [c.codigo for c in contratos_sem]

        # Buscar codNatureza por contrato (MIN para evitar ambiguidade)
        empenhos = db.session.query(
            Empenho.codContrato,
            func.min(Empenho.codNatureza).label('codNatureza')
        ).filter(
            Empenho.codContrato.in_(codigos),
            Empenho.codNatureza.isnot(None)
        ).group_by(Empenho.codContrato).all()

        if not empenhos:
            return 0

        # Mapear codContrato → codNatureza
        mapa_natureza = {str(e.codContrato): e.codNatureza for e in empenhos}

        # Buscar natdespesas por codigo
        cod_naturezas = list(set(mapa_natureza.values()))
        natdespesas = NatDespesa.query.filter(NatDespesa.codigo.in_(cod_naturezas)).all()
        mapa_nd = {nd.codigo: nd.id for nd in natdespesas}

        # Atualizar contratos
        atualizados = 0
        for contrato in contratos_sem:
            cod_nat = mapa_natureza.get(contrato.codigo)
            if cod_nat and cod_nat in mapa_nd:
                contrato.natureza_id = mapa_nd[cod_nat]
                atualizados += 1

        if atualizados:
            db.session.commit()

        return atualizados

    @staticmethod
    def listar_contratos_paginado(codigo=None, contratado=None, situacao=None,
                                   natureza_codigo=None, tipo_execucao_id=None,
                                   centro_de_custo_id=None, tipo_contrato=None,
                                   pdm_id=None, subitem_despesa=None,
                                   tipo_patrimonial=None, page=1, per_page=20):
        """Lista contratos paginados com filtros."""
        return InfoContratoRepository.listar_com_filtros(
            codigo=codigo,
            contratado=contratado,
            situacao=situacao,
            natureza_codigo=natureza_codigo,
            tipo_execucao_id=tipo_execucao_id,
            centro_de_custo_id=centro_de_custo_id,
            tipo_contrato=tipo_contrato,
            pdm_id=pdm_id,
            subitem_despesa=subitem_despesa,
            tipo_patrimonial=tipo_patrimonial,
            page=page,
            per_page=per_page
        )

    @staticmethod
    def listar_situacoes():
        """Lista situações distintas dos contratos."""
        return InfoContratoRepository.listar_situacoes_distintas()

    @staticmethod
    def buscar_contrato(codigo):
        """Busca um contrato pelo código."""
        return InfoContratoRepository.get_by_codigo(codigo)

    # ===== TIPIFICAÇÃO =====

    @staticmethod
    def tipificar_contrato(codigo_contrato, catserv_classe_id=None,
                           catmat_classe_id=None, catmat_pdm_id=None,
                           usuario_id=None):
        """
        Tipifica um contrato.
        CATSERV: até Classe / CATMAT: até PDM.

        Args:
            codigo_contrato: código do contrato
            catserv_classe_id: código da classe CATSERV (para SERVICO ou MISTO)
            catmat_classe_id: ID da classe CATMAT (para MATERIAL ou MISTO)
            catmat_pdm_id: ID do PDM CATMAT (para MATERIAL ou MISTO)
            usuario_id: ID do usuário que está tipificando
        """
        from app.models.contrato import Contrato

        contrato = db.session.get(Contrato, codigo_contrato)
        if not contrato:
            raise ValueError('Contrato não encontrado.')

        tipo = contrato.tipo_contrato

        # Validar dados conforme tipo
        if tipo == 'SERVICO' and not catserv_classe_id:
            raise ValueError('Classe CATSERV é obrigatória para contratos de Serviço.')
        if tipo in ('MATERIAL', 'MISTO'):
            if not catmat_classe_id:
                raise ValueError('Classe CATMAT é obrigatória para contratos de Material.')
            if not catmat_pdm_id:
                raise ValueError('PDM CATMAT é obrigatório para contratos de Material.')
        if tipo == 'MISTO' and not catserv_classe_id:
            raise ValueError('Classe CATSERV é obrigatória para contratos Mistos.')

        # Salvar tipificação
        if catserv_classe_id:
            contrato.catserv_classe_id = int(catserv_classe_id)
        if catmat_classe_id:
            contrato.catmat_classe_id = int(catmat_classe_id)
        if catmat_pdm_id:
            contrato.catmat_pdm_id = int(catmat_pdm_id)

        contrato.data_tipificacao = datetime.now()
        contrato.tipificado_por = usuario_id

        db.session.commit()
        return contrato

    @staticmethod
    def obter_tipificacao(codigo_contrato):
        """
        Retorna os dados de tipificação de um contrato com hierarquia completa.

        Returns:
            dict com hierarquias catserv e catmat, ou None se não tipificado.
        """
        from app.models.contrato import Contrato

        contrato = db.session.get(Contrato, codigo_contrato)
        if not contrato or not contrato.esta_tipificado:
            return None

        resultado = {
            'tipo_contrato': contrato.tipo_contrato_label,
            'tipo_contrato_display': contrato.tipo_contrato_display,
            'data_tipificacao': contrato.data_tipificacao,
            'catserv': None,
            'catmat': None
        }

        # Hierarquia CATSERV
        if contrato.catserv_classe_id:
            from app.models.catserv import CatservClasse
            classe = db.session.get(CatservClasse, contrato.catserv_classe_id)
            if classe:
                grupo = classe.grupo
                divisao = grupo.divisao
                secao = divisao.secao
                resultado['catserv'] = {
                    'secao': {'id': secao.codigo_secao, 'nome': secao.nome},
                    'divisao': {'id': divisao.codigo_divisao, 'nome': divisao.nome},
                    'grupo': {'id': grupo.codigo_grupo, 'nome': grupo.nome},
                    'classe': {'id': classe.codigo_classe, 'nome': classe.nome}
                }

        # Hierarquia CATMAT (Grupo → Classe → PDM)
        if contrato.catmat_classe_id:
            from app.models.catmat import CatmatClasse, CatmatPdm
            classe = db.session.get(CatmatClasse, contrato.catmat_classe_id)
            if classe and classe.grupo:
                grupo = classe.grupo
                catmat_data = {
                    'grupo': {'id': grupo.id, 'codigo': grupo.codigo, 'nome': grupo.nome},
                    'classe': {'id': classe.id, 'codigo': classe.codigo, 'nome': classe.nome},
                    'pdm': None
                }
                # PDM tipificado
                if contrato.catmat_pdm_id:
                    pdm = db.session.get(CatmatPdm, contrato.catmat_pdm_id)
                    if pdm:
                        catmat_data['pdm'] = {
                            'id': pdm.id, 'codigo': pdm.codigo, 'nome': pdm.nome
                        }
                resultado['catmat'] = catmat_data

        return resultado

    @staticmethod
    def listar_itens_disponiveis(contrato):
        """
        Lista itens disponíveis baseado na tipificação do contrato.

        Returns:
            dict com tipo_contrato e listas de servicos/materiais.
        """
        resultado = {
            'tipo': contrato.tipo_contrato_label,
            'servicos': [],
            'materiais': []
        }

        # Serviços (CATSERV)
        if contrato.catserv_classe_id and contrato.tipo_contrato_label in ('SERVICO', 'MISTO'):
            from app.models.catserv import CatservServico
            servicos = CatservServico.query.filter_by(
                codigo_classe=contrato.catserv_classe_id
            ).order_by(CatservServico.nome).all()
            resultado['servicos'] = [
                {'id': s.codigo_servico, 'nome': s.nome, 'tipo': 'S'}
                for s in servicos
            ]

        # Materiais (CATMAT): classe → PDMs → itens
        if contrato.catmat_classe_id and contrato.tipo_contrato_label in ('MATERIAL', 'MISTO'):
            from app.models.catmat import CatmatItem, CatmatPdm, CatmatClasse
            classe = db.session.get(CatmatClasse, contrato.catmat_classe_id)
            if classe:
                pdm_codigos = db.session.query(CatmatPdm.codigo).filter(
                    CatmatPdm.codigo_classe == classe.codigo
                ).subquery()
                itens = CatmatItem.query.filter(
                    CatmatItem.codigo_pdm.in_(pdm_codigos)
                ).order_by(CatmatItem.descricao).limit(200).all()
                resultado['materiais'] = [
                    {'id': i.id, 'nome': i.descricao, 'tipo': 'M'}
                    for i in itens
                ]

        return resultado

    # ===== VINCULAÇÃO DE ITENS =====

    @staticmethod
    def vincular_item(codigo_contrato, item_contrato_id, usuario_id):
        """
        Vincula um item da itens_contrato ao contrato.
        Se o item tem de-para (CATSERV/CATMAT), preenche automaticamente
        os campos catserv_servico_id / catmat_item_id.

        Args:
            codigo_contrato: código do contrato
            item_contrato_id: ID do item na tabela itens_contrato
            usuario_id: ID do usuário

        Returns:
            tupla (success: bool, message: str, vinculacao_id: int ou None)
        """
        from app.models.contrato import Contrato
        from app.models.item_vinculado import ItemVinculado
        from app.models.item_contrato import ItemContrato

        contrato = db.session.get(Contrato, codigo_contrato)
        if not contrato:
            return False, 'Contrato não encontrado.', None

        item_ct = db.session.get(ItemContrato, int(item_contrato_id))
        if not item_ct:
            return False, 'Item de contrato não encontrado.', None

        # Verificar duplicata por item_contrato_id
        existente = ItemVinculado.query.filter_by(
            codigo_contrato=codigo_contrato,
            item_contrato_id=int(item_contrato_id)
        ).first()
        if existente:
            return False, 'Este item já está vinculado ao contrato.', None

        # Determinar tipo e IDs CATSERV/CATMAT via de-para
        tipo = None
        catserv_id = None
        catmat_id = None

        if item_ct.catserv_servico_id:
            tipo = 'S'
            catserv_id = item_ct.catserv_servico_id
        elif item_ct.catmat_item_id:
            tipo = 'M'
            catmat_id = item_ct.catmat_item_id

        # Se não tem de-para, usar tipo genérico baseado no tipo_item
        if not tipo:
            if item_ct.tipo_item and 'material' in item_ct.tipo_item.lower():
                tipo = 'M'
            else:
                tipo = 'S'  # Default para serviço

        # Criar vinculação
        try:
            vinculacao = ItemVinculado(
                codigo_contrato=codigo_contrato,
                tipo=tipo,
                catserv_servico_id=catserv_id,
                catmat_item_id=catmat_id,
                item_contrato_id=int(item_contrato_id),
                vinculado_por=usuario_id
            )
            db.session.add(vinculacao)

            # Auto-tipificação: se o contrato não está tipificado e o item
            # tem de-para, subir a hierarquia e preencher automaticamente.
            tipificou = PrestacaoContratoService._auto_tipificar_contrato(
                contrato, catserv_id, catmat_id
            )

            db.session.commit()

            msg = 'Item vinculado com sucesso.'
            if tipificou:
                msg += ' Contrato tipificado automaticamente.'
            return True, msg, vinculacao.id
        except Exception as e:
            db.session.rollback()
            return False, f'Erro ao vincular item: {str(e)}', None

    @staticmethod
    def _auto_tipificar_contrato(contrato, catserv_servico_id=None, catmat_item_id=None):
        """
        Auto-tipifica o contrato subindo a hierarquia a partir do de-para.
        Só atua se o contrato ainda NÃO está tipificado.

        CATSERV: servico -> classe (ou grupo -> primeira classe do grupo)
        CATMAT:  item -> pdm -> classe

        Args:
            contrato: instância do Contrato (já em sessão)
            catserv_servico_id: código do serviço CATSERV (ou None)
            catmat_item_id: id do item CATMAT (ou None)

        Returns:
            bool: True se tipificou, False se não (já tipificado ou sem dados)
        """
        from datetime import datetime

        # Já tipificado? Não sobrescrever
        if contrato.catserv_classe_id or contrato.catmat_classe_id:
            return False

        tipificou = False

        # --- CATSERV: servico -> classe ---
        if catserv_servico_id:
            from app.models.catserv import CatservServico, CatservClasse
            servico = db.session.get(CatservServico, catserv_servico_id)
            if servico:
                if servico.codigo_classe:
                    # Serviço tem classe direta
                    contrato.catserv_classe_id = servico.codigo_classe
                    tipificou = True
                elif servico.codigo_grupo:
                    # Serviço sem classe — pegar primeira classe do grupo
                    primeira_classe = CatservClasse.query.filter_by(
                        codigo_grupo=servico.codigo_grupo
                    ).order_by(CatservClasse.codigo_classe).first()
                    if primeira_classe:
                        contrato.catserv_classe_id = primeira_classe.codigo_classe
                        tipificou = True

        # --- CATMAT: item -> pdm -> classe ---
        if catmat_item_id:
            from app.models.catmat import CatmatItem, CatmatPdm, CatmatClasse
            item_mat = db.session.get(CatmatItem, catmat_item_id)
            if item_mat and item_mat.codigo_pdm:
                pdm = CatmatPdm.query.filter_by(codigo=item_mat.codigo_pdm).first()
                if pdm:
                    classe = CatmatClasse.query.filter_by(codigo=pdm.codigo_classe).first()
                    if classe:
                        contrato.catmat_pdm_id = pdm.id
                        contrato.catmat_classe_id = classe.id
                        tipificou = True

        if tipificou:
            contrato.data_tipificacao = datetime.now()

        return tipificou

    @staticmethod
    def desvincular_item(vinculacao_id):
        """
        Remove vinculação de um item do contrato.

        Returns:
            tupla (success: bool, message: str, warning: str ou None)
        """
        from app.models.item_vinculado import ItemVinculado
        from app.models.prestacao import Prestacao

        vinculacao = db.session.get(ItemVinculado, vinculacao_id)
        if not vinculacao:
            return False, 'Vinculação não encontrada.', None

        # Verificar se há execuções usando este item
        query = Prestacao.query.filter_by(
            codigo_contrato=vinculacao.codigo_contrato,
            tipo=vinculacao.tipo
        )
        if vinculacao.tipo == 'S':
            query = query.filter_by(catserv_servico_id=vinculacao.catserv_servico_id)
        else:
            query = query.filter_by(catmat_item_id=vinculacao.catmat_item_id)

        contagem = query.count()
        warning = None
        if contagem > 0:
            warning = f'Atenção: este item está em uso em {contagem} execução(ões).'

        # Remover vinculação
        try:
            db.session.delete(vinculacao)
            db.session.commit()
            return True, 'Item desvinculado com sucesso.', warning
        except Exception as e:
            db.session.rollback()
            return False, f'Erro ao desvincular: {str(e)}', None

    @staticmethod
    def listar_itens_vinculados(codigo_contrato, tipo=None):
        """
        Lista itens vinculados a um contrato.
        Retorna dados do item_contrato (nome que a usuária conhece) +
        dados do CATMAT/CATSERV associado (quando de-para existir).

        Args:
            codigo_contrato: código do contrato
            tipo: opcional, 'S' ou 'M' para filtrar

        Returns:
            lista de dicts com dados dos itens vinculados.
        """
        from app.models.item_vinculado import ItemVinculado

        query = ItemVinculado.query.filter_by(codigo_contrato=codigo_contrato)
        if tipo:
            query = query.filter_by(tipo=tipo)
        query = query.order_by(ItemVinculado.data_vinculacao.desc())

        vinculacoes = query.all()
        resultado = []

        for v in vinculacoes:
            item_data = {
                'vinculacao_id': v.id,
                'tipo': v.tipo,
                'data_vinculacao': v.data_vinculacao,
                'item_contrato_descricao': None,
                'associacao': None,
            }

            # Dados do item_contrato (nome que a usuária conhece)
            if v.item_contrato:
                item_data['item_contrato_descricao'] = v.item_contrato.descricao
                item_data['item_contrato_id'] = v.item_contrato.id
                item_data['descricao'] = v.item_contrato.descricao

                # IDs técnicos CATSERV/CATMAT para o fluxo de execuções
                # Prioridade: IDs do item_vinculado > IDs do item_contrato (de-para)
                item_data['catserv_servico_id'] = v.catserv_servico_id
                item_data['catmat_item_id'] = v.catmat_item_id
                # item_id para compatibilidade (ID técnico CATSERV ou CATMAT)
                if v.tipo == 'S' and v.catserv_servico_id:
                    item_data['item_id'] = v.catserv_servico_id
                    item_data['codigo'] = v.catserv_servico_id
                elif v.tipo == 'M' and v.catmat_item_id:
                    # catmat_item_id armazena catmat_itens.id
                    from app.models.catmat import CatmatItem as CatmatItemModel
                    item_mat = db.session.get(CatmatItemModel, v.catmat_item_id)
                    item_data['item_id'] = item_mat.id if item_mat else v.catmat_item_id
                    item_data['codigo'] = item_mat.codigo if item_mat else v.catmat_item_id
                else:
                    # Sem de-para — usa item_contrato_id como referência
                    item_data['item_id'] = v.item_contrato.id
                    item_data['codigo'] = v.item_contrato.id
            else:
                # Fallback para vinculações antigas sem item_contrato
                if v.tipo == 'S' and v.servico:
                    item_data['item_id'] = v.servico.codigo_servico
                    item_data['codigo'] = v.servico.codigo_servico
                    item_data['descricao'] = v.servico.nome
                    item_data['catserv_servico_id'] = v.servico.codigo_servico
                    item_data['catmat_item_id'] = None
                elif v.tipo == 'M' and v.catmat_item_id:
                    # catmat_item_id armazena catmat_itens.id
                    from app.models.catmat import CatmatItem as CatmatItemModel
                    item_mat = db.session.get(CatmatItemModel, v.catmat_item_id)
                    if item_mat:
                        item_data['item_id'] = item_mat.id
                        item_data['codigo'] = item_mat.codigo
                        item_data['descricao'] = item_mat.descricao
                    else:
                        item_data['item_id'] = v.catmat_item_id
                        item_data['codigo'] = v.catmat_item_id
                        item_data['descricao'] = '(Item CATMAT não encontrado)'
                    item_data['catserv_servico_id'] = None
                    item_data['catmat_item_id'] = v.catmat_item_id
                else:
                    continue  # Pula vinculações órfãs

            # Dados de associação CATMAT/CATSERV (de-para)
            if v.tipo == 'S' and v.servico:
                item_data['associacao'] = f'{v.servico.codigo_servico} - {v.servico.nome}'
            elif v.tipo == 'M' and v.catmat_item_id:
                # catmat_item_id armazena catmat_itens.id
                from app.models.catmat import CatmatItem as CatmatItemModel
                item_mat = db.session.get(CatmatItemModel, v.catmat_item_id)
                if item_mat:
                    item_data['associacao'] = f'{item_mat.codigo} - {item_mat.descricao}'

            resultado.append(item_data)

        return resultado

    @staticmethod
    def listar_itens_para_execucao(contrato):
        """Lista itens vinculados filtrados pela tipificação do contrato.

        Somente retorna itens cujo tipo e classificação CATSERV/CATMAT
        correspondam à tipificação configurada no contrato.

        - SERVIÇO (S): serviços da classe (ou grupo) tipificada.
        - MATERIAL (M): materiais do PDM (ou classe) tipificado.
        - MISTO (SM): ambos filtrados por seus respectivos catálogos.

        Returns:
            tuple (itens_servicos, itens_materiais, total_ocultados)
        """
        from app.models.catserv import CatservServico
        from app.models.catmat import CatmatItem as CatmatItemModel, CatmatPdm

        tipo = contrato.tipo_contrato  # 'S', 'M', 'SM'
        if not tipo:
            # Sem tipo definido — fallback: retorna tudo
            todos = PrestacaoContratoService.listar_itens_vinculados(contrato.codigo)
            return (
                [i for i in todos if i['tipo'] == 'S'],
                [i for i in todos if i['tipo'] == 'M'],
                0
            )

        itens_servicos = []
        itens_materiais = []
        total_ocultados = 0

        # ── SERVIÇO: filtrar pela classe/grupo do contrato ──────────────
        if tipo in ('S', 'SM'):
            todos_servicos = PrestacaoContratoService.listar_itens_vinculados(
                contrato.codigo, tipo='S'
            )

            # Determinar serviços permitidos pela tipificação
            servicos_permitidos = None  # None = sem filtro
            if contrato.catserv_classe_id:
                rows = CatservServico.query.filter_by(
                    codigo_classe=contrato.catserv_classe_id
                ).with_entities(CatservServico.codigo_servico).all()
                servicos_permitidos = {r[0] for r in rows}
            elif contrato.catserv_grupo_id:
                rows = CatservServico.query.filter_by(
                    codigo_grupo=contrato.catserv_grupo_id
                ).with_entities(CatservServico.codigo_servico).all()
                servicos_permitidos = {r[0] for r in rows}

            for item in todos_servicos:
                srv_id = item.get('catserv_servico_id')
                if not srv_id:
                    total_ocultados += 1
                    continue  # Sem de-para → ocultar
                if servicos_permitidos is not None and srv_id not in servicos_permitidos:
                    total_ocultados += 1
                    continue  # Não pertence à tipificação
                itens_servicos.append(item)

        # ── MATERIAL: filtrar pelo PDM/classe do contrato ───────────────
        if tipo in ('M', 'SM'):
            todos_materiais = PrestacaoContratoService.listar_itens_vinculados(
                contrato.codigo, tipo='M'
            )

            # Determinar itens CATMAT permitidos pela tipificação
            itens_permitidos = None  # None = sem filtro
            if contrato.catmat_pdm_id:
                # catmat_pdm_id armazena CatmatPdm.id (autoincrement)
                pdm = db.session.get(CatmatPdm, contrato.catmat_pdm_id)
                if pdm:
                    rows = CatmatItemModel.query.filter_by(
                        codigo_pdm=pdm.codigo
                    ).with_entities(CatmatItemModel.id).all()
                    itens_permitidos = {r[0] for r in rows}
            elif contrato.catmat_classe_id:
                from app.models.catmat import CatmatClasse
                classe = db.session.query(CatmatClasse).filter_by(
                    id=contrato.catmat_classe_id
                ).first()
                if classe:
                    pdm_codigos = {p[0] for p in CatmatPdm.query.filter_by(
                        codigo_classe=classe.codigo
                    ).with_entities(CatmatPdm.codigo).all()}
                    rows = CatmatItemModel.query.filter(
                        CatmatItemModel.codigo_pdm.in_(pdm_codigos)
                    ).with_entities(CatmatItemModel.id).all()
                    itens_permitidos = {r[0] for r in rows}

            for item in todos_materiais:
                mat_id = item.get('catmat_item_id')
                if not mat_id:
                    total_ocultados += 1
                    continue  # Sem de-para → ocultar
                if itens_permitidos is not None and mat_id not in itens_permitidos:
                    total_ocultados += 1
                    continue  # Não pertence à tipificação
                itens_materiais.append(item)

        return itens_servicos, itens_materiais, total_ocultados

    @staticmethod
    def listar_catalogo_para_vincular(contrato):
        """Lista itens do catálogo CATSERV/CATMAT disponíveis para vincular.

        Mostra diretamente os itens dos catálogos conforme a tipificação:
        - SERVIÇO: serviços CATSERV da mesma classe (ou grupo) do contrato
        - MATERIAL: PDMs CATMAT da mesma classe do PDM do contrato
        - MISTO: combina ambos

        Retorna lista de dicts: [{tipo, id, codigo, descricao, ja_vinculado}]
        """
        from app.models.catmat import CatmatPdm, CatmatItem as CatmatItemModel
        from app.models.catserv import CatservServico
        from app.models.item_vinculado import ItemVinculado

        tipo = contrato.tipo_contrato_label  # 'SERVICO', 'MATERIAL', 'MISTO'
        resultado = []

        if not tipo:
            return resultado

        # Carregar vinculações existentes para marcar "já vinculado"
        vinculacoes = ItemVinculado.query.filter_by(
            codigo_contrato=contrato.codigo
        ).all()
        ids_serv_vinculados = {v.catserv_servico_id for v in vinculacoes if v.tipo == 'S' and v.catserv_servico_id}
        ids_mat_vinculados = {v.catmat_item_id for v in vinculacoes if v.tipo == 'M' and v.catmat_item_id}

        # ── SERVIÇOS: da mesma classe (ou grupo) ──
        if tipo in ('SERVICO', 'MISTO'):
            servicos = []
            if contrato.catserv_classe_id:
                servicos = CatservServico.query.filter_by(
                    codigo_classe=contrato.catserv_classe_id
                ).order_by(CatservServico.nome).all()
            elif contrato.catserv_grupo_id:
                servicos = CatservServico.query.filter_by(
                    codigo_grupo=contrato.catserv_grupo_id
                ).order_by(CatservServico.nome).all()

            for srv in servicos:
                resultado.append({
                    'tipo': 'S',
                    'id': srv.codigo_servico,
                    'codigo': srv.codigo_servico,
                    'descricao': srv.nome,
                    'ja_vinculado': srv.codigo_servico in ids_serv_vinculados,
                })

        # ── MATERIAIS: PDMs da mesma classe CATMAT ──
        if tipo in ('MATERIAL', 'MISTO'):
            pdms = []
            if contrato.catmat_pdm_id:
                # Resolver classe do PDM do contrato
                pdm_contrato = db.session.get(CatmatPdm, contrato.catmat_pdm_id)
                if pdm_contrato:
                    pdms = CatmatPdm.query.filter_by(
                        codigo_classe=pdm_contrato.codigo_classe
                    ).order_by(CatmatPdm.nome).all()

            for pdm in pdms:
                resultado.append({
                    'tipo': 'M',
                    'id': pdm.codigo,
                    'codigo': pdm.codigo,
                    'descricao': pdm.nome,
                    'ja_vinculado': pdm.codigo in ids_mat_vinculados,
                })

        return resultado

    @staticmethod
    def vincular_catalogo_item(codigo_contrato, tipo, catalogo_id, usuario_id):
        """Vincula um item do catálogo (serviço CATSERV ou PDM CATMAT) ao contrato.

        Args:
            codigo_contrato: código do contrato
            tipo: 'S' (serviço) ou 'M' (PDM material)
            catalogo_id: codigo_servico (CATSERV) ou codigo PDM (CATMAT)
            usuario_id: ID do usuário

        Returns:
            tupla (success: bool, message: str, vinculacao_id: int ou None)
        """
        from app.models.contrato import Contrato
        from app.models.item_vinculado import ItemVinculado

        contrato = db.session.get(Contrato, codigo_contrato)
        if not contrato:
            return False, 'Contrato não encontrado.', None

        catserv_id = catalogo_id if tipo == 'S' else None
        catmat_id = catalogo_id if tipo == 'M' else None

        # Verificar duplicata
        existente = ItemVinculado.query.filter_by(
            codigo_contrato=codigo_contrato,
            tipo=tipo,
            catserv_servico_id=catserv_id,
            catmat_item_id=catmat_id,
        ).first()
        if existente:
            return False, 'Este item já está vinculado ao contrato.', None

        try:
            vinculacao = ItemVinculado(
                codigo_contrato=codigo_contrato,
                tipo=tipo,
                catserv_servico_id=catserv_id,
                catmat_item_id=catmat_id,
                item_contrato_id=None,
                vinculado_por=usuario_id,
            )
            db.session.add(vinculacao)
            db.session.commit()

            return True, 'Item vinculado com sucesso.', vinculacao.id
        except Exception as e:
            db.session.rollback()
            return False, f'Erro ao vincular item: {str(e)}', None

    # ===== EXECUÇÕES (Prestações) =====

    @staticmethod
    def listar_prestacoes():
        """Lista todas as execuções com detalhes."""
        return PrestacaoRepository.listar_com_detalhes()

    @staticmethod
    def listar_prestacoes_paginado(page=1, per_page=20, filtro_contrato=None,
                                    filtro_competencia=None, filtro_item=None):
        """Lista execuções paginadas com detalhes e filtros opcionais."""
        return PrestacaoRepository.listar_com_detalhes_paginado(
            page=page, per_page=per_page,
            filtro_contrato=filtro_contrato,
            filtro_competencia=filtro_competencia,
            filtro_item=filtro_item
        )

    @staticmethod
    def criar_prestacao(tipo, codigo_contrato, quantidade, valor_str, data, usuario_id,
                        catserv_servico_id=None, catmat_item_id=None):
        """
        Cria uma nova execução.

        Args:
            tipo: 'S' (Serviço) ou 'M' (Material)
            codigo_contrato: código do contrato
            quantidade: quantidade
            valor_str: valor em formato brasileiro (1.234,56)
            data: data da execução
            usuario_id: ID do usuário
            catserv_servico_id: ID do serviço CATSERV (quando tipo='S')
            catmat_item_id: ID do item CATMAT (quando tipo='M')
        """
        valor = PrestacaoContratoService._converter_valor_br(valor_str)

        if tipo == 'S' and not catserv_servico_id:
            raise ValueError('catserv_servico_id é obrigatório para tipo Serviço.')
        if tipo == 'M' and not catmat_item_id:
            raise ValueError('catmat_item_id é obrigatório para tipo Material.')

        return PrestacaoRepository.create(
            tipo=tipo,
            codigo_contrato=codigo_contrato,
            quantidade=quantidade,
            valor=valor,
            data=data,
            usuario_id=usuario_id,
            catserv_servico_id=catserv_servico_id if tipo == 'S' else None,
            catmat_item_id=catmat_item_id if tipo == 'M' else None
        )

    # ===== SALDO =====

    @staticmethod
    def buscar_saldo(codigo_contrato):
        """Busca o saldo de um contrato."""
        return SaldoContrato.query.filter_by(codigo_contrato=codigo_contrato).first()

    @staticmethod
    def criar_saldo(codigo_contrato, saldo_global, data_inicio, usuario_id):
        """Cria um saldo para o contrato."""
        valor = PrestacaoContratoService._converter_valor_br(saldo_global)
        saldo = SaldoContrato(
            codigo_contrato=codigo_contrato,
            saldo_global=valor,
            data_inicio=data_inicio or None,
            usuario_id=usuario_id
        )
        db.session.add(saldo)
        db.session.commit()
        return saldo

    @staticmethod
    def atualizar_saldo(saldo_id, saldo_global, data_inicio, usuario_id):
        """Atualiza o saldo de um contrato e registra movimentação."""
        saldo = SaldoContrato.query.get(saldo_id)
        if not saldo:
            return None

        valor_antigo = saldo.saldo_global or 0
        valor_novo = PrestacaoContratoService._converter_valor_br(saldo_global)
        diferenca = valor_novo - float(valor_antigo)

        saldo.saldo_global = valor_novo
        if data_inicio:
            saldo.data_inicio = data_inicio

        # Registra movimentação
        if diferenca != 0:
            mov = MovimentacaoSaldo(
                saldo_contrato_id=saldo.id,
                tipo='CREDITO' if diferenca > 0 else 'DEBITO',
                valor=abs(diferenca),
                descricao=f'Saldo atualizado de R$ {valor_antigo:,.2f} para R$ {valor_novo:,.2f}',
                usuario_id=usuario_id
            )
            db.session.add(mov)

        # Reajuste proporcional dos itens de saldo
        if diferenca != 0:
            PrestacaoContratoService._reajustar_divisao_proporcional(
                saldo.id, float(valor_antigo), valor_novo
            )

        db.session.commit()
        return saldo

    @staticmethod
    def buscar_divisao_saldo(saldo_id):
        """Retorna a divisão do saldo por item vinculado."""
        return SaldoContratoItem.query.filter_by(
            saldo_contrato_id=saldo_id
        ).all()

    @staticmethod
    def salvar_divisao_saldo(saldo_id, itens_valores):
        """
        Salva a divisão do saldo global por itens vinculados.
        itens_valores: dict {item_vinculado_id: valor_float}
        """
        for item_id, valor in itens_valores.items():
            existente = SaldoContratoItem.query.filter_by(
                saldo_contrato_id=saldo_id,
                item_vinculado_id=item_id
            ).first()
            if existente:
                existente.valor = valor
            else:
                novo = SaldoContratoItem(
                    saldo_contrato_id=saldo_id,
                    item_vinculado_id=item_id,
                    valor=valor
                )
                db.session.add(novo)
        db.session.commit()

    @staticmethod
    def _reajustar_divisao_proporcional(saldo_id, valor_antigo, valor_novo):
        """Reajusta a divisão do saldo proporcionalmente quando o saldo global muda."""
        itens = SaldoContratoItem.query.filter_by(saldo_contrato_id=saldo_id).all()
        if not itens or float(valor_antigo) == 0:
            return
        for item in itens:
            proporcao = float(item.valor) / float(valor_antigo)
            item.valor = round(proporcao * float(valor_novo), 2)

    # ===== FINANCEIRO (Empenhos, Liquidações, Pagamentos - dados SIAFE) =====

    @staticmethod
    def listar_empenhos(codigo_contrato):
        """Lista empenhos do SIAFE para o contrato via codContrato."""
        from app.models.empenho import Empenho
        try:
            cod = int(codigo_contrato)
        except (ValueError, TypeError):
            return []
        return Empenho.query.filter_by(
            codContrato=cod
        ).order_by(Empenho.dataEmissao.desc()).all()

    @staticmethod
    def listar_liquidacoes(codigo_contrato):
        """Lista liquidações do SIAFE para o contrato via codContrato."""
        from app.models.liquidacao import Liquidacao
        try:
            cod = int(codigo_contrato)
        except (ValueError, TypeError):
            return []
        return Liquidacao.query.filter_by(
            codContrato=cod
        ).order_by(Liquidacao.dataEmissao.desc()).all()

    @staticmethod
    def listar_pagamentos_contrato(codigo_contrato):
        """Lista OBs (pagamentos) do SIAFE para o contrato via codContrato."""
        from app.models.ob import OB
        return OB.query.filter_by(
            codContrato=str(codigo_contrato)
        ).order_by(OB.dataEmissao.desc()).all()

    @staticmethod
    def listar_pds(codigo_contrato):
        """Lista PDs (Programacao de Desembolso) do SIAFE para o contrato via codContrato."""
        from app.models.pd import PD
        try:
            cod = int(codigo_contrato)
        except (ValueError, TypeError):
            return []
        return PD.query.filter_by(
            codContrato=cod
        ).order_by(PD.dataEmissao.desc()).all()

    @staticmethod
    def listar_solicitacoes_contrato(codigo_contrato):
        """Busca solicitações de pagamento vinculadas ao contrato."""
        from app.models.solicitacao import Solicitacao, SolicitacaoEmpenho

        solicitacoes = Solicitacao.query.filter_by(
            codigo_contrato=str(codigo_contrato)
        ).order_by(Solicitacao.data_solicitacao.desc()).all()

        resultado = []
        for sol in solicitacoes:
            # Buscar empenho vinculado
            empenho = SolicitacaoEmpenho.query.filter_by(
                id_solicitacao=sol.id
            ).first()

            resultado.append({
                'id': sol.id,
                'protocolo': sol.protocolo_gerado_sei,
                'link_sei': sol.link_processo_sei,
                'etapa_nome': sol.etapa.nome if sol.etapa else 'Sem etapa',
                'etapa_cor': sol.etapa.cor_hex if sol.etapa and sol.etapa.cor_hex else '#6c757d',
                'valor': float(empenho.valor) if empenho and empenho.valor else 0,
                'ne': empenho.ne if empenho else None,
                'ano': sol.data_solicitacao.year if sol.data_solicitacao else None,
                'data': sol.data_solicitacao
            })

        return resultado

    # ===== CENTRO DE CUSTO E TIPO DE EXECUÇÃO =====

    @staticmethod
    def listar_centros_de_custo():
        """Lista todos os centros de custo disponíveis."""
        from app.models.centro_de_custo import CentroDeCusto
        return CentroDeCusto.query.order_by(CentroDeCusto.descricao).all()

    @staticmethod
    def salvar_centro_de_custo(codigo_contrato, centro_de_custo_id):
        """Salva o centro de custo do contrato."""
        from app.models.contrato import Contrato
        contrato = Contrato.query.get(codigo_contrato)
        if not contrato:
            raise ValueError('Contrato não encontrado.')
        contrato.centro_de_custo_id = centro_de_custo_id or None
        db.session.commit()

    @staticmethod
    def listar_tipos_execucao():
        """Lista todos os tipos de execução disponíveis."""
        from app.models.tipo_execucao import TipoExecucao
        return TipoExecucao.query.order_by(TipoExecucao.descricao).all()

    @staticmethod
    def salvar_tipo_execucao(codigo_contrato, tipo_execucao_id):
        """Salva o tipo de execução do contrato."""
        from app.models.contrato import Contrato
        contrato = Contrato.query.get(codigo_contrato)
        if not contrato:
            raise ValueError('Contrato não encontrado.')
        contrato.tipo_execucao_id = tipo_execucao_id or None
        db.session.commit()

    @staticmethod
    def listar_pdms_utilizados():
        """Lista PDMs que estão vinculados a pelo menos um contrato."""
        from app.models.contrato import Contrato
        from app.models.catmat import CatmatPdm

        pdm_ids = db.session.query(Contrato.catmat_pdm_id).filter(
            Contrato.catmat_pdm_id.isnot(None)
        ).distinct().all()
        ids = [r[0] for r in pdm_ids]
        if not ids:
            return []
        return CatmatPdm.query.filter(CatmatPdm.id.in_(ids)).order_by(CatmatPdm.nome).all()

    @staticmethod
    def buscar_naturezas_por_contratos(codigos_contratos):
        """Busca TODAS as naturezas distintas de cada contrato via empenho_itens.
        Retorna dict: {codigo_contrato: [{'codigo': 339039, 'titulo': '...', 'id_titulo': '...'}, ...]}
        """
        from app.models.empenho_item import EmpenhoItem
        from app.models.nat_despesa import NatDespesa

        if not codigos_contratos:
            return {}

        # Naturezas a excluir (estornos/cancelamentos)
        EXCLUDE_NATUREZA = {'339092', '449092'}

        # CodContrato é BIGINT → converter para int
        codigos_int = []
        for c in codigos_contratos:
            try:
                codigos_int.append(int(c))
            except (ValueError, TypeError):
                pass
        if not codigos_int:
            return {}

        # Buscar pares (CodContrato, Natureza) distintos da tabela empenho_itens
        pares = db.session.query(
            EmpenhoItem.CodContrato,
            EmpenhoItem.Natureza
        ).filter(
            EmpenhoItem.CodContrato.in_(codigos_int),
            EmpenhoItem.Natureza.isnot(None),
            EmpenhoItem.Natureza != '',
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ).distinct().all()

        if not pares:
            return {}

        # Coletar todos os códigos de Natureza únicos (converter Text → int para lookup)
        todos_cod_nat = set()
        for p in pares:
            try:
                todos_cod_nat.add(int(p.Natureza))
            except (ValueError, TypeError):
                pass
        todos_cod_nat = list(todos_cod_nat)

        if not todos_cod_nat:
            return {}

        # Buscar dados das naturezas
        natdespesas = NatDespesa.query.filter(NatDespesa.codigo.in_(todos_cod_nat)).all()
        mapa_nd = {nd.codigo: nd for nd in natdespesas}

        # Montar mapa: contrato → lista de naturezas
        resultado = {}
        for p in pares:
            cod_contrato = str(p.CodContrato)
            try:
                cod_nat = int(p.Natureza)
            except (ValueError, TypeError):
                continue
            nd = mapa_nd.get(cod_nat)
            if nd:
                if cod_contrato not in resultado:
                    resultado[cod_contrato] = []
                # Evitar duplicatas (mesmo contrato pode ter vários empenho_itens com mesma natureza)
                if not any(n['codigo'] == nd.codigo for n in resultado[cod_contrato]):
                    resultado[cod_contrato].append({
                        'codigo': nd.codigo,
                        'titulo': nd.titulo,
                        'id_titulo': nd.id_titulo
                    })

        # Ordenar naturezas de cada contrato por código
        for cod in resultado:
            resultado[cod].sort(key=lambda x: x['codigo'])

        return resultado

    @staticmethod
    def listar_naturezas_utilizadas():
        """Lista naturezas de despesa que aparecem nos empenho_itens vinculados a contratos.
        Retorna lista de NatDespesa ordenados por codigo."""
        from app.models.empenho_item import EmpenhoItem
        from app.models.nat_despesa import NatDespesa

        # Naturezas a excluir (estornos/cancelamentos)
        EXCLUDE_NATUREZA = {'339092', '449092'}

        cod_naturezas = db.session.query(
            EmpenhoItem.Natureza
        ).filter(
            EmpenhoItem.Natureza.isnot(None),
            EmpenhoItem.Natureza != '',
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA),
            EmpenhoItem.CodContrato.isnot(None),
            EmpenhoItem.CodContrato != 0
        ).distinct().all()

        # Converter Text → int para lookup na tabela natdespesas
        codigos = []
        for r in cod_naturezas:
            try:
                codigos.append(int(r[0]))
            except (ValueError, TypeError):
                pass

        if not codigos:
            return []

        return NatDespesa.query.filter(
            NatDespesa.codigo.in_(codigos)
        ).order_by(NatDespesa.titulo).all()

    # ===== DETALHAMENTO FINANCEIRO (Natureza → SubItem → valores) =====

    @staticmethod
    def obter_detalhamento_financeiro(codigo_contrato):
        """
        Busca detalhamento financeiro hierárquico do contrato:
        Natureza de Despesa → Sub-Item da Despesa + Tipo Patrimonial → NEs com valores.

        Faz JOIN entre empenho_itens (classificadores) e empenho (valores)
        para calcular totais por agrupamento.

        Returns:
            dict com:
                - naturezas: lista hierárquica [{codigo, nome, total, subitens: [{...}]}]
                - total_geral: float
                - qtd_nes: int
                - tem_dados: bool
        """
        from app.models.empenho_item import EmpenhoItem, ClassTipoPatrimonial, ClassSubItemDespesa
        from app.models.empenho import Empenho
        from app.models.nat_despesa import NatDespesa
        from sqlalchemy import func, cast, String, literal_column

        # Naturezas a excluir (estornos/cancelamentos)
        EXCLUDE_NATUREZA = {'339092', '449092'}

        resultado = {
            'naturezas': [],
            'total_geral': 0,
            'qtd_nes': 0,
            'tem_dados': False
        }

        # Query agrupada: (Natureza, SubItemDespesa, TipoPatrimonial) com SUM(valor)
        dados = db.session.query(
            EmpenhoItem.Natureza,
            EmpenhoItem.SubItemDespesa,
            EmpenhoItem.TipoPatrimonial,
            func.count().label('qtd_nes'),
            func.sum(Empenho.valor).label('total_valor'),
            literal_column(
                "GROUP_CONCAT(DISTINCT empenho_itens.Fonte ORDER BY empenho_itens.Fonte SEPARATOR ',')"
            ).label('fontes')
        ).join(
            Empenho,
            db.and_(
                Empenho.codigo == EmpenhoItem.codigo,
                cast(Empenho.codigoUG, String(20)) == EmpenhoItem.codigoUG
            )
        ).filter(
            EmpenhoItem.CodContrato == int(codigo_contrato),
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ).group_by(
            EmpenhoItem.Natureza,
            EmpenhoItem.SubItemDespesa,
            EmpenhoItem.TipoPatrimonial
        ).order_by(
            EmpenhoItem.Natureza,
            EmpenhoItem.SubItemDespesa
        ).all()

        if not dados:
            return resultado

        resultado['tem_dados'] = True

        # Buscar NEs individuais para a tabela de detalhe
        nes_detalhe = db.session.query(
            EmpenhoItem.codigo,
            EmpenhoItem.dataEmissao,
            EmpenhoItem.Natureza,
            EmpenhoItem.SubItemDespesa,
            EmpenhoItem.TipoPatrimonial,
            EmpenhoItem.Fonte,
            Empenho.valor
        ).join(
            Empenho,
            db.and_(
                Empenho.codigo == EmpenhoItem.codigo,
                cast(Empenho.codigoUG, String(20)) == EmpenhoItem.codigoUG
            )
        ).filter(
            EmpenhoItem.CodContrato == int(codigo_contrato),
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ).order_by(
            EmpenhoItem.Natureza,
            EmpenhoItem.SubItemDespesa,
            EmpenhoItem.dataEmissao.desc()
        ).all()

        # Resolver nomes: natdespesas
        codigos_nat = list(set(d.Natureza for d in dados if d.Natureza))
        codigos_nat_int = []
        for c in codigos_nat:
            try:
                codigos_nat_int.append(int(c))
            except (ValueError, TypeError):
                pass
        mapa_nat = {}
        if codigos_nat_int:
            nats = NatDespesa.query.filter(NatDespesa.codigo.in_(codigos_nat_int)).all()
            for nd in nats:
                mapa_nat[str(nd.codigo)] = nd.titulo

        # Resolver nomes: class_subitemdespesa
        mapa_subitem = {}
        todos_sub = ClassSubItemDespesa.query.all()
        for s in todos_sub:
            cod1 = str(s.valoresClassificador1 or '').strip()
            cod2 = str(s.valoresClassificador2 or '').strip()
            cod_completo = f"{cod1}.{cod2}" if cod2 else cod1
            mapa_subitem[cod_completo] = s.nomeClassificador

        # Resolver nomes: class_tipopatrimonial
        mapa_tipopatr = {}
        todos_tp = ClassTipoPatrimonial.query.all()
        for t in todos_tp:
            cod = str(t.valoresClassificador1 or '').strip()
            mapa_tipopatr[cod] = t.nomeClassificador

        # Montar hierarquia: Natureza → SubItens
        naturezas_dict = {}  # cod_nat -> {dados}
        total_geral = 0
        qtd_total = 0

        for d in dados:
            cod_nat = d.Natureza or '—'
            valor = float(d.total_valor or 0)
            qtd = d.qtd_nes
            total_geral += valor
            qtd_total += qtd

            if cod_nat not in naturezas_dict:
                naturezas_dict[cod_nat] = {
                    'codigo': cod_nat,
                    'nome': mapa_nat.get(cod_nat, 'Não identificada'),
                    'total': 0,
                    'qtd_nes': 0,
                    'subitens': []
                }

            naturezas_dict[cod_nat]['total'] += valor
            naturezas_dict[cod_nat]['qtd_nes'] += qtd

            cod_sub = d.SubItemDespesa or '—'
            cod_tp = d.TipoPatrimonial or '—'

            naturezas_dict[cod_nat]['subitens'].append({
                'codigo': cod_sub,
                'nome': mapa_subitem.get(cod_sub, 'Não identificado'),
                'tipo_patrimonial_codigo': cod_tp,
                'tipo_patrimonial_nome': mapa_tipopatr.get(cod_tp, 'Não identificado'),
                'qtd_nes': qtd,
                'total': valor,
                'fontes': d.fontes or ''
            })

        # Montar lista de NEs individuais com dados resolvidos
        empenhos_detalhe = []
        for ne in nes_detalhe:
            cod_sub = ne.SubItemDespesa or '—'
            cod_tp = ne.TipoPatrimonial or '—'
            # Formatar data
            data_fmt = None
            if ne.dataEmissao:
                try:
                    from datetime import datetime as dt_cls
                    dt_obj = dt_cls.fromisoformat(str(ne.dataEmissao).split('.')[0])
                    data_fmt = dt_obj.strftime('%d/%m/%Y')
                except (ValueError, TypeError):
                    data_fmt = str(ne.dataEmissao)[:10]

            empenhos_detalhe.append({
                'codigo_ne': ne.codigo,
                'data_emissao': data_fmt,
                'natureza': ne.Natureza,
                'natureza_nome': mapa_nat.get(ne.Natureza or '', ''),
                'subitem_codigo': cod_sub,
                'subitem_nome': mapa_subitem.get(cod_sub, 'Não identificado'),
                'tipo_patrimonial_codigo': cod_tp,
                'tipo_patrimonial_nome': mapa_tipopatr.get(cod_tp, 'Não identificado'),
                'fonte': ne.Fonte,
                'valor': float(ne.valor or 0)
            })

        resultado['naturezas'] = sorted(naturezas_dict.values(), key=lambda x: x['codigo'])
        resultado['empenhos_detalhe'] = empenhos_detalhe
        resultado['total_geral'] = total_geral
        resultado['qtd_nes'] = qtd_total

        return resultado

    # ===== DADOS DE COLUNA: SubItem + TipoPatrimonial por contrato (batch) =====

    @staticmethod
    def buscar_classificadores_por_contratos(codigos_contratos):
        """Busca Fonte, SubItemDespesa e TipoPatrimonial distintos de cada contrato.

        Retorna dict: {codigo_contrato: {
            'fontes': ['5.00', '7.06'],
            'subitens': [{'codigo': '2399.01', 'nome': 'COMBUSTÍVEIS...'}],
            'tipos_patrimoniais': [{'codigo': '40', 'nome': 'Material de Consumo'}]
        }}
        """
        from app.models.empenho_item import EmpenhoItem, ClassSubItemDespesa, ClassTipoPatrimonial
        from app.models.class_fonte import ClassFonte
        from sqlalchemy import literal_column

        if not codigos_contratos:
            return {}

        # Naturezas a excluir (estornos/cancelamentos)
        EXCLUDE_NATUREZA = {'339092', '449092'}

        # Query agrupada: CodContrato → Fontes, SubItens e TipoPatrimoniais concatenados
        # CodContrato é BIGINT → converter para int
        codigos_int = []
        for c in codigos_contratos:
            try:
                codigos_int.append(int(c))
            except (ValueError, TypeError):
                pass
        if not codigos_int:
            return {}

        rows = db.session.query(
            EmpenhoItem.CodContrato,
            literal_column(
                "GROUP_CONCAT(DISTINCT empenho_itens.Fonte SEPARATOR '||')"
            ).label('fontes_raw'),
            literal_column(
                "GROUP_CONCAT(DISTINCT empenho_itens.SubItemDespesa SEPARATOR '||')"
            ).label('subitens_raw'),
            literal_column(
                "GROUP_CONCAT(DISTINCT empenho_itens.TipoPatrimonial SEPARATOR '||')"
            ).label('tipos_raw')
        ).filter(
            EmpenhoItem.CodContrato.in_(codigos_int),
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ).group_by(
            EmpenhoItem.CodContrato
        ).all()

        if not rows:
            return {}

        # Coletar todos os codigos unicos para resolver nomes em batch
        todos_cod_sub = set()
        todos_cod_tp = set()
        for r in rows:
            if r.subitens_raw:
                for bloco in r.subitens_raw.split('||'):
                    for val in bloco.split(' | '):
                        todos_cod_sub.add(val.strip())
            if r.tipos_raw:
                for bloco in r.tipos_raw.split('||'):
                    for val in bloco.split(' | '):
                        todos_cod_tp.add(val.strip())

        # Resolver nomes SubItem
        mapa_subitem = {}
        if todos_cod_sub:
            for s in ClassSubItemDespesa.query.all():
                cod1 = str(s.valoresClassificador1 or '').strip()
                cod2 = str(s.valoresClassificador2 or '').strip()
                cod_completo = f"{cod1}.{cod2}" if cod2 else cod1
                mapa_subitem[cod_completo] = s.nomeClassificador

        # Resolver nomes TipoPatrimonial
        mapa_tp = {}
        if todos_cod_tp:
            for t in ClassTipoPatrimonial.query.all():
                cod = str(t.valoresClassificador1 or '').strip()
                mapa_tp[cod] = t.nomeClassificador

        # Resolver nomes Fonte (empenho_itens.Fonte = "5.00" → codigo class_fonte = "500")
        # Normaliza chave como string pura (cobre INT e VARCHAR)
        mapa_fonte = {}
        for f in ClassFonte.query.all():
            chave = str(f.codigo).strip()
            mapa_fonte[chave] = f.descricao
            # Também indexar sem zeros à esquerda para cobrir variações
            try:
                mapa_fonte[str(int(chave))] = f.descricao
            except (ValueError, TypeError):
                pass

        # Montar resultado por contrato (chave = str para compatibilidade com codigos_contratos)
        resultado = {}
        for r in rows:
            cod = str(r.CodContrato)
            fontes = []
            subitens = []
            tipos = []

            # Fontes (converter "5.00" → "500" para resolver nome)
            if r.fontes_raw:
                fontes_set = set()
                for val in r.fontes_raw.split('||'):
                    val = val.strip()
                    if val and val not in fontes_set:
                        fontes_set.add(val)
                        # Converter formato empenho (5.00) → código tabela (500)
                        cod_fonte = val.replace('.', '').strip()
                        # Tentar lookup direto e sem zeros à esquerda
                        nome_fonte = mapa_fonte.get(cod_fonte)
                        if not nome_fonte:
                            try:
                                nome_fonte = mapa_fonte.get(str(int(cod_fonte)))
                            except (ValueError, TypeError):
                                pass
                        fontes.append({
                            'codigo': cod_fonte,
                            'nome': nome_fonte or val
                        })

            if r.subitens_raw:
                codigos_vistos = set()
                for bloco in r.subitens_raw.split('||'):
                    for val in bloco.split(' | '):
                        val = val.strip()
                        if val and val not in codigos_vistos:
                            codigos_vistos.add(val)
                            subitens.append({
                                'codigo': val,
                                'nome': mapa_subitem.get(val, val)
                            })

            if r.tipos_raw:
                codigos_vistos = set()
                for bloco in r.tipos_raw.split('||'):
                    for val in bloco.split(' | '):
                        val = val.strip()
                        if val and val not in codigos_vistos:
                            codigos_vistos.add(val)
                            tipos.append({
                                'codigo': val,
                                'nome': mapa_tp.get(val, val)
                            })

            resultado[cod] = {
                'fontes': sorted(fontes, key=lambda x: x['codigo']),
                'subitens': sorted(subitens, key=lambda x: x['codigo']),
                'tipos_patrimoniais': sorted(tipos, key=lambda x: x['codigo'])
            }

        return resultado

    @staticmethod
    def listar_subitens_utilizados():
        """Lista SubItens de Despesa que aparecem em empenho_itens de contratos.
        Retorna lista de dicts [{codigo, nome}] para popular filtro."""
        from app.models.empenho_item import EmpenhoItem, ClassSubItemDespesa

        # Naturezas a excluir (estornos/cancelamentos)
        EXCLUDE_NATUREZA = {'339092', '449092'}

        rows = db.session.query(
            EmpenhoItem.SubItemDespesa
        ).filter(
            EmpenhoItem.CodContrato != 0,
            EmpenhoItem.SubItemDespesa.isnot(None),
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ).distinct().all()

        # Expandir multi-valores (pipe-separated)
        codigos = set()
        for r in rows:
            if r.SubItemDespesa:
                for val in r.SubItemDespesa.split(' | '):
                    codigos.add(val.strip())

        # Resolver nomes
        mapa = {}
        for s in ClassSubItemDespesa.query.all():
            cod1 = str(s.valoresClassificador1 or '').strip()
            cod2 = str(s.valoresClassificador2 or '').strip()
            cod_completo = f"{cod1}.{cod2}" if cod2 else cod1
            mapa[cod_completo] = s.nomeClassificador

        resultado = []
        for cod in codigos:
            resultado.append({
                'codigo': cod,
                'nome': mapa.get(cod, cod)
            })
        # nomeClassificador tem formato "01 - DESCRIÇÃO", ordenar pela descrição
        resultado.sort(key=lambda x: x['nome'].split(' - ', 1)[-1] if ' - ' in x['nome'] else x['nome'])
        return resultado

    @staticmethod
    def listar_tipos_patrimoniais_utilizados():
        """Lista Tipos Patrimoniais que aparecem em empenho_itens de contratos.
        Retorna lista de dicts [{codigo, nome}] para popular filtro."""
        from app.models.empenho_item import EmpenhoItem, ClassTipoPatrimonial

        # Naturezas a excluir (estornos/cancelamentos)
        EXCLUDE_NATUREZA = {'339092', '449092'}

        rows = db.session.query(
            EmpenhoItem.TipoPatrimonial
        ).filter(
            EmpenhoItem.CodContrato != 0,
            EmpenhoItem.TipoPatrimonial.isnot(None),
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ).distinct().all()

        # Expandir multi-valores
        codigos = set()
        for r in rows:
            if r.TipoPatrimonial:
                for val in r.TipoPatrimonial.split(' | '):
                    codigos.add(val.strip())

        # Resolver nomes
        mapa = {}
        for t in ClassTipoPatrimonial.query.all():
            cod = str(t.valoresClassificador1 or '').strip()
            mapa[cod] = t.nomeClassificador

        resultado = []
        for cod in codigos:
            resultado.append({
                'codigo': cod,
                'nome': mapa.get(cod, cod)
            })
        resultado.sort(key=lambda x: x['nome'])
        return resultado

    # ===== FILTROS ENCADEADOS: Natureza → TipoPatrimonial → SubItem =====

    @staticmethod
    def listar_tipos_patrimoniais_por_natureza(natureza_codigo):
        """Lista Tipos Patrimoniais filtrados por uma natureza específica.
        Retorna lista de dicts [{codigo, nome}]."""
        from app.models.empenho_item import EmpenhoItem, ClassTipoPatrimonial

        EXCLUDE_NATUREZA = {'339092', '449092'}

        filtros = [
            EmpenhoItem.CodContrato != 0,
            EmpenhoItem.TipoPatrimonial.isnot(None),
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ]
        if natureza_codigo:
            filtros.append(EmpenhoItem.Natureza == str(natureza_codigo))

        rows = db.session.query(
            EmpenhoItem.TipoPatrimonial
        ).filter(*filtros).distinct().all()

        codigos = set()
        for r in rows:
            if r.TipoPatrimonial:
                for val in r.TipoPatrimonial.split(' | '):
                    codigos.add(val.strip())

        mapa = {}
        for t in ClassTipoPatrimonial.query.all():
            cod = str(t.valoresClassificador1 or '').strip()
            mapa[cod] = t.nomeClassificador

        resultado = [{'codigo': cod, 'nome': mapa.get(cod, cod)} for cod in codigos]
        resultado.sort(key=lambda x: x['nome'])
        return resultado

    @staticmethod
    def listar_subitens_por_natureza(natureza_codigo, tipo_patrimonial_codigo=None):
        """Lista SubItens filtrados por natureza e opcionalmente por tipo patrimonial.
        Retorna lista de dicts [{codigo, nome}]."""
        from app.models.empenho_item import EmpenhoItem, ClassSubItemDespesa

        EXCLUDE_NATUREZA = {'339092', '449092'}

        filtros = [
            EmpenhoItem.CodContrato != 0,
            EmpenhoItem.SubItemDespesa.isnot(None),
            EmpenhoItem.Natureza.notin_(EXCLUDE_NATUREZA)
        ]
        if natureza_codigo:
            filtros.append(EmpenhoItem.Natureza == str(natureza_codigo))
        if tipo_patrimonial_codigo:
            filtros.append(EmpenhoItem.TipoPatrimonial.contains(tipo_patrimonial_codigo))

        rows = db.session.query(
            EmpenhoItem.SubItemDespesa
        ).filter(*filtros).distinct().all()

        codigos = set()
        for r in rows:
            if r.SubItemDespesa:
                for val in r.SubItemDespesa.split(' | '):
                    codigos.add(val.strip())

        mapa = {}
        for s in ClassSubItemDespesa.query.all():
            cod1 = str(s.valoresClassificador1 or '').strip()
            cod2 = str(s.valoresClassificador2 or '').strip()
            cod_completo = f"{cod1}.{cod2}" if cod2 else cod1
            mapa[cod_completo] = s.nomeClassificador

        resultado = [{'codigo': cod, 'nome': mapa.get(cod, cod)} for cod in codigos]
        # nomeClassificador tem formato "01 - DESCRIÇÃO", ordenar pela descrição
        resultado.sort(key=lambda x: x['nome'].split(' - ', 1)[-1] if ' - ' in x['nome'] else x['nome'])
        return resultado

    # ===== TOTAIS FINANCEIROS =====

    @staticmethod
    def obter_totais_financeiros(codigo_contrato):
        """Retorna totais gerais e do exercício atual de empenho, liquidação, PD e pagamento."""
        from app.models.empenho import Empenho
        from app.models.liquidacao import Liquidacao
        from app.models.ob import OB
        from app.models.pd import PD
        from sqlalchemy import func, extract
        from datetime import date

        ano_atual = date.today().year

        try:
            cod_int = int(codigo_contrato)
        except (ValueError, TypeError):
            vazio = {'empenho': 0, 'liquidacao': 0, 'pd': 0, 'pd_aberto': 0, 'pd_executada': 0, 'pagamento': 0}
            return {'geral': vazio, 'exercicio': vazio, 'ano': ano_atual}

        cod_str = str(codigo_contrato)

        # --- Totais gerais ---
        total_emp = db.session.query(
            func.coalesce(func.sum(Empenho.valor), 0)
        ).filter_by(codContrato=cod_int).scalar()

        total_liq = db.session.query(
            func.coalesce(func.sum(Liquidacao.valor), 0)
        ).filter(
            Liquidacao.codContrato == cod_int,
            Liquidacao.statusDocumento == 'CONTABILIZADO'
        ).scalar()

        total_pd = db.session.query(
            func.coalesce(func.sum(PD.valor), 0)
        ).filter(
            PD.codContrato == cod_int,
            PD.statusDocumento == 'CONTABILIZADO'
        ).scalar()

        total_pd_aberto = db.session.query(
            func.coalesce(func.sum(PD.valor), 0)
        ).filter(
            PD.codContrato == cod_int,
            PD.statusDocumento == 'CONTABILIZADO',
            PD.statusExecucao == 'STATUS_DISPONIVEL'
        ).scalar()

        total_pd_executada = db.session.query(
            func.coalesce(func.sum(PD.valor), 0)
        ).filter(
            PD.codContrato == cod_int,
            PD.statusDocumento == 'CONTABILIZADO',
            PD.statusExecucao == 'STATUS_EXECUTADA'
        ).scalar()

        total_pag = db.session.query(
            func.coalesce(func.sum(OB.valor), 0)
        ).filter(
            OB.codContrato == cod_str,
            OB.statusDocumento == 'CONTABILIZADO'
        ).scalar()

        # --- Totais exercício atual ---
        emp_ano = db.session.query(
            func.coalesce(func.sum(Empenho.valor), 0)
        ).filter(
            Empenho.codContrato == cod_int,
            extract('year', Empenho.dataEmissao) == ano_atual
        ).scalar()

        liq_ano = db.session.query(
            func.coalesce(func.sum(Liquidacao.valor), 0)
        ).filter(
            Liquidacao.codContrato == cod_int,
            Liquidacao.statusDocumento == 'CONTABILIZADO',
            extract('year', Liquidacao.dataEmissao) == ano_atual
        ).scalar()

        pd_ano = db.session.query(
            func.coalesce(func.sum(PD.valor), 0)
        ).filter(
            PD.codContrato == cod_int,
            PD.statusDocumento == 'CONTABILIZADO',
            extract('year', PD.dataEmissao) == ano_atual
        ).scalar()

        pd_ano_aberto = db.session.query(
            func.coalesce(func.sum(PD.valor), 0)
        ).filter(
            PD.codContrato == cod_int,
            PD.statusDocumento == 'CONTABILIZADO',
            extract('year', PD.dataEmissao) == ano_atual,
            PD.statusExecucao == 'STATUS_DISPONIVEL'
        ).scalar()

        pd_ano_executada = db.session.query(
            func.coalesce(func.sum(PD.valor), 0)
        ).filter(
            PD.codContrato == cod_int,
            PD.statusDocumento == 'CONTABILIZADO',
            extract('year', PD.dataEmissao) == ano_atual,
            PD.statusExecucao == 'STATUS_EXECUTADA'
        ).scalar()

        pag_ano = db.session.query(
            func.coalesce(func.sum(OB.valor), 0)
        ).filter(
            OB.codContrato == cod_str,
            OB.statusDocumento == 'CONTABILIZADO',
            extract('year', OB.dataEmissao) == ano_atual
        ).scalar()

        return {
            'geral': {
                'empenho': float(total_emp),
                'liquidacao': float(total_liq),
                'pd': float(total_pd),
                'pd_aberto': float(total_pd_aberto),
                'pd_executada': float(total_pd_executada),
                'pagamento': float(total_pag)
            },
            'exercicio': {
                'empenho': float(emp_ano),
                'liquidacao': float(liq_ano),
                'pd': float(pd_ano),
                'pd_aberto': float(pd_ano_aberto),
                'pd_executada': float(pd_ano_executada),
                'pagamento': float(pag_ano)
            },
            'ano': ano_atual
        }

    # ===== ADITIVOS =====

    @staticmethod
    def listar_aditivos(codigo_contrato):
        """Lista aditivos de um contrato, ordenados por data de vigência início (desc)."""
        from app.models.contrato_aditivo import ContratoAditivo
        return ContratoAditivo.query.filter_by(
            codigo_contrato=str(codigo_contrato)
        ).order_by(ContratoAditivo.dtVigenciaIni.desc()).all()

    # ===== UTILITÁRIOS =====

    @staticmethod
    def _converter_valor_br(valor_str):
        """
        Converte valor em formato brasileiro para float.
        Ex: 'R$ 1.234,56' -> 1234.56
            '1.234,56' -> 1234.56
            '1234,56' -> 1234.56
        """
        if not valor_str:
            return 0.0
        # Remove R$, espaços
        valor = re.sub(r'[R$\s]', '', str(valor_str))
        # Remove pontos de milhar, troca vírgula por ponto
        valor = valor.replace('.', '').replace(',', '.')
        try:
            return float(valor)
        except ValueError:
            return 0.0
