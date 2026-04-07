"""
Service layer do módulo de Diárias.
Contém toda a lógica de negócio para solicitações de viagem.
"""
from datetime import date, datetime, timedelta
from decimal import Decimal
from sqlalchemy import func

from app.extensions import db
from app.models.diaria import (
    DiariasItinerario, DiariasItemItinerario, DiariasParada,
    DiariasJustificativa, DiariasCotacao, DiariasCotacaoVoo,
    DiariasValorCargo,
    DiariasStatusViagem, DiariasTipoSolicitacao, DiariasCargo,
    DiariasNatureza, Municipio, Estado, Setor, Orgao,
    DiariasEtapa, DiariasHistoricoMovimentacao, DiariasMovimentacao,
)
from app.models.contrato import Contrato


class DiariaService:
    """Serviço para operações de diárias/viagens."""

    # ── Constantes ───────────────────────────────────────────────────────

    TIPO_ESTADUAL = 1
    TIPO_NACIONAL = 2
    TIPO_INTERNACIONAL = 3
    COD_IBGE_PIAUI = 22
    COD_IBGE_TERESINA = 2211001
    NATUREZA_FORA_ESTADO = 1
    NATUREZA_DENTRO_ESTADO = 2

    STATUS_GERADO = 1
    STATUS_ACEITO = 2
    STATUS_REJEITADO = 3
    STATUS_CANCELADO = 4

    # ── Cálculos ─────────────────────────────────────────────────────────

    @staticmethod
    def calcular_diarias(data_viagem, data_retorno):
        """Calcula quantidade de diárias: dias + 0.5."""
        delta = (data_retorno - data_viagem).days
        if delta < 0:
            raise ValueError('Data de retorno deve ser posterior à data de viagem.')
        return delta + 0.5

    @staticmethod
    def get_valor_cargo(cargo_id, tipo_itinerario_id):
        """Busca valor da diária para cargo + tipo de itinerário."""
        vc = DiariasValorCargo.query.filter_by(
            cargo_id=cargo_id,
            tipo_itinerario_id=tipo_itinerario_id,
        ).first()
        return vc.valor if vc else Decimal('0.00')

    @staticmethod
    def calcular_valor_total_estadual(itinerario_id):
        """Calcula valor total para viagem estadual: SUM(valor_cargo) × qtd_diarias."""
        itinerario = DiariasItinerario.query.get(itinerario_id)
        if not itinerario:
            return Decimal('0.00')

        soma_valores = db.session.query(
            func.sum(DiariasItemItinerario.valor_cargo)
        ).filter_by(id_itinerario=itinerario_id).scalar() or Decimal('0.00')

        return soma_valores * Decimal(str(itinerario.qtd_diarias_solicitadas))

    @staticmethod
    def calcular_valor_total_nacional(itinerario_id):
        """Calcula valor total para viagem nacional: SUM(valor_cargo) × qtd_diarias + SUM(cotacoes)."""
        itinerario = DiariasItinerario.query.get(itinerario_id)
        if not itinerario:
            return Decimal('0.00')

        soma_valores = db.session.query(
            func.sum(DiariasItemItinerario.valor_cargo)
        ).filter_by(id_itinerario=itinerario_id).scalar() or Decimal('0.00')

        # Soma das cotações selecionadas para cada pessoa (single query)
        soma_cotacoes = db.session.query(
            func.coalesce(func.sum(DiariasCotacao.valor), 0)
        ).join(
            DiariasItemItinerario,
            DiariasItemItinerario.cotacao_id == DiariasCotacao.id
        ).filter(
            DiariasItemItinerario.id_itinerario == itinerario_id
        ).scalar() or Decimal('0.00')
        soma_cotacoes = Decimal(str(soma_cotacoes))

        return soma_valores * Decimal(str(itinerario.qtd_diarias_solicitadas)) + soma_cotacoes

    @staticmethod
    def exige_justificativa(data_viagem):
        """Verifica se a viagem exige justificativa (menos de 10 dias)."""
        if isinstance(data_viagem, str):
            try:
                data_viagem = datetime.strptime(data_viagem, '%Y-%m-%dT%H:%M').date()
            except ValueError:
                data_viagem = datetime.strptime(data_viagem, '%Y-%m-%d').date()
        elif hasattr(data_viagem, 'date'):
            data_viagem = data_viagem.date()
        limite = date.today() + timedelta(days=10)
        return data_viagem < limite

    # ── CRUD ─────────────────────────────────────────────────────────────

    @staticmethod
    def criar_itinerario(dados, pessoas, paradas_ids=None, justificativa_texto=None):
        """
        Cria um novo itinerário com todos os dados associados.

        Args:
            dados: dict com campos do itinerário principal
            pessoas: list de dicts com {cpf, entidade_id}
            paradas_ids: list de cod_ibge dos municípios de parada (estadual)
            justificativa_texto: texto da justificativa (opcional)

        Returns:
            DiariasItinerario criado
        """
        tipo = dados['tipo_itinerario']
        data_viagem = dados['data_viagem']
        data_retorno = dados['data_retorno']

        if isinstance(data_viagem, str):
            try:
                data_viagem = datetime.strptime(data_viagem, '%Y-%m-%dT%H:%M')
            except ValueError:
                data_viagem = datetime.strptime(data_viagem, '%Y-%m-%d')
        if isinstance(data_retorno, str):
            try:
                data_retorno = datetime.strptime(data_retorno, '%Y-%m-%dT%H:%M')
            except ValueError:
                data_retorno = datetime.strptime(data_retorno, '%Y-%m-%d')

        qtd_diarias = DiariaService.calcular_diarias(data_viagem, data_retorno)

        # Regras por tipo
        if tipo == DiariaService.TIPO_ESTADUAL:
            # Estadual: origem e destino fixos Teresina-PI
            origem = str(DiariaService.COD_IBGE_TERESINA)
            estado_origem = DiariaService.COD_IBGE_PIAUI
            estado_destino = DiariaService.COD_IBGE_PIAUI
            natureza_id = DiariaService.NATUREZA_DENTRO_ESTADO
        else:
            origem = str(DiariaService.COD_IBGE_TERESINA)
            estado_origem = dados.get('estado_origem')
            estado_destino = dados.get('estado_destino')
            natureza_id = DiariaService.NATUREZA_FORA_ESTADO

        itinerario = DiariasItinerario(
            usuario_gerador=dados['usuario_gerador'],
            tipo_solicitacao_id=dados['tipo_solicitacao_id'],
            qtd_diarias_solicitadas=qtd_diarias,
            tipo_itinerario=tipo,
            status_id=DiariaService.STATUS_GERADO,
            data_solicitacao=date.today(),
            data_viagem=data_viagem,
            data_retorno=data_retorno,
            origem=origem,
            estado_origem=estado_origem,
            estado_destino=estado_destino,
            objetivo=dados.get('objetivo'),
        )
        db.session.add(itinerario)
        db.session.flush()  # Para obter o ID

        # Adiciona pessoas com cargo e valor da diária
        valor_total_diarias = Decimal('0.00')
        for p in pessoas:
            cargo_id = p.get('cargo_id')
            cargo_assessorado_id = p.get('cargo_assessorado_id')

            # Se assessorando, usa o cargo do assessorado para cálculo do valor
            cargo_para_calculo = cargo_assessorado_id or cargo_id
            valor_cargo = DiariaService.get_valor_cargo(cargo_para_calculo, tipo) if cargo_para_calculo else Decimal('0.00')
            valor_total_diarias += valor_cargo

            item = DiariasItemItinerario(
                id_itinerario=itinerario.id,
                cpf_pessoa=p['cpf'],
                matricula_pessoa=p.get('matricula', ''),
                nome_pessoa=p.get('nome', ''),
                cargo_id=cargo_id,
                cargo_assessorado_id=cargo_assessorado_id,
                natureza_id=natureza_id,
                valor_cargo=valor_cargo,
                # Campos da API pessoaSGA
                banco_agencia=p.get('banco_agencia', '') or None,
                banco_conta=p.get('banco_conta', '') or None,
                vinculo=p.get('vinculo', '') or None,
                cargo_folha=p.get('cargo_folha', '') or None,
                setor=p.get('setor', '') or None,
                orgao=p.get('orgao', '') or None,
            )
            db.session.add(item)

        # Calcula valor total: SUM(valor_cargo) × qtd_diarias
        itinerario.valor_total = valor_total_diarias * Decimal(str(qtd_diarias))

        # Adiciona paradas (estadual)
        if paradas_ids:
            for mun_id in paradas_ids:
                if mun_id:
                    parada = DiariasParada(
                        itinerario_id=itinerario.id,
                        municipio_id=int(mun_id),
                    )
                    db.session.add(parada)

        # Adiciona justificativa
        if justificativa_texto:
            justificativa = DiariasJustificativa(
                itinerario_id=itinerario.id,
                descricao=justificativa_texto,
                tipo_justificativa='Dias de viagem',
            )
            db.session.add(justificativa)

        db.session.commit()
        return itinerario

    @staticmethod
    def atender_itinerario(itinerario_id, conclusao, cotacoes_pessoas=None):
        """
        Processa o atendimento/aprovação de um itinerário.

        Args:
            itinerario_id: ID do itinerário
            conclusao: ID do novo status (2=Aceito, 3=Rejeitado, 4=Cancelado)
            cotacoes_pessoas: dict {item_id: cotacao_id} para viagens nacionais
        """
        itinerario = DiariasItinerario.query.get(itinerario_id)
        if not itinerario:
            raise ValueError('Itinerário não encontrado.')

        conclusao = int(conclusao)

        # Para rejeição/cancelamento, apenas atualiza status
        if conclusao in (DiariaService.STATUS_REJEITADO, DiariaService.STATUS_CANCELADO):
            itinerario.status_id = conclusao
            db.session.commit()
            return itinerario

        # Para aprovação, calcula valor total
        if itinerario.tipo_itinerario == DiariaService.TIPO_ESTADUAL:
            itinerario.valor_total = DiariaService.calcular_valor_total_estadual(itinerario_id)
        else:
            # Nacional/Internacional: precisa selecionar cotações para cada pessoa
            if not cotacoes_pessoas:
                raise ValueError('Selecione uma cotação para cada pessoa.')

            itens = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario_id).all()
            for item in itens:
                cotacao_id = cotacoes_pessoas.get(str(item.id))
                if not cotacao_id:
                    raise ValueError(f'Cotação não selecionada para item {item.id}.')
                item.cotacao_id = int(cotacao_id)

            db.session.flush()
            itinerario.valor_total = DiariaService.calcular_valor_total_nacional(itinerario_id)

        itinerario.status_id = conclusao
        db.session.commit()
        return itinerario

    # ── Consultas ────────────────────────────────────────────────────────

    @staticmethod
    def listar_itinerarios(usuario_cpf=None, filtros=None, page=1, per_page=20):
        """Lista itinerários com filtros e paginação."""
        query = DiariasItinerario.query

        if usuario_cpf:
            query = query.filter_by(usuario_gerador=usuario_cpf)

        if filtros:
            if filtros.get('tipo_itinerario'):
                query = query.filter_by(tipo_itinerario=int(filtros['tipo_itinerario']))
            if filtros.get('status'):
                query = query.filter_by(status_id=int(filtros['status']))
            if filtros.get('n_processo'):
                query = query.filter(DiariasItinerario.n_processo.ilike(f"%{filtros['n_processo']}%"))
            if filtros.get('data_viagem'):
                query = query.filter_by(data_viagem=filtros['data_viagem'])

        query = query.order_by(DiariasItinerario.data_solicitacao.desc())
        return query.paginate(page=page, per_page=per_page, error_out=False)

    @staticmethod
    def listar_todos_itinerarios(filtros=None, page=1, per_page=20):
        """Lista todos os itinerários (visão admin) com filtros."""
        query = DiariasItinerario.query

        if filtros:
            if filtros.get('tipo_itinerario'):
                query = query.filter_by(tipo_itinerario=int(filtros['tipo_itinerario']))
            if filtros.get('status'):
                query = query.filter_by(status_id=int(filtros['status']))
            if filtros.get('n_processo'):
                query = query.filter(DiariasItinerario.n_processo.ilike(f"%{filtros['n_processo']}%"))
            if filtros.get('data_viagem'):
                query = query.filter_by(data_viagem=filtros['data_viagem'])
            if filtros.get('usuario'):
                query = query.filter(DiariasItinerario.usuario_gerador.ilike(f"%{filtros['usuario']}%"))

        query = query.order_by(DiariasItinerario.data_solicitacao.desc())
        return query.paginate(page=page, per_page=per_page, error_out=False)

    @staticmethod
    def get_itinerario_completo(itinerario_id):
        """Retorna itinerário com todos os dados relacionados."""
        itinerario = DiariasItinerario.query.get(itinerario_id)
        if not itinerario:
            return None

        itens = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario_id).all()
        paradas = DiariasParada.query.filter_by(itinerario_id=itinerario_id).all()
        cotacoes = DiariasCotacao.query.filter_by(itinerario_id=itinerario_id).all()
        cotacoes_voos = DiariasCotacaoVoo.query.filter_by(
            itinerario_id=itinerario_id
        ).order_by(DiariasCotacaoVoo.tipo_trecho, DiariasCotacaoVoo.valor).all()

        return {
            'itinerario': itinerario,
            'itens': itens,
            'paradas': paradas,
            'cotacoes': cotacoes,
            'cotacoes_voos': cotacoes_voos,
        }

    # ── Cotações ─────────────────────────────────────────────────────────

    @staticmethod
    def criar_cotacao(itinerario_id, contrato_codigo, valor, data_hora=None, auto_commit=True):
        """Cria uma nova cotação para um itinerário."""
        cotacao = DiariasCotacao(
            itinerario_id=itinerario_id,
            contrato_codigo=contrato_codigo,
            valor=Decimal(str(valor)),
            data_hora=data_hora or datetime.now(),
        )
        db.session.add(cotacao)
        if auto_commit:
            db.session.commit()
        return cotacao

    @staticmethod
    def get_cotacoes_itinerario(itinerario_id):
        """Retorna cotações de um itinerário."""
        return DiariasCotacao.query.filter_by(itinerario_id=itinerario_id).all()

    # ── Cotações de Voos (detalhadas) ────────────────────────────────────

    @staticmethod
    def criar_cotacao_voo(itinerario_id, contrato_codigo, tipo_trecho, cia, voo,
                          saida, chegada, origem, destino, valor, bagagem=None,
                          cia_conexao=None, voo_conexao=None, saida_conexao=None,
                          chegada_conexao=None, origem_conexao=None, destino_conexao=None,
                          fonte='manual', auto_commit=True):
        """Cria uma cotacao de voo detalhada (com suporte a conexao)."""
        cotacao = DiariasCotacaoVoo(
            itinerario_id=itinerario_id,
            contrato_codigo=contrato_codigo or None,
            tipo_trecho=tipo_trecho,
            cia=cia,
            voo=voo,
            saida=saida,
            chegada=chegada,
            origem=origem,
            destino=destino,
            bagagem=bagagem,
            valor=Decimal(str(valor)),
            fonte=fonte,
            cia_conexao=cia_conexao or None,
            voo_conexao=voo_conexao or None,
            saida_conexao=saida_conexao,
            chegada_conexao=chegada_conexao,
            origem_conexao=origem_conexao or None,
            destino_conexao=destino_conexao or None,
        )
        db.session.add(cotacao)
        if auto_commit:
            db.session.commit()
        return cotacao

    @staticmethod
    def get_cotacoes_voos_itinerario(itinerario_id):
        """Retorna cotacoes de voos agrupadas por tipo_trecho."""
        todas = DiariasCotacaoVoo.query.filter_by(
            itinerario_id=itinerario_id
        ).order_by(DiariasCotacaoVoo.tipo_trecho, DiariasCotacaoVoo.valor).all()
        ida = [c for c in todas if c.tipo_trecho == 'ida']
        volta = [c for c in todas if c.tipo_trecho == 'volta']
        return {'ida': ida, 'volta': volta, 'todas': todas}

    @staticmethod
    def excluir_cotacao_voo(cotacao_id, auto_commit=True):
        """Exclui uma cotacao de voo pelo ID."""
        cotacao = DiariasCotacaoVoo.query.get(cotacao_id)
        if not cotacao:
            return False
        db.session.delete(cotacao)
        if auto_commit:
            db.session.commit()
        return True

    # ── Timeline / Movimentações ──────────────────────────────────────────

    @staticmethod
    def registrar_movimentacao(id_itinerario, etapa_nova_id, usuario_id=None, comentario=None, auto_commit=True):
        """
        Registra uma transição de etapa no histórico e atualiza a etapa atual.

        Args:
            id_itinerario: ID do itinerário
            etapa_nova_id: ID da nova etapa (DiariasEtapaID)
            usuario_id: ID do usuário responsável (opcional)
            comentario: comentário sobre a movimentação (opcional)
            auto_commit: se True, faz commit; se False, deixa para o chamador (§1.1)
        """
        itinerario = DiariasItinerario.query.get(id_itinerario)
        if not itinerario:
            return None

        etapa_anterior_id = itinerario.etapa_atual_id

        historico = DiariasHistoricoMovimentacao(
            id_itinerario=id_itinerario,
            id_etapa_anterior=etapa_anterior_id,
            id_etapa_nova=int(etapa_nova_id),
            id_usuario_responsavel=usuario_id,
            data_movimentacao=datetime.now(),
            comentario=comentario,
        )
        db.session.add(historico)

        itinerario.etapa_atual_id = int(etapa_nova_id)
        if auto_commit:
            db.session.commit()
        return historico

    @staticmethod
    def obter_timeline(itinerario):
        """
        Monta os dados da timeline para exibição na página de detalhes.

        Usa a tabela diarias_movimentacao (espelho SEI) para determinar:
        - Quais sub-itens (documentos) existem em cada etapa
        - Data de cada documento (coluna Data)
        - Link de acesso ao documento no SEI (coluna LinkAcesso)
        - Data da etapa = data do primeiro documento correspondente
        - Range de datas = primeiro doc da etapa ... último doc da etapa

        A etapa "Escolha do Voo" (ID 2) só aparece para solicitações
        do tipo "Diárias + Passagens" (id=2) e "Apenas Passagens" (id=3).

        Returns:
            Lista de dicts com chaves:
            etapa, concluida, data, data_fim, atual, comentario, tempo_decorrido,
            subitens[{id, nome, concluido, doc_formatado, link_acesso, data}]
        """
        from app.constants import DiariasEtapaID, DIARIAS_SUBITENS, TIPOS_COM_PASSAGENS
        from app.services.diarias_sei_integration import SERIE_TIPO_DOCUMENTO_MAP

        # 1. Busca todas as etapas ordenadas
        todas_etapas = DiariasEtapa.query.order_by(DiariasEtapa.ordem).all()

        # 2. Filtra etapa "Escolha do Voo" se não tem passagens
        tipo_sol = getattr(itinerario, 'tipo_solicitacao_id', None)
        tem_passagens = tipo_sol in TIPOS_COM_PASSAGENS

        if not tem_passagens:
            todas_etapas = [e for e in todas_etapas if e.id != DiariasEtapaID.ESCOLHA_VOO]

        # 3. Determina ordem da etapa atual para cálculo de concluída
        etapa_atual_obj = next((e for e in todas_etapas if e.id == itinerario.etapa_atual_id), None)
        etapa_atual_ordem = etapa_atual_obj.ordem if etapa_atual_obj else 0

        # 4. Busca documentos da diarias_movimentacao para este processo
        #    e mapeia IdSerie → doc_tipo usando SERIE_TIPO_DOCUMENTO_MAP
        protocolo = itinerario.n_processo or itinerario.sei_protocolo
        docs_por_tipo = {}  # doc_tipo → lista de {data, link_acesso, doc_formatado, serie_nome}

        if protocolo:
            movs = DiariasMovimentacao.query.filter_by(
                protocolo_procedimento=protocolo
            ).all()

            for mov in movs:
                id_serie_str = str(mov.id_serie) if mov.id_serie else ''
                tipo_doc = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie_str)

                # Refinamento: IdSerie 264 (Documento Externo) pode ser prestação SCDP
                if id_serie_str == '264':
                    texto_ref = ((mov.descricao or '') + ' ' + (mov.numero or '')).lower()
                    if any(kw in texto_ref for kw in ('scdp', 'prestação', 'prestacao')):
                        tipo_doc = 'prestacao_scdp'

                if not tipo_doc:
                    continue

                # Parse da data (formato dd/mm/yyyy ou yyyy-mm-dd)
                data_parsed = None
                if mov.data:
                    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S'):
                        try:
                            data_parsed = datetime.strptime(mov.data.strip(), fmt)
                            break
                        except (ValueError, AttributeError):
                            continue

                if tipo_doc not in docs_por_tipo:
                    docs_por_tipo[tipo_doc] = []

                docs_por_tipo[tipo_doc].append({
                    'data': data_parsed,
                    'data_str': mov.data or '',
                    'link_acesso': mov.link_acesso or '',
                    'doc_formatado': mov.documento_formatado or '',
                    'serie_nome': mov.serie_nome or '',
                })

            # Ordena cada lista por data (mais antigo primeiro)
            for tipo_doc in docs_por_tipo:
                docs_por_tipo[tipo_doc].sort(
                    key=lambda d: d['data'] or datetime.min
                )

        # 5. Monta índice invertido: doc_tipo → etapa_id (para calcular datas por etapa)
        doc_tipo_para_etapa = {}
        for etapa_id, subs in DIARIAS_SUBITENS.items():
            for sub_cfg in subs:
                doc_tipo_para_etapa[sub_cfg['doc_tipo']] = etapa_id

        # 6. Calcula data_inicio e data_fim por etapa (a partir dos docs encontrados)
        etapa_datas = {}  # etapa_id → {'inicio': datetime, 'fim': datetime}
        for tipo_doc, docs_lista in docs_por_tipo.items():
            etapa_id = doc_tipo_para_etapa.get(tipo_doc)
            if etapa_id is None:
                continue

            for doc_info in docs_lista:
                if not doc_info['data']:
                    continue
                if etapa_id not in etapa_datas:
                    etapa_datas[etapa_id] = {'inicio': doc_info['data'], 'fim': doc_info['data']}
                else:
                    if doc_info['data'] < etapa_datas[etapa_id]['inicio']:
                        etapa_datas[etapa_id]['inicio'] = doc_info['data']
                    if doc_info['data'] > etapa_datas[etapa_id]['fim']:
                        etapa_datas[etapa_id]['fim'] = doc_info['data']

        # 7. Monta timeline com sub-itens
        timeline = []
        data_anterior = None

        for etapa in todas_etapas:
            datas_etapa = etapa_datas.get(etapa.id)
            data_inicio = datas_etapa['inicio'] if datas_etapa else None
            data_fim = datas_etapa['fim'] if datas_etapa else None

            concluida = etapa.ordem < etapa_atual_ordem

            # Última etapa (Prestação de Contas): concluída se todos os subitens têm docs
            if not concluida and etapa.id == itinerario.etapa_atual_id:
                subitens_cfg = DIARIAS_SUBITENS.get(etapa.id, [])
                if subitens_cfg:
                    todos_concluidos = all(
                        docs_por_tipo.get(s['doc_tipo'])
                        for s in subitens_cfg
                        if not s.get('condicional') or (
                            s.get('condicional') == 'passagens'
                            and itinerario.tipo_solicitacao_id in TIPOS_COM_PASSAGENS
                        )
                    )
                    if todos_concluidos:
                        concluida = True

            # Tempo decorrido entre etapas
            tempo_decorrido = None
            if concluida and data_anterior and data_inicio:
                diff = data_inicio - data_anterior
                dias = diff.days
                if dias > 0:
                    tempo_decorrido = f"{dias}d"
                else:
                    horas = max(diff.seconds // 3600, 0)
                    if horas > 0:
                        tempo_decorrido = f"{horas}h"
                    else:
                        minutos = max(diff.seconds // 60, 0)
                        tempo_decorrido = f"{minutos}min"

            atual = etapa.id == itinerario.etapa_atual_id

            # Sub-itens: só para etapas concluídas ou a atual
            subitens = []
            if concluida or atual:
                config_subitens = DIARIAS_SUBITENS.get(etapa.id, [])
                for sub_cfg in config_subitens:
                    if sub_cfg.get('condicional') == 'passagens' and not tem_passagens:
                        continue

                    docs_lista = docs_por_tipo.get(sub_cfg['doc_tipo'], [])
                    # Pega o primeiro documento encontrado para este tipo
                    doc_info = docs_lista[0] if docs_lista else None
                    sub_concluido = doc_info is not None

                    subitens.append({
                        'id': sub_cfg['id'],
                        'nome': sub_cfg['nome'],
                        'concluido': sub_concluido,
                        'doc_formatado': doc_info['doc_formatado'] if doc_info else None,
                        'link_acesso': doc_info['link_acesso'] if doc_info else None,
                        'data': doc_info['data'] if doc_info else None,
                        'data_str': doc_info['data_str'] if doc_info else None,
                    })

            timeline.append({
                'etapa': etapa,
                'concluida': concluida,
                'data': data_inicio,
                'data_fim': data_fim,
                'atual': atual,
                'comentario': None,
                'tempo_decorrido': tempo_decorrido,
                'subitens': subitens,
            })

            if data_fim:
                data_anterior = data_fim
            elif data_inicio:
                data_anterior = data_inicio

        return timeline

    # ── Dados de referência ──────────────────────────────────────────────

    @staticmethod
    def get_tipos_solicitacao():
        return DiariasTipoSolicitacao.query.order_by(DiariasTipoSolicitacao.id).all()

    @staticmethod
    def get_status_list():
        return DiariasStatusViagem.query.order_by(DiariasStatusViagem.id).all()

    @staticmethod
    def get_cargos():
        return DiariasCargo.query.order_by(DiariasCargo.nome).all()

    @staticmethod
    def get_agencias():
        """Retorna agências de viagem a partir dos contratos com Natureza 339033."""
        from sqlalchemy import text
        # Busca contratos distintos por CNPJ (codigoContratado) que possuem
        # empenhos com Natureza de despesa 339033 (Passagens e Despesas com Locomoção).
        # Agrupa por empresa e pega o contrato mais recente (MAX codigo).
        rows = db.session.execute(text("""
            SELECT MAX(c.codigo) AS codigo,
                   MAX(c.nomeContratadoResumido) AS nomeContratadoResumido,
                   MAX(c.nomeContratado) AS nomeContratado,
                   c.codigoContratado
            FROM contratos c
            INNER JOIN empenho_itens ei ON ei.CodContrato = c.codigo
            WHERE ei.Natureza = '339033'
            GROUP BY c.codigoContratado
            ORDER BY nomeContratadoResumido
        """)).fetchall()
        return rows

    @staticmethod
    def get_estados():
        return Estado.query.order_by(Estado.nome).all()

    @staticmethod
    def get_municipios_por_estado(cod_ibge_estado=None):
        """Retorna municípios, opcionalmente filtrados por estado."""
        query = Municipio.query
        if cod_ibge_estado:
            # Municípios do PI começam com 22xxxxx
            prefixo = str(cod_ibge_estado)
            query = query.filter(
                Municipio.cod_ibge.between(
                    int(prefixo + '00000'),
                    int(prefixo + '99999')
                )
            )
        return query.order_by(Municipio.nome).all()

    @staticmethod
    def get_setores_por_orgao(idorgao):
        """Retorna setores de um órgão."""
        return Setor.query.filter_by(idorgao=idorgao).order_by(Setor.nome).all()

    @staticmethod
    def get_orgaos():
        return Orgao.query.order_by(Orgao.nome).all()
