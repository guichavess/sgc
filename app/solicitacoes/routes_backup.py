"""
Rotas do módulo de Solicitações.
"""
import os
import json
import time
import sys
import asyncio
import requests
import concurrent.futures

from flask import (
    Blueprint, render_template, render_template_string, session,
    redirect, url_for, request, jsonify, flash, Response,
    stream_with_context, make_response, current_app
)
from sqlalchemy import or_, text, func, case
from datetime import datetime, timedelta, time as datetime_time
from telethon import TelegramClient

# Models
from app.models import (
    Contrato, Usuario, Solicitacao, HistoricoMovimentacao,
    Etapa, SolicitacaoEmpenho, StatusEmpenho, Empenho,
    SaldoEmpenho, SeiMovimentacao
)
from app.extensions import db

# Services
from app.services.siafe_service import validar_ne_siafe
from app.services.sei_integration import criar_procedimento_pagamento, gerar_documento_pagamento, assinar_documento
from app.services.sei_auth import gerar_token_sei_admin
from app.services.email_service import enviar_email_teste




solicitacoes_bp = Blueprint('solicitacoes', __name__)

def task_atualizar_saldo(app_obj, cod_contrato, competencia):
    with app_obj.app_context():
        # Lógica de soma no SIAFE (Tabela empenho)
        ano_filtro = int(competencia.split('/')[-1]) if '/' in competencia else datetime.now().year
        cod_limpo = "".join(filter(str.isdigit, str(cod_contrato)))

        # --- ALTERAÇÃO AQUI ---
        # Soma direta da coluna 'vlr' (que já possui os sinais corretos)
        total = db.session.query(
            func.sum(Empenho.vlr)
        ).filter(
            Empenho.codigoUG == '210101',
            Empenho.statusDocumento == 'CONTABILIZADO',
            Empenho.anoProcesso == ano_filtro,
            Empenho.codContrato == cod_limpo
        ).scalar() or 0.0
        # ----------------------

        # Insere novo registro na tabela de Saldo
        novo_saldo = SaldoEmpenho(
            saldo=float(total),
            cod_contrato=cod_contrato,
            competencia=competencia,
            data=datetime.now()
        )
        db.session.add(novo_saldo)
        db.session.commit()

def registrar_historico(solicitacao, id_nova_etapa, data_real_sei, id_usuario, texto, id_anterior_forcado=None):
    """
    Registra histórico garantindo a sequência lógica.
    Aceita 'id_anterior_forcado' para permitir encadeamento dentro do loop (ex: 2 -> 15 -> 12).
    """
    existe = HistoricoMovimentacao.query.filter_by(
        id_solicitacao=solicitacao.id,
        id_etapa_nova=id_nova_etapa
    ).first()

    if not existe:
        # Se passarmos o ID anterior explicitamente (durante o loop), usa ele.
        # Caso contrário, pega o que está no banco.
        etapa_anterior = id_anterior_forcado if id_anterior_forcado else solicitacao.etapa_atual_id
        
        novo = HistoricoMovimentacao(
            id_solicitacao=solicitacao.id,
            id_etapa_anterior=etapa_anterior,
            id_etapa_nova=id_nova_etapa,
            id_usuario_responsavel=id_usuario,
            data_movimentacao=data_real_sei,
            comentario=texto
        )
        db.session.add(novo)
        return True # Retorna True para indicar que houve mudança
    return False

def disparar_notificacao_telegram(solicitacao):
    """
    Lógica de notificação isolada (código existente reutilizado).
    """
    try:
        # Busca Fiscais
        sql_fiscais = text("SELECT nome, telefone FROM sgc.fiscais_contrato WHERE codigo_contrato = :cod AND telefone IS NOT NULL")
        fiscais = db.session.execute(sql_fiscais, {'cod': solicitacao.codigo_contrato}).fetchall()
        
        if not fiscais: return

        # Prepara Mensagem
        contrato_info = solicitacao.contrato
        nome_resumido = contrato_info.nomeContratadoResumido or contrato_info.nomeContratado
        
        msg_telegram = (
            f"🔔 *Aviso de Pagamento - SGC*\n\n"
            f"Olá! O processo de pagamento do contrato *{nome_resumido}* "
            f"entrou em fase de Fiscalização.\n\n"
            f"Por favor, verifique a documentação no SEI.\n"
            f"🔗 Link: {solicitacao.link_processo_sei}"
        )

        # Envio Assíncrono (Credenciais via variáveis de ambiente)
        api_id = int(os.getenv('TELEGRAM_API_ID', '0'))
        api_hash = os.getenv('TELEGRAM_API_HASH', '')
        session_path = os.getenv('TELEGRAM_SESSION_PATH', os.path.join(os.getcwd(), 'api_telegram', 'minha_sessao')) 

        async def _envio():
            async with TelegramClient(session_path, api_id, api_hash) as client:
                for fiscal in fiscais:
                    telefone = fiscal.telefone
                    if not telefone.startswith('+'): telefone = '+55' + ''.join(filter(str.isdigit, telefone))
                    try:
                        await client.send_message(telefone, msg_telegram)
                    except Exception as e:
                        print(f"Erro Telegram: {e}")

        asyncio.run(_envio())
        print(f"Telegram disparado para solicitação {solicitacao.id}")

    except Exception as e:
        print(f"Erro ao disparar notificação: {e}")



def disparar_notificacao_email(solicitacao):
    """
    Busca e-mails dos fiscais no banco e envia aviso de TESTE.
    """
    try:
        # 1. Busca Emails na tabela 'fiscais_contrato' do banco 'sgc'
        sql_fiscais = text("SELECT nome, email FROM sgc.fiscais_contrato WHERE codigo_contrato = :cod AND email IS NOT NULL")
        fiscais = db.session.execute(sql_fiscais, {'cod': solicitacao.codigo_contrato}).fetchall()
        
        if not fiscais: 
            print(f"⚠️ [Email] Nenhum fiscal com e-mail encontrado para o contrato {solicitacao.codigo_contrato}.")
            return

        # 2. Prepara a Mensagem de Teste (HTML)
        contrato_info = solicitacao.contrato
        nome_resumido = contrato_info.nomeContratadoResumido or contrato_info.nomeContratado
        
        assunto = f"🚧 TESTE: Automação SGC - {nome_resumido}"
        
        mensagem_html = f"""
        <div style="font-family: Arial, sans-serif; color: #333;">
            <div style="background-color: #f8d7da; color: #721c24; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h2 style="margin-top: 0;">⚠️ MENSAGEM DE TESTE</h2>
                <p><strong>Por favor, DESCONSIDERE este e-mail.</strong></p>
                <p>Este é um teste da nova ferramenta de automação de geração de relatórios e notificações do sistema SGC.</p>
            </div>
            
            <p>Olá,</p>
            <p>O sistema identificou uma movimentação no processo de pagamento abaixo:</p>
            <ul>
                <li><strong>Contrato:</strong> {nome_resumido}</li>
                <li><strong>Nº Original:</strong> {contrato_info.numeroOriginal}</li>
                <li><strong>Processo SEI:</strong> {solicitacao.protocolo_gerado_sei}</li>
                <li><strong>Link:</strong> <a href="{solicitacao.link_processo_sei}">Acessar Processo</a></li>
            </ul>
            <hr>
            <p style="font-size: 12px; color: #666;">Sistema de Gestão de Contratos - SEAD/PI</p>
        </div>
        """

        # 3. Itera sobre os fiscais e envia (Mesma lógica do Telegram: um por um)
        for fiscal in fiscais:
            email_destino = fiscal.email.strip()
            if "@" in email_destino: # Validação básica
                print(f"📧 Enviando e-mail de teste para fiscal: {fiscal.nome} ({email_destino})...")
                # Enviamos uma lista com um único destinatário para manter a privacidade (sem cópia aberta)
                enviar_email_teste(assunto, mensagem_html, [email_destino])

    except Exception as e:
        print(f"❌ Erro ao disparar notificação de email: {e}")



def formatar_diferenca(dt_final, dt_inicial):
    if not dt_inicial or not dt_final:
        return None
    
    diff = dt_final - dt_inicial
    total_seconds = int(diff.total_seconds())
    
    days = diff.days
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    
    if days > 0:
        return f"{days} dias"
    elif hours > 0:
        return f"{hours} horas"
    elif minutes > 0:
        return f"{minutes} min"
    else:
        return "instantes"
    

def gerar_dados_timeline(solicitacao, historico):
    print("--- INICIANDO DEBUG TIMELINE ---")
    todas_etapas = Etapa.query.order_by(Etapa.ordem).all()
    
    # IDs que compõem o grupo de Fiscalização
    ids_fiscalizacao = [12, 13, 14]
    
    # Nomes que realmente devem sumir da linha do tempo (etapas de erro/espera)
    nomes_ignorar = [
        "Aguardando Documentação",
        "Documentação Incompleta"
    ]
    
    # Criamos as etapas visíveis escondendo os nomes da lista E as subetapas 13 e 14
    # Deixamos a 12 como a "representante" do grupo no gráfico
    etapas_visiveis = [
        e for e in todas_etapas 
        if e.nome not in nomes_ignorar and e.id not in [13, 14]
    ]

    etapa_real_id = solicitacao.etapa_atual_id
    etapa_real_obj = next((e for e in todas_etapas if e.id == etapa_real_id), None)
    
    if not etapa_real_obj:
        return []

    # Lógica para definir qual etapa visual destacar como "atual"
    etapa_visual_atual = None
    if etapa_real_id in ids_fiscalizacao:
        # Se estiver em qualquer uma das três, a 12 (Fiscalização) é a atual visual
        etapa_visual_atual = next((e for e in etapas_visiveis if e.id == 12), None)
    elif any(e.id == etapa_real_id for e in etapas_visiveis):
        etapa_visual_atual = next(e for e in etapas_visiveis if e.id == etapa_real_id)
    else:
        # Fallback para etapas ocultas
        candidatos_futuros = [e for e in etapas_visiveis if e.ordem > etapa_real_obj.ordem]
        etapa_visual_atual = candidatos_futuros[0] if candidatos_futuros else etapas_visiveis[-1]

    # MAPEAMENTO DE DATAS
    mapa_datas = {mov.id_etapa_nova: mov.data_movimentacao for mov in historico}
    timeline_data = []
    data_anterior = None

    for etapa in etapas_visiveis:
        data_conclusao = mapa_datas.get(etapa.id)
        
        # Fallback para data de criação na primeira etapa
        if not data_conclusao and etapa.ordem == 1:
            data_conclusao = solicitacao.data_solicitacao
        
        # Coleta subetapas se for o marco de Fiscalização (ID 12)
        subetapas_info = []
        if etapa.id == 12:
            datas_encontradas = []
            for sub_id in ids_fiscalizacao:
                d_sub = mapa_datas.get(sub_id)
                if d_sub:
                    e_obj = next((e for e in todas_etapas if e.id == sub_id), None)
                    subetapas_info.append({
                        'nome': e_obj.nome if e_obj else "Etapa",
                        'data': d_sub
                    })
            
        # Define se é a etapa atual visualmente
        eh_atual = (etapa_visual_atual and etapa.id == etapa_visual_atual.id)
        
        # Lógica de Conclusão
        foi_concluida = False
        if etapa_visual_atual:
            if etapa.ordem < etapa_visual_atual.ordem:
                foi_concluida = True
            # Se a etapa visual atual é "Pago" (6), então "Liquidado" (5) está concluído
            if etapa_visual_atual.id == 6 and etapa.id == 5:
                foi_concluida = True
            # Se a etapa real já passou da 14, o bloco 12 está concluído
            if etapa.id == 12 and etapa_real_obj.ordem > next((e.ordem for e in todas_etapas if e.id == 14), 0):
                foi_concluida = True

        # Cálculo do tempo decorrido
        tempo_decorrido = None
        if data_conclusao and data_anterior:
            try:
                tempo_decorrido = formatar_diferenca(data_conclusao, data_anterior)
            except (ValueError, TypeError) as e:
                current_app.logger.warning(f"Erro ao calcular tempo decorrido: {e}") 

        if data_conclusao:
            data_anterior = data_conclusao

        timeline_data.append({
            'etapa': etapa,
            'concluida': foi_concluida,
            'data': data_conclusao,
            'atual': eh_atual,
            'tempo_decorrido': tempo_decorrido,
            'subetapas': subetapas_info  # Novo campo para o HTML
        })
    
    return timeline_data


def registrar_e_atualizar_saldo(solicitacao, usuario_id, valor_solicitado=0.0):
    """
    Consulta o saldo na tabela 'empenho' e gera um registro em 'solicitacaoempenho'.
    """
    saldo_calculado = 0.0
    try:
        # 1. Filtro de Ano (Extrai de 'Junho/2025' ou '06/2025')
        ano_filtro = datetime.now().year
        if solicitacao.competencia and '/' in solicitacao.competencia:
            try:
                # Pega a última parte após a barra
                ano_filtro = int(solicitacao.competencia.split('/')[-1])
            except (ValueError, IndexError):
                pass  # Mantém o ano atual como fallback
        
        # 2. Filtro de Contrato (Limpa para garantir apenas números)
        cod_contrato_limpo = "".join(filter(str.isdigit, str(solicitacao.codigo_contrato)))

        # 3. Consulta SQL de Soma (Baseada na sua lógica original)
        total_empenhado = db.session.query(
            func.sum(Empenho.vlr)
        ).filter(
            Empenho.codigoUG == '210101',
            Empenho.statusDocumento == 'CONTABILIZADO',                
            Empenho.anoProcesso == ano_filtro,
            Empenho.codContrato == cod_contrato_limpo
        ).scalar()

        if total_empenhado:
            saldo_calculado = float(total_empenhado)

        # 4. Criação do registro na tabela solicitacaoempenho
        novo_registro = SolicitacaoEmpenho(
            id_solicitacao=solicitacao.id,
            valor=valor_solicitado,
            competencia=solicitacao.competencia,
            id_user=usuario_id,
            data=datetime.now(),
            saldo_momento=saldo_calculado
        )
        db.session.add(novo_registro)
        return True
    except Exception as e:
        print(f"Erro ao processar saldo: {e}")
        return False
    
    


@solicitacoes_bp.route('/api/solicitar-empenho', methods=['POST'])
def api_solicitar_empenho():
    if 'usuario_db_id' not in session: 
        return jsonify({'sucesso': False, 'msg': 'Login necessário'}), 401

    try:
        dados = request.get_json()
        id_solicitacao = dados.get('id_solicitacao')
        valor_str = dados.get('valor', '0')

        # Tratamento de valores
        valor_limpo = valor_str.replace('R$', '').replace('.', '').replace(',', '.').strip()
        valor = float(valor_limpo)

        solicitacao = Solicitacao.query.get(id_solicitacao)
        if not solicitacao:
            return jsonify({'sucesso': False, 'msg': 'Solicitação não encontrada.'})

        # Utiliza a função centralizada para calcular o saldo e salvar
        if registrar_e_atualizar_saldo(solicitacao, session['usuario_db_id'], valor_solicitado=valor):
            solicitacao.status_empenho_id = 1 
            db.session.commit()
            return jsonify({'sucesso': True, 'msg': 'Empenho solicitado com sucesso!'})
        else:
            return jsonify({'sucesso': False, 'msg': 'Erro ao calcular saldo.'})

    except Exception as e:
        db.session.rollback()
        return jsonify({'sucesso': False, 'msg': f'Erro ao processar: {e}'})


def baixar_documentos_thread(app_obj, protocolo, token_sei, base_url):
    with app_obj.app_context():
        start_time = time.time() # Para calcular o tempo_execucao
        protocolo_limpo = "".join(filter(str.isdigit, protocolo))
        
        headers = {'token': token_sei, 'Accept': 'application/json'}
        params = {
            "protocolo_procedimento": protocolo_limpo,
            "pagina": 1,
            "quantidade": 1000, 
            "sinal_completo": "N"
        }

        try:
            resp = requests.get(base_url, headers=headers, params=params, timeout=120)
            tempo_total = round(time.time() - start_time, 3)
            
            if resp.status_code == 200:
                data_json = resp.json()
                
                # Busca a lista de documentos
                items_found = []
                if isinstance(data_json, dict):
                    items_found = data_json.get('Documentos', [])
                    if not items_found and 'resultados' in data_json:
                        items_found = data_json['resultados']
                elif isinstance(data_json, list):
                    items_found = data_json
                
                if items_found:
                    for doc in items_found:
                        # Extração segura dos sub-objetos
                        serie = doc.get('Serie', {})
                        unidade = doc.get('UnidadeElaboradora', {})
                        
                        novo_mov = SeiMovimentacao(
                            # Mapeamento Direto
                            id_documento = str(doc.get('IdDocumento', '')),
                            protocolo_procedimento = protocolo, # Sua chave externa
                            id_procedimento = str(doc.get('IdProcedimento', '')),
                            procedimento_formatado = str(doc.get('ProcedimentoFormatado', '')),
                            documento_formatado = str(doc.get('DocumentoFormatado', '')),
                            link_acesso = doc.get('LinkAcesso'),
                            descricao = doc.get('Descricao'),
                            data = doc.get('Data') or doc.get('DataGeracao'), # Fallback
                            numero = doc.get('Numero'),
                            
                            # Mapeamento do objeto 'Serie'
                            id_serie = int(serie.get('IdSerie')) if serie.get('IdSerie') and str(serie.get('IdSerie')).isdigit() else None,
                            serie_nome = serie.get('Nome'),
                            serie_aplicabilidade = serie.get('Aplicabilidade'),
                            
                            # Mapeamento do objeto 'UnidadeElaboradora'
                            unidade_id = str(unidade.get('IdUnidade', '')),
                            unidade_sigla = unidade.get('Sigla'),
                            unidade_descricao = unidade.get('Descricao'),
                            
                            # Metadados
                            tempo_execucao = tempo_total,
                            obs = ""
                        )
                        db.session.add(novo_mov)
                    
                    db.session.commit()
                    return (True, f"OK: {protocolo}")
                else:
                    return (True, f"Vazio: {protocolo}")
            
            else:
                # Log de erro de resposta da API (ex: 401, 404, 500)
                app_obj.logger.warning(f"API SEI retornou status {resp.status_code} para o protocolo {protocolo}")
                return (False, f"Erro API {protocolo}: {resp.status_code}")

        except Exception as e:
            db.session.rollback()
            # Grava o erro real com o traceback completo no log do servidor
            app_obj.logger.error(f"ERRO CRÍTICO DOWNLOAD SEI (Protocolo {protocolo}): {str(e)}", exc_info=True)
            return (False, f"Erro Tratamento {protocolo}: {str(e)}")


@solicitacoes_bp.route('/dashboard')
def dashboard():
    if 'usuario_db_id' not in session: return redirect(url_for('auth.login'))
    
    # 1. Preparação da Query Base
    query = Solicitacao.query.join(Contrato).join(Etapa)
    
    # 2. Captura dos Filtros da URL
    page = request.args.get('page', 1, type=int)
    search_q = request.args.get('q', '').strip()
    # filtro_contratados removido
    filtro_status = request.args.getlist('filtro_status')

    filtro_competencia = request.args.getlist('filtro_competencia')

    data_inicio = request.args.get('data_inicio')
    data_fim = request.args.get('data_fim')
    # 3. Aplicação dos Filtros
    
    # A) Busca Textual Geral (Código, NE, Objeto, Nome Contratado, Processo)
    if search_q:
        query = query.filter(
            or_(
                Contrato.codigo.like(f'%{search_q}%'),
                Contrato.numeroOriginal.like(f'%{search_q}%'),
                Contrato.objeto.like(f'%{search_q}%'),
                # Novos campos de busca:
                Contrato.nomeContratado.like(f'%{search_q}%'),
                Contrato.numProcesso.like(f'%{search_q}%'),
                Solicitacao.protocolo_gerado_sei.like(f'%{search_q}%')
            )
        )

    # B) Filtro por Checkbox de Status/Etapa (Mantido)
    if filtro_status:
        query = query.filter(Etapa.nome.in_(filtro_status))

    
    if filtro_competencia:
        query = query.filter(Solicitacao.competencia.in_(filtro_competencia))

    if data_inicio:
        try:
            dt_inicio = datetime.strptime(data_inicio, '%Y-%m-%d')
            query = query.filter(Solicitacao.data_solicitacao >= dt_inicio)
        except ValueError:
            pass # Ignora se a data for inválida

    if data_fim:
        try:
            dt_fim = datetime.strptime(data_fim, '%Y-%m-%d')
            # Ajusta para o final do dia (23:59:59) para incluir todo o dia selecionado
            dt_fim = dt_fim.replace(hour=23, minute=59, second=59)
            query = query.filter(Solicitacao.data_solicitacao <= dt_fim)
        except ValueError:
            pass

    # 4. Execução da Query Principal
    per_page = 10
    pagination = query.order_by(Solicitacao.id.desc()).paginate(page=page, per_page=per_page)
    solicitacoes = pagination.items

    # 5. Dados para popular os filtros
    # (Removida a query de todos_contratados pois não será mais usada no template)

    todos_status = db.session.query(Etapa.nome).join(Solicitacao).distinct().order_by(Etapa.nome).all()
    todos_status = [s[0] for s in todos_status]

    raw_competencias = db.session.query(Solicitacao.competencia)\
        .distinct()\
        .filter(Solicitacao.competencia.isnot(None))\
        .all()
    
    lista_competencias = [c[0] for c in raw_competencias if c[0]]

    def chave_ordenacao(comp_str):
        meses = {
            'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
            'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
        }
        try:
            partes = comp_str.split('/')
            if len(partes) == 2:
                mes_nome, ano = partes
                # Retorna (Ano, Mês) para ordenar primeiro pelo ano, depois pelo mês
                return (int(ano), meses.get(mes_nome.capitalize(), 0))
        except (ValueError, IndexError, AttributeError):
            pass  # Retorna valor padrão para ordenação
        return (0, 0)
    

    lista_competencias.sort(key=chave_ordenacao, reverse=True)

    return render_template('solicitacoes/dashboard.html', 
                           solicitacoes=solicitacoes,
                           pagination=pagination,
                           todos_status=todos_status,
                           todas_competencias=lista_competencias)

                           

@solicitacoes_bp.route('/nova', methods=['GET', 'POST'])
def nova_solicitacao():
    if 'usuario_db_id' not in session: return redirect(url_for('auth.login'))
    lista_unidades = session.get('unidades', [])

    # Variáveis de controle do Modal
    modal_abrir = False
    doc_protocolo = ""
    doc_id_formatado = "" # O número visual do documento
    unidade_atual = ""
    proc_formatado = ""

    if request.method == 'POST':
        codigo_contrato = request.form.get('contrato_selecionado')
        competencia = request.form.get('competencia')
        unidade_id = request.form.get('unidade_procedimento')

        if not codigo_contrato or not unidade_id:
            flash('Selecione o contrato e a unidade.', 'warning')
            return redirect(url_for('solicitacoes.nova_solicitacao'))

        contrato = Contrato.query.get(codigo_contrato)
        usuario_id = session['usuario_db_id']
        token_sei = session.get('sei_token')
        
        # Prepara dados
        dados_contrato_api = {
            'numeroOriginal': contrato.numeroOriginal, 
            'nomeContratado': contrato.nomeContratado,
            'codigo': contrato.codigo,
            'nomeContratadoResumido': contrato.nomeContratadoResumido 
        }
        
        # 1. Cria Processo
        proc_criado = criar_procedimento_pagamento(token_sei, unidade_id, dados_contrato_api, competencia)
        
        if proc_criado:
            ctx_doc = {
                'num_contrato': contrato.numeroOriginal,
                'empresa': contrato.nomeContratado,
                'competencia': competencia,
                'usuario_nome': session.get('usuario_nome'),
                # AQUI: Usamos o cargo salvo na sessão (vindo do login)
                'usuario_cargo': session.get('usuario_cargo', 'Colaborador'), 
                'objeto': contrato.objeto or "Objeto não informado"
            }
            doc_criado = gerar_documento_pagamento(token_sei, unidade_id, proc_criado['IdProcedimento'], ctx_doc)
            
            # 2. Salva no Banco (Status inicial: AGUARDANDO ASSINATURA)
            nova_sol = Solicitacao(
                codigo_contrato=codigo_contrato,
                id_usuario_solicitante=usuario_id,
                protocolo_gerado_sei=proc_criado['ProcedimentoFormatado'],
                id_procedimento_sei=proc_criado['IdProcedimento'],
                link_processo_sei=proc_criado['LinkAcesso'],
                especificacao=proc_criado['EspecificacaoGerada'],
                competencia=competencia,
                id_caixa_sei=unidade_id,
                descricao=f"Solicitação de Pagamento - {competencia}",
                status_geral="AGUARDANDO_ASSINATURA" # Status provisório
            )
            db.session.add(nova_sol)
            db.session.commit()
            
            # 3. Prepara o Modal (NÃO REDIRECIONA AINDA)
            modal_abrir = True
            doc_id_formatado = doc_criado['DocumentoFormatado'] # Ex: 0004512
            unidade_atual = unidade_id
            proc_formatado = proc_criado['ProcedimentoFormatado']
            
            # Opcional: Flash message informativa
            flash(f'Processo {proc_formatado} criado. Insira sua senha para assinar.', 'info')

    return render_template('solicitacoes/nova.html', 
                           unidades=lista_unidades,
                           modal_abrir=modal_abrir,
                           doc_protocolo=doc_id_formatado,
                           unidade_atual=unidade_atual)


@solicitacoes_bp.route('/api/assinar_ajax', methods=['POST'])
def assinar_ajax():
    # 1. Segurança e Validação Básica
    if 'usuario_db_id' not in session: 
        return jsonify({'sucesso': False, 'erro': 'Sessão expirada'}), 401
    
    dados = request.json
    senha = dados.get('senha')
    protocolo_doc = dados.get('protocolo')
    unidade = dados.get('unidade')
    
    if not senha:
        return jsonify({'sucesso': False, 'erro': 'Senha obrigatória'})

    # 2. Recupera a solicitação pendente do usuário logado
    usuario_id = session['usuario_db_id']
    solicitacao = Solicitacao.query.filter_by(id_usuario_solicitante=usuario_id)\
                                   .order_by(Solicitacao.id.desc()).first()

    if not solicitacao:
         return jsonify({'sucesso': False, 'erro': 'Solicitação não encontrada para registrar histórico.'})

    # 3. Monta dados para o serviço de assinatura
    dados_ass = {
        'protocolo_doc': protocolo_doc,
        'orgao': session.get('usuario_orgao', 'SEAD-PI'),
        'cargo': session.get('usuario_cargo', 'Colaborador'),
        'id_login': session.get('usuario_sei_id_login'),
        'id_usuario': session.get('usuario_sei_id'),
        'senha': senha
    }
    
    token = session.get('sei_token')
    
    # 4. Tenta assinar via API SEI
    resultado = assinar_documento(token, unidade, dados_ass)
    
    if resultado['sucesso']:
        try:
            # === SUCESSO NA ASSINATURA ===
            
            # A. Atualiza Status da Solicitação
            if solicitacao.status_geral == 'AGUARDANDO_ASSINATURA':
                solicitacao.status_geral = 'ABERTO'
            
            # B. Cria o Histórico de Movimentação (Conforme sua DDL)
            novo_historico = HistoricoMovimentacao(
                id_solicitacao=solicitacao.id,       # FK para sis_solicitacoes
                id_etapa_anterior=None,              # NULL (primeira movimentação)
                id_etapa_nova=1,                     # FK para sis_etapas_fluxo (Certifique-se que ID 1 existe!)
                id_usuario_responsavel=usuario_id,   # FK para sis_usuarios
                data_movimentacao=datetime.now(),
                comentario=f"Processo criado e documento {protocolo_doc} assinado com sucesso."
            )
            
            registrar_e_atualizar_saldo(solicitacao, usuario_id, valor_empenho=0.0)
            db.session.add(novo_historico)
            db.session.commit() # Salva o status novo e o histórico numa transação só
            
            return jsonify({'sucesso': True})
            
        except Exception as e:
            db.session.rollback()
            print(f"Erro ao salvar histórico: {e}")
            return jsonify({'sucesso': False, 'erro': "Assinado, mas erro ao salvar histórico."})
    else:
        # Falha na assinatura (senha errada, etc)
        return jsonify({'sucesso': False, 'erro': resultado['erro']})


# API Melhorada para Busca em Tabela
@solicitacoes_bp.route('/api/contratos')
def api_buscar_contratos():
    if 'usuario_db_id' not in session: return jsonify([]), 401
    
    termo = request.args.get('q', '').strip()
    
    if not termo or len(termo) < 3:
        return jsonify({'results': []})

    query = Contrato.query.filter(
        or_(
            Contrato.codigo.like(f'%{termo}%'),
            Contrato.numeroOriginal.like(f'%{termo}%'),
            Contrato.nomeContratado.like(f'%{termo}%'),
            Contrato.numProcesso.like(f'%{termo}%')
        )
    )
    
    resultados = query.limit(20).all()
    
    dados = [{
        'codigo': c.codigo,
        'numeroOriginal': c.numeroOriginal,
        'nomeContratado': c.nomeContratado,
        'objeto': c.objeto[:100] + '...' if c.objeto else 'Sem objeto',
        'numProcesso': c.numProcesso
        
    } for c in resultados]
    
    return jsonify({'results': dados})


# Em app/solicitacoes/routes.py

@solicitacoes_bp.route('/solicitacao/<int:id_solicitacao>', methods=['GET', 'POST'])
def detalhes_solicitacao(id_solicitacao):
    if 'usuario_db_id' not in session: return redirect(url_for('auth.login'))
    
    solicitacao = Solicitacao.query.get_or_404(id_solicitacao)
    usuario_id = session['usuario_db_id']
    protocolo = solicitacao.protocolo_gerado_sei

    # --- CORREÇÃO AQUI: IDs de série como STRING ('420') ---
    # Isso garante que o banco encontre o registro corretamente
    mov_nl = SeiMovimentacao.query.filter_by(protocolo_procedimento=protocolo, id_serie='420').first()
    mov_pd = SeiMovimentacao.query.filter_by(protocolo_procedimento=protocolo, id_serie='421').first()
    mov_ob = SeiMovimentacao.query.filter_by(protocolo_procedimento=protocolo, id_serie='422').first()

    ultimo_empenho = SolicitacaoEmpenho.query.filter_by(id_solicitacao=id_solicitacao).order_by(SolicitacaoEmpenho.id.desc()).first()
    historico = HistoricoMovimentacao.query.filter_by(id_solicitacao=id_solicitacao).order_by(HistoricoMovimentacao.data_movimentacao.asc()).all()
    
    etapa_id = solicitacao.etapa_atual_id if solicitacao.etapa_atual_id else 1
    etapa_atual = Etapa.query.get(etapa_id)

    # --- LÓGICA DE ALERTAS (MANTIDA) ---
    substatus_alerta = None
    substatus_nomes = [
        "Aguardando Documentação",
        "Documentação Incompleta",
        "Aguardando Empenho",
        "Fiscais Notificados",
        "NF com pendência"
    ]

    if etapa_atual.nome in substatus_nomes:
        substatus_alerta = {
            'mensagem': etapa_atual.nome,
            'descricao': "Aguardando resolução desta pendência para avançar.",
            'tipo': 'warning'
        }
        if "Incompleta" in etapa_atual.nome or "pendência" in etapa_atual.nome:
            substatus_alerta['icone'] = 'bi-exclamation-triangle-fill'
            substatus_alerta['cor'] = 'text-warning'
        else:
            substatus_alerta['icone'] = 'bi-info-circle-fill'
            substatus_alerta['cor'] = 'text-info'
    
    # --- PROCESSAMENTO DO POST (MANTIDO) ---
    if request.method == 'POST':
        acao = request.form.get('acao')
        if not acao and request.is_json:
            dados_json = request.get_json()
            acao = dados_json.get('acao')
        
        comentario = request.form.get('comentario', '')
        nova_etapa_id = None
        msg_sucesso = ""
        erro = False

        # 1. DOCUMENTAÇÃO
        if etapa_atual.alias in ['aguardando_docs', 'criado', 'doc_incompleta']:
            if acao == 'doc_completa':
                nova_etapa_id = 8
                msg_sucesso = "Documentação Completa!"
            elif acao == 'doc_incompleta':
                nova_etapa_id = 7
                msg_sucesso = "Documentação Incompleta."

        # 3. NOTIFICAÇÃO DE FISCAIS
        elif etapa_atual.alias in ['doc_completa', 'empenhado'] and acao == 'notificar_fiscais':
            try:
                # Busca Fiscais
                sql_fiscais = text("SELECT nome, telefone FROM sgc.fiscais_contrato WHERE codigo_contrato = :cod AND telefone IS NOT NULL")
                fiscais = db.session.execute(sql_fiscais, {'cod': solicitacao.codigo_contrato}).fetchall()
                
                enviados_count = 0
                
                if not fiscais:
                    msg_sucesso = "Nenhum fiscal com telefone encontrado. Fase avançada manualmente."
                else:
                    # Lógica de envio (Telegram + Email) mantida integralmente...
                    contrato_info = solicitacao.contrato
                    nome_resumido = contrato_info.nomeContratadoResumido or contrato_info.nomeContratado
                    link_sei = solicitacao.link_processo_sei
                    num_original = contrato_info.numeroOriginal
                    
                    msg_telegram = (
                        f"🔔 *Aviso de Pagamento - SGC*\n\n"
                        f"Olá! O processo de pagamento do contrato *{nome_resumido}* (Nº {num_original}) "
                        f"teve o Empenho Realizado.\n\n"
                        f"Por favor, verifique a documentação e o atesto no SEI.\n"
                        f"🔗 Link: {link_sei}"
                    )

                    api_id = 33442091
                    api_hash = '87c21695c8217183cd9b508234994dea'
                    session_path = os.path.join(os.getcwd(), 'api_telegram', 'minha_sessao') 

                    async def enviar_telegram():
                        async with TelegramClient(session_path, api_id, api_hash) as client:
                            for fiscal in fiscais:
                                telefone = fiscal.telefone
                                if not telefone.startswith('+'):
                                    telefone = '+55' + ''.join(filter(str.isdigit, telefone))
                                try:
                                    await client.send_message(telefone, msg_telegram)
                                except Exception as e:
                                    print(f"Erro ao enviar para {fiscal.nome}: {e}")
                                
                    print("Iniciando disparo manual de e-mails...")
                    disparar_notificacao_email(solicitacao)
                    asyncio.run(enviar_telegram())
                    enviados_count = len(fiscais)
                    msg_sucesso = f"Notificações enviadas para {enviados_count} fiscais via Telegram! E-mails de teste disparados."

                nova_etapa_id = 12 
                
            except Exception as e:
                erro = True
                msg_sucesso = f"Erro ao notificar: {str(e)}"
                print(e)

        # 4. NOTA FISCAL
        elif etapa_atual.alias in ['notificacao_enviada', 'nf_pendente']:
            if acao == 'nf_atestada':
                nova_etapa_id = 11
                msg_sucesso = "Nota Fiscal Atestada!"
            elif acao == 'nf_pendente':
                nova_etapa_id = 10
                msg_sucesso = "Pendência registrada na NF."

        # 5. LIQUIDAÇÃO E PAGAMENTO
        elif etapa_atual.alias == 'nf_atestada' and acao == 'realizar_liquidacao':
            nova_etapa_id = 5
            msg_sucesso = "Liquidado!"
        elif etapa_atual.alias == 'liquidado' and acao == 'realizar_pagamento':
            nova_etapa_id = 6
            msg_sucesso = "Pago!"

        # FINALIZAÇÃO DO POST
        if nova_etapa_id and not erro:
            novo_mov = HistoricoMovimentacao(
                id_solicitacao=solicitacao.id, id_etapa_anterior=etapa_atual.id, 
                id_etapa_nova=nova_etapa_id, id_usuario_responsavel=usuario_id, comentario=comentario or msg_sucesso
            )
            solicitacao.etapa_atual_id = nova_etapa_id
            db.session.add(novo_mov)
            db.session.commit()
            
            etapa_atual = Etapa.query.get(nova_etapa_id)
            historico = HistoricoMovimentacao.query.filter_by(id_solicitacao=id_solicitacao).order_by(HistoricoMovimentacao.data_movimentacao.asc()).all()
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            if erro: return jsonify({'sucesso': False, 'msg': msg_sucesso})
            historico_atualizado = HistoricoMovimentacao.query.filter_by(id_solicitacao=id_solicitacao).order_by(HistoricoMovimentacao.data_movimentacao.asc()).all()
            timeline_data_ajax = gerar_dados_timeline(solicitacao, historico_atualizado)

            html_timeline = render_template('solicitacoes/partials/timeline_content.html', 
                                            solicitacao=solicitacao, etapa_atual=etapa_atual, historico=historico,timeline_data=timeline_data_ajax)
            html_acoes = render_template('solicitacoes/partials/acoes_content.html', 
                                         solicitacao=solicitacao, etapa_atual=etapa_atual)
            return jsonify({'sucesso': True, 'msg': msg_sucesso, 'html_timeline': html_timeline, 'html_acoes': html_acoes})

        if not erro: flash(msg_sucesso, 'success')
        return redirect(url_for('solicitacoes.detalhes_solicitacao', id_solicitacao=id_solicitacao))
    
    timeline_data = gerar_dados_timeline(solicitacao, historico)

    # --- RETORNO COM AS VARIÁVEIS FINANCEIRAS ---
    return render_template(
        'solicitacoes/detalhes.html', 
        solicitacao=solicitacao, 
        historico=historico, 
        etapa_atual=etapa_atual,
        timeline_data=timeline_data,
        substatus_alerta=substatus_alerta,
        ultimo_empenho=ultimo_empenho,
        mov_nl=mov_nl, # Passando o objeto NL
        mov_pd=mov_pd, # Passando o objeto PD
        mov_ob=mov_ob  # Passando o objeto OB
    )



@solicitacoes_bp.route('/pendencias-ne', methods=['GET', 'POST'])
def pendencias_ne():
    if 'usuario_db_id' not in session: return redirect(url_for('auth.login'))
    usuario_id = session['usuario_db_id']

    # POST: Validar NE
    if request.method == 'POST':
        id_solicitacao = request.form.get('id_solicitacao')
        ne_digitada = request.form.get('ne')
        
        if id_solicitacao and ne_digitada:
            solicitacao = Solicitacao.query.get(id_solicitacao)
            
            # Validação SIAFE (Mantida)
            resultado = validar_ne_siafe(ne_digitada, solicitacao.codigo_contrato)
            if not resultado['sucesso']:
                flash(resultado['mensagem'], resultado['categoria'])
                return redirect(url_for('solicitacoes.pendencias_ne'))
            
            # Atualiza tabela de empenho (Pega o último sem NE ou o último criado)
            # Filtramos pelo que não tem NE para garantir que estamos atualizando o pendente
            empenho = SolicitacaoEmpenho.query.filter_by(id_solicitacao=id_solicitacao, ne=None).first()
            
            # Fallback: se não achar por NE None, pega o último ID (segurança)
            if not empenho:
                empenho = SolicitacaoEmpenho.query.filter_by(id_solicitacao=id_solicitacao).order_by(SolicitacaoEmpenho.id.desc()).first()

            if empenho: 
                empenho.ne = ne_digitada
            
            # ATUALIZA STATUS PARA 'ATENDIDO' (ID 2)
            solicitacao.status_empenho_id = 2
            
            # Opcional: Registra no histórico
            mov = HistoricoMovimentacao(
                id_solicitacao=solicitacao.id, 
                id_etapa_anterior=solicitacao.etapa_atual_id, 
                id_etapa_nova=solicitacao.etapa_atual_id,
                id_usuario_responsavel=usuario_id,
                comentario=f"NE {ne_digitada} inserida. Status de Empenho atualizado para Atendido."
            )
            db.session.add(mov)
            db.session.commit()
            
            nome_resumido = solicitacao.contrato.nomeContratadoResumido or solicitacao.contrato.nomeContratado
            flash(f"NE Vinculada! | {nome_resumido}", 'success')
        
        return redirect(url_for('solicitacoes.pendencias_ne'))

    # GET: Listagem CORRIGIDA
    # Retorna (Solicitacao, SolicitacaoEmpenho) para satisfazer o template
    query = db.session.query(Solicitacao, SolicitacaoEmpenho).join(
        SolicitacaoEmpenho, Solicitacao.id == SolicitacaoEmpenho.id_solicitacao
    ).join(
        Contrato, Solicitacao.codigo_contrato == Contrato.codigo
    ).filter(
        Solicitacao.status_empenho_id == 1,   # Apenas Status 'Solicitado'
        SolicitacaoEmpenho.ne.is_(None)       # Apenas o empenho que ainda não tem número
    )

    # Filtros de busca (Mantidos)
    search_q = request.args.get('q', '').strip()
    if search_q:
        query = query.filter(
            or_(
                Contrato.codigo.like(f'%{search_q}%'), 
                Contrato.numeroOriginal.like(f'%{search_q}%')
            )
        )
    
    pendencias = query.all()

    # Popular listas para o filtro lateral
    contratados_pendentes = db.session.query(Contrato.nomeContratado)\
        .join(Solicitacao).filter(Solicitacao.status_empenho_id == 1)\
        .distinct().order_by(Contrato.nomeContratado).all()
    
    todos_contratados = [c[0] for c in contratados_pendentes]
    
    return render_template('solicitacoes/pendencias_ne.html', 
                           pendencias=pendencias, 
                           todos_contratados=todos_contratados)



def processar_item_sei(app_obj, sol_id, token_sei, usuario_id, mapa_ordem):
    """
    Lógica Unificada (Baseada no processar_etapas_offline.py).
    Processa documentos do SEI e atualiza etapas/status com lógica de correção e avanço.
    """
    with app_obj.app_context():
        sol = Solicitacao.query.get(sol_id)
        if not sol or not sol.protocolo_gerado_sei:
            return None

        # ==============================================================================
        # 1. CONFIGURAÇÕES E CONSTANTES
        # ==============================================================================
        SERIE_SOLICITACAO = '2614'
        SERIE_EMAIL = '30'
        SERIE_REQUERIMENTO = '64'   # GATILHO: Etapa 8 e Etapa 12
        SERIE_ATESTO_FISCAL = '461' 
        SERIE_ATESTO_GESTOR = '464' 
        SERIE_LIQUIDACAO = '420'
        SERIE_PD = '421'
        SERIE_OB = '422'
        
        MAPA_ORDEM_LOCAL = {
            1: 1,   # Solicitação Criada
            2: 2,   # Documentação Solicitada
            8: 3,   # Documentação Recebida
            15: 4,  # Solicitação da NF
            12: 5,  # Fiscais Notificados
            13: 6,  # Contrato Fiscalizado
            14: 7,  # Atestado pelo Controle Interno
            11: 8,  # NF Atestada
            5: 9,   # Liquidado
            6: 10   # Pago
        }
        
        mudou = False

        # ==============================================================================
        # 2. FUNÇÕES AUXILIARES INTERNAS
        # ==============================================================================
        
        def extrair_data_segura(doc_obj):
            if not doc_obj or not doc_obj.data: return None
            try:
                raw = str(doc_obj.data).strip().replace("'", "").replace('"', "")
                data_str = raw.split(' ')[0]
                for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
                    try: return datetime.strptime(data_str, fmt)
                    except ValueError: continue
                return None
            except (AttributeError, TypeError):
                return None

        def registrar_historico_forcado(id_etapa, data_doc, msg):
            if not data_doc: return False
            hist = HistoricoMovimentacao.query.filter_by(id_solicitacao=sol.id, id_etapa_nova=id_etapa).first()
            if hist:
                data_banco = hist.data_movimentacao
                if data_banco is None or data_banco.date() != data_doc.date():
                    hist.data_movimentacao = data_doc
                    hist.comentario = msg
                    return True
                return False
            novo = HistoricoMovimentacao(
                id_solicitacao=sol.id, id_etapa_anterior=sol.etapa_atual_id,
                id_etapa_nova=id_etapa, id_usuario_responsavel=usuario_id,
                data_movimentacao=data_doc, comentario=msg
            )
            db.session.add(novo)
            return True

        def tentar_avancar_status(id_nova_etapa):
            ordem_atual = MAPA_ORDEM_LOCAL.get(sol.etapa_atual_id, 0)
            ordem_nova = MAPA_ORDEM_LOCAL.get(id_nova_etapa, 0)
            if ordem_nova > ordem_atual:
                sol.etapa_atual_id = id_nova_etapa
                return True
            return False

        def calcular_tempo_total(data_fim):
            if not sol.data_solicitacao or not data_fim: return
            if sol.tempo_total: return
            try:
                diferenca = data_fim - sol.data_solicitacao
                sol.tempo_total = f"{diferenca.days} dias"
            except (TypeError, AttributeError):
                pass  # Tipos incompatíveis para subtração de datas

        # ==============================================================================
        # 3. CARREGAMENTO DOS DADOS
        # ==============================================================================
        try:
            # Busca docs filtrando pelo PROTOCOLO DO PROCEDIMENTO (Conforme solicitado)
            docs = SeiMovimentacao.query.filter_by(
                protocolo_procedimento=sol.protocolo_gerado_sei
            ).order_by(SeiMovimentacao.id_documento.asc()).all()

            if not docs: return None

            ids_series = [str(d.id_serie) for d in docs]
            emails = [d for d in docs if str(d.id_serie) == SERIE_EMAIL]

            # ==========================================================================
            # 4. LÓGICA DE NEGÓCIO
            # ==========================================================================

            # --- A. ETAPA 8 e 12: DOCUMENTAÇÃO RECEBIDA (Requerimento - 64) ---
            if SERIE_REQUERIMENTO in ids_series:
                doc_req = next((d for d in docs if str(d.id_serie) == SERIE_REQUERIMENTO), None)
                dt = extrair_data_segura(doc_req)
                if registrar_historico_forcado(8, dt, "Documentação Recebida (Req. 64)"): mudou = True
                if registrar_historico_forcado(12, dt, "Fiscais Notificados (Automático)"): mudou = True
                if tentar_avancar_status(12): mudou = True
                elif tentar_avancar_status(8): mudou = True 

            # --- B. ETAPA 11: NF ATESTADA ---
            if len(emails) >= 2:
                try:
                    segundo_email = emails[1]
                    idx_2_email = docs.index(segundo_email)
                    doc_alvo = None
                    for i in range(idx_2_email + 1, len(docs)):
                        if str(docs[i].id_serie) != SERIE_EMAIL:
                            doc_alvo = docs[i]
                            break
                    if doc_alvo:
                        dt = extrair_data_segura(doc_alvo)
                        if registrar_historico_forcado(11, dt, f"NF Atestada (Doc {doc_alvo.serie_nome})"):
                            mudou = True; tentar_avancar_status(11)
                except ValueError: pass

            # --- C. FINANCEIRO (OB > PD > NL) ---
            doc_nl = next((d for d in docs if str(d.id_serie) == SERIE_LIQUIDACAO), None)
            doc_pd = next((d for d in docs if str(d.id_serie) == SERIE_PD), None)
            doc_ob = next((d for d in docs if str(d.id_serie) == SERIE_OB), None)

            # --- ATUALIZAÇÃO DOS NÚMEROS (NL, PD, OB) ---
            # Agora capturando da coluna 'numero' da tabela SeiMovimentacao
            
            if doc_nl and doc_nl.numero and sol.num_nl != str(doc_nl.numero):
                sol.num_nl = str(doc_nl.numero)
                mudou = True
            
            if doc_pd and doc_pd.numero and sol.num_pd != str(doc_pd.numero):
                sol.num_pd = str(doc_pd.numero)
                mudou = True
                
            if doc_ob and doc_ob.numero and sol.num_ob != str(doc_ob.numero):
                sol.num_ob = str(doc_ob.numero)
                mudou = True

            # Lógica de Avanço Financeiro
            fin_etapa = None; fin_doc = None; msg_fin = ""

            if doc_ob:
                fin_etapa = 6; fin_doc = doc_ob; msg_fin = f"Pago (OB {sol.num_ob})"
                if sol.status_geral != 'PAGO': sol.status_geral = 'PAGO'; mudou = True
                dt_ob = extrair_data_segura(doc_ob)
                if registrar_historico_forcado(5, dt_ob, "Liquidado (Via Pagamento)"): mudou = True
                calcular_tempo_total(dt_ob)

            elif doc_pd:
                fin_etapa = 5; fin_doc = doc_pd; msg_fin = f"Programado (PD {sol.num_pd})"
                if sol.status_geral != 'EM LIQUIDAÇÃO': sol.status_geral = 'EM LIQUIDAÇÃO'; mudou = True

            elif doc_nl:
                fin_etapa = 5; fin_doc = doc_nl; msg_fin = f"Liquidado (NL {sol.num_nl})"
                if sol.status_geral != 'EM LIQUIDAÇÃO': sol.status_geral = 'EM LIQUIDAÇÃO'; mudou = True

            if fin_etapa:
                dt = extrair_data_segura(fin_doc)
                if registrar_historico_forcado(fin_etapa, dt, msg_fin):
                    mudou = True; tentar_avancar_status(fin_etapa)

            # --- D. OUTRAS ETAPAS ---
            if SERIE_SOLICITACAO in ids_series:
                d = next(d for d in docs if str(d.id_serie) == SERIE_SOLICITACAO)
                if registrar_historico_forcado(1, extrair_data_segura(d), "Solicitação Criada"):
                    mudou = True; tentar_avancar_status(1)
            
            if len(emails) >= 1:
                if registrar_historico_forcado(2, extrair_data_segura(emails[0]), "Doc Solicitada"):
                    mudou = True; tentar_avancar_status(2)
            
            if len(emails) >= 2:
                if registrar_historico_forcado(15, extrair_data_segura(emails[1]), "Solicitação NF"):
                    mudou = True; tentar_avancar_status(15)

            if SERIE_ATESTO_GESTOR in ids_series:
                d = next(d for d in docs if str(d.id_serie) == SERIE_ATESTO_GESTOR)
                if registrar_historico_forcado(13, extrair_data_segura(d), "Atesto Gestor"):
                    mudou = True; tentar_avancar_status(13)

            if SERIE_ATESTO_FISCAL in ids_series:
                d = next(d for d in docs if str(d.id_serie) == SERIE_ATESTO_FISCAL)
                if registrar_historico_forcado(14, extrair_data_segura(d), "Atesto Fiscal"):
                    mudou = True; tentar_avancar_status(14)

            # ==========================================================================
            # 5. FINALIZAÇÃO
            # ==========================================================================
            if mudou:
                db.session.commit()
                return sol.protocolo_gerado_sei
            
            return None

        except Exception as e:
            db.session.rollback()
            print(f"Erro processamento {sol_id}: {e}")
            return None


@solicitacoes_bp.route('/api/atualizar-etapas-sei', methods=['GET'])
def atualizar_etapas_sei():
    """
    FASE 2:
    Dispara threads que leem a tabela seimovimentacao e aplicam as regras de negócio
    para mover os cards na timeline.
    
    ATUALIZAÇÃO: Ignora processos com etapa_atual_id == 6 (PAGO/OB).
    """
    if 'usuario_db_id' not in session: 
        return jsonify({'sucesso': False, 'msg': 'Sessão expirada.'}), 401

    usuario_id = session['usuario_db_id']
    token_sei = session.get('sei_token')

    # Pega o objeto app real para passar para as threads (necessário no Flask)
    app_real = current_app._get_current_object()

    def gerar_atualizacoes():
        # 1. Prepara dados para as threads
        # Pega todas as etapas para saber a ordem (1 < 2 < 3...)
        todas_etapas = Etapa.query.all()
        # Mapeia ID da etapa -> Ordem (ex: Etapa 6 -> Ordem 10)
        mapa_ordem = {e.id: e.ordem for e in todas_etapas}
        
        # --- FILTRO OTIMIZADO ---
        # Busca apenas IDs de solicitações que NÃO estão na etapa 6 (Pago)
        # e que NÃO estão canceladas (boa prática manter)
        ids_para_processar = [
            s.id for s in Solicitacao.query.filter(
                Solicitacao.etapa_atual_id != 6,
                Solicitacao.status_geral != 'CANCELADO'
            ).all()
        ]
        
        total = len(ids_para_processar)
        
        if total == 0:
            yield f"data: {json.dumps({'progresso': 100, 'msg': 'Todos os processos ativos já estão atualizados ou finalizados.', 'concluido': True})}\n\n"
            return

        yield f"data: {json.dumps({'progresso': 0, 'msg': f'Iniciando análise de {total} processos pendentes...'})}\n\n"

        # 2. Inicia Threads Paralelas (Performance)
        # A função 'processar_item_sei' é aquela que definimos anteriormente
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_id = {
                executor.submit(
                    processar_item_sei, 
                    app_real, 
                    sid, 
                    token_sei, 
                    usuario_id, 
                    mapa_ordem
                ): sid for sid in ids_para_processar
            }
            
            completed_count = 0
            atualizados_count = 0

            for future in concurrent.futures.as_completed(future_to_id):
                completed_count += 1
                percentual = int((completed_count / total) * 100)
                
                try:
                    resultado = future.result()
                    if resultado: # Se retornou protocolo (string), é porque houve mudança
                        atualizados_count += 1
                        msg = f"Movimentado: {resultado}"
                        yield f"data: {json.dumps({'progresso': percentual, 'msg': msg})}\n\n"
                    else:
                        # Loga apenas periodicamente para economizar banda do navegador
                        if completed_count % 5 == 0:
                            yield f"data: {json.dumps({'progresso': percentual, 'msg': f'Analisando... {completed_count}/{total}'})}\n\n"
                            
                except Exception as exc:
                    print(f"Erro na thread: {exc}")
                    yield f"data: {json.dumps({'progresso': percentual, 'msg': 'Erro ao processar um item.'})}\n\n"

        yield f"data: {json.dumps({'progresso': 100, 'msg': f'Concluído! {atualizados_count} processos avançaram de etapa.', 'concluido': True})}\n\n"

    return Response(stream_with_context(gerar_atualizacoes()), mimetype='text/event-stream')


def processar_saldo_thread(app_obj, sol_id, usuario_id):
    """Executa o cálculo de saldo em uma thread separada com seu próprio contexto."""
    with app_obj.app_context():
        sol = Solicitacao.query.get(sol_id)
        if sol:
            # Reutiliza a sua função lógica já existente
            return registrar_e_atualizar_saldo(sol, usuario_id, valor_solicitado=0.0)
    return False

@solicitacoes_bp.route('/api/sincronizar-seimovimentacao', methods=['GET'])
def sincronizar_seimovimentacao():
    if 'usuario_db_id' not in session: 
        return jsonify({'sucesso': False, 'msg': 'Login necessário'}), 401

    app_real = current_app._get_current_object()

    def stream_sincronizacao():
        with app_real.app_context():
            yield f"data: {json.dumps({'progresso': 5, 'msg': 'Autenticando...'})}\n\n"
            token_sei = gerar_token_sei_admin()
            
            if not token_sei:
                yield f"data: {json.dumps({'progresso': 0, 'msg': 'Erro Fatal: Falha ao gerar Token Admin.'})}\n\n"
                return

            # --- LÓGICA DE FILTRO INTELIGENTE ---
            # Busca apenas solicitações que NÃO estão finalizadas (Etapa 6)
            # E que possuem protocolo válido
            solicitacoes_pendentes = Solicitacao.query.filter(
                Solicitacao.protocolo_gerado_sei.isnot(None),
                Solicitacao.etapa_atual_id != 6,
                Solicitacao.status_geral != 'CANCELADO'
            ).all()
            
            total = len(solicitacoes_pendentes)
            
            if total == 0:
                yield f"data: {json.dumps({'progresso': 100, 'msg': 'Todos os processos já estão concluídos. Nada a sincronizar.', 'concluido': True})}\n\n"
                return

            # Extrai a lista de protocolos para limpeza cirúrgica
            lista_protocolos = [s.protocolo_gerado_sei for s in solicitacoes_pendentes]

            # --- LIMPEZA SELETIVA ---
            yield f"data: {json.dumps({'progresso': 10, 'msg': f'Limpando dados de {total} processos em andamento...'})}\n\n"
            try:
                # Deleta apenas as movimentações dos processos que serão atualizados
                # O parâmetro synchronize_session=False é importante para performance em deletes em massa
                db.session.query(SeiMovimentacao).filter(
                    SeiMovimentacao.protocolo_procedimento.in_(lista_protocolos)
                ).delete(synchronize_session=False)
                
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                yield f"data: {json.dumps({'progresso': 0, 'msg': f'Erro ao limpar banco: {str(e)}'})}\n\n"
                return

            base_url = "https://api.sei.pi.gov.br/v1/unidades/110006213/procedimentos/documentos"
            
            # --- DOWNLOAD APENAS DOS PENDENTES ---
            yield f"data: {json.dumps({'progresso': 15, 'msg': 'Iniciando download atualizado...'})}\n\n"
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_prot = {
                    executor.submit(baixar_documentos_thread, app_real, s.protocolo_gerado_sei, token_sei, base_url): s.protocolo_gerado_sei 
                    for s in solicitacoes_pendentes
                }
                
                completed = 0
                sucessos = 0
                
                for future in concurrent.futures.as_completed(future_to_prot):
                    protocolo = future_to_prot[future]
                    completed += 1
                    percentual = 15 + int((completed / total) * 85)
                    
                    try:
                        sucesso, mensagem = future.result()
                        
                        if sucesso:
                            sucessos += 1
                            if completed % 5 == 0:
                                yield f"data: {json.dumps({'progresso': percentual, 'msg': f'Baixando... ({completed}/{total})'})}\n\n"
                        else:
                            yield f"data: {json.dumps({'progresso': percentual, 'msg': f'[ALERTA] {mensagem}'})}\n\n"
                            
                    except Exception as exc:
                        yield f"data: {json.dumps({'progresso': percentual, 'msg': f'Erro Crítico {protocolo}: {str(exc)}'})}\n\n"

            yield f"data: {json.dumps({'progresso': 100, 'msg': f'Sincronização Inteligente Concluída! {sucessos}/{total} atualizados.', 'concluido': True})}\n\n"

    return Response(stream_with_context(stream_sincronizacao()), mimetype='text/event-stream')

@solicitacoes_bp.route('/api/atualizar-todos-saldos', methods=['POST'])
def atualizar_todos_saldos():
    usuario_id = session.get('usuario_db_id')
    app_real = current_app._get_current_object()

    def gerar_stream():
        # Busca todas as solicitações (removido filtro de etapa para pegar as 633)
        solicitacoes = Solicitacao.query.filter_by(id_usuario_solicitante=usuario_id).all()
        total = len(solicitacoes)
        
        # Usamos um Set para não atualizar o mesmo contrato/competência várias vezes na mesma leva
        tarefas_unicas = {(s.codigo_contrato, s.competencia) for s in solicitacoes}
        total_tarefas = len(tarefas_unicas)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(task_atualizar_saldo, app_real, c, comp) for c, comp in tarefas_unicas]
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                progresso = int(((i + 1) / total_tarefas) * 100)
                yield f"data: {json.dumps({'progresso': progresso, 'msg': f'Processando contrato {i+1} de {total_tarefas}'})}\n\n"
        
        yield f"data: {json.dumps({'progresso': 100, 'msg': 'Atualização concluída!', 'concluido': True})}\n\n"

    return Response(stream_with_context(gerar_stream()), mimetype='text/event-stream')


@solicitacoes_bp.route('/relatorios')
def relatorios():
    if 'usuario_db_id' not in session: return redirect(url_for('auth.login'))
    
    # --- HELPER: Formata tempo ---
    def formatar_delta_str(diff):
        """
        Retorna dias inteiros.
        Protege contra valores negativos e arredonda 0 dias para 1 se houver horas significativas.
        """
        dias = diff.days
        if dias < 0: return "0d"
        # Opcional: Se for 0 dias mas tiver horas, pode mostrar algo ou manter 0d
        return f"{dias}d"
    
    # --- 1. FILTROS E PARÂMETROS ---
    filtro_contratado = request.args.get('contratado', '').strip()
    filtro_competencia = request.args.get('competencia', '').strip()
    data_inicio = request.args.get('data_inicio')
    data_fim = request.args.get('data_fim')
    
    page_estoque = request.args.get('page_estoque', 1, type=int)
    page_matriz = request.args.get('page_matriz', 1, type=int)
    per_page = 20 

    # --- 2. DADOS AUXILIARES (Filtros) ---
    contratados_db = db.session.query(Contrato.nomeContratado).join(Solicitacao).distinct().order_by(Contrato.nomeContratado).all()
    todos_contratados = [c[0] for c in contratados_db]

    competencias_db = db.session.query(Solicitacao.competencia).distinct().filter(Solicitacao.competencia.isnot(None)).all()
    todas_competencias = [c[0] for c in competencias_db if c[0]]

    def chave_ordenacao(comp_str):
        meses = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12}
        try:
            partes = comp_str.split('/')
            if len(partes) == 2: return (int(partes[1]), meses.get(partes[0].capitalize(), 0))
        except (ValueError, IndexError, AttributeError):
            pass  # Retorna valor padrão para ordenação
        return (0, 0)
    todas_competencias.sort(key=chave_ordenacao, reverse=True)

    # --- 3. HELPER DE QUERY (Reutilizável) ---
    def aplicar_filtros_globais(query_obj, model_solicitacao=Solicitacao, model_contrato=Contrato):
        if filtro_contratado: query_obj = query_obj.filter(model_contrato.nomeContratado.like(f'%{filtro_contratado}%'))
        if filtro_competencia: query_obj = query_obj.filter(model_solicitacao.competencia == filtro_competencia)
        if data_inicio:
            try:
                dt_ini = datetime.strptime(data_inicio, '%Y-%m-%d')
                query_obj = query_obj.filter(model_solicitacao.data_solicitacao >= dt_ini)
            except ValueError:
                pass  # Data inválida, ignora filtro
        if data_fim:
            try:
                dt_fim = datetime.strptime(data_fim, '%Y-%m-%d'); dt_fim = dt_fim.replace(hour=23, minute=59, second=59)
                query_obj = query_obj.filter(model_solicitacao.data_solicitacao <= dt_fim)
            except ValueError:
                pass  # Data inválida, ignora filtro
        return query_obj

    # =========================================================================
    # CONFIGURAÇÃO DOS 7 CHECKPOINTS
    # =========================================================================
    todas_etapas_db = Etapa.query.all()
    mapa_nome_id = {e.nome.strip().lower(): e.id for e in todas_etapas_db}
    
    CHECKPOINTS = [
        {'label': 'Solicitação Criada', 'ids': [1], 'tipo': 'single', 'cor': '#0d6efd'},
        {'label': 'Documentação Solicitada', 'ids': [2, 7], 'tipo': 'single', 'cor': '#6610f2'},
        {'label': 'Documentação Recebida', 'ids': [3, 8], 'tipo': 'single', 'cor': '#6f42c1'},
        {'label': 'Solicitação da NF', 'ids': [4, 10], 'tipo': 'single', 'cor': '#d63384'},
        {'label': 'Atesto e Fiscalização', 'ids': [12, 13, 14], 'tipo': 'group', 'cor': '#fd7e14'},
        {'label': 'NF Atestada', 'ids': [11, 15], 'tipo': 'single', 'cor': '#ffc107'},
        {'label': 'Financeiro', 'ids': [5, 6], 'tipo': 'group', 'cor': '#198754'}
    ]

    # Mapeamento ID -> Índice da Coluna
    mapa_cp_idx = {}
    
    # 1. Pela lista oficial
    for idx, cp in enumerate(CHECKPOINTS):
        for eid in cp['ids']: mapa_cp_idx[eid] = idx
        
    # 2. Reforço por nomes (Fallback)
    ids_adicionais_nomes = {
        'solicitação criada': 0, 'documentação solicitada': 1, 'documentação incompleta': 1,
        'documentação recebida': 2, 'documentação completa': 2, 'aguardando empenho': 2,
        'solicitação da nf': 3, 'empenho realizado': 3, 'nf com pendência': 3,
        'nf atestada': 5, 'liquidado': 6, 'pago': 6
    }
    for nome, idx in ids_adicionais_nomes.items():
        fid = mapa_nome_id.get(nome)
        if fid: mapa_cp_idx[fid] = idx

    # Query Base
    query_base = Solicitacao.query.join(Etapa).join(Contrato)
    query_base = aplicar_filtros_globais(query_base)


    # === ABA 1: VISÃO GERAL (BIG NUMBERS) ===
    
    # Busca a contagem global (sem paginação) para os cards
    query_contagem = db.session.query(Solicitacao.etapa_atual_id, func.count(Solicitacao.id))\
        .join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
    query_contagem = aplicar_filtros_globais(query_contagem)
    dados_contagem = query_contagem.group_by(Solicitacao.etapa_atual_id).all()
    
    mapa_contagem_bruta = {r[0]: r[1] for r in dados_contagem}
    
    resumo_agrupado = []
    for idx, cp in enumerate(CHECKPOINTS):
        qtd_total = sum(mapa_contagem_bruta.get(eid, 0) for eid in cp['ids'])
        resumo_agrupado.append({
            'nome': cp['label'],
            'cor': cp.get('cor', '#6c757d'),
            'qtd': qtd_total,
            'eh_grupo': (cp['tipo'] == 'group')
        })

    # Tabela Estoque (Paginada)
    pagination_estoque = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc())\
        .paginate(page=page_estoque, per_page=per_page, error_out=False)
    detalhes_estoque = pagination_estoque.items


    # === ABA 2: MÉTRICAS (LÓGICA CORRIGIDA) ===

    colunas_matriz = [{'id': i, 'nome': cp['label']} for i, cp in enumerate(CHECKPOINTS)]
    agora = datetime.now()

    # =========================================================================
    # HELPER: Calcula tempos de permanência em cada fase para uma solicitação
    # Reutilizado tanto para o gráfico global quanto para a tabela paginada
    # =========================================================================
    def calcular_tempos_processo(sol):
        hist = HistoricoMovimentacao.query.filter_by(id_solicitacao=sol.id)\
            .order_by(HistoricoMovimentacao.data_movimentacao.asc()).all()
        tempos = {}

        # Identifica a fase atual do processo e se está finalizado
        idx_fase_atual = mapa_cp_idx.get(sol.etapa_atual_id)
        eh_finalizado = (sol.status_geral in ['PAGO', 'CONCLUIDO', 'CANCELADO']) if sol.status_geral else False
        if idx_fase_atual == 6: eh_finalizado = True

        for i in range(len(hist)):
            m_atual = hist[i]
            idx_coluna = mapa_cp_idx.get(m_atual.id_etapa_nova)

            if idx_coluna is not None:
                if i < len(hist) - 1:
                    dt_saida = hist[i+1].data_movimentacao
                else:
                    # Última movimentação registrada:
                    if eh_finalizado:
                        dt_saida = m_atual.data_movimentacao  # Relógio parado
                    elif idx_fase_atual is not None and idx_fase_atual != idx_coluna:
                        # Processo avançou além desta fase sem registro intermediário
                        dt_saida = m_atual.data_movimentacao
                    else:
                        dt_saida = agora  # Processo parado aqui, conta até agora

                delta = dt_saida - m_atual.data_movimentacao
                if delta.total_seconds() < 0: delta = timedelta(0)
                tempos[idx_coluna] = tempos.get(idx_coluna, timedelta()) + delta

        # Correção: fase atual sem registro no histórico (processo parado)
        if idx_fase_atual is not None and idx_fase_atual not in tempos and not eh_finalizado:
            if hist:
                dt_entrada_fase = hist[-1].data_movimentacao
            else:
                dt_entrada_fase = sol.data_solicitacao or agora
            delta = agora - dt_entrada_fase
            if delta.total_seconds() < 0: delta = timedelta(0)
            tempos[idx_fase_atual] = delta

        return tempos

    # =========================================================================
    # PASSO 1: TIMELINE GLOBAL (Usa TODOS os processos, sem paginação)
    # Isso garante que as médias reflitam o fluxo completo, incluindo fases
    # avançadas como NF Atestada e Financeiro.
    # =========================================================================
    todos_processos_metricas = query_base.all()
    acumulador_medias = {i: [] for i in range(len(CHECKPOINTS))}

    for sol in todos_processos_metricas:
        tempos = calcular_tempos_processo(sol)
        for idx_coluna, delta in tempos.items():
            acumulador_medias[idx_coluna].append(delta)

    # Calcula as médias finais para a Timeline
    timeline_medias = []
    for idx, cp in enumerate(CHECKPOINTS):
        item_visual = {'id': idx, 'nome': cp['label'], 'cor': cp.get('cor', '#6c757d'), 'eh_grupo': (cp['tipo'] == 'group')}

        lista_duracoes = acumulador_medias.get(idx, [])
        media_str = "--"
        qtd = 0

        if lista_duracoes:
            qtd = len(lista_duracoes)
            media = sum(lista_duracoes, timedelta()) / qtd
            media_str = formatar_delta_str(media)

        item_visual['media_entrada'] = media_str
        item_visual['qtd_base'] = qtd
        timeline_medias.append(item_visual)

    # =========================================================================
    # PASSO 2: TABELA MATRIZ (Paginada - apenas os itens da página atual)
    # =========================================================================
    pagination_matriz = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc())\
        .paginate(page=page_matriz, per_page=per_page, error_out=False)
    detalhes_matriz = pagination_matriz.items

    matriz_tempos = []
    for sol in detalhes_matriz:
        tempos = calcular_tempos_processo(sol)
        tempos_fmt = {k: formatar_delta_str(v) for k, v in tempos.items()}
        matriz_tempos.append({'solicitacao': sol, 'tempos': tempos_fmt})

    return render_template('solicitacoes/relatorios.html', 
                           resumo=resumo_agrupado,
                           detalhes=detalhes_estoque,
                           pagination=pagination_estoque,
                           pagination_matriz=pagination_matriz,
                           matriz_tempos=matriz_tempos,
                           colunas_etapas=colunas_matriz,
                           timeline_medias=timeline_medias, # Passando a timeline corrigida
                           todos_contratados=todos_contratados,
                           todas_competencias=todas_competencias,
                           request=request)



# No final de app/solicitacoes/routes.py

@solicitacoes_bp.route('/relatorios/imprimir')
def relatorios_imprimir():
    if 'usuario_db_id' not in session: return redirect(url_for('auth.login'))
    
    # 1. Filtros
    aba_ativa = request.args.get('aba_ativa', 'geral')
    filtro_contratado = request.args.get('contratado', '').strip()
    filtro_competencia = request.args.get('competencia', '').strip()
    data_inicio = request.args.get('data_inicio')
    data_fim = request.args.get('data_fim')

    def aplicar_filtros_globais(query_obj, model_solicitacao=Solicitacao, model_contrato=Contrato):
        if filtro_contratado: query_obj = query_obj.filter(model_contrato.nomeContratado.like(f'%{filtro_contratado}%'))
        if filtro_competencia: query_obj = query_obj.filter(model_solicitacao.competencia == filtro_competencia)
        if data_inicio:
            try:
                dt_ini = datetime.strptime(data_inicio, '%Y-%m-%d')
                query_obj = query_obj.filter(model_solicitacao.data_solicitacao >= dt_ini)
            except ValueError:
                pass  # Data inválida, ignora filtro
        if data_fim:
            try:
                dt_fim = datetime.strptime(data_fim, '%Y-%m-%d'); dt_fim = dt_fim.replace(hour=23, minute=59, second=59)
                query_obj = query_obj.filter(model_solicitacao.data_solicitacao <= dt_fim)
            except ValueError:
                pass  # Data inválida, ignora filtro
        return query_obj

    def formatar_delta_str(diff):
        # Exibe "0d" para tempos muito curtos, mas não negativos
        dias = diff.days
        if dias < 0: return "0d"
        if dias == 0 and diff.seconds > 3600: return "1d" # Arredonda se > 1h (opcional)
        return f"{dias}d"

    # 2. Definição dos Checkpoints (Fases)
    CHECKPOINTS = [
        {'label': 'Solicitação Criada', 'ids': [1], 'cor': '#0d6efd', 'tipo': 'single'},
        {'label': 'Documentação Solicitada', 'ids': [2, 7], 'cor': '#6610f2', 'tipo': 'single'},
        {'label': 'Documentação Recebida', 'ids': [3, 8], 'cor': '#6f42c1', 'tipo': 'single'},
        {'label': 'Solicitação da NF', 'ids': [4, 10], 'cor': '#d63384', 'tipo': 'single'},
        {'label': 'Atesto e Fiscalização', 'ids': [12, 13, 14], 'cor': '#fd7e14', 'tipo': 'group'},
        {'label': 'NF Atestada', 'ids': [11, 15], 'cor': '#ffc107', 'tipo': 'single'},
        {'label': 'Financeiro', 'ids': [5, 6], 'cor': '#198754', 'tipo': 'group'}
    ]

    # Mapa para identificar em qual coluna (0 a 6) cada ID de etapa cai
    todas_etapas_db = Etapa.query.all()
    mapa_nome_id = {e.nome.strip().lower(): e.id for e in todas_etapas_db}
    
    # IDs manuais para garantir
    ids_adicionais = {
        'solicitação criada': 0, 'documentação solicitada': 1, 'documentação incompleta': 1,
        'documentação recebida': 2, 'documentação completa': 2, 'aguardando empenho': 2,
        'solicitação da nf': 3, 'empenho realizado': 3, 'nf com pendência': 3,
        'nf atestada': 5, 'liquidado': 6, 'pago': 6
    }
    
    mapa_cp_idx = {}
    # Prioridade 1: IDs definidos no CHECKPOINTS
    for idx, cp in enumerate(CHECKPOINTS):
        for eid in cp['ids']: mapa_cp_idx[eid] = idx
    # Prioridade 2: Nomes
    for nome, idx in ids_adicionais.items():
        fid = mapa_nome_id.get(nome)
        if fid: mapa_cp_idx[fid] = idx

    dados_render = {}
    query_base = Solicitacao.query.join(Etapa).join(Contrato)
    query_base = aplicar_filtros_globais(query_base)

    if aba_ativa == 'geral':
        # (Lógica da Visão Geral mantida igual)
        query_contagem = db.session.query(Solicitacao.etapa_atual_id, func.count(Solicitacao.id))\
            .join(Contrato, Solicitacao.codigo_contrato == Contrato.codigo)
        query_contagem = aplicar_filtros_globais(query_contagem)
        dados_contagem = query_contagem.group_by(Solicitacao.etapa_atual_id).all()
        
        mapa_contagem = {r[0]: r[1] for r in dados_contagem}
        resumo = []
        for cp in CHECKPOINTS:
            qtd = sum(mapa_contagem.get(eid, 0) for eid in cp['ids'])
            resumo.append({'nome': cp['label'], 'qtd': qtd, 'cor': cp['cor']})
        
        lista_completa = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()).all()
        dados_render['resumo'] = resumo
        dados_render['detalhes'] = lista_completa
        dados_render['titulo'] = "Relatório Geral de Estoque"

    elif aba_ativa == 'metricas':
        # --- LÓGICA DE CÁLCULO DE TEMPO CORRIGIDA ---
        
        # Acumulador Global: { 0: [timedelta, timedelta], 1: [...] }
        acumulador_medias = {i: [] for i in range(len(CHECKPOINTS))}
        
        lista_completa = query_base.order_by(Etapa.ordem, Solicitacao.data_solicitacao.desc()).all()
        matriz_tempos = []
        agora = datetime.now()

        for sol in lista_completa:
            hist = HistoricoMovimentacao.query.filter_by(id_solicitacao=sol.id).order_by(HistoricoMovimentacao.data_movimentacao.asc()).all()
            tempos_processo = {}
            
            # Identifica a fase atual e se está finalizado
            idx_fase_atual = mapa_cp_idx.get(sol.etapa_atual_id)
            eh_finalizado = (sol.status_geral in ['PAGO', 'CONCLUIDO', 'CANCELADO']) if sol.status_geral else False
            if idx_fase_atual == 6: eh_finalizado = True
            
            for i in range(len(hist)):
                m_atual = hist[i]
                idx_coluna = mapa_cp_idx.get(m_atual.id_etapa_nova)
                
                if idx_coluna is not None:
                    if i < len(hist) - 1:
                        dt_saida = hist[i+1].data_movimentacao
                    else:
                        if eh_finalizado:
                            dt_saida = m_atual.data_movimentacao
                        elif idx_fase_atual is not None and idx_fase_atual != idx_coluna:
                            dt_saida = m_atual.data_movimentacao
                        else:
                            dt_saida = agora
                    
                    delta = dt_saida - m_atual.data_movimentacao
                    if delta.total_seconds() < 0: delta = timedelta(0)
                    tempos_processo[idx_coluna] = tempos_processo.get(idx_coluna, timedelta()) + delta
                    acumulador_medias[idx_coluna].append(delta)

            # Correção: fase atual sem registro no histórico
            if idx_fase_atual is not None and idx_fase_atual not in tempos_processo and not eh_finalizado:
                if hist:
                    dt_entrada_fase = hist[-1].data_movimentacao
                else:
                    dt_entrada_fase = sol.data_solicitacao or agora
                delta = agora - dt_entrada_fase
                if delta.total_seconds() < 0: delta = timedelta(0)
                tempos_processo[idx_fase_atual] = delta
                acumulador_medias[idx_fase_atual].append(delta)

            tempos_formatados = {k: formatar_delta_str(v) for k, v in tempos_processo.items()}
            matriz_tempos.append({'solicitacao': sol, 'tempos': tempos_formatados})

        # 2. Calcula as Médias Globais para a Timeline
        timeline_medias = []
        for idx, cp in enumerate(CHECKPOINTS):
            item = {'nome': cp['label'], 'cor': cp['cor'], 'eh_grupo': cp.get('tipo') == 'group'}
            
            # Pega todas as durações registradas nesta fase
            lista_duracoes = acumulador_medias.get(idx, [])
            
            media_str = "--"
            if lista_duracoes:
                # Média simples: Soma total / Qtd de ocorrências
                media = sum(lista_duracoes, timedelta()) / len(lista_duracoes)
                media_str = formatar_delta_str(media)
            
            item['media_entrada'] = media_str
            timeline_medias.append(item)

        dados_render['timeline_medias'] = timeline_medias
        dados_render['matriz_tempos'] = matriz_tempos
        dados_render['colunas_etapas'] = [{'id': i, 'nome': cp['label']} for i, cp in enumerate(CHECKPOINTS)]
        dados_render['titulo'] = "Relatório de Métricas e Performance"

    return render_template('solicitacoes/relatorios_impressao.html', 
                           **dados_render, 
                           aba_ativa=aba_ativa,
                           agora=datetime.now())