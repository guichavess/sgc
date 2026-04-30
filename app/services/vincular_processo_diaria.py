"""
Serviço de Vinculação/Importação de Processos SEI no módulo de Diárias.

Funções públicas:
  1. verificar_protocolo_sei(protocolo)
     — usado pelo endpoint AJAX de verificação

  2. vincular_processo_sei(itinerario_id, protocolo_sei, etapa_id, usuario_id)
     — vincula um processo SEI a uma solicitação EXISTENTE (edita itinerário)

  3. importar_processo_sei_como_novo(protocolo_sei, etapa_id, usuario_id, usuario_gerador)
     — cria uma NOVA solicitação no sistema a partir de um processo SEI existente
       (importação pelo gestor via botão "Vincular Processo" na lista de administração)

Importações usadas pelos testes (patcheadas individualmente):
    app.services.vincular_processo_diaria.gerar_token_sei_admin
    app.services.vincular_processo_diaria.consultar_procedimento_sei
    app.services.vincular_processo_diaria.listar_documentos_procedimento_sei
    app.services.vincular_processo_diaria.baixar_documento_sei
"""
from datetime import datetime

from flask import current_app

from app.extensions import db
from app.services.sei_auth import gerar_token_sei_admin
from app.services.sei_integration import (
    consultar_procedimento_sei,
    listar_documentos_procedimento_sei,
)
from app.services.diarias_sei_integration import (
    baixar_documento_sei,
    SERIE_TIPO_DOCUMENTO_MAP,
    ID_SERIE_REQUISICAO_DIARIAS,
)
from app.services.requisicao_parser import parsear_html_requisicao_diarias

# cod_ibge do Piauí (viagem dentro do Piauí = Estadual)
COD_IBGE_PIAUI = 22


# ── Função de apoio ───────────────────────────────────────────────────────────

def detectar_tipo_itinerario(estado_origem_ibge, estado_destino_ibge):
    """
    Determina o tipo de itinerário com base no estado de destino.

    Regras:
        destino = 22 (Piauí) → 1 (Estadual)
        destino = outro estado → 2 (Nacional)
        destino = None → None (não altera o tipo existente)

    Args:
        estado_origem_ibge: cod_ibge do estado de origem (pode ser None)
        estado_destino_ibge: cod_ibge do estado de destino (pode ser None)

    Returns:
        int|None — 1, 2 ou None
    """
    if estado_destino_ibge is None:
        return None
    return 1 if estado_destino_ibge == COD_IBGE_PIAUI else 2


# ── AJAX helper ──────────────────────────────────────────────────────────────

def verificar_protocolo_sei(protocolo_sei):
    """
    Verifica se um processo existe no SEI. Usado pelo endpoint AJAX.

    Args:
        protocolo_sei: string com o número do processo (formatado ou só dígitos)

    Returns:
        dict com {sucesso, protocolo_formatado, id_procedimento,
                  link_acesso, especificacao, dados_procedimento, erro}
    """
    if not protocolo_sei or not str(protocolo_sei).strip():
        return {
            'sucesso': False,
            'protocolo_formatado': '',
            'id_procedimento': '',
            'link_acesso': '',
            'especificacao': '',
            'dados_procedimento': None,
            'erro': 'Número do processo não informado.',
        }

    token = gerar_token_sei_admin()
    if not token:
        return {
            'sucesso': False,
            'protocolo_formatado': '',
            'id_procedimento': '',
            'link_acesso': '',
            'especificacao': '',
            'dados_procedimento': None,
            'erro': 'Não foi possível autenticar no SEI. Verifique as credenciais.',
        }

    return consultar_procedimento_sei(token, protocolo_sei)


# ── Serviço principal ─────────────────────────────────────────────────────────

def vincular_processo_sei(itinerario_id, protocolo_sei, etapa_id, usuario_id):
    """
    Vincula um processo SEI existente a uma solicitação de diárias.

    Passos:
        1. Autentica no SEI e consulta o procedimento
        2. Atualiza sei_protocolo, sei_id_procedimento, link_processo_sei,
           especificacao_sei e etapa_atual_id no itinerário
        3. Lista documentos do processo e cria DiariasDocumentoSei para cada
           documento mapeado em SERIE_TIPO_DOCUMENTO_MAP
        4. Localiza a Requisição de Diárias (IdSerie 532), baixa e parseia
        5. Atualiza tipo_itinerario conforme trecho (PI/PI = Estadual, etc.)
        6. Insere os integrantes em DiariasItemItinerario (idempotente por CPF)
        7. Registra histórico de movimentação
        8. flush() — commit fica a cargo do chamador (rota ou teste)

    Args:
        itinerario_id: int — PK do DiariasItinerario
        protocolo_sei:  str — número do processo SEI
        etapa_id:       int — ID da etapa para a qual o itinerário avança
        usuario_id:     int — ID do usuário responsável pela ação

    Returns:
        dict {sucesso: bool, msgs: list[str], erro: str|None}
    """
    from app.models.diaria import (
        DiariasItinerario,
        DiariasItemItinerario,
        DiariasHistoricoMovimentacao,
    )

    resultado = {'sucesso': False, 'msgs': [], 'erro': None}

    # ── 1. Autenticação SEI ──────────────────────────────────────────────────
    token = gerar_token_sei_admin()
    if not token:
        resultado['erro'] = 'Não foi possível autenticar no SEI.'
        return resultado

    # ── 2. Consultar procedimento ────────────────────────────────────────────
    proc = consultar_procedimento_sei(token, protocolo_sei)
    if not proc.get('sucesso'):
        resultado['erro'] = proc.get('erro') or 'Processo não encontrado no SEI.'
        return resultado

    # ── 3. Carregar itinerário ───────────────────────────────────────────────
    itinerario = DiariasItinerario.query.get(itinerario_id)
    if not itinerario:
        resultado['erro'] = f'Solicitação {itinerario_id} não encontrada.'
        return resultado

    # ── 4. Atualizar campos do processo SEI ──────────────────────────────────
    protocolo_formatado = proc.get('protocolo_formatado') or protocolo_sei
    itinerario.sei_protocolo = protocolo_formatado
    itinerario.sei_id_procedimento = proc.get('id_procedimento', '')
    itinerario.link_processo_sei = proc.get('link_acesso', '')
    itinerario.especificacao_sei = proc.get('especificacao', '')
    itinerario.etapa_atual_id = etapa_id

    resultado['msgs'].append(f'Processo SEI {protocolo_formatado} localizado.')

    # ── 5. Listar documentos ─────────────────────────────────────────────────
    docs_result = listar_documentos_procedimento_sei(token, protocolo_sei)
    documentos = []
    if docs_result.get('sucesso') and docs_result.get('documentos'):
        documentos = docs_result['documentos']

    # ── 6. Criar DiariasDocumentoSei para cada doc mapeado ───────────────────
    requisicao_id_doc = None  # IdDocumento da Requisição de Diárias (IdSerie 532)

    for doc in documentos:
        serie = doc.get('Serie') or {}
        id_serie = str(serie.get('IdSerie', ''))
        tipo = SERIE_TIPO_DOCUMENTO_MAP.get(id_serie)

        if tipo:
            itinerario.set_doc(
                tipo,
                sei_id=str(doc.get('IdDocumento', '')),
                sei_formatado=str(doc.get('DocumentoFormatado', '')),
            )

        if id_serie == str(ID_SERIE_REQUISICAO_DIARIAS):
            requisicao_id_doc = doc.get('IdDocumento')

    if documentos:
        resultado['msgs'].append(f'{len(documentos)} documento(s) identificado(s) no processo.')

    # ── 7. Download e parse da Requisição de Diárias (IdSerie 532) ───────────
    if requisicao_id_doc:
        try:
            html_bytes = baixar_documento_sei(str(requisicao_id_doc))
            if html_bytes:
                parsed = parsear_html_requisicao_diarias(html_bytes)

                # Atualiza tipo_itinerario conforme trecho
                tipo_detectado = detectar_tipo_itinerario(
                    parsed.get('estado_origem'),
                    parsed.get('estado_destino'),
                )
                if tipo_detectado is not None:
                    itinerario.tipo_itinerario = tipo_detectado
                    tipo_nome = 'Estadual' if tipo_detectado == 1 else 'Nacional'
                    resultado['msgs'].append(f'Tipo de itinerário detectado: {tipo_nome}.')

                # Atualiza objetivo se disponível
                if parsed.get('objetivo') and not itinerario.objetivo:
                    itinerario.objetivo = parsed['objetivo']

                # Atualiza estados origem/destino
                if parsed.get('estado_origem'):
                    itinerario.estado_origem = parsed['estado_origem']
                if parsed.get('estado_destino'):
                    itinerario.estado_destino = parsed['estado_destino']

                # Insere integrantes (idempotente por CPF)
                integrantes = parsed.get('integrantes', [])
                inseridos = 0
                for integ in integrantes:
                    cpf = (integ.get('cpf') or '').strip()
                    if not cpf:
                        continue
                    existing = DiariasItemItinerario.query.filter_by(
                        id_itinerario=itinerario.id,
                        cpf_pessoa=cpf,
                    ).first()
                    if not existing:
                        item = DiariasItemItinerario(
                            id_itinerario=itinerario.id,
                            cpf_pessoa=cpf,
                            nome_pessoa=integ.get('nome', ''),
                            matricula_pessoa=integ.get('matricula', ''),
                            cargo_folha=integ.get('cargo', ''),
                            vinculo=integ.get('vinculo', ''),
                            banco_agencia=integ.get('banco_agencia', ''),
                            banco_conta=integ.get('banco_conta', ''),
                            valor_cargo=integ.get('valor_unitario', 0) or 0,
                        )
                        db.session.add(item)
                        inseridos += 1

                if inseridos:
                    resultado['msgs'].append(
                        f'{inseridos} integrante(s) importado(s) da Requisição.'
                    )

        except Exception as e:
            # Falha ao processar a requisição não deve abortar a vinculação
            current_app.logger.warning(
                f'[DIARIAS] Erro ao processar Requisição {requisicao_id_doc}: {e}'
            )
            resultado['msgs'].append('Aviso: não foi possível importar integrantes da Requisição.')

    # ── 8. Registrar histórico de movimentação ────────────────────────────────
    hist = DiariasHistoricoMovimentacao(
        id_itinerario=itinerario.id,
        id_etapa_anterior=None,
        id_etapa_nova=etapa_id,
        id_usuario_responsavel=usuario_id,
        data_movimentacao=datetime.now(),
        comentario=f'Processo SEI vinculado: {protocolo_formatado}',
    )
    db.session.add(hist)

    # ── 9. Flush (commit fica a cargo do chamador) ────────────────────────────
    db.session.flush()

    resultado['sucesso'] = True
    resultado['msgs'].append(f'Processo {protocolo_formatado} vinculado com sucesso.')
    return resultado


# ── Importação como novo itinerário ──────────────────────────────────────────

def importar_processo_sei_como_novo(
    protocolo_sei,
    etapa_id,
    usuario_id,
    usuario_gerador='importacao',
):
    """
    Importa um processo SEI existente como uma NOVA solicitação de diárias
    no sistema, sem partir de um itinerário pré-existente.

    Diferente de `vincular_processo_sei` (que edita um itinerário existente),
    esta função cria o DiariasItinerario do zero a partir dos dados extraídos
    do SEI — Requisição de Diárias (IdSerie 532) e documentos associados.

    Passos:
        1. Autentica no SEI e valida o protocolo
        2. Cria um novo DiariasItinerario com status Importado
        3. Delega para vincular_processo_sei() (que faz todo o trabalho de
           download, parse de integrantes, documentos e histórico)

    Args:
        protocolo_sei:   str — número do processo SEI
        etapa_id:        int — etapa em que o processo se encontra
        usuario_id:      int — ID do usuário que faz a importação
        usuario_gerador: str — login/sigla gravado no campo usuario_gerador

    Returns:
        dict {sucesso: bool, itinerario_id: int|None, msgs: list, erro: str|None}
    """
    from app.models.diaria import DiariasItinerario

    resultado = {'sucesso': False, 'itinerario_id': None, 'msgs': [], 'erro': None}

    # ── 1. Validação rápida do protocolo antes de criar qualquer registro ─────
    verif = verificar_protocolo_sei(protocolo_sei)
    if not verif.get('sucesso'):
        resultado['erro'] = verif.get('erro') or 'Processo não encontrado no SEI.'
        return resultado

    protocolo_formatado = verif.get('protocolo_formatado') or protocolo_sei

    # ── 2. Criar novo itinerário "casca" ──────────────────────────────────────
    novo = DiariasItinerario(
        usuario_gerador=usuario_gerador,
        tipo_itinerario=1,          # padrão Estadual; ajustado pelo parse da Requisição
        tipo_solicitacao_id=1,      # padrão "Apenas Diárias"; ajustável depois
        qtd_diarias_solicitadas=0,
        etapa_atual_id=etapa_id,
        status_id=1,
        data_solicitacao=datetime.now(),
        sei_protocolo=protocolo_formatado,
        sei_id_procedimento=verif.get('id_procedimento', ''),
        link_processo_sei=verif.get('link_acesso', ''),
        especificacao_sei=verif.get('especificacao', ''),
    )
    db.session.add(novo)
    db.session.flush()  # garante novo.id

    resultado['msgs'].append(f'Novo itinerário #{novo.id} criado.')

    # ── 3. Reutilizar vincular_processo_sei para documentos e integrantes ─────
    vinc = vincular_processo_sei(
        itinerario_id=novo.id,
        protocolo_sei=protocolo_sei,
        etapa_id=etapa_id,
        usuario_id=usuario_id,
    )

    resultado['msgs'].extend(vinc.get('msgs', []))

    if not vinc.get('sucesso'):
        # Problema ao buscar documentos/integrantes — não aborta, apenas avisa
        current_app.logger.warning(
            f'[DIARIAS] importar_processo: vinculação incompleta para {protocolo_sei}: '
            f'{vinc.get("erro")}'
        )
        resultado['msgs'].append(
            'Aviso: importação parcial — verifique documentos e integrantes no detalhe.'
        )

    resultado['sucesso'] = True
    resultado['itinerario_id'] = novo.id
    return resultado
