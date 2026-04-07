"""
Importação de Diárias Históricas via SEI API.

Busca processos de diárias no SEI, baixa os PDFs das requisições,
extrai dados e popula as tabelas do módulo de diárias.

Uso:
    # Teste com 1 processo (DRY-RUN)
    python scripts/importar_diarias_sei.py --teste

    # Teste com 1 processo (EXECUTAR de fato)
    python scripts/importar_diarias_sei.py --teste --executar

    # Importar todos os processos (DRY-RUN)
    python scripts/importar_diarias_sei.py

    # Importar todos (EXECUTAR)
    python scripts/importar_diarias_sei.py --executar
"""

import sys
import os
import re
import io
import time
import argparse
import unicodedata
from datetime import datetime, date
from decimal import Decimal

import requests
import urllib3
from bs4 import BeautifulSoup
from html import unescape

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Fix Windows terminal encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ── Ajustar path para importar módulos da app ──────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import create_app
from app.extensions import db
from app.services.sei_auth import gerar_token_sei_admin

# ── Constantes SEI ─────────────────────────────────────────────────────
BASE_URL = "https://api.sei.pi.gov.br"
UNIDADE_SEAD = "110006213"

# IdSeries relevantes para detecção de etapa
SERIE_REQUISICAO_DIARIAS = "532"
SERIE_MEMORANDO_SGA = "2986"
SERIE_COTACAO = "272"
SERIE_ESCOLHA_PASSAGENS = "2977"
SERIE_NOTA_RESERVA = "425"
SERIE_QUADRO_ORCAMENTARIO = "723"
SERIE_NOTA_EMPENHO = "419"
SERIE_DESPACHO_SGA = "2987"
SERIE_ANALISE_PAGAMENTO = "461"
SERIE_DESPACHO_NCI = "5"
SERIE_DESPACHO = "754"
SERIE_NL = "420"
SERIE_PD = "421"
SERIE_OB = "422"
SERIE_RELATORIO_VIAGEM = "1908"
SERIE_NP = "423"
SERIE_COMPROVANTE = "35"
SERIE_REQUISICAO_PASSAGENS = "2975"

# Processo de teste
PROCESSO_TESTE = "00002.009305/2025-23"

# Arquivo com lista de processos
ARQUIVO_PROCESSOS = os.path.join(os.path.dirname(__file__), '..', 'processos de diárias.txt')

# Diretório para salvar PDFs baixados (debug)
DIR_PDFS = os.path.join(os.path.dirname(__file__), '..', 'pdfs_diarias')

# ── Mapeamento de estados brasileiros (sigla → cod_ibge) ──────────────
ESTADOS_SIGLA = {
    'AC': 12, 'AL': 27, 'AP': 16, 'AM': 13, 'BA': 29, 'CE': 23, 'DF': 53,
    'ES': 32, 'GO': 52, 'MA': 21, 'MT': 51, 'MS': 50, 'MG': 31, 'PA': 15,
    'PB': 25, 'PR': 41, 'PE': 26, 'PI': 22, 'RJ': 33, 'RN': 24, 'RS': 43,
    'RO': 11, 'RR': 14, 'SC': 42, 'SE': 28, 'SP': 35, 'TO': 17,
}

# Mapeamento de cidades conhecidas → estado (para parsing de trecho)
CIDADES_ESTADO = {
    'TERESINA': 'PI', 'SAO PAULO': 'SP', 'BRASILIA': 'DF', 'BRASÍLIA': 'DF',
    'RIO DE JANEIRO': 'RJ', 'BELO HORIZONTE': 'MG', 'SALVADOR': 'BA',
    'FORTALEZA': 'CE', 'RECIFE': 'PE', 'CURITIBA': 'PR', 'MANAUS': 'AM',
    'BELEM': 'PA', 'BELÉM': 'PA', 'GOIANIA': 'GO', 'GOIÂNIA': 'GO',
    'SAO LUIS': 'MA', 'SÃO LUÍS': 'MA', 'NATAL': 'RN', 'MACEIO': 'AL',
    'MACEIÓ': 'AL', 'JOAO PESSOA': 'PB', 'JOÃO PESSOA': 'PB',
    'PORTO ALEGRE': 'RS', 'FLORIANOPOLIS': 'SC', 'FLORIANÓPOLIS': 'SC',
    'VITORIA': 'ES', 'VITÓRIA': 'ES', 'CAMPO GRANDE': 'MS', 'CUIABA': 'MT',
    'CUIABÁ': 'MT', 'PALMAS': 'TO', 'MACAPA': 'AP', 'MACAPÁ': 'AP',
    'BOA VISTA': 'RR', 'PORTO VELHO': 'RO', 'RIO BRANCO': 'AC',
    'ARACAJU': 'SE', 'PARNAIBA': 'PI', 'PARNAÍBA': 'PI',
    'PICOS': 'PI', 'FLORIANO': 'PI', 'OEIRAS': 'PI',
    'PIRIPIRI': 'PI', 'CAMPO MAIOR': 'PI', 'BARRAS': 'PI',
    'PEDRO II': 'PI', 'SAO RAIMUNDO NONATO': 'PI',
    'CORRENTE': 'PI', 'URUÇUÍ': 'PI', 'URUCUI': 'PI',
    'BOM JESUS': 'PI', 'CANTO DO BURITI': 'PI',
}


# ═══════════════════════════════════════════════════════════════════════
# FASE 1: INTERAÇÃO COM API SEI
# ═══════════════════════════════════════════════════════════════════════

def listar_documentos(token, protocolo_processo):
    """Lista todos os documentos de um processo SEI."""
    protocolo_limpo = "".join(filter(str.isdigit, protocolo_processo))
    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/documentos"
    params = {
        'protocolo_procedimento': protocolo_limpo,
        'quantidade': 1000,
    }
    headers = {'token': token, 'Accept': 'application/json'}

    resp = requests.get(url, params=params, headers=headers, timeout=180, verify=False)
    if resp.status_code != 200:
        print(f"  ERRO ao listar documentos ({resp.status_code}): {resp.text[:200]}")
        return []

    data = resp.json()
    if isinstance(data, dict) and 'Documentos' in data:
        return data['Documentos']
    elif isinstance(data, list):
        return data
    return [data] if data else []


def consultar_procedimento(token, protocolo_processo):
    """Consulta metadados de um processo SEI."""
    protocolo_limpo = "".join(filter(str.isdigit, protocolo_processo))
    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/consulta"
    params = {'protocolo_procedimento': protocolo_limpo}
    headers = {'token': token, 'Accept': 'application/json'}

    resp = requests.get(url, params=params, headers=headers, timeout=180, verify=False)
    if resp.status_code != 200:
        print(f"  ERRO ao consultar procedimento ({resp.status_code})")
        return None
    return resp.json()


def baixar_documento_pdf(token, id_documento):
    """
    Baixa o PDF de um documento SEI.
    GET /v1/unidades/{id_unidade}/documentos/baixar?protocolo_documento={id}
    """
    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos/baixar"
    params = {'protocolo_documento': str(id_documento)}
    headers = {'token': token, 'Accept': 'application/pdf'}

    resp = requests.get(url, params=params, headers=headers, timeout=300, verify=False)

    if resp.status_code != 200:
        print(f"  ERRO ao baixar documento {id_documento} ({resp.status_code}): {resp.text[:200]}")
        return None

    content_type = resp.headers.get('Content-Type', '')
    print(f"  Download OK - Content-Type: {content_type}, Tamanho: {len(resp.content)} bytes")

    # Se retorna JSON com base64
    if 'json' in content_type.lower():
        try:
            import base64
            data = resp.json()
            # Tentar campos comuns
            for campo in ('Conteudo', 'conteudo', 'Content', 'content', 'Arquivo', 'arquivo'):
                if campo in data:
                    return base64.b64decode(data[campo])
            print(f"  AVISO: Resposta JSON sem campo de conteúdo. Chaves: {list(data.keys())}")
            return None
        except Exception as e:
            print(f"  ERRO ao decodificar JSON/base64: {e}")
            return None

    # Se retorna binário direto (PDF)
    return resp.content


def filtrar_documentos_por_serie(documentos, id_serie):
    """Filtra documentos por IdSerie."""
    return [d for d in documentos if d.get('Serie', {}).get('IdSerie') == str(id_serie)]


# ═══════════════════════════════════════════════════════════════════════
# FASE 2: PARSING DO PDF
# ═══════════════════════════════════════════════════════════════════════

def _normalizar_texto(texto):
    """Remove acentos e normaliza para comparação."""
    if not texto:
        return ''
    nfkd = unicodedata.normalize('NFKD', texto)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


def _limpar_valor_monetario(texto):
    """Converte texto monetário para Decimal. Ex: 'R$ 2.160,00' → Decimal('2160.00')"""
    if not texto:
        return Decimal('0')
    # Remover R$, espaços, &nbsp; (char \xa0) e outros whitespace
    texto = re.sub(r'[R$\s\xa0\u00a0]', '', texto)
    texto = texto.replace('.', '').replace(',', '.')
    try:
        return Decimal(texto)
    except:
        return Decimal('0')


def _extrair_sigla_estado_do_trecho(trecho_texto):
    """
    Extrai sigla de estado do trecho.
    Ex: 'TERESINA-PI/SÃO PAULO-SP/TERESINA-PI' → ('PI', 'SP')
    """
    if not trecho_texto:
        return None, None

    trecho = trecho_texto.upper().strip()

    # Padrão: CIDADE-UF/CIDADE-UF/...
    partes = re.findall(r'([A-ZÀ-Ü\s]+)\s*[-–]\s*([A-Z]{2})', trecho)
    if partes:
        estado_origem = partes[0][1]
        # Destino = segundo item (ou o que não for a origem)
        for cidade, uf in partes[1:]:
            if uf != estado_origem:
                return estado_origem, uf
        # Se todos iguais, é estadual
        return estado_origem, estado_origem

    # Fallback: tentar por cidade conhecida
    for cidade, uf in CIDADES_ESTADO.items():
        if cidade in trecho:
            if not hasattr(_extrair_sigla_estado_do_trecho, '_primeiro'):
                _extrair_sigla_estado_do_trecho._primeiro = uf
            else:
                return _extrair_sigla_estado_do_trecho._primeiro, uf

    return None, None


def parsear_html_requisicao_diarias(html_bytes):
    """
    Extrai dados estruturados do HTML da Requisição de Diárias retornado pela API SEI.

    Retorna dict com:
        numero_requisicao, ano_requisicao,
        integrantes: [{matricula, nome, cargo, vinculo, cpf, banco_info, qtd_diarias, valor_unitario, valor_total}],
        valor_total_geral,
        objetivo,
        trecho, estado_origem, estado_destino,
        periodo_inicio, periodo_fim,
        processo_sei
    """
    resultado = {
        'numero_requisicao': None,
        'ano_requisicao': None,
        'integrantes': [],
        'valor_total_geral': Decimal('0'),
        'objetivo': '',
        'trecho': '',
        'estado_origem': None,
        'estado_destino': None,
        'periodo_inicio': None,
        'periodo_fim': None,
        'processo_sei': '',
    }

    # Decodificar HTML (API retorna iso-8859-1)
    if isinstance(html_bytes, bytes):
        html_str = html_bytes.decode('iso-8859-1', errors='replace')
    else:
        html_str = html_bytes

    # Pré-limpar HTML: remover tags <a> inline (SEI insere <a target="_blank">P</a>edro)
    # que quebram nomes quando extraímos texto
    html_str = re.sub(r'<a[^>]*>([^<]*)</a>', r'\1', html_str)

    soup = BeautifulSoup(html_str, 'lxml')

    texto_completo = soup.get_text(separator='\n')

    # ── Número da Requisição (do <title> ou do corpo) ──
    title = soup.find('title')
    if title:
        title_text = title.get_text()
        print(f"  Título: {title_text}")

    match_req = re.search(
        r'REQUISI[ÇC\xc7][ÃA\xc3]O\s+DE\s+DI[ÁA\xc1]RIAS\s+N[ºo°\xba]\s*(\d+)/(\d{4})',
        texto_completo
    )
    if match_req:
        resultado['numero_requisicao'] = int(match_req.group(1))
        resultado['ano_requisicao'] = int(match_req.group(2))
        print(f"  Requisição Nº {resultado['numero_requisicao']}/{resultado['ano_requisicao']}")

    # ── Processo SEI (do title ou do corpo) ──
    match_sei = re.search(r'SEI\s+(\d{5}\.\d{6}/\d{4}-\d{2})', texto_completo)
    if not match_sei:
        # Tentar no título do documento
        match_sei = re.search(r'(\d{5}\.\d{6}/\d{4}-\d{2})', str(soup.find('title')))
    if match_sei:
        resultado['processo_sei'] = match_sei.group(1)

    # ── Tabela principal (integrantes + objetivo + trecho) ──
    tabela_principal = soup.find('table')
    if not tabela_principal:
        print("  AVISO: Tabela principal não encontrada no HTML!")
        return resultado

    linhas = tabela_principal.find_all('tr')

    # Identificar header (primeira linha)
    header_cells = linhas[0].find_all(['td', 'th']) if linhas else []
    header_textos = [_normalizar_texto(c.get_text()) for c in header_cells]
    print(f"  Header detectado ({len(header_textos)} colunas): {header_textos[:5]}...")

    # Mapear índices das colunas
    col_indices = _mapear_colunas_header(header_textos)

    # Iterar linhas de dados
    for tr in linhas[1:]:
        cells = tr.find_all('td')
        if not cells:
            continue

        # Pegar texto de cada célula
        cell_texts = [c.get_text(separator=' ', strip=True) for c in cells]
        cell_texts_joined = ' '.join(cell_texts).upper()

        # ── Linha de VALOR TOTAL ──
        if 'VALOR TOTAL' in cell_texts_joined:
            # A última célula tem o valor
            for ct in reversed(cell_texts):
                valor = _limpar_valor_monetario(ct)
                if valor > 0:
                    resultado['valor_total_geral'] = valor
                    print(f"  Valor Total Geral: R$ {valor}")
                    break
            continue

        # ── Linha de OBJETIVO DA VIAGEM ──
        if 'OBJETIVO DA VIAGEM' in cell_texts_joined or 'OBJETIVO' in cell_texts_joined:
            # Extrair todo o texto da célula (pode ter múltiplos <p>)
            td = cells[0] if len(cells) == 1 else cells[-1]
            paragrafos = td.find_all('p')
            textos_obj = []
            for p in paragrafos:
                t = p.get_text(strip=True)
                if t and 'OBJETIVO DA VIAGEM' not in t.upper():
                    textos_obj.append(t)
            resultado['objetivo'] = ' '.join(textos_obj).strip()
            if resultado['objetivo']:
                print(f"  Objetivo: {resultado['objetivo'][:80]}...")
            continue

        # ── Linha de TRECHO / PERÍODO ──
        if 'TRECHO' in cell_texts_joined:
            for ct in cell_texts:
                match_trecho = re.search(r'TRECHO:\s*(.+)', ct, re.IGNORECASE)
                if match_trecho:
                    resultado['trecho'] = match_trecho.group(1).strip()

                match_periodo = re.search(
                    r'PER[ÍI\xcd]ODO:\s*(\d{2}/\d{2}/\d{4})\s*[AaÀà]\s*(\d{2}/\d{2}/\d{4})',
                    ct, re.IGNORECASE
                )
                if match_periodo:
                    try:
                        resultado['periodo_inicio'] = datetime.strptime(match_periodo.group(1), '%d/%m/%Y')
                        resultado['periodo_fim'] = datetime.strptime(match_periodo.group(2), '%d/%m/%Y')
                        print(f"  Período: {match_periodo.group(1)} a {match_periodo.group(2)}")
                    except ValueError:
                        pass
            continue

        # ── Linha de DESPACHO (ignorar) ──
        if 'DESPACHO' in cell_texts_joined:
            continue

        # ── Linha de integrante (tem CPF) ──
        has_cpf = any(re.search(r'\d{3}[.\s]?\d{3}[.\s]?\d{3}[-.\s]?\d{2}', ct) for ct in cell_texts)
        has_matricula = any(re.search(r'\d{5,}', ct) for ct in cell_texts[:2])

        if has_cpf or has_matricula:
            integrante = _parsear_linha_integrante_html(cells, col_indices)
            if integrante and integrante.get('cpf'):
                resultado['integrantes'].append(integrante)
                print(f"    Integrante: {integrante['nome']} | CPF: {integrante['cpf']} | "
                      f"Cargo: {integrante['cargo']} | {integrante['qtd_diarias']} diárias")

    # ── Estado origem/destino do trecho ──
    uf_orig, uf_dest = _extrair_sigla_estado_do_trecho(resultado['trecho'])
    resultado['estado_origem'] = ESTADOS_SIGLA.get(uf_orig)
    resultado['estado_destino'] = ESTADOS_SIGLA.get(uf_dest)
    if uf_orig and uf_dest:
        print(f"  Trecho: {resultado['trecho']} ({uf_orig} → {uf_dest})")

    return resultado


def _mapear_colunas_header(header_textos):
    """Mapeia nomes de colunas para índices lógicos."""
    mapa = {}
    for i, h in enumerate(header_textos):
        if 'MATRIC' in h:
            mapa['matricula'] = i
        elif h == 'NOME' or (h.startswith('NOME') and 'SUB' not in h):
            mapa['nome'] = i
        elif 'CARGO' in h or 'FUNCAO' in h:
            mapa['cargo'] = i
        elif 'EFETIVO' in h or 'COMISSION' in h or 'TERCEIR' in h:
            mapa['vinculo'] = i
        elif 'CPF' in h:
            mapa['cpf'] = i
        elif 'BANCO' in h or 'AGENCIA' in h:
            mapa['banco_info'] = i
        elif 'QUANT' in h:
            mapa['qtd_diarias'] = i
        elif 'UNITARIO' in h or 'UNIT' in h:
            mapa['valor_unitario'] = i
        elif 'TOTAL' in h and 'VALOR' in h:
            mapa['valor_total'] = i
    return mapa


def _parsear_linha_integrante_html(cells, col_indices):
    """Parseia uma linha da tabela HTML de integrantes."""
    integrante = {
        'matricula': '',
        'nome': '',
        'cargo': '',
        'vinculo': '',
        'cpf': '',
        'banco_info': '',
        'banco_nome': '',
        'banco_agencia': '',
        'banco_conta': '',
        'qtd_diarias': 0,
        'valor_unitario': Decimal('0'),
        'valor_total': Decimal('0'),
    }

    # Extrair texto de cada célula, colapsando whitespace
    cell_textos = []
    for c in cells:
        texto = c.get_text(separator=' ', strip=True)
        # Colapsar espaços múltiplos e newlines
        texto = re.sub(r'\s+', ' ', texto).strip()
        cell_textos.append(texto)

    # Tentar mapear por índice se temos o mapa de colunas
    # Mas o HTML pode ter colspans que alteram os índices...
    # Estratégia: usar heurística baseada no conteúdo de cada célula

    for i, texto in enumerate(cell_textos):
        texto_limpo = texto.strip()
        if not texto_limpo:
            continue

        # Detectar CPF (###.###.###-##)
        cpf_match = re.search(r'(\d{3}[.\s]?\d{3}[.\s]?\d{3}[-.\s]?\d{2})', texto_limpo)
        if cpf_match and not integrante['cpf']:
            integrante['cpf'] = cpf_match.group(1).replace(' ', '')
            continue

        # Detectar matrícula (6+ dígitos, possivelmente com '-' no final)
        if i == 0 and re.match(r'^\d{5,}[-\d]*$', texto_limpo.replace(' ', '')):
            integrante['matricula'] = texto_limpo
            continue

        # Detectar banco info (contém AG. ou CONTA ou BANCO)
        texto_upper = texto_limpo.upper()
        if 'AG.' in texto_upper or 'CONTA' in texto_upper or 'BANCO' in texto_upper:
            integrante['banco_info'] = texto_limpo
            _parsear_banco_info(integrante, texto_limpo)
            continue

        # Detectar quantidade de diárias (número com vírgula, ex: 4,5)
        if re.match(r'^\d+[,.]?\d*$', texto_limpo.replace(' ', '')) and len(texto_limpo) <= 5:
            valor_num = texto_limpo.replace(',', '.')
            try:
                num = float(valor_num)
                if num <= 60:  # Diárias razoável
                    if not integrante['qtd_diarias']:
                        integrante['qtd_diarias'] = num
                        continue
            except:
                pass

        # Detectar valores monetários (R$ xxx ou x.xxx,xx)
        if re.search(r'R\$|^\d{1,3}\.\d{3}|\d+,\d{2}$', texto_limpo):
            valor = _limpar_valor_monetario(texto_limpo)
            if valor > 0:
                if not integrante['valor_unitario']:
                    integrante['valor_unitario'] = valor
                elif not integrante['valor_total']:
                    integrante['valor_total'] = valor
                continue

        # Detectar vínculo (Comissionado, Efetivo, Terceirizado)
        if texto_upper in ('COMISSIONADO', 'EFETIVO', 'TERCEIRIZADO', 'TEMPORARIO',
                           'TEMPORÁRIO', 'ESTAGIARIO', 'ESTAGIÁRIO'):
            integrante['vinculo'] = texto_limpo
            continue

        # Detectar cargo (palavras conhecidas)
        cargos_conhecidos = ['SUPERINTENDENTE', 'SECRETARIO', 'SECRETÁRIO', 'DIRETOR',
                             'ASSESSOR', 'MOTORISTA', 'COORDENADOR', 'GERENTE',
                             'ANALISTA', 'TECNICO', 'TÉCNICO', 'AUXILIAR', 'ASSISTENTE']
        if any(c in texto_upper for c in cargos_conhecidos):
            integrante['cargo'] = texto_limpo
            continue

        # O que sobra e parece nome (texto sem números, > 5 chars)
        if len(texto_limpo) > 5 and not re.search(r'\d', texto_limpo) and not integrante['nome']:
            integrante['nome'] = texto_limpo
            continue

    # Limpar matrícula
    if integrante['matricula']:
        mat_limpo = re.sub(r'[^\d]', '', integrante['matricula'])
        if mat_limpo:
            integrante['matricula'] = mat_limpo

    return integrante


def _parsear_banco_info(integrante, banco_info):
    """
    Parseia informação bancária do PDF.
    Formato típico: 'AG. 3178-X CONTA CORRENTE: 28118-2 BANCO DO BRASIL'
    ou com quebras de linha.
    """
    if not banco_info:
        return

    texto = banco_info.replace('\n', ' ').upper()

    # Agência
    match_ag = re.search(r'AG\.?\s*([\d\w.-]+)', texto)
    if match_ag:
        integrante['banco_agencia'] = match_ag.group(1).strip()

    # Conta
    match_conta = re.search(r'CONTA\s*(?:CORRENTE)?:?\s*([\d\w.-]+)', texto)
    if match_conta:
        integrante['banco_conta'] = match_conta.group(1).strip()

    # Banco
    bancos_conhecidos = ['BANCO DO BRASIL', 'CAIXA ECONOMICA', 'CAIXA ECONÔMICA',
                         'BRADESCO', 'ITAU', 'ITAÚ', 'SANTANDER', 'SICOOB',
                         'NUBANK', 'INTER', 'SICREDI']
    for banco in bancos_conhecidos:
        if banco in texto:
            integrante['banco_nome'] = banco
            break


# ═══════════════════════════════════════════════════════════════════════
# FASE 3: DETECÇÃO DE ETAPA
# ═══════════════════════════════════════════════════════════════════════

def detectar_etapa_atual(documentos):
    """
    Detecta a etapa atual do processo baseado nos IdSeries dos documentos presentes.
    Retorna (etapa_id, series_encontradas).
    """
    series_presentes = set()
    for doc in documentos:
        id_serie = doc.get('Serie', {}).get('IdSerie', '')
        if id_serie:
            series_presentes.add(id_serie)

    # Detectar de trás pra frente (a etapa mais avançada encontrada)
    etapa = 1  # Default: SOLICITACAO_INICIADA

    # Mapa de detecção (ordenado do mais avançado para o menos)
    deteccao = [
        (11, [SERIE_NP]),                                          # PRESTACAO_CONTAS_CCDP
        (10, [SERIE_RELATORIO_VIAGEM, SERIE_COMPROVANTE]),         # COMPROVANTE_VIAGEM
        (9, [SERIE_NL, SERIE_PD, SERIE_OB]),                      # DESPACHO_GEO
        # (8, []),  # DESPACHO_DIRETOR - sem série específica fácil de detectar
        (7, [SERIE_DESPACHO]),                                     # DESPACHO_APOIO
        (6, [SERIE_ANALISE_PAGAMENTO, SERIE_DESPACHO_NCI]),        # ANALISE_NCI
        (5, [SERIE_DESPACHO_SGA]),                                 # CIENCIA_SGA
        (4, [SERIE_NOTA_EMPENHO]),                                 # DESPACHO_CCDP
        (3, [SERIE_COTACAO, SERIE_ESCOLHA_PASSAGENS]),             # AQUISICAO_PASSAGENS
        (2, [SERIE_NOTA_RESERVA, SERIE_QUADRO_ORCAMENTARIO]),     # FINANCEIRO
        (1, [SERIE_REQUISICAO_DIARIAS, SERIE_MEMORANDO_SGA]),     # SOLICITACAO_INICIADA
    ]

    for etapa_id, series_check in deteccao:
        if series_presentes.intersection(series_check):
            etapa = max(etapa, etapa_id)

    return etapa, series_presentes


def determinar_tipo_viagem(estado_origem, estado_destino):
    """Determina tipo de itinerário: 1=Estadual, 2=Nacional, 3=Internacional."""
    if estado_origem is None or estado_destino is None:
        return 2  # Default nacional se não conseguir determinar
    if estado_origem == estado_destino:
        return 1  # Estadual
    return 2  # Nacional


def determinar_tipo_solicitacao(documentos, tipo_viagem):
    """
    Determina tipo de solicitação:
    1=Apenas Diárias, 2=Diárias+Passagens, 3=Apenas Passagens
    """
    series_presentes = {d.get('Serie', {}).get('IdSerie', '') for d in documentos}

    tem_requisicao_passagens = SERIE_REQUISICAO_PASSAGENS in series_presentes
    tem_cotacao = SERIE_COTACAO in series_presentes
    tem_requisicao_diarias = SERIE_REQUISICAO_DIARIAS in series_presentes

    if tem_requisicao_diarias and (tem_requisicao_passagens or tem_cotacao):
        return 2  # Diárias + Passagens
    elif not tem_requisicao_diarias and (tem_requisicao_passagens or tem_cotacao):
        return 3  # Apenas Passagens
    return 1  # Apenas Diárias


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════

def processar_processo(token, protocolo_processo, executar=False):
    """
    Pipeline completo para um processo:
    1. Listar documentos
    2. Encontrar e baixar PDF da Requisição de Diárias
    3. Parsear PDF
    4. Detectar etapa
    5. Inserir no banco (se --executar)
    """
    print(f"\n{'='*70}")
    print(f"PROCESSANDO: {protocolo_processo}")
    print(f"{'='*70}")

    # 1. Consultar procedimento (para pegar IdProcedimento)
    proc_data = consultar_procedimento(token, protocolo_processo)
    id_procedimento = None
    if proc_data:
        id_procedimento = proc_data.get('IdProcedimento')
        print(f"  IdProcedimento: {id_procedimento}")
    else:
        print(f"  AVISO: Não foi possível consultar procedimento. Continuando...")

    # 2. Listar documentos
    documentos = listar_documentos(token, protocolo_processo)
    if not documentos:
        print(f"  ERRO: Nenhum documento encontrado para {protocolo_processo}")
        return None
    print(f"  Total de documentos: {len(documentos)}")

    # Listar séries encontradas (debug)
    series_info = {}
    for doc in documentos:
        serie = doc.get('Serie', {})
        sid = serie.get('IdSerie', '?')
        snome = serie.get('Nome', '?')
        series_info[sid] = snome
    print(f"  Séries encontradas:")
    for sid, snome in sorted(series_info.items()):
        print(f"    [{sid}] {snome}")

    # 3. Encontrar Requisição de Diárias (IdSerie:532)
    docs_requisicao = filtrar_documentos_por_serie(documentos, SERIE_REQUISICAO_DIARIAS)
    if not docs_requisicao:
        print(f"  ERRO: Requisição de Diárias (IdSerie:{SERIE_REQUISICAO_DIARIAS}) não encontrada!")
        return None

    doc_requisicao = docs_requisicao[0]  # Pegar o primeiro (geralmente só tem 1)
    id_documento = doc_requisicao.get('IdDocumento')
    doc_formatado = doc_requisicao.get('DocumentoFormatado', '')
    print(f"  Requisição de Diárias: IdDocumento={id_documento}, Formatado={doc_formatado}")

    # 4. Baixar PDF (usar DocumentoFormatado como protocolo_documento)
    print(f"  Baixando PDF do documento {doc_formatado}...")
    pdf_bytes = baixar_documento_pdf(token, doc_formatado)
    if not pdf_bytes:
        print(f"  ERRO: Falha ao baixar PDF do documento {id_documento}")
        return None

    # Salvar PDF para debug
    os.makedirs(DIR_PDFS, exist_ok=True)
    pdf_path = os.path.join(DIR_PDFS, f"requisicao_{protocolo_processo.replace('/', '_')}.pdf")
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)
    print(f"  PDF salvo em: {pdf_path}")

    # 5. Parsear PDF
    print(f"  Parseando PDF...")
    dados_pdf = parsear_html_requisicao_diarias(pdf_bytes)

    # Fallback: processo_sei do parâmetro se não veio do HTML
    if not dados_pdf['processo_sei']:
        dados_pdf['processo_sei'] = protocolo_processo

    if not dados_pdf['integrantes']:
        print(f"  AVISO: Nenhum integrante extraído do PDF!")

    # 6. Detectar etapa
    etapa_atual, series_presentes = detectar_etapa_atual(documentos)
    print(f"  Etapa detectada: {etapa_atual}")

    # 7. Determinar tipos
    tipo_viagem = determinar_tipo_viagem(dados_pdf['estado_origem'], dados_pdf['estado_destino'])
    tipo_solicitacao = determinar_tipo_solicitacao(documentos, tipo_viagem)
    tipo_nomes = {1: 'Estadual', 2: 'Nacional', 3: 'Internacional'}
    tipo_sol_nomes = {1: 'Apenas Diárias', 2: 'Diárias + Passagens', 3: 'Apenas Passagens'}
    print(f"  Tipo viagem: {tipo_nomes.get(tipo_viagem, '?')}")
    print(f"  Tipo solicitação: {tipo_sol_nomes.get(tipo_solicitacao, '?')}")

    # Consolidar resultado
    resultado = {
        'protocolo_processo': protocolo_processo,
        'id_procedimento': id_procedimento,
        'documentos': documentos,
        'dados_pdf': dados_pdf,
        'etapa_atual': etapa_atual,
        'tipo_viagem': tipo_viagem,
        'tipo_solicitacao': tipo_solicitacao,
        'series_presentes': series_presentes,
    }

    # Resumo
    print(f"\n  --- RESUMO ---")
    print(f"  Requisição: Nº {dados_pdf.get('numero_requisicao')}/{dados_pdf.get('ano_requisicao')}")
    print(f"  Integrantes: {len(dados_pdf['integrantes'])}")
    for i, intg in enumerate(dados_pdf['integrantes'], 1):
        print(f"    {i}. {intg['nome']} | CPF: {intg['cpf']} | "
              f"Cargo: {intg['cargo']} | {intg['qtd_diarias']} diárias | "
              f"R$ {intg['valor_unitario']} unit. | R$ {intg['valor_total']} total")
    print(f"  Valor Total Geral: R$ {dados_pdf['valor_total_geral']}")
    print(f"  Objetivo: {dados_pdf['objetivo'][:100]}...")
    print(f"  Trecho: {dados_pdf['trecho']}")
    print(f"  Período: {dados_pdf.get('periodo_inicio')} → {dados_pdf.get('periodo_fim')}")
    print(f"  Processo SEI: {dados_pdf.get('processo_sei')}")
    print(f"  Etapa: {etapa_atual}")

    if not executar:
        print(f"\n  [DRY-RUN] Nenhuma alteração no banco de dados.")
    else:
        print(f"\n  Inserindo no banco de dados...")
        inserir_no_banco(resultado)

    return resultado


# ═══════════════════════════════════════════════════════════════════════
# FASE 4: INSERÇÃO NO BANCO (placeholder para próxima etapa)
# ═══════════════════════════════════════════════════════════════════════

def _mapear_cargo_id(cargo_texto):
    """
    Mapeia o texto de cargo do PDF para o ID em diarias_cargos.
    NUNCA retorna None — se não encontrar, cria um novo ou usa "Servidor" (fallback).
    """
    from app.models.diaria import DiariasCargo

    # Mapa direto de palavras-chave → cargo_id
    MAPA_CARGOS = {
        'SECRETARIO': 1,
        'SUPERINTENDENTE': 2,
        'DIRETOR': 3,
        'ASSESSOR': 4,
        'MOTORISTA': 5,
        'COORDENADOR': 6,
        'GERENTE': 7,
    }

    CARGO_FALLBACK_NOME = 'Servidor'  # cargo genérico para quando não há info

    if not cargo_texto or not cargo_texto.strip():
        # Sem cargo → usar "Servidor" genérico
        fallback = DiariasCargo.query.filter_by(nome=CARGO_FALLBACK_NOME).first()
        if not fallback:
            fallback = DiariasCargo(nome=CARGO_FALLBACK_NOME)
            db.session.add(fallback)
            db.session.flush()
            print(f"    Novo cargo cadastrado: '{CARGO_FALLBACK_NOME}' (id={fallback.id})")
        return fallback.id

    cargo_upper = _normalizar_texto(cargo_texto)

    for palavra, cargo_id in MAPA_CARGOS.items():
        if palavra in cargo_upper:
            return cargo_id

    # Fallback: buscar no banco por similaridade
    cargos = DiariasCargo.query.all()
    for c in cargos:
        c_norm = _normalizar_texto(c.nome)
        if c_norm and (c_norm in cargo_upper or cargo_upper in c_norm):
            return c.id

    # Se não encontrou, cadastrar novo cargo
    novo = DiariasCargo(nome=cargo_texto.strip().title())
    db.session.add(novo)
    db.session.flush()
    print(f"    Novo cargo cadastrado: '{cargo_texto.strip().title()}' (id={novo.id})")
    return novo.id


def _natureza_id_por_tipo_viagem(tipo_viagem):
    """Retorna natureza_id baseado no tipo de viagem."""
    # 1=Fora do Estado (Nacional), 2=Dentro do Estado (Estadual), 3=Exterior
    return {1: 2, 2: 1, 3: 3}.get(tipo_viagem, 1)


def _mapear_sei_documentos(documentos):
    """
    Mapeia documentos SEI encontrados para as colunas sei_* do DiariasItinerario.
    Retorna dict {coluna: valor} para cada documento encontrado.
    """
    # IdSerie → tipo_documento
    SERIE_TIPO_MAP = {
        SERIE_REQUISICAO_DIARIAS: 'requisicao',
        SERIE_MEMORANDO_SGA: 'memorando',
        SERIE_REQUISICAO_PASSAGENS: 'requisicao_passagens',
        SERIE_NOTA_RESERVA: 'nota_reserva',
        SERIE_QUADRO_ORCAMENTARIO: 'quadro_orcamentario',
        SERIE_ESCOLHA_PASSAGENS: 'escolha_passagens',
        SERIE_NOTA_EMPENHO: 'nota_empenho',
        SERIE_DESPACHO_SGA: 'despacho_sga',
        SERIE_ANALISE_PAGAMENTO: 'analise_pagamento',
        SERIE_DESPACHO_NCI: 'despacho_nci',
        SERIE_NL: 'nl',
        SERIE_PD: 'pd',
        SERIE_OB: 'ob',
        SERIE_RELATORIO_VIAGEM: 'relatorio_viagem',
        SERIE_COMPROVANTE: 'comprovante_viagem',
        SERIE_NP: 'np',
    }

    # resultado: dict { tipo_documento: (sei_id, sei_formatado) }
    resultado = {}
    for doc in documentos:
        id_serie = doc.get('Serie', {}).get('IdSerie', '')
        if id_serie in SERIE_TIPO_MAP:
            tipo = SERIE_TIPO_MAP[id_serie]
            if tipo not in resultado:
                resultado[tipo] = (str(doc.get('IdDocumento', '')),
                                   str(doc.get('DocumentoFormatado', '')))

    # Mapear despachos (IdSerie 754) — aparecem múltiplas vezes em etapas diferentes
    docs_despacho_754 = [d for d in documentos
                         if d.get('Serie', {}).get('IdSerie') == SERIE_DESPACHO]
    despacho_tipos = [
        'despacho_ccdp', 'despacho_apoio', 'despacho_diretor',
        'despacho_geo', 'despacho_final',
    ]
    for i, doc in enumerate(docs_despacho_754):
        if i < len(despacho_tipos):
            resultado[despacho_tipos[i]] = (str(doc.get('IdDocumento', '')),
                                             str(doc.get('DocumentoFormatado', '')))

    return resultado


def _formatar_cpf(cpf_raw):
    """Normaliza CPF para formato ###.###.###-##."""
    if not cpf_raw:
        return ''
    digits = re.sub(r'[^\d]', '', cpf_raw)
    if len(digits) == 11:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
    return cpf_raw


def inserir_no_banco(resultado):
    """
    Insere os dados extraídos no banco de dados.

    Tabelas populadas:
    1. diarias_servidores (upsert por CPF)
    2. diarias_itinerario (registro principal)
    3. diarias_itens_itinerario (1 por integrante)
    4. diarias_historico_movimentacoes (1 por etapa até etapa_atual)
    5. diarias_controle_viagens (1 por processo)
    6. diarias_controle_servidores (1 por integrante)
    7. diarias_controle_prestacao (1 por servidor de controle)
    """
    from app.models.diaria import (
        DiariasServidor, DiariasItinerario,
        DiariasItemItinerario,
        DiariasHistoricoMovimentacao, DiariasControleViagem,
        DiariasControleServidor, DiariasControlePrestacao,
    )

    dados_pdf = resultado['dados_pdf']
    protocolo = resultado['protocolo_processo']
    etapa_atual = resultado['etapa_atual']
    tipo_viagem = resultado['tipo_viagem']
    tipo_solicitacao = resultado['tipo_solicitacao']
    documentos = resultado['documentos']

    # Verificar se já existe itinerário com esse processo
    existente = DiariasItinerario.query.filter_by(sei_protocolo=protocolo).first()
    if existente:
        print(f"  AVISO: Processo {protocolo} já importado (itinerario.id={existente.id}). Pulando.")
        return existente.id

    try:
        # ── 1. Upsert Servidores ──────────────────────────────────────
        for intg in dados_pdf['integrantes']:
            cpf_fmt = _formatar_cpf(intg['cpf'])
            if not cpf_fmt:
                continue

            servidor = DiariasServidor.query.filter_by(cpf=cpf_fmt).first()
            if not servidor:
                # Gerar idpessoa sequencial (legado PHP)
                max_idp = db.session.execute(
                    db.text('SELECT COALESCE(MAX(idpessoa), 0) FROM diarias_servidores')
                ).scalar()
                servidor = DiariasServidor(
                    idpessoa=max_idp + 1,
                    nome=intg['nome'],
                    matricula=intg['matricula'] or None,
                    cpf=cpf_fmt,
                    cargo=intg['cargo'],
                    vinculo=intg['vinculo'],
                    num_banco=(intg.get('banco_nome', '') or '')[:10],
                    num_agencia_banco=intg.get('banco_agencia', ''),
                    num_conta_banco=intg.get('banco_conta', ''),
                )
                db.session.add(servidor)
                print(f"    Servidor criado: {intg['nome']} ({cpf_fmt})")
            else:
                # Atualizar dados bancários se estavam vazios
                if not servidor.num_agencia_banco and intg.get('banco_agencia'):
                    servidor.num_agencia_banco = intg['banco_agencia']
                    servidor.num_conta_banco = intg.get('banco_conta', '')
                    servidor.num_banco = (intg.get('banco_nome', '') or '')[:10]
                if not servidor.matricula and intg['matricula']:
                    servidor.matricula = intg['matricula']
                print(f"    Servidor existente: {servidor.nome} ({cpf_fmt})")

        db.session.flush()

        # ── 2. Criar DiariasItinerario ────────────────────────────────
        # Mapear documentos SEI para colunas sei_*
        sei_cols = _mapear_sei_documentos(documentos)

        # Calcular qtd_diarias
        qtd_diarias = 0
        if dados_pdf['periodo_inicio'] and dados_pdf['periodo_fim']:
            delta = (dados_pdf['periodo_fim'] - dados_pdf['periodo_inicio']).days
            qtd_diarias = delta + 0.5
        elif dados_pdf['integrantes']:
            qtd_diarias = dados_pdf['integrantes'][0].get('qtd_diarias', 0)

        itinerario = DiariasItinerario(
            usuario_gerador='IMPORTACAO_SEI',
            tipo_solicitacao_id=tipo_solicitacao,
            qtd_diarias_solicitadas=qtd_diarias,
            tipo_itinerario=tipo_viagem,
            n_processo=protocolo,
            status_id=2,  # Aceito (processos históricos já em andamento)
            data_solicitacao=dados_pdf.get('periodo_inicio', datetime.now()).date()
                            if dados_pdf.get('periodo_inicio') else date.today(),
            data_viagem=dados_pdf.get('periodo_inicio') or datetime.now(),
            data_retorno=dados_pdf.get('periodo_fim') or datetime.now(),
            origem=str(ESTADOS_SIGLA.get('PI', 22)),  # cod_ibge Teresina/PI
            estado_origem=dados_pdf.get('estado_origem') or 22,
            estado_destino=dados_pdf.get('estado_destino'),
            objetivo=dados_pdf.get('objetivo', ''),
            valor_total=dados_pdf.get('valor_total_geral', Decimal('0')),
            sei_protocolo=protocolo,
            sei_id_procedimento=str(resultado.get('id_procedimento', '')),
            etapa_atual_id=etapa_atual,
            superintendente_assinou=etapa_atual >= 2,
        )

        # Preencher documentos SEI na tabela normalizada
        for tipo, (sei_id, sei_formatado) in sei_cols.items():
            itinerario.set_doc(tipo, sei_id=sei_id, sei_formatado=sei_formatado)

        db.session.add(itinerario)
        db.session.flush()
        itinerario_id = itinerario.id
        print(f"    Itinerário criado: id={itinerario_id}")

        # ── 3. Criar DiariasItemItinerario (integrantes) ──────────────
        natureza_id = _natureza_id_por_tipo_viagem(tipo_viagem)

        for intg in dados_pdf['integrantes']:
            cpf_fmt = _formatar_cpf(intg['cpf'])
            if not cpf_fmt:
                continue

            cargo_id = _mapear_cargo_id(intg['cargo'])

            item = DiariasItemItinerario(
                id_itinerario=itinerario_id,
                cpf_pessoa=cpf_fmt,
                matricula_pessoa=intg['matricula'] or None,
                nome_pessoa=intg['nome'],
                cargo_id=cargo_id,
                natureza_id=natureza_id,
                valor_cargo=intg['valor_unitario'],
                banco_agencia=intg.get('banco_agencia', ''),
                banco_conta=intg.get('banco_conta', ''),
                vinculo=intg.get('vinculo', ''),
                cargo_folha=intg.get('cargo', ''),
            )
            db.session.add(item)
            print(f"    Item itinerário: {intg['nome']} (cargo_id={cargo_id})")

        # ── 4. Histórico de Movimentações ─────────────────────────────
        # Criar uma entrada para cada etapa até a etapa_atual
        for etapa_id in range(1, etapa_atual + 1):
            hist = DiariasHistoricoMovimentacao(
                id_itinerario=itinerario_id,
                id_etapa_anterior=etapa_id - 1 if etapa_id > 1 else None,
                id_etapa_nova=etapa_id,
                id_usuario_responsavel=None,
                data_movimentacao=datetime.now(),
                comentario='Importação histórica via SEI',
            )
            db.session.add(hist)
        print(f"    Histórico: {etapa_atual} movimentações criadas")

        # ── 5. Controle de Viagem ─────────────────────────────────────
        # Extrair texto de destino do trecho
        trecho = dados_pdf.get('trecho', '')
        destino_texto = trecho  # Texto completo como fallback
        partes_trecho = re.findall(r'([A-ZÀ-Ü\s]+)\s*[-–]\s*([A-Z]{2})', trecho.upper())
        if len(partes_trecho) >= 2:
            destino_texto = f"{partes_trecho[1][0].strip()}-{partes_trecho[1][1]}"

        # Determinar se tem relatório de viagem (prestação entregue)
        series_presentes = resultado.get('series_presentes', set())
        tem_relatorio = SERIE_RELATORIO_VIAGEM in series_presentes

        controle_viagem = DiariasControleViagem(
            processo=protocolo,
            itinerario_id=itinerario_id,
            tipo_viagem=tipo_viagem,
            origem='TERESINA-PI',
            destino=destino_texto,
            origem_id=22 if tipo_viagem == 2 else 2211001,  # estado PI ou município Teresina
            destino_id=dados_pdf.get('estado_destino'),
            data_inicio=dados_pdf.get('periodo_inicio', datetime.now()).date()
                        if dados_pdf.get('periodo_inicio') else date.today(),
            data_termino=dados_pdf.get('periodo_fim', datetime.now()).date()
                         if dados_pdf.get('periodo_fim') else date.today(),
            status_viagem=1,  # Realizada
        )
        db.session.add(controle_viagem)
        db.session.flush()
        print(f"    Controle viagem: id={controle_viagem.id}")

        # ── 6. Controle Servidores + 7. Prestação de Contas ───────────
        natureza_str = {1: '339014', 2: '339014', 3: '339014'}.get(tipo_viagem, '339014')
        sub_item = {1: '02', 2: '01', 3: '03'}.get(tipo_viagem, '01')

        for intg in dados_pdf['integrantes']:
            cpf_fmt = _formatar_cpf(intg['cpf'])
            if not cpf_fmt:
                continue

            ctrl_servidor = DiariasControleServidor(
                viagem_id=controle_viagem.id,
                cpf=cpf_fmt,
                nome=intg['nome'],
                vinculo=intg.get('vinculo', ''),
                qtd_diarias=Decimal(str(intg.get('qtd_diarias', 0))),
                valor_unitario=intg.get('valor_unitario', Decimal('0')),
                valor_total=intg.get('valor_total', Decimal('0')),
                natureza_despesa=natureza_str,
                sub_item=sub_item,
            )
            db.session.add(ctrl_servidor)
            db.session.flush()

            # Prestação de contas
            prestacao = DiariasControlePrestacao(
                servidor_id=ctrl_servidor.id,
                status=1 if tem_relatorio else 2,  # 1=Entregue, 2=Pendente
                relatorio=1 if tem_relatorio else 3,  # 1=Aprovado, 3=Pendente
                ano_referencia=dados_pdf.get('periodo_inicio').year
                               if dados_pdf.get('periodo_inicio') else 2026,
            )
            db.session.add(prestacao)
            print(f"    Controle servidor: {intg['nome']} | Prestação: "
                  f"{'Entregue' if tem_relatorio else 'Pendente'}")

        db.session.commit()
        print(f"  SUCESSO: Processo {protocolo} importado (itinerario_id={itinerario_id})")
        return itinerario_id

    except Exception as e:
        db.session.rollback()
        print(f"  ERRO ao inserir no banco: {e}")
        import traceback
        traceback.print_exc()
        return None


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def carregar_lista_processos():
    """Carrega lista de processos do arquivo."""
    if not os.path.exists(ARQUIVO_PROCESSOS):
        print(f"ERRO: Arquivo não encontrado: {ARQUIVO_PROCESSOS}")
        return []

    processos = []
    with open(ARQUIVO_PROCESSOS, 'r', encoding='utf-8') as f:
        for linha in f:
            linha = linha.strip()
            if linha and re.match(r'\d{5}\.\d{6}/\d{4}-\d{2}', linha):
                processos.append(linha)
    return processos


def main():
    parser = argparse.ArgumentParser(description='Importar diárias históricas do SEI')
    parser.add_argument('--teste', action='store_true',
                        help=f'Testar com apenas 1 processo ({PROCESSO_TESTE})')
    parser.add_argument('--executar', action='store_true',
                        help='Executar inserção no banco (default: DRY-RUN)')
    parser.add_argument('--processo', type=str, default=None,
                        help='Processar um processo específico')
    args = parser.parse_args()

    print("=" * 70)
    print("IMPORTAÇÃO DE DIÁRIAS HISTÓRICAS VIA SEI")
    print(f"Modo: {'EXECUTAR' if args.executar else 'DRY-RUN'}")
    print("=" * 70)

    app = create_app()
    with app.app_context():
        # Autenticar
        print("\nAutenticando no SEI...")
        token = gerar_token_sei_admin()
        if not token:
            print("ERRO FATAL: Falha na autenticação SEI.")
            sys.exit(1)
        print("Autenticação OK.\n")

        if args.teste or args.processo:
            # Modo teste: 1 processo
            processo = args.processo or PROCESSO_TESTE
            resultado = processar_processo(token, processo, executar=args.executar)
            if resultado:
                print(f"\nTeste concluído com sucesso!")
            else:
                print(f"\nTeste falhou!")
        else:
            # Modo completo: todos os processos
            processos = carregar_lista_processos()
            print(f"Total de processos a importar: {len(processos)}")

            ok = 0
            erros = 0
            for i, processo in enumerate(processos, 1):
                print(f"\n[{i}/{len(processos)}]", end='')
                try:
                    resultado = processar_processo(token, processo, executar=args.executar)
                    if resultado:
                        ok += 1
                    else:
                        erros += 1
                except Exception as e:
                    print(f"  EXCEÇÃO: {e}")
                    erros += 1

                # Rate limiting
                if i < len(processos):
                    time.sleep(1.5)

            print(f"\n{'='*70}")
            print(f"RESULTADO FINAL: {ok} OK, {erros} erros, {len(processos)} total")
            print(f"{'='*70}")


if __name__ == '__main__':
    main()
