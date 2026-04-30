"""
Parser HTML da Requisição de Diárias (IdSerie 532).

Extrai dados estruturados do documento HTML retornado pela API SEI quando se
faz download da Requisição de Diárias. Utilizado pelo serviço de vinculação
de processos existentes (vincular_processo_diaria.py).

Funções públicas:
    parsear_html_requisicao_diarias(html_bytes) -> dict
    detectar_tipo_itinerario_pelo_trecho(trecho) -> (tipo_id, estado_orig, estado_dest)

Código extraído e adaptado de scripts/importar_diarias_sei.py.
"""
import re
import unicodedata
from datetime import datetime
from decimal import Decimal

from bs4 import BeautifulSoup


# ── Mapeamento de estados brasileiros (sigla → cod_ibge) ─────────────────────

ESTADOS_SIGLA = {
    'AC': 12, 'AL': 27, 'AP': 16, 'AM': 13, 'BA': 29, 'CE': 23, 'DF': 53,
    'ES': 32, 'GO': 52, 'MA': 21, 'MT': 51, 'MS': 50, 'MG': 31, 'PA': 15,
    'PB': 25, 'PR': 41, 'PE': 26, 'PI': 22, 'RJ': 33, 'RN': 24, 'RS': 43,
    'RO': 11, 'RR': 14, 'SC': 42, 'SE': 28, 'SP': 35, 'TO': 17,
}

COD_IBGE_PIAUI = 22

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
    'PEDRO II': 'PI', 'SAO RAIMUNDO NONATO': 'PI', 'CORRENTE': 'PI',
    'URUÇUÍ': 'PI', 'URUCUI': 'PI', 'BOM JESUS': 'PI',
}


# ── Helpers internos ──────────────────────────────────────────────────────────

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
    texto = re.sub(r'[R$\s\xa0\u00a0]', '', str(texto))
    texto = texto.replace('.', '').replace(',', '.')
    try:
        return Decimal(texto)
    except Exception:
        return Decimal('0')


def _extrair_sigla_estado_do_trecho(trecho_texto):
    """
    Extrai siglas de estado de origem e destino a partir do texto do trecho.

    Exemplos:
        'TERESINA-PI/TERESINA-PI'     → ('PI', 'PI')
        'TERESINA-PI/SÃO PAULO-SP'    → ('PI', 'SP')
        'TERESINA-PI/BRASÍLIA-DF/...' → ('PI', 'DF')

    Returns:
        (uf_origem, uf_destino): strings de 2 letras ou (None, None)
    """
    if not trecho_texto:
        return None, None

    trecho = trecho_texto.upper().strip()

    # Padrão: CIDADE-UF/CIDADE-UF
    partes = re.findall(r'([A-ZÀ-Ü\s]+)\s*[-–]\s*([A-Z]{2})', trecho)
    if partes:
        estado_origem = partes[0][1]
        for _cidade, uf in partes[1:]:
            if uf != estado_origem:
                return estado_origem, uf
        return estado_origem, estado_origem

    # Fallback: detectar por cidade conhecida
    encontrados = []
    for cidade, uf in CIDADES_ESTADO.items():
        if cidade in trecho and uf not in encontrados:
            encontrados.append(uf)
    if len(encontrados) >= 2:
        return encontrados[0], encontrados[1]
    if len(encontrados) == 1:
        return encontrados[0], encontrados[0]

    return None, None


def _mapear_colunas_header(header_textos):
    """Mapeia nomes de colunas normalizados para índices lógicos."""
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


def _parsear_banco_info(integrante, banco_info):
    """Parseia informação bancária: 'AG. 3178-X CONTA CORRENTE: 28118-2 BANCO DO BRASIL'."""
    if not banco_info:
        return

    texto = banco_info.replace('\n', ' ').upper()

    match_ag = re.search(r'AG\.?\s*([\d\w.-]+)', texto)
    if match_ag:
        integrante['banco_agencia'] = match_ag.group(1).strip()

    match_conta = re.search(r'CONTA\s*(?:CORRENTE)?:?\s*([\d\w.-]+)', texto)
    if match_conta:
        integrante['banco_conta'] = match_conta.group(1).strip()

    bancos_conhecidos = [
        'BANCO DO BRASIL', 'CAIXA ECONOMICA', 'CAIXA ECONÔMICA',
        'BRADESCO', 'ITAU', 'ITAÚ', 'SANTANDER', 'SICOOB',
        'NUBANK', 'INTER', 'SICREDI',
    ]
    for banco in bancos_conhecidos:
        if banco in texto:
            integrante['banco_nome'] = banco
            break


def _parsear_linha_integrante_html(cells, col_indices):
    """Parseia uma linha <tr> da tabela HTML de integrantes."""
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

    cell_textos = []
    for c in cells:
        texto = c.get_text(separator=' ', strip=True)
        texto = re.sub(r'\s+', ' ', texto).strip()
        cell_textos.append(texto)

    for i, texto in enumerate(cell_textos):
        texto_limpo = texto.strip()
        if not texto_limpo:
            continue

        # CPF (###.###.###-##)
        cpf_match = re.search(r'(\d{3}[.\s]?\d{3}[.\s]?\d{3}[-.\s]?\d{2})', texto_limpo)
        if cpf_match and not integrante['cpf']:
            integrante['cpf'] = cpf_match.group(1).replace(' ', '')
            continue

        # Matrícula (6+ dígitos na primeira coluna)
        if i == 0 and re.match(r'^\d{5,}[-\d]*$', texto_limpo.replace(' ', '')):
            integrante['matricula'] = texto_limpo
            continue

        # Banco info
        texto_upper = texto_limpo.upper()
        if 'AG.' in texto_upper or 'CONTA' in texto_upper or 'BANCO' in texto_upper:
            integrante['banco_info'] = texto_limpo
            _parsear_banco_info(integrante, texto_limpo)
            continue

        # Quantidade de diárias (número <= 60)
        if re.match(r'^\d+[,.]?\d*$', texto_limpo.replace(' ', '')) and len(texto_limpo) <= 5:
            valor_num = texto_limpo.replace(',', '.')
            try:
                num = float(valor_num)
                if num <= 60 and not integrante['qtd_diarias']:
                    integrante['qtd_diarias'] = num
                    continue
            except Exception:
                pass

        # Valores monetários
        if re.search(r'R\$|^\d{1,3}\.\d{3}|\d+,\d{2}$', texto_limpo):
            valor = _limpar_valor_monetario(texto_limpo)
            if valor > 0:
                if not integrante['valor_unitario']:
                    integrante['valor_unitario'] = valor
                elif not integrante['valor_total']:
                    integrante['valor_total'] = valor
                continue

        # Vínculo
        if texto_upper in ('COMISSIONADO', 'EFETIVO', 'TERCEIRIZADO',
                           'TEMPORARIO', 'TEMPORÁRIO', 'ESTAGIARIO', 'ESTAGIÁRIO'):
            integrante['vinculo'] = texto_limpo
            continue

        # Cargo
        cargos_conhecidos = [
            'SUPERINTENDENTE', 'SECRETARIO', 'SECRETÁRIO', 'DIRETOR',
            'ASSESSOR', 'MOTORISTA', 'COORDENADOR', 'GERENTE',
            'ANALISTA', 'TECNICO', 'TÉCNICO', 'AUXILIAR', 'ASSISTENTE',
        ]
        if any(c in texto_upper for c in cargos_conhecidos):
            integrante['cargo'] = texto_limpo
            continue

        # Nome (texto longo sem números)
        if len(texto_limpo) > 5 and not re.search(r'\d', texto_limpo) and not integrante['nome']:
            integrante['nome'] = texto_limpo
            continue

    # Normalizar matrícula
    if integrante['matricula']:
        mat_limpo = re.sub(r'[^\d]', '', integrante['matricula'])
        if mat_limpo:
            integrante['matricula'] = mat_limpo

    return integrante


# ── Função pública principal ──────────────────────────────────────────────────

def parsear_html_requisicao_diarias(html_bytes):
    """
    Extrai dados estruturados do HTML da Requisição de Diárias (IdSerie 532).

    A API SEI retorna o documento em encoding ISO-8859-1. Esta função aceita
    tanto bytes quanto string e retorna um dict estruturado.

    Args:
        html_bytes: bytes (ISO-8859-1) ou str do HTML da Requisição

    Returns:
        dict com:
            numero_requisicao    (int|None)
            ano_requisicao       (int|None)
            integrantes          list[dict] — um por servidor
            valor_total_geral    Decimal
            objetivo             str
            trecho               str
            estado_origem        int|None — cod_ibge
            estado_destino       int|None — cod_ibge
            periodo_inicio       datetime|None
            periodo_fim          datetime|None
            processo_sei         str
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

    # Decodificar HTML (API SEI retorna iso-8859-1)
    if isinstance(html_bytes, bytes):
        try:
            html_str = html_bytes.decode('iso-8859-1', errors='replace')
        except Exception:
            html_str = html_bytes.decode('utf-8', errors='replace')
    else:
        html_str = html_bytes

    # Remover tags <a> inline que quebram nomes
    html_str = re.sub(r'<a[^>]*>([^<]*)</a>', r'\1', html_str)

    soup = BeautifulSoup(html_str, 'html.parser')
    texto_completo = soup.get_text(separator='\n')

    # Número da Requisição
    match_req = re.search(
        r'REQUISI[ÇCÇ][ÃAÃ]O\s+DE\s+DI[ÁAÁ]RIAS\s+N[ºo°]\s*(\d+)/(\d{4})',
        texto_completo, re.IGNORECASE
    )
    if match_req:
        resultado['numero_requisicao'] = int(match_req.group(1))
        resultado['ano_requisicao'] = int(match_req.group(2))

    # Processo SEI
    match_sei = re.search(r'SEI\s+(\d{5}\.\d{6}/\d{4}-\d{2})', texto_completo)
    if not match_sei:
        title = soup.find('title')
        if title:
            match_sei = re.search(r'(\d{5}\.\d{6}/\d{4}-\d{2})', title.get_text())
    if match_sei:
        resultado['processo_sei'] = match_sei.group(1)

    # Tabela principal
    tabela_principal = soup.find('table')
    if not tabela_principal:
        return resultado

    linhas = tabela_principal.find_all('tr')
    if not linhas:
        return resultado

    # Identificar header
    header_cells = linhas[0].find_all(['td', 'th'])
    header_textos = [_normalizar_texto(c.get_text()) for c in header_cells]
    col_indices = _mapear_colunas_header(header_textos)

    # Iterar linhas de dados
    for tr in linhas[1:]:
        cells = tr.find_all('td')
        if not cells:
            continue

        cell_texts = [c.get_text(separator=' ', strip=True) for c in cells]
        cell_texts_joined = ' '.join(cell_texts).upper()

        # Linha de VALOR TOTAL
        if 'VALOR TOTAL' in cell_texts_joined:
            for ct in reversed(cell_texts):
                valor = _limpar_valor_monetario(ct)
                if valor > 0:
                    resultado['valor_total_geral'] = valor
                    break
            continue

        # Linha de OBJETIVO DA VIAGEM
        if 'OBJETIVO DA VIAGEM' in cell_texts_joined or 'OBJETIVO' in cell_texts_joined:
            td = cells[0] if len(cells) == 1 else cells[-1]
            paragrafos = td.find_all('p')
            textos_obj = []
            for p in paragrafos:
                t = p.get_text(strip=True)
                if t and 'OBJETIVO DA VIAGEM' not in t.upper():
                    textos_obj.append(t)
            if textos_obj:
                resultado['objetivo'] = ' '.join(textos_obj).strip()
            else:
                # Texto direto na célula
                raw = td.get_text(separator=' ', strip=True)
                cleaned = re.sub(r'(?i)objetivo da viagem\s*:?\s*', '', raw).strip()
                if cleaned:
                    resultado['objetivo'] = cleaned
            continue

        # Linha de TRECHO / PERÍODO
        if 'TRECHO' in cell_texts_joined:
            full_text = ' '.join(cell_texts)
            match_trecho = re.search(r'TRECHO:\s*(.+?)(?:PERÍODO|PERIODO|$)', full_text, re.IGNORECASE)
            if match_trecho:
                resultado['trecho'] = match_trecho.group(1).strip()
            else:
                match_trecho2 = re.search(r'TRECHO[:\s]*(.+)', full_text, re.IGNORECASE)
                if match_trecho2:
                    resultado['trecho'] = match_trecho2.group(1).strip()

            match_periodo = re.search(
                r'PER[ÍIÍ]ODO:\s*(\d{2}/\d{2}/\d{4})\s*[AaÀà]\s*(\d{2}/\d{2}/\d{4})',
                full_text, re.IGNORECASE
            )
            if match_periodo:
                try:
                    resultado['periodo_inicio'] = datetime.strptime(match_periodo.group(1), '%d/%m/%Y')
                    resultado['periodo_fim'] = datetime.strptime(match_periodo.group(2), '%d/%m/%Y')
                except ValueError:
                    pass
            continue

        # Linha de DESPACHO (ignorar)
        if 'DESPACHO' in cell_texts_joined:
            continue

        # Linha de integrante (tem CPF ou matrícula)
        has_cpf = any(re.search(r'\d{3}[.\s]?\d{3}[.\s]?\d{3}[-.\s]?\d{2}', ct) for ct in cell_texts)
        has_matricula = any(re.search(r'\d{5,}', ct) for ct in cell_texts[:2])

        if has_cpf or has_matricula:
            integrante = _parsear_linha_integrante_html(cells, col_indices)
            if integrante and integrante.get('cpf'):
                resultado['integrantes'].append(integrante)

    # Estado origem/destino do trecho
    uf_orig, uf_dest = _extrair_sigla_estado_do_trecho(resultado['trecho'])
    resultado['estado_origem'] = ESTADOS_SIGLA.get(uf_orig)
    resultado['estado_destino'] = ESTADOS_SIGLA.get(uf_dest)

    # Valor total geral — fallback: buscar no texto completo
    if resultado['valor_total_geral'] == 0:
        match_total = re.search(r'VALOR\s+TOTAL[^\d]*([\d.,]+)', texto_completo)
        if match_total:
            resultado['valor_total_geral'] = _limpar_valor_monetario(match_total.group(1))

    return resultado
