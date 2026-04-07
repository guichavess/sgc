"""
Parser de PDFs de cotação de passagens aéreas.

Estratégia de extração (em ordem de prioridade):
  1. Extrai imagens embutidas no PDF (PyPDF2) — mais limpo, sem rendering
  2. Fallback: renderiza PDF como imagem (pypdfium2)

OCR: Tesseract com PSM 11 (sparse text) a 3x de escala para extração
máxima de tokens (preços, voos, datas, cidades), seguido de reagrupamento
por máquina de estados.

Agrupamento de conexões:
  - Trecho com preço + trecho sem preço → conexão (preço no 1º trecho)
  - Trecho sem preço + trecho com preço → conexão (preço no 2º trecho)
  - Trecho com preço seguido de outro trecho com preço → ambos standalone
"""
import io
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# OCR engine singleton (EasyOCR — carregado sob demanda)
_ocr_reader = None

# ── Regex ────────────────────────────────────────────────────────────────────
_RE_VOO = re.compile(
    r'(LA|AD|G3|JJ|TP|AF|AA|DL|UA|AV|CM|IB|LH|KL|BA|EK|QR|ET|AR|2Z|VB)\s*(\d{3,5})',
    re.IGNORECASE
)
_RE_DATA_HORA = re.compile(r'(\d{2}/\d{2}/\d{4})\s*-?\s*(\d{2})\s*:\s*(\d{2})')
_RE_HORA_ONLY = re.compile(r'-\s*(\d{2})\s*:\s*(\d{2})')
_RE_PRECO = re.compile(r'R[\$S]\s*([\d.]+,\d{2})')
_RE_PRECO_VALOR = re.compile(r'([\d.]+,\d{2})\s*$|[Vv]alor.*?([\d.]+,\d{2})')

# ── Cidades conhecidas ──────────────────────────────────────────────────────
_CIDADES = [
    # Capitais e aeroportos principais
    'Teresina', 'Guarulhos', 'Congonhas', 'Brasilia', 'Brasília',
    'Recife', 'Fortaleza', 'Salvador', 'Galeão', 'Galeao',
    'Confins', 'Manaus', 'Belém', 'Belem', 'Curitiba',
    'Porto Alegre', 'Florianópolis', 'Florianopolis', 'Goiânia', 'Goiania',
    'Campinas', 'Viracopos', 'Natal', 'Maceió', 'Maceio',
    'São Luís', 'Sao Luis', 'João Pessoa', 'Joao Pessoa',
    'Aracaju', 'Palmas', 'Cuiabá', 'Cuiaba', 'Campo Grande',
    'Rio Branco', 'Macapá', 'Macapa', 'Boa Vista', 'Porto Velho',
    'São Paulo', 'Sao Paulo', 'Rio de Janeiro',
    # Cidades intermediárias com aeroporto
    'Vitória', 'Vitoria', 'Petrolina', 'Imperatriz', 'Marabá', 'Maraba',
    'Santarém', 'Santarem', 'Carajás', 'Carajas', 'Parnaíba', 'Parnaiba',
    'Juazeiro do Norte', 'Uberlândia', 'Uberlandia', 'Ribeirão Preto',
    'Ribeirao Preto', 'Londrina', 'Maringá', 'Maringa', 'Foz do Iguaçu',
    'Foz do Iguacu', 'Navegantes', 'Joinville', 'Chapecó', 'Chapeco',
    'Caxias do Sul', 'Montes Claros', 'Ilhéus', 'Ilheus', 'Porto Seguro',
    'Juiz de Fora', 'Bauru', 'São José do Rio Preto', 'Presidente Prudente',
    'Sinop', 'Ji-Paraná', 'Ji-Parana', 'Cruzeiro do Sul',
    # Códigos IATA comuns (3 letras maiúsculas reconhecidas)
    'THE', 'GRU', 'CGH', 'BSB', 'REC', 'FOR', 'SSA', 'GIG', 'SDU',
    'CNF', 'MAO', 'BEL', 'CWB', 'POA', 'FLN', 'GYN', 'VCP', 'NAT',
    'MCZ', 'SLZ', 'JPA', 'AJU', 'PMW', 'CGB', 'CGR', 'RBR', 'MCP',
    'BVB', 'PVH', 'VIX', 'PTN', 'IMP', 'STM', 'CKS', 'PHB',
]
_CIDADES_LOWER = {c.lower(): c for c in _CIDADES}

# ── CIA mapping (sigla do voo → nome) ───────────────────────────────────────
_CIA_MAP = {
    'LA': 'LATAM', 'AD': 'Azul', 'G3': 'GOL', 'JJ': 'LATAM',
    'TP': 'TAP', 'AF': 'Air France', 'AA': 'American', 'DL': 'Delta',
    'UA': 'United', 'AV': 'Avianca', 'CM': 'Copa', 'IB': 'Iberia',
    'LH': 'Lufthansa', 'KL': 'KLM', 'BA': 'British Airways',
    'EK': 'Emirates', 'QR': 'Qatar', 'ET': 'Ethiopian',
    'AR': 'Aerolíneas', '2Z': 'Voepass', 'VB': 'VivaAir',
}

# ── Regex adicionais para formato textual ────────────────────────────────────
# Rota: "Cidade1 – Cidade2 DD/MM/YYYY" OU "Cidade1 DD/MM/YYYY Cidade2" (OCR invertido)
_RE_ROTA = re.compile(r'(.+?)\s*[-–]?\s+(.+?)\s+(\d{2}/\d{2}/\d{4})')
_RE_ROTA_ALT = re.compile(r'(.+?)\s+(\d{2}/\d{2}/\d{4})\s+(.+)')

# Correções comuns de OCR (letra trocada)
_OCR_FIXES = {
    'ieresina': 'Teresina', 'leresina': 'Teresina',
    'guarulnos': 'Guarulhos', 'cuaruihos': 'Guarulhos',
    'brasiiia': 'Brasília', 'brasiilia': 'Brasília',
}
_RE_OPCAO = re.compile(r'Op[çc][aã]o\s*\d+\s*:\s*(.+)', re.IGNORECASE)
_RE_VOO_HORA = re.compile(
    r'(\d{3,5})\s+(.+?)\s+(\d{2}[:.]\d{2})h?\s*[/]?\s*(.+?)\s+(\d{2}[:.]\d{2})h?'
)


def _cia_from_voo(voo_code):
    """Deduz companhia aérea pela sigla do voo."""
    sigla = voo_code.split()[0] if voo_code else ''
    return _CIA_MAP.get(sigla, sigla)


# ══════════════════════════════════════════════════════════════════════════════
# Extração de imagens do PDF
# ══════════════════════════════════════════════════════════════════════════════

def _extract_images_pypdf2(pdf_bytes):
    """Extrai imagens embutidas do PDF usando PyPDF2. Retorna lista de PIL Image."""
    try:
        import PyPDF2
        from PIL import Image
    except ImportError:
        return []

    images = []
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            res = page.get('/Resources')
            if not res or '/XObject' not in res:
                continue
            xobjects = res['/XObject']
            for name in xobjects:
                obj = xobjects[name].get_object()
                if obj.get('/Subtype') != '/Image':
                    continue
                w = obj['/Width']
                h = obj['/Height']
                if w < 200 or h < 50:
                    continue
                data = obj.get_data()
                bits = obj.get('/BitsPerComponent', 8)
                cs = str(obj.get('/ColorSpace', ''))
                expected_size = w * h * 3  # RGB
                if len(data) == expected_size:
                    img = Image.frombytes('RGB', (w, h), data)
                    images.append(img)
                elif len(data) == w * h:
                    img = Image.frombytes('L', (w, h), data)
                    images.append(img.convert('RGB'))
    except Exception as e:
        logger.warning('[COTACAO_PARSER] Falha ao extrair imagens PyPDF2: %s', e)

    # Maiores primeiro (imagem principal do PDF primeiro)
    images.sort(key=lambda i: i.width * i.height, reverse=True)
    return images


def _render_pypdfium2(pdf_bytes):
    """Fallback: renderiza páginas do PDF como imagens usando pypdfium2."""
    try:
        import pypdfium2 as pdfium
    except ImportError:
        return []

    images = []
    try:
        pdf = pdfium.PdfDocument(pdf_bytes)
        for page in pdf:
            bitmap = page.render(scale=3)
            img = bitmap.to_pil()
            images.append(img)
        pdf.close()
    except Exception as e:
        logger.warning('[COTACAO_PARSER] Falha ao renderizar pypdfium2: %s', e)

    return images


# ══════════════════════════════════════════════════════════════════════════════
# OCR + Classificação de tokens
# ══════════════════════════════════════════════════════════════════════════════

def _get_ocr_reader():
    """Retorna instância singleton do EasyOCR Reader."""
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr
        _ocr_reader = easyocr.Reader(['pt', 'en'], gpu=False, verbose=False)
    return _ocr_reader


def _ocr_image(img, psm=11, scale=3):
    """Aplica OCR em uma imagem PIL via EasyOCR. Retorna texto bruto.

    O parâmetro `psm` é mantido por compatibilidade mas não é usado
    pelo EasyOCR (era específico do Tesseract).
    """
    import numpy as np
    from PIL import ImageEnhance

    # Escalar apenas até 2x para evitar estouro de memória no EasyOCR
    effective_scale = min(scale, 2)
    if effective_scale > 1:
        w, h = img.size
        img = img.resize((w * effective_scale, h * effective_scale))
    img = ImageEnhance.Contrast(img).enhance(1.3)

    if img.mode != 'RGB':
        img = img.convert('RGB')

    reader = _get_ocr_reader()
    img_np = np.array(img)
    results = reader.readtext(img_np, detail=1, paragraph=False)

    # Reconstruir linhas na ordem de leitura (agrupar por coordenada Y)
    if not results:
        return ''

    # Ordenar por Y, depois X
    results.sort(key=lambda r: (r[0][0][1], r[0][0][0]))

    lines = []
    current_line_parts = []
    last_y = None
    line_height = 15  # threshold para agrupar na mesma linha

    for bbox, text, conf in results:
        y = bbox[0][1]
        if last_y is not None and abs(y - last_y) > line_height:
            lines.append(' '.join(current_line_parts))
            current_line_parts = []
        current_line_parts.append(text)
        last_y = y

    if current_line_parts:
        lines.append(' '.join(current_line_parts))

    return '\n'.join(lines)


def _classify_line(line):
    """
    Classifica uma linha OCR. Retorna tupla (tipo, dados).

    Tipos: 'secao_ida', 'secao_volta', 'secao_volta_extra', 'voo', 'data',
           'cidade', 'preco', 'header', 'skip'
    """
    stripped = line.strip()
    if not stripped:
        return ('skip', None)

    upper = stripped.upper()

    # Cabeçalhos de seção
    if 'IDA' in upper and ('VOO' in upper or 'OP' in upper):
        return ('secao_ida', None)
    if 'VOLTA' in upper and ('VOO' in upper or 'OP' in upper):
        return ('secao_volta', None)

    # Cabeçalho de tabela
    if ('CIA' in upper or 'VOO' in upper) and ('TOTAL' in upper or 'BAGAGEM' in upper or 'SAIDA' in upper):
        return ('header', None)
    if 'DISPON' in upper or 'COTAC' in upper:
        return ('header', None)

    # Preço
    m_preco = _RE_PRECO.search(stripped)
    if m_preco:
        valor = m_preco.group(1).replace('.', '').replace(',', '.')
        return ('preco', valor)

    # Código de voo
    m_voo = _RE_VOO.search(stripped)
    if m_voo:
        voo = f'{m_voo.group(1).upper()} {m_voo.group(2)}'
        return ('voo', voo)

    # Data com hora completa (DD/MM/YYYY - HH:MM)
    m_data = _RE_DATA_HORA.search(stripped)
    if m_data:
        try:
            dt = datetime.strptime(
                f'{m_data.group(1)} {m_data.group(2)}:{m_data.group(3)}',
                '%d/%m/%Y %H:%M'
            )
            return ('data', dt)
        except ValueError:
            pass

    # Hora parcial (- HH:MM) — imagens pequenas sem data
    m_hora = _RE_HORA_ONLY.search(stripped)
    if m_hora and not any(c.isalpha() for c in stripped.replace('-', '')):
        hora = f'{m_hora.group(1)}:{m_hora.group(2)}'
        return ('hora_parcial', hora)

    # Cidade
    lower = stripped.lower().strip()
    # Remove caracteres espúrios do OCR (como %, ?, etc)
    lower_clean = re.sub(r'[^a-záàâãéèêíïóôõúüç\s]', '', lower).strip()
    for cl, cn in _CIDADES_LOWER.items():
        if cl in lower_clean:
            return ('cidade', cn)

    return ('skip', None)


# ══════════════════════════════════════════════════════════════════════════════
# Montagem de trechos a partir de tokens (PSM 11)
# ══════════════════════════════════════════════════════════════════════════════

def _tokens_to_trechos(tokens):
    """
    Converte lista de (tipo, dados) em lista de dicts de trechos.

    Cada trecho: {voo, datas[], cidades[], preco, secao}

    Lógica: acumula tokens até encontrar um novo VOO ou PRECO.
    PRECO fecha o trecho atual. Novo VOO inicia um novo trecho.
    """
    trechos = []
    secao = None  # 'ida', 'volta'
    current = None  # trecho em construção

    def _flush():
        nonlocal current
        if current and current['voo']:
            trechos.append(current)
        current = None

    for tipo, dados in tokens:
        if tipo == 'secao_ida':
            _flush()
            secao = 'ida'
            continue
        if tipo in ('secao_volta', 'secao_volta_extra'):
            _flush()
            secao = 'volta'
            continue
        if tipo in ('header', 'skip'):
            continue

        if tipo == 'voo':
            # Novo voo — fecha anterior se existir
            if current and current['voo']:
                _flush()
            current = {
                'voo': dados,
                'datas': [],
                'cidades': [],
                'preco': None,
                'secao': secao,
            }
            continue

        if not current:
            # Tokens antes do primeiro voo — ignora
            continue

        if tipo == 'data':
            current['datas'].append(dados)
        elif tipo == 'hora_parcial':
            current['datas'].append(dados)  # string HH:MM em vez de datetime
        elif tipo == 'cidade':
            if dados not in current['cidades']:
                current['cidades'].append(dados)
        elif tipo == 'preco':
            current['preco'] = dados

    _flush()
    return trechos


def _agrupar_conexoes(trechos):
    """
    Agrupa trechos em opções de voo (standalone ou conexão).

    Regras:
      - Trecho com preço + próximo sem preço → conexão
      - Trecho sem preço + próximo com preço → conexão
      - Trecho com preço sozinho → standalone
    """
    opcoes = []
    i = 0

    while i < len(trechos):
        curr = trechos[i]
        nxt = trechos[i + 1] if i + 1 < len(trechos) else None

        # Conexão: só agrupa se estão na mesma seção
        mesma_secao = nxt and curr['secao'] == nxt['secao']

        if curr['preco'] and mesma_secao and nxt and not nxt['preco']:
            opcoes.append(_montar_opcao_v2(curr, nxt))
            i += 2
        elif not curr['preco'] and mesma_secao and nxt and nxt['preco']:
            opcoes.append(_montar_opcao_v2(curr, nxt))
            i += 2
        else:
            opcoes.append(_montar_opcao_v2(curr, None))
            i += 1

    return opcoes


def _montar_opcao_v2(trecho1, trecho2=None):
    """Monta dict de opção de voo a partir de trechos tokenizados."""
    preco = trecho1.get('preco') or (trecho2.get('preco') if trecho2 else None)

    def _get_dt(datas, idx):
        if idx < len(datas):
            val = datas[idx]
            if isinstance(val, datetime):
                return val
        return None

    opcao = {
        'cia': _cia_from_voo(trecho1['voo']),
        'voo': trecho1['voo'],
        'saida': _get_dt(trecho1['datas'], 0),
        'chegada': _get_dt(trecho1['datas'], 1),
        'origem': trecho1['cidades'][0] if trecho1['cidades'] else '',
        'destino': trecho1['cidades'][1] if len(trecho1['cidades']) >= 2 else (
            trecho1['cidades'][0] if len(trecho1['cidades']) == 1 else ''
        ),
        'valor': preco,
        'bagagem': '1',
        'secao': trecho1.get('secao'),
        'cia_conexao': None,
        'voo_conexao': None,
        'saida_conexao': None,
        'chegada_conexao': None,
        'origem_conexao': None,
        'destino_conexao': None,
    }

    # Corrigir destino quando só 1 cidade (a cidade pode ser igual à origem)
    if opcao['origem'] == opcao['destino'] and len(trecho1['cidades']) == 1:
        opcao['destino'] = ''

    if trecho2:
        opcao['cia_conexao'] = _cia_from_voo(trecho2['voo'])
        opcao['voo_conexao'] = trecho2['voo']
        opcao['saida_conexao'] = _get_dt(trecho2['datas'], 0)
        opcao['chegada_conexao'] = _get_dt(trecho2['datas'], 1)
        opcao['origem_conexao'] = trecho2['cidades'][0] if trecho2['cidades'] else ''
        opcao['destino_conexao'] = trecho2['cidades'][1] if len(trecho2['cidades']) >= 2 else (
            trecho2['cidades'][0] if len(trecho2['cidades']) == 1 else ''
        )
        if opcao['origem_conexao'] == opcao['destino_conexao'] and len(trecho2['cidades']) == 1:
            opcao['destino_conexao'] = ''

    return opcao


# ══════════════════════════════════════════════════════════════════════════════
# Extração de texto embutido (PDFs text-based do SEI)
# ══════════════════════════════════════════════════════════════════════════════

def _extract_embedded_text(pdf_bytes):
    """
    Extrai texto embutido do PDF usando PyPDF2.
    Retorna string com todo o texto ou None se vazio/falhar.
    Muitos PDFs do SEI são text-based (não precisam de OCR).
    """
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        all_text = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                all_text.append(text)
        full = '\n'.join(all_text).strip()
        # Só retorna se tiver conteúdo substantivo (>50 chars e algum token útil)
        if len(full) > 50 and (_RE_VOO.search(full) or _RE_PRECO.search(full)):
            return full
    except Exception as e:
        logger.debug('[COTACAO_PARSER] Falha ao extrair texto embutido: %s', e)
    return None


def _extrair_data_cotacao(texto, resultado):
    """Extrai a data da cotação do texto e salva em resultado['data_cotacao']."""
    m = re.search(r'(?:dispon.vel|disponivel|disponibilidade)\s+em\s+(\d{2}/\s*\d{2}/\s*\d{4})',
                  texto, re.IGNORECASE)
    if m:
        resultado['data_cotacao'] = re.sub(r'\s+', '', m.group(1))


# ══════════════════════════════════════════════════════════════════════════════
# Parser principal (v2 — token-based com extração de imagens)
# ══════════════════════════════════════════════════════════════════════════════

def extrair_cotacoes_pdf(pdf_bytes):
    """
    Extrai opções de voo de um PDF de cotação de passagens.

    Estratégia:
      1. Extrai imagens embutidas do PDF (PyPDF2)
      2. Se não houver imagens, renderiza o PDF (pypdfium2)
      3. OCR com PSM 11 (sparse text) para máxima extração de tokens
      4. Se PSM 11 não encontra seções IDA/VOLTA, tenta PSM 6 (tabela)
      5. Fallback: formato textual (Opção N: CIA)

    Returns:
        dict com:
            ida: list de opções de voo
            volta: list de opções de voo
            data_cotacao: str ou None
            erros: list de str

        Cada opção de voo:
            cia, voo, saida, chegada, origem, destino, valor, bagagem,
            cia_conexao, voo_conexao, saida_conexao, chegada_conexao,
            origem_conexao, destino_conexao
    """
    resultado = {'ida': [], 'volta': [], 'data_cotacao': None, 'erros': []}

    # 0. Tentar extrair texto embutido no PDF (muitos PDFs do SEI são text-based)
    texto_embutido = _extract_embedded_text(pdf_bytes)
    if texto_embutido:
        logger.info('[COTACAO_PARSER] Texto embutido extraído (%d chars)', len(texto_embutido))
        tokens_emb = [_classify_line(line) for line in texto_embutido.split('\n')]
        trechos_emb = _tokens_to_trechos(tokens_emb)
        if trechos_emb:
            opcoes_emb = _agrupar_conexoes(trechos_emb)
            for opcao in opcoes_emb:
                secao = opcao.pop('secao', None)
                if secao == 'ida':
                    resultado['ida'].append(opcao)
                elif secao == 'volta':
                    resultado['volta'].append(opcao)
                else:
                    resultado['ida'].append(opcao)
            if resultado['ida'] or resultado['volta']:
                logger.info('[COTACAO_PARSER] Texto embutido: %d ida, %d volta',
                             len(resultado['ida']), len(resultado['volta']))
                # Extrair data da cotação do texto embutido
                _extrair_data_cotacao(texto_embutido, resultado)
                return resultado

        # Texto embutido sem seções IDA/VOLTA — tentar fallbacks textuais
        # Primeiro: formato tabular v2 (sub-seções OPÇÕES POR)
        if _detectar_formato_tabular_v2(texto_embutido):
            _parse_formato_tabular_v2(texto_embutido, resultado)

        if not resultado['ida'] and not resultado['volta']:
            upper_emb = texto_embutido.upper()
            if 'VOOS IDA' in upper_emb or 'VOO IDA' in upper_emb:
                _parse_formato_tabular(texto_embutido, resultado)
            elif _RE_ROTA.search(texto_embutido) and _RE_OPCAO.search(texto_embutido):
                _parse_formato_textual(texto_embutido, resultado)

        if resultado['ida'] or resultado['volta']:
            logger.info('[COTACAO_PARSER] Texto embutido (fallback): %d ida, %d volta',
                         len(resultado['ida']), len(resultado['volta']))
            _extrair_data_cotacao(texto_embutido, resultado)
            return resultado

        logger.info('[COTACAO_PARSER] Texto embutido não produziu resultados, tentando OCR...')

    # Verificar se EasyOCR está disponível antes de tentar OCR
    try:
        import easyocr  # noqa: F401
    except ImportError:
        logger.warning('[COTACAO_PARSER] easyocr não instalado — OCR indisponível.')
        resultado['erros'].append('OCR indisponível (easyocr não instalado).')
        return resultado

    # 1. Extrair imagens
    images = _extract_images_pypdf2(pdf_bytes)
    if not images:
        images = _render_pypdfium2(pdf_bytes)

    if not images:
        resultado['erros'].append('Não foi possível extrair imagens do PDF.')
        return resultado

    logger.info('[COTACAO_PARSER] %d imagem(ns) extraída(s) do PDF', len(images))

    # 2. OCR todas as imagens (EasyOCR — uma única chamada por imagem)
    all_tokens = []
    all_text_parts = []

    for idx, img in enumerate(images):
        ocr_text = _ocr_image(img, scale=2)
        tokens = [_classify_line(line) for line in ocr_text.split('\n')]
        all_tokens.extend(tokens)
        all_text_parts.append(ocr_text)
        logger.debug('[COTACAO_PARSER] Img %d: %d tokens, %d linhas',
                      idx, len(tokens), len(ocr_text.split('\n')))

    # 3. Tentar parsing token-based
    trechos = _tokens_to_trechos(all_tokens)

    if trechos:
        opcoes = _agrupar_conexoes(trechos)
        for opcao in opcoes:
            secao = opcao.pop('secao', None)
            if secao == 'ida':
                resultado['ida'].append(opcao)
            elif secao == 'volta':
                resultado['volta'].append(opcao)
            else:
                resultado['ida'].append(opcao)

        logger.info('[COTACAO_PARSER] Token-based: %d ida, %d volta',
                     len(resultado['ida']), len(resultado['volta']))

    # 4. Se token-based não encontrou, tentar parsers de formato
    if not resultado['ida'] and not resultado['volta']:
        texto_completo = '\n'.join(all_text_parts)
        if texto_completo.strip():
            # Primeiro: formato tabular v2 (sub-seções OPÇÕES POR)
            if _detectar_formato_tabular_v2(texto_completo):
                _parse_formato_tabular_v2(texto_completo, resultado)

            if not resultado['ida'] and not resultado['volta']:
                upper = texto_completo.upper()
                if 'VOOS IDA' in upper or 'VOO IDA' in upper:
                    _parse_formato_tabular(texto_completo, resultado)
                elif _RE_ROTA.search(texto_completo) or _RE_ROTA_ALT.search(texto_completo):
                    _parse_formato_textual(texto_completo, resultado)
                else:
                    _parse_formato_tabular(texto_completo, resultado)

            logger.info('[COTACAO_PARSER] Fallback textual/tabular: %d ida, %d volta',
                         len(resultado['ida']), len(resultado['volta']))

    # 5. Extrair data da cotação
    for text in all_text_parts:
        m = re.search(r'(?:dispon.vel|disponivel|disponibilidade)\s+em\s+(\d{2}/\d{2}/\d{4})',
                       text, re.IGNORECASE)
        if m:
            resultado['data_cotacao'] = m.group(1)
            break

    # Extrair também do texto embutido no PDF
    if not resultado['data_cotacao']:
        try:
            import PyPDF2
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            for page in reader.pages:
                page_text = page.extract_text() or ''
                m = re.search(r'(?:dispon.vel|disponivel|disponibilidade)\s+em\s+(\d{2}/\s*\d{2}/\s*\d{4})',
                               page_text, re.IGNORECASE)
                if m:
                    resultado['data_cotacao'] = re.sub(r'\s+', '', m.group(1))
                    break
        except Exception:
            pass

    if not resultado['ida'] and not resultado['volta']:
        resultado['erros'].append('Não foi possível extrair opções de voo do PDF.')

    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# Parser tabular v2 — layout com sub-seções (OPÇÕES POR CONGONHAS/GUARULHOS)
# ══════════════════════════════════════════════════════════════════════════════

def _normalizar_ocr_tabular(texto):
    """Normaliza artefatos comuns de OCR no layout tabular."""
    # Separadores de hora: *, ;, , → :  (ex: 18*05 → 18:05, 19;50 → 19:50)
    texto = re.sub(r'(\d{2})[*;,](\d{2})', r'\1:\2', texto)
    # Números colados com datas que deveriam ser horas: 19840 → 19:40 (só em contexto de horário)
    texto = re.sub(r'(\d{2})8(\d{2})\b', r'\1:\2', texto)  # OCR lê : como 8
    # Voo colado: 1A → LA (OCR confunde L com 1)
    texto = re.sub(r'\b1A\s*(\d{3,5})', r'LA \1', texto)
    # L4 → LA (OCR confunde A com 4)
    texto = re.sub(r'\bL4\s*(\d{3,5})', r'LA \1', texto)
    # Rs, RS → R$ para preços
    texto = re.sub(r'\bR[sS]\s+', 'R$ ', texto)
    return texto


_RE_HORA_FLEX = re.compile(r'(\d{2})[:\-.](\d{2})')
_RE_DATA_FLEX = re.compile(r'(\d{2}/\d{2}/\d{4})')


def _parse_linha_tabular_v2(linha):
    """
    Extrai dados de uma linha de tabela de cotação (layout v2).
    Retorna dict com campos extraídos ou None se não é uma linha de voo.
    """
    linha = _normalizar_ocr_tabular(linha)

    # Procurar voo (CIA + número)
    m_voo = _RE_VOO.search(linha)
    if not m_voo:
        return None

    cia_code = m_voo.group(1).upper()
    voo_num = m_voo.group(2)
    cia_nome = _CIA_MAP.get(cia_code, cia_code)
    voo_str = f'{cia_code} {voo_num}'

    # Extrair datas (DD/MM/YYYY)
    datas = _RE_DATA_FLEX.findall(linha)

    # Extrair horas (HH:MM) — pode haver várias
    horas = _RE_HORA_FLEX.findall(linha)
    # Filtrar horas que fazem parte das datas (ex: dentro de DD/MM/YYYY)
    horas_limpas = []
    for h, m in horas:
        hora_str = f'{h}:{m}'
        # Verificar se não é parte de uma data
        if hora_str not in linha.replace('/', ':'):
            horas_limpas.append(hora_str)

    # Combinar datas e horas em datetimes
    saida = None
    chegada = None
    if len(datas) >= 1 and len(horas_limpas) >= 1:
        try:
            saida = datetime.strptime(f'{datas[0]} {horas_limpas[0]}', '%d/%m/%Y %H:%M')
        except ValueError:
            pass
    if len(datas) >= 2 and len(horas_limpas) >= 2:
        try:
            chegada = datetime.strptime(f'{datas[1]} {horas_limpas[1]}', '%d/%m/%Y %H:%M')
        except ValueError:
            pass
    elif len(datas) >= 1 and len(horas_limpas) >= 2:
        # Mesma data, 2 horas (saída e chegada no mesmo dia)
        try:
            chegada = datetime.strptime(f'{datas[0]} {horas_limpas[1]}', '%d/%m/%Y %H:%M')
        except ValueError:
            pass

    # Extrair cidades
    cidades = []
    for cidade in _CIDADES:
        if re.search(r'\b' + re.escape(cidade) + r'\b', linha, re.IGNORECASE):
            if cidade.lower() not in [c.lower() for c in cidades]:
                cidades.append(cidade)

    origem = cidades[0] if len(cidades) >= 1 else None
    destino = cidades[1] if len(cidades) >= 2 else None

    # Extrair preço
    m_preco = _RE_PRECO.search(linha)
    valor = None
    if m_preco:
        valor = _parse_valor(m_preco.group(1))

    return {
        'cia': cia_nome,
        'voo': voo_str,
        'saida': saida,
        'chegada': chegada,
        'origem': origem,
        'destino': destino,
        'valor': valor,
    }


def _parse_formato_tabular_v2(texto, resultado):
    """
    Parser para layout tabular com sub-seções (OPÇÕES POR CONGONHAS/GUARULHOS).
    Cada sub-seção tem sub-tabelas IDA e VOLTA.
    Conexões: 2 linhas de voo onde a 1ª não tem preço e a 2ª tem (ou vice-versa).
    """
    lines = texto.split('\n')
    secao_atual = None  # 'ida' ou 'volta'
    voos_pendentes = []  # acumula linhas de voo da seção atual

    def _flush_secao(voos, secao, resultado):
        """Processa os voos acumulados de uma seção, agrupando conexões."""
        i = 0
        while i < len(voos):
            v = voos[i]
            # Verificar se é conexão: voo atual sem preço + próximo com preço (ou vice-versa)
            if i + 1 < len(voos):
                v_next = voos[i + 1]
                if v['valor'] is not None and v_next['valor'] is None:
                    # Conexão: preço no 1º, 2º é trecho de conexão
                    opcao = {
                        'cia': v['cia'], 'voo': v['voo'],
                        'saida': v['saida'], 'chegada': v['chegada'],
                        'origem': v['origem'], 'destino': v['destino'],
                        'valor': v['valor'], 'bagagem': '1',
                        'cia_conexao': v_next['cia'], 'voo_conexao': v_next['voo'],
                        'saida_conexao': v_next['saida'], 'chegada_conexao': v_next['chegada'],
                        'origem_conexao': v_next['origem'], 'destino_conexao': v_next['destino'],
                    }
                    resultado[secao].append(opcao)
                    i += 2
                    continue
                elif v['valor'] is None and v_next['valor'] is not None:
                    # Conexão: preço no 2º, 1º é trecho principal
                    opcao = {
                        'cia': v['cia'], 'voo': v['voo'],
                        'saida': v['saida'], 'chegada': v['chegada'],
                        'origem': v['origem'], 'destino': v['destino'],
                        'valor': v_next['valor'], 'bagagem': '1',
                        'cia_conexao': v_next['cia'], 'voo_conexao': v_next['voo'],
                        'saida_conexao': v_next['saida'], 'chegada_conexao': v_next['chegada'],
                        'origem_conexao': v_next['origem'], 'destino_conexao': v_next['destino'],
                    }
                    resultado[secao].append(opcao)
                    i += 2
                    continue

            # Voo standalone (com preço)
            if v['valor'] is not None:
                opcao = {
                    'cia': v['cia'], 'voo': v['voo'],
                    'saida': v['saida'], 'chegada': v['chegada'],
                    'origem': v['origem'], 'destino': v['destino'],
                    'valor': v['valor'], 'bagagem': '1',
                    'cia_conexao': None, 'voo_conexao': None,
                    'saida_conexao': None, 'chegada_conexao': None,
                    'origem_conexao': None, 'destino_conexao': None,
                }
                resultado[secao].append(opcao)
            i += 1

    for line in lines:
        upper = line.upper().strip()

        # Detectar seção IDA/VOLTA
        if re.search(r'OP[CÇ][OÕ]ES\s+DE\s+VOOS?\s+IDA', upper) or re.search(r'VOOS?\s+IDA', upper):
            if voos_pendentes and secao_atual:
                _flush_secao(voos_pendentes, secao_atual, resultado)
                voos_pendentes = []
            secao_atual = 'ida'
            continue
        elif re.search(r'OP[CÇ][OÕ]ES\s+DE\s+VOOS?\s+VOLTA', upper) or re.search(r'VOOS?\s+VOLTA', upper):
            if voos_pendentes and secao_atual:
                _flush_secao(voos_pendentes, secao_atual, resultado)
                voos_pendentes = []
            secao_atual = 'volta'
            continue

        # Ignorar headers de tabela e seções de agrupamento
        if re.search(r'OP[CÇ][OÕ]ES\s+POR', upper):
            continue
        if re.search(r'\bCIA\b.*\bVOO\b.*\bSA[IÍ]DA\b', upper):
            continue

        # Tentar parsear como linha de voo
        if secao_atual and line.strip():
            parsed = _parse_linha_tabular_v2(line)
            if parsed:
                voos_pendentes.append(parsed)

    # Flush última seção
    if voos_pendentes and secao_atual:
        _flush_secao(voos_pendentes, secao_atual, resultado)


def _detectar_formato_tabular_v2(texto):
    """Detecta se o texto corresponde ao layout tabular v2 (com sub-seções OPÇÕES POR)."""
    upper = texto.upper()
    return bool(re.search(r'OP[CÇ][OÕ]ES\s+POR\s+', upper))


# ══════════════════════════════════════════════════════════════════════════════
# Parsers de fallback (line-based, do código original)
# ══════════════════════════════════════════════════════════════════════════════

def _find_cidades(texto):
    """Encontra cidades no texto OCR, ordenadas por posição."""
    found = []
    lower = texto.lower()
    for cl, cn in _CIDADES_LOWER.items():
        idx = lower.find(cl)
        if idx >= 0:
            found.append((idx, cn))
    found.sort(key=lambda x: x[0])
    return [c[1] for c in found]


def _parse_preco(texto):
    """Extrai valor R$ como float string ('2701.75') ou None."""
    m = _RE_PRECO.search(texto)
    if m:
        return m.group(1).replace('.', '').replace(',', '.')
    return None


def _parse_datas(texto):
    """Extrai datetimes do texto."""
    result = []
    for d, h, mi in _RE_DATA_HORA.findall(texto):
        try:
            result.append(datetime.strptime(f'{d} {h}:{mi}', '%d/%m/%Y %H:%M'))
        except ValueError:
            pass
    return result


def _parse_voo(texto):
    """Extrai código de voo ('LA 3195') ou None."""
    m = _RE_VOO.search(texto)
    if m:
        return f'{m.group(1).upper()} {m.group(2)}'
    return None


def _is_header(text):
    """Verifica se a linha é cabeçalho de seção ou tabela."""
    u = text.upper()
    if any(h in u for h in ['VOOS IDA', 'VOOS VOLTA', 'VOO IDA', 'VOO VOLTA']):
        return True
    if ('CIA' in u or 'VOO' in u) and ('TOTAL' in u or 'BAGAGEM' in u or 'SAIDA' in u):
        return True
    if 'DISPON' in u or 'COTAC' in u:
        return True
    return False


def _parse_linha(text):
    """Parseia uma linha de texto OCR. Retorna dict com dados extraídos ou None."""
    voo = _parse_voo(text)
    if not voo:
        return None
    return {
        'voo': voo,
        'datas': _parse_datas(text),
        'cidades': _find_cidades(text),
        'preco': _parse_preco(text),
        'raw': text,
    }


def _parse_formato_tabular(texto, resultado):
    """Parser para formato A (tabela imagem): OPÇÕES DE VOOS IDA / VOLTA."""
    linhas = texto.split('\n')
    secao_ida = []
    secao_volta = []
    secao_atual = None

    for linha in linhas:
        linha = linha.strip()
        if not linha:
            continue

        upper = linha.upper()
        if 'IDA' in upper and ('VOO' in upper or 'OP' in upper):
            secao_atual = 'ida'
            continue
        if 'VOLTA' in upper and ('VOO' in upper or 'OP' in upper):
            secao_atual = 'volta'
            continue

        if _is_header(linha):
            continue

        if secao_atual == 'ida':
            secao_ida.append(linha)
        elif secao_atual == 'volta':
            secao_volta.append(linha)

    resultado['ida'] = _processar_secao(secao_ida)
    resultado['volta'] = _processar_secao(secao_volta)
    return resultado


def _extrair_valor_linha(linha):
    """Extrai valor monetário de uma linha. Retorna string float ou None.

    Reconhece formatos:
      - R$ 1.803,84 / RS 1.803,84
      - Valor a partir de R$ 1.803,84
      - Valor a partir de 1.159,12 (sem R$)
      - de RS 1.256,38 ~Valor a partir (ordem invertida pelo OCR)
    """
    m = _RE_PRECO.search(linha)
    if m:
        return m.group(1).replace('.', '').replace(',', '.')
    m = _RE_PRECO_VALOR.search(linha)
    if m:
        val = m.group(1) or m.group(2)
        if val:
            return val.replace('.', '').replace(',', '.')
    return None


def _fix_ocr(text):
    """Corrige erros comuns de OCR em nomes de cidades."""
    for wrong, correct in _OCR_FIXES.items():
        if wrong in text.lower():
            text = re.sub(re.escape(wrong), correct, text, flags=re.IGNORECASE)
    return text


def _parse_rota_linha(linha):
    """Detecta linha de rota usando cidades conhecidas + data.

    Retorna (origem, destino, data_str) ou None.
    Lida com variações de OCR:
      - 'Teresina – São Paulo 15/03/2026'
      - 'Teresina São Paulo 15/03/2026'  (sem separador)
      - 'São Paulo Teresina 21/03/2026'  (cidades com espaço)
      - 'Teresina 21/03/2026 São Paulo'  (data no meio)
    """
    # Precisa ter uma data DD/MM/YYYY na linha
    m_data = re.search(r'(\d{2}/\d{2}/\d{4})', linha)
    if not m_data:
        return None

    data_str = m_data.group(1)
    # Separar texto antes e depois da data
    before = linha[:m_data.start()].strip().rstrip('-–').strip()
    after = linha[m_data.end():].strip().lstrip('-–').strip()

    # Não deve conter padrões de voo/opção/valor (filtrar linhas que não são rota)
    if _RE_OPCAO.match(linha) or _RE_VOO_HORA.match(linha):
        return None
    if re.search(r'dispon|valor|partir|cotiz', linha, re.IGNORECASE):
        return None

    # Caso 1: Data no final — "Cidade1 [–] Cidade2 DD/MM/YYYY"
    if before and not after:
        # Tentar separar por – ou -
        parts = re.split(r'\s*[-–]\s*', before, maxsplit=1)
        if len(parts) == 2:
            return (parts[0].strip(), parts[1].strip(), data_str)
        # Sem separador: usar cidades conhecidas para delimitar
        cidade1, cidade2 = _split_cidades(before)
        if cidade1 and cidade2:
            return (cidade1, cidade2, data_str)

    # Caso 2: Data no meio — "Cidade1 DD/MM/YYYY Cidade2"
    if before and after:
        return (before, after, data_str)

    return None


def _split_cidades(text):
    """Divide texto em duas cidades usando a lista _CIDADES.

    Tenta match da cidade mais longa no início, o resto é a segunda cidade.
    Ex: 'São Paulo Teresina' → ('São Paulo', 'Teresina')
    """
    text_lower = text.lower().strip()
    # Ordenar cidades por tamanho decrescente para match mais longo primeiro
    sorted_cidades = sorted(_CIDADES, key=len, reverse=True)

    for cidade in sorted_cidades:
        cidade_lower = cidade.lower()
        if text_lower.startswith(cidade_lower):
            rest = text[len(cidade):].strip().rstrip('-–').strip()
            if rest:
                # Verificar se o resto também é uma cidade conhecida
                rest_lower = rest.lower()
                for c2 in sorted_cidades:
                    if rest_lower == c2.lower() or rest_lower.startswith(c2.lower()):
                        return (cidade, rest.strip())
                # Mesmo se não for cidade conhecida, aceitar (OCR pode distorcer)
                return (cidade, rest.strip())
    return (None, None)


def _parse_formato_textual(texto, resultado):
    """Parser para formato B (texto corrido com 'Opção N: CIA')."""
    linhas = texto.split('\n')
    trechos = []
    rota_atual = None
    cia_atual = None
    cidade_base = None

    for linha in linhas:
        linha = _fix_ocr(linha.strip())
        if not linha:
            continue

        # Tentar detectar rota (Cidade1 – Cidade2 DD/MM/YYYY ou variações OCR)
        rota_parsed = _parse_rota_linha(linha)
        if rota_parsed:
            rota_atual = rota_parsed
            if cidade_base is None:
                cidade_base = rota_atual[0]
            cia_atual = None
            continue

        m_opcao = _RE_OPCAO.match(linha)
        if m_opcao:
            cia_atual = m_opcao.group(1).strip()
            continue

        m_voo = _RE_VOO_HORA.match(linha)
        if m_voo and rota_atual:
            num_voo = m_voo.group(1)
            origem = m_voo.group(2).strip()
            hora_saida = m_voo.group(3).replace('.', ':')
            destino = m_voo.group(4).strip()
            hora_chegada = m_voo.group(5).replace('.', ':')

            data_str = rota_atual[2]
            saida = None
            chegada = None
            try:
                saida = datetime.strptime(f'{data_str} {hora_saida}', '%d/%m/%Y %H:%M')
                chegada = datetime.strptime(f'{data_str} {hora_chegada}', '%d/%m/%Y %H:%M')
            except ValueError:
                pass

            trechos.append({
                'cia': cia_atual or _deduz_cia_texto(num_voo),
                'voo': _formata_voo_texto(num_voo, cia_atual),
                'saida': saida,
                'chegada': chegada,
                'origem': origem,
                'destino': destino,
                'valor': None,
                'rota_origem': rota_atual[0],
                'rota_destino': rota_atual[1],
            })
            continue

        valor_str = _extrair_valor_linha(linha)
        if valor_str and trechos and trechos[-1]['valor'] is None:
            trechos[-1]['valor'] = valor_str
            continue

    if not cidade_base:
        cidade_base = trechos[0]['origem'] if trechos else ''

    cidade_base_lower = cidade_base.lower()

    for trecho in trechos:
        rota_orig_lower = trecho.get('rota_origem', '').lower()
        rota_dest_lower = trecho.get('rota_destino', '').lower()
        origem_lower = trecho['origem'].lower()

        opcao = {
            'cia': trecho['cia'],
            'voo': trecho['voo'],
            'saida': trecho['saida'],
            'chegada': trecho['chegada'],
            'origem': trecho['origem'],
            'destino': trecho['destino'],
            'valor': trecho['valor'],
            'bagagem': '1',
            'cia_conexao': None, 'voo_conexao': None,
            'saida_conexao': None, 'chegada_conexao': None,
            'origem_conexao': None, 'destino_conexao': None,
        }

        # Classificar ida/volta pela ORIGEM DO VOO (mais confiável que rota OCR)
        if cidade_base_lower in origem_lower:
            resultado['ida'].append(opcao)
        else:
            resultado['volta'].append(opcao)

    return resultado


def _deduz_cia_texto(num_voo):
    """Tenta deduzir a CIA pelo range do número de voo."""
    try:
        n = int(num_voo)
        if 1000 <= n <= 9999:
            return 'LATAM'
    except ValueError:
        pass
    return ''


def _formata_voo_texto(num_voo, cia_nome):
    """Formata código de voo a partir do número e nome da CIA."""
    cia_lower = (cia_nome or '').lower().strip()
    sigla_map = {
        'latam': 'LA', 'gol': 'G3', 'azul': 'AD',
        'tap': 'TP', 'american': 'AA', 'delta': 'DL',
        'united': 'UA', 'avianca': 'AV',
    }
    sigla = sigla_map.get(cia_lower, '')
    if sigla:
        return f'{sigla} {num_voo}'
    return num_voo


def _processar_secao(linhas_texto):
    """Processa linhas de uma seção IDA/VOLTA (fallback line-based)."""
    voo_lines = []
    pending_voo = None

    for text in linhas_texto:
        parsed = _parse_linha(text)

        if parsed:
            if not parsed['datas'] and not parsed['preco'] and not parsed['cidades']:
                if pending_voo:
                    voo_lines.append(pending_voo)
                pending_voo = parsed
            else:
                voo_lines.append(parsed)
                pending_voo = None
        elif pending_voo:
            datas = _parse_datas(text)
            cidades = _find_cidades(text)
            preco = _parse_preco(text)
            if datas or preco or cidades:
                pending_voo['datas'] = datas
                pending_voo['cidades'] = cidades
                pending_voo['preco'] = preco
                voo_lines.append(pending_voo)
                pending_voo = None

    if pending_voo:
        voo_lines.append(pending_voo)

    if not voo_lines:
        return []

    opcoes = []
    i = 0
    while i < len(voo_lines):
        curr = voo_lines[i]
        nxt = voo_lines[i + 1] if i + 1 < len(voo_lines) else None

        if curr['preco'] and nxt and not nxt['preco']:
            opcoes.append(_montar_opcao_legacy(curr, nxt))
            i += 2
        elif not curr['preco'] and nxt and nxt['preco']:
            opcoes.append(_montar_opcao_legacy(curr, nxt))
            i += 2
        else:
            opcoes.append(_montar_opcao_legacy(curr, None))
            i += 1

    return opcoes


def _montar_opcao_legacy(trecho1, trecho2=None):
    """Monta dict de opção de voo (formato legacy line-based)."""
    preco = trecho1.get('preco') or (trecho2.get('preco') if trecho2 else None)

    opcao = {
        'cia': _cia_from_voo(trecho1['voo']),
        'voo': trecho1['voo'],
        'saida': trecho1['datas'][0] if trecho1['datas'] else None,
        'chegada': trecho1['datas'][1] if len(trecho1['datas']) >= 2 else None,
        'origem': trecho1['cidades'][0] if trecho1['cidades'] else '',
        'destino': trecho1['cidades'][1] if len(trecho1['cidades']) >= 2 else '',
        'valor': preco,
        'bagagem': '1',
        'cia_conexao': None, 'voo_conexao': None,
        'saida_conexao': None, 'chegada_conexao': None,
        'origem_conexao': None, 'destino_conexao': None,
    }

    if trecho2:
        opcao['cia_conexao'] = _cia_from_voo(trecho2['voo'])
        opcao['voo_conexao'] = trecho2['voo']
        opcao['saida_conexao'] = trecho2['datas'][0] if trecho2['datas'] else None
        opcao['chegada_conexao'] = trecho2['datas'][1] if len(trecho2['datas']) >= 2 else None
        opcao['origem_conexao'] = trecho2['cidades'][0] if trecho2['cidades'] else ''
        opcao['destino_conexao'] = trecho2['cidades'][1] if len(trecho2['cidades']) >= 2 else ''

    return opcao
