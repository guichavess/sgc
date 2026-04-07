"""
Parser para documentos de Escolha de Passagens (IdSerie 2977 e 543).

Extrai do PDF ou HTML: opcoes escolhidas, menor valor, justificativas e declaracao.
A API do SEI retorna HTML para documentos internos e PDF para documentos externos.
"""
import io
import re
import html as html_mod
import unicodedata


def _normalizar(texto):
    """Remove acentos e normaliza whitespace."""
    nfkd = unicodedata.normalize('NFKD', texto)
    sem_acento = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r'\s+', ' ', sem_acento).strip()


def _extrair_texto(doc_bytes):
    """
    Extrai texto do documento. Detecta se e HTML ou PDF e trata adequadamente.
    A API do SEI retorna HTML para docs internos (gerados no SEI) e PDF para externos.
    """
    # Detectar se e HTML (comeca com <!DOCTYPE ou <html)
    header = doc_bytes[:100].lstrip()
    if header.startswith(b'<!DOCTYPE') or header.startswith(b'<html') or header.startswith(b'<HTML'):
        return _extrair_texto_html(doc_bytes)
    else:
        return _extrair_texto_pdf(doc_bytes)


def _extrair_texto_html(html_bytes):
    """Extrai texto limpo de HTML do SEI."""
    # Detectar charset do meta tag
    charset_match = re.search(rb'charset[= ]+["\']?([^"\' ;>]+)', html_bytes[:1000])
    detected = charset_match.group(1).decode('ascii').strip() if charset_match else None

    # Tentar encoding detectado primeiro, depois fallbacks
    encodings = []
    if detected:
        encodings.append(detected)
    encodings.extend(['iso-8859-1', 'cp1252', 'utf-8'])

    for enc in encodings:
        try:
            text = html_bytes.decode(enc)
            break
        except (UnicodeDecodeError, ValueError, LookupError):
            continue
    else:
        text = html_bytes.decode('utf-8', errors='replace')

    # Decodificar entidades HTML
    text = html_mod.unescape(text)
    # Remover tags HTML preservando espacos
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    # Normalizar whitespace (manter newlines)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n', text)
    return text.strip()


def _extrair_texto_pdf(pdf_bytes):
    """Extrai texto concatenado de todas as paginas do PDF via PyPDF2."""
    import PyPDF2
    reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
    partes = []
    for page in reader.pages:
        texto = page.extract_text()
        if texto:
            partes.append(texto)
    return '\n'.join(partes)


# ── Regex patterns ──────────────────────────────────────────────────────────

# Cada opcao no documento: ( X ) OPÇÃO N (ID 0021850201) ou ( ) OPÇÃO N ()
# Captura: grupo 1 = conteudo entre parens do checkbox, grupo 2 = numero da opcao,
#          grupo 3 = conteudo entre parens do ID
_RE_OPCAO = re.compile(
    r'\(\s*([^)]*?)\s*\)\s*OP[CÇ][AÃ]O\s+(\d+)\s*\(([^)]*)\)',
    re.IGNORECASE
)

# Menor valor: (X) SIM ... (X) NÃO
_RE_MENOR_VALOR = re.compile(
    r'MENOR\s+VALOR\??\s*'
    r'\(\s*([^)]*?)\s*\)\s*SIM'
    r'\s*\(\s*([^)]*?)\s*\)\s*N[AÃ]O',
    re.IGNORECASE
)

# Declaracao de responsabilidade
# PyPDF2 pode quebrar palavras em bold: "D eclaro" ou "Declaro"
_RE_DECLARACAO = re.compile(
    r'\(\s*([^)]*?)\s*\)\s*D\s*eclaro\s+ter\s+ci[eê]ncia',
    re.IGNORECASE
)

# Justificativas J1-J5: checkbox seguido de keyword
_JUSTIFICATIVAS = [
    ('J1', re.compile(
        r'\(\s*([^)]*?)\s*\)\s*[^\n]*?valor\s+de\s+di[aá]rias\s+previstas',
        re.IGNORECASE
    )),
    ('J2', re.compile(
        r'\(\s*([^)]*?)\s*\)\s*[^\n]*?recomenda[cç][aã]o\s+m[eé]dica',
        re.IGNORECASE
    )),
    ('J3', re.compile(
        r'\(\s*([^)]*?)\s*\)\s*[^\n]*?disposi[cç][oõ]es\s+[^\n]*?art[\.\s]*6',
        re.IGNORECASE
    )),
    ('J4', re.compile(
        r'\(\s*([^)]*?)\s*\)\s*[^\n]*?autoriza[cç][aã]o\s+do\s+Governador',
        re.IGNORECASE
    )),
    ('J5', re.compile(
        r'\(\s*([^)]*?)\s*\)\s*Outros\s*:',
        re.IGNORECASE
    )),
]


def _tem_marca(texto_checkbox):
    """Verifica se o conteudo do checkbox indica marcado (x, X)."""
    t = texto_checkbox.strip().upper()
    return t in ('X', 'x') or bool(re.match(r'^[xX]$', t))


def _extrair_texto_outros(texto_completo):
    """Extrai texto livre apos 'Outros:' se marcado com (x)."""
    # Procura padrao: (x) Outros: <texto livre>
    # Termina antes da declaracao, assinatura ou rodape
    m = re.search(
        r'\(\s*[xX]\s*\)\s*Outros\s*:\s*(.*?)(?='
        r'\(\s*[xX ]*\s*\)\s*D\s*eclaro'  # declaracao (com possivel espaco em "D eclaro")
        r'|\(assinado'                       # bloco de assinatura
        r'|Documento\s+assinado'             # rodape de assinatura
        r'|SEAD_'                            # rodape do documento SEI
        r'|\Z)',
        texto_completo,
        re.IGNORECASE | re.DOTALL
    )
    if m:
        texto = m.group(1).strip()
        texto = re.sub(r'\s+', ' ', texto).strip()
        # Truncar em 500 chars para evitar capturar lixo
        if len(texto) > 500:
            texto = texto[:500].rsplit(' ', 1)[0] + '...'
        if texto:
            return texto
    return ''


def extrair_escolha_passagens(pdf_bytes):
    """
    Extrai dados de escolha de passagens de um PDF do SEI.

    Suporta ambos formatos:
    - IdSerie 2977 (SEAD_ESCOLHA_PASSAGENS): opcoes sem IDs
    - IdSerie 543 (SEAD_REQUISICAO_INTERNA): opcoes com IDs de documentos SEI

    Args:
        pdf_bytes: conteudo binario do PDF

    Returns:
        dict com:
            opcoes_escolhidas: list[int] — numeros das opcoes marcadas
            ids_documentos: dict[str, str] — {numero_opcao: id_documento} (vazio se sem IDs)
            menor_valor: bool|None — True=SIM, False=NAO, None=nao encontrado
            justificativa_codigos: list[str] — codigos J1-J5 marcados
            justificativa_outros_texto: str — texto livre de "Outros"
            declaracao: bool — declaracao de responsabilidade marcada
            erros: list[str] — avisos de parsing
    """
    resultado = {
        'opcoes_escolhidas': [],
        'ids_documentos': {},
        'menor_valor': None,
        'justificativa_codigos': [],
        'justificativa_outros_texto': '',
        'declaracao': False,
        'erros': [],
    }

    # 1. Extrair texto do PDF
    try:
        texto = _extrair_texto(pdf_bytes)
    except Exception as e:
        resultado['erros'].append(f'Falha ao extrair texto do PDF: {str(e)[:100]}')
        return resultado

    if not texto or len(texto.strip()) < 50:
        resultado['erros'].append('PDF sem texto extraivel ou muito curto.')
        return resultado

    # Normalizar texto para matching (manter original para "Outros" text)
    texto_norm = _normalizar(texto)

    # 2. Extrair opcoes escolhidas
    for match in _RE_OPCAO.finditer(texto):
        checkbox = match.group(1)
        num_opcao = int(match.group(2))
        id_doc = match.group(3).strip()

        if _tem_marca(checkbox):
            resultado['opcoes_escolhidas'].append(num_opcao)

        # Extrair ID do documento se presente (formato 543)
        # Limpar prefixo "ID" e espacos
        id_limpo = re.sub(r'^ID\s*', '', id_doc, flags=re.IGNORECASE).strip()
        if id_limpo and id_limpo not in ('', 'ID'):
            resultado['ids_documentos'][str(num_opcao)] = id_limpo

    if not resultado['opcoes_escolhidas']:
        # Fallback: tentar no texto normalizado (sem acentos)
        for match in _RE_OPCAO.finditer(texto_norm):
            checkbox = match.group(1)
            num_opcao = int(match.group(2))
            if _tem_marca(checkbox):
                resultado['opcoes_escolhidas'].append(num_opcao)

    if not resultado['opcoes_escolhidas']:
        resultado['erros'].append('Nenhuma opcao de voo marcada encontrada no documento.')

    # 3. Menor valor
    m = _RE_MENOR_VALOR.search(texto)
    if not m:
        m = _RE_MENOR_VALOR.search(texto_norm)

    if m:
        sim_marcado = _tem_marca(m.group(1))
        nao_marcado = _tem_marca(m.group(2))
        if sim_marcado and not nao_marcado:
            resultado['menor_valor'] = True
        elif nao_marcado and not sim_marcado:
            resultado['menor_valor'] = False
        elif sim_marcado and nao_marcado:
            resultado['menor_valor'] = True  # ambiguidade, assume SIM
            resultado['erros'].append('Ambos SIM e NAO marcados para menor valor.')
        else:
            resultado['menor_valor'] = None
            resultado['erros'].append('Menor valor: nenhuma opcao marcada.')
    else:
        resultado['erros'].append('Campo "menor valor" nao encontrado no documento.')

    # 4. Justificativas J1-J5
    for codigo, regex in _JUSTIFICATIVAS:
        m = regex.search(texto)
        if not m:
            m = regex.search(texto_norm)
        if m and _tem_marca(m.group(1)):
            resultado['justificativa_codigos'].append(codigo)

    # 5. Texto livre de "Outros" (J5)
    if 'J5' in resultado['justificativa_codigos']:
        resultado['justificativa_outros_texto'] = _extrair_texto_outros(texto)

    # 6. Declaracao
    m = _RE_DECLARACAO.search(texto)
    if not m:
        m = _RE_DECLARACAO.search(texto_norm)
    if m and _tem_marca(m.group(1)):
        resultado['declaracao'] = True

    return resultado
