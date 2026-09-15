"""
Garante que nenhuma página depende de CDN externa.

A intranet pode bloquear CDNs; sem os arquivos locais as telas ficam sem layout,
ícones, máscaras, selects e gráficos. Todas as bibliotecas vivem em
app/static/vendor/ (ver app/static/vendor/README.md).
"""
import re
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parent.parent / 'app'
TEMPLATES = APP / 'templates'
STATIC = APP / 'static'
VENDOR = STATIC / 'vendor'

EXTERNO = r'(?:https?:)?//(?!localhost[:/])'

PADROES = {
    'script src': re.compile(r'<script\b[^>]*\bsrc\s*=\s*["\']' + EXTERNO, re.I),
    'link href': re.compile(r'<link\b[^>]*\bhref\s*=\s*["\']' + EXTERNO, re.I),
    '@import': re.compile(r'@import\s+(?:url\()?\s*["\']?' + EXTERNO, re.I),
    'url()': re.compile(r'url\(\s*["\']?' + EXTERNO, re.I),
    'language.url': re.compile(r'language\s*:\s*\{[^}]*url\s*:\s*["\']' + EXTERNO, re.I),
}


def _arquivos():
    for pasta, exts in ((TEMPLATES, ('.html',)), (STATIC / 'css', ('.css',)), (STATIC / 'js', ('.js',))):
        for arq in pasta.rglob('*'):
            if arq.suffix in exts and VENDOR not in arq.parents:
                yield arq


def _ocorrencias():
    for arq in _arquivos():
        texto = arq.read_text(encoding='utf-8', errors='ignore')
        for nome, padrao in PADROES.items():
            for m in padrao.finditer(texto):
                linha = texto.count('\n', 0, m.start()) + 1
                yield f'{arq.relative_to(APP)}:{linha} ({nome})'


def test_nenhum_recurso_carregado_de_fora():
    encontrados = list(_ocorrencias())
    assert not encontrados, 'Recursos externos encontrados:\n' + '\n'.join(encontrados)


def test_referencias_vendor_existem_no_disco():
    padrao = re.compile(r"url_for\(\s*['\"]static['\"]\s*,\s*filename\s*=\s*['\"](vendor/[^'\"]+)['\"]")
    faltando = []
    for arq in TEMPLATES.rglob('*.html'):
        for caminho in padrao.findall(arq.read_text(encoding='utf-8', errors='ignore')):
            if not (STATIC / caminho).is_file():
                faltando.append(f'{arq.relative_to(APP)} -> {caminho}')
    assert not faltando, 'Arquivos vendor ausentes:\n' + '\n'.join(faltando)


@pytest.mark.parametrize('css', [
    'vendor/bootstrap-icons/bootstrap-icons.css',
    'css/base/fonts.css',
])
def test_fontes_referenciadas_no_css_existem(css):
    arq = STATIC / css
    assert arq.is_file(), f'{css} não existe'
    urls = re.findall(r'url\(\s*["\']?([^"\')?#]+)', arq.read_text(encoding='utf-8'))
    assert urls, f'{css} não referencia nenhuma fonte'
    faltando = [u for u in urls if not (arq.parent / u).resolve().is_file()]
    assert not faltando, f'Fontes ausentes em {css}: {faltando}'


def test_classes_font_awesome_nao_sao_usadas():
    """O Font Awesome foi removido; fa-spin/fa-2x/fa-3x viraram icon-*."""
    padrao = re.compile(r'\bfa-(?:spin|2x|3x)\b')
    usados = [
        str(arq.relative_to(APP))
        for arq in _arquivos()
        if padrao.search(arq.read_text(encoding='utf-8', errors='ignore'))
    ]
    assert not usados, f'Classes do Font Awesome ainda usadas: {usados}'
