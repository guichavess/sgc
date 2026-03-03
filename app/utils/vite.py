"""
Integração Flask + Vite
Gerencia assets em desenvolvimento e produção

Modos:
  - Vite Dev Server (HMR): defina VITE_DEV=1 no .env e rode o Vite dev server
  - Build (produção/debug): rode `npx vite build` — assets servidos do manifest
"""
import json
import os
from flask import current_app, url_for


def _is_vite_dev():
    """Retorna True apenas se VITE_DEV=1 estiver definido no ambiente."""
    return os.getenv('VITE_DEV', '0') == '1'


def get_manifest():
    """
    Carrega o manifest do Vite (gerado no build).
    """
    manifest_path = os.path.join(
        current_app.static_folder,
        'dist',
        '.vite',
        'manifest.json'
    )

    if not os.path.exists(manifest_path):
        return None

    with open(manifest_path, 'r') as f:
        return json.load(f)


def _resolve_from_manifest(entry_name, field='file'):
    """
    Busca um entry no manifest e retorna a URL do campo solicitado.

    Args:
        entry_name: Nome do entry point
        field: 'file' para JS, 'css' para lista de CSS

    Returns:
        Para field='file': URL string ou None
        Para field='css': lista de URLs ou []
    """
    manifest = get_manifest()
    if not manifest:
        return None if field == 'file' else []

    possible_keys = [
        f"js/{entry_name}.js",
        f"{entry_name}.js",
        f"{entry_name}.tsx",
        entry_name,
    ]

    for key in possible_keys:
        if key in manifest:
            if field == 'css':
                css_files = manifest[key].get('css', [])
                return [url_for('static', filename=f'dist/{css}') for css in css_files]
            else:
                file_path = manifest[key].get('file', '')
                return url_for('static', filename=f'dist/{file_path}')

    return None if field == 'file' else []


def vite_asset(entry_name):
    """
    Retorna a URL do asset Vite.

    - Com VITE_DEV=1: aponta para o Vite dev server (HMR)
    - Sem VITE_DEV: usa o manifest do build (funciona em debug e produção)

    Args:
        entry_name: Nome do entry point (ex: 'pages/dashboard', 'dashboards/main')

    Returns:
        URL do asset
    """
    # Modo dev server (HMR) — apenas quando explicitamente habilitado
    if _is_vite_dev():
        vite_server = os.getenv('VITE_DEV_SERVER', 'http://localhost:5173')
        vite_base = os.getenv('VITE_BASE', '/static/dist/')
        if entry_name.startswith('dashboards/'):
            return f"{vite_server}{vite_base}{entry_name}.tsx"
        return f"{vite_server}{vite_base}js/{entry_name}.js"

    # Modo build — usa o manifest
    url = _resolve_from_manifest(entry_name, field='file')
    if url:
        return url

    # Fallback: caminho direto (pode não existir)
    return url_for('static', filename=f'dist/js/{entry_name}.js')


def vite_css(entry_name):
    """
    Retorna as URLs de CSS associadas a um entry point do Vite.

    Args:
        entry_name: Nome do entry point

    Returns:
        Lista de URLs de CSS ou lista vazia
    """
    # Em modo dev server, o CSS é injetado pelo Vite via HMR
    if _is_vite_dev():
        return []

    return _resolve_from_manifest(entry_name, field='css')


def register_vite_helpers(app):
    """
    Registra os helpers do Vite como funções globais de template.

    Uso:
        register_vite_helpers(app)

    Depois no template:
        {{ vite_asset('pages/dashboard') }}
    """
    @app.context_processor
    def vite_context():
        return {
            'vite_asset': vite_asset,
            'vite_css': vite_css,
            'vite_dev_mode': _is_vite_dev(),
        }
