"""
Factory da aplicação Flask.
Configura e inicializa todas as extensões e blueprints.
"""
import os
import sys
import logging
import subprocess
import threading
from logging.handlers import RotatingFileHandler
from flask import Flask, redirect, url_for, render_template, jsonify
from flask_login import current_user, login_required

from app.config import get_config
from app.extensions import db, login_manager, session, migrate, init_extensions
from app.utils.vite import register_vite_helpers


def create_app(config_class=None):
    """
    Factory para criar a aplicação Flask.

    Args:
        config_class: Classe de configuração opcional. Se não fornecida,
                      usa a configuração baseada em FLASK_ENV.

    Returns:
        Aplicação Flask configurada.
    """
    app = Flask(__name__)

    # Carrega configurações
    if config_class is None:
        config_class = get_config()
    app.config.from_object(config_class)

    # Inicializa extensões
    init_extensions(app)

    # Registra blueprints
    _register_blueprints(app)

    # Registra helpers do Vite para templates
    register_vite_helpers(app)

    # Rota raiz
    @app.route('/')
    def index():
        if current_user.is_authenticated:
            return redirect(url_for('hub'))
        return redirect(url_for('auth.login'))

    # Hub de seleção de módulos
    @app.route('/hub')
    @login_required
    def hub():
        return render_template('hub.html')

    # Rota para atualizar SIAFE (apenas Pedro Alexandre)
    # Grupos: SIAFE (execução sequencial interna, paralela entre grupos) e LOA (separado)
    SIAFE_SCRIPTS = [
        {'id': 'reserva',     'arquivo': 'atualizar_reserva.py',     'nome': 'Reservas',     'grupo': 'siafe', 'args': []},
        {'id': 'empenho',     'arquivo': 'atualizar_empenho.py',     'nome': 'Empenhos',     'grupo': 'siafe', 'args': []},
        {'id': 'liquidacao',  'arquivo': 'atualizar_liquidacao.py',  'nome': 'Liquidações',  'grupo': 'siafe', 'args': []},
        {'id': 'pd',          'arquivo': 'atualizar_pd.py',          'nome': 'PDs',           'grupo': 'siafe', 'args': []},
        {'id': 'ob',          'arquivo': 'atualizar_ob.py',          'nome': 'OBs',           'grupo': 'siafe', 'args': []},
        {'id': 'contratos',   'arquivo': 'atualizar_contratos.py',   'nome': 'Contratos',     'grupo': 'siafe', 'args': []},
        {'id': 'loa',         'arquivo': 'atualizar_loa.py',         'nome': 'LOA',           'grupo': 'loa',   'args': ['--years', '2026']},
        {'id': 'sei',         'arquivo': 'atualizar_etapas_sei.py',  'nome': 'Etapas SEI',    'grupo': 'sei',   'args': []},
    ]
    SIAFE_SCRIPTS_MAP = {s['id']: s for s in SIAFE_SCRIPTS}
    _siafe_status = {'running': False, 'scripts': []}

    def _executar_script(scripts_dir, script_info, idx):
        """Executa um único script e atualiza o status."""
        _siafe_status['scripts'][idx]['status'] = 'running'
        script_path = os.path.join(scripts_dir, script_info['arquivo'])
        cmd = [sys.executable, script_path] + script_info.get('args', [])
        try:
            env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600,
                cwd=scripts_dir, env=env
            )
            ok = result.returncode == 0
            _siafe_status['scripts'][idx]['status'] = 'ok' if ok else 'erro'
            if not ok and result.stderr:
                _siafe_status['scripts'][idx]['erro'] = result.stderr[-300:]
        except subprocess.TimeoutExpired:
            _siafe_status['scripts'][idx]['status'] = 'erro'
            _siafe_status['scripts'][idx]['erro'] = 'Timeout (10min)'
        except Exception as e:
            _siafe_status['scripts'][idx]['status'] = 'erro'
            _siafe_status['scripts'][idx]['erro'] = str(e)

    def _executar_scripts_siafe(selected_scripts):
        """Executa scripts em paralelo por grupo independente."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        scripts_dir = os.path.join(app.root_path, '..', 'scripts')

        # Agrupa scripts por grupo mantendo a ordem e o índice original
        grupos = {}
        for i, s in enumerate(selected_scripts):
            g = s.get('grupo', 'siafe')
            grupos.setdefault(g, []).append((i, s))

        def run_group(items):
            """Executa scripts de um grupo sequencialmente."""
            for idx, script_info in items:
                _executar_script(scripts_dir, script_info, idx)

        # Executa grupos em paralelo (siafe e loa rodam ao mesmo tempo)
        with ThreadPoolExecutor(max_workers=len(grupos)) as pool:
            futures = [pool.submit(run_group, items) for items in grupos.values()]
            for f in as_completed(futures):
                f.result()  # propaga exceções

        _siafe_status['running'] = False

    @app.route('/api/atualizar-siafe', methods=['POST'])
    @login_required
    def atualizar_siafe():
        from flask import request as req
        if not current_user.nome or 'PEDRO ALEXANDRE' not in current_user.nome.upper():
            return jsonify({'erro': 'Acesso negado'}), 403
        if _siafe_status['running']:
            return jsonify({'erro': 'Atualização já em andamento'}), 409

        data = req.get_json(silent=True) or {}
        selected_ids = data.get('scripts')
        if selected_ids:
            selected_scripts = [SIAFE_SCRIPTS_MAP[sid] for sid in selected_ids if sid in SIAFE_SCRIPTS_MAP]
        else:
            selected_scripts = [s for s in SIAFE_SCRIPTS if s['grupo'] == 'siafe']

        if not selected_scripts:
            return jsonify({'erro': 'Nenhum script selecionado'}), 400

        _siafe_status['running'] = True
        _siafe_status['scripts'] = [
            {'nome': s['nome'], 'status': 'pendente', 'erro': None}
            for s in selected_scripts
        ]
        thread = threading.Thread(
            target=_executar_scripts_siafe, args=(selected_scripts,), daemon=True
        )
        thread.start()
        return jsonify({'ok': True})

    @app.route('/api/atualizar-siafe/status')
    @login_required
    def atualizar_siafe_status():
        if not current_user.nome or 'PEDRO ALEXANDRE' not in current_user.nome.upper():
            return jsonify({'erro': 'Acesso negado'}), 403
        return jsonify({
            'running': _siafe_status['running'],
            'scripts': _siafe_status['scripts']
        })

    # Migração inline: adicionar cod_natureza e cod_subitem ao planejamento_orcamentario
    with app.app_context():
        for col_sql in [
            "ALTER TABLE planejamento_orcamentario ADD COLUMN cod_natureza VARCHAR(10) NULL",
            "ALTER TABLE planejamento_orcamentario ADD COLUMN cod_subitem VARCHAR(20) NULL",
        ]:
            try:
                db.session.execute(db.text(col_sql))
                db.session.commit()
            except Exception:
                db.session.rollback()  # coluna já existe

    # Registra filtros Jinja2
    from app.constants import normalizar_competencia
    app.jinja_env.filters['normalizar_comp'] = normalizar_competencia

    # Context processor para notificacoes (disponivel em todos os templates)
    _register_context_processors(app)

    # Configura logging
    _setup_logging(app)

    # Inicializa scheduler de notificacoes periodicas
    from app.services.scheduler import init_scheduler
    init_scheduler(app)

    return app


def _register_blueprints(app):
    """Registra todos os blueprints da aplicação."""
    from app.auth.routes import auth_bp
    from app.solicitacoes.routes import solicitacoes_bp
    from app.prestacoes_contratos.routes import prestacoes_contratos_bp
    from app.financeiro.routes import financeiro_bp
    from app.usuarios.routes import usuarios_bp
    from app.dashboards.routes import dashboards_bp
    from app.diarias.routes import diarias_bp

    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(solicitacoes_bp, url_prefix='/solicitacoes')
    app.register_blueprint(prestacoes_contratos_bp, url_prefix='/prestacoes-contratos')
    app.register_blueprint(financeiro_bp, url_prefix='/financeiro')
    app.register_blueprint(usuarios_bp, url_prefix='/usuarios')
    app.register_blueprint(dashboards_bp, url_prefix='/dashboards')
    app.register_blueprint(diarias_bp, url_prefix='/diarias')

    from app.notificacoes import notificacoes_bp
    app.register_blueprint(notificacoes_bp, url_prefix='/notificacoes')


def _register_context_processors(app):
    """Registra context processors globais."""

    @app.context_processor
    def inject_notificacao_data():
        """Injeta dados de notificacao em todos os templates."""
        if current_user.is_authenticated:
            from app.repositories.notificacao_repository import NotificacaoRepository
            try:
                count = NotificacaoRepository.contar_nao_lidas(current_user.id)
                criticas = NotificacaoRepository.buscar_criticas_pendentes(current_user.id)
                precisa_contato = not current_user.contato_preenchido
            except Exception:
                count = 0
                criticas = []
                precisa_contato = False

            return {
                'notificacao_count': count,
                'notificacoes_criticas': criticas,
                'precisa_preencher_contato': precisa_contato,
            }
        return {
            'notificacao_count': 0,
            'notificacoes_criticas': [],
            'precisa_preencher_contato': False,
        }


def _setup_logging(app):
    """Configura o sistema de logging."""
    if app.debug:
        return  # Em desenvolvimento, usa o logger padrão

    log_dir = app.config.get('LOG_DIR', 'logs')
    log_file = app.config.get('LOG_FILE', 'sistema_pagamentos.log')
    max_bytes = app.config.get('LOG_MAX_BYTES', 10240)
    backup_count = app.config.get('LOG_BACKUP_COUNT', 10)

    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, log_file),
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.info('Sistema de Pagamentos iniciado')
