"""
Factory da aplicação Flask.
Configura e inicializa todas as extensões e blueprints.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from flask import Flask, redirect, url_for, render_template
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
