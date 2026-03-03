"""
Extensões Flask centralizadas.
Todas as extensões são inicializadas aqui e importadas pela factory.
"""
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_session import Session
from flask_migrate import Migrate

# Instâncias das extensões (serão inicializadas com app na factory)
db = SQLAlchemy()
login_manager = LoginManager()
session = Session()
migrate = Migrate()


def init_extensions(app):
    """
    Inicializa todas as extensões com a aplicação Flask.

    Args:
        app: Instância da aplicação Flask
    """
    db.init_app(app)
    login_manager.init_app(app)
    session.init_app(app)
    migrate.init_app(app, db)

    # Configuração do Login Manager
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Por favor, faça login para acessar esta página.'
    login_manager.login_message_category = 'warning'
