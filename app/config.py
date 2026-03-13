"""
Configurações centralizadas da aplicação.
Carrega todas as configurações de variáveis de ambiente.
"""
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()


class Config:
    """Configuração base da aplicação."""

    # ==========================================================================
    # FLASK
    # ==========================================================================
    SECRET_KEY = os.getenv('SECRET_KEY', 'chave-secreta-desenvolvimento')
    SESSION_TYPE = 'filesystem'

    # ==========================================================================
    # BANCO DE DADOS
    # ==========================================================================
    DB_USER = os.getenv('DB_USER', 'root')
    DB_PASS = os.getenv('DB_PASS', 'root')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_NAME = os.getenv('DB_NAME', 'sgc')

    SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # ==========================================================================
    # SEI (Sistema Eletrônico de Informações)
    # ==========================================================================
    SEI_API_URL = os.getenv('SEI_API_URL', 'https://api.sei.pi.gov.br')
    SEI_SISTEMA = os.getenv('SEI_SISTEMA', '')
    SEI_UNIDADE = os.getenv('SEI_UNIDADE', '')
    SEI_USUARIO_ADMIN = os.getenv('SEI_USUARIO_ADMIN', '')
    SEI_SENHA_ADMIN = os.getenv('SEI_SENHA_ADMIN', '')

    # ==========================================================================
    # SIAFE (Sistema de Administração Financeira)
    # ==========================================================================
    SIAFE_URL = os.getenv('SIAFE_URL', '')
    SIAFE_USUARIO = os.getenv('SIAFE_USUARIO', '')
    SIAFE_SENHA = os.getenv('SIAFE_SENHA', '')

    # ==========================================================================
    # TELEGRAM (Notificações via Bot API)
    # ==========================================================================
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    # Legado (Telethon) - mantido para compatibilidade
    TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID', '')
    TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH', '')
    TELEGRAM_SESSION_PATH = os.getenv('TELEGRAM_SESSION_PATH', 'api_telegram/minha_sessao')

    # ==========================================================================
    # EMAIL
    # ==========================================================================
    MAIL_SERVER = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
    MAIL_PORT = int(os.getenv('MAIL_PORT', '587'))
    MAIL_USE_TLS = os.getenv('MAIL_USE_TLS', 'True').lower() == 'true'
    MAIL_USERNAME = os.getenv('MAIL_USERNAME', '')
    MAIL_PASSWORD = os.getenv('MAIL_PASSWORD', '')

    # ==========================================================================
    # SGA (Gestor SEAD - Consulta de Servidores)
    # ==========================================================================
    SGA_API_URL = os.getenv('SGA_API_URL', 'https://gestor.sead.pi.gov.br/api/pessoaSGA')
    SGA_API_HASHKEY = os.getenv('SGA_API_HASHKEY', '')

    # ==========================================================================
    # TRINO DATA LAKE (CGFR - leitura do Data Lake SEI)
    # ==========================================================================
    TRINO_HOST = os.getenv('TRINO_HOST', '10.0.122.75')
    TRINO_PORT = int(os.getenv('TRINO_PORT', '8443'))
    TRINO_USER = os.getenv('TRINO_USER', '')
    TRINO_PASSWORD = os.getenv('TRINO_PASSWORD', '')
    TRINO_CATALOG = os.getenv('TRINO_CATALOG', 'iceberg')
    TRINO_SCHEMA = os.getenv('TRINO_SCHEMA', 'sei')

    # ==========================================================================
    # LOGGING
    # ==========================================================================
    LOG_DIR = os.getenv('LOG_DIR', 'logs')
    LOG_FILE = os.getenv('LOG_FILE', 'sistema_pagamentos.log')
    LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', '10240'))
    LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '10'))


class DevelopmentConfig(Config):
    """Configuração para desenvolvimento."""
    DEBUG = True
    FLASK_ENV = 'development'


class ProductionConfig(Config):
    """Configuração para produção."""
    DEBUG = False
    FLASK_ENV = 'production'


class TestingConfig(Config):
    """Configuração para testes."""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'


# Mapeamento de configurações por ambiente
config_by_name = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config():
    """Retorna a configuração baseada na variável de ambiente FLASK_ENV."""
    env = os.getenv('FLASK_ENV', 'development')
    return config_by_name.get(env, DevelopmentConfig)
