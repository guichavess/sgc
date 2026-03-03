"""
Script para criar tabelas do sistema de notificacoes e popular dados iniciais.

Uso:
    python scripts/criar_tabelas_notificacoes.py
    python scripts/criar_tabelas_notificacoes.py --seed
"""
import sys
import os
import argparse

# Adiciona o diretorio raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db


# Seed de tipos de notificacao
TIPOS_NOTIFICACAO_SEED = [
    # ===== MODULO SOLICITACOES (Pagamentos) =====
    {
        'codigo': 'solicitacao.criada',
        'modulo': 'solicitacoes',
        'nome': 'Solicitacao de Pagamento Criada',
        'descricao': 'Notifica quando uma nova solicitacao de pagamento e criada',
        'nivel': 'silenciosa',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,
        'periodicidade': None,
    },
    {
        'codigo': 'solicitacao.etapa_avancou',
        'modulo': 'solicitacoes',
        'nome': 'Etapa de Pagamento Avancou',
        'descricao': 'Notifica quando uma solicitacao muda de etapa na timeline',
        'nivel': 'lembrete',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': None,
    },
    {
        'codigo': 'solicitacao.paga',
        'modulo': 'solicitacoes',
        'nome': 'Solicitacao Paga (OB Emitida)',
        'descricao': 'Notifica quando uma solicitacao atinge o status PAGO',
        'nivel': 'lembrete',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': None,
    },
    # ===== MODULO FINANCEIRO =====
    {
        'codigo': 'financeiro.nova_solicitacao',
        'modulo': 'financeiro',
        'nome': 'Nova Solicitacao Recebida (Financeiro)',
        'descricao': 'Notifica o setor financeiro sobre nova solicitacao de pagamento',
        'nivel': 'lembrete',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': None,
    },
    {
        'codigo': 'financeiro.ne_pendente',
        'modulo': 'financeiro',
        'nome': 'Nota de Empenho Pendente',
        'descricao': 'Lembrete sobre NEs ainda nao inseridas',
        'nivel': 'alerta',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': 'diaria',
    },
    {
        'codigo': 'financeiro.etapa_avancou',
        'modulo': 'financeiro',
        'nome': 'Etapa de Pagamento Avancou (Financeiro)',
        'descricao': 'Notifica equipe financeira sobre avancos de etapa em solicitacoes',
        'nivel': 'lembrete',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,
        'periodicidade': None,
    },
    # ===== CONTRATOS (Vigencia) =====
    {
        'codigo': 'contrato.vigencia_expirando',
        'modulo': 'prestacoes_contratos',
        'nome': 'Vigencia de Contrato Expirando',
        'descricao': (
            'Notifica sobre contratos proximos do vencimento '
            '(a cada 10 dias, comecando 90 dias antes). '
            'Nivel escalado dinamicamente: 90-61d=lembrete, 60-31d=alerta, 30-0d=critica'
        ),
        'nivel': 'lembrete',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': '10_dias',
    },
    {
        'codigo': 'contrato.vigencia_expirada',
        'modulo': 'prestacoes_contratos',
        'nome': 'Vigencia de Contrato Expirada',
        'descricao': 'Notifica que a vigencia de um contrato ja venceu',
        'nivel': 'critica',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': None,
    },
    # ===== DIARIAS =====
    {
        'codigo': 'diarias.nova_solicitacao',
        'modulo': 'diarias',
        'nome': 'Nova Solicitacao de Diaria',
        'descricao': 'Notifica sobre nova solicitacao de diaria pendente de aprovacao',
        'nivel': 'lembrete',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': None,
    },
    {
        'codigo': 'diarias.aprovada',
        'modulo': 'diarias',
        'nome': 'Diaria Aprovada',
        'descricao': 'Notifica que uma diaria foi aprovada',
        'nivel': 'lembrete',
        'canal_in_app': True, 'canal_email': False, 'canal_telegram': False,  # email/telegram em standby
        'periodicidade': None,
    },
]


def criar_tabelas():
    """Cria as tabelas do sistema de notificacoes via DDL raw.

    Usa SQL direto para garantir compatibilidade com sis_usuarios.id
    que e BIGINT UNSIGNED no MySQL (SQLAlchemy gera BIGINT signed).
    """
    inspector = db.inspect(db.engine)
    tabelas_existentes = inspector.get_table_names()

    DDL_TABELAS = {
        'notificacao_tipos': """
            CREATE TABLE notificacao_tipos (
                id INT NOT NULL AUTO_INCREMENT,
                codigo VARCHAR(50) NOT NULL,
                modulo VARCHAR(50) NOT NULL,
                nome VARCHAR(150) NOT NULL,
                descricao TEXT NULL,
                nivel ENUM('silenciosa','lembrete','alerta','critica') NOT NULL DEFAULT 'lembrete',
                canal_in_app TINYINT(1) NOT NULL DEFAULT 1,
                canal_email TINYINT(1) NOT NULL DEFAULT 0,
                canal_telegram TINYINT(1) NOT NULL DEFAULT 0,
                periodicidade VARCHAR(30) NULL,
                ativo TINYINT(1) NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uq_tipo_codigo (codigo)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        'notificacoes': """
            CREATE TABLE notificacoes (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                tipo_id INT NOT NULL,
                usuario_id BIGINT UNSIGNED NOT NULL,
                titulo VARCHAR(255) NOT NULL,
                mensagem TEXT NOT NULL,
                nivel ENUM('silenciosa','lembrete','alerta','critica') NOT NULL,
                ref_modulo VARCHAR(50) NULL,
                ref_id VARCHAR(50) NULL,
                ref_url VARCHAR(500) NULL,
                lida TINYINT(1) NOT NULL DEFAULT 0,
                lida_em DATETIME NULL,
                descartada TINYINT(1) NOT NULL DEFAULT 0,
                descartada_em DATETIME NULL,
                enviada_email TINYINT(1) NOT NULL DEFAULT 0,
                enviada_telegram TINYINT(1) NOT NULL DEFAULT 0,
                erro_envio TEXT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_notificacoes_usuario_lida (usuario_id, lida, created_at),
                KEY idx_notificacoes_ref (ref_modulo, ref_id),
                CONSTRAINT notificacoes_ibfk_1 FOREIGN KEY (tipo_id) REFERENCES notificacao_tipos (id),
                CONSTRAINT notificacoes_ibfk_2 FOREIGN KEY (usuario_id) REFERENCES sis_usuarios (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        'notificacao_critica_confirmacoes': """
            CREATE TABLE notificacao_critica_confirmacoes (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                notificacao_id BIGINT UNSIGNED NOT NULL,
                usuario_id BIGINT UNSIGNED NOT NULL,
                cpf_informado VARCHAR(11) NOT NULL,
                confirmada_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uq_critica_usuario (notificacao_id, usuario_id),
                CONSTRAINT critica_conf_ibfk_1 FOREIGN KEY (notificacao_id) REFERENCES notificacoes (id),
                CONSTRAINT critica_conf_ibfk_2 FOREIGN KEY (usuario_id) REFERENCES sis_usuarios (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        'notificacao_preferencias': """
            CREATE TABLE notificacao_preferencias (
                id INT NOT NULL AUTO_INCREMENT,
                usuario_id BIGINT UNSIGNED NOT NULL,
                tipo_id INT NOT NULL,
                canal_in_app TINYINT(1) NOT NULL DEFAULT 1,
                canal_email TINYINT(1) NOT NULL DEFAULT 1,
                canal_telegram TINYINT(1) NOT NULL DEFAULT 1,
                silenciado TINYINT(1) NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE KEY uq_pref_usuario_tipo (usuario_id, tipo_id),
                CONSTRAINT pref_ibfk_1 FOREIGN KEY (usuario_id) REFERENCES sis_usuarios (id),
                CONSTRAINT pref_ibfk_2 FOREIGN KEY (tipo_id) REFERENCES notificacao_tipos (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    }

    ordem = [
        'notificacao_tipos',
        'notificacoes',
        'notificacao_critica_confirmacoes',
        'notificacao_preferencias',
    ]

    with db.engine.begin() as conn:
        for nome in ordem:
            if nome not in tabelas_existentes:
                conn.execute(db.text(DDL_TABELAS[nome]))
                print(f'[OK] Tabela "{nome}" criada.')
            else:
                print(f'[--] Tabela "{nome}" ja existe.')


def adicionar_colunas_sis_usuarios():
    """Adiciona colunas de contato e notificacao em sis_usuarios."""
    inspector = db.inspect(db.engine)
    colunas = [col['name'] for col in inspector.get_columns('sis_usuarios')]

    novas_colunas = [
        ('email', 'VARCHAR(255) NULL'),
        ('telefone', 'VARCHAR(20) NULL'),
        ('telegram_chat_id', 'VARCHAR(50) NULL'),
        ('cpf', 'VARCHAR(11) NULL'),
        ('contato_preenchido', 'TINYINT(1) NOT NULL DEFAULT 0'),
        ('notificacoes_email', 'TINYINT(1) NOT NULL DEFAULT 1'),
        ('notificacoes_telegram', 'TINYINT(1) NOT NULL DEFAULT 1'),
    ]

    with db.engine.begin() as conn:
        for col_name, col_def in novas_colunas:
            if col_name not in colunas:
                conn.execute(db.text(
                    f'ALTER TABLE sis_usuarios ADD COLUMN {col_name} {col_def}'
                ))
                print(f'[OK] Coluna "{col_name}" adicionada em sis_usuarios.')
            else:
                print(f'[--] Coluna "{col_name}" ja existe em sis_usuarios.')


def adicionar_colunas_contratos():
    """Adiciona colunas de silenciamento de vigencia em contratos."""
    inspector = db.inspect(db.engine)
    colunas = [col['name'] for col in inspector.get_columns('contratos')]

    novas_colunas = [
        ('vigencia_notificacao_silenciada', 'TINYINT(1) NOT NULL DEFAULT 0'),
        ('vigencia_silenciada_por', 'BIGINT NULL'),
        ('vigencia_silenciada_em', 'DATETIME NULL'),
        ('vigencia_silenciada_motivo', 'VARCHAR(255) NULL'),
    ]

    with db.engine.begin() as conn:
        for col_name, col_def in novas_colunas:
            if col_name not in colunas:
                conn.execute(db.text(
                    f'ALTER TABLE contratos ADD COLUMN {col_name} {col_def}'
                ))
                print(f'[OK] Coluna "{col_name}" adicionada em contratos.')
            else:
                print(f'[--] Coluna "{col_name}" ja existe em contratos.')


def seed_tipos_notificacao():
    """Popula a tabela notificacao_tipos com os tipos padrao."""
    from app.models.notificacao import NotificacaoTipo

    inseridos = 0
    existentes = 0

    for tipo_data in TIPOS_NOTIFICACAO_SEED:
        existente = NotificacaoTipo.query.filter_by(codigo=tipo_data['codigo']).first()
        if existente:
            existentes += 1
            continue

        tipo = NotificacaoTipo(
            codigo=tipo_data['codigo'],
            modulo=tipo_data['modulo'],
            nome=tipo_data['nome'],
            descricao=tipo_data.get('descricao'),
            nivel=tipo_data['nivel'],
            canal_in_app=tipo_data.get('canal_in_app', True),
            canal_email=tipo_data.get('canal_email', False),
            canal_telegram=tipo_data.get('canal_telegram', False),
            periodicidade=tipo_data.get('periodicidade'),
            ativo=True,
        )
        db.session.add(tipo)
        inseridos += 1

    db.session.commit()
    print(f'[OK] Tipos de notificacao: {inseridos} inseridos, {existentes} ja existiam.')


def main():
    parser = argparse.ArgumentParser(
        description='Cria tabelas do sistema de notificacoes'
    )
    parser.add_argument('--seed', action='store_true',
                        help='Popula a tabela notificacao_tipos com os tipos padrao')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        print('=== Sistema de Notificacoes - Migracao ===\n')

        # 1. Criar tabelas novas
        criar_tabelas()

        # 2. Adicionar colunas em tabelas existentes
        print()
        adicionar_colunas_sis_usuarios()
        print()
        adicionar_colunas_contratos()

        # 3. Seed opcional
        if args.seed:
            print()
            seed_tipos_notificacao()

        print('\n=== Concluido! ===')


if __name__ == '__main__':
    main()
