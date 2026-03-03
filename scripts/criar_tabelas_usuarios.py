"""
Script para criar as tabelas de perfis/permissões e configurar controle de acesso.

Uso:
    python scripts/criar_tabelas_usuarios.py
    python scripts/criar_tabelas_usuarios.py --seed
    python scripts/criar_tabelas_usuarios.py --set-admin USER_ID

Lógica de acesso:
    - is_admin=True  → acesso ao módulo Usuários + acesso total a todos os módulos
    - Perfil com permissões → define quais módulos o usuário pode acessar
    - Sem perfil → sem acesso a nenhum módulo
"""
import sys
import os
import argparse

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.perfil import Perfil, PerfilPermissao, MODULOS, ACOES
from app.models.usuario import Usuario


def criar_tabelas():
    """Cria as tabelas perfis e perfil_permissoes se não existirem."""
    inspector = db.inspect(db.engine)
    tabelas_existentes = inspector.get_table_names()

    if 'perfis' not in tabelas_existentes:
        Perfil.__table__.create(db.engine)
        print('[OK] Tabela "perfis" criada.')
    else:
        print('[--] Tabela "perfis" já existe.')

    if 'perfil_permissoes' not in tabelas_existentes:
        PerfilPermissao.__table__.create(db.engine)
        print('[OK] Tabela "perfil_permissoes" criada.')
    else:
        print('[--] Tabela "perfil_permissoes" já existe.')


def adicionar_colunas_sis_usuarios():
    """Adiciona colunas perfil_id e is_admin em sis_usuarios se não existirem."""
    inspector = db.inspect(db.engine)
    colunas = [col['name'] for col in inspector.get_columns('sis_usuarios')]

    with db.engine.begin() as conn:
        if 'perfil_id' not in colunas:
            conn.execute(db.text(
                'ALTER TABLE sis_usuarios ADD COLUMN perfil_id INT NULL, '
                'ADD CONSTRAINT fk_usuario_perfil FOREIGN KEY (perfil_id) REFERENCES perfis(id)'
            ))
            print('[OK] Coluna "perfil_id" adicionada em sis_usuarios.')
        else:
            print('[--] Coluna "perfil_id" já existe em sis_usuarios.')

        if 'is_admin' not in colunas:
            conn.execute(db.text(
                'ALTER TABLE sis_usuarios ADD COLUMN is_admin TINYINT(1) NOT NULL DEFAULT 0'
            ))
            print('[OK] Coluna "is_admin" adicionada em sis_usuarios.')
        else:
            print('[--] Coluna "is_admin" já existe em sis_usuarios.')


def seed_perfil_padrao():
    """Cria perfil padrão 'Acesso Total' com permissão a todos os módulos.

    Este perfil serve para dar acesso a todos os módulos de negócio
    (Pagamentos, Financeiro, Execuções) para usuários NÃO admin.
    """
    existente = Perfil.query.filter_by(nome='Acesso Total').first()
    if existente:
        print('[--] Perfil "Acesso Total" já existe (id=%d).' % existente.id)
        return existente

    perfil = Perfil(
        nome='Acesso Total',
        descricao='Acesso a todos os módulos de negócio (Pagamentos, Financeiro, Execuções)'
    )
    db.session.add(perfil)
    db.session.flush()

    for modulo_key, _ in MODULOS:
        for acao_key, _ in ACOES:
            perm = PerfilPermissao(
                perfil_id=perfil.id,
                modulo=modulo_key,
                acao=acao_key
            )
            db.session.add(perm)

    db.session.commit()
    print('[OK] Perfil "Acesso Total" criado com %d permissões (id=%d).' % (
        len(MODULOS) * len(ACOES), perfil.id
    ))
    return perfil


def set_admin(usuario_id):
    """Marca um usuário como administrador (is_admin=True)."""
    usuario = Usuario.query.get(usuario_id)
    if not usuario:
        print('[ERRO] Usuário id=%d não encontrado.' % usuario_id)
        return

    usuario.is_admin = True
    db.session.commit()
    print('[OK] Usuário "%s" (id=%d) agora é ADMINISTRADOR.' % (
        usuario.nome, usuario.id
    ))


def main():
    parser = argparse.ArgumentParser(
        description='Cria tabelas de perfis/permissões e configura controle de acesso'
    )
    parser.add_argument('--seed', action='store_true',
                        help='Cria perfil padrão "Acesso Total" com todas as permissões')
    parser.add_argument('--set-admin', type=int, metavar='USER_ID',
                        help='Marca o usuário com este ID como administrador (is_admin=True)')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        print('=== Configuração do Controle de Acesso ===\n')

        criar_tabelas()
        adicionar_colunas_sis_usuarios()

        if args.seed:
            print()
            seed_perfil_padrao()

        if args.set_admin:
            print()
            set_admin(args.set_admin)

        print('\n=== Concluído! ===')


if __name__ == '__main__':
    main()
