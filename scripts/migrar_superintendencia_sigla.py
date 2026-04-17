"""
Migração: adiciona as colunas `unidade_sei_id`, `unidade_sei_sigla` e
`superintendencia_sigla` em `sis_usuarios` e tenta preencher a
`superintendencia_sigla` dos usuários existentes usando o `setor_vinculado`
(texto livre) quando possível.

Usuários que não forem identificáveis terão os campos NULL — serão preenchidos
automaticamente no próximo login do usuário (ver app/auth/routes.py →
sincronizar_usuario_local).

Uso:
    python scripts/migrar_superintendencia_sigla.py
    python scripts/migrar_superintendencia_sigla.py --apenas-colunas
    python scripts/migrar_superintendencia_sigla.py --apenas-backfill
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models import Usuario
from app.utils.unidade_sei import (
    SUPERINTENDENCIAS_SEAD, MAPEAMENTO_DIRETORIA_SUPER, extrair_superintendencia,
)


def adicionar_colunas():
    """Adiciona as 3 colunas em sis_usuarios (idempotente)."""
    inspector = db.inspect(db.engine)
    colunas = {col['name'] for col in inspector.get_columns('sis_usuarios')}

    alterations = []
    if 'unidade_sei_id' not in colunas:
        alterations.append('ADD COLUMN unidade_sei_id VARCHAR(50) NULL')
    if 'unidade_sei_sigla' not in colunas:
        alterations.append('ADD COLUMN unidade_sei_sigla VARCHAR(255) NULL')
    if 'superintendencia_sigla' not in colunas:
        alterations.append('ADD COLUMN superintendencia_sigla VARCHAR(50) NULL')

    if alterations:
        with db.engine.begin() as conn:
            conn.execute(db.text(
                'ALTER TABLE sis_usuarios ' + ', '.join(alterations)
            ))
            print(f'[OK] Colunas adicionadas: {len(alterations)}')
    else:
        print('[--] Todas as colunas já existem.')

    # Índices (idempotente — falha silenciosa se já existem)
    indices = {
        'idx_usuario_unidade_sei_id': 'unidade_sei_id',
        'idx_usuario_unidade_sei_sigla': 'unidade_sei_sigla',
        'idx_usuario_super_sigla': 'superintendencia_sigla',
    }
    indices_existentes = {idx['name'] for idx in inspector.get_indexes('sis_usuarios')}
    for nome_idx, coluna in indices.items():
        if nome_idx in indices_existentes:
            print(f'[--] Índice "{nome_idx}" já existe.')
            continue
        try:
            with db.engine.begin() as conn:
                conn.execute(db.text(
                    f'ALTER TABLE sis_usuarios ADD INDEX {nome_idx} ({coluna})'
                ))
                print(f'[OK] Índice "{nome_idx}" criado.')
        except Exception as e:
            print(f'[WARN] Não foi possível criar índice "{nome_idx}": {e}')


def _tentar_inferir_super_de_texto(texto):
    """Tenta identificar a sigla da superintendência a partir de texto livre.

    Ex: 'SGA - Superintendência de Gestão Administrativa' → 'SGACG'
        'DFIN - Diretoria de Planejamento e Finanças'     → 'SGACG' (via mapeamento)
    """
    if not texto:
        return None
    txt = texto.upper()

    # Caso especial: 'SGA' no texto → SGACG (SGA é como é referida em documentos,
    # mas no SEI a Sigla é SGACG)
    if 'SGA' in txt and 'SGACG' not in txt:
        txt_norm = txt.replace('SGA', 'SGACG')
    else:
        txt_norm = txt

    # Match direto com superintendência conhecida
    for super_sigla in SUPERINTENDENCIAS_SEAD:
        if super_sigla in txt_norm:
            return super_sigla

    # Match com diretoria órfã
    for diretoria, super_sigla in MAPEAMENTO_DIRETORIA_SUPER.items():
        if diretoria in txt_norm:
            return super_sigla

    return None


def backfill_superintendencia():
    """Tenta preencher superintendencia_sigla para usuários existentes."""
    usuarios = Usuario.query.filter(Usuario.superintendencia_sigla.is_(None)).all()
    print(f'[--] {len(usuarios)} usuários sem superintendencia_sigla.')

    preenchidos = 0
    for u in usuarios:
        super_sigla = None

        # Estratégia 1: se já tem unidade_sei_sigla (raro nesta migração)
        if u.unidade_sei_sigla:
            super_sigla, _ = extrair_superintendencia(u.unidade_sei_sigla)

        # Estratégia 2: inferir do setor_vinculado (texto livre)
        if not super_sigla and u.setor_vinculado:
            super_sigla = _tentar_inferir_super_de_texto(u.setor_vinculado)

        # Estratégia 3: se tem setor_id → SetorSead, usa a sigla do setor/super dele
        if not super_sigla and u.setor:
            setor_obj = u.setor
            if setor_obj.is_superintendencia:
                # Sigla do CSV (SGA, SLC, etc.) pode diferir da Sigla SEI (SGACG, etc.)
                super_sigla = _tentar_inferir_super_de_texto(setor_obj.sigla or setor_obj.nome)
            elif setor_obj.superintendencia:
                super_sigla = _tentar_inferir_super_de_texto(
                    setor_obj.superintendencia.sigla or setor_obj.superintendencia.nome
                )

        if super_sigla:
            u.superintendencia_sigla = super_sigla
            preenchidos += 1

    db.session.commit()
    print(f'[OK] {preenchidos}/{len(usuarios)} usuários preenchidos via backfill.')
    print(f'[--] {len(usuarios) - preenchidos} ficarão NULL até o próximo login.')


def main():
    parser = argparse.ArgumentParser(
        description='Migra sis_usuarios para usar superintendência derivada da Sigla SEI'
    )
    parser.add_argument('--apenas-colunas', action='store_true',
                        help='Só adiciona as colunas, não faz backfill')
    parser.add_argument('--apenas-backfill', action='store_true',
                        help='Só faz o backfill (colunas já existem)')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        print('=== Migração Superintendência Sigla (derivada do SEI) ===\n')

        if not args.apenas_backfill:
            adicionar_colunas()
            print()

        if not args.apenas_colunas:
            backfill_superintendencia()

        print('\n=== Concluído! ===')


if __name__ == '__main__':
    main()
