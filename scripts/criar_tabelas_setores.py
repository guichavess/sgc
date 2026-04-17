"""
Cria tabelas `tipos_entidade` e `setores` (estrutura hierárquica SEAD)
e importa dados dos CSVs em data/tipo_entidade.csv e data/entidades.csv.

Uso:
    python scripts/criar_tabelas_setores.py              # cria tabelas e importa
    python scripts/criar_tabelas_setores.py --apenas-criar  # só cria tabelas
    python scripts/criar_tabelas_setores.py --apenas-importar  # só importa
    python scripts/criar_tabelas_setores.py --reimportar  # limpa e reimporta

Após criar, adiciona coluna `setor_id` em sis_usuarios.
"""
import sys
import os
import csv
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.setor import SetorSead, TipoEntidade, ID_GABINETE


DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
CSV_TIPOS = os.path.join(DATA_DIR, 'tipo_entidade.csv')
CSV_ENTIDADES = os.path.join(DATA_DIR, 'entidades.csv')


def _parse_int_or_none(valor):
    """Converte string para int, retornando None se 'NULL' ou vazio."""
    if valor is None:
        return None
    s = str(valor).strip()
    if not s or s.upper() == 'NULL':
        return None
    try:
        return int(s)
    except ValueError:
        return None


def criar_tabelas():
    """Cria as tabelas tipos_entidade e setores."""
    inspector = db.inspect(db.engine)
    tabelas_existentes = inspector.get_table_names()

    if 'tipos_entidade' not in tabelas_existentes:
        TipoEntidade.__table__.create(db.engine)
        print('[OK] Tabela "tipos_entidade" criada.')
    else:
        print('[--] Tabela "tipos_entidade" já existe.')

    if 'setores' not in tabelas_existentes:
        SetorSead.__table__.create(db.engine)
        print('[OK] Tabela "setores" criada.')
    else:
        print('[--] Tabela "setores" já existe.')


def adicionar_coluna_setor_id():
    """Adiciona coluna setor_id em sis_usuarios se não existir."""
    inspector = db.inspect(db.engine)
    colunas = [col['name'] for col in inspector.get_columns('sis_usuarios')]

    if 'setor_id' in colunas:
        print('[--] Coluna "setor_id" já existe em sis_usuarios.')
        return

    with db.engine.begin() as conn:
        conn.execute(db.text(
            'ALTER TABLE sis_usuarios ADD COLUMN setor_id INT NULL, '
            'ADD CONSTRAINT fk_usuario_setor FOREIGN KEY (setor_id) REFERENCES setores(id), '
            'ADD INDEX idx_usuario_setor (setor_id)'
        ))
        print('[OK] Coluna "setor_id" adicionada em sis_usuarios.')


def importar_tipos_entidade():
    """Importa tipos de entidade do CSV."""
    if not os.path.exists(CSV_TIPOS):
        print(f'[ERRO] Arquivo não encontrado: {CSV_TIPOS}')
        return 0

    count = 0
    with open(CSV_TIPOS, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cod = _parse_int_or_none(row['codtipoentidade'])
            if cod is None:
                continue

            existente = TipoEntidade.query.get(cod)
            if existente:
                existente.nome = row['nome'].strip()
                existente.nivel = _parse_int_or_none(row['nivel']) or 0
                existente.nome2 = (row.get('nome2') or '').strip() or None
            else:
                te = TipoEntidade(
                    codtipoentidade=cod,
                    nome=row['nome'].strip(),
                    nivel=_parse_int_or_none(row['nivel']) or 0,
                    nome2=(row.get('nome2') or '').strip() or None,
                )
                db.session.add(te)
                count += 1

    db.session.commit()
    print(f'[OK] {count} tipos de entidade importados/atualizados.')
    return count


def importar_entidades():
    """Importa setores/entidades do CSV.

    Processa em 2 passadas:
    1. Cria registros sem FKs (para permitir referências cruzadas).
    2. Atualiza parent_id e superintendencia_id.
    """
    if not os.path.exists(CSV_ENTIDADES):
        print(f'[ERRO] Arquivo não encontrado: {CSV_ENTIDADES}')
        return 0

    # Passada 1: lê todas as linhas
    linhas = []
    with open(CSV_ENTIDADES, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            id_ent = _parse_int_or_none(row['identidade'])
            if id_ent is None:
                continue
            linhas.append({
                'id': id_ent,
                'nome': row['nome'].strip(),
                'sigla': (row.get('sigla') or '').strip() or None,
                'superintendencia_id': _parse_int_or_none(row.get('idsuperintendencia')),
                'parent_id': _parse_int_or_none(row.get('codentidadepai')),
                'tipo_entidade_id': _parse_int_or_none(row.get('codtipoentidade')) or 25,  # 25=Unidade (fallback)
            })

    # Normalização: entidades sem superintendência mas subordinadas ao Gabinete
    # viram "superintendencia_id = 96" (Gabinete como pseudo-superintendência).
    for linha in linhas:
        if linha['superintendencia_id'] is None and linha['parent_id'] == ID_GABINETE:
            linha['superintendencia_id'] = ID_GABINETE

    # Passada 1: cria sem FKs
    count_novo = 0
    count_atualizado = 0
    for linha in linhas:
        existente = SetorSead.query.get(linha['id'])
        if existente:
            existente.nome = linha['nome']
            existente.sigla = linha['sigla']
            existente.tipo_entidade_id = linha['tipo_entidade_id']
            existente.ativo = True
            count_atualizado += 1
        else:
            setor = SetorSead(
                id=linha['id'],
                nome=linha['nome'],
                sigla=linha['sigla'],
                tipo_entidade_id=linha['tipo_entidade_id'],
                ativo=True,
            )
            db.session.add(setor)
            count_novo += 1

    db.session.flush()  # força insert antes de resolver FKs

    # Passada 2: resolve FKs (parent_id e superintendencia_id)
    ids_validos = {linha['id'] for linha in linhas}
    for linha in linhas:
        setor = SetorSead.query.get(linha['id'])
        if not setor:
            continue
        # Só grava FK se o destino existe (evita erro de integridade referencial)
        setor.parent_id = linha['parent_id'] if linha['parent_id'] in ids_validos else None
        setor.superintendencia_id = linha['superintendencia_id'] if linha['superintendencia_id'] in ids_validos else None

    db.session.commit()
    print(f'[OK] {count_novo} setores criados, {count_atualizado} atualizados (total: {len(linhas)}).')
    return len(linhas)


def limpar_setores():
    """Remove todos os setores (cuidado!)."""
    print('[!!!] Limpando tabela setores...')
    # Zera FKs primeiro para evitar erro circular
    db.session.execute(db.text('UPDATE setores SET parent_id=NULL, superintendencia_id=NULL'))
    db.session.execute(db.text('UPDATE sis_usuarios SET setor_id=NULL WHERE setor_id IS NOT NULL'))
    SetorSead.query.delete()
    db.session.commit()
    print('[OK] Tabela setores limpa.')


def main():
    parser = argparse.ArgumentParser(description='Cria e popula tabelas de setores/superintendências')
    parser.add_argument('--apenas-criar', action='store_true', help='Só cria tabelas')
    parser.add_argument('--apenas-importar', action='store_true', help='Só importa CSVs')
    parser.add_argument('--reimportar', action='store_true', help='Limpa e reimporta tudo')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        print('=== Setup de Setores/Superintendências SEAD ===\n')

        if not args.apenas_importar:
            criar_tabelas()
            adicionar_coluna_setor_id()

        if not args.apenas_criar:
            if args.reimportar:
                limpar_setores()
            print()
            importar_tipos_entidade()
            importar_entidades()

        print('\n=== Concluído! ===')


if __name__ == '__main__':
    main()
