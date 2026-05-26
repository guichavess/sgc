"""
Cascata hierárquica do endpoint /dashboards/api/filtros-orcamentario.

Regra de negócio (CLAUDE.md §6):
- Ao escolher uma Ação, somente as Fontes que coexistem com ela em `loa`
  devem aparecer na lista de fontes; o mesmo vale para Naturezas.
- Ao escolher uma Fonte (após Ação), somente as Naturezas que coexistem
  com (Ação, Fonte) devem aparecer.
- A lista de Ações sempre traz o universo do ano (raiz da hierarquia).
"""
import uuid

from sqlalchemy import text


ANO = 2026


def _uid():
    return uuid.uuid4().hex[:8]


def _login_admin(client, db_session):
    from app.models.usuario import Usuario

    uid = _uid()
    u = Usuario(
        id_usuario_sei=f'dash_{uid}',
        nome='ADMIN DASH TESTE',
        sigla_login=f'dash_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(u)
    db_session.flush()
    with client.session_transaction() as sess:
        sess['_user_id'] = str(u.id)
        sess['_fresh'] = True
    return u


def _criar_tabelas_descricao(db_session):
    """Cria tabelas `acao` e `fonterecurso` (não existem como models)."""
    db_session.execute(text(
        "CREATE TABLE IF NOT EXISTS acao "
        "(codigo VARCHAR(10) PRIMARY KEY, titulo VARCHAR(200))"
    ))
    db_session.execute(text(
        "CREATE TABLE IF NOT EXISTS fonterecurso "
        "(codigo VARCHAR(10) PRIMARY KEY, titulo VARCHAR(200))"
    ))


def _seed_descricao(db_session):
    _criar_tabelas_descricao(db_session)
    db_session.execute(text("DELETE FROM acao"))
    db_session.execute(text("DELETE FROM fonterecurso"))
    db_session.execute(text("DELETE FROM natdespesas"))

    db_session.execute(text(
        "INSERT INTO acao (codigo, titulo) VALUES "
        "('1000', 'Acao Alpha'), ('2000', 'Acao Beta')"
    ))
    db_session.execute(text(
        "INSERT INTO fonterecurso (codigo, titulo) VALUES "
        "('100', 'Fonte Tesouro'), ('200', 'Fonte Convenio'), ('300', 'Fonte Propria')"
    ))
    # natdespesas: id é PK autoincrement; precisamos só dos códigos
    db_session.execute(text(
        "INSERT INTO natdespesas (codigo, titulo) VALUES "
        "(319011, 'Vencimentos'), (339030, 'Material'), (339039, 'Servicos PJ')"
    ))


def _seed_loa(db_session, linhas):
    """linhas = lista de (codAcao, codFonte, codNatureza)."""
    db_session.execute(text("DELETE FROM loa"))
    for idx, (acao, fonte, natureza) in enumerate(linhas, start=1):
        db_session.execute(text(
            "INSERT INTO loa (row_id, codigoUG, ano, mes, id, codAcao, codFonte, codNatureza) "
            "VALUES (:rid, '210101', :ano, 1, '622110101', :acao, :fonte, :nat)"
        ), {'rid': idx, 'ano': ANO, 'acao': acao, 'fonte': fonte, 'nat': natureza})
    db_session.flush()


def test_sem_filtros_retorna_todos(client, db_session):
    """Sem params acao/fonte: traz universo do ano."""
    _login_admin(client, db_session)
    _seed_descricao(db_session)
    _seed_loa(db_session, [
        ('1000', '100', '319011'),
        ('1000', '200', '339030'),
        ('2000', '300', '339039'),
    ])

    resp = client.get(f'/dashboards/api/filtros-orcamentario?ano={ANO}')
    assert resp.status_code == 200
    data = resp.get_json()

    acoes = {a['codigo'] for a in data['acoes']}
    fontes = {f['codigo'] for f in data['fontes']}
    naturezas = {n['codigo'] for n in data['naturezas']}

    assert acoes == {'1000', '2000'}
    assert fontes == {'100', '200', '300'}
    assert naturezas == {'319011', '339030', '339039'}


def test_acao_filtra_fontes_e_naturezas(client, db_session):
    """Selecionar Ação restringe Fontes e Naturezas àquelas combinações."""
    _login_admin(client, db_session)
    _seed_descricao(db_session)
    _seed_loa(db_session, [
        ('1000', '100', '319011'),
        ('1000', '200', '339030'),
        ('2000', '300', '339039'),
    ])

    resp = client.get(f'/dashboards/api/filtros-orcamentario?ano={ANO}&acao=1000')
    assert resp.status_code == 200
    data = resp.get_json()

    # Lista de ações permanece com universo (raiz da hierarquia)
    assert {a['codigo'] for a in data['acoes']} == {'1000', '2000'}
    # Fontes e Naturezas restritas
    assert {f['codigo'] for f in data['fontes']} == {'100', '200'}
    assert {n['codigo'] for n in data['naturezas']} == {'319011', '339030'}


def test_acao_e_fonte_filtram_naturezas(client, db_session):
    """Selecionar Ação + Fonte restringe ainda mais as Naturezas."""
    _login_admin(client, db_session)
    _seed_descricao(db_session)
    _seed_loa(db_session, [
        ('1000', '100', '319011'),
        ('1000', '100', '339030'),
        ('1000', '200', '339039'),
        ('2000', '300', '339039'),
    ])

    resp = client.get(
        f'/dashboards/api/filtros-orcamentario?ano={ANO}&acao=1000&fonte=100'
    )
    assert resp.status_code == 200
    data = resp.get_json()

    # Fontes mostram só as válidas para a Ação 1000
    assert {f['codigo'] for f in data['fontes']} == {'100', '200'}
    # Naturezas restritas à interseção (Ação=1000, Fonte=100)
    assert {n['codigo'] for n in data['naturezas']} == {'319011', '339030'}


def test_fonte_sozinha_filtra_naturezas(client, db_session):
    """Selecionar Fonte sem Ação: restringe Naturezas pelas combinações com essa fonte."""
    _login_admin(client, db_session)
    _seed_descricao(db_session)
    _seed_loa(db_session, [
        ('1000', '100', '319011'),
        ('1000', '200', '339030'),
        ('2000', '100', '339039'),
    ])

    resp = client.get(f'/dashboards/api/filtros-orcamentario?ano={ANO}&fonte=100')
    assert resp.status_code == 200
    data = resp.get_json()

    assert {n['codigo'] for n in data['naturezas']} == {'319011', '339039'}
