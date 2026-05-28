"""
Configuração global de testes do SGC.

Usa SQLite in-memory para isolamento completo — nenhuma query toca o banco
de produção ou desenvolvimento. Cada sessão de testes cria e destrói o schema
inteiro.

Uso:
    pip install -r requirements-test.txt
    pytest tests/ -v
    pytest tests/ -v -k "test_get_valor_cargo"   # filtro por nome
    pytest tests/diarias/ -v                       # apenas módulo diárias
"""
import os
import pytest

# Força ambiente de testes ANTES de qualquer import da aplicação
os.environ['FLASK_ENV'] = 'testing'


# ── SQLite BigInteger PK fix ──────────────────────────────────────────────────
# SQLAlchemy uses a table-level PRIMARY KEY constraint for BigInteger PKs,
# which is NOT treated as a rowid alias in SQLite — so autoincrement fails
# with "NOT NULL constraint failed". This event listener auto-generates IDs
# for BigInteger PK columns before each flush in the test session.

def _bigint_pk_autoincrement(session, flush_context, instances):
    """Auto-assign IDs for BigInteger PKs in SQLite (test environment only).

    Tracks next available ID per table within the same flush to avoid duplicate
    IDs when multiple objects of the same type are added before the first flush.
    """
    from sqlalchemy import BigInteger, text
    from sqlalchemy import inspect as sa_inspect

    # Per-table counter for this flush — avoids re-querying MAX before inserts land
    next_id: dict = {}

    for obj in list(session.new):
        try:
            mapper = sa_inspect(type(obj))
        except Exception:
            continue
        for col_attr in mapper.column_attrs:
            col = col_attr.columns[0]
            if not col.primary_key:
                continue
            if not isinstance(col.type, BigInteger):
                continue
            if getattr(obj, col_attr.key) is not None:
                continue
            tname = mapper.local_table.name
            col_name = col.name
            key = (tname, col_name)
            if key not in next_id:
                # Query DB once per table/column per flush
                try:
                    conn = session.connection()
                    val = conn.execute(
                        text(f'SELECT COALESCE(MAX("{col_name}"), 0) FROM "{tname}"')
                    ).scalar()
                    next_id[key] = (val or 0) + 1
                except Exception:
                    import time
                    next_id[key] = int(time.time() * 1000) % 2_000_000_000
            else:
                next_id[key] += 1
            setattr(obj, col_attr.key, next_id[key])


@pytest.fixture(scope='session')
def app():
    """
    Cria a aplicação Flask configurada para testes.

    scope='session': criada uma única vez por execução de pytest.
    O banco SQLite in-memory é compartilhado entre todos os testes da sessão
    via 'check_same_thread=False' (necessário para Flask-SQLAlchemy).
    """
    from app import create_app
    from app.config import TestingConfig

    class SGCTestConfig(TestingConfig):
        TESTING = True
        SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:?check_same_thread=False'
        SQLALCHEMY_TRACK_MODIFICATIONS = False
        WTF_CSRF_ENABLED = False
        SECRET_KEY = 'test-secret-key'
        SESSION_TYPE = 'filesystem'
        SESSION_FILE_DIR = '/tmp/sgc_test_sessions'
        # Desabilita integrações externas nos testes
        SEI_API_URL = 'http://mock-sei'
        SIAFE_URL = 'http://mock-siafe'
        DISABLE_SCHEDULER = True
        LOGIN_DISABLED = False

    flask_app = create_app(SGCTestConfig)

    yield flask_app


@pytest.fixture(scope='session')
def db(app):
    """
    Cria todas as tabelas no SQLite in-memory.

    Executado uma vez por sessão. O schema é criado do zero a partir dos models
    SQLAlchemy registrados — sem migrations, sem ALTER TABLE.
    """
    from sqlalchemy import event
    from app.extensions import db as _db

    with app.app_context():
        _db.create_all()
        # Register BigInteger PK autoincrement fix for SQLite
        event.listen(_db.session, 'before_flush', _bigint_pk_autoincrement)
        yield _db
        event.remove(_db.session, 'before_flush', _bigint_pk_autoincrement)
        _db.drop_all()


@pytest.fixture(scope='function')
def db_session(db, app):
    """
    Isolamento por teste via TRUNCATE no teardown.

    Em Flask-SQLAlchemy 3.x, `db.session` é uma scoped_session que resolve o
    engine via `current_app` — bind manual em connection externa é ignorado, e
    o SAVEPOINT pattern clássico do SQLAlchemy não funciona de forma confiável.
    Como vários endpoints de produção chamam `db.session.commit()` dentro de
    rotas testadas, esses commits vazam para o banco entre testes (contaminação
    silenciosa: UNIQUE constraint failures, estado residual de seeders, etc.).

    Solução: deixar o teste rodar livremente (commits inclusive) e, no teardown,
    DELETE em todas as tabelas na ordem reversa de dependência FK. SQLite
    in-memory torna isso barato (~5ms por teste).
    """
    with app.app_context():
        yield db.session

        # Teardown: rollback de transação aberta + delete em todas as tabelas.
        db.session.rollback()
        for table in reversed(db.metadata.sorted_tables):
            db.session.execute(table.delete())
        db.session.commit()
        db.session.remove()


@pytest.fixture(scope='function')
def client(app, db_session):
    """Cliente HTTP de teste com contexto de banco isolado."""
    return app.test_client()


@pytest.fixture(scope='function')
def runner(app):
    """CLI test runner."""
    return app.test_cli_runner()


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures de dados reutilizáveis
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def usuario_admin(db_session, app):
    """Cria e retorna um usuário administrador para testes."""
    with app.app_context():
        from app.models.usuario import Usuario

        u = Usuario(
            id_usuario_sei='admin_teste_sei',
            nome='ADMIN TESTE',
            sigla_login='admin_teste',
            is_admin=True,
            ativo=True,
        )
        db_session.add(u)
        db_session.flush()
        return u


@pytest.fixture()
def usuario_comum(db_session, app):
    """Cria e retorna um usuário sem admin para testes de permissão."""
    with app.app_context():
        from app.models.usuario import Usuario

        u = Usuario(
            id_usuario_sei='user_teste_sei',
            nome='USUARIO COMUM',
            sigla_login='user_teste',
            is_admin=False,
            ativo=True,
        )
        db_session.add(u)
        db_session.flush()
        return u
