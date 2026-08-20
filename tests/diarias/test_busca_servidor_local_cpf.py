"""
Fallback local da busca de servidor nunca encontrava ninguem (producao, 20/08/2026).

Os CPFs em diarias_servidores estao gravados FORMATADOS ('992.027.433-04' —
a coluna DiariasControleServidor.cpf e String(14), exatamente o tamanho do CPF
com pontuacao). A rota /diarias/api/buscar-pessoa consultava
filter_by(cpf=cpf_limpo), com apenas digitos, entao a comparacao jamais casava
e toda busca caia na API externa. Com o SGA fora do ar, ficava sem nenhuma via.

Como rodar:
    pytest tests/diarias/test_busca_servidor_local_cpf.py -v
"""
import pytest

from app.models.diaria import DiariasServidor
from app.models.usuario import Usuario


@pytest.fixture
def admin(db_session):
    """
    Admin logado nas requisicoes.

    Criado aqui (e nao pela fixture global `admin`) porque aquela
    abre um app_context proprio: ao sair dele a scoped_session e descartada
    e o usuario apenas flushado some, derrubando o login para 302.
    """
    u = Usuario(
        id_usuario_sei='admin_cpf_teste',
        nome='ADMIN CPF TESTE',
        sigla_login='admin_cpf',
        is_admin=True,
        ativo=True,
    )
    db_session.add(u)
    db_session.commit()
    return u


@pytest.fixture
def servidor_formatado(db_session):
    """Servidor gravado como esta em producao: CPF com pontuacao."""
    servidor = DiariasServidor(
        idpessoa=101,
        nome='PEDRO ALEXANDRE CABRAL DE OLIVEIRA',
        cpf='992.027.433-04',
        matricula='427189-X',
        cargo='Superintendente',
    )
    db_session.add(servidor)
    db_session.commit()
    return servidor


@pytest.fixture
def servidor_digitos(db_session):
    """Servidor gravado sem pontuacao — formato que a rota de salvar produzia."""
    servidor = DiariasServidor(
        idpessoa=102,
        nome='ALICIA LUNA DE SOUZA',
        cpf='06387512336',
        matricula='500001-X',
    )
    db_session.add(servidor)
    db_session.commit()
    return servidor


def _login(client, admin):
    with client.session_transaction() as sess:
        sess['_user_id'] = str(admin.id)
        sess['_fresh'] = True


def _sem_chamar_api(monkeypatch):
    """A busca local deve resolver sozinha, sem tocar no SGA."""
    monkeypatch.setattr(
        'app.diarias.routes.api.SGAService.buscar_pessoa_por_cpf',
        lambda cpf: pytest.fail('Busca local falhou e caiu na API externa.'),
    )


class TestBuscaLocalPorCpf:

    def test_encontra_servidor_formatado_digitando_apenas_numeros(
        self, client, app, db_session, admin, servidor_formatado, monkeypatch
    ):
        """Usuario digita '99202743304'; banco tem '992.027.433-04'."""
        _login(client, admin)
        _sem_chamar_api(monkeypatch)

        resp = client.get('/diarias/api/buscar-pessoa?cpf=99202743304')

        assert resp.status_code == 200
        dados = resp.get_json()
        assert dados['encontrado'] is True, (
            'CPF gravado formatado nao foi encontrado a partir dos digitos.'
        )
        assert dados['origem'] == 'local'
        assert dados['nome'] == 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA'

    def test_encontra_servidor_formatado_digitando_com_pontuacao(
        self, client, app, db_session, admin, servidor_formatado, monkeypatch
    ):
        _login(client, admin)
        _sem_chamar_api(monkeypatch)

        resp = client.get('/diarias/api/buscar-pessoa?cpf=992.027.433-04')

        assert resp.status_code == 200
        assert resp.get_json()['encontrado'] is True

    def test_encontra_servidor_gravado_sem_pontuacao(
        self, client, app, db_session, admin, servidor_digitos, monkeypatch
    ):
        """Registros legados gravados so com digitos continuam encontraveis."""
        _login(client, admin)
        _sem_chamar_api(monkeypatch)

        resp = client.get('/diarias/api/buscar-pessoa?cpf=063.875.123-36')

        assert resp.status_code == 200
        assert resp.get_json()['encontrado'] is True

    def test_cpf_com_menos_de_11_digitos_retorna_400(
        self, client, app, db_session, admin
    ):
        _login(client, admin)

        resp = client.get('/diarias/api/buscar-pessoa?cpf=123')

        assert resp.status_code == 400


class TestSalvarServidorNormalizaCpf:

    def test_salva_cpf_formatado(
        self, client, app, db_session, admin
    ):
        """Novos cadastros devem seguir o formato ja usado no banco."""
        _login(client, admin)

        resp = client.post(
            '/diarias/api/salvar-servidor',
            json={'cpf': '99211289300', 'nome': 'LUANA MARIA PORTELA'},
        )

        assert resp.status_code == 200
        salvo = DiariasServidor.query.filter(
            DiariasServidor.nome == 'LUANA MARIA PORTELA'
        ).first()
        assert salvo is not None
        assert salvo.cpf == '992.112.893-00', (
            f'CPF gravado como {salvo.cpf!r}; esperado formatado.'
        )

    def test_nao_duplica_servidor_existente_com_formato_diferente(
        self, client, app, db_session, admin, servidor_formatado
    ):
        """Salvar '99202743304' deve atualizar o registro '992.027.433-04'."""
        _login(client, admin)

        resp = client.post(
            '/diarias/api/salvar-servidor',
            json={'cpf': '99202743304', 'nome': 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA'},
        )

        assert resp.status_code == 200
        assert resp.get_json().get('atualizado') is True, (
            'Criou registro novo em vez de atualizar — gera CPF duplicado no banco.'
        )
        total = DiariasServidor.query.filter(
            DiariasServidor.cpf.in_(['992.027.433-04', '99202743304'])
        ).count()
        assert total == 1
