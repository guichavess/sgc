"""
Regressoes para normalizacao de codigos de Fonte vindos do SIAFE.

Alguns endpoints podem devolver a fonte em formato classificador ("7.55",
"5.00"). O banco/modelos e os joins com class_fonte usam o codigo sem pontos
("755", "500"). Se o script apenas converter "7.55" para numerico, o valor
vira 7 e sai do padrao do modulo.
"""
import importlib


def test_normalizar_codigo_fonte_remove_pontos_sem_perder_digitos():
    from app.utils.siafe import normalizar_codigo_fonte

    assert normalizar_codigo_fonte('7.55') == '755'
    assert normalizar_codigo_fonte('5.00') == '500'
    assert normalizar_codigo_fonte(755) == '755'
    assert normalizar_codigo_fonte(None) is None


def test_atualizar_ob_normaliza_cod_fonte_descritivo(monkeypatch):
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASS', 'test_pass')
    monkeypatch.setenv('DB_HOST', 'localhost')
    monkeypatch.setenv('DB_NAME', 'test_db')

    modulo = importlib.import_module('scripts.atualizar_ob')

    class FakeResponse:
        status_code = 200

        def json(self):
            return [{
                'id': 1,
                'codigo': '2025OB000001',
                'codigoUG': '210102',
                'statusDocumento': 'CONTABILIZADO',
                'codFonte': '7.55',
                'codNatureza': '3.3.90.39',
                'dataEmissao': '2025-03-10',
                'competencia': '03 - Marco/2025',
                'codClassificacao': '00000000000000000000000000 25014874. 0',
            }]

    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            return FakeResponse()

    _, df, qtd, _, status = modulo.fetch_data(FakeSession(), '210102', 'token', 2025)

    assert status == 200
    assert qtd == 1
    assert df.loc[0, 'codFonte'] == '755'
    assert df.loc[0, 'competencia'] == '03/2025'
