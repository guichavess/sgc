import io
import sys

import pytest


@pytest.fixture()
def console_cp1252(monkeypatch):
    """stdout como o do servidor no Windows: emoji no print levanta UnicodeEncodeError.

    Devolve um ativador: o pytest reinstala a captura de stdout depois das fixtures,
    então a troca precisa acontecer no corpo do teste.
    """
    def ativar():
        monkeypatch.setattr(sys, 'stdout', io.TextIOWrapper(io.BytesIO(), encoding='cp1252'))
        with pytest.raises(UnicodeEncodeError):
            print('📡')
    return ativar
