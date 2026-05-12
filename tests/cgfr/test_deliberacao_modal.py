from pathlib import Path

import pytest
from flask import render_template

from app.cgfr.models import CgfrProcessoEnviado
from app.cgfr.services.processo_service import ProcessoService
from app.config import Config
from app.constants import CGFR_DELIBERACAO_OPTIONS


def test_modal_edicao_deliberacao_renderiza_dropdown(app):
    with app.test_request_context('/cgfr/'):
        html = render_template(
            'cgfr/partials/modal_edicao.html',
            cgfr_deliberacao_options=CGFR_DELIBERACAO_OPTIONS,
        )

    assert '<select class="form-select form-select-sm" id="modal-deliberacao">' in html
    assert 'id="modal-deliberacao" rows=' not in html
    for opcao in CGFR_DELIBERACAO_OPTIONS:
        assert f'value="{opcao}"' in html


def test_constantes_trino_usam_credenciais_atualizadas():
    assert Config.TRINO_USER == 'admin'
    assert Config.TRINO_PASSWORD == 'LOC35q3dgZn'


def test_classificacao_rejeita_deliberacao_fora_do_dropdown(db_session):
    processo = CgfrProcessoEnviado(
        processo_formatado='00002.000001/2026-01',
        deliberacao='Aprovado',
    )
    db_session.add(processo)
    db_session.commit()

    with pytest.raises(ValueError, match='Deliberação inválida'):
        ProcessoService.classificar_processo(
            processo.processo_formatado,
            {'deliberacao': 'Texto livre indevido'},
        )

    db_session.refresh(processo)
    assert processo.deliberacao == 'Aprovado'


def test_modal_edicao_nao_mantem_campo_aberto_para_deliberacao():
    template_path = Path('app/templates/cgfr/partials/modal_edicao.html')
    html = template_path.read_text(encoding='utf-8')

    assert 'textarea class="form-control form-control-sm" id="modal-deliberacao"' not in html
