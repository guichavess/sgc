from pathlib import Path


NOVA_EXPANSAO = 'Comissão de Gestão Financeira e Gestão Por Resultados'
EXPANSOES_ANTIGAS = (
    'Consultoria de Gestão Financeira',
    'Consultoria de Gestao Financeira',
)


def test_expansao_da_sigla_cgfr_atualizada_em_pontos_visiveis():
    root = Path(__file__).resolve().parents[2]
    arquivos = [
        root / 'app/templates/hub.html',
        root / 'app/templates/cgfr/reports/report.html',
        root / 'app/templates/cgfr/reports/report_pdf.html',
        root / 'app/cgfr/models.py',
        root / 'app/cgfr/routes/__init__.py',
        root / 'app/models/__init__.py',
        root / 'app/static/css/components/cgfr.css',
    ]

    conteudo = '\n'.join(path.read_text(encoding='utf-8') for path in arquivos)

    assert NOVA_EXPANSAO in conteudo
    for antiga in EXPANSOES_ANTIGAS:
        assert antiga not in conteudo
