"""
tests/prestacoes_contratos/test_backfill_competencia_empenho.py

TDD para o backfill de `empenho.competencia` por engenharia reversa via fases
posteriores (NL, PD, OB).

Contexto: SIAFE devolve `competencia` direto/via classificadores para PD/OB/NL,
mas para Empenho só em ~25% dos casos (limitacao da API). Os documentos
posteriores referenciam o NE:

  - Liquidacao.codigoEmpenhoVinculado = Empenho.codigo
  - PD.codigoNE                       = Empenho.codigo
  - OB.codigoNE                       = Empenho.codigo

Como 1 NE pode ter varios filhos com competencias distintas (a despesa empenhada
e diluida em varios meses de execucao), a politica de escolha e:
  1) moda (competencia mais frequente entre os candidatos)
  2) em empate, a mais antiga (menor MM/YYYY ordenado por YYYY-MM)

E a busca e em cascata: NL → PD → OB. Para no primeiro nivel que devolver match.
"""
import pytest

from app.models.empenho import Empenho
from app.models.liquidacao import Liquidacao
from app.models.pd import PD
from app.models.ob import OB
from app.services.backfill_competencia_service import (
    escolher_competencia,
    mapear_competencias_empenho,
)


# ── escolher_competencia: politica pura ──────────────────────────────────────


def test_escolher_competencia_unico_candidato():
    assert escolher_competencia(['03/2025']) == '03/2025'


def test_escolher_competencia_lista_vazia():
    assert escolher_competencia([]) is None


def test_escolher_competencia_ignora_none_e_vazio():
    assert escolher_competencia([None, '', '04/2025']) == '04/2025'


def test_escolher_competencia_so_none_retorna_none():
    assert escolher_competencia([None, None, '']) is None


def test_escolher_competencia_moda_simples():
    """03/2025 aparece 2x, 04/2025 aparece 1x → moda 03/2025."""
    assert escolher_competencia(['03/2025', '04/2025', '03/2025']) == '03/2025'


def test_escolher_competencia_empate_pega_mais_antiga():
    """04/2025 e 03/2025 cada uma 1x → empate → mais antiga: 03/2025."""
    assert escolher_competencia(['04/2025', '03/2025']) == '03/2025'


def test_escolher_competencia_empate_atravessa_ano():
    """12/2024 e 01/2025 cada uma 1x → mais antiga: 12/2024 (ano menor)."""
    assert escolher_competencia(['01/2025', '12/2024']) == '12/2024'


def test_escolher_competencia_empate_de_modas_pega_mais_antiga():
    """03/2025 (2x), 04/2025 (2x), 05/2025 (1x) → empate de modas → 03/2025."""
    candidatos = ['03/2025', '04/2025', '03/2025', '04/2025', '05/2025']
    assert escolher_competencia(candidatos) == '03/2025'


def test_escolher_competencia_normaliza_formato_descritivo():
    """Formato descritivo da API tambem entra na politica canonica MM/YYYY."""
    assert escolher_competencia(['03 - Marco/2025', '03/2025']) == '03/2025'


# ── mapear_competencias_empenho: cascata NL → PD → OB ────────────────────────


def test_mapear_resolve_via_nl_quando_nl_tem_competencia(db_session):
    """NE com 1 NL preenchida → mapeado via NL."""
    db_session.add(Empenho(id=1, codigo='2025NE000001', codContrato=100))
    db_session.add(Liquidacao(
        id=10, codigo='2025NL000010',
        codigoEmpenhoVinculado='2025NE000001',
        competencia='03/2025',
    ))
    db_session.flush()

    resultado = mapear_competencias_empenho(db_session, ['2025NE000001'])

    assert resultado['2025NE000001']['competencia'] == '03/2025'
    assert resultado['2025NE000001']['origem'] == 'NL'


def test_mapear_aplica_moda_quando_multiplas_nls(db_session):
    """NE com 3 NLs (2× 03/2025, 1× 04/2025) → escolhe 03/2025."""
    db_session.add(Empenho(id=1, codigo='2025NE000002', codContrato=100))
    db_session.add_all([
        Liquidacao(id=20, codigo='2025NL000020',
                   codigoEmpenhoVinculado='2025NE000002', competencia='03/2025'),
        Liquidacao(id=21, codigo='2025NL000021',
                   codigoEmpenhoVinculado='2025NE000002', competencia='03/2025'),
        Liquidacao(id=22, codigo='2025NL000022',
                   codigoEmpenhoVinculado='2025NE000002', competencia='04/2025'),
    ])
    db_session.flush()

    resultado = mapear_competencias_empenho(db_session, ['2025NE000002'])

    assert resultado['2025NE000002']['competencia'] == '03/2025'
    assert resultado['2025NE000002']['origem'] == 'NL'


def test_mapear_cai_pra_pd_quando_nls_nao_tem_competencia(db_session):
    """NE com NL sem competencia mas PD com → fallback PD."""
    db_session.add(Empenho(id=1, codigo='2025NE000003', codContrato=100))
    db_session.add(Liquidacao(
        id=30, codigo='2025NL000030',
        codigoEmpenhoVinculado='2025NE000003',
        competencia=None,
    ))
    db_session.add(PD(
        id=300, codigo='2025PD000300',
        codigoNE='2025NE000003',
        competencia='05/2025',
    ))
    db_session.flush()

    resultado = mapear_competencias_empenho(db_session, ['2025NE000003'])

    assert resultado['2025NE000003']['competencia'] == '05/2025'
    assert resultado['2025NE000003']['origem'] == 'PD'


def test_mapear_cai_pra_ob_quando_nl_e_pd_vazios(db_session):
    """NE sem NL nem PD mas com OB → fallback OB."""
    db_session.add(Empenho(id=1, codigo='2025NE000004', codContrato=100))
    db_session.add(OB(
        id=400, codigo='2025OB000400',
        codigoNE='2025NE000004',
        competencia='06/2025',
        codContrato='100',
    ))
    db_session.flush()

    resultado = mapear_competencias_empenho(db_session, ['2025NE000004'])

    assert resultado['2025NE000004']['competencia'] == '06/2025'
    assert resultado['2025NE000004']['origem'] == 'OB'


def test_mapear_ne_sem_match_em_lugar_nenhum_fica_de_fora(db_session):
    """NE sem NL/PD/OB filhos → ausente do resultado (None implicito)."""
    db_session.add(Empenho(id=1, codigo='2025NE000005', codContrato=100))
    db_session.flush()

    resultado = mapear_competencias_empenho(db_session, ['2025NE000005'])

    assert '2025NE000005' not in resultado


def test_mapear_lote_misto(db_session):
    """Lote com 3 NEs: um via NL, um via PD, um sem match."""
    db_session.add_all([
        Empenho(id=1, codigo='2025NE000006', codContrato=100),
        Empenho(id=2, codigo='2025NE000007', codContrato=100),
        Empenho(id=3, codigo='2025NE000008', codContrato=100),
    ])
    db_session.add(Liquidacao(
        id=60, codigo='2025NL000060',
        codigoEmpenhoVinculado='2025NE000006',
        competencia='07/2025',
    ))
    db_session.add(PD(
        id=700, codigo='2025PD000700',
        codigoNE='2025NE000007',
        competencia='08/2025',
    ))
    db_session.flush()

    resultado = mapear_competencias_empenho(
        db_session,
        ['2025NE000006', '2025NE000007', '2025NE000008'],
    )

    assert resultado['2025NE000006'] == {'competencia': '07/2025', 'origem': 'NL'}
    assert resultado['2025NE000007'] == {'competencia': '08/2025', 'origem': 'PD'}
    assert '2025NE000008' not in resultado


def test_mapear_lista_vazia(db_session):
    """Entrada vazia → dict vazio, sem queries."""
    resultado = mapear_competencias_empenho(db_session, [])
    assert resultado == {}


def test_mapear_nao_consulta_nivel_seguinte_se_nl_resolveu(db_session):
    """
    Se a NL ja resolveu, PD com competencia diferente NAO deve sobrescrever.
    Garante a semantica de cascata 'para no primeiro nivel que matchar'.
    """
    db_session.add(Empenho(id=1, codigo='2025NE000009', codContrato=100))
    db_session.add(Liquidacao(
        id=90, codigo='2025NL000090',
        codigoEmpenhoVinculado='2025NE000009',
        competencia='03/2025',
    ))
    db_session.add(PD(
        id=900, codigo='2025PD000900',
        codigoNE='2025NE000009',
        competencia='12/2099',  # valor "errado" pra detectar overwrite
    ))
    db_session.flush()

    resultado = mapear_competencias_empenho(db_session, ['2025NE000009'])

    assert resultado['2025NE000009']['competencia'] == '03/2025'
    assert resultado['2025NE000009']['origem'] == 'NL'


def test_script_update_nao_sobrescreve_empenho_ja_preenchido(db_session):
    """O script so atualiza NEs que continuam sem competencia no momento do UPDATE."""
    from scripts.backfill_competencia_empenho import _aplicar_updates

    db_session.add_all([
        Empenho(id=1, codigo='2025NE000010', codContrato=100, competencia=None),
        Empenho(id=2, codigo='2025NE000011', codContrato=100, competencia='02/2025'),
    ])
    db_session.flush()

    total = _aplicar_updates(db_session, {
        '2025NE000010': {'competencia': '03/2025', 'origem': 'NL'},
        '2025NE000011': {'competencia': '04/2025', 'origem': 'PD'},
    })
    db_session.flush()

    atualizado = Empenho.query.filter_by(codigo='2025NE000010').first()
    preservado = Empenho.query.filter_by(codigo='2025NE000011').first()

    assert total == 1
    assert atualizado.competencia == '03/2025'
    assert preservado.competencia == '02/2025'
