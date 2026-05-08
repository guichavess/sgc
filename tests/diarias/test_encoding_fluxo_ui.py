"""
Testes para normalizacao de texto e layout do fluxo de diarias.
"""
from pathlib import Path


def test_corrigir_mojibake_cp850_em_texto_de_diarias():
    from app.utils.text_encoding import corrigir_mojibake_cp850

    texto_quebrado = (
        "Participa├º├úo no 3┬║ F├│rum Anual do Programa Progest├úo, "
        "fortalece as capacidades t├®cnicas e amplia a rede de colabora├º├úo."
    )

    assert corrigir_mojibake_cp850(texto_quebrado) == (
        "Participação no 3º Fórum Anual do Programa Progestão, "
        "fortalece as capacidades técnicas e amplia a rede de colaboração."
    )


def test_criar_itinerario_normaliza_objetivo_e_justificativa(db_session, app):
    with app.app_context():
        from app.models.diaria import (
            DiariasCargo,
            DiariasEtapa,
            DiariasStatusViagem,
            DiariasTipoItinerario,
            DiariasTipoSolicitacao,
            DiariasValorCargo,
        )
        from app.services.diaria_service import DiariaService

        if not DiariasEtapa.query.get(1):
            db_session.add(DiariasEtapa(id=1, nome="Solicitação Inicial", alias="solicitacao_inicial", ordem=1))
        if not DiariasTipoItinerario.query.get(1):
            db_session.add(DiariasTipoItinerario(id=1, nome="Estadual"))
        if not DiariasTipoSolicitacao.query.get(1):
            db_session.add(DiariasTipoSolicitacao(id=1, nome="Apenas Diárias"))
        if not DiariasStatusViagem.query.get(1):
            db_session.add(DiariasStatusViagem(id=1, nome="Gerado"))
        cargo = DiariasCargo(nome="Assessor")
        db_session.add(cargo)
        db_session.flush()
        db_session.add(DiariasValorCargo(cargo_id=cargo.id, tipo_itinerario_id=1, valor="120.00"))
        db_session.flush()

        itinerario = DiariaService.criar_itinerario(
            {
                "tipo_solicitacao_id": 1,
                "tipo_itinerario": 1,
                "data_viagem": "2026-06-01T08:00",
                "data_retorno": "2026-06-03T18:00",
                "usuario_gerador": "teste",
                "estado_origem": 22,
                "estado_destino": 22,
                "objetivo": "Participa├º├úo no F├│rum",
                "unidade_sei_id": "110006213",
                "unidade_sei_sigla": "SEADPREV-PI/CPAG",
                "unidade_sei_descricao": "Coordena├º├úo de Pagamentos",
            },
            [{"cpf": "00000000001", "nome": "TESTE", "cargo_id": cargo.id}],
            None,
            "Justificativa t├®cnica",
        )

        assert itinerario.objetivo == "Participação no Fórum"
        assert itinerario.unidade_geradora_descricao == "Coordenação de Pagamentos"
        assert itinerario.justificativa.descricao == "Justificativa técnica"


def test_fluxo_stepper_tem_cartoes_de_autorizacao_separados():
    template = Path("app/templates/diarias/partials/fluxo_stepper.html").read_text(encoding="utf-8")

    assert 'class="fluxo-approval-grid"' in template
    assert template.count('class="fluxo-approval-card') >= 2
