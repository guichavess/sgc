from datetime import datetime
from decimal import Decimal
from unittest.mock import patch


HTML_REQUISICAO = """
<!DOCTYPE html>
<html>
<head><title>REQUISIÇÃO DE DIÁRIAS Nº 77/2026 - SEI 00002.004241/2026-55</title></head>
<body>
<h2>REQUISIÇÃO DE DIÁRIAS Nº 77/2026</h2>
<p>Processo SEI 00002.004241/2026-55</p>
<table>
  <tr>
    <th>Matrícula</th>
    <th>Nome</th>
    <th>Cargo/Função</th>
    <th>Vínculo</th>
    <th>CPF</th>
    <th>Banco/Agência/Conta</th>
    <th>Quantidade de Diárias</th>
    <th>Valor Unitário</th>
    <th>Valor Total</th>
  </tr>
  <tr>
    <td>123456</td>
    <td>ANA TESTE DA SILVA</td>
    <td>ANALISTA</td>
    <td>Efetivo</td>
    <td>123.456.789-01</td>
    <td>AG. 0001 CONTA CORRENTE: 12345-6 BANCO DO BRASIL</td>
    <td>2,5</td>
    <td>R$ 320,00</td>
    <td>R$ 800,00</td>
  </tr>
  <tr>
    <td colspan="8">TRECHO: TERESINA-PI/SÃO PAULO-SP PERÍODO: 10/06/2026 a 12/06/2026</td>
  </tr>
  <tr>
    <td colspan="8">OBJETIVO DA VIAGEM: Participação em agenda institucional</td>
  </tr>
  <tr>
    <td colspan="7">VALOR TOTAL:</td>
    <td>R$ 800,00</td>
  </tr>
</table>
</body>
</html>
""".encode("iso-8859-1")


def _login_admin_com_token(client, db_session):
    import uuid
    from app.models.usuario import Usuario

    uid = uuid.uuid4().hex[:12]
    usuario = Usuario(
        id_usuario_sei=f"sync_{uid}",
        nome="Admin Sync",
        sigla_login=f"admin_sync_{uid}",
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()
    with client.session_transaction() as sess:
        sess["_user_id"] = str(usuario.id)
        sess["sei_token"] = "TOKEN_USUARIO"
    return usuario


def _docs_iniciais():
    return {
        "sucesso": True,
        "documentos": [
            {
                "IdDocumento": "DOC_MEMO",
                "DocumentoFormatado": "0020000001",
                "Serie": {"IdSerie": "2986", "Nome": "SEAD_MEMORANDO_SGA"},
            },
            {
                "IdDocumento": "DOC_REQ",
                "DocumentoFormatado": "0020000002",
                "Serie": {"IdSerie": "532", "Nome": "SEAD_REQUISIÇÃO DE DIÁRIAS"},
            },
            {
                "IdDocumento": "DOC_PASS",
                "DocumentoFormatado": "0020000003",
                "Serie": {"IdSerie": "2975", "Nome": "SEAD_REQUISIÇÃO_DE_PASSAGENS_AÉREAS"},
            },
        ],
        "erro": None,
    }


class TestSincronizarProcessosBlocoService:
    def test_cria_itinerario_para_processo_inexistente(self, app, db_session):
        from app.models.diaria import DiariasItinerario, DiariasItemItinerario
        from app.services.vincular_processo_diaria import sincronizar_processos_bloco_diarias

        with patch(
                 "app.services.vincular_processo_diaria.consultar_bloco_diarias",
                 return_value={
                     "sucesso": True,
                     "protocolos": [{"ProtocoloFormatado": "00002.004241/2026-55"}],
                     "erro": None,
                 },
             ), \
             patch(
                 "app.services.vincular_processo_diaria.consultar_procedimento_sei",
                 return_value={
                     "sucesso": True,
                     "protocolo_formatado": "00002.004241/2026-55",
                     "id_procedimento": "PROC123",
                     "link_acesso": "https://sei/pi/proc",
                     "especificacao": "DIÁRIAS TESTE",
                 },
             ), \
             patch(
                 "app.services.vincular_processo_diaria.listar_documentos_procedimento_sei",
                 return_value=_docs_iniciais(),
             ), \
             patch(
                 "app.services.vincular_processo_diaria.baixar_documento_sei",
                 return_value=HTML_REQUISICAO,
             ) as baixar:
            resultado = sincronizar_processos_bloco_diarias(
                token="TOKEN_USUARIO",
                usuario_id=1,
                usuario_gerador="admin_sync",
            )

        assert resultado["sucesso"] is True
        assert resultado["criados"] == 1
        it = db_session.query(DiariasItinerario).filter_by(sei_protocolo="00002.004241/2026-55").one()
        assert it.sei_id_procedimento == "PROC123"
        assert it.tipo_solicitacao_id == 2
        assert it.tipo_itinerario == 2
        assert it.objetivo == "Participação em agenda institucional"
        assert it.valor_total == Decimal("800.00")
        assert db_session.query(DiariasItemItinerario).filter_by(id_itinerario=it.id).count() == 1
        baixar.assert_called_once_with("0020000002", token="TOKEN_USUARIO", timeout=120)

    def test_ignora_processo_ja_existente(self, app, db_session):
        from app.models.diaria import DiariasItinerario
        from app.services.vincular_processo_diaria import sincronizar_processos_bloco_diarias

        existente = DiariasItinerario(
            usuario_gerador="existente",
            tipo_solicitacao_id=1,
            tipo_itinerario=1,
            status_id=1,
            data_solicitacao=datetime(2026, 5, 1),
            data_viagem=datetime(2026, 6, 1),
            data_retorno=datetime(2026, 6, 2),
            qtd_diarias_solicitadas=Decimal("1.0"),
            sei_protocolo="00002.004241/2026-55",
        )
        db_session.add(existente)
        db_session.flush()

        with patch(
                 "app.services.vincular_processo_diaria.consultar_bloco_diarias",
                 return_value={
                     "sucesso": True,
                     "protocolos": [{"ProtocoloFormatado": "00002.004241/2026-55"}],
                     "erro": None,
                 },
             ), \
             patch(
                 "app.services.vincular_processo_diaria.listar_documentos_procedimento_sei",
             ) as listar:
            resultado = sincronizar_processos_bloco_diarias(
                token="TOKEN_USUARIO",
                usuario_id=1,
                usuario_gerador="admin_sync",
            )

        assert resultado["sucesso"] is True
        assert resultado["existentes"] == 1
        assert db_session.query(DiariasItinerario).filter_by(sei_protocolo="00002.004241/2026-55").count() == 1
        listar.assert_not_called()


class TestSincronizarProcessosBlocoRota:
    def test_botao_aparece_na_administracao(self, client, app, db_session):
        with app.app_context():
            _login_admin_com_token(client, db_session)
            resp = client.get("/diarias/administracao")

        assert resp.status_code == 200
        html = resp.data.decode("utf-8", errors="replace")
        assert "Sincronizar Processos" in html

    def test_rota_usa_token_da_sessao(self, client, app, db_session):
        with app.app_context():
            _login_admin_com_token(client, db_session)
            with patch(
                "app.diarias.routes.admin.sincronizar_processos_bloco_diarias",
                return_value={"sucesso": True, "total": 1, "criados": 1, "existentes": 0, "erros": []},
            ) as sync:
                resp = client.post("/diarias/administracao/sincronizar-processos")

        assert resp.status_code in (302, 303)
        _, kwargs = sync.call_args
        assert kwargs["token"] == "TOKEN_USUARIO"
        assert kwargs["usuario_gerador"].startswith("admin_sync_")

    def test_rota_ajax_retorna_json_com_resumo(self, client, app, db_session):
        with app.app_context():
            _login_admin_com_token(client, db_session)
            with patch(
                "app.diarias.routes.admin.sincronizar_processos_bloco_diarias",
                return_value={
                    "sucesso": True,
                    "total": 3,
                    "criados": 2,
                    "existentes": 1,
                    "erros": [],
                    "msgs": ["Bloco processado."],
                },
            ):
                resp = client.post(
                    "/diarias/administracao/sincronizar-processos",
                    headers={"X-Requested-With": "XMLHttpRequest"},
                )

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["sucesso"] is True
        assert data["criados"] == 2
        assert data["existentes"] == 1

    def test_rota_ajax_precheck_retorna_qtd_novos(self, client, app, db_session):
        with app.app_context():
            _login_admin_com_token(client, db_session)
            with patch(
                "app.diarias.routes.admin.prever_processos_bloco_diarias",
                return_value={
                    "sucesso": True,
                    "total": 5,
                    "novos": 3,
                    "existentes": 2,
                    "erro": None,
                },
            ):
                resp = client.post(
                    "/diarias/administracao/sincronizar-processos",
                    data={"acao": "precheck"},
                    headers={"X-Requested-With": "XMLHttpRequest"},
                )

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["sucesso"] is True
        assert data["novos"] == 3
        assert data["existentes"] == 2

    def test_popup_de_sincronizacao_existe_na_pagina(self, client, app, db_session):
        with app.app_context():
            _login_admin_com_token(client, db_session)
            resp = client.get("/diarias/administracao")

        html = resp.data.decode("utf-8", errors="replace")
        assert "modalSincronizarProcessos" in html
        assert "Consultando bloco de processos" in html
        assert "sincronizarProcessosForm" in html
        assert "processos novos encontrados" in html
