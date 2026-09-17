"""
API da tela Criar em lote (POST /solicitacoes/api/criar-lote), com o SEI simulado.

Contrato com nova_lote.html: JSON {contratos, competencia, id_tipo_pagamento, unidade_id}
→ text/event-stream com eventos por contrato e um evento final {concluido, total_criados, total_erros}.
A tela só encerra o modal quando recebe `concluido`: todo caminho do stream precisa terminar nele.
"""
import json

import pytest

URL = '/solicitacoes/api/criar-lote'
API = 'app.solicitacoes.routes.api'
PERM_CRIAR = [('solicitacoes', '', 'criar')]
PROC = {
    'IdProcedimento': '555', 'ProcedimentoFormatado': '00002.000555/2026-55',
    'LinkAcesso': 'https://sei.pi.gov.br/p/555', 'EspecificacaoGerada': 'PAGAMENTO',
}


@pytest.fixture()
def dados(db_session):
    from app.models import Contrato, TipoPagamento

    db_session.add(TipoPagamento(id=1, nome='Regular'))
    for codigo, nome in (('22026000123', 'LIDERANCA LTDA'), ('22026000456', 'OUTRA EMPRESA SA')):
        db_session.add(Contrato(codigo=codigo, numeroOriginal='045/2025', nomeContratado=nome, objeto='Limpeza'))
    db_session.commit()


@pytest.fixture()
def cliente(dados, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7321, permissoes=PERM_CRIAR))
    with c.session_transaction() as sess:
        sess['sei_token'] = 'token-sessao'
    return c


@pytest.fixture()
def sei(monkeypatch):
    estado = {'criar': [], 'falhar': set(), 'explodir': set()}

    def criar(token, unidade_id, dados_contrato, competencia, *a, **k):
        codigo = dados_contrato['codigo']
        estado['criar'].append((token, unidade_id, codigo, competencia))
        if codigo in estado['explodir']:
            raise RuntimeError('SEI fora do ar')
        if codigo in estado['falhar']:
            return None
        proc = dict(PROC)
        proc['ProcedimentoFormatado'] = f'00002.{codigo[-6:]}/2026-55'
        return proc

    monkeypatch.setattr(f'{API}.criar_procedimento_pagamento', criar)
    monkeypatch.setattr(f'{API}.gerar_documento_pagamento', lambda *a, **k: {'DocumentoFormatado': '1'})
    monkeypatch.setattr(f'{API}.gerar_token_sei_admin', lambda: None)
    monkeypatch.setattr(f'{API}.SaldoService.registrar_e_atualizar_saldo', lambda *a, **k: None)
    return estado


def _payload(**extra):
    base = {'contratos': ['22026000123', '22026000456'], 'competencia': '08/2026',
            'id_tipo_pagamento': 1, 'unidade_id': '110000001'}
    base.update(extra)
    return base


def _eventos(resp):
    assert resp.status_code == 200
    assert resp.mimetype == 'text/event-stream'
    return [json.loads(l[6:]) for l in resp.get_data(as_text=True).split('\n') if l.startswith('data: ')]


def _solicitacoes():
    from app.models import Solicitacao
    return Solicitacao.query.order_by(Solicitacao.id).all()


# ─ acesso
def test_exige_login(client, dados):
    resp = client.post(URL, json=_payload())
    assert resp.status_code in (302, 401)
    assert not _solicitacoes()


def test_sem_permissao_criar_nao_cria(dados, sei, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7322, permissoes=[('solicitacoes', '', 'visualizar')]))
    resp = c.post(URL, json=_payload())
    assert resp.status_code in (302, 403)
    assert sei['criar'] == []


# ─ validação (400 antes de abrir o stream)
@pytest.mark.parametrize('extra', [
    {'contratos': []},
    {'contratos': '22026000123'},
    {'competencia': ''},
    {'competencia': None},
    {'competencia': '13/2026'},
    {'competencia': '2026-08'},
    {'unidade_id': ''},
    {'id_tipo_pagamento': None},
    {'id_tipo_pagamento': 'abc'},
])
def test_dados_invalidos_retornam_400(cliente, sei, extra):
    resp = cliente.post(URL, json=_payload(**extra))
    assert resp.status_code == 400
    assert resp.get_json()['sucesso'] is False
    assert resp.get_json()['erro']
    assert sei['criar'] == []


def test_corpo_nao_json_retorna_400(cliente, sei):
    resp = cliente.post(URL, data='x', content_type='text/plain')
    assert resp.status_code == 400


def test_unidade_numerica_e_aceita(cliente, sei):
    eventos = _eventos(cliente.post(URL, json=_payload(contratos=['22026000123'], unidade_id=110000001)))
    assert eventos[-1]['total_criados'] == 1
    assert sei['criar'][0][1] == '110000001'


# ─ caminho feliz
def test_cria_um_processo_por_contrato(cliente, sei):
    eventos = _eventos(cliente.post(URL, json=_payload()))

    assert [c[2] for c in sei['criar']] == ['22026000123', '22026000456']
    assert all(c[0] == 'token-sessao' and c[3] == '08/2026' for c in sei['criar'])

    por_contrato = [e for e in eventos if e.get('contrato')]
    assert [e['sucesso'] for e in por_contrato] == [True, True]
    assert por_contrato[0]['protocolo'] == '00002.000123/2026-55'
    assert por_contrato[0]['contratado'] == 'LIDERANCA LTDA'
    assert por_contrato[0]['link_sei'].startswith('https://')

    final = eventos[-1]
    assert final['concluido'] is True and final['progresso'] == 100
    assert (final['total_criados'], final['total_erros']) == (2, 0)

    sols = _solicitacoes()
    assert [s.codigo_contrato for s in sols] == ['22026000123', '22026000456']
    assert all(s.criado_em_lote and s.competencia == '08/2026' and s.id_tipo_pagamento == 1 for s in sols)


def test_progresso_cresce_ate_100(cliente, sei):
    progresso = [e['progresso'] for e in _eventos(cliente.post(URL, json=_payload()))]
    assert progresso == sorted(progresso) and progresso[-1] == 100


def test_contratos_repetidos_criam_um_processo_so(cliente, sei):
    eventos = _eventos(cliente.post(URL, json=_payload(contratos=['22026000123', '22026000123'])))
    assert len(sei['criar']) == 1
    assert eventos[-1]['total_criados'] == 1
    assert len(_solicitacoes()) == 1


# ─ falhas por contrato não param o lote
def test_contrato_inexistente_segue_para_o_proximo(cliente, sei):
    eventos = _eventos(cliente.post(URL, json=_payload(contratos=['99999999999', '22026000456'])))
    erro = next(e for e in eventos if e.get('contrato') == '99999999999')
    assert erro['sucesso'] is False
    assert (eventos[-1]['total_criados'], eventos[-1]['total_erros']) == (1, 1)


def test_falha_do_sei_conta_erro(cliente, sei):
    sei['falhar'].add('22026000123')
    eventos = _eventos(cliente.post(URL, json=_payload()))
    assert (eventos[-1]['total_criados'], eventos[-1]['total_erros']) == (1, 1)
    assert [s.codigo_contrato for s in _solicitacoes()] == ['22026000456']


def test_excecao_em_um_contrato_nao_derruba_o_lote(cliente, sei):
    sei['explodir'].add('22026000123')
    eventos = _eventos(cliente.post(URL, json=_payload()))
    erro = next(e for e in eventos if e.get('contrato') == '22026000123')
    assert erro['sucesso'] is False
    assert eventos[-1]['concluido'] is True
    assert (eventos[-1]['total_criados'], eventos[-1]['total_erros']) == (1, 1)


# ─ sem token do SEI: o stream também precisa terminar em `concluido`
def test_sem_token_sei_encerra_com_concluido(dados, sei, usuario_com_permissoes, logar):
    c = logar(usuario_com_permissoes(7323, permissoes=PERM_CRIAR))
    eventos = _eventos(c.post(URL, json=_payload()))
    assert sei['criar'] == []
    final = eventos[-1]
    assert final['concluido'] is True
    assert (final['total_criados'], final['total_erros']) == (0, 2)
    assert 'SEI' in final['msg']
