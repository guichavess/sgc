"""Verifica (READ-ONLY) o estado de autorizacao de uma diaria.

Uso:
    python scripts/verificar_autorizacao_diaria.py 00002.004523/2026-52

Saida: relatorio com:
  1. Itinerario (existencia, etapa, tipo, valor)
  2. Integrantes (CPF, nome, cargo)
  3. Documentos da etapa 1 (memorando, requisicao, autorizacao, assinaturas)
  4. Usuarios autorizadores cadastrados (secretario, sec.exercicio, superintendente)
  5. Resultado de get_nivel_autorizacao() — qual nivel o sistema computaria
  6. Flag superintendente_assinou
  7. Historico de movimentacao recente

Nenhuma alteracao e feita no banco. Somente SELECTs.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from app import create_app
from app.extensions import db
from app.models.diaria import (
    DiariasItinerario,
    DiariasItemItinerario,
    DiariasDocumentoSei,
    DiariasHistoricoMovimentacao,
)
from app.models.usuario import Usuario
from app.services.diarias_autorizacao import (
    get_nivel_autorizacao,
    superintendente_dispensado,
)


def _hr(titulo):
    print()
    print('=' * 78)
    print(f' {titulo}')
    print('=' * 78)


def _so_digitos(valor):
    if not valor:
        return ''
    return ''.join(c for c in str(valor) if c.isdigit())


def main(protocolo):
    app = create_app()
    with app.app_context():
        _hr(f'VERIFICACAO READ-ONLY — {protocolo}')

        # 1. Itinerario
        it = DiariasItinerario.query.filter(
            db.or_(
                DiariasItinerario.n_processo == protocolo,
                DiariasItinerario.sei_protocolo == protocolo,
            )
        ).first()

        if not it:
            print(f'[X] Processo {protocolo!r} nao encontrado em diarias_itinerario.')
            return 1

        _hr('1. ITINERARIO')
        print(f'  ID interno...........: {it.id}')
        print(f'  sei_protocolo........: {it.sei_protocolo}')
        print(f'  n_processo...........: {it.n_processo}')
        print(f'  sei_id_procedimento..: {it.sei_id_procedimento}')
        print(f'  tipo_solicitacao_id..: {it.tipo_solicitacao_id} (1=Apenas Diarias, 2=Nacional, 3=Internacional)')
        print(f'  tipo_itinerario......: {it.tipo_itinerario}')
        print(f'  status_id............: {it.status_id}')
        print(f'  etapa_atual_id.......: {it.etapa_atual_id} (esperado=1 para autorizacao pendente)')
        print(f'  valor_total..........: R$ {it.valor_total}')
        print(f'  data_solicitacao.....: {it.data_solicitacao}')
        print(f'  data_viagem..........: {it.data_viagem}')
        print(f'  superintendente_assinou.....: {it.superintendente_assinou}')
        print(f'  superintendente_assinou_data: {it.superintendente_assinou_data}')

        # 2. Integrantes
        _hr('2. INTEGRANTES (diarias_itens_itinerario)')
        itens = it.itens.all()
        if not itens:
            print('  (nenhum integrante cadastrado)')
        else:
            for i in itens:
                print(f'  - CPF {i.cpf_pessoa!r:>16} | {i.nome_pessoa or "(sem nome)"} | '
                      f'cargo_id={i.cargo_id}')
        cpfs_integrantes = {(i.cpf_pessoa or '').strip() for i in itens if i.cpf_pessoa}
        cpfs_integrantes_normalizados = {_so_digitos(c) for c in cpfs_integrantes if c}

        # 3. Documentos da etapa 1
        _hr('3. DOCUMENTOS (diarias_itinerario_documentos)')
        docs = DiariasDocumentoSei.query.filter_by(itinerario_id=it.id).all()
        if not docs:
            print('  (nenhum documento registrado)')
        else:
            for d in sorted(docs, key=lambda x: x.tipo_documento):
                print(f'  - tipo={d.tipo_documento:<25} | sei_id={d.sei_id} | '
                      f'sei_formatado={d.sei_formatado} | assinado={d.assinado} | '
                      f'data_criacao={d.data_criacao}')

        tipos_etapa1 = ('memorando', 'requisicao', 'requisicao_passagens', 'autorizacao')
        print()
        print('  Status dos documentos esperados na Etapa 1:')
        for tipo in tipos_etapa1:
            doc_match = next((d for d in docs if d.tipo_documento == tipo), None)
            if doc_match:
                marker = '[OK]' if doc_match.sei_id else '[!?]'
                print(f'    {marker} {tipo:<25} sei_id={doc_match.sei_id} assinado={doc_match.assinado}')
            else:
                print(f'    [--] {tipo:<25} (nao registrado)')

        # 4. Usuarios autorizadores
        _hr('4. USUARIOS AUTORIZADORES (sis_usuarios)')
        cargos = ('secretario', 'secretario_exercicio', 'superintendente')
        autorizadores = Usuario.query.filter(
            Usuario.cargo_gestao.in_(cargos),
            Usuario.ativo == True,  # noqa: E712
        ).all()

        if not autorizadores:
            print('  [X] NENHUM usuario com cargo_gestao autorizador esta cadastrado/ativo.')
        else:
            for u in sorted(autorizadores, key=lambda x: x.cargo_gestao):
                cpf_norm = _so_digitos(u.cpf)
                conflito = '<-- INTEGRANTE' if cpf_norm in cpfs_integrantes_normalizados else ''
                print(f'  - id={u.id:<5} cargo_gestao={u.cargo_gestao:<22} '
                      f'nome={u.nome[:40]:<40} cpf={u.cpf!r:<18} '
                      f'sigla={u.sigla_login!r:<35} {conflito}')

        cargos_presentes = {u.cargo_gestao for u in autorizadores}
        for esperado in cargos:
            if esperado not in cargos_presentes:
                print(f'  [!] AUSENTE: nenhum usuario ativo com cargo_gestao={esperado!r}')

        # 5. get_nivel_autorizacao
        _hr('5. RESULTADO DE get_nivel_autorizacao()')
        try:
            nivel_info = get_nivel_autorizacao(it)
            print(f'  nivel computado.....: {nivel_info["nivel"]}')
            print(f'  motivo_escalada.....: {nivel_info["motivo_escalada"]}')
            print(f'  autorizadores ativos:')
            if not nivel_info['autorizadores']:
                print(f'    [X] LISTA VAZIA — sistema computou nivel {nivel_info["nivel"]} mas '
                      f'nao ha usuario ativo com esse cargo. Autorizacao ficaria bloqueada.')
            for u in nivel_info['autorizadores']:
                print(f'    - id={u.id} nome={u.nome} sigla={u.sigla_login}')
        except Exception as e:
            print(f'  [X] erro ao computar nivel: {e!r}')

        try:
            disp = superintendente_dispensado(it)
            print(f'  superintendente_dispensado_da_pre_assinatura: {disp}')
        except Exception as e:
            print(f'  [X] erro em superintendente_dispensado: {e!r}')

        # 6. Historico
        _hr('6. HISTORICO DE MOVIMENTACAO (ultimas 10)')
        historico = (
            DiariasHistoricoMovimentacao.query
            .filter_by(id_itinerario=it.id)
            .order_by(DiariasHistoricoMovimentacao.data_movimentacao.desc())
            .limit(10)
            .all()
        )
        if not historico:
            print('  (sem registros)')
        else:
            for h in historico:
                comentario = (h.comentario or '')[:80]
                print(f'  - {h.data_movimentacao} | etapa {h.id_etapa_anterior} -> {h.id_etapa_nova} | '
                      f'usuario={h.id_usuario_responsavel} | {comentario}')

        # 7. Diagnostico final
        _hr('7. DIAGNOSTICO')
        problemas = []
        if it.etapa_atual_id != 1:
            problemas.append(f'etapa_atual_id={it.etapa_atual_id} (esperado=1 para autorizacao em aberto)')
        if 'secretario' not in cargos_presentes:
            problemas.append('falta usuario ativo com cargo_gestao=secretario (titular)')
        if 'secretario_exercicio' not in cargos_presentes:
            problemas.append('falta usuario ativo com cargo_gestao=secretario_exercicio')
        if 'superintendente' not in cargos_presentes:
            problemas.append('falta usuario ativo com cargo_gestao=superintendente')

        try:
            nivel_info = get_nivel_autorizacao(it)
            if nivel_info['nivel'] == 2 and not nivel_info['autorizadores']:
                problemas.append('nivel computado=2 mas sem autorizador (Sec.Exercicio nao cadastrado)')
            if nivel_info['nivel'] == 3 and not nivel_info['autorizadores']:
                problemas.append('nivel computado=3 mas sem autorizador (Superintendente nao cadastrado)')
        except Exception:
            pass

        if not problemas:
            print('  [OK] Nenhum problema detectado. Estado coerente para autorizacao.')
        else:
            print('  [!] Problemas detectados:')
            for p in problemas:
                print(f'    - {p}')

        return 0


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python scripts/verificar_autorizacao_diaria.py <protocolo_sei>')
        print('Ex.: python scripts/verificar_autorizacao_diaria.py 00002.004523/2026-52')
        sys.exit(1)
    sys.exit(main(sys.argv[1]) or 0)
