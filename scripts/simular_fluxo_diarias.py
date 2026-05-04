"""
CLI para simular, via backend, as assinaturas/etapas de um processo de diárias.

Uso EXCLUSIVO para processos de teste que estejam na lista
`DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS` (app/constants.py). Em processos reais,
o script aborta com erro — evitando uso indevido.

Subcomandos disponíveis:

    status        Mostra o estado atual do processo e a próxima ação sugerida.
    next          Executa automaticamente a próxima ação do fluxo.
    secretario    Simula a autorização do Secretário (etapa 1 → 3).
    dfin          Simula o despacho do Diretor DFIN (gera 754, envia para GPO).
    quadro        Simula a geração + assinatura do Quadro Orçamentário (723) pela GPO.

Exemplos:
    python scripts/simular_fluxo_diarias.py status
    python scripts/simular_fluxo_diarias.py next
    python scripts/simular_fluxo_diarias.py secretario --protocolo 00002.003853/2026-21
    python scripts/simular_fluxo_diarias.py dfin
    python scripts/simular_fluxo_diarias.py quadro

Sempre usa o protocolo padrão (00002.003853/2026-21) se `--protocolo` não for informado.
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.diaria import (
    DiariasItinerario, DiariasItemItinerario, DiariasNotaReserva,
)
from app.constants import (
    DiariasEtapaID, protocolo_tem_bypass_assinatura,
)
from app.services.diaria_service import DiariaService
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, enviar_procedimento,
    gerar_despacho_dfin, gerar_quadro_orcamentario,
    UNIDADE_DFIN_APOIO, UNIDADE_GPO,
)
from app.services.sei_integration import assinar_documento


PROTOCOLO_PADRAO = '00002.003853/2026-21'


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def carregar_itinerario(protocolo):
    """Carrega o itinerário e valida que está na lista de bypass."""
    it = DiariasItinerario.query.filter(
        db.or_(
            DiariasItinerario.sei_protocolo == protocolo,
            DiariasItinerario.n_processo == protocolo,
        )
    ).first()
    if not it:
        raise RuntimeError(f'Processo {protocolo!r} não encontrado.')

    if not protocolo_tem_bypass_assinatura(it.sei_protocolo):
        raise RuntimeError(
            f'Protocolo {it.sei_protocolo!r} NÃO está em DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS. '
            'Este script é apenas para processos de teste.'
        )
    return it


def _assinar_bypass(doc_id, unidade_id, cargo, protocolo_proc):
    """Executa assinatura via bypass (sem credenciais reais)."""
    return assinar_documento(
        token='bypass-token',
        unidade_id=unidade_id,
        dados_assinatura={
            'protocolo_doc': doc_id,
            'orgao': 'SEAD-PI',
            'cargo': cargo,
            'id_login': 'bypass-login',
            'id_usuario': 'bypass-user',
            'senha': 'bypass',
        },
        protocolo_proc=protocolo_proc,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Subcomandos
# ─────────────────────────────────────────────────────────────────────────────

def cmd_status(protocolo):
    """Mostra estado atual + próxima ação sugerida."""
    it = carregar_itinerario(protocolo)

    print(f'=== Processo {it.sei_protocolo} (id={it.id}) ===')
    print(f'  tipo_solicitacao_id:    {it.tipo_solicitacao_id} (1=Apenas Diárias)')
    print(f'  etapa_atual_id:         {it.etapa_atual_id}')
    print(f'  superintendente_assinou: {it.superintendente_assinou}')
    print(f'  sei_id_procedimento:    {it.sei_id_procedimento}')

    # Documentos já registrados
    from app.models.diaria import DiariasDocumentoSei
    docs = DiariasDocumentoSei.query.filter_by(itinerario_id=it.id).all()
    print(f'\n  Documentos SEI vinculados ({len(docs)}):')
    for d in docs:
        print(f'    - {d.tipo_documento:<25} {d.sei_formatado or d.sei_id or "-"}')

    # NRs por servidor
    total_servidores = DiariasItemItinerario.query.filter_by(id_itinerario=it.id).count()
    nrs = DiariasNotaReserva.query.filter_by(itinerario_id=it.id).count()
    print(f'\n  Notas de Reserva: {nrs}/{total_servidores} servidor(es) com NR')

    # Quadro
    print(f'  Quadro Orçamentário: {"preenchido" if it.quadro_orcamentario and it.quadro_orcamentario.ug else "não preenchido"}')

    # Próxima ação
    print('\n  Próxima ação sugerida:')
    proxima = _proxima_acao(it)
    if proxima:
        print(f'    -> {proxima}')
    else:
        print('    (fluxo completo ou etapa não automatizável pelo CLI ainda)')


def _proxima_acao(it):
    """Retorna string descritiva da próxima ação automatizável."""
    if it.etapa_atual_id == DiariasEtapaID.SOLICITACAO_INICIAL:
        if not it.superintendente_assinou:
            return 'Superintendente deve assinar primeiro (via UI — Aprovar Solicitações)'
        return 'secretario — simular autorização do Secretário'

    if it.etapa_atual_id == DiariasEtapaID.ANALISE_SOLICITACAO:
        if not it.has_doc('despacho_dfin'):
            return 'dfin — simular despacho do Diretor DFIN'

        # Verificar NRs
        total_servidores = DiariasItemItinerario.query.filter_by(id_itinerario=it.id).count()
        nrs = DiariasNotaReserva.query.filter_by(itinerario_id=it.id).count()
        if nrs < total_servidores:
            return f'Inserir Notas de Reserva via UI — {nrs}/{total_servidores} cadastradas'

        if not (it.quadro_orcamentario and it.quadro_orcamentario.ug):
            return 'quadro — simular geração do Quadro Orçamentário'

        return '(próxima etapa: Despacho GEO / Análise de Habilitação — ainda não automatizado)'

    return None


def cmd_next(protocolo):
    """Executa automaticamente a próxima ação."""
    it = carregar_itinerario(protocolo)

    if it.etapa_atual_id == DiariasEtapaID.SOLICITACAO_INICIAL:
        if not it.superintendente_assinou:
            print('[ERRO] Superintendente precisa assinar primeiro (via UI).')
            return False
        return cmd_secretario(protocolo)

    if it.etapa_atual_id == DiariasEtapaID.ANALISE_SOLICITACAO:
        if not it.has_doc('despacho_dfin'):
            return cmd_dfin(protocolo)

        total_servidores = DiariasItemItinerario.query.filter_by(id_itinerario=it.id).count()
        nrs = DiariasNotaReserva.query.filter_by(itinerario_id=it.id).count()
        if nrs < total_servidores:
            print(f'[INFO] Faltam {total_servidores - nrs} NRs. Insira via UI (/financeiro/diarias/{it.id}).')
            return False

        if not (it.quadro_orcamentario and it.quadro_orcamentario.ug):
            print('[INFO] Quadro não preenchido. Use a UI ou o subcomando `quadro` com valores.')
            return False

    print('[--] Nenhuma ação automatizável pendente para o estado atual.')
    return False


def cmd_secretario(protocolo):
    """Simula autorização do Secretário: avança etapa 1 → 3 + envia para DFIN/APOIO."""
    it = carregar_itinerario(protocolo)

    if it.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIAL:
        print(f'[ERRO] Etapa atual é {it.etapa_atual_id} — não está em Solicitação Inicial.')
        return False
    if not it.superintendente_assinou:
        print('[ERRO] Superintendente ainda não assinou.')
        return False
    if not it.sei_id_procedimento:
        print('[ERRO] Processo sem sei_id_procedimento.')
        return False

    # 1. Avança etapa
    DiariaService.registrar_movimentacao(
        id_itinerario=it.id,
        etapa_nova_id=DiariasEtapaID.ANALISE_SOLICITACAO,
        usuario_id=None,
        comentario='Autorização do Secretário simulada via backend (bypass por protocolo).',
    )
    db.session.commit()
    print(f'[OK] Etapa avançada: {int(DiariasEtapaID.SOLICITACAO_INICIAL)} → {int(DiariasEtapaID.ANALISE_SOLICITACAO)}')

    # 2. Envia para DFIN/APOIO
    token = gerar_token_sei_admin()
    if token:
        envio = enviar_procedimento(token, it.sei_protocolo, [UNIDADE_DFIN_APOIO], manter_aberto=True)
        if envio and envio.get('sucesso'):
            print(f'[OK] Processo encaminhado ao DFIN/APOIO ({UNIDADE_DFIN_APOIO}).')
        else:
            erro = envio.get('erro', 'Sem resposta') if envio else 'Sem resposta'
            print(f'[AVISO] Falha no encaminhamento SEI: {erro}')
    else:
        print('[AVISO] Token admin indisponível — processo avançou local mas não foi encaminhado no SEI.')

    return True


def cmd_dfin(protocolo):
    """Simula despacho do Diretor DFIN: gera SEAD_DESPACHO (754), assina, envia para GPO."""
    it = carregar_itinerario(protocolo)

    if it.etapa_atual_id != DiariasEtapaID.ANALISE_SOLICITACAO:
        print(f'[ERRO] Etapa atual é {it.etapa_atual_id} — DFIN só atua em ANALISE_SOLICITACAO (3).')
        return False
    if it.has_doc('despacho_dfin'):
        doc = it.get_doc('despacho_dfin')
        print(f'[--] Despacho DFIN já existe: {doc.sei_formatado or doc.sei_id}')
        return True
    if not it.sei_id_procedimento:
        print('[ERRO] Processo sem sei_id_procedimento.')
        return False

    token = gerar_token_sei_admin()
    if not token:
        print('[ERRO] Falha ao obter token admin SEI.')
        return False

    # 1. Gerar despacho DFIN (série 754)
    itens = DiariasItemItinerario.query.filter_by(id_itinerario=it.id).all()
    nomes = [i.nome_pessoa for i in itens if i.nome_pessoa]

    # nome_assinante/cargo_assinante intencionalmente omitidos —
    # gerar_despacho_dfin resolve o titular DFIN automaticamente
    ret = gerar_despacho_dfin(
        token=token,
        id_procedimento=it.sei_id_procedimento,
        sei_protocolo=it.sei_protocolo,
        interessados=nomes,
    )
    if not ret:
        print('[ERRO] Falha ao gerar despacho DFIN no SEI.')
        return False

    doc_id = str(ret.get('IdDocumento', ''))
    doc_fmt = ret.get('DocumentoFormatado', '')
    print(f'[OK] Despacho DFIN gerado: {doc_fmt} (id={doc_id})')

    # 2. Assinar (bypass)
    ret_ass = _assinar_bypass(
        doc_id=doc_id,
        unidade_id=UNIDADE_DFIN_APOIO,
        cargo='Diretor de Planejamento e Finanças da SEAD-PI',
        protocolo_proc=it.sei_protocolo,
    )
    if ret_ass and ret_ass.get('sucesso'):
        print(f'[OK] Despacho assinado (bypass).')
    else:
        erro = ret_ass.get('erro', '') if ret_ass else 'sem resposta'
        print(f'[AVISO] Assinatura retornou erro: {erro}')

    # 3. Salvar no banco
    it.set_doc('despacho_dfin', sei_id=doc_id, sei_formatado=doc_fmt)
    db.session.commit()
    print('[OK] Documento registrado no banco.')

    # 4. Enviar para GPO
    envio = enviar_procedimento(token, it.sei_protocolo, [UNIDADE_GPO], manter_aberto=True)
    if envio and envio.get('sucesso'):
        print(f'[OK] Processo encaminhado à GPO ({UNIDADE_GPO}).')
    else:
        erro = envio.get('erro', 'Sem resposta') if envio else 'Sem resposta'
        print(f'[AVISO] Falha no encaminhamento à GPO: {erro}')

    return True


def cmd_quadro(protocolo, args):
    """Simula geração do Quadro Orçamentário pela GPO."""
    from app.models.diaria import DiariasQuadroOrcamentario
    from decimal import Decimal

    it = carregar_itinerario(protocolo)

    if it.etapa_atual_id != DiariasEtapaID.ANALISE_SOLICITACAO:
        print(f'[ERRO] Etapa atual {it.etapa_atual_id} — precisa estar em ANALISE_SOLICITACAO.')
        return False

    # Gate: NR de todos os servidores
    total_servidores = DiariasItemItinerario.query.filter_by(id_itinerario=it.id).count()
    nrs = DiariasNotaReserva.query.filter_by(itinerario_id=it.id).count()
    if nrs < total_servidores:
        print(f'[ERRO] Faltam NRs: {nrs}/{total_servidores} servidor(es).')
        return False

    if it.quadro_orcamentario and it.quadro_orcamentario.ug:
        print(f'[--] Quadro já preenchido (UG={it.quadro_orcamentario.ug}).')
        return True

    # Valores padrão para o teste
    dados_quadro = {
        'ug': args.ug,
        'funcao': args.funcao,
        'subfuncao': args.subfuncao,
        'programa': args.programa,
        'plano_interno': args.plano_interno,
        'fonte_recursos': args.fonte,
        'natureza_despesa': args.natureza,
        'valor_inicial_nr': Decimal(args.valor_inicial) if args.valor_inicial else None,
        'saldo_nr': Decimal(args.saldo_nr) if args.saldo_nr else None,
        'valor_despesa': Decimal(args.valor_despesa),
        'saldo_atual_nr': Decimal(args.saldo_atual) if args.saldo_atual else None,
    }

    # 1. Salvar no banco
    if not it.quadro_orcamentario:
        it.quadro_orcamentario = DiariasQuadroOrcamentario(itinerario_id=it.id)
    for campo, valor in dados_quadro.items():
        setattr(it.quadro_orcamentario, campo, valor)
    db.session.commit()
    print('[OK] Quadro Orçamentário salvo no banco.')

    # 2. Gerar documento SEI (série 723)
    token = gerar_token_sei_admin()
    if not token:
        print('[AVISO] Token admin indisponível — quadro local salvo, mas documento SEI não foi gerado.')
        return True

    ret = gerar_quadro_orcamentario(
        token=token,
        id_procedimento=it.sei_id_procedimento,
        dados_quadro=dados_quadro,
        sei_protocolo=it.sei_protocolo,
    )
    if not ret:
        print('[AVISO] Geração do Quadro no SEI falhou.')
        return True

    doc_id = str(ret.get('IdDocumento', ''))
    doc_fmt = ret.get('DocumentoFormatado', '')
    print(f'[OK] Quadro Orçamentário gerado no SEI: {doc_fmt}')

    # 3. Assinar (bypass)
    ret_ass = _assinar_bypass(
        doc_id=doc_id,
        unidade_id=UNIDADE_GPO,
        cargo='Gerente de Planejamento e Orçamento',
        protocolo_proc=it.sei_protocolo,
    )
    if ret_ass and ret_ass.get('sucesso'):
        print('[OK] Quadro assinado (bypass).')
    else:
        erro = ret_ass.get('erro', '') if ret_ass else 'sem resposta'
        print(f'[AVISO] Assinatura retornou erro: {erro}')

    # 4. Registrar no banco
    it.set_doc('quadro_orcamentario', sei_id=doc_id, sei_formatado=doc_fmt)
    db.session.commit()
    print('[OK] Documento vinculado ao itinerário.')
    return True


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Simula assinaturas/etapas de um processo de diárias de teste.'
    )
    parser.add_argument('--protocolo', default=PROTOCOLO_PADRAO,
                        help=f'Protocolo SEI (padrão: {PROTOCOLO_PADRAO})')

    sub = parser.add_subparsers(dest='acao', required=True)

    sub.add_parser('status', help='Mostra estado atual do processo.')
    sub.add_parser('next', help='Executa a próxima ação do fluxo automaticamente.')
    sub.add_parser('secretario', help='Simula autorização do Secretário.')
    sub.add_parser('dfin', help='Simula despacho do Diretor DFIN.')

    p_quadro = sub.add_parser('quadro', help='Simula geração do Quadro Orçamentário.')
    p_quadro.add_argument('--ug', default='210101', help='Unidade Gestora')
    p_quadro.add_argument('--funcao', default='04', help='Função')
    p_quadro.add_argument('--subfuncao', default='122', help='Subfunção')
    p_quadro.add_argument('--programa', default='0016', help='Programa')
    p_quadro.add_argument('--plano-interno', default='0000', help='Plano Interno')
    p_quadro.add_argument('--fonte', default='100', help='Fonte de Recursos')
    p_quadro.add_argument('--natureza', default='339014', help='Natureza de Despesa')
    p_quadro.add_argument('--valor-inicial', default=None, help='Valor inicial NR (BRL)')
    p_quadro.add_argument('--saldo-nr', default=None, help='Saldo NR')
    p_quadro.add_argument('--valor-despesa', required=True, help='Valor da despesa (obrigatório)')
    p_quadro.add_argument('--saldo-atual', default=None, help='Saldo atual NR')

    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        print(f'=== Simular fluxo — processo {args.protocolo} ===\n')
        try:
            if args.acao == 'status':
                cmd_status(args.protocolo)
            elif args.acao == 'next':
                cmd_next(args.protocolo)
            elif args.acao == 'secretario':
                cmd_secretario(args.protocolo)
            elif args.acao == 'dfin':
                cmd_dfin(args.protocolo)
            elif args.acao == 'quadro':
                cmd_quadro(args.protocolo, args)
            else:
                print(f'[ERRO] Subcomando desconhecido: {args.acao}')
                sys.exit(1)
        except RuntimeError as e:
            print(f'[ERRO] {e}')
            sys.exit(1)


if __name__ == '__main__':
    main()
