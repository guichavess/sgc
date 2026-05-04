"""
Simula, via backend, a autorização do Secretário no processo teste.

Uso exclusivo para o processo teste 00002.003853/2026-21 (tipo 1 — Apenas Diárias)
onde não temos credenciais reais de assinantes. Reproduz as ações da rota
`autorizar_solicitacao` (app/diarias/routes/admin.py) mas sem precisar de
request HTTP nem login.

Para tipo 1, NÃO gera documento 574 (SEAD_AUTORIZAÇÃO_DO_SECRETÁRIO). A
autorização é materializada apenas avançando a etapa + enviando o processo
SEI para DFIN/APOIO.

Uso:
    python scripts/simular_autorizacao_secretario.py
    python scripts/simular_autorizacao_secretario.py --protocolo 00002.003853/2026-21
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasItinerario
from app.constants import DiariasEtapaID, protocolo_tem_bypass_assinatura
from app.services.diaria_service import DiariaService
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, enviar_procedimento, UNIDADE_DFIN_APOIO,
)


PROTOCOLO_PADRAO = '00002.003853/2026-21'


def simular_autorizacao(protocolo):
    """Simula autorização do Secretário para um processo.

    Ações:
    1. Valida que o processo está na etapa 1 + superintendente_assinou=True.
    2. Valida que o processo está na lista de bypass (segurança extra).
    3. Avança etapa para ANALISE_SOLICITACAO (3).
    4. Envia processo no SEI para DFIN/APOIO.
    """
    itinerario = DiariasItinerario.query.filter(
        db.or_(
            DiariasItinerario.sei_protocolo == protocolo,
            DiariasItinerario.n_processo == protocolo,
        )
    ).first()

    if not itinerario:
        print(f'[ERRO] Processo {protocolo!r} não encontrado no banco.')
        return False

    print(f'[--] Itinerário #{itinerario.id} (protocolo: {itinerario.sei_protocolo})')
    print(f'     Tipo solicitação: {itinerario.tipo_solicitacao_id} (1=Apenas Diárias)')
    print(f'     Etapa atual: {itinerario.etapa_atual_id}')
    print(f'     Super assinou: {itinerario.superintendente_assinou}')

    # Guards
    if not protocolo_tem_bypass_assinatura(itinerario.sei_protocolo):
        print(
            f'[ERRO] Protocolo {itinerario.sei_protocolo!r} NÃO está em '
            'DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS. Este script é apenas para '
            'processos de teste. Adicione ao constants.py se necessário.'
        )
        return False

    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_INICIAL:
        print(
            f'[ERRO] Etapa atual é {itinerario.etapa_atual_id} — só pode autorizar '
            f'na etapa {int(DiariasEtapaID.SOLICITACAO_INICIAL)} (Solicitação Inicial).'
        )
        return False

    if not itinerario.superintendente_assinou:
        print('[ERRO] Superintendente ainda não assinou. Assine antes de autorizar.')
        return False

    if not itinerario.sei_id_procedimento:
        print('[ERRO] Processo sem sei_id_procedimento — não é possível encaminhar no SEI.')
        return False

    # 1. Avança etapa: SOLICITACAO_INICIAL → ANALISE_SOLICITACAO
    comentario = (
        f'Autorização do Secretário simulada via backend (bypass por protocolo). '
        f'Processo avança para Análise 1ª Parte.'
    )
    DiariaService.registrar_movimentacao(
        id_itinerario=itinerario.id,
        etapa_nova_id=DiariasEtapaID.ANALISE_SOLICITACAO,
        usuario_id=None,
        comentario=comentario,
    )
    db.session.commit()
    print(f'[OK] Etapa avançada para {int(DiariasEtapaID.ANALISE_SOLICITACAO)} (ANALISE_SOLICITACAO).')

    # 2. Envia processo SEI para DFIN/APOIO
    token_admin = gerar_token_sei_admin()
    if not token_admin:
        print('[AVISO] Falha ao obter token admin SEI. Processo avançou localmente, mas não foi encaminhado no SEI.')
        return True

    try:
        envio = enviar_procedimento(
            token_admin,
            itinerario.sei_protocolo,
            [UNIDADE_DFIN_APOIO],
            manter_aberto=True,
        )
        if envio and envio.get('sucesso'):
            print(f'[OK] Processo {itinerario.sei_protocolo} encaminhado ao DFIN/APOIO ({UNIDADE_DFIN_APOIO}).')
        else:
            erro = envio.get('erro', 'Sem resposta') if envio else 'Sem resposta'
            print(f'[AVISO] Processo avançou localmente, mas encaminhamento no SEI falhou: {erro}')
    except Exception as e:
        print(f'[AVISO] Erro ao encaminhar no SEI: {e}')

    return True


def main():
    parser = argparse.ArgumentParser(
        description='Simula a autorização do Secretário para um processo de teste.'
    )
    parser.add_argument(
        '--protocolo', default=PROTOCOLO_PADRAO,
        help=f'Protocolo SEI do processo (padrão: {PROTOCOLO_PADRAO})',
    )
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        print('=== Simular Autorização do Secretário (backend) ===\n')
        ok = simular_autorizacao(args.protocolo)
        print('\n=== Concluído! ===' if ok else '\n=== Falhou. ===')


if __name__ == '__main__':
    main()
