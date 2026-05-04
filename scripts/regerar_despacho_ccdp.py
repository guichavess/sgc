"""
Regenera o Despacho CCDP de um processo de Diarias via backend.

Util para validar mudancas no template do despacho (ex: novo corpo com
links clicaveis para as Notas de Empenho).

Uso:
    python scripts/regerar_despacho_ccdp.py 00002.003853/2026-21
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasItinerario, DiariasDocumentoSei
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, enviar_procedimento, gerar_despacho_ccdp,
    UNIDADE_CCDP, UNIDADE_APOIOSGA,
)
from app.services.sei_integration import assinar_documento
from app.services.sei_auth import autenticar_usuario_sei


def regerar(n_processo):
    itin = DiariasItinerario.query.filter_by(n_processo=n_processo).first()
    if not itin:
        print(f'[X] Processo {n_processo} nao encontrado.')
        return False

    print(f'Itin {itin.id} | etapa={itin.etapa_atual_id} | sei_id_proc={itin.sei_id_procedimento}')

    # Limpar registro local do despacho_ccdp para nao bloquear regeneracao
    doc_old = itin.get_doc('despacho_ccdp')
    if doc_old:
        print(f'  Despacho CCDP anterior: sei_id={doc_old.sei_id} formatado={doc_old.sei_formatado}')
        doc_db = DiariasDocumentoSei.query.filter_by(
            itinerario_id=itin.id, tipo_documento='despacho_ccdp'
        ).first()
        if doc_db:
            db.session.delete(doc_db)
            db.session.commit()
            print('  [OK] Registro local removido.')
        # Limpar cache
        itin._docs_cache.pop('despacho_ccdp', None)

    # 1. Token admin
    token_admin = gerar_token_sei_admin()
    if not token_admin:
        print('[X] Falha ao obter token admin SEI.')
        return False
    print('  [OK] Token admin obtido.')

    sei_protocolo = itin.sei_protocolo or itin.n_processo or ''

    # 2. Enviar processo para CCDP
    enviar_procedimento(token_admin, sei_protocolo, [UNIDADE_CCDP], manter_aberto=True)
    print(f'  [OK] Processo enviado para CCDP ({UNIDADE_CCDP}).')

    # 3. Autenticacao com bypass por protocolo (processo teste)
    auth = autenticar_usuario_sei('bypass', 'bypass', protocolo_bypass=sei_protocolo)
    if not auth or not auth.get('token'):
        print(f'[X] Falha ao autenticar (bypass). auth={auth}')
        return False
    print('  [OK] Autenticacao bypass OK.')

    # 4. Gerar despacho (com novo corpo: links clicaveis para NEs)
    retorno = gerar_despacho_ccdp(
        token=token_admin,
        id_procedimento=itin.sei_id_procedimento,
        sei_protocolo=sei_protocolo,
        itinerario=itin,
    )
    if not retorno:
        print('[X] Falha ao gerar despacho CCDP.')
        return False

    doc_id = str(retorno.get('IdDocumento', ''))
    doc_formatado = retorno.get('DocumentoFormatado', '')
    print(f'  [OK] Despacho gerado: sei_id={doc_id} | formatado={doc_formatado}')

    # 5. Assinar
    res_assin = assinar_documento(
        token=auth['token'],
        unidade_id=UNIDADE_CCDP,
        dados_assinatura={
            'protocolo_doc': doc_id,
            'orgao': 'SEAD-PI',
            'cargo': 'Coordenador',
            'id_login': auth['id_login'],
            'id_usuario': auth['id_usuario'],
            'senha': 'bypass',
        },
        protocolo_proc=sei_protocolo,
    )
    if not res_assin or not res_assin.get('sucesso'):
        print(f'  [!] Assinatura falhou: {res_assin}')
        # Salva o doc mesmo sem assinatura
        itin.set_doc('despacho_ccdp', sei_id=doc_id, sei_formatado=doc_formatado)
        db.session.commit()
        return False
    print('  [OK] Documento assinado.')

    # 6. Enviar procedimento para APOIOSGA
    envio = enviar_procedimento(
        token=token_admin,
        protocolo_procedimento=sei_protocolo,
        unidades_destino=[UNIDADE_APOIOSGA],
        unidade_origem=UNIDADE_CCDP,
    )
    print(f'  [OK] Processo enviado para APOIOSGA ({UNIDADE_APOIOSGA}). resp={envio}')

    # 7. Salvar referencia
    itin.set_doc('despacho_ccdp', sei_id=doc_id, sei_formatado=doc_formatado)
    db.session.commit()
    print(f'\n=== Concluido ===')
    print(f'Novo despacho CCDP: {doc_formatado} (sei_id={doc_id})')
    print(f'Link SEI: https://sei.pi.gov.br/sei/controlador.php?acao=protocolo_visualizar'
          f'&id_protocolo={doc_id}&infra_sistema=100000100&infra_unidade_atual=110006213')
    return True


def main():
    if len(sys.argv) < 2:
        print('Uso: python scripts/regerar_despacho_ccdp.py <n_processo>')
        sys.exit(1)
    n_processo = sys.argv[1]
    app = create_app()
    with app.app_context():
        ok = regerar(n_processo)
        sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
