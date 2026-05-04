"""
Gera os 3 despachos pos-Analise NCI (APOIO->DFIN, DIRETOR->GEO, GEO->CCDP)
para um processo de Diarias, apos bypass local da verificacao NCI.

Usado para destravar o fluxo quando nao e possivel criar o SINCIN/Despacho NCI
reais no SEI (ex: processos teste). Le o `analise_pagamento` e `despacho_nci`
gravados localmente (placeholders do bypass) e segue os passos 5-10 da rota
`confirmar_analise_nci` original.

Uso:
    python scripts/simular_despachos_pos_nci.py 00002.003853/2026-21
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasItinerario
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, enviar_procedimento,
    gerar_despacho_apoio, gerar_despacho_diretor, gerar_despacho_geo,
    UNIDADE_DFIN_APOIO, UNIDADE_GEO, UNIDADE_CCDP, UNIDADE_APOIOSGA,
)
from app.services.sei_integration import assinar_documento
from app.services.sei_auth import autenticar_usuario_sei


def _assinar(auth, unidade_id, doc_id, cargo, sei_protocolo):
    return assinar_documento(
        token=auth['token'],
        unidade_id=unidade_id,
        dados_assinatura={
            'protocolo_doc': doc_id,
            'orgao': 'SEAD-PI',
            'cargo': cargo,
            'id_login': auth['id_login'],
            'id_usuario': auth['id_usuario'],
            'senha': 'bypass',
        },
        protocolo_proc=sei_protocolo,
    )


def gerar(n_processo):
    itin = DiariasItinerario.query.filter_by(n_processo=n_processo).first()
    if not itin:
        print(f'[X] Processo {n_processo} nao encontrado.')
        return False

    print(f'Itin {itin.id} | etapa_atual={itin.etapa_atual_id} | sei_id_proc={itin.sei_id_procedimento}')
    sei_protocolo = itin.sei_protocolo or itin.n_processo or ''

    # Precondicoes: SINCIN + Despacho NCI ja no banco (bypass)
    doc_sincin = itin.get_doc('analise_pagamento')
    doc_dnci = itin.get_doc('despacho_nci')
    if not (doc_sincin and doc_dnci):
        print('[X] analise_pagamento/despacho_nci nao encontrados no banco. Rode o bypass antes.')
        return False
    sincin_fmt = doc_sincin.sei_formatado or doc_sincin.sei_id or ''
    print(f'  SINCIN (placeholder): {sincin_fmt}')
    print(f'  Despacho NCI (placeholder): {doc_dnci.sei_formatado or doc_dnci.sei_id}')

    # Precondicoes: despacho SGA existente
    doc_sga = itin.get_doc('despacho_sga')
    if not (doc_sga and doc_sga.sei_id):
        print('[X] despacho_sga nao existe. Gere o despacho SGA->NCI primeiro.')
        return False
    print(f'  Despacho SGA: {doc_sga.sei_formatado} ({doc_sga.sei_id})')

    # Token admin + auth bypass
    token_admin = gerar_token_sei_admin()
    if not token_admin:
        print('[X] Falha token admin.')
        return False
    auth = autenticar_usuario_sei('bypass', 'bypass', protocolo_bypass=sei_protocolo)
    if not auth or not auth.get('token'):
        print('[X] Falha auth bypass.')
        return False
    print('  [OK] Token admin + auth bypass.')

    # ── 1. DESPACHO APOIO/DFIN (serie 754) ──────────────────────────────
    print('\n--- 1/3: Despacho APOIO -> DFIN ---')
    enviar_procedimento(token_admin, sei_protocolo, [UNIDADE_DFIN_APOIO], manter_aberto=True)
    r_apoio = gerar_despacho_apoio(
        token=token_admin,
        id_procedimento=itin.sei_id_procedimento,
        sei_protocolo=sei_protocolo,
        ref_analise_nci_id=sincin_fmt,
        itinerario=itin,
    )
    if not r_apoio:
        print('[X] Falha ao gerar despacho APOIO.')
        return False
    apoio_doc_id = str(r_apoio.get('IdDocumento', ''))
    apoio_fmt = r_apoio.get('DocumentoFormatado', '')
    print(f'  [OK] Gerado: {apoio_fmt} (sei_id={apoio_doc_id})')

    r_sig = _assinar(auth, UNIDADE_DFIN_APOIO, apoio_doc_id, 'Superintendente', sei_protocolo)
    if r_sig and r_sig.get('sucesso'):
        print('  [OK] Assinado (Superintendente SGA).')
    else:
        print(f'  [!] Assinatura falhou: {r_sig}')

    itin.set_doc('despacho_apoio', sei_id=apoio_doc_id, sei_formatado=apoio_fmt)
    db.session.commit()

    # ── 2. DESPACHO DIRETOR DFIN (serie 754) ────────────────────────────
    print('\n--- 2/3: Despacho DFIN -> GEO ---')
    r_dir = gerar_despacho_diretor(
        token=token_admin,
        id_procedimento=itin.sei_id_procedimento,
        sei_protocolo=sei_protocolo,
        ref_despacho_apoio_id=apoio_fmt,
        itinerario=itin,
    )
    if not r_dir:
        print('[X] Falha ao gerar despacho DIRETOR.')
        return False
    dir_doc_id = str(r_dir.get('IdDocumento', ''))
    dir_fmt = r_dir.get('DocumentoFormatado', '')
    print(f'  [OK] Gerado: {dir_fmt} (sei_id={dir_doc_id})')

    r_sig = _assinar(auth, UNIDADE_DFIN_APOIO, dir_doc_id, 'Diretor DFIN', sei_protocolo)
    if r_sig and r_sig.get('sucesso'):
        print('  [OK] Assinado (Diretor DFIN).')
    else:
        print(f'  [!] Assinatura falhou: {r_sig}')

    itin.set_doc('despacho_diretor', sei_id=dir_doc_id, sei_formatado=dir_fmt)
    db.session.commit()

    # ── 3. DESPACHO GEO -> CCDP (serie 754) ─────────────────────────────
    print('\n--- 3/3: Despacho GEO -> CCDP ---')
    enviar_procedimento(token_admin, sei_protocolo, [UNIDADE_GEO], manter_aberto=True, unidade_origem=UNIDADE_DFIN_APOIO)
    r_geo = gerar_despacho_geo(
        token=token_admin,
        id_procedimento=itin.sei_id_procedimento,
        sei_protocolo=sei_protocolo,
        itinerario=itin,
    )
    if not r_geo:
        print('[X] Falha ao gerar despacho GEO.')
        return False
    geo_doc_id = str(r_geo.get('IdDocumento', ''))
    geo_fmt = r_geo.get('DocumentoFormatado', '')
    print(f'  [OK] Gerado: {geo_fmt} (sei_id={geo_doc_id})')

    r_sig = _assinar(auth, UNIDADE_GEO, geo_doc_id, 'Gerente GEO', sei_protocolo)
    if r_sig and r_sig.get('sucesso'):
        print('  [OK] Assinado (Gerente GEO).')
    else:
        print(f'  [!] Assinatura falhou: {r_sig}')

    itin.set_doc('despacho_geo', sei_id=geo_doc_id, sei_formatado=geo_fmt)
    db.session.commit()

    # ── Envio final: processo para CCDP ─────────────────────────────────
    print('\n--- Envio final: processo -> CCDP ---')
    envio = enviar_procedimento(
        token=token_admin,
        protocolo_procedimento=sei_protocolo,
        unidades_destino=[UNIDADE_CCDP],
        unidade_origem=UNIDADE_GEO,
        manter_aberto=True,
    )
    print(f'  Envio: {envio}')

    print('\n=== Concluido ===')
    print(f'Despacho APOIO:   {apoio_fmt} (sei_id={apoio_doc_id})')
    print(f'Despacho DIRETOR: {dir_fmt} (sei_id={dir_doc_id})')
    print(f'Despacho GEO:     {geo_fmt} (sei_id={geo_doc_id})')
    return True


def main():
    if len(sys.argv) < 2:
        print('Uso: python scripts/simular_despachos_pos_nci.py <n_processo>')
        sys.exit(1)
    n_processo = sys.argv[1]
    app = create_app()
    with app.app_context():
        ok = gerar(n_processo)
        sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
