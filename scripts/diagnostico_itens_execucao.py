#!/usr/bin/env python
"""
scripts/diagnostico_itens_execucao.py

Diagnóstico READ-ONLY: identifica itens vinculados que seriam ocultados
pelo filtro de tipificação em listar_itens_para_execucao().

Não altera banco. Apenas SELECT + relatório no stdout.

Uso:
    cd pagamentos
    python scripts/diagnostico_itens_execucao.py
    python scripts/diagnostico_itens_execucao.py --contrato 23000466
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def diagnosticar(app, filtro_codigo=None):
    with app.app_context():
        from app.extensions import db
        from app.models.item_vinculado import ItemVinculado
        from app.models.contrato import Contrato
        from app.models.catserv import CatservServico
        from app.models.catmat import CatmatItem as CatmatItemModel, CatmatPdm

        q = Contrato.query
        if filtro_codigo:
            q = q.filter(Contrato.codigo == filtro_codigo)

        contratos = q.all()
        print(f"\n{'='*70}")
        print(f"DIAGNOSTICO: itens vinculados vs filtro de tipificacao")
        print(f"Contratos analisados: {len(contratos)}")
        print(f"{'='*70}\n")

        total_contratos_afetados = 0
        total_itens_ocultados = 0

        for contrato in contratos:
            vinculados = ItemVinculado.query.filter_by(
                codigo_contrato=contrato.codigo
            ).all()

            if not vinculados:
                continue

            tipo = contrato.tipo_contrato
            if not tipo:
                continue  # sem tipo → fallback retorna tudo sem filtro

            ocultados = []

            # ── Simular filtro CATMAT ────────────────────────────────────────
            if tipo in ('M', 'SM'):
                itens_permitidos = None
                if contrato.catmat_pdm_id:
                    pdm = db.session.get(CatmatPdm, contrato.catmat_pdm_id)
                    if pdm:
                        rows = CatmatItemModel.query.filter_by(
                            codigo_pdm=pdm.codigo
                        ).with_entities(CatmatItemModel.id).all()
                        itens_permitidos = {r[0] for r in rows}
                elif contrato.catmat_classe_id:
                    from app.models.catmat import CatmatClasse
                    classe = db.session.get(CatmatClasse, contrato.catmat_classe_id)
                    if classe:
                        pdm_codigos = {p[0] for p in CatmatPdm.query.filter_by(
                            codigo_classe=classe.codigo
                        ).with_entities(CatmatPdm.codigo).all()}
                        rows = CatmatItemModel.query.filter(
                            CatmatItemModel.codigo_pdm.in_(pdm_codigos)
                        ).with_entities(CatmatItemModel.id).all()
                        itens_permitidos = {r[0] for r in rows}

                for v in vinculados:
                    if v.tipo != 'M':
                        continue
                    mat_id = v.catmat_item_id
                    if not mat_id:
                        ocultados.append((v, 'SEM_DEPARA'))
                    elif itens_permitidos is not None and mat_id not in itens_permitidos:
                        ocultados.append((v, 'FORA_TIPIFICACAO'))

            # ── Simular filtro CATSERV ───────────────────────────────────────
            if tipo in ('S', 'SM'):
                servicos_permitidos = None
                if contrato.catserv_classe_id:
                    rows = CatservServico.query.filter_by(
                        codigo_classe=contrato.catserv_classe_id
                    ).with_entities(CatservServico.codigo_servico).all()
                    servicos_permitidos = {r[0] for r in rows}
                elif contrato.catserv_grupo_id:
                    rows = CatservServico.query.filter_by(
                        codigo_grupo=contrato.catserv_grupo_id
                    ).with_entities(CatservServico.codigo_servico).all()
                    servicos_permitidos = {r[0] for r in rows}

                for v in vinculados:
                    if v.tipo != 'S':
                        continue
                    srv_id = v.catserv_servico_id
                    if not srv_id:
                        ocultados.append((v, 'SEM_DEPARA'))
                    elif servicos_permitidos is not None and srv_id not in servicos_permitidos:
                        ocultados.append((v, 'FORA_TIPIFICACAO'))

            if ocultados:
                total_contratos_afetados += 1
                total_itens_ocultados += len(ocultados)
                print(f"Contrato: {contrato.codigo}")
                print(f"  Objeto  : {(contrato.objeto or '')[:80]}")
                print(f"  Tipo    : {tipo}")
                print(f"  CATSERV  classe_id={contrato.catserv_classe_id}  grupo_id={contrato.catserv_grupo_id}")
                print(f"  CATMAT   pdm_id={contrato.catmat_pdm_id}  classe_id={contrato.catmat_classe_id}")
                print(f"  Itens vinculados: {len(vinculados)}  ocultados: {len(ocultados)}")
                for v, motivo in ocultados:
                    item_ref = v.catserv_servico_id if v.tipo == 'S' else v.catmat_item_id
                    ic_desc = (v.item_contrato.descricao[:60]
                               if v.item_contrato else '(sem item_contrato)')
                    print(f"    [{v.tipo}] item={item_ref}  motivo={motivo}  desc='{ic_desc}'")
                print()

        print(f"{'='*70}")
        print(f"RESUMO")
        print(f"  Contratos analisados : {len(contratos)}")
        print(f"  Contratos afetados   : {total_contratos_afetados}")
        print(f"  Itens ocultados total: {total_itens_ocultados}")
        print(f"{'='*70}\n")

        if total_contratos_afetados == 0:
            print("Nenhum item seria ocultado pelo filtro de tipificacao.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Diagnostico de itens ocultados pelo re-filtro de tipificacao (read-only)'
    )
    parser.add_argument('--contrato', help='Filtrar por codigo de contrato especifico')
    args = parser.parse_args()

    from app.config import DevelopmentConfig
    from app import create_app
    application = create_app(DevelopmentConfig)
    diagnosticar(application, filtro_codigo=args.contrato)
