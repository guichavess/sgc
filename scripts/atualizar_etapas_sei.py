"""
Script standalone para atualizar etapas SEI dos processos de pagamento.
Replica a lógica de api_atualizar_etapas (solicitacoes/routes/api.py)
para uso via modal SIAFE.
"""
import sys
import os
import concurrent.futures

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import create_app
from app.extensions import db
from app.models import Solicitacao, Etapa, HistoricoMovimentacao, SeiMovimentacao
from app.constants import SerieDocumentoSEI
from datetime import datetime


def processar_item(app_obj, sol_id, mapa_ordem):
    """Processa uma solicitação: lê SEI e avança etapas."""
    with app_obj.app_context():
        sol = Solicitacao.query.get(sol_id)
        if not sol or not sol.protocolo_gerado_sei:
            return None

        SERIE_EMAIL = SerieDocumentoSEI.EMAIL
        SERIE_REQUERIMENTO = SerieDocumentoSEI.REQUERIMENTO
        SERIE_NOTA_EMPENHO = SerieDocumentoSEI.NOTA_EMPENHO
        SERIE_LIQUIDACAO = SerieDocumentoSEI.LIQUIDACAO
        SERIE_PD = SerieDocumentoSEI.PD
        SERIE_OB = SerieDocumentoSEI.OB

        MAPA_ORDEM_LOCAL = {
            1: 1, 2: 2, 8: 3, 15: 4, 12: 5,
            13: 6, 14: 7, 11: 8, 5: 9, 6: 10
        }

        mudou = False

        def extrair_data_segura(doc_obj):
            if not doc_obj or not doc_obj.data:
                return None
            try:
                raw = str(doc_obj.data).strip().replace("'", "").replace('"', "")
                data_str = raw.split(' ')[0]
                for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
                    try:
                        return datetime.strptime(data_str, fmt)
                    except ValueError:
                        continue
                return None
            except (AttributeError, TypeError):
                return None

        def registrar_historico_forcado(id_etapa, data_doc, msg):
            nonlocal mudou
            if not data_doc:
                return False
            hist = HistoricoMovimentacao.query.filter_by(
                id_solicitacao=sol.id, id_etapa_nova=id_etapa
            ).first()
            if hist:
                data_banco = hist.data_movimentacao
                if data_banco is None or data_banco.date() != data_doc.date():
                    hist.data_movimentacao = data_doc
                    hist.comentario = msg
                    return True
                return False
            novo = HistoricoMovimentacao(
                id_solicitacao=sol.id,
                id_etapa_anterior=sol.etapa_atual_id,
                id_etapa_nova=id_etapa,
                id_usuario_responsavel=1,  # sistema
                data_movimentacao=data_doc,
                comentario=msg
            )
            db.session.add(novo)
            return True

        def tentar_avancar_status(id_nova_etapa):
            nonlocal mudou
            ordem_atual = MAPA_ORDEM_LOCAL.get(sol.etapa_atual_id, 0)
            ordem_nova = MAPA_ORDEM_LOCAL.get(id_nova_etapa, 0)
            if ordem_nova > ordem_atual:
                sol.etapa_atual_id = id_nova_etapa
                mudou = True
                return True
            return False

        def calcular_tempo_total(data_fim):
            if not data_fim or sol.tempo_total:
                return
            try:
                primeiro_hist = HistoricoMovimentacao.query.filter_by(
                    id_solicitacao=sol.id
                ).order_by(HistoricoMovimentacao.data_movimentacao.asc()).first()
                data_inicio = primeiro_hist.data_movimentacao if primeiro_hist and primeiro_hist.data_movimentacao else sol.data_solicitacao
                if not data_inicio:
                    return
                dias = max((data_fim - data_inicio).days, 0)
                sol.tempo_total = f"{dias} dias"
            except (TypeError, AttributeError):
                pass

        try:
            docs = SeiMovimentacao.query.filter_by(
                protocolo_procedimento=sol.protocolo_gerado_sei
            ).order_by(SeiMovimentacao.id_documento.asc()).all()

            if not docs:
                return None

            ids_series = [str(d.id_serie) for d in docs]
            emails = [d for d in docs if str(d.id_serie) == SERIE_EMAIL]

            # A. ETAPA 8 e 12: DOCUMENTAÇÃO RECEBIDA
            if SERIE_REQUERIMENTO in ids_series:
                doc_req = next((d for d in docs if str(d.id_serie) == SERIE_REQUERIMENTO), None)
                dt = extrair_data_segura(doc_req)
                if registrar_historico_forcado(8, dt, "Documentação Recebida (Req. 64)"):
                    mudou = True
                if registrar_historico_forcado(12, dt, "Fiscais Notificados (Automático)"):
                    mudou = True
                if not tentar_avancar_status(12):
                    tentar_avancar_status(8)

            # B. ETAPA 11: NF ATESTADA
            if len(emails) >= 2:
                try:
                    segundo_email = emails[1]
                    idx_2_email = docs.index(segundo_email)
                    doc_alvo = None
                    for i in range(idx_2_email + 1, len(docs)):
                        if str(docs[i].id_serie) != SERIE_EMAIL:
                            doc_alvo = docs[i]
                            break
                    if doc_alvo:
                        dt = extrair_data_segura(doc_alvo)
                        if registrar_historico_forcado(11, dt, f"NF Atestada (Doc {doc_alvo.serie_nome})"):
                            mudou = True
                            tentar_avancar_status(11)
                except ValueError:
                    pass

            # C. FINANCEIRO (OB > PD > NL > NE)
            doc_ne = next((d for d in docs if str(d.id_serie) == SERIE_NOTA_EMPENHO), None)
            doc_nl = next((d for d in docs if str(d.id_serie) == SERIE_LIQUIDACAO), None)
            doc_pd = next((d for d in docs if str(d.id_serie) == SERIE_PD), None)
            doc_ob = next((d for d in docs if str(d.id_serie) == SERIE_OB), None)

            if doc_ne and doc_ne.numero and sol.num_ne != str(doc_ne.numero):
                sol.num_ne = str(doc_ne.numero)
                if sol.status_empenho_id != 2:
                    sol.status_empenho_id = 2
                mudou = True

            if doc_nl and doc_nl.numero and sol.num_nl != str(doc_nl.numero):
                sol.num_nl = str(doc_nl.numero)
                mudou = True

            if doc_pd and doc_pd.numero and sol.num_pd != str(doc_pd.numero):
                sol.num_pd = str(doc_pd.numero)
                mudou = True

            if doc_ob and doc_ob.numero and sol.num_ob != str(doc_ob.numero):
                sol.num_ob = str(doc_ob.numero)
                mudou = True

            # Lógica de Avanço Financeiro
            fin_etapa = None
            fin_doc = None
            msg_fin = ""

            if doc_ob:
                fin_etapa = 6
                fin_doc = doc_ob
                msg_fin = f"Pago (OB {sol.num_ob})"
                if sol.status_geral != 'PAGO':
                    sol.status_geral = 'PAGO'
                    mudou = True
            elif doc_pd:
                fin_etapa = 5
                fin_doc = doc_pd
                msg_fin = f"PD Emitida ({sol.num_pd})"
            elif doc_nl:
                fin_etapa = 5
                fin_doc = doc_nl
                msg_fin = f"Liquidado (NL {sol.num_nl})"

            if fin_etapa and fin_doc:
                dt = extrair_data_segura(fin_doc)
                if registrar_historico_forcado(fin_etapa, dt, msg_fin):
                    mudou = True
                tentar_avancar_status(fin_etapa)
                if fin_etapa == 6:
                    calcular_tempo_total(dt)

            if mudou:
                db.session.commit()
                return f"Atualizado: Solicitação #{sol.id}"
            else:
                return None

        except Exception as e:
            db.session.rollback()
            print(f"Erro ao processar solicitação {sol_id}: {e}")
            return None


def main():
    app = create_app()

    with app.app_context():
        todas_etapas = Etapa.query.all()
        mapa_ordem = {e.id: e.ordem for e in todas_etapas}

        ids_para_processar = [
            s.id for s in Solicitacao.query.filter(
                Solicitacao.etapa_atual_id != 6,
                Solicitacao.status_geral != 'CANCELADO'
            ).all()
        ]

        total = len(ids_para_processar)
        print(f"Processando {total} solicitações pendentes...")

        if total == 0:
            print("Nenhuma solicitação pendente.")
            return

        atualizados = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(processar_item, app, sid, mapa_ordem): sid
                for sid in ids_para_processar
            }

            for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    resultado = future.result()
                    if resultado:
                        atualizados += 1
                        print(f"  [{i}/{total}] {resultado}")
                    elif i % 20 == 0:
                        print(f"  [{i}/{total}] Analisando...")
                except Exception as e:
                    print(f"  [{i}/{total}] Erro: {e}")

        print(f"\nConcluído! {atualizados}/{total} processos atualizados.")


if __name__ == '__main__':
    main()
