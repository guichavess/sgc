"""Reverte o processo 00002.004523/2026-52 para etapa 1 (Solicitacao Inicial)."""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasItinerario, DiariasHistoricoMovimentacao

PROTOCOLO = '00002.004523/2026-52'


def main():
    app = create_app()
    with app.app_context():
        it = DiariasItinerario.query.filter(
            db.or_(
                DiariasItinerario.n_processo == PROTOCOLO,
                DiariasItinerario.sei_protocolo == PROTOCOLO,
            )
        ).first()

        if not it:
            print(f"Processo {PROTOCOLO} nao encontrado!")
            return

        print(f"Processo: {PROTOCOLO} (ID={it.id})")
        print(f"Etapa atual: {it.etapa_atual_id}")

        if it.etapa_atual_id == 1:
            print("Ja esta na etapa 1. Nada a fazer.")
            return

        print(f"Revertendo de etapa {it.etapa_atual_id} -> 1...")
        it.etapa_atual_id = 1

        # Remove historico de transicoes indevidas (etapa 1 -> 3)
        indevidos = DiariasHistoricoMovimentacao.query.filter(
            DiariasHistoricoMovimentacao.id_itinerario == it.id,
            DiariasHistoricoMovimentacao.id_etapa_anterior == 1,
            DiariasHistoricoMovimentacao.id_etapa_nova == 3,
        ).all()

        for h in indevidos:
            print(f"  Removendo historico: {h.data_movimentacao} etapa 1->3")
            db.session.delete(h)

        db.session.commit()
        print(f"[OK] Processo revertido para etapa 1.")
        print(f"     Etapa atual: {it.etapa_atual_id}")
        print(f"     {len(indevidos)} registro(s) de historico removido(s).")


if __name__ == '__main__':
    main()
