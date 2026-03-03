import React from 'react';
import type { VigenciaContrato } from '../../types/api';

interface VigenciaTableProps {
  contratos: VigenciaContrato[];
  formatCurrency: (value: number) => string;
}

function getDiasClass(dias: number): string {
  if (dias <= 15) return 'urgente';
  if (dias <= 30) return 'atencao';
  return 'ok';
}

export default function VigenciaTable({
  contratos,
  formatCurrency,
}: VigenciaTableProps) {
  if (contratos.length === 0) return null;

  return (
    <div className="card chart-card">
      <div className="chart-title">
        <i className="bi bi-exclamation-triangle me-2"></i>
        Contratos com Vigência Próxima do Fim
      </div>
      <div className="table-responsive">
        <table className="table table-hover vigencia-table mb-0">
          <thead>
            <tr>
              <th>Contrato</th>
              <th>Contratado</th>
              <th>Fim Vigência</th>
              <th>Dias Restantes</th>
              <th className="text-end">Valor Total</th>
            </tr>
          </thead>
          <tbody>
            {contratos.map((c) => (
              <tr key={c.codigo}>
                <td>
                  <strong>{c.codigo}</strong>
                </td>
                <td>{c.contratado}</td>
                <td>{c.fim_vigencia}</td>
                <td>
                  <span className={`dias-restantes ${getDiasClass(c.dias_restantes)}`}>
                    {c.dias_restantes} dias
                  </span>
                </td>
                <td className="text-end">{formatCurrency(c.valor)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
