import React from 'react';
import { useApi } from '../hooks/useApi';
import { useYearFilter } from '../hooks/useYearFilter';
import {
  adaptDonut,
  adaptCategoryData,
} from '../adapters/chartAdapters';
import ChartCard from '../components/shared/ChartCard';
import YearSelector from '../components/shared/YearSelector';
import DonutChart from '../components/charts/DonutChart';
import VerticalBarChart from '../components/charts/VerticalBarChart';
import type {
  DonutResponse,
  CategoryDataResponse,
} from '../types/api';

export default function Pagamentos() {
  const { year, setYear } = useYearFilter();

  const etapas = useApi<CategoryDataResponse>('/dashboards/api/solicitacoes-por-etapa');
  const status = useApi<DonutResponse>('/dashboards/api/status-solicitacoes');
  const tempoMedio = useApi<CategoryDataResponse>('/dashboards/api/tempo-medio-etapa');
  const competencia = useApi<CategoryDataResponse>('/dashboards/api/distribuicao-competencia');

  return (
    <>
      {/* Header */}
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h4 className="mb-1 fw-bold">Dashboard de Pagamentos</h4>
          <small className="text-muted">Análise de solicitações de pagamento</small>
        </div>
        <YearSelector value={year} onChange={setYear} />
      </div>

      {/* Row 1 */}
      <div className="row g-3 mb-4">
        <div className="col-lg-7">
          <ChartCard
            title="Solicitações por Etapa"
            icon="bi bi-funnel"
            loading={etapas.loading}
            empty={!etapas.data?.categories?.length}
          >
            {etapas.data && (
              <VerticalBarChart
                data={adaptCategoryData(etapas.data)}
                colors={etapas.data.colors || ['#0d6efd']}
                distributed
                yAxisLabel="Quantidade"
              />
            )}
          </ChartCard>
        </div>
        <div className="col-lg-5">
          <ChartCard
            title="Status das Solicitações"
            icon="bi bi-pie-chart"
            loading={status.loading}
            empty={!status.data?.labels?.length}
          >
            {status.data && (
              <DonutChart data={adaptDonut(status.data)} />
            )}
          </ChartCard>
        </div>
      </div>

      {/* Row 2 */}
      <div className="row g-3 mb-4">
        <div className="col-lg-6">
          <ChartCard
            title="Tempo Médio por Etapa (dias)"
            icon="bi bi-clock-history"
            loading={tempoMedio.loading}
            empty={!tempoMedio.data?.categories?.length}
          >
            {tempoMedio.data && (
              <VerticalBarChart
                data={adaptCategoryData(tempoMedio.data)}
                colors={['#6f42c1']}
                valueFormatter={(v) => `${v}d`}
                yAxisLabel="Dias"
              />
            )}
          </ChartCard>
        </div>
        <div className="col-lg-6">
          <ChartCard
            title="Distribuição por Competência"
            icon="bi bi-calendar3"
            loading={competencia.loading}
            empty={!competencia.data?.categories?.length}
          >
            {competencia.data && (
              <VerticalBarChart
                data={adaptCategoryData(competencia.data)}
                colors={['#1B998B']}
                yAxisLabel="Quantidade"
              />
            )}
          </ChartCard>
        </div>
      </div>
    </>
  );
}
