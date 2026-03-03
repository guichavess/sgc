import React from 'react';
import { useApi } from '../hooks/useApi';
import { useCurrency } from '../hooks/useCurrency';
import { adaptDonut, adaptCategorySeries, adaptGantt } from '../adapters/chartAdapters';
import KpiCard from '../components/shared/KpiCard';
import ChartCard from '../components/shared/ChartCard';
import DonutChart from '../components/charts/DonutChart';
import HorizontalBarChart from '../components/charts/HorizontalBarChart';
import GanttChart from '../components/charts/GanttChart';
import VigenciaTable from '../components/charts/VigenciaTable';
import type {
  ContratosKpis,
  DonutResponse,
  CategorySeriesResponse,
  GanttResponse,
  VigenciaResponse,
} from '../types/api';

export default function Contratos() {
  const { format } = useCurrency();

  const kpis = useApi<ContratosKpis>('/dashboards/api/kpis-contratos');
  const situacao = useApi<DonutResponse>('/dashboards/api/contratos-por-situacao');
  const valorContratado = useApi<CategorySeriesResponse>('/dashboards/api/valor-por-contratado');
  const gantt = useApi<GanttResponse>('/dashboards/api/contratos-gantt');
  const vigencia = useApi<VigenciaResponse>('/dashboards/api/contratos-vigencia-proxima');

  const k = kpis.data;

  return (
    <>
      {/* Header */}
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h4 className="mb-1 fw-bold">Dashboard de Contratos</h4>
          <small className="text-muted">Visão geral dos contratos</small>
        </div>
      </div>

      {/* KPIs */}
      <div className="row g-3 mb-4">
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-file-earmark-text"
            color="#0d6efd"
            value={k?.total_contratos ?? '—'}
            label="Total Contratos"
          />
        </div>
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-check-circle"
            color="#198754"
            value={k?.contratos_ativos ?? '—'}
            label="Ativos"
          />
        </div>
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-x-circle"
            color="#6c757d"
            value={k?.contratos_encerrados ?? '—'}
            label="Encerrados"
          />
        </div>
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-currency-dollar"
            color="#B8860B"
            value={k ? format(k.valor_total) : '—'}
            label="Valor Total"
          />
        </div>
      </div>

      {/* Row 1 */}
      <div className="row g-3 mb-4">
        <div className="col-lg-5">
          <ChartCard
            title="Contratos por Situação"
            icon="bi bi-pie-chart"
            loading={situacao.loading}
            empty={!situacao.data?.labels?.length}
          >
            {situacao.data && (
              <DonutChart data={adaptDonut(situacao.data)} />
            )}
          </ChartCard>
        </div>
        <div className="col-lg-7">
          <ChartCard
            title="Valor Total por Contratado (Top 15)"
            icon="bi bi-bar-chart"
            loading={valorContratado.loading}
            empty={!valorContratado.data?.categories?.length}
          >
            {valorContratado.data && (
              <HorizontalBarChart
                data={adaptCategorySeries(valorContratado.data)}
                dataKeys={valorContratado.data.series.map((s) => s.name)}
                colors={['#0d6efd']}
                valueFormatter={(v) => format(v)}
              />
            )}
          </ChartCard>
        </div>
      </div>

      {/* Row 2 - Gantt */}
      <div className="row g-3 mb-4">
        <div className="col-12">
          <ChartCard
            title="Vigência dos Contratos"
            icon="bi bi-calendar-range"
            loading={gantt.loading}
            empty={!gantt.data?.series?.[0]?.data?.length}
          >
            {gantt.data && (() => {
              const g = adaptGantt(gantt.data);
              return (
                <GanttChart
                  items={g.items}
                  minDate={g.minDate}
                  maxDate={g.maxDate}
                />
              );
            })()}
          </ChartCard>
        </div>
      </div>

      {/* Row 3 - Tabela de Vigência */}
      {vigencia.data && vigencia.data.contratos.length > 0 && (
        <div className="row g-3 mb-4">
          <div className="col-12">
            <VigenciaTable
              contratos={vigencia.data.contratos}
              formatCurrency={format}
            />
          </div>
        </div>
      )}
    </>
  );
}
