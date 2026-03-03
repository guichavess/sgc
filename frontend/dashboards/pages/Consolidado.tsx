import React from 'react';
import { useApi } from '../hooks/useApi';
import { useCurrency } from '../hooks/useCurrency';
import { useYearFilter } from '../hooks/useYearFilter';
import { adaptDonut, adaptCategorySeries } from '../adapters/chartAdapters';
import KpiCard from '../components/shared/KpiCard';
import ChartCard from '../components/shared/ChartCard';
import YearSelector from '../components/shared/YearSelector';
import DonutChart from '../components/charts/DonutChart';
import VerticalBarChart from '../components/charts/VerticalBarChart';
import HorizontalBarChart from '../components/charts/HorizontalBarChart';
import type {
  ConsolidadoKpis,
  DonutResponse,
  CategorySeriesResponse,
} from '../types/api';

export default function Consolidado() {
  const { year, setYear } = useYearFilter();
  const { format } = useCurrency();

  const kpis = useApi<ConsolidadoKpis>('/dashboards/api/kpis-consolidado', { ano: year });
  const statusData = useApi<DonutResponse>('/dashboards/api/status-solicitacoes');
  const evolucao = useApi<CategorySeriesResponse>('/dashboards/api/evolucao-mensal', { ano: year });
  const topContratos = useApi<CategorySeriesResponse>('/dashboards/api/top-contratos-empenhado', { ano: year });

  const k = kpis.data;
  const saldoPositivo = (k?.saldo_global ?? 0) >= 0;

  return (
    <>
      {/* Header + Filtro de Ano */}
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h4 className="mb-1 fw-bold">Dashboard Consolidado</h4>
          <small className="text-muted">Visão geral do sistema</small>
        </div>
        <YearSelector value={year} onChange={setYear} />
      </div>

      {/* KPIs */}
      <div className="row g-3 mb-4">
        <div className="col-xl-2 col-md-4 col-6">
          <KpiCard
            icon="bi bi-file-earmark-text"
            color="#0d6efd"
            value={k?.total_solicitacoes ?? '—'}
            label="Solicitações"
          />
        </div>
        <div className="col-xl-2 col-md-4 col-6">
          <KpiCard
            icon="bi bi-building"
            color="#198754"
            value={`${k?.contratos_ativos ?? '—'} / ${k?.total_contratos ?? '—'}`}
            label="Contratos Ativos"
          />
        </div>
        <div className="col-xl-2 col-md-4 col-6">
          <KpiCard
            icon="bi bi-cash-coin"
            color="#B8860B"
            value={k ? format(k.total_empenhado) : '—'}
            label="Total Empenhado"
          />
        </div>
        <div className="col-xl-2 col-md-4 col-6">
          <KpiCard
            icon="bi bi-check2-circle"
            color="#6f42c1"
            value={k ? format(k.total_liquidado) : '—'}
            label="Total Liquidado"
          />
        </div>
        <div className="col-xl-2 col-md-4 col-6">
          <KpiCard
            icon="bi bi-wallet2"
            color={saldoPositivo ? '#198754' : '#dc3545'}
            value={k ? format(k.saldo_global) : '—'}
            label="Saldo Global"
          />
        </div>
        <div className="col-xl-2 col-md-4 col-6">
          <KpiCard
            icon="bi bi-exclamation-circle"
            color={(k?.nes_pendentes ?? 0) > 0 ? '#dc3545' : '#6c757d'}
            value={k?.nes_pendentes ?? '—'}
            label="NEs Pendentes"
          />
        </div>
      </div>

      {/* Charts Row 1 */}
      <div className="row g-3 mb-4">
        <div className="col-lg-5">
          <ChartCard
            title="Solicitações por Status"
            icon="bi bi-pie-chart"
            loading={statusData.loading}
            empty={!statusData.data?.labels?.length}
          >
            {statusData.data && (
              <DonutChart data={adaptDonut(statusData.data)} />
            )}
          </ChartCard>
        </div>
        <div className="col-lg-7">
          <ChartCard
            title="Evolução Mensal"
            icon="bi bi-bar-chart"
            loading={evolucao.loading}
            empty={!evolucao.data?.series?.length}
          >
            {evolucao.data && (
              <VerticalBarChart
                data={adaptCategorySeries(evolucao.data)}
                dataKeys={evolucao.data.series.map((s) => s.name)}
                colors={['#0d6efd']}
                yAxisLabel="Quantidade"
              />
            )}
          </ChartCard>
        </div>
      </div>

      {/* Charts Row 2 */}
      <div className="row g-3 mb-4">
        <div className="col-12">
          <ChartCard
            title="Top 10 Contratos por Valor Empenhado"
            icon="bi bi-trophy"
            loading={topContratos.loading}
            empty={!topContratos.data?.categories?.length}
          >
            {topContratos.data && (
              <HorizontalBarChart
                data={adaptCategorySeries(topContratos.data)}
                dataKeys={topContratos.data.series.map((s) => s.name)}
                colors={['#B8860B']}
                maxLabelWidth={250}
                valueFormatter={(v) => format(v)}
                showDataLabels
              />
            )}
          </ChartCard>
        </div>
      </div>
    </>
  );
}
