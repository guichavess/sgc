import React from 'react';
import { useApi } from '../hooks/useApi';
import { useCurrency } from '../hooks/useCurrency';
import { useYearFilter } from '../hooks/useYearFilter';
import { adaptCategorySeries } from '../adapters/chartAdapters';
import KpiCard from '../components/shared/KpiCard';
import ChartCard from '../components/shared/ChartCard';
import YearSelector from '../components/shared/YearSelector';
import HorizontalBarChart from '../components/charts/HorizontalBarChart';
import DashboardAreaChart from '../components/charts/DashboardAreaChart';
import type {
  FinanceiroKpis,
  CategorySeriesResponse,
} from '../types/api';

export default function Financeiro() {
  const { year, setYear } = useYearFilter();
  const { format } = useCurrency();

  const kpis = useApi<FinanceiroKpis>('/dashboards/api/kpis-financeiro', { ano: year });
  const empVsLiq = useApi<CategorySeriesResponse>('/dashboards/api/empenhado-vs-liquidado', { ano: year });
  const evolucao = useApi<CategorySeriesResponse>('/dashboards/api/evolucao-empenhos', { ano: year });
  const saldo = useApi<CategorySeriesResponse>('/dashboards/api/saldo-por-contrato', { ano: year });

  const k = kpis.data;
  const saldoPositivo = (k?.saldo_total ?? 0) >= 0;

  return (
    <>
      {/* Header */}
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h4 className="mb-1 fw-bold">Dashboard Financeiro</h4>
          <small className="text-muted">Empenhos, liquidações e saldos</small>
        </div>
        <YearSelector value={year} onChange={setYear} />
      </div>

      {/* KPIs */}
      <div className="row g-3 mb-4">
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-cash-coin"
            color="#B8860B"
            value={k ? format(k.total_empenhado) : '—'}
            label="Total Empenhado"
          />
        </div>
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-check2-circle"
            color="#6f42c1"
            value={k ? format(k.total_liquidado) : '—'}
            label="Total Liquidado"
          />
        </div>
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-wallet2"
            color={saldoPositivo ? '#198754' : '#dc3545'}
            value={k ? format(k.saldo_total) : '—'}
            label="Saldo"
          />
        </div>
        <div className="col-md-3 col-6">
          <KpiCard
            icon="bi bi-exclamation-circle"
            color={(k?.nes_pendentes ?? 0) > 0 ? '#dc3545' : '#6c757d'}
            value={k?.nes_pendentes ?? '—'}
            label="NEs Pendentes"
          />
        </div>
      </div>

      {/* Row 1 - Empenhado vs Liquidado */}
      <div className="row g-3 mb-4">
        <div className="col-12">
          <ChartCard
            title="Empenhado vs Liquidado (Top 15)"
            icon="bi bi-bar-chart-steps"
            loading={empVsLiq.loading}
            empty={!empVsLiq.data?.categories?.length}
          >
            {empVsLiq.data && (
              <HorizontalBarChart
                data={adaptCategorySeries(empVsLiq.data)}
                dataKeys={empVsLiq.data.series.map((s) => s.name)}
                colors={['#B8860B', '#6f42c1']}
                valueFormatter={(v) => format(v)}
              />
            )}
          </ChartCard>
        </div>
      </div>

      {/* Row 2 */}
      <div className="row g-3 mb-4">
        <div className="col-lg-6">
          <ChartCard
            title={`Evolução de Empenhos ${year}`}
            icon="bi bi-graph-up"
            loading={evolucao.loading}
            empty={!evolucao.data?.series?.length}
          >
            {evolucao.data && (
              <DashboardAreaChart
                data={adaptCategorySeries(evolucao.data)}
                dataKey={evolucao.data.series[0]?.name || 'value'}
                color="#B8860B"
                valueFormatter={(v) => format(v)}
              />
            )}
          </ChartCard>
        </div>
        <div className="col-lg-6">
          <ChartCard
            title="Saldo por Contrato (Top 15)"
            icon="bi bi-piggy-bank"
            loading={saldo.loading}
            empty={!saldo.data?.categories?.length}
          >
            {saldo.data && (
              <HorizontalBarChart
                data={adaptCategorySeries(saldo.data)}
                dataKeys={saldo.data.series.map((s) => s.name)}
                colors={['#198754']}
                valueFormatter={(v) => format(v)}
              />
            )}
          </ChartCard>
        </div>
      </div>
    </>
  );
}
