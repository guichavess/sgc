import React, { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, BarChart, Bar, PieChart, Pie, Cell,
} from 'recharts';
import { useApi } from '../hooks/useApi';
import { useCurrency } from '../hooks/useCurrency';
import { useYearFilter } from '../hooks/useYearFilter';
import { adaptCategorySeries } from '../adapters/chartAdapters';
import ChartCard from '../components/shared/ChartCard';
import YearSelector from '../components/shared/YearSelector';
import LoadingSpinner from '../components/shared/LoadingSpinner';
import type {
  OrcamentarioKpis,
  CategorySeriesResponse,
  TabelaContratosResponse,
  TabelaContratoRow,
  FiltrosOrcamentario,
} from '../types/api';

/* =====================================================================
   KPI Card com barra de progresso
   ===================================================================== */

const KPI_COLORS: Record<string, string> = {
  reservado: '#343990',
  empenhado: '#d4a017',
  liquidado: '#dc3545',
  pd: '#1B998B',
  pd_aberto: '#fd7e14',
  pago: '#e74c3c',
};

interface OrcKpiCardProps {
  label: string;
  value: number;
  pct: number;
  color: string;
  format: (v: number) => string;
}

function OrcKpiCard({ label, value, pct, color, format }: OrcKpiCardProps) {
  return (
    <div className="card h-100" style={{ borderLeft: `4px solid ${color}` }}>
      <div className="card-body py-2 px-3">
        <div style={{ fontSize: '0.7rem', fontWeight: 700, textTransform: 'uppercase', color: '#6c757d', letterSpacing: '0.05em' }}>
          {label}
        </div>
        <div style={{ fontSize: '1.15rem', fontWeight: 700, color: '#212529' }}>
          {format(value)}
        </div>
        <div className="d-flex align-items-center gap-2 mt-1">
          <small style={{ color: '#999', fontSize: '0.72rem' }}>Execução sobre o Orçado:</small>
        </div>
        <div className="d-flex align-items-center gap-2 mt-1">
          <div className="progress flex-grow-1" style={{ height: 8 }}>
            <div
              className="progress-bar"
              role="progressbar"
              style={{ width: `${Math.min(pct, 100)}%`, backgroundColor: color }}
            />
          </div>
          <small style={{ fontWeight: 600, fontSize: '0.78rem', minWidth: 45, textAlign: 'right' }}>
            {pct.toFixed(2)}%
          </small>
        </div>
      </div>
    </div>
  );
}

/* =====================================================================
   Seletor de métricas para gráficos
   ===================================================================== */

const METRICAS = [
  { key: 'Reserva', color: '#343990' },
  { key: 'Empenho', color: '#d4a017' },
  { key: 'Liquidação', color: '#dc3545' },
  { key: 'PD Executada', color: '#1B998B' },
  { key: 'OB', color: '#e74c3c' },
];

const METRICA_API_MAP: Record<string, string> = {
  'Reserva': 'reserva',
  'Empenho': 'empenho',
  'Liquidação': 'liquidacao',
  'PD Executada': 'pd',
  'OB': 'ob',
};

/* =====================================================================
   Tabela de PDs com sort, paginação
   ===================================================================== */

interface PdRow {
  codigo: string;
  statusDocumento: string;
  codNatureza: string;
  credor: string;
  valor: number;
  statusExecucao: string;
  statusExecucaoRaw: string;
  competencia: string;
}

type SortKey = keyof PdRow;
type SortDir = 'asc' | 'desc';
const PAGE_SIZE = 10;

function sortRows(rows: PdRow[], key: SortKey, dir: SortDir) {
  return [...rows].sort((a, b) => {
    const va = a[key];
    const vb = b[key];
    if (typeof va === 'number' && typeof vb === 'number') {
      return dir === 'asc' ? va - vb : vb - va;
    }
    return dir === 'asc'
      ? String(va).localeCompare(String(vb))
      : String(vb).localeCompare(String(va));
  });
}

function exportToExcel(rows: PdRow[], fmt: (v: number) => string) {
  const header = ['Código', 'Status Documento', 'Cód. Natureza', 'Credor', 'Valor', 'Status Execução', 'Competência'];
  const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  let html = `<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel" xmlns="http://www.w3.org/TR/REC-html40">
<head><meta charset="utf-8">
<style>
  table { border-collapse: collapse; }
  th { background-color: #343990; color: #fff; font-weight: bold; padding: 8px 12px; border: 1px solid #ccc; text-align: center; }
  td { padding: 6px 12px; border: 1px solid #ddd; vertical-align: top; }
</style>
</head><body><table>
<thead><tr>${header.map(h => `<th>${esc(h)}</th>`).join('')}</tr></thead>
<tbody>`;

  for (const r of rows) {
    html += `<tr>
      <td>${esc(r.codigo)}</td>
      <td style="text-align:center">${esc(r.statusDocumento)}</td>
      <td style="text-align:center">${esc(r.codNatureza)}</td>
      <td>${esc(r.credor)}</td>
      <td style="mso-number-format:'#.##0\\,00';text-align:right">${r.valor.toFixed(2).replace('.', ',')}</td>
      <td style="text-align:center">${esc(r.statusExecucao)}</td>
      <td style="text-align:center">${esc(r.competencia || '')}</td>
    </tr>`;
  }
  html += '</tbody></table></body></html>';

  const blob = new Blob(['\uFEFF' + html], { type: 'application/vnd.ms-excel;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'listagem_pd.xls';
  a.click();
  URL.revokeObjectURL(url);
}

function fmtBRL(v: number): string {
  return 'R$ ' + v.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtCompact(v: number): string {
  if (v === 0) return 'R$ 0';
  const abs = Math.abs(v);
  if (abs >= 1_000_000_000) return `R$ ${(v / 1_000_000_000).toFixed(1)}Bi`;
  if (abs >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(1)}Mi`;
  if (abs >= 1_000) return `R$ ${(v / 1_000).toFixed(0)}K`;
  return `R$ ${v.toFixed(0)}`;
}

/* =====================================================================
   Select com busca e múltipla seleção
   ===================================================================== */

interface SelectOption { value: string; label: string; }

interface SearchableSelectProps {
  options: SelectOption[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  allLabel?: string;
}

function SearchableSelect({ options, value, onChange, placeholder = 'Pesquisar...', allLabel = 'Todos' }: SearchableSelectProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef<HTMLDivElement>(null);

  const selected = useMemo(() => {
    if (!value) return new Set<string>();
    return new Set(value.split(',').filter(Boolean));
  }, [value]);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const filtered = useMemo(() => {
    if (!search.trim()) return options;
    const q = search.toLowerCase();
    return options.filter((o) => o.label.toLowerCase().includes(q));
  }, [options, search]);

  const toggleOption = (val: string) => {
    const next = new Set(selected);
    if (next.has(val)) { next.delete(val); } else { next.add(val); }
    onChange(Array.from(next).join(','));
  };

  const selectAll = () => { onChange(''); setOpen(false); };

  let displayLabel: string;
  if (selected.size === 0) { displayLabel = allLabel; }
  else if (selected.size === 1) {
    const opt = options.find((o) => selected.has(o.value));
    displayLabel = opt?.label || value;
  } else { displayLabel = `${selected.size} selecionados`; }

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        type="button"
        className="form-select form-select-sm text-start"
        onClick={() => { setOpen(!open); setSearch(''); }}
        style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', backgroundColor: '#fff', cursor: 'pointer' }}
      >
        {displayLabel}
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 1050,
          backgroundColor: '#fff', borderRadius: 8, marginTop: 4,
          boxShadow: '0 8px 30px rgba(0,0,0,0.18)', border: '1px solid #dee2e6',
          maxHeight: 320, display: 'flex', flexDirection: 'column', minWidth: 300,
        }}>
          <div style={{ padding: '8px 10px', borderBottom: '1px solid #e9ecef' }}>
            <input type="text" className="form-control form-control-sm" placeholder={placeholder}
              value={search} onChange={(e) => setSearch(e.target.value)} autoFocus style={{ border: '1px solid #dee2e6' }} />
          </div>
          <div style={{ overflowY: 'auto', maxHeight: 250 }}>
            <label style={{
              display: 'flex', alignItems: 'center', gap: 8, padding: '7px 14px', cursor: 'pointer', fontSize: '0.82rem',
              backgroundColor: selected.size === 0 ? '#e8edf8' : 'transparent', fontWeight: selected.size === 0 ? 600 : 400, borderBottom: '1px solid #e9ecef',
            }}
              onMouseEnter={(e) => { if (selected.size > 0) e.currentTarget.style.backgroundColor = '#f8f9fa'; }}
              onMouseLeave={(e) => { if (selected.size > 0) e.currentTarget.style.backgroundColor = selected.size === 0 ? '#e8edf8' : 'transparent'; }}
            >
              <input type="checkbox" checked={selected.size === 0} onChange={selectAll} style={{ accentColor: '#343990' }} />
              {allLabel}
            </label>
            {filtered.length === 0 ? (
              <div style={{ padding: '12px 14px', color: '#adb5bd', fontSize: '0.82rem', textAlign: 'center' }}>Nenhum resultado</div>
            ) : (
              filtered.map((opt) => {
                const isChecked = selected.has(opt.value);
                return (
                  <label key={opt.value} style={{
                    display: 'flex', alignItems: 'center', gap: 8, padding: '7px 14px', cursor: 'pointer', fontSize: '0.82rem',
                    backgroundColor: isChecked ? '#e8edf8' : 'transparent', fontWeight: isChecked ? 600 : 400, borderTop: '1px solid #f0f2f5',
                  }}
                    onMouseEnter={(e) => { if (!isChecked) e.currentTarget.style.backgroundColor = '#f8f9fa'; }}
                    onMouseLeave={(e) => { if (!isChecked) e.currentTarget.style.backgroundColor = isChecked ? '#e8edf8' : 'transparent'; }}
                  >
                    <input type="checkbox" checked={isChecked} onChange={() => toggleOption(opt.value)} style={{ accentColor: '#343990' }} />
                    {opt.label}
                  </label>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* =====================================================================
   Tooltip customizado estilo card moderno
   ===================================================================== */

const METRICA_COLORS: Record<string, string> = {
  Reserva: '#343990', Empenho: '#d4a017', 'Liquidação': '#dc3545', PD: '#1B998B', OB: '#e74c3c',
};

interface CustomTooltipProps {
  active?: boolean;
  payload?: Array<{ name: string; value: number; dataKey: string }>;
  label?: string;
  data?: Record<string, number>[];
  formatValue: (v: number) => string;
}

function CustomChartTooltip({ active, payload, label, data, formatValue }: CustomTooltipProps) {
  if (!active || !payload?.length) return null;
  const meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];
  const idx = meses.indexOf(label || '');

  return (
    <div style={{ background: '#fff', borderRadius: 12, padding: '14px 18px', boxShadow: '0 8px 30px rgba(0,0,0,0.15)', border: '1px solid #eee', minWidth: 220 }}>
      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#212529', marginBottom: 10 }}>{label}</div>
      {payload.map((entry) => {
        const color = METRICA_COLORS[entry.name] || '#6c757d';
        let pctChange: number | null = null;
        if (data && idx > 0) {
          const prev = data[idx - 1]?.[entry.dataKey];
          if (typeof prev === 'number' && prev > 0) { pctChange = ((entry.value - prev) / prev) * 100; }
          else if (typeof prev === 'number' && prev === 0 && entry.value > 0) { pctChange = 100; }
        }
        return (
          <div key={entry.name} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <span style={{ width: 10, height: 10, borderRadius: '50%', backgroundColor: color, flexShrink: 0 }} />
            <span style={{ fontSize: '0.8rem', color: '#6c757d', minWidth: 70 }}>{entry.name}</span>
            <span style={{ fontSize: '0.9rem', fontWeight: 700, color: entry.value > 0 ? '#198754' : '#adb5bd', marginLeft: 'auto' }}>
              {formatValue(entry.value)}
            </span>
            {pctChange !== null && (
              <span style={{ fontSize: '0.72rem', fontWeight: 600, color: pctChange >= 0 ? '#198754' : '#dc3545', marginLeft: 4, whiteSpace: 'nowrap' }}>
                {pctChange >= 0 ? '↗' : '↘'}{pctChange >= 0 ? '+' : ''}{pctChange.toFixed(1)}%
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}

/* =====================================================================
   Componente principal - Programação de Desembolso (PD)
   ===================================================================== */

export default function Pagamentos() {
  const { year, setYear } = useYearFilter();
  const { format } = useCurrency();

  // Estados
  const [sortKey, setSortKey] = useState<SortKey>('pd');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [page, setPage] = useState(1);

  // Filtros
  const [mes, setMes] = useState('');
  const [acao, setAcao] = useState('');
  const [nat, setNat] = useState('');
  const [fonte, setFonte] = useState('');
  const [busca, setBusca] = useState('');
  const [statusExec, setStatusExec] = useState('');
  const [appliedFilters, setAppliedFilters] = useState<Record<string, string>>({});

  // Opções de filtro — cascata Ação → Fonte → Natureza
  const filtrosParams = useMemo(() => {
    const p: Record<string, string | number> = { ano: year };
    if (acao) p.acao = acao;
    if (fonte) p.fonte = fonte;
    return p;
  }, [year, acao, fonte]);
  const filtros = useApi<FiltrosOrcamentario>('/dashboards/api/filtros-orcamentario', filtrosParams);

  // Montar params com filtros aplicados
  const apiParams = useMemo(() => {
    const p: Record<string, string | number> = { ano: year };
    if (appliedFilters.mes) p.mes = appliedFilters.mes;
    if (appliedFilters.acao) p.acao = appliedFilters.acao;
    if (appliedFilters.natureza) p.natureza = appliedFilters.natureza;
    if (appliedFilters.fonte) p.fonte = appliedFilters.fonte;
    if (appliedFilters.statusExec) p.status_execucao = appliedFilters.statusExec;
    return p;
  }, [year, appliedFilters]);

  const handleFiltrar = () => { setAppliedFilters({ mes, acao, natureza: nat, fonte, statusExec }); setPage(1); };
  const handleLimpar = () => { setMes(''); setAcao(''); setNat(''); setFonte(''); setBusca(''); setStatusExec(''); setAppliedFilters({}); setPage(1); };

  // API calls
  const kpis = useApi<OrcamentarioKpis>('/dashboards/api/kpis-orcamentario', apiParams);
  const pdStatus = useApi<{ labels: string[]; series: number[]; colors: string[]; qtd_pds: number[]; qtd_credores: number[] }>('/dashboards/api/pd-por-status', apiParams);
  const pdNatureza = useApi<CategorySeriesResponse>('/dashboards/api/pd-por-natureza', apiParams);
  const tabela = useApi<{ rows: PdRow[] }>('/dashboards/api/listagem-pd', apiParams);

  const k = kpis.data;
  const dot = k?.dotacao || 1;

  // Sorted + filtered table rows
  const sortedRows = useMemo(() => {
    if (!tabela.data?.rows) return [];
    let rows = tabela.data.rows;
    if (busca.trim()) {
      const q = busca.trim().toLowerCase();
      rows = rows.filter((r) => r.credor.toLowerCase().includes(q) || r.codigo.toLowerCase().includes(q));
    }
    return sortRows(rows, sortKey, sortDir);
  }, [tabela.data, sortKey, sortDir, busca]);

  const totalPages = Math.ceil(sortedRows.length / PAGE_SIZE);
  const pagedRows = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sortedRows.slice(start, start + PAGE_SIZE);
  }, [sortedRows, page]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) { setSortDir((d) => (d === 'asc' ? 'desc' : 'asc')); }
    else { setSortKey(key); setSortDir('desc'); }
    setPage(1);
  };

  const sortIcon = (key: SortKey) => {
    if (sortKey !== key) return <i className="bi bi-arrow-down-up ms-1 opacity-25" style={{ fontSize: '0.7rem' }} />;
    return sortDir === 'asc'
      ? <i className="bi bi-sort-up ms-1" style={{ fontSize: '0.7rem' }} />
      : <i className="bi bi-sort-down ms-1" style={{ fontSize: '0.7rem' }} />;
  };

  return (
    <>
      {/* Tooltip CSS */}
      <style>{`
        .contrato-link { position: relative; }
        .contrato-link:hover { text-decoration: underline; }
        .contrato-link[data-tooltip]:not([data-tooltip=""]):hover::after {
          content: attr(data-tooltip); position: absolute; left: 0; top: calc(100% + 6px); z-index: 1000;
          background: #212529; color: #fff; font-size: 0.78rem; font-weight: 400; line-height: 1.4;
          padding: 8px 12px; border-radius: 6px; white-space: normal; width: max-content; max-width: 360px;
          box-shadow: 0 4px 12px rgba(0,0,0,0.25); pointer-events: none;
        }
        .contrato-link[data-tooltip]:not([data-tooltip=""]):hover::before {
          content: ''; position: absolute; left: 12px; top: calc(100% + 0px); z-index: 1001;
          border: 6px solid transparent; border-bottom-color: #212529; pointer-events: none;
        }
      `}</style>

      {/* Header */}
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h4 className="mb-1 fw-bold">Programação de Desembolso (PD)</h4>
          <small className="text-muted">Visão geral da Programação de Desembolso SEAD</small>
        </div>
      </div>

      {/* Barra de Filtros */}
      <div className="card border-0 shadow-sm mb-4" style={{ borderRadius: 10, overflow: 'visible', position: 'relative', zIndex: 20 }}>
        <div className="card-body py-3 px-4" style={{ overflow: 'visible' }}>
          <div className="row g-3 align-items-end">
            <div className="col-auto" style={{ minWidth: 100 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Ano</label>
              <YearSelector value={year} onChange={(y) => { setYear(y); setAppliedFilters({}); setMes(''); setAcao(''); setNat(''); setFonte(''); }} />
            </div>
            <div className="col" style={{ minWidth: 150 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Mês</label>
              <SearchableSelect value={mes} onChange={setMes} allLabel="Todos" placeholder="Pesquisar mês..."
                options={(filtros.data?.meses || []).map((m) => ({ value: String(m.valor), label: m.label || '' }))} />
            </div>
            <div className="col" style={{ minWidth: 200 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Ação</label>
              <SearchableSelect value={acao} onChange={(v) => { setAcao(v); setFonte(''); setNat(''); }} allLabel="Todas" placeholder="Pesquisar ação..."
                options={(filtros.data?.acoes || []).map((a) => ({ value: String(a.codigo), label: `${a.codigo} - ${a.descricao}` }))} />
            </div>
            <div className="col" style={{ minWidth: 180 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Fonte</label>
              <SearchableSelect value={fonte} onChange={(v) => { setFonte(v); setNat(''); }} allLabel="Todas" placeholder="Pesquisar fonte..."
                options={(filtros.data?.fontes || []).map((f) => ({ value: String(f.codigo), label: `${f.codigo} - ${f.descricao}` }))} />
            </div>
            <div className="col" style={{ minWidth: 200 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Natureza</label>
              <SearchableSelect value={nat} onChange={setNat} allLabel="Todas" placeholder="Pesquisar natureza..."
                options={(filtros.data?.naturezas || []).map((n) => ({ value: String(n.codigo), label: `${n.codigo} - ${n.descricao}` }))} />
            </div>
            <div className="col" style={{ minWidth: 170 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Status Execução</label>
              <select className="form-select form-select-sm" value={statusExec} onChange={(e) => setStatusExec(e.target.value)}>
                <option value="">Todos</option>
                <option value="STATUS_EXECUTADA">Executada</option>
                <option value="STATUS_DISPONIVEL">Em Aberto</option>
                <option value="STATUS_ERRO">Com Erro</option>
                <option value="STATUS_CANCELADA">Cancelada</option>
              </select>
            </div>
            <div className="col" style={{ minWidth: 220 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Credor / Contrato</label>
              <div className="input-group input-group-sm">
                <span className="input-group-text bg-white"><i className="bi bi-search" style={{ fontSize: '0.75rem' }}></i></span>
                <input type="text" className="form-control form-control-sm" placeholder="Buscar credor ou contrato..."
                  value={busca} onChange={(e) => { setBusca(e.target.value); setPage(1); }} />
              </div>
            </div>
            <div className="col-auto d-flex gap-2">
              <button className="btn btn-sm btn-primary d-flex align-items-center gap-1" style={{ backgroundColor: '#343990', borderColor: '#343990' }} onClick={handleFiltrar}>
                <i className="bi bi-funnel-fill"></i> Filtrar
              </button>
              <button className="btn btn-sm btn-outline-secondary d-flex align-items-center gap-1" onClick={handleLimpar}>
                <i className="bi bi-x-lg"></i> Limpar
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* KPI Cards */}
      {kpis.loading ? (
        <LoadingSpinner />
      ) : k ? (
        <div className="row g-3 mb-4">
          <div className="col-xl col-md-4 col-6">
            <OrcKpiCard label="PD" value={k.pd} pct={(k.pd / dot) * 100} color={KPI_COLORS.pd} format={format} />
          </div>
          <div className="col-xl col-md-4 col-6">
            <OrcKpiCard label="PD (em aberto)" value={k.pd_aberto ?? 0} pct={((k.pd_aberto ?? 0) / dot) * 100} color={KPI_COLORS.pd_aberto} format={format} />
          </div>
        </div>
      ) : null}

      {/* Charts Row */}
      <div className="row g-3 mb-4 align-items-stretch">
        {/* Pie Chart - PD por Status */}
        <div className="col-lg-5 d-flex">
          <ChartCard
            title="PD por Status de Execução"
            subtitle={`${pdStatus.data?.labels?.length || 0} status`}
            icon="bi bi-pie-chart"
            loading={pdStatus.loading}
            empty={!pdStatus.data?.labels?.length}
            className="w-100"
          >
            {pdStatus.data && (() => {
              const pieData = pdStatus.data.labels.map((label, i) => ({
                name: label,
                value: pdStatus.data!.series[i] || 0,
                color: pdStatus.data!.colors[i] || '#adb5bd',
                qtdPds: pdStatus.data!.qtd_pds?.[i] || 0,
                qtdCredores: pdStatus.data!.qtd_credores?.[i] || 0,
              }));
              const total = pieData.reduce((s, d) => s + d.value, 0);
              return (
                <>
                  <ResponsiveContainer width="100%" height={280}>
                    <PieChart>
                      <Pie
                        data={pieData}
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={110}
                        dataKey="value"
                        paddingAngle={2}
                        stroke="none"
                      >
                        {pieData.map((entry, i) => (
                          <Cell key={i} fill={entry.color} />
                        ))}
                      </Pie>
                      <Tooltip
                        content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0].payload;
                          const pct = total > 0 ? ((d.value / total) * 100).toFixed(1) : '0';
                          return (
                            <div style={{
                              background: '#fff', borderRadius: 12, padding: '14px 18px',
                              boxShadow: '0 8px 30px rgba(0,0,0,0.15)', border: '1px solid #eee', minWidth: 200,
                            }}>
                              <div style={{ fontSize: '0.95rem', fontWeight: 700, color: '#212529', marginBottom: 8 }}>
                                {d.name}
                              </div>
                              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                                <span style={{ width: 10, height: 10, borderRadius: '50%', backgroundColor: d.color }} />
                                <span style={{ fontSize: '0.95rem', fontWeight: 700, color: '#198754' }}>
                                  {format(d.value)}
                                </span>
                                <span style={{ fontSize: '0.78rem', color: '#6c757d', marginLeft: 'auto' }}>
                                  {pct}%
                                </span>
                              </div>
                              <div style={{ borderTop: '1px solid #e9ecef', paddingTop: 6, marginTop: 2 }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.78rem', color: '#495057', marginBottom: 3 }}>
                                  <span>Qtde de Credores</span>
                                  <span style={{ fontWeight: 700 }}>{d.qtdCredores?.toLocaleString('pt-BR') || 0}</span>
                                </div>
                                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.78rem', color: '#495057' }}>
                                  <span>Qtde de PDs</span>
                                  <span style={{ fontWeight: 700 }}>{d.qtdPds?.toLocaleString('pt-BR') || 0}</span>
                                </div>
                              </div>
                            </div>
                          );
                        }}
                        wrapperStyle={{ outline: 'none' }}
                      />
                    </PieChart>
                  </ResponsiveContainer>
                  {/* Legenda */}
                  <div className="d-flex flex-wrap justify-content-center gap-3 mt-2 px-2">
                    {pieData.map((d, i) => {
                      const pct = total > 0 ? ((d.value / total) * 100).toFixed(1) : '0';
                      return (
                        <div key={i} className="text-center" style={{ minWidth: 100 }}>
                          <div className="d-flex align-items-center justify-content-center gap-1 mb-1">
                            <span style={{ width: 10, height: 10, borderRadius: '50%', backgroundColor: d.color, display: 'inline-block' }} />
                            <span style={{ fontSize: '0.75rem', color: '#6c757d', fontWeight: 600 }}>{d.name}</span>
                          </div>
                          <div style={{ fontSize: '0.85rem', fontWeight: 700, color: '#212529' }}>{format(d.value)}</div>
                          <div style={{ fontSize: '0.72rem', color: '#999' }}>{pct}%</div>
                        </div>
                      );
                    })}
                  </div>
                </>
              );
            })()}
          </ChartCard>
        </div>

        {/* Bar Chart - PD por Natureza */}
        <div className="col-lg-7 d-flex">
          <ChartCard
            title="PD por Natureza de Despesa"
            subtitle={`${pdNatureza.data?.categories?.length || 0} naturezas`}
            icon="bi bi-bar-chart"
            loading={pdNatureza.loading}
            empty={!pdNatureza.data?.categories?.length}
            className="w-100"
          >
            {pdNatureza.data && (
              <>
                <ResponsiveContainer width="100%" height={350}>
                  <BarChart data={adaptCategorySeries(pdNatureza.data)} layout="vertical" margin={{ top: 5, right: 20, left: 5, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" tick={{ fontSize: 10 }} tickFormatter={fmtCompact} />
                    <YAxis type="category" dataKey="name" tick={{ fontSize: 9 }} width={70} />
                    <Tooltip
                      content={({ active, payload, label }) => {
                        if (!active || !payload?.length) return null;
                        const idx = pdNatureza.data?.categories?.indexOf(String(label)) ?? -1;
                        const fullLabel = (pdNatureza.data as any)?.labels?.[idx] || label;
                        // Calcular total geral de todas as naturezas para o percentual
                        const chartData = adaptCategorySeries(pdNatureza.data!);
                        const grandTotal = chartData.reduce((sum: number, d: any) => {
                          return sum + (Number(d['PD Executada'] || 0)) + (Number(d['PD (em aberto)'] || 0));
                        }, 0);
                        // Total desta natureza
                        const rowTotal = payload.reduce((s: number, e: any) => s + Number(e.value || 0), 0);
                        return (
                          <div style={{ background: '#fff', borderRadius: 12, padding: '14px 18px', boxShadow: '0 8px 30px rgba(0,0,0,0.15)', border: '1px solid #eee', minWidth: 240 }}>
                            <div style={{ fontSize: '0.82rem', fontWeight: 700, color: '#212529', marginBottom: 8, maxWidth: 300, lineHeight: 1.3 }}>{fullLabel}</div>
                            {payload.map((e: any, i: number) => {
                              const val = Number(e.value || 0);
                              const itemPctNat = rowTotal > 0 ? ((val / rowTotal) * 100).toFixed(2) : '0.00';
                              return (
                                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                                  <span style={{ width: 10, height: 10, borderRadius: '50%', backgroundColor: e.fill || e.color }} />
                                  <span style={{ fontSize: '0.78rem', color: '#6c757d', minWidth: 90 }}>{e.name}</span>
                                  <span style={{ fontSize: '0.9rem', fontWeight: 700, color: '#198754', marginLeft: 'auto' }}>{format(val)}</span>
                                  <span style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d', minWidth: 55, textAlign: 'right' }}>({itemPctNat}%)</span>
                                </div>
                              );
                            })}
                            <div style={{ borderTop: '1px solid #e9ecef', marginTop: 8, paddingTop: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                              <span style={{ fontSize: '0.78rem', color: '#6c757d', fontWeight: 600 }}>Total Natureza</span>
                              <span style={{ fontSize: '0.88rem', fontWeight: 700, color: '#212529' }}>{format(rowTotal)}</span>
                            </div>
                          </div>
                        );
                      }}
                      wrapperStyle={{ outline: 'none' }}
                    />
                    <Legend />
                    <Bar dataKey="PD Executada" stackId="pd" fill="#1B998B" maxBarSize={22} />
                    <Bar dataKey="PD (em aberto)" stackId="pd" fill="#fd7e14" radius={[0, 4, 4, 0]} maxBarSize={22} />
                  </BarChart>
                </ResponsiveContainer>
              </>
            )}
          </ChartCard>
        </div>
      </div>

      {/* ============ Tabela ============ */}
      <div className="card mb-4 border-0 shadow-sm" style={{ borderRadius: 12, overflow: 'hidden' }}>
        <div className="d-flex justify-content-between align-items-center px-4 py-3 bg-white" style={{ borderBottom: '2px solid #343990' }}>
          <h6 className="mb-0 fw-bold" style={{ color: '#343990' }}>Listagem de PD</h6>
          <div className="d-flex align-items-center gap-3">
            {sortedRows.length > 0 && (
              <button className="btn btn-sm btn-outline-success d-flex align-items-center gap-1" onClick={() => exportToExcel(sortedRows, format)}>
                <i className="bi bi-file-earmark-excel"></i> Exportar Excel
              </button>
            )}
            <span className="badge rounded-pill" style={{ backgroundColor: '#343990', fontSize: '0.78rem', padding: '6px 14px' }}>{sortedRows.length} PDs</span>
          </div>
        </div>

        {tabela.loading ? (
          <div className="py-5"><LoadingSpinner /></div>
        ) : sortedRows.length === 0 ? (
          <div className="text-center text-muted py-5">Sem dados disponíveis</div>
        ) : (
          <>
            <div className="table-responsive">
              <table className="table mb-0" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
                <thead>
                  <tr>
                    {([
                      { key: 'codigo' as SortKey, label: 'Código', align: 'start' },
                      { key: 'statusDocumento' as SortKey, label: 'Status Documento', align: 'center' },
                      { key: 'codNatureza' as SortKey, label: 'Cód. Natureza', align: 'center' },
                      { key: 'credor' as SortKey, label: 'Credor', align: 'start' },
                      { key: 'valor' as SortKey, label: 'Valor', align: 'end' },
                      { key: 'statusExecucao' as SortKey, label: 'Status Execução', align: 'center' },
                      { key: 'competencia' as SortKey, label: 'Competência', align: 'center' },
                    ]).map((col) => (
                      <th key={col.key} className={`text-${col.align}`} onClick={() => handleSort(col.key)}
                        style={{
                          backgroundColor: '#343990', color: '#fff', cursor: 'pointer', padding: '14px 20px',
                          fontWeight: 600, fontSize: '0.78rem', letterSpacing: '0.02em', borderBottom: 'none', whiteSpace: 'nowrap',
                        }}
                      >
                        {col.label} {sortIcon(col.key)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pagedRows.map((row, i) => {
                    const statusColor: Record<string, { bg: string; text: string }> = {
                      'STATUS_EXECUTADA': { bg: '#d1e7dd', text: '#0f5132' },
                      'STATUS_DISPONIVEL': { bg: '#fff3cd', text: '#664d03' },
                      'STATUS_ERRO': { bg: '#f8d7da', text: '#842029' },
                      'STATUS_CANCELADA': { bg: '#e2e3e5', text: '#41464b' },
                    };
                    const sc = statusColor[row.statusExecucaoRaw] || { bg: '#e2e3e5', text: '#41464b' };
                    return (
                      <tr key={i} style={{ backgroundColor: '#fff', borderBottom: '1px solid #e9ecef', transition: 'background-color 0.15s' }}
                        onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = '#f8f9fc')}
                        onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = '#fff')}
                      >
                        <td style={{ padding: '14px 20px', fontWeight: 700, fontSize: '0.85rem', verticalAlign: 'middle', whiteSpace: 'nowrap', color: '#343990' }}>
                          {row.codigo}
                        </td>
                        <td className="text-center" style={{ padding: '14px 20px', fontSize: '0.8rem', verticalAlign: 'middle' }}>
                          <span style={{ fontSize: '0.75rem', padding: '3px 10px', borderRadius: 20, backgroundColor: '#d1e7dd', color: '#0f5132', fontWeight: 600 }}>
                            {row.statusDocumento}
                          </span>
                        </td>
                        <td className="text-center" style={{ padding: '14px 20px', fontSize: '0.85rem', verticalAlign: 'middle', fontWeight: 600, color: '#495057' }}>
                          {row.codNatureza}
                        </td>
                        <td style={{ padding: '14px 20px', color: '#495057', fontSize: '0.83rem', verticalAlign: 'middle', lineHeight: 1.4, maxWidth: 350 }}>
                          {row.credor}
                        </td>
                        <td className="text-end" style={{
                          padding: '14px 20px', verticalAlign: 'middle', whiteSpace: 'nowrap', fontWeight: 700, fontSize: '0.85rem',
                          color: row.valor > 0 ? '#198754' : '#adb5bd',
                        }}>
                          {row.valor > 0 ? fmtBRL(row.valor) : '—'}
                        </td>
                        <td className="text-center" style={{ padding: '14px 20px', verticalAlign: 'middle' }}>
                          <span style={{
                            fontSize: '0.75rem', padding: '3px 10px', borderRadius: 20,
                            backgroundColor: sc.bg, color: sc.text, fontWeight: 600,
                          }}>
                            {row.statusExecucao}
                          </span>
                        </td>
                        <td className="text-center" style={{ padding: '14px 20px', fontSize: '0.85rem', verticalAlign: 'middle', fontWeight: 600, color: '#495057', whiteSpace: 'nowrap' }}>
                          {row.competencia || '—'}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {/* Paginação */}
            {totalPages > 1 && (
              <div className="d-flex justify-content-between align-items-center px-4 py-3" style={{ borderTop: '1px solid #e9ecef', backgroundColor: '#fafbfc' }}>
                <small className="text-muted">
                  Mostrando {(page - 1) * PAGE_SIZE + 1}–{Math.min(page * PAGE_SIZE, sortedRows.length)} de {sortedRows.length}
                </small>
                <nav>
                  <ul className="pagination pagination-sm mb-0">
                    <li className={`page-item ${page <= 1 ? 'disabled' : ''}`}>
                      <button className="page-link" onClick={() => setPage(page - 1)} disabled={page <= 1}><i className="bi bi-chevron-left" /></button>
                    </li>
                    {Array.from({ length: totalPages }, (_, i) => i + 1)
                      .filter((p) => p === 1 || p === totalPages || Math.abs(p - page) <= 2)
                      .reduce<(number | '...')[]>((acc, p, idx, arr) => {
                        if (idx > 0 && p - (arr[idx - 1] as number) > 1) acc.push('...');
                        acc.push(p);
                        return acc;
                      }, [])
                      .map((p, idx) =>
                        p === '...' ? (
                          <li key={`dots-${idx}`} className="page-item disabled"><span className="page-link">...</span></li>
                        ) : (
                          <li key={p} className={`page-item ${page === p ? 'active' : ''}`}>
                            <button className="page-link" onClick={() => setPage(p as number)}
                              style={page === p ? { backgroundColor: '#343990', borderColor: '#343990' } : {}}>
                              {p}
                            </button>
                          </li>
                        )
                      )}
                    <li className={`page-item ${page >= totalPages ? 'disabled' : ''}`}>
                      <button className="page-link" onClick={() => setPage(page + 1)} disabled={page >= totalPages}><i className="bi bi-chevron-right" /></button>
                    </li>
                  </ul>
                </nav>
              </div>
            )}
          </>
        )}
      </div>
    </>
  );
}
