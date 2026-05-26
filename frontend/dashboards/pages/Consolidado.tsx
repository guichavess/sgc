import React, { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, BarChart, Bar,
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
   KPI Card orçamentário com barra de progresso
   ===================================================================== */

const KPI_COLORS: Record<string, string> = {
  reservado: '#343990',
  empenhado: '#d4a017',
  liquidado: '#dc3545',
  pd: '#1B998B',
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
  { key: 'PD', color: '#1B998B' },
  { key: 'OB', color: '#e74c3c' },
];

const METRICA_API_MAP: Record<string, string> = {
  'Reserva': 'reserva',
  'Empenho': 'empenho',
  'Liquidação': 'liquidacao',
  'PD': 'pd',
  'OB': 'ob',
};

/* =====================================================================
   Tabela de contratos com sort, paginação e barras de execução
   ===================================================================== */

type SortKey = keyof TabelaContratoRow;
type SortDir = 'asc' | 'desc';
const PAGE_SIZE = 10;

function sortRows(rows: TabelaContratoRow[], key: SortKey, dir: SortDir) {
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

function exportToExcel(rows: TabelaContratoRow[], fmt: (v: number) => string) {
  const header = ['Contrato', 'Credor', 'Reserva', 'Empenho', 'Liquidação', 'PD', 'PD (em aberto)', 'Pago'];
  const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const numCell = (v: number) => `<td style="mso-number-format:'#.##0\\,00';text-align:right">${v.toFixed(2).replace('.', ',')}</td>`;

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
      <td>${esc(String(r.contrato))}</td>
      <td>${esc(r.credor)}</td>
      ${numCell(r.reserva)}
      ${numCell(r.empenho)}
      ${numCell(r.liquidacao)}
      ${numCell(r.pd)}
      ${numCell(r.pd_aberto ?? 0)}
      ${numCell(r.ob)}
    </tr>`;
  }
  html += '</tbody></table></body></html>';

  const blob = new Blob(['\uFEFF' + html], { type: 'application/vnd.ms-excel;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'execucao_por_contrato.xls';
  a.click();
  URL.revokeObjectURL(url);
}

/** Formata valor como R$ 1.234.567,89 */
function fmtBRL(v: number): string {
  return 'R$ ' + v.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/** Formata valor compacto para eixo Y (ex: R$ 15Mi, R$ 500K) */
function fmtCompact(v: number): string {
  if (v === 0) return 'R$ 0';
  const abs = Math.abs(v);
  if (abs >= 1_000_000_000) return `R$ ${(v / 1_000_000_000).toFixed(1)}Bi`;
  if (abs >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(1)}Mi`;
  if (abs >= 1_000) return `R$ ${(v / 1_000).toFixed(0)}K`;
  return `R$ ${v.toFixed(0)}`;
}

/* =====================================================================
   Select com busca e múltipla seleção (Searchable Dropdown)
   ===================================================================== */

interface SelectOption {
  value: string;
  label: string;
}

interface SearchableSelectProps {
  options: SelectOption[];
  value: string;            // valores separados por vírgula: "500,700"
  onChange: (value: string) => void;
  placeholder?: string;
  allLabel?: string;
}

function SearchableSelect({ options, value, onChange, placeholder = 'Pesquisar...', allLabel = 'Todos' }: SearchableSelectProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef<HTMLDivElement>(null);

  // Parse valores selecionados
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
    if (next.has(val)) {
      next.delete(val);
    } else {
      next.add(val);
    }
    onChange(Array.from(next).join(','));
  };

  const selectAll = () => {
    onChange('');
    setOpen(false);
  };

  // Label do botão
  let displayLabel: string;
  if (selected.size === 0) {
    displayLabel = allLabel;
  } else if (selected.size === 1) {
    const opt = options.find((o) => selected.has(o.value));
    displayLabel = opt?.label || value;
  } else {
    displayLabel = `${selected.size} selecionados`;
  }

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        type="button"
        className="form-select form-select-sm text-start"
        onClick={() => { setOpen(!open); setSearch(''); }}
        style={{
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          backgroundColor: '#fff', cursor: 'pointer',
        }}
      >
        {displayLabel}
      </button>

      {open && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 1050,
          backgroundColor: '#fff', borderRadius: 8, marginTop: 4,
          boxShadow: '0 8px 30px rgba(0,0,0,0.18)', border: '1px solid #dee2e6',
          maxHeight: 320, display: 'flex', flexDirection: 'column',
          minWidth: 300,
        }}>
          {/* Campo de busca */}
          <div style={{ padding: '8px 10px', borderBottom: '1px solid #e9ecef' }}>
            <input
              type="text"
              className="form-control form-control-sm"
              placeholder={placeholder}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              autoFocus
              style={{ border: '1px solid #dee2e6' }}
            />
          </div>

          {/* Lista com checkboxes */}
          <div style={{ overflowY: 'auto', maxHeight: 250 }}>
            {/* Opção "Todos" (limpa seleção) */}
            <label
              style={{
                display: 'flex', alignItems: 'center', gap: 8,
                padding: '7px 14px', cursor: 'pointer', fontSize: '0.82rem',
                backgroundColor: selected.size === 0 ? '#e8edf8' : 'transparent',
                fontWeight: selected.size === 0 ? 600 : 400,
                borderBottom: '1px solid #e9ecef',
              }}
              onMouseEnter={(e) => { if (selected.size > 0) e.currentTarget.style.backgroundColor = '#f8f9fa'; }}
              onMouseLeave={(e) => { if (selected.size > 0) e.currentTarget.style.backgroundColor = selected.size === 0 ? '#e8edf8' : 'transparent'; }}
            >
              <input
                type="checkbox"
                checked={selected.size === 0}
                onChange={selectAll}
                style={{ accentColor: '#343990' }}
              />
              {allLabel}
            </label>

            {filtered.length === 0 ? (
              <div style={{ padding: '12px 14px', color: '#adb5bd', fontSize: '0.82rem', textAlign: 'center' }}>
                Nenhum resultado
              </div>
            ) : (
              filtered.map((opt) => {
                const isChecked = selected.has(opt.value);
                return (
                  <label
                    key={opt.value}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 8,
                      padding: '7px 14px', cursor: 'pointer', fontSize: '0.82rem',
                      backgroundColor: isChecked ? '#e8edf8' : 'transparent',
                      fontWeight: isChecked ? 600 : 400,
                      borderTop: '1px solid #f0f2f5',
                    }}
                    onMouseEnter={(e) => { if (!isChecked) e.currentTarget.style.backgroundColor = '#f8f9fa'; }}
                    onMouseLeave={(e) => { if (!isChecked) e.currentTarget.style.backgroundColor = isChecked ? '#e8edf8' : 'transparent'; }}
                  >
                    <input
                      type="checkbox"
                      checked={isChecked}
                      onChange={() => toggleOption(opt.value)}
                      style={{ accentColor: '#343990' }}
                    />
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

  // Encontra o index do mês atual e anterior para calcular variação
  const meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];
  const idx = meses.indexOf(label || '');

  return (
    <div style={{
      background: '#fff',
      borderRadius: 12,
      padding: '14px 18px',
      boxShadow: '0 8px 30px rgba(0,0,0,0.15)',
      border: '1px solid #eee',
      minWidth: 220,
    }}>
      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#212529', marginBottom: 10 }}>
        {label}
      </div>
      {payload.map((entry) => {
        const color = METRICA_COLORS[entry.name] || '#6c757d';
        // Variação com mês anterior
        let pctChange: number | null = null;
        if (data && idx > 0) {
          const prev = data[idx - 1]?.[entry.dataKey];
          if (typeof prev === 'number' && prev > 0) {
            pctChange = ((entry.value - prev) / prev) * 100;
          } else if (typeof prev === 'number' && prev === 0 && entry.value > 0) {
            pctChange = 100;
          }
        }

        return (
          <div key={entry.name} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <span style={{
              width: 10, height: 10, borderRadius: '50%', backgroundColor: color, flexShrink: 0,
            }} />
            <span style={{ fontSize: '0.8rem', color: '#6c757d', minWidth: 70 }}>{entry.name}</span>
            <span style={{ fontSize: '0.9rem', fontWeight: 700, color: entry.value > 0 ? '#198754' : '#adb5bd', marginLeft: 'auto' }}>
              {formatValue(entry.value)}
            </span>
            {pctChange !== null && (
              <span style={{
                fontSize: '0.72rem',
                fontWeight: 600,
                color: pctChange >= 0 ? '#198754' : '#dc3545',
                marginLeft: 4,
                whiteSpace: 'nowrap',
              }}>
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
   Componente principal
   ===================================================================== */

export default function Consolidado() {
  const { year, setYear } = useYearFilter();
  const { format } = useCurrency();

  // Estados
  const [selectedMetricas, setSelectedMetricas] = useState<string[]>(['Empenho']);
  const [natMetrica, setNatMetrica] = useState('empenho');
  const [sortKey, setSortKey] = useState<SortKey>('empenho');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [page, setPage] = useState(1);

  // Filtros
  const [mes, setMes] = useState('');
  const [acao, setAcao] = useState('');
  const [nat, setNat] = useState('');
  const [fonte, setFonte] = useState('');
  const [busca, setBusca] = useState('');
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
    return p;
  }, [year, appliedFilters]);

  const handleFiltrar = () => {
    setAppliedFilters({ mes, acao, natureza: nat, fonte });
    setPage(1);
  };

  const handleLimpar = () => {
    setMes(''); setAcao(''); setNat(''); setFonte(''); setBusca('');
    setAppliedFilters({});
    setPage(1);
  };

  // API calls (usam filtros aplicados)
  const kpis = useApi<OrcamentarioKpis>('/dashboards/api/kpis-orcamentario', apiParams);
  const evolucao = useApi<CategorySeriesResponse>('/dashboards/api/evolucao-orcamentaria', apiParams);
  const natureza = useApi<CategorySeriesResponse>('/dashboards/api/execucao-por-natureza', { ...apiParams, metrica: natMetrica });
  const tabela = useApi<TabelaContratosResponse>('/dashboards/api/tabela-contratos-orcamentario', apiParams);

  const k = kpis.data;
  const dot = k?.dotacao || 1;

  // Toggle métrica no line chart
  const toggleMetrica = useCallback((key: string) => {
    setSelectedMetricas((prev) =>
      prev.includes(key) ? (prev.length > 1 ? prev.filter((m) => m !== key) : prev) : [...prev, key]
    );
  }, []);

  // Sorted + filtered table rows
  const sortedRows = useMemo(() => {
    if (!tabela.data?.rows) return [];
    let rows = tabela.data.rows;
    if (busca.trim()) {
      const q = busca.trim().toLowerCase();
      rows = rows.filter((r) =>
        r.credor.toLowerCase().includes(q) || r.contrato.toLowerCase().includes(q)
      );
    }
    return sortRows(rows, sortKey, sortDir);
  }, [tabela.data, sortKey, sortDir, busca]);

  const totalPages = Math.ceil(sortedRows.length / PAGE_SIZE);
  const pagedRows = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sortedRows.slice(start, start + PAGE_SIZE);
  }, [sortedRows, page]);

  // Calcula o total (empenho) para as barras de %
  const maxEmpenho = useMemo(() => {
    if (!sortedRows.length) return 1;
    return Math.max(...sortedRows.map((r) => r.empenho), 1);
  }, [sortedRows]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
    setPage(1); // Reset para primeira página ao reordenar
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
        .contrato-link { position: relative; cursor: pointer; }
        .contrato-link:hover { text-decoration: underline; }
      `}</style>

      {/* Header */}
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h4 className="mb-1 fw-bold">Execução Financeira</h4>
          <small className="text-muted">Visão geral da Execução financeira SEAD</small>
        </div>
      </div>

      {/* Barra de Filtros */}
      <div className="card border-0 shadow-sm mb-4" style={{ borderRadius: 10, overflow: 'visible', position: 'relative', zIndex: 20 }}>
        <div className="card-body py-3 px-4" style={{ overflow: 'visible' }}>
          <div className="row g-3 align-items-end">
            {/* Ano */}
            <div className="col-auto" style={{ minWidth: 100 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Ano</label>
              <YearSelector value={year} onChange={(y) => { setYear(y); setAppliedFilters({}); setMes(''); setAcao(''); setNat(''); setFonte(''); }} />
            </div>
            {/* Mês */}
            <div className="col" style={{ minWidth: 150 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Mês</label>
              <SearchableSelect
                value={mes}
                onChange={setMes}
                allLabel="Todos"
                placeholder="Pesquisar mês..."
                options={(filtros.data?.meses || []).map((m) => ({ value: String(m.valor), label: m.label || '' }))}
              />
            </div>
            {/* Ação */}
            <div className="col" style={{ minWidth: 200 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Ação</label>
              <SearchableSelect
                value={acao}
                onChange={(v) => { setAcao(v); setFonte(''); setNat(''); }}
                allLabel="Todas"
                placeholder="Pesquisar ação..."
                options={(filtros.data?.acoes || []).map((a) => ({ value: String(a.codigo), label: `${a.codigo} - ${a.descricao}` }))}
              />
            </div>
            {/* Fonte */}
            <div className="col" style={{ minWidth: 180 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Fonte</label>
              <SearchableSelect
                value={fonte}
                onChange={(v) => { setFonte(v); setNat(''); }}
                allLabel="Todas"
                placeholder="Pesquisar fonte..."
                options={(filtros.data?.fontes || []).map((f) => ({ value: String(f.codigo), label: `${f.codigo} - ${f.descricao}` }))}
              />
            </div>
            {/* Natureza */}
            <div className="col" style={{ minWidth: 200 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Natureza</label>
              <SearchableSelect
                value={nat}
                onChange={setNat}
                allLabel="Todas"
                placeholder="Pesquisar natureza..."
                options={(filtros.data?.naturezas || []).map((n) => ({ value: String(n.codigo), label: `${n.codigo} - ${n.descricao}` }))}
              />
            </div>
            {/* Busca por credor/contrato */}
            <div className="col" style={{ minWidth: 220 }}>
              <label className="form-label mb-1" style={{ fontSize: '0.75rem', fontWeight: 600, color: '#6c757d' }}>Credor / Contrato</label>
              <div className="input-group input-group-sm">
                <span className="input-group-text bg-white"><i className="bi bi-search" style={{ fontSize: '0.75rem' }}></i></span>
                <input
                  type="text"
                  className="form-control form-control-sm"
                  placeholder="Buscar credor ou contrato..."
                  value={busca}
                  onChange={(e) => { setBusca(e.target.value); setPage(1); }}
                />
              </div>
            </div>
            {/* Botões */}
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
            <OrcKpiCard label="Reservado" value={k.reservado} pct={(k.reservado / dot) * 100} color={KPI_COLORS.reservado} format={format} />
          </div>
          <div className="col-xl col-md-4 col-6">
            <OrcKpiCard label="Empenhado" value={k.empenhado} pct={(k.empenhado / dot) * 100} color={KPI_COLORS.empenhado} format={format} />
          </div>
          <div className="col-xl col-md-4 col-6">
            <OrcKpiCard label="Liquidado" value={k.liquidado} pct={(k.liquidado / dot) * 100} color={KPI_COLORS.liquidado} format={format} />
          </div>
          <div className="col-xl col-md-4 col-6">
            <OrcKpiCard label="PD" value={k.pd} pct={(k.pd / dot) * 100} color={KPI_COLORS.pd} format={format} />
          </div>
          <div className="col-xl col-md-4 col-6">
            <OrcKpiCard label="PD (em aberto)" value={k.pd_aberto ?? 0} pct={((k.pd_aberto ?? 0) / dot) * 100} color="#fd7e14" format={format} />
          </div>
          <div className="col-xl col-md-4 col-6">
            <OrcKpiCard label="Pago" value={k.pago} pct={(k.pago / dot) * 100} color={KPI_COLORS.pago} format={format} />
          </div>
        </div>
      ) : null}

      {/* Charts Row */}
      <div className="row g-3 mb-4 align-items-stretch">
        {/* Line Chart - Evolução mensal */}
        <div className="col-lg-7 d-flex">
          <ChartCard
            title={`Evolução Mensal ${year}`}
            subtitle="Comparativo mensal por tipo de execução"
            icon="bi bi-graph-up"
            loading={evolucao.loading}
            empty={!evolucao.data?.series?.length}
            className="w-100"
          >
            {evolucao.data && (
              <>
                {/* Metric toggles */}
                <div className="d-flex flex-wrap gap-2 mb-3 px-2">
                  {METRICAS.map((m) => (
                    <button
                      key={m.key}
                      className={`btn btn-sm ${selectedMetricas.includes(m.key) ? '' : 'btn-outline-secondary'}`}
                      style={
                        selectedMetricas.includes(m.key)
                          ? { backgroundColor: m.color, borderColor: m.color, color: '#fff' }
                          : {}
                      }
                      onClick={() => toggleMetrica(m.key)}
                    >
                      {m.key}
                    </button>
                  ))}
                </div>
                <ResponsiveContainer width="100%" height={320}>
                  <LineChart
                    data={adaptCategorySeries(evolucao.data)}
                    margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                    <YAxis tick={{ fontSize: 10 }} tickFormatter={fmtCompact} width={80} />
                    <Tooltip
                      content={<CustomChartTooltip data={adaptCategorySeries(evolucao.data)} formatValue={format} />}
                      wrapperStyle={{ outline: 'none' }}
                    />
                    <Legend />
                    {METRICAS.filter((m) => selectedMetricas.includes(m.key)).map((m) => (
                      <Line
                        key={m.key}
                        type="monotone"
                        dataKey={m.key}
                        stroke={m.color}
                        strokeWidth={2}
                        dot={{ r: 3 }}
                        activeDot={{ r: 5 }}
                      />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              </>
            )}
          </ChartCard>
        </div>

        {/* Bar Chart - Natureza de despesa */}
        <div className="col-lg-5 d-flex">
          <ChartCard
            title="Execução por Natureza de Despesa"
            subtitle={`${natureza.data?.categories?.length || 0} naturezas`}
            icon="bi bi-bar-chart"
            loading={natureza.loading}
            empty={!natureza.data?.categories?.length}
            className="w-100"
          >
            {natureza.data && (
              <>
                <div className="px-2 mb-3">
                  <select
                    className="form-select form-select-sm"
                    value={natMetrica}
                    onChange={(e) => setNatMetrica(e.target.value)}
                    style={{ maxWidth: 180 }}
                  >
                    <option value="reserva">Reserva</option>
                    <option value="empenho">Empenho</option>
                    <option value="liquidacao">Liquidação</option>
                    <option value="pd">PD</option>
                    <option value="ob">OB</option>
                  </select>
                </div>
                <ResponsiveContainer width="100%" height={320}>
                  <BarChart
                    data={adaptCategorySeries(natureza.data)}
                    layout="vertical"
                    margin={{ top: 5, right: 20, left: 5, bottom: 5 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" tick={{ fontSize: 10 }} tickFormatter={fmtCompact} />
                    <YAxis
                      type="category"
                      dataKey="name"
                      tick={{ fontSize: 9 }}
                      width={70}
                    />
                    <Tooltip
                      content={({ active, payload, label }) => {
                        if (!active || !payload?.length) return null;
                        // Buscar label completo (código + nome) do array labels da API
                        const idx = natureza.data?.categories?.indexOf(String(label)) ?? -1;
                        const fullLabel = (natureza.data as any)?.labels?.[idx] || label;
                        return (
                          <div style={{
                            background: '#fff', borderRadius: 12, padding: '12px 16px',
                            boxShadow: '0 8px 30px rgba(0,0,0,0.15)', border: '1px solid #eee', minWidth: 200,
                          }}>
                            <div style={{ fontSize: '0.82rem', fontWeight: 700, color: '#212529', marginBottom: 6, maxWidth: 300, lineHeight: 1.3 }}>
                              {fullLabel}
                            </div>
                            {payload.map((e: any, i: number) => (
                              <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                <span style={{ width: 10, height: 10, borderRadius: '50%', backgroundColor: '#343990' }} />
                                <span style={{ fontSize: '0.9rem', fontWeight: 700, color: '#198754' }}>
                                  {format(Number(e.value))}
                                </span>
                              </div>
                            ))}
                          </div>
                        );
                      }}
                      wrapperStyle={{ outline: 'none' }}
                    />
                    <Bar
                      dataKey={natureza.data.series[0]?.name || 'value'}
                      fill="#343990"
                      radius={[0, 4, 4, 0]}
                      maxBarSize={24}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </>
            )}
          </ChartCard>
        </div>
      </div>

      {/* ============ Tabela - estilo igual à imagem ============ */}
      <div className="card mb-4 border-0 shadow-sm" style={{ borderRadius: 12, overflow: 'visible' }}>
        {/* Barra superior */}
        <div className="d-flex justify-content-between align-items-center px-4 py-3 bg-white" style={{ borderBottom: '2px solid #343990', borderRadius: '12px 12px 0 0' }}>
          <h6 className="mb-0 fw-bold" style={{ color: '#343990' }}>Execução por Contrato</h6>
          <div className="d-flex align-items-center gap-3">
            {sortedRows.length > 0 && (
              <button
                className="btn btn-sm btn-outline-success d-flex align-items-center gap-1"
                onClick={() => exportToExcel(sortedRows, format)}
              >
                <i className="bi bi-file-earmark-excel"></i> Exportar Excel
              </button>
            )}
            <span className="badge rounded-pill" style={{ backgroundColor: '#343990', fontSize: '0.78rem', padding: '6px 14px' }}>
              {sortedRows.length} contratos
            </span>
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
                {/* Cabeçalho roxo */}
                <thead>
                  <tr>
                    {([
                      { key: 'contrato' as SortKey, label: 'Contrato', align: 'start' },
                      { key: 'credor' as SortKey, label: 'Nome do Credor', align: 'start' },
                      { key: 'reserva' as SortKey, label: 'Reserva', align: 'end' },
                      { key: 'empenho' as SortKey, label: 'Empenho', align: 'end' },
                      { key: 'liquidacao' as SortKey, label: 'Liquidação', align: 'end' },
                      { key: 'pd' as SortKey, label: 'PD', align: 'end' },
                      { key: 'pd_aberto' as SortKey, label: 'PD (em aberto)', align: 'end' },
                      { key: 'ob' as SortKey, label: 'Pago', align: 'end' },
                    ]).map((col) => (
                      <th
                        key={col.key}
                        className={`text-${col.align}`}
                        onClick={() => handleSort(col.key)}
                        style={{
                          backgroundColor: '#343990',
                          color: '#fff',
                          cursor: 'pointer',
                          padding: '14px 20px',
                          fontWeight: 600,
                          fontSize: '0.78rem',
                          letterSpacing: '0.02em',
                          borderBottom: 'none',
                          whiteSpace: 'nowrap',
                        }}
                      >
                        {col.label} {sortIcon(col.key)}
                      </th>
                    ))}
                  </tr>
                </thead>

                {/* Linhas */}
                <tbody>
                  {pagedRows.map((row, i) => (
                    <tr
                      key={i}
                      style={{
                        backgroundColor: '#fff',
                        borderBottom: '1px solid #e9ecef',
                        transition: 'background-color 0.15s',
                      }}
                      onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = '#f8f9fc')}
                      onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = '#fff')}
                    >
                      {/* Contrato - link com tooltip do objeto */}
                      <td style={{ padding: '16px 20px', fontWeight: 700, fontSize: '0.85rem', verticalAlign: 'top', whiteSpace: 'nowrap', position: 'relative' }}>
                        <a
                          href={`/prestacoes-contratos/contratos/${row.contrato}/gerenciar`}
                          className="contrato-link"
                          style={{ color: '#343990', textDecoration: 'none' }}
                          onMouseEnter={(e) => {
                            if (!row.objeto) return;
                            const old = document.getElementById('contrato-tooltip');
                            if (old) old.remove();
                            const tip = document.createElement('div');
                            tip.id = 'contrato-tooltip';
                            tip.textContent = row.objeto;
                            const rect = e.currentTarget.getBoundingClientRect();
                            const spaceBelow = window.innerHeight - rect.bottom;
                            const showAbove = spaceBelow < 120;
                            Object.assign(tip.style, {
                              position: 'fixed',
                              left: `${rect.left}px`,
                              ...(showAbove
                                ? { bottom: `${window.innerHeight - rect.top + 8}px` }
                                : { top: `${rect.bottom + 8}px` }
                              ),
                              zIndex: '99999',
                              background: '#212529',
                              color: '#fff',
                              fontSize: '0.78rem',
                              fontWeight: '400',
                              lineHeight: '1.4',
                              padding: '8px 12px',
                              borderRadius: '6px',
                              whiteSpace: 'normal',
                              maxWidth: '360px',
                              boxShadow: '0 4px 12px rgba(0,0,0,0.25)',
                              pointerEvents: 'none',
                            });
                            document.body.appendChild(tip);
                          }}
                          onMouseLeave={() => {
                            const tip = document.getElementById('contrato-tooltip');
                            if (tip) tip.remove();
                          }}
                        >
                          {row.contrato}
                        </a>
                      </td>

                      {/* Credor - texto quebra linha */}
                      <td style={{ padding: '16px 20px', color: '#495057', fontSize: '0.85rem', verticalAlign: 'top', lineHeight: 1.4, maxWidth: 320 }}>
                        {row.credor}
                      </td>

                      {/* Valores monetários - texto verde, alinhado à direita */}
                      {[
                        { val: row.reserva, key: 'reserva' },
                        { val: row.empenho, key: 'empenho' },
                        { val: row.liquidacao, key: 'liquidacao' },
                        { val: row.pd, key: 'pd' },
                        { val: row.pd_aberto ?? 0, key: 'pd_aberto' },
                        { val: row.ob, key: 'ob' },
                      ].map(({ val, key: colKey }, vi) => (
                        <td
                          key={vi}
                          className="text-end"
                          style={{
                            padding: '16px 20px',
                            verticalAlign: 'top',
                            whiteSpace: 'nowrap',
                            fontWeight: 600,
                            fontSize: '0.85rem',
                            color: val > 0 ? '#198754' : '#adb5bd',
                            position: colKey === 'pd_aberto' ? 'relative' : undefined,
                            cursor: colKey === 'pd_aberto' && val > 0 ? 'pointer' : undefined,
                          }}
                          {...(colKey === 'pd_aberto' && val > 0 ? {
                            onMouseEnter: (e: React.MouseEvent<HTMLTableCellElement>) => {
                              const td = e.currentTarget;
                              // Remover tooltip antigo se existir
                              const old = document.getElementById('pd-aberto-tooltip');
                              if (old) old.remove();

                              const pds = row.pd_aberto_pds || [];
                              const tip = document.createElement('div');
                              tip.id = 'pd-aberto-tooltip';
                              tip.innerHTML = `
                                <div style="font-weight:700;font-size:0.8rem;margin-bottom:6px;color:#343990;border-bottom:1px solid #e9ecef;padding-bottom:5px">
                                  PDs em Aberto (${pds.length})
                                </div>
                                ${pds.length === 0 ? '<div style="color:#999;font-size:0.78rem">Nenhuma PD</div>' : ''}
                                <div style="max-height:200px;overflow-y:auto">
                                <table style="width:100%;border-collapse:collapse;font-size:0.78rem">
                                  <thead>
                                    <tr style="border-bottom:1px solid #e9ecef">
                                      <th style="text-align:left;padding:3px 6px;color:#6c757d;font-weight:600">PD</th>
                                      <th style="text-align:left;padding:3px 6px;color:#6c757d;font-weight:600">Competência</th>
                                      <th style="text-align:right;padding:3px 6px;color:#6c757d;font-weight:600">Valor</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    ${pds.map((pd: any) => `
                                      <tr style="border-bottom:1px solid #f1f3f5">
                                        <td style="padding:3px 6px;color:#212529;font-weight:600">${pd.codigo}</td>
                                        <td style="padding:3px 6px;color:#495057">${pd.competencia || '—'}</td>
                                        <td style="padding:3px 6px;text-align:right;color:#198754;font-weight:600">${fmtBRL(pd.valor)}</td>
                                      </tr>
                                    `).join('')}
                                  </tbody>
                                </table>
                                </div>
                              `;
                              const rect = td.getBoundingClientRect();
                              const tipHeight = 250; // estimativa
                              const spaceAbove = rect.top;
                              const showBelow = spaceAbove < tipHeight;

                              Object.assign(tip.style, {
                                position: 'fixed',
                                left: `${Math.max(10, rect.right - 340)}px`,
                                ...(showBelow
                                  ? { top: `${rect.bottom + 6}px` }
                                  : { bottom: `${window.innerHeight - rect.top + 6}px` }
                                ),
                                background: '#fff',
                                borderRadius: '10px',
                                padding: '10px 12px',
                                boxShadow: '0 8px 30px rgba(0,0,0,0.18)',
                                border: '1px solid #e2e5ea',
                                minWidth: '340px',
                                zIndex: '99999',
                                whiteSpace: 'normal',
                              });
                              document.body.appendChild(tip);
                            },
                            onMouseLeave: () => {
                              const tip = document.getElementById('pd-aberto-tooltip');
                              if (tip) tip.remove();
                            },
                          } : {})}
                        >
                          {val > 0 ? fmtBRL(val) : '—'}
                        </td>
                      ))}
                    </tr>
                  ))}
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
                      <button className="page-link" onClick={() => setPage(page - 1)} disabled={page <= 1}>
                        <i className="bi bi-chevron-left" />
                      </button>
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
                          <li key={`dots-${idx}`} className="page-item disabled">
                            <span className="page-link">...</span>
                          </li>
                        ) : (
                          <li key={p} className={`page-item ${page === p ? 'active' : ''}`}>
                            <button
                              className="page-link"
                              onClick={() => setPage(p as number)}
                              style={page === p ? { backgroundColor: '#343990', borderColor: '#343990' } : {}}
                            >
                              {p}
                            </button>
                          </li>
                        )
                      )}
                    <li className={`page-item ${page >= totalPages ? 'disabled' : ''}`}>
                      <button className="page-link" onClick={() => setPage(page + 1)} disabled={page >= totalPages}>
                        <i className="bi bi-chevron-right" />
                      </button>
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
