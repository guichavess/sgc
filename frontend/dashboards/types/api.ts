// Respostas genéricas dos endpoints (formato ApexCharts)

export interface CategorySeriesResponse {
  categories: string[];
  series: Array<{ name: string; data: number[] }>;
}

export interface CategoryDataResponse {
  categories: string[];
  data: number[];
  colors?: string[];
}

export interface DonutResponse {
  labels: string[];
  series: number[];
  colors?: string[];
}

export interface GanttDataPoint {
  x: string;
  y: [number, number];
}

export interface GanttResponse {
  series: Array<{ name: string; data: GanttDataPoint[] }>;
}

export interface VigenciaContrato {
  codigo: string;
  contratado: string;
  fim_vigencia: string;
  dias_restantes: number;
  valor: number;
}

export interface VigenciaResponse {
  contratos: VigenciaContrato[];
}

// KPIs

export interface ConsolidadoKpis {
  total_solicitacoes: number;
  status_counts: Record<string, number>;
  contratos_ativos: number;
  total_contratos: number;
  nes_pendentes: number;
  total_empenhado: number;
  total_liquidado: number;
  saldo_global: number;
}

export interface FinanceiroKpis {
  total_empenhado: number;
  total_liquidado: number;
  saldo_total: number;
  nes_pendentes: number;
}

export interface ContratosKpis {
  total_contratos: number;
  contratos_ativos: number;
  contratos_encerrados: number;
  valor_total: number;
}

// Formato Recharts (após adaptação)

export interface RechartsDataPoint {
  name: string;
  [key: string]: string | number;
}

export interface DonutDataPoint {
  name: string;
  value: number;
  color?: string;
}

export interface GanttItem {
  name: string;
  start: number;
  end: number;
}

export interface GanttData {
  items: GanttItem[];
  minDate: number;
  maxDate: number;
}
