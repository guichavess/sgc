import type {
  CategorySeriesResponse,
  CategoryDataResponse,
  DonutResponse,
  GanttResponse,
  RechartsDataPoint,
  DonutDataPoint,
  GanttData,
} from '../types/api';

/**
 * Adapta {categories, series[{name, data}]} → [{name:"Jan", Empenhado:10, Liquidado:5}]
 * Usado para bar charts com múltiplas séries e area charts.
 */
export function adaptCategorySeries(
  apiData: CategorySeriesResponse
): RechartsDataPoint[] {
  return apiData.categories.map((cat, i) => {
    const point: RechartsDataPoint = { name: cat };
    apiData.series.forEach((s) => {
      point[s.name] = s.data[i] ?? 0;
    });
    return point;
  });
}

/**
 * Adapta {categories, data[], colors?[]} → [{name, value, color?}]
 * Usado para bar charts com dados simples (etapas, competência, tempo médio).
 */
export function adaptCategoryData(
  apiData: CategoryDataResponse
): DonutDataPoint[] {
  return apiData.categories.map((cat, i) => ({
    name: cat,
    value: apiData.data[i] ?? 0,
    color: apiData.colors?.[i],
  }));
}

/**
 * Adapta {labels, series, colors?} → [{name, value, color?}]
 * Usado para donut/pie charts.
 */
export function adaptDonut(apiData: DonutResponse): DonutDataPoint[] {
  return apiData.labels.map((label, i) => ({
    name: label,
    value: apiData.series[i] ?? 0,
    color: apiData.colors?.[i],
  }));
}

/**
 * Adapta {series[{data[{x, y:[ts1,ts2]}]}]} → {items, minDate, maxDate}
 * Usado para o gráfico Gantt de vigência de contratos.
 */
export function adaptGantt(apiData: GanttResponse): GanttData {
  const items =
    apiData.series[0]?.data.map((d) => ({
      name: d.x,
      start: d.y[0],
      end: d.y[1],
    })) ?? [];

  const allDates = items.flatMap((i) => [i.start, i.end]);

  return {
    items,
    minDate: allDates.length ? Math.min(...allDates) : 0,
    maxDate: allDates.length ? Math.max(...allDates) : 0,
  };
}
