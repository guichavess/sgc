import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import type { GanttItem } from '../../types/api';

interface GanttChartProps {
  items: GanttItem[];
  minDate: number;
  maxDate: number;
  color?: string;
  minHeight?: number;
  barHeight?: number;
}

function formatDate(ts: number): string {
  return new Date(ts).toLocaleDateString('pt-BR');
}

function formatAxis(ts: number): string {
  const d = new Date(ts);
  const m = d.toLocaleDateString('pt-BR', { month: 'short' });
  return `${m}/${d.getFullYear().toString().slice(2)}`;
}

export default function GanttChart({
  items,
  minDate,
  maxDate,
  color = '#1B998B',
  minHeight = 350,
  barHeight = 35,
}: GanttChartProps) {
  const height = Math.max(minHeight, items.length * barHeight);

  // Transforma em barras empilhadas: offset (transparente) + duração (colorida)
  const chartData = items.map((item) => ({
    name: item.name,
    offset: item.start - minDate,
    duration: item.end - item.start,
    start: item.start,
    end: item.end,
  }));

  const domain = [0, maxDate - minDate];

  // Gerar ticks para o eixo X (a cada ~3 meses)
  const range = maxDate - minDate;
  const tickCount = Math.max(4, Math.min(12, Math.floor(range / (90 * 24 * 60 * 60 * 1000))));
  const tickStep = range / tickCount;
  const ticks = Array.from({ length: tickCount + 1 }, (_, i) =>
    Math.round(i * tickStep)
  );

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart
        data={chartData}
        layout="vertical"
        margin={{ top: 5, right: 30, left: 10, bottom: 5 }}
        barCategoryGap="20%"
      >
        <XAxis
          type="number"
          domain={domain}
          ticks={ticks}
          tickFormatter={(v: number) => formatAxis(minDate + v)}
          tick={{ fontSize: 10 }}
        />
        <YAxis
          type="category"
          dataKey="name"
          width={220}
          tick={{ fontSize: 11 }}
        />
        <Tooltip
          content={({ active, payload }) => {
            if (!active || !payload?.length) return null;
            const d = payload[0]?.payload;
            if (!d) return null;
            return (
              <div
                className="px-3 py-2 bg-white border rounded shadow-sm"
                style={{ fontSize: 13 }}
              >
                <strong>{d.name}</strong>
                <br />
                Início: {formatDate(d.start)}
                <br />
                Fim: {formatDate(d.end)}
              </div>
            );
          }}
        />
        {/* Barra invisível para offset */}
        <Bar dataKey="offset" stackId="gantt" fill="transparent" barSize={20}>
          {chartData.map((_, i) => (
            <Cell key={i} fill="transparent" />
          ))}
        </Bar>
        {/* Barra colorida para duração */}
        <Bar dataKey="duration" stackId="gantt" fill={color} radius={[0, 3, 3, 0]} barSize={20} />
      </BarChart>
    </ResponsiveContainer>
  );
}
