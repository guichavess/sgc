import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import type { DonutDataPoint, RechartsDataPoint } from '../../types/api';

interface VerticalBarChartProps {
  data: DonutDataPoint[] | RechartsDataPoint[];
  dataKeys?: string[];
  colors?: string[];
  height?: number;
  yAxisLabel?: string;
  valueFormatter?: (value: number) => string;
  distributed?: boolean;
  showDataLabels?: boolean;
}

export default function VerticalBarChart({
  data,
  dataKeys,
  colors = ['#0d6efd'],
  height = 320,
  yAxisLabel,
  valueFormatter,
  distributed = false,
  showDataLabels = true,
}: VerticalBarChartProps) {
  const isSimple = !dataKeys || dataKeys.length === 0;
  const effectiveKeys = isSimple ? ['value'] : dataKeys;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart
        data={data}
        margin={{ top: 10, right: 20, left: 10, bottom: 40 }}
      >
        <CartesianGrid strokeDasharray="3 3" vertical={false} />
        <XAxis
          dataKey="name"
          angle={-45}
          textAnchor="end"
          height={60}
          tick={{ fontSize: 11 }}
          interval={0}
        />
        <YAxis
          tick={{ fontSize: 11 }}
          label={
            yAxisLabel
              ? {
                  value: yAxisLabel,
                  angle: -90,
                  position: 'insideLeft',
                  style: { fontSize: 12 },
                }
              : undefined
          }
          tickFormatter={valueFormatter}
        />
        <Tooltip
          formatter={(value, name) => [
            valueFormatter ? valueFormatter(Number(value)) : String(value),
            name === 'value' ? '' : String(name),
          ]}
          contentStyle={{ borderRadius: 8, border: '1px solid #eee' }}
        />
        {effectiveKeys!.length > 1 && <Legend />}

        {effectiveKeys!.map((key, ki) => (
          <Bar
            key={key}
            dataKey={key}
            fill={colors[ki % colors.length]}
            radius={[4, 4, 0, 0]}
            maxBarSize={60}
            label={
              showDataLabels
                ? {
                    position: 'top' as const,
                    fontSize: 11,
                    formatter: valueFormatter
                      ? (v: unknown) => valueFormatter(Number(v))
                      : undefined,
                  }
                : false
            }
          >
            {distributed &&
              (data as DonutDataPoint[]).map((entry, index) => (
                <Cell
                  key={index}
                  fill={entry.color || colors[index % colors.length]}
                />
              ))}
          </Bar>
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}
