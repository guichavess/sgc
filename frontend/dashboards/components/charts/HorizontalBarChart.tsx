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
} from 'recharts';
import type { RechartsDataPoint } from '../../types/api';

interface HorizontalBarChartProps {
  data: RechartsDataPoint[];
  dataKeys: string[];
  colors: string[];
  height?: number;
  maxLabelWidth?: number;
  valueFormatter?: (value: number) => string;
  showDataLabels?: boolean;
  minHeight?: number;
  barHeight?: number;
}

export default function HorizontalBarChart({
  data,
  dataKeys,
  colors,
  height,
  maxLabelWidth = 200,
  valueFormatter,
  showDataLabels = false,
  minHeight = 300,
  barHeight = 35,
}: HorizontalBarChartProps) {
  const calculatedHeight = height || Math.max(minHeight, data.length * barHeight);

  return (
    <ResponsiveContainer width="100%" height={calculatedHeight}>
      <BarChart
        data={data}
        layout="vertical"
        margin={{ top: 5, right: 30, left: 10, bottom: 5 }}
      >
        <CartesianGrid strokeDasharray="3 3" horizontal={false} />
        <XAxis
          type="number"
          tick={{ fontSize: 11 }}
          tickFormatter={valueFormatter}
        />
        <YAxis
          type="category"
          dataKey="name"
          width={maxLabelWidth}
          tick={{ fontSize: 11 }}
        />
        <Tooltip
          formatter={(value, name) => [
            valueFormatter ? valueFormatter(Number(value)) : String(value),
            String(name),
          ]}
          contentStyle={{ borderRadius: 8, border: '1px solid #eee' }}
        />
        {dataKeys.length > 1 && <Legend verticalAlign="top" />}

        {dataKeys.map((key, i) => (
          <Bar
            key={key}
            dataKey={key}
            fill={colors[i % colors.length]}
            radius={[0, 4, 4, 0]}
            maxBarSize={30}
            label={
              showDataLabels
                ? {
                    position: 'right' as const,
                    fontSize: 11,
                    formatter: valueFormatter
                      ? (v: unknown) => valueFormatter(Number(v))
                      : undefined,
                  }
                : false
            }
          />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}
