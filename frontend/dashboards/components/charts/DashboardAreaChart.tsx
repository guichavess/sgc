import React from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import type { RechartsDataPoint } from '../../types/api';

interface DashboardAreaChartProps {
  data: RechartsDataPoint[];
  dataKey: string;
  color?: string;
  height?: number;
  valueFormatter?: (value: number) => string;
}

export default function DashboardAreaChart({
  data,
  dataKey,
  color = '#B8860B',
  height = 320,
  valueFormatter,
}: DashboardAreaChartProps) {
  const gradientId = `gradient-${dataKey.replace(/\s+/g, '-')}`;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={color} stopOpacity={0.4} />
            <stop offset="95%" stopColor={color} stopOpacity={0.05} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" vertical={false} />
        <XAxis dataKey="name" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} tickFormatter={valueFormatter} />
        <Tooltip
          formatter={(value) => [
            valueFormatter ? valueFormatter(Number(value)) : String(value),
            dataKey,
          ]}
          contentStyle={{ borderRadius: 8, border: '1px solid #eee' }}
        />
        <Area
          type="monotone"
          dataKey={dataKey}
          stroke={color}
          strokeWidth={2}
          fill={`url(#${gradientId})`}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
