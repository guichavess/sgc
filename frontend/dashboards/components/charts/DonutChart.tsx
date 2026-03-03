import React from 'react';
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import type { PieLabelRenderProps } from 'recharts';
import type { DonutDataPoint } from '../../types/api';

interface DonutChartProps {
  data: DonutDataPoint[];
  height?: number;
  showTotal?: boolean;
}

const DEFAULT_COLORS = ['#0d6efd', '#198754', '#ffc107', '#dc3545', '#6c757d', '#0dcaf0', '#d63384'];

const RADIAN = Math.PI / 180;

function renderLabel(props: PieLabelRenderProps) {
  const cx = Number(props.cx ?? 0);
  const cy = Number(props.cy ?? 0);
  const midAngle = Number(props.midAngle ?? 0);
  const innerRadius = Number(props.innerRadius ?? 0);
  const outerRadius = Number(props.outerRadius ?? 0);
  const value = props.value;

  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);

  return (
    <text
      x={x}
      y={y}
      fill="white"
      textAnchor="middle"
      dominantBaseline="central"
      fontSize={12}
      fontWeight={600}
    >
      {String(value)}
    </text>
  );
}

export default function DonutChart({
  data,
  height = 320,
  showTotal = true,
}: DonutChartProps) {
  const total = data.reduce((sum, d) => sum + d.value, 0);

  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          innerRadius="45%"
          outerRadius="75%"
          paddingAngle={2}
          label={renderLabel}
          labelLine={false}
        >
          {data.map((entry, index) => (
            <Cell
              key={entry.name}
              fill={entry.color || DEFAULT_COLORS[index % DEFAULT_COLORS.length]}
            />
          ))}
        </Pie>
        {showTotal && (
          <text
            x="50%"
            y="50%"
            textAnchor="middle"
            dominantBaseline="central"
            fontSize={20}
            fontWeight={700}
            fill="#333"
          >
            {total}
          </text>
        )}
        <Tooltip
          formatter={(value) => [String(value), '']}
          contentStyle={{ borderRadius: 8, border: '1px solid #eee' }}
        />
        <Legend
          verticalAlign="bottom"
          iconType="circle"
          wrapperStyle={{ fontSize: '13px' }}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}
