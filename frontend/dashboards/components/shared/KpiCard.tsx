import React from 'react';

interface KpiCardProps {
  icon: string;
  color: string;
  value: string | number;
  label: string;
  subtitle?: string;
}

export default function KpiCard({ icon, color, value, label, subtitle }: KpiCardProps) {
  return (
    <div className="card kpi-card h-100">
      <div className="card-body d-flex align-items-center gap-3">
        <div
          className="kpi-icon d-flex align-items-center justify-content-center"
          style={{ backgroundColor: `${color}15`, color }}
        >
          <i className={icon} style={{ fontSize: '1.4rem' }}></i>
        </div>
        <div>
          <div className="kpi-value" style={{ color }}>
            {value}
          </div>
          <div className="kpi-label">{label}</div>
          {subtitle && (
            <small className="text-muted">{subtitle}</small>
          )}
        </div>
      </div>
    </div>
  );
}
