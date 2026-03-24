import React from 'react';
import LoadingSpinner from './LoadingSpinner';
import EmptyState from './EmptyState';

interface ChartCardProps {
  title: string;
  subtitle?: string;
  icon?: string;
  loading: boolean;
  empty?: boolean;
  children: React.ReactNode;
  className?: string;
}

export default function ChartCard({
  title,
  subtitle,
  icon,
  loading,
  empty,
  children,
  className,
}: ChartCardProps) {
  return (
    <div className={`card chart-card ${className || ''}`}>
      <div className="chart-card-header">
        <div className="d-flex align-items-center gap-2">
          {icon && (
            <div className="chart-card-icon">
              <i className={icon}></i>
            </div>
          )}
          <div>
            <div className="chart-card-title">{title}</div>
            {subtitle && <div className="chart-card-subtitle">{subtitle}</div>}
          </div>
        </div>
      </div>
      {loading ? (
        <LoadingSpinner />
      ) : empty ? (
        <EmptyState />
      ) : (
        children
      )}
    </div>
  );
}
