import React from 'react';
import LoadingSpinner from './LoadingSpinner';
import EmptyState from './EmptyState';

interface ChartCardProps {
  title: string;
  icon?: string;
  loading: boolean;
  empty?: boolean;
  children: React.ReactNode;
  className?: string;
}

export default function ChartCard({
  title,
  icon,
  loading,
  empty,
  children,
  className,
}: ChartCardProps) {
  return (
    <div className={`card chart-card ${className || ''}`}>
      <div className="chart-title">
        {icon && <i className={`${icon} me-2`}></i>}
        {title}
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
