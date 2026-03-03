import React from 'react';

interface EmptyStateProps {
  message?: string;
}

export default function EmptyState({ message = 'Sem dados disponíveis' }: EmptyStateProps) {
  return (
    <div className="chart-empty">
      <i className="bi bi-inbox me-2"></i>
      {message}
    </div>
  );
}
