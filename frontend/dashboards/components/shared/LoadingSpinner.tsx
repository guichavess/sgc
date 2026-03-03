import React from 'react';

export default function LoadingSpinner() {
  return (
    <div className="chart-loading">
      <div className="spinner-border text-secondary" role="status">
        <span className="visually-hidden">Carregando...</span>
      </div>
    </div>
  );
}
