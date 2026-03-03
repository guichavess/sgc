import React from 'react';

interface YearSelectorProps {
  value: number;
  onChange: (year: number) => void;
  years?: number[];
}

const DEFAULT_YEARS = [2024, 2025, 2026];

export default function YearSelector({
  value,
  onChange,
  years = DEFAULT_YEARS,
}: YearSelectorProps) {
  return (
    <select
      className="form-select form-select-sm ano-selector"
      value={value}
      onChange={(e) => onChange(Number(e.target.value))}
    >
      {years.map((y) => (
        <option key={y} value={y}>
          {y}
        </option>
      ))}
    </select>
  );
}
