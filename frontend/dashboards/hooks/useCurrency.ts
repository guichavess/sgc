import { useCallback } from 'react';

const formatter = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

export function useCurrency() {
  const format = useCallback((value: number): string => {
    return formatter.format(value);
  }, []);

  return { format };
}
