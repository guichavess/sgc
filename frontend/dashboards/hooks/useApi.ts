import { useState, useEffect, useCallback } from 'react';

interface UseApiResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useApi<T>(
  url: string,
  params?: Record<string, string | number>
): UseApiResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const paramsKey = params ? JSON.stringify(params) : '';

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);

    const queryString = params
      ? '?' +
        Object.entries(params)
          .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
          .join('&')
      : '';

    fetch(`${url}${queryString}`, { credentials: 'same-origin' })
      .then((res) => {
        if (res.status === 401 || res.status === 403) {
          window.location.href = '/auth/login';
          throw new Error('Sessão expirada');
        }
        if (!res.ok) throw new Error(`Erro ${res.status}`);
        return res.json();
      })
      .then((json: T) => {
        setData(json);
        setLoading(false);
      })
      .catch((err: Error) => {
        setError(err.message);
        setLoading(false);
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url, paramsKey]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}
