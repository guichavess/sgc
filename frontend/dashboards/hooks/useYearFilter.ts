import { useSearchParams } from 'react-router-dom';

export function useYearFilter(defaultYear?: number) {
  const [searchParams, setSearchParams] = useSearchParams();

  const year =
    Number(searchParams.get('ano')) ||
    defaultYear ||
    new Date().getFullYear();

  const setYear = (y: number) => {
    setSearchParams({ ano: String(y) });
  };

  return { year, setYear };
}
