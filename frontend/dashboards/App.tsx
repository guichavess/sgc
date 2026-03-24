import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import DashboardLayout from './components/layout/DashboardLayout';
import Consolidado from './pages/Consolidado';
import Pagamentos from './pages/Pagamentos';
import Financeiro from './pages/Financeiro';
import Contratos from './pages/Contratos';

export default function App() {
  return (
    <Routes>
      <Route element={<DashboardLayout />}>
        <Route index element={<Consolidado />} />
        <Route path="programacao-desembolso" element={<Pagamentos />} />
        <Route path="financeiro" element={<Financeiro />} />
        <Route path="contratos" element={<Contratos />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
