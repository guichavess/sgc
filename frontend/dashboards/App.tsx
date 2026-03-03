import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import DashboardLayout from './components/layout/DashboardLayout';
import Consolidado from './pages/Consolidado';
import Pagamentos from './pages/Pagamentos';
import Financeiro from './pages/Financeiro';
import Contratos from './pages/Contratos';

export interface AppConfig {
  usuarioNome: string;
  hubUrl: string;
  logoutUrl: string;
  logoUrl: string;
}

export default function App({ config }: { config: AppConfig }) {
  return (
    <Routes>
      <Route element={<DashboardLayout config={config} />}>
        <Route index element={<Consolidado />} />
        <Route path="pagamentos" element={<Pagamentos />} />
        <Route path="financeiro" element={<Financeiro />} />
        <Route path="contratos" element={<Contratos />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
