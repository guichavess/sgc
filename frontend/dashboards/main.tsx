import React from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import App from './App';
import type { AppConfig } from './App';

const rootEl = document.getElementById('dashboards-root')!;

const config: AppConfig = {
  usuarioNome: rootEl.dataset.usuarioNome || '',
  hubUrl: rootEl.dataset.hubUrl || '/hub',
  logoutUrl: rootEl.dataset.logoutUrl || '/auth/logout',
  logoUrl: rootEl.dataset.logoUrl || '',
};

createRoot(rootEl).render(
  <React.StrictMode>
    <BrowserRouter basename="/dashboards">
      <App config={config} />
    </BrowserRouter>
  </React.StrictMode>
);
