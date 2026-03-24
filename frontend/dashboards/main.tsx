import React from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import App from './App';

const rootEl = document.getElementById('dashboards-root')!;

createRoot(rootEl).render(
  <React.StrictMode>
    <BrowserRouter basename="/dashboards">
      <App />
    </BrowserRouter>
  </React.StrictMode>
);
