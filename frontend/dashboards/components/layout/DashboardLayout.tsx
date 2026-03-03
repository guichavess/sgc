import React from 'react';
import { Outlet } from 'react-router-dom';
import DashboardNavbar from './DashboardNavbar';

interface LayoutProps {
  config: {
    usuarioNome: string;
    hubUrl: string;
    logoutUrl: string;
    logoUrl: string;
  };
}

export default function DashboardLayout({ config }: LayoutProps) {
  return (
    <>
      <DashboardNavbar
        usuarioNome={config.usuarioNome}
        hubUrl={config.hubUrl}
        logoutUrl={config.logoutUrl}
        logoUrl={config.logoUrl}
      />
      <div className="container-fluid px-4 pb-5">
        <Outlet />
      </div>
    </>
  );
}
