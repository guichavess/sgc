import React from 'react';
import { Link, useLocation } from 'react-router-dom';

interface NavbarProps {
  usuarioNome: string;
  hubUrl: string;
  logoutUrl: string;
  logoUrl: string;
}

const NAV_ITEMS = [
  { path: '/', label: 'Consolidado', icon: 'bi-speedometer2' },
  { path: '/pagamentos', label: 'Pagamentos', icon: 'bi-cash-stack' },
  { path: '/financeiro', label: 'Financeiro', icon: 'bi-bank' },
  { path: '/contratos', label: 'Contratos', icon: 'bi-clipboard-check' },
];

function getShortName(fullName: string): string {
  const parts = fullName.split(' ');
  return parts.length >= 2 ? parts.slice(0, 2).join(' ') : fullName;
}

export default function DashboardNavbar({
  usuarioNome,
  hubUrl,
  logoutUrl,
  logoUrl,
}: NavbarProps) {
  const location = useLocation();
  const nomeCurto = getShortName(usuarioNome);

  const isActive = (path: string) => {
    if (path === '/') return location.pathname === '/';
    return location.pathname.startsWith(path);
  };

  return (
    <nav
      className="navbar navbar-expand-lg navbar-dark mb-4 py-3 shadow-sm"
      style={{ backgroundColor: '#1B998B' }}
    >
      <div className="container-fluid px-4">
        <Link
          className="navbar-brand d-flex align-items-center"
          to="/"
        >
          {logoUrl && (
            <img
              src={logoUrl}
              alt="SEAD Piauí"
              style={{ height: '38px' }}
              className="me-3"
            />
          )}
          <span className="fw-bold">SGC</span>
          <span className="mx-2 opacity-50">|</span>
          <small className="opacity-75 fw-light fs-6">Dashboards</small>
        </Link>

        <button
          className="navbar-toggler border-0"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target="#navbarDashboards"
        >
          <span className="navbar-toggler-icon"></span>
        </button>

        <div className="collapse navbar-collapse" id="navbarDashboards">
          <ul className="navbar-nav me-auto mb-2 mb-lg-0 ms-lg-4">
            {NAV_ITEMS.map((item) => (
              <li className="nav-item" key={item.path}>
                <Link
                  className={`nav-link px-3 d-inline-flex align-items-center ${
                    isActive(item.path) ? 'active' : ''
                  }`}
                  to={item.path}
                >
                  <i className={`bi ${item.icon} me-1`}></i> {item.label}
                </Link>
              </li>
            ))}
          </ul>

          <ul className="navbar-nav ms-auto align-items-center gap-3">
            <li className="nav-item">
              <a
                className="btn btn-outline-light btn-sm px-3 rounded-pill d-inline-flex align-items-center"
                href={hubUrl}
              >
                <i className="bi bi-grid me-1"></i> Menu
              </a>
            </li>

            <li className="nav-item dropdown">
              <a
                className="nav-link dropdown-toggle text-white d-inline-flex align-items-center px-3"
                href="#"
                role="button"
                data-bs-toggle="dropdown"
                aria-expanded="false"
              >
                <i className="bi bi-person-circle me-2"></i>
                {nomeCurto}
              </a>
              <ul className="dropdown-menu dropdown-menu-end shadow-sm">
                <li>
                  <a className="dropdown-item text-danger d-flex align-items-center" href={logoutUrl}>
                    <i className="bi bi-box-arrow-right me-2"></i> Sair
                  </a>
                </li>
              </ul>
            </li>
          </ul>
        </div>
      </div>
    </nav>
  );
}
