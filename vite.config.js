import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'path';

export default defineConfig({
    root: './frontend',
    base: '/static/dist/',

    plugins: [
        react({
            include: /dashboards\/.*\.tsx?$/,
        }),
    ],

    build: {
        outDir: '../app/static/dist',
        emptyOutDir: true,
        manifest: true,
        rollupOptions: {
            input: {
                // Entry points principais
                main: resolve(__dirname, 'frontend/js/main.js'),

                // Páginas específicas
                login: resolve(__dirname, 'frontend/js/pages/login.js'),
                dashboard: resolve(__dirname, 'frontend/js/pages/dashboard.js'),
                nova: resolve(__dirname, 'frontend/js/pages/nova.js'),
                detalhes: resolve(__dirname, 'frontend/js/pages/detalhes.js'),
                pendencias: resolve(__dirname, 'frontend/js/pages/pendencias.js'),
                relatorios: resolve(__dirname, 'frontend/js/pages/relatorios.js'),

                // React SPA - Dashboards
                'dashboards-app': resolve(__dirname, 'frontend/dashboards/main.tsx'),
            },
            output: {
                // Organiza os arquivos de saída
                entryFileNames: 'js/[name]-[hash].js',
                chunkFileNames: 'js/chunks/[name]-[hash].js',
                assetFileNames: (assetInfo) => {
                    if (assetInfo.name.endsWith('.css')) {
                        return 'css/[name]-[hash][extname]';
                    }
                    return 'assets/[name]-[hash][extname]';
                },
                manualChunks: {
                    'react-vendor': ['react', 'react-dom', 'react-router-dom'],
                    'recharts-vendor': ['recharts'],
                },
            },
        },
    },

    server: {
        origin: 'http://localhost:5173',
        cors: true,
        proxy: {
            '/dashboards/api': 'http://localhost:5000',
            '/auth': 'http://localhost:5000',
            '/hub': 'http://localhost:5000',
            '/static': 'http://localhost:5000',
        },
    },

    resolve: {
        alias: {
            '@': resolve(__dirname, 'frontend/js'),
            '@utils': resolve(__dirname, 'frontend/js/utils'),
            '@components': resolve(__dirname, 'frontend/js/components'),
            '@pages': resolve(__dirname, 'frontend/js/pages'),
            '@dashboards': resolve(__dirname, 'frontend/dashboards'),
        },
    },
});
