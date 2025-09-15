// vitest.config.ts
import { defineConfig } from 'vitest/config';
export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        include: ['test/**/*.{test,spec}.ts', 'tests/**/*.{test,spec}.ts'],
        exclude: ['test/**/*.e2e.test.ts', 'tests/**/*.e2e.test.ts', 'node_modules/**'],
        passWithNoTests: true
    }
});
