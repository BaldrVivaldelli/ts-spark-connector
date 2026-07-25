import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        globals: true,
        environment: "node",
        include: ["test/**/*.{test,spec}.ts", "tests/**/*.{test,spec}.ts"],
        exclude: ["node_modules/**", "test/**/*.e2e.test.ts", "tests/**/*.e2e.test.ts"],
        passWithNoTests: false,
        coverage: {
            provider: "v8",
            reporter: ["text", "json-summary", "lcov"],
            reportsDirectory: "coverage",
            include: ["src/**/*.ts"],
            thresholds: {
                statements: 100,
                branches: 100,
                functions: 100,
                lines: 100,
            },
        },
    },
});
