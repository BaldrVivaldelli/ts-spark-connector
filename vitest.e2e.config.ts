import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        globals: true,
        environment: "node",
        include: ["test/**/*.e2e.test.ts", "tests/**/*.e2e.test.ts"],
        exclude: ["node_modules/**"],
        passWithNoTests: false,
        testTimeout: 120_000,
        hookTimeout: 120_000,
        sequence: {
            concurrent: false,
        },
    },
});
