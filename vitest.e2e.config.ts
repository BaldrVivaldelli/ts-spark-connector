import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        globals: true,
        environment: "node",
        include: ["test/**/*.e2e.test.ts", "tests/**/*.e2e.test.ts"],
        exclude: ["node_modules/**"],
        passWithNoTests: false,
        testTimeout: 120_000,
        // One file at a time: the suites share one Spark server, and a slow
        // job in one file would otherwise delay timing-sensitive tests in
        // another (and overload small CI runners).
        fileParallelism: false,
        hookTimeout: 120_000,
        sequence: {
            concurrent: false,
        },
    },
});
