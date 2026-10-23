import { defineConfig } from "vitest/config";

// Opt-in suite against real AWS sessions (npm run test:aws). It bills while
// sessions run, so it is kept out of the unit and E2E configs.
export default defineConfig({
    test: {
        globals: true,
        environment: "node",
        include: ["test/**/*.aws.test.ts"],
        exclude: ["node_modules/**"],
        passWithNoTests: false,
        // An EMR Serverless application can take minutes to start a session.
        testTimeout: 20 * 60_000,
        hookTimeout: 20 * 60_000,
        // One file at a time and one test at a time: sessions are billed.
        fileParallelism: false,
        sequence: {
            concurrent: false,
        },
    },
});
