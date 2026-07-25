import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import process from "node:process";
import { URL } from "node:url";
import { SparkSession, col } from "../dist/index.js";

const baseline = JSON.parse(readFileSync(
    new URL("../benchmarks/plan-compilation.json", import.meta.url),
    "utf8",
));
const iterations = Number(process.env.SPARK_PLAN_BENCH_ITERATIONS ?? 5_000);
const maxRegressionPercent = Number(
    process.env.SPARK_PLAN_BENCH_MAX_REGRESSION_PERCENT ?? baseline.maxRegressionPercent,
);
const minimumPlansPerSecond = Number(
    process.env.SPARK_PLAN_BENCH_MIN_PLANS_PER_SECOND
    ?? baseline.plansPerSecond * (1 - maxRegressionPercent / 100),
);
const maximumHeapGrowthBytes = Number(
    process.env.SPARK_PLAN_BENCH_MAX_HEAP_GROWTH_BYTES ?? baseline.maxHeapGrowthBytes,
);
assert.ok(Number.isSafeInteger(iterations) && iterations > 0,
    "SPARK_PLAN_BENCH_ITERATIONS must be a positive safe integer");
assert.ok(Number.isFinite(maxRegressionPercent) && maxRegressionPercent >= 0 && maxRegressionPercent < 100,
    "SPARK_PLAN_BENCH_MAX_REGRESSION_PERCENT must be between 0 (inclusive) and 100 (exclusive)");
assert.ok(Number.isFinite(minimumPlansPerSecond) && minimumPlansPerSecond > 0,
    "SPARK_PLAN_BENCH_MIN_PLANS_PER_SECOND must be a finite number greater than zero");
assert.ok(Number.isSafeInteger(maximumHeapGrowthBytes) && maximumHeapGrowthBytes > 0,
    "SPARK_PLAN_BENCH_MAX_HEAP_GROWTH_BYTES must be a positive safe integer");

const session = SparkSession.builder().getOrCreate();
const compile = () => session.read.parquet("/data/events")
    .filter(col("amount").gt(100))
    .select("user_id", "amount")
    .orderBy(col("amount").descNullsLast())
    .limit(20)
    .toProtoJSON();

for (let index = 0; index < Math.min(iterations, 250); index += 1) compile();

const initialHeapBytes = process.memoryUsage().heapUsed;
const started = performance.now();
let bytes = 0;
for (let index = 0; index < iterations; index += 1) bytes += compile().length;
const durationMs = performance.now() - started;
const heapGrowthBytes = Math.max(0, process.memoryUsage().heapUsed - initialHeapBytes);
const plansPerSecond = iterations / (durationMs / 1_000);

assert.ok(bytes > 0 && Number.isFinite(plansPerSecond));
assert.ok(
    plansPerSecond >= minimumPlansPerSecond,
    `Plan compilation regression: ${plansPerSecond.toFixed(2)} plans/s is below the ` +
    `${minimumPlansPerSecond.toFixed(2)} plans/s floor (${maxRegressionPercent}% below baseline)`,
);
assert.ok(
    heapGrowthBytes <= maximumHeapGrowthBytes,
    `Plan compilation heap regression: ${heapGrowthBytes} bytes exceeds ${maximumHeapGrowthBytes}`,
);
process.stdout.write(`${JSON.stringify({
    benchmark: "logical-plan-to-proto-json",
    iterations,
    durationMs: Number(durationMs.toFixed(2)),
    plansPerSecond: Number(plansPerSecond.toFixed(2)),
    baselinePlansPerSecond: baseline.plansPerSecond,
    maxRegressionPercent,
    minimumPlansPerSecond,
    averageBytes: Math.round(bytes / iterations),
    heapGrowthBytes,
    maximumHeapGrowthBytes,
})}\n`);
