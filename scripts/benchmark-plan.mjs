import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import process from "node:process";
import { SparkSession, col } from "../dist/index.js";

const iterations = Number(process.env.SPARK_PLAN_BENCH_ITERATIONS ?? 5_000);
const minimumPlansPerSecond = Number(process.env.SPARK_PLAN_BENCH_MIN_PLANS_PER_SECOND ?? 1_000);
assert.ok(Number.isSafeInteger(iterations) && iterations > 0,
    "SPARK_PLAN_BENCH_ITERATIONS must be a positive safe integer");
assert.ok(Number.isFinite(minimumPlansPerSecond) && minimumPlansPerSecond > 0,
    "SPARK_PLAN_BENCH_MIN_PLANS_PER_SECOND must be a finite number greater than zero");

const session = SparkSession.builder().getOrCreate();
const compile = () => session.read.parquet("/data/events")
    .filter(col("amount").gt(100))
    .select("user_id", "amount")
    .orderBy(col("amount").descNullsLast())
    .limit(20)
    .toProtoJSON();

for (let index = 0; index < Math.min(iterations, 250); index += 1) compile();

const started = performance.now();
let bytes = 0;
for (let index = 0; index < iterations; index += 1) bytes += compile().length;
const durationMs = performance.now() - started;
const plansPerSecond = iterations / (durationMs / 1_000);

assert.ok(bytes > 0 && Number.isFinite(plansPerSecond));
assert.ok(
    plansPerSecond >= minimumPlansPerSecond,
    `Plan compilation regression: ${plansPerSecond.toFixed(2)} plans/s is below the ${minimumPlansPerSecond} plans/s floor`,
);
process.stdout.write(`${JSON.stringify({
    benchmark: "logical-plan-to-proto-json",
    iterations,
    durationMs: Number(durationMs.toFixed(2)),
    plansPerSecond: Number(plansPerSecond.toFixed(2)),
    minimumPlansPerSecond,
    averageBytes: Math.round(bytes / iterations),
})}\n`);
