import type { DFAlg, ExprAlg } from "../algebra/read";

const NUMERIC_RX = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$";
const DESCRIBE_STATS = ["count", "mean", "stddev", "min", "max"] as const;
const SUMMARY_DEFAULTS = [
    "count",
    "mean",
    "stddev",
    "min",
    "25%",
    "50%",
    "75%",
    "max",
] as const;

type BuiltinSummaryName = "count" | "mean" | "stddev" | "min" | "max";
type ParsedSummaryMetric =
    | {
        kind: "builtin";
        name: BuiltinSummaryName;
        label: string;
        suffix: string;
    }
    | {
        kind: "percentile";
        percentile: number;
        label: string;
        suffix: string;
    };

const BUILTIN_SUMMARY_NAMES: readonly BuiltinSummaryName[] = [
    "count",
    "mean",
    "stddev",
    "min",
    "max",
];

function isBuiltinSummaryName(value: string): value is BuiltinSummaryName {
    return (BUILTIN_SUMMARY_NAMES as readonly string[]).includes(value);
}

export function assertStatisticsColumns(
    operation: "describe" | "summary",
    columnNames: readonly string[],
): void {
    if (columnNames.includes("summary")) {
        throw new TypeError(`${operation}() cannot ${operation === "describe" ? "describe" : "summarize"} a column named 'summary'.`);
    }
}

function numericExpression<E>(EX: ExprAlg<E>, name: string): E {
    const nullDouble = EX.call("nullif", [EX.lit(1.0), EX.lit(1.0)]);
    return EX.caseWhen(
        [{
            when: EX.call("rlike", [
                EX.call("concat", [EX.lit(""), EX.col(name)]),
                EX.lit(NUMERIC_RX),
            ]),
            then: EX.bin("*", EX.lit(1.0), EX.col(name)),
        }],
        nullDouble,
    );
}

function stringExpression<E>(EX: ExprAlg<E>, expression: E): E {
    return EX.call("concat", [EX.lit(""), expression]);
}

export function buildDescribePlan<R, E, G, CDF>(
    DF: DFAlg<R, E, G, CDF>,
    EX: ExprAlg<E>,
    dataframe: R,
    columnNames: readonly string[],
): R {
    const pruned = DF.select(dataframe, columnNames.map(name => EX.col(name)));
    const measures = Object.fromEntries(
        columnNames.flatMap(name => {
            const numeric = numericExpression(EX, name);
            return [
                [`__${name}_count`, EX.call("count", [EX.col(name)])],
                [`__${name}_mean`, EX.call("avg", [numeric])],
                [`__${name}_stddev`, EX.call("stddev_samp", [numeric])],
                [`__${name}_min`, EX.call("min", [EX.col(name)])],
                [`__${name}_max`, EX.call("max", [EX.col(name)])],
            ];
        }),
    );
    const aggregated = DF.agg(DF.groupBy(pruned, []), measures);
    const projectFor = (statistic: typeof DESCRIBE_STATS[number]) =>
        DF.select(aggregated, [
            EX.alias(EX.lit(statistic), "summary"),
            ...columnNames.map(name =>
                EX.alias(stringExpression(EX, EX.col(`__${name}_${statistic}`)), name)
            ),
        ]);
    const first = projectFor(DESCRIBE_STATS[0]);
    return DESCRIBE_STATS.slice(1).reduce(
        (accumulator, statistic) => DF.union(accumulator, projectFor(statistic), { byName: true }),
        first,
    );
}

function parseSummaryMetric(input: string): ParsedSummaryMetric {
    let metric = input.toLowerCase();
    if (metric === "median") metric = "50%";
    if (/%$/.test(metric)) {
        const percentile = Number.parseFloat(metric) / 100;
        if (!(percentile >= 0 && percentile <= 1)) {
            throw new Error(`summary(): invalid percentile '${metric}'`);
        }
        const rounded = Math.round(percentile * 100);
        return {
            kind: "percentile",
            percentile,
            label: `${rounded}%`,
            suffix: `p${rounded}`,
        };
    }
    if (metric === "std") metric = "stddev";
    if (isBuiltinSummaryName(metric)) {
        return { kind: "builtin", name: metric, label: metric, suffix: metric };
    }
    throw new Error(`summary(): unsupported metric '${metric}'`);
}

function builtinMeasure<E>(
    EX: ExprAlg<E>,
    name: string,
    metric: BuiltinSummaryName,
    numeric: E,
): E {
    switch (metric) {
        case "count":
            return EX.call("count", [EX.col(name)]);
        case "mean":
            return EX.call("avg", [numeric]);
        case "stddev":
            return EX.call("stddev_samp", [numeric]);
        case "min":
            return EX.call("min", [EX.col(name)]);
        case "max":
            return EX.call("max", [EX.col(name)]);
    }
}

export function buildSummaryPlan<R, E, G, CDF>(
    DF: DFAlg<R, E, G, CDF>,
    EX: ExprAlg<E>,
    dataframe: R,
    metrics: readonly string[] | undefined,
    columnNames: readonly string[],
): R {
    const requested = metrics?.length ? metrics : SUMMARY_DEFAULTS;
    const parsed = requested.map(parseSummaryMetric);
    const pruned = DF.select(dataframe, columnNames.map(name => EX.col(name)));
    const pairs: Array<[string, E]> = [];

    for (const name of columnNames) {
        const numeric = numericExpression(EX, name);
        for (const metric of parsed) {
            const expression = metric.kind === "builtin"
                ? builtinMeasure(EX, name, metric.name, numeric)
                : EX.call("percentile_approx", [numeric, EX.lit(metric.percentile)]);
            pairs.push([`__${name}_${metric.suffix}`, expression]);
        }
    }

    const aggregated = DF.agg(DF.groupBy(pruned, []), Object.fromEntries(pairs));
    const projectFor = (metric: ParsedSummaryMetric) =>
        DF.select(aggregated, [
            EX.alias(EX.lit(metric.label), "summary"),
            ...columnNames.map(name =>
                EX.alias(stringExpression(EX, EX.col(`__${name}_${metric.suffix}`)), name)
            ),
        ]);
    const first = projectFor(parsed[0]!);
    return parsed.slice(1).reduce(
        (accumulator, metric) => DF.union(accumulator, projectFor(metric), { byName: true }),
        first,
    );
}
