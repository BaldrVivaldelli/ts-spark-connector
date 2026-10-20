import { describe, expect, it } from "vitest";
import { ProtoExprAlg } from "../src/engine/compilerRead";
import { col, lit, type EBuilder } from "../src/engine/column";
import * as F from "../src/functions";

type Proto = Record<string, any>;
const proto = (expression: EBuilder): Proto => expression.build(ProtoExprAlg) as Proto;
const attribute = (name: string) => ({ unresolved_attribute: { unparsed_identifier: name } });
const call = (name: string, ...args: Proto[]) => ({ unresolved_function: { function_name: name, arguments: args } });

describe("hand-written functions", () => {
    it("aliases col/column/lit/expr/when to the column helpers", () => {
        expect(proto(F.column("a"))).toEqual(attribute("a"));
        expect(proto(F.col("a"))).toEqual(attribute("a"));
        expect(proto(F.lit(1))).toEqual({ literal: { integer: 1 } });
        expect(proto(F.expr("a + 1"))).toEqual({ expression_string: { expression: "a + 1" } });
        expect(proto(F.when(col("a").gt(1), "big").otherwise("small"))).toHaveProperty(
            "unresolved_function.function_name",
            "if",
        );
    });

    it("counts rows for count('*') and non-null values otherwise", () => {
        // A star argument would expand to every column over Connect.
        expect(proto(F.count("*"))).toEqual(call("count", { literal: { integer: 1 } }));
        expect(proto(F.count("a"))).toEqual(call("count", attribute("a")));
    });

    it("sends distinct aggregates as count/sum with is_distinct", () => {
        expect(proto(F.count_distinct("a", col("b")))).toEqual({
            unresolved_function: {
                function_name: "count",
                arguments: [attribute("a"), attribute("b")],
                is_distinct: true,
            },
        });
        expect(proto(F.sum_distinct("a"))).toEqual({
            unresolved_function: { function_name: "sum", arguments: [attribute("a")], is_distinct: true },
        });
    });

    it("maps log(col) to ln and log(base, col) to log", () => {
        expect(proto(F.log("x"))).toEqual(call("ln", attribute("x")));
        expect(proto(F.log(2, "x"))).toEqual(call("log", { literal: { integer: 2 } }, attribute("x")));
        expect(proto(F.log("base", "x"))).toEqual(call("log", attribute("base"), attribute("x")));
        const unsafeLog = F.log as unknown as (base: number) => EBuilder;
        expect(() => unsafeLog(2)).toThrow("log(base) needs the column");
    });

    it("sends trim/ltrim/rtrim characters first, as Spark's two-argument form expects", () => {
        for (const [trim, name] of [[F.trim, "trim"], [F.ltrim, "ltrim"], [F.rtrim, "rtrim"]] as const) {
            expect(proto(trim("s"))).toEqual(call(name, attribute("s")));
            expect(proto(trim("s", "x"))).toEqual(call(name, { literal: { string: "x" } }, attribute("s")));
        }
    });

    it("fills the slide of a time window the way PySpark does", () => {
        const ts = attribute("ts");
        const s = (value: string) => ({ literal: { string: value } });
        expect(proto(F.window("ts", "10 minutes"))).toEqual(call("window", ts, s("10 minutes")));
        expect(proto(F.window("ts", "10 minutes", "5 minutes"))).toEqual(
            call("window", ts, s("10 minutes"), s("5 minutes")),
        );
        expect(proto(F.window("ts", "10 minutes", "5 minutes", "1 minute"))).toEqual(
            call("window", ts, s("10 minutes"), s("5 minutes"), s("1 minute")),
        );
        // A start offset without a slide is a tumbling window: slide = duration.
        expect(proto(F.window("ts", "10 minutes", undefined, "1 minute"))).toEqual(
            call("window", ts, s("10 minutes"), s("10 minutes"), s("1 minute")),
        );
    });

    it("builds sort keys for orderBy", () => {
        const cases = [
            [F.asc, "asc", undefined],
            [F.asc_nulls_first, "asc", "nullsFirst"],
            [F.asc_nulls_last, "asc", "nullsLast"],
            [F.desc, "desc", undefined],
            [F.desc_nulls_first, "desc", "nullsFirst"],
            [F.desc_nulls_last, "desc", "nullsLast"],
        ] as const;
        for (const [key, direction, nulls] of cases) {
            expect(key("a")(ProtoExprAlg)).toEqual({ expr: attribute("a"), direction, nulls });
        }
    });
});

describe("higher-order functions", () => {
    const variable = (name: string) => ({ unresolved_named_lambda_variable: { name_parts: [name] } });
    const lambdaOf = (expression: Proto, argumentIndex: number) =>
        expression.unresolved_function.arguments[argumentIndex].lambda_function;
    const names = (lambda: Proto): string[] =>
        lambda.arguments.map((argument: Proto) => argument.name_parts[0]);

    it("passes the element, and the index when the callback declares it", () => {
        const single = lambdaOf(proto(F.transform("xs", x => x.plus(1))), 1);
        expect(names(single)).toHaveLength(1);
        expect(names(single)[0]).toMatch(/^x_\d+$/);
        expect(single.function).toEqual(call("+", variable(names(single)[0]!), { literal: { integer: 1 } }));

        const indexed = lambdaOf(proto(F.filter("xs", (x, i) => x.gt(i))), 1);
        const [x, i] = names(indexed);
        expect(x).toMatch(/^x_\d+$/);
        expect(i).toMatch(/^i_\d+$/);
        expect(indexed.function).toEqual(call(">", variable(x!), variable(i!)));
        expect(proto(F.filter("xs", x => x.isNotNull()))).toHaveProperty("unresolved_function.function_name", "filter");
    });

    it("gives nested lambdas distinct variables", () => {
        const nested = proto(F.transform("matrix", row => F.transform(row, cell => cell.multiply(2))));
        const outer = lambdaOf(nested, 1);
        const inner = lambdaOf(outer.function, 1);
        expect(names(outer)[0]).not.toBe(names(inner)[0]);
        expect(outer.function.unresolved_function.arguments[0]).toEqual(variable(names(outer)[0]!));
    });

    it("builds every higher-order function with its parameter names", () => {
        const cases: Array<[EBuilder, string, number, RegExp[]]> = [
            [F.exists("xs", x => x.gt(0)), "exists", 1, [/^x_/]],
            [F.forall("xs", x => x.gt(0)), "forall", 1, [/^x_/]],
            [F.zip_with("xs", "ys", (x, y) => x.plus(y)), "zip_with", 2, [/^x_/, /^y_/]],
            [F.map_filter("m", (k, v) => v.gt(k)), "map_filter", 1, [/^k_/, /^v_/]],
            [F.transform_keys("m", (k, _v) => k.cast("string")), "transform_keys", 1, [/^k_/, /^v_/]],
            [F.transform_values("m", (_k, v) => v.plus(1)), "transform_values", 1, [/^k_/, /^v_/]],
            [F.map_zip_with("m1", "m2", (_k, a, b) => F.coalesce(a, b)), "map_zip_with", 2, [/^k_/, /^v1_/, /^v2_/]],
            [F.array_sort("xs", (l, r) => r.minus(l)), "array_sort", 1, [/^l_/, /^r_/]],
        ];
        for (const [expression, name, lambdaIndex, patterns] of cases) {
            const built = proto(expression);
            expect(built.unresolved_function.function_name).toBe(name);
            const parameters = names(lambdaOf(built, lambdaIndex));
            expect(parameters).toHaveLength(patterns.length);
            parameters.forEach((parameter, index) => expect(parameter).toMatch(patterns[index]!));
        }
        expect(proto(F.array_sort("xs"))).toEqual(call("array_sort", attribute("xs")));
    });

    it("folds with aggregate and reduce, with an optional finish lambda", () => {
        for (const fold of [F.aggregate, F.reduce]) {
            const plain = proto(fold("xs", 0, (acc, x) => acc.plus(x)));
            expect(plain.unresolved_function.arguments).toHaveLength(3);
            expect(plain.unresolved_function.arguments[1]).toEqual({ literal: { integer: 0 } });
            expect(names(lambdaOf(plain, 2))).toEqual([expect.stringMatching(/^acc_/), expect.stringMatching(/^x_/)]);

            const finished = proto(fold("xs", lit(0).cast("bigint"), (acc, x) => acc.plus(x), acc => acc.multiply(10)));
            expect(finished.unresolved_function.arguments).toHaveLength(4);
            expect(names(lambdaOf(finished, 3))).toEqual([expect.stringMatching(/^acc_/)]);
        }
    });

    it("rejects a callback that does not return a column expression", () => {
        const notAColumn = (() => 1) as unknown as (x: EBuilder) => EBuilder;
        expect(() => F.exists("xs", notAColumn)).toThrow(
            "A higher-order function callback must return a column expression.",
        );
    });

    it("builds partition transforms and dynamic calls", () => {
        const fn = (e: EBuilder) => (e.build(ProtoExprAlg) as { unresolved_function: Record<string, unknown> }).unresolved_function;
        const ts = { unresolved_attribute: { unparsed_identifier: "ts" } };
        expect(fn(F.years("ts"))).toEqual({ function_name: "years", arguments: [ts] });
        expect(fn(F.months(col("ts")))).toEqual({ function_name: "months", arguments: [ts] });
        expect(fn(F.days("ts"))).toEqual({ function_name: "days", arguments: [ts] });
        expect(fn(F.hours("ts"))).toEqual({ function_name: "hours", arguments: [ts] });
        expect(fn(F.bucket(4, "id"))).toEqual({
            function_name: "bucket",
            arguments: [{ literal: { integer: 4 } }, { unresolved_attribute: { unparsed_identifier: "id" } }],
        });
        expect(() => F.bucket(0, "id")).toThrow(RangeError);
        expect(() => F.bucket(1.5, "id")).toThrow(RangeError);

        expect(fn(F.call_function("greatest", "a", col("b")))).toEqual({
            function_name: "greatest",
            arguments: [
                { unresolved_attribute: { unparsed_identifier: "a" } },
                { unresolved_attribute: { unparsed_identifier: "b" } },
            ],
        });
        expect(fn(F.call_function("now"))).toEqual({ function_name: "now", arguments: [] });
        expect(() => F.call_function("")).toThrow(TypeError);
    });
});
