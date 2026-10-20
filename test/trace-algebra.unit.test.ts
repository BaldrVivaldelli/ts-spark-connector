import { describe, expect, it } from "vitest";
import {
    SparkSession,
    Window,
    call,
    coalesce,
    col,
    elementAt,
    explode,
    expr,
    from_json,
    functions as F,
    getField,
    getItem,
    isNotNull,
    isNull,
    lit,
    map_keys,
    map_values,
    posexplode,
    split,
    struct,
    to_json,
    when,
} from "../src";
import { TraceDFAlg, TraceExprAlg, type TraceNode } from "../src/trace/trace";

/**
 * Plan inspection used to throw for ordinary expressions (`and()`, `isNull()`,
 * `drop()`, `sample()`, ...) because the trace algebras implemented only a
 * subset of `ExprAlg` / `DFAlg`. They are now typed against the algebras; this
 * suite drives every public builder through the real entry points so a new
 * algebra method without a trace counterpart fails here, not in a user's REPL.
 */

const session = SparkSession.builder().getOrCreate();
const people = () => session.read.option("header", "true").csv("/tmp/people.csv");
const purchases = () => session.read.option("header", "true").csv("/tmp/purchases.csv");

type Inspectable = {
    toClientASTJSON(): string;
    toClientASTMermaid(): string;
    toSparkLogicalPlanJSON(): string;
};

function labelsOf(df: Inspectable): string[] {
    const root = JSON.parse(df.toClientASTJSON()) as TraceNode;
    const out: string[] = [];
    const walk = (node: TraceNode) => {
        out.push(node.label);
        node.children.forEach(walk);
    };
    walk(root);
    return out;
}

function expectInspectable(df: Inspectable): string[] {
    const labels = labelsOf(df);
    expect(labels.length).toBeGreaterThan(0);
    expect(df.toClientASTMermaid()).toMatch(/^flowchart TD\n/);
    expect(() => df.toSparkLogicalPlanJSON()).not.toThrow();
    return labels;
}

const matching = (labels: string[], pattern: RegExp) => labels.filter(label => pattern.test(label));

describe("client-side plan inspection covers the whole algebra", () => {
    it("renders logical operators, null checks and ordering", () => {
        const df = people()
            .filter(col("age").gt(18).and(col("name").isNotNull()).or(isNull("email")))
            .filter(isNotNull(col("id")).and(col("age").gte(1)).and(col("age").lt(120)).and(col("age").lte(119)))
            .orderBy(col("age").descNullsLast(), "name")
            .sort(col("id").asc("nullsFirst"), col("age").ascNullsLast(), col("name").descNullsFirst());

        const labels = expectInspectable(df);
        expect(labels).toEqual(expect.arrayContaining([
            "AND", "OR", "isNotNull", "isNull", "filter", "orderBy[2]", "sort[3]", ">", ">=", "<", "<=",
        ]));
    });

    it("serializes bigint literals in the logical plan JSON", () => {
        const json = people().select(lit(9007199254740993n).alias("big")).toSparkLogicalPlanJSON();
        expect(json).toContain("9007199254740993");
    });

    it("renders every expression helper inside a projection", () => {
        const spec = Window.partitionBy("user_id")
            .orderBy(col("ts").desc(), "id", col("plain"))
            .rowsBetween({ type: "ValuePreceding", value: 5 }, { type: "CurrentRow" });
        const df = people().select(
            col("name").alias("n"),
            lit(1),
            lit("x"),
            lit(true),
            lit(10n),
            lit(null),
            when(col("age").gt(65), "senior").when(col("age").gt(18), "adult").otherwise("minor").alias("bucket"),
            coalesce("nickname", col("name"), "anon"),
            explode("tags"),
            posexplode(col("tags")),
            getField(col("address"), "city"),
            getItem(col("tags"), 0),
            getItem(col("attrs"), "k"),
            getItem(col("attrs"), col("key")),
            map_keys(col("attrs")),
            map_values(col("attrs")),
            elementAt(col("attrs"), "k"),
            elementAt(col("attrs"), col("key")),
            split(col("csv"), ","),
            split(col("csv"), col("sep")),
            from_json(col("payload"), "a INT, b STRING"),
            col("payload").from_json("a INT"),
            to_json(struct(col("a"), col("b"))),
            col("payload").to_json(),
            call("upper", ["name"]),
            col("amount").over(spec),
        );

        const labels = expectInspectable(df);
        expect(labels).toEqual(expect.arrayContaining([
            "select[26]", "alias(n)", "lit(1)", 'lit("x")', "lit(true)", "lit(10n)", "lit(null)", "case_when",
            "coalesce", "explode", "posexplode", "map_keys", "map_values",
            "window(partitionBy[1],orderBy[3],rows[ValuePreceding(5)..CurrentRow])",
        ]));
        expect(matching(labels, /^getField/).length).toBeGreaterThanOrEqual(1);
        expect(matching(labels, /^getItem/).length).toBeGreaterThanOrEqual(3);
        expect(matching(labels, /^element_at/).length).toBeGreaterThanOrEqual(2);
        expect(matching(labels, /^split/).length).toBeGreaterThanOrEqual(2);
        expect(matching(labels, /^from_json\(/).length).toBeGreaterThanOrEqual(2);
        expect(matching(labels, /^to_json/).length).toBeGreaterThanOrEqual(2);
    });

    it("renders casts, SQL expressions, lambdas, distinct calls and struct updates", () => {
        const df = people()
            .filter("age > 18")
            .where(col("age").lt(99))
            .select(
                col("age").cast("bigint"),
                col("age").tryCast("int"),
                expr("upper(name)"),
                F.transform("tags", (tag, i) => tag.plus(i)),
                F.count_distinct("id"),
                col("address").withField("zip", lit("1000")).dropFields("street"),
                col("address").getField("city"),
            );

        const labels = expectInspectable(df);
        expect(labels).toEqual(expect.arrayContaining([
            'expr("age > 18")', "cast(bigint)", "try_cast(int)", 'expr("upper(name)")', "count(distinct)",
            "withField(zip)", "dropField(street)", "extract",
        ]));
        expect(matching(labels, /^lambda\(x_\d+,i_\d+\)$/)).toHaveLength(1);
        expect(matching(labels, /^[xi]_\d+$/)).toHaveLength(2);

        const plan = JSON.parse(df.toSparkLogicalPlanJSON()) as { input: { condition: unknown }; columns: unknown[] };
        expect(plan.columns).toEqual(expect.arrayContaining([
            { type: "Cast", input: { type: "Column", name: "age" }, dataType: "bigint" },
            { type: "Cast", input: { type: "Column", name: "age" }, dataType: "int", evalMode: "try" },
            { type: "ExpressionString", expression: "upper(name)" },
            { type: "UnresolvedFunction", name: "count", args: [{ type: "Column", name: "id" }], isDistinct: true },
            {
                type: "UpdateFields",
                struct: {
                    type: "UpdateFields",
                    struct: { type: "Column", name: "address" },
                    field: "zip",
                    value: { type: "Literal", value: "1000" },
                },
                field: "street",
            },
            {
                type: "ExtractValue",
                child: { type: "Column", name: "address" },
                extraction: { type: "Literal", value: "city" },
            },
        ]));
        const lambda = (plan.columns[3] as { args: Array<{ type: string; params: string[]; body: unknown }> }).args[1]!;
        expect(lambda.type).toBe("LambdaFunction");
        expect(lambda.body).toEqual({
            type: "Binary",
            op: "+",
            left: { type: "LambdaVariable", name: lambda.params[0] },
            right: { type: "LambdaVariable", name: lambda.params[1] },
        });
        expect(JSON.stringify(plan)).toContain('"ExpressionString","expression":"age > 18"');
    });

    it("renders every DataFrame relation reachable from the public API", () => {
        const left = people()
            .withColumn("adult", col("age").gt(18))
            .withColumnRenamed("name", "full_name")
            .withColumnsRenamed({ id: "pid", age: "years" })
            .drop("email", "phone");
        const right = purchases()
            .sample(0.1, true, 42)
            .hint("broadcast")
            .hint("range_join", 10)
            .broadcast()
            .mergeHint()
            .shuffleHashHint()
            .shuffleReplicateNLHint()
            .repartition(4)
            .repartition(3, false)
            .coalescePartitions(2)
            .cache()
            .persist("MEMORY_ONLY")
            .persist()
            .unpersist(true)
            .limit(10)
            .distinct()
            .dropDuplicates()
            .dropDuplicates("user_id", "product");
        const joined = left.join(right, col("pid").eq(col("user_id")), "left");
        const grouped = joined
            .groupBy("pid", "product")
            .agg({ total: "sum(amount)", maxAmount: call("max", ["amount"]) });
        const combined = grouped
            .union(grouped)
            .unionByName(grouped)
            .unionByName(grouped, true)
            .intersect(grouped)
            .intersectAll(grouped)
            .except(grouped)
            .exceptAll(grouped);

        const labels = expectInspectable(combined);
        expect(labels).toEqual(expect.arrayContaining([
            "withColumn(adult)", "rename(name→full_name)", "renameMany(id→pid,age→years)", "drop(email,phone)",
            "sample(0,0.1,withReplacement,seed=42)", "hint(broadcast)", "hint(range_join,1 params)", "hint(merge)",
            "hint(shuffle_hash)", "hint(shuffle_replicate_nl)", "repartition(4,shuffle)", "repartition(3)",
            "coalesce(2)", "cache", "persist(MEMORY_ONLY)", "persist(MEMORY_AND_DISK)", "unpersist", "limit(10)",
            "distinct", "dropDuplicates[0]", "dropDuplicates[2]", "join(left)", "groupBy[2]", "agg[2]", "as(total)",
            "as(maxAmount)", "union", "unionByName", "unionByName(allowMissing)", "intersect", "intersectAll",
            "except", "exceptAll",
        ]));
    });

    it("renders statistics, splits, SQL and streaming relations", () => {
        expectInspectable(people().describe(["age"]));
        expectInspectable(people().summary(["count", "mean"], ["age"]));
        expectInspectable(people().summary(undefined, ["age"]));

        const [train, test] = people().randomSplit([0.7, 0.3], 7);
        for (const part of [train!, test!]) {
            expect(matching(expectInspectable(part), /^sample\(.*,seed=7,deterministic\)$/)).toHaveLength(1);
        }

        expect(expectInspectable(session.sql("SELECT 1 AS one").limit(1))).toEqual(
            expect.arrayContaining(["relation(sql)", "limit(1)"]),
        );
        expect(expectInspectable(people().sql("SELECT 1 AS one"))).toContain("sql(query)");

        const stream = session
            .readStream("rate", { rowsPerSecond: "1" })
            .withWatermark(col("timestamp"), "10 seconds")
            .filter(col("value").gt(0));
        const streamLabels = labelsOf(stream);
        expect(streamLabels).toEqual(expect.arrayContaining(["readStream(rate,opts)", "withWatermark(10 seconds)", "filter"]));
        expect(stream.toClientASTMermaid()).toContain("withWatermark(10 seconds)");
    });

    it("labels the interpreter-only variants that the public API never emits", () => {
        const id = TraceExprAlg.col("id");
        expect(TraceExprAlg.caseWhen([{ when: id, then: id }]).children).toHaveLength(2);
        expect(TraceExprAlg.sortKey(id, "asc").label).toBe("sortKey(asc)");
        expect(TraceExprAlg.sortKey(id, "desc", "nullsLast").label).toBe("sortKey(desc,nullsLast)");
        expect(TraceExprAlg.star().label).toBe("*");
        expect(TraceExprAlg.alias(id, "a", "b").label).toBe("alias(a,b)");
        expect(TraceExprAlg.win(id, { partitionBy: [], orderBy: [] }).label).toBe("window(partitionBy[0],orderBy[0])");
        expect(TraceExprAlg.logical("OR", id, id).label).toBe("OR");
        expect(TraceExprAlg.map_keys(id).label).toBe("map_keys");
        expect(TraceExprAlg.map_values(id).label).toBe("map_values");
        expect(TraceExprAlg.elementAt(id, id).children).toHaveLength(2);
        expect(TraceExprAlg.split(id, ",").label).toBe('split(",")');
        expect(TraceExprAlg.split(id, id).label).toBe("split");
        expect(TraceExprAlg.getItem(id, 0).label).toBe("getItem(0)");
        expect(TraceExprAlg.getItem(id, id).label).toBe("getItem");
        expect(TraceExprAlg.cast(id, "int", "ansi").label).toBe("cast(int,ansi)");
        expect(TraceExprAlg.cast(id, "int", "legacy").label).toBe("cast(int,legacy)");
        expect(TraceExprAlg.lambdaVar("x_1").label).toBe("x_1");
        expect(TraceExprAlg.extractValue(id, id).children).toHaveLength(2);

        const rel = TraceDFAlg.relation("csv", "/tmp/x");
        expect(TraceDFAlg.join(rel, rel, id).label).toBe("join(INNER)");
        expect(TraceDFAlg.agg(TraceDFAlg.groupBy(rel, [id]), { n: id }, "cube").label).toBe("agg[1](cube)");
        expect(TraceDFAlg.persist(rel).label).toBe("persist(MEMORY_AND_DISK)");
        expect(TraceDFAlg.hint(rel, "broadcast", []).label).toBe("hint(broadcast)");
        expect(TraceDFAlg.sample(rel, 0, 0.5).label).toBe("sample(0,0.5)");
        expect(TraceDFAlg.readStream("rate").label).toBe("readStream(rate)");
        expect(TraceDFAlg.withPlanId!(rel, 3)).toBe(rel);
        expect(TraceDFAlg.describe(rel, [id]).label).toBe("describe[1]");
        expect(TraceDFAlg.summary(rel, [id, id], [id]).label).toBe("summary[2x1]");
    });
});
