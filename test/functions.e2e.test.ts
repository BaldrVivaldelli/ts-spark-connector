import { createHash } from "node:crypto";
import { crc32 } from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";
import {
    SparkConnectError,
    SparkSession,
    Window,
    col,
    expr,
    functions as F,
    getField,
    lit,
    schema,
    type EBuilder,
} from "../src";
import { FUNCTION_CATALOG } from "./fixtures/function-catalog";
import { SAMPLE_COLUMNS, callFunction, sampleCall } from "./fixtures/function-samples";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";

function connectedBuilder() {
    const builder = SparkSession.builder().config("spark.connect.url", connectUrl);
    if (connectUrl.startsWith("scs://")) {
        builder.enableTLS({
            trustStorePath: caPath,
            serverNameOverride: serverName,
        });
    }
    return builder;
}

const session = connectedBuilder().getOrCreate();
afterAll(async () => session.close());

// Three people: ids 1 and 3 in "ar", id 2 in "us" with a null score.
const people = () => session.sql(`
    SELECT * FROM VALUES
      (1, 'Alice', 10.5D, array(1, 2, 3), map('a', 1, 'b', 2), named_struct('city', 'BA', 'zip', '1000'),
       DATE'2024-03-15', '{"k": 7, "tags": ["x", "y"]}', 'ar'),
      (2, 'bob', CAST(NULL AS DOUBLE), array(4, 5), map('c', 3), named_struct('city', 'NY', 'zip', '2000'),
       DATE'2024-12-31', '{"k": 8, "tags": []}', 'us'),
      (3, 'Carol', 7.0D, CAST(array() AS ARRAY<INT>), CAST(map() AS MAP<STRING, INT>),
       named_struct('city', 'BA', 'zip', '1001'), DATE'2025-01-01', 'not json', 'ar')
    AS t(id, name, score, xs, m, addr, d, js, country)`);

/** Evaluates named expressions against Alice's row. */
async function alice(columns: Record<string, EBuilder>): Promise<Record<string, unknown>> {
    const rows = await people()
        .filter(col("id").eq(1))
        .select(...Object.entries(columns).map(([name, expression]) => expression.alias(name)))
        .collect();
    expect(rows).toHaveLength(1);
    return rows[0]!;
}

/** Evaluates one expression per person, ordered by id. */
async function perPerson(expression: EBuilder): Promise<unknown[]> {
    const rows = await people().orderBy("id").select(expression.alias("v")).collect();
    return rows.map(row => row.v);
}

describe("function library against Spark", () => {
    it("only names functions the live server knows", async () => {
        const registry = new Set(
            (await session.sql("SHOW FUNCTIONS").collect()).map(row => String(row.function)),
        );
        const handWritten = [
            "count", "sum", "ln", "log", "transform", "filter", "exists", "forall", "aggregate", "reduce",
            "zip_with", "array_sort", "map_filter", "transform_keys", "transform_values", "map_zip_with",
            "window", "map", "not", "in", "like", "ilike", "rlike", "startswith", "endswith", "contains",
            "substr", "isnan",
        ];
        const missing = [...FUNCTION_CATALOG.map(fn => fn.sql), ...handWritten]
            // `product` is resolved by the Connect planner, not the registry.
            .filter(name => name !== "product" && !registry.has(name));
        expect(missing).toEqual([]);
    });

    it("resolves every catalog function with its required and its full argument list", async () => {
        // Spark analyzes each call (AnalyzePlan) over string columns c0..c19.
        // Type mismatches are expected with such arguments; what must never
        // happen is an unknown routine or a wrong argument count, which would
        // mean the catalog sends a name or an arity this server rejects.
        const relation = session.sql(`SELECT ${SAMPLE_COLUMNS.map(name => `'1' AS ${name}`).join(", ")}`);
        const unresolved: string[] = [];
        for (const fn of FUNCTION_CATALOG) {
            for (const includeOptional of [false, true]) {
                const { values } = sampleCall(fn, includeOptional);
                try {
                    await relation.select(callFunction(fn, values).alias("r")).explain();
                } catch (error) {
                    const { errorClass, message } = error as SparkConnectError;
                    if (/UNRESOLVED_ROUTINE|ROUTINE_NOT_FOUND|WRONG_NUM_ARGS/.test(`${errorClass} ${message}`)) {
                        unresolved.push(`${fn.name}/${values.length}: ${errorClass ?? message}`);
                    }
                }
            }
        }
        expect(unresolved).toEqual([]);
    }, 600_000);

    it("evaluates column methods with PySpark semantics", async () => {
        expect(await alice({
            asString: col("id").cast("string"),
            invalid: lit("abc").tryCast("int"),
            plus: col("id").plus(1),
            product: col("id").multiply(col("id")),
            half: col("score").divide(2),
            rem: col("id").mod(2),
            neq: col("id").neq(2),
            nullSafe: col("score").eqNullSafe(null),
            not: col("id").gt(1).not(),
            isin: col("id").isin(1, 3),
            between: col("id").between(0, 1),
            like: col("name").like("A%"),
            ilike: col("name").ilike("a%"),
            rlike: col("name").rlike("^A.*e$"),
            starts: col("name").startsWith("Al"),
            ends: col("name").endsWith("ce"),
            contains: col("name").contains("lic"),
            substr: col("name").substr(2, 3),
            nan: col("score").isNaN(),
            and: col("id").bitwiseAND(3),
            or: col("id").bitwiseOR(2),
            xor: col("id").bitwiseXOR(1),
            first: col("xs").getItem(0),
            mapValue: col("m").getItem("b"),
            city: col("addr").getField("city"),
            zip: getField(col("addr"), "zip"),
            added: col("addr").withField("country", lit("AR")),
            dropped: col("addr").dropFields("zip"),
        })).toEqual({
            asString: "1",
            invalid: null,
            plus: 2,
            product: 1,
            half: 5.25,
            rem: 1,
            neq: true,
            nullSafe: false,
            not: true,
            isin: true,
            between: true,
            like: true,
            ilike: true,
            rlike: true,
            starts: true,
            ends: true,
            contains: true,
            substr: "lic",
            nan: false,
            and: 1,
            or: 3,
            xor: 0,
            first: 1,
            mapValue: 2,
            city: "BA",
            zip: "1000",
            added: { city: "BA", zip: "1000", country: "AR" },
            dropped: { city: "BA" },
        });
        expect(await perPerson(col("score").eqNullSafe(null))).toEqual([false, true, false]);
    });

    it("runs SQL text through expr, selectExpr, filter and where", async () => {
        const rows = await people()
            .selectExpr("id * 10 AS tens", "upper(name) AS up")
            .filter("tens > 10")
            .where(expr("tens < 40"))
            .orderBy("tens")
            .collect();
        expect(rows).toEqual([{ tens: 20, up: "BOB" }, { tens: 30, up: "CAROL" }]);
    });

    it("evaluates string functions", async () => {
        expect(await alice({
            upper: F.upper("name"),
            initcap: F.initcap(lit("hello world")),
            lpad: F.lpad("name", 8, "*"),
            rpad: F.rpad("name", 6, "!"),
            concatWs: F.concat_ws("-", "name", "country"),
            replaced: F.regexp_replace("name", "[aeiou]", "_"),
            extracted: F.regexp_extract("name", "(A)(l)", 2),
            split: F.split(lit("a,b,c"), ","),
            splitLimit: F.split(lit("a,b,c"), ",", 2),
            index: F.substring_index(lit("a.b.c"), ".", 2),
            translate: F.translate(lit("abc"), "ab", "xy"),
            formatted: F.format_string("%s-%d", "name", "id"),
            length: F.length("name"),
            locate: F.locate("c", "name"),
            instr: F.instr("name", "ice"),
            repeat: F.repeat(lit("ab"), 2),
            reversed: F.reverse("name"),
            trimmed: F.trim(lit("xxhixx"), "x"),
            left: F.left("name", 2),
            right: F.right("name", 2),
            masked: F.mask(lit("Ab1-")),
            part: F.split_part(lit("a,b,c"), ",", 2),
            ascii: F.ascii("name"),
            base64: F.base64(lit("hi")),
        })).toEqual({
            upper: "ALICE",
            initcap: "Hello World",
            lpad: "***Alice",
            rpad: "Alice!",
            concatWs: "Alice-ar",
            replaced: "Al_c_",
            extracted: "l",
            split: ["a", "b", "c"],
            splitLimit: ["a", "b,c"],
            index: "a.b",
            translate: "xyc",
            formatted: "Alice-1",
            length: 5,
            locate: 4,
            instr: 3,
            repeat: "abab",
            reversed: "ecilA",
            trimmed: "hi",
            left: "Al",
            right: "ce",
            masked: "Xxn-",
            part: "b",
            ascii: 65,
            base64: "aGk=",
        });
    });

    it("trims in PySpark's argument order and buckets time windows", async () => {
        const at = F.to_timestamp(lit("2024-03-15 10:07:00"));
        const start = (window: EBuilder) => window.getField("start").cast("string");
        expect(await alice({
            trim: F.trim(lit("xxhixx"), "x"),
            ltrim: F.ltrim(lit("xxhixx"), "x"),
            rtrim: F.rtrim(lit("xxhixx"), "x"),
            spaces: F.trim(lit("  hi  ")),
            btrim: F.btrim(lit("xxhixx"), "x"),
        })).toEqual({
            trim: "hi",
            ltrim: "hixx",
            rtrim: "xxhi",
            spaces: "hi",
            btrim: "hi",
        });
        // Spark allows one time window per projection.
        expect(await alice({ tumbling: start(F.window(at, "5 minutes")) }))
            .toEqual({ tumbling: "2024-03-15 10:05:00" });
        expect(await alice({ offset: start(F.window(at, "5 minutes", undefined, "1 minute")) }))
            .toEqual({ offset: "2024-03-15 10:06:00" });
        const sliding = await people()
            .filter(col("id").eq(1))
            .select(start(F.window(at, "10 minutes", "5 minutes")).alias("start"))
            .orderBy("start")
            .collect();
        expect(sliding.map(row => row.start)).toEqual(["2024-03-15 10:00:00", "2024-03-15 10:05:00"]);
    });

    it("evaluates math functions", async () => {
        const row = await alice({
            round: F.round("score"),
            bround: F.bround(lit(2.5)),
            pow: F.pow(2, 10),
            sqrt: F.sqrt(lit(16)),
            abs: F.abs(lit(-3)),
            log2: F.log(2, lit(8)),
            ln: F.log(lit(1)),
            greatest: F.greatest("id", lit(5)),
            pmod: F.pmod(lit(-7), 3),
            ceil: F.ceil(lit(1.2)),
            floor: F.floor(lit(1.8)),
            hex: F.hex(lit(255)),
            conv: F.conv(lit("ff"), 16, 10),
            factorial: F.factorial(lit(5)),
            signum: F.signum(lit(-2)),
            pi: F.pi(),
        });
        expect(row).toMatchObject({
            round: 11,
            bround: 2,
            pow: 1024,
            sqrt: 4,
            abs: 3,
            log2: 3,
            ln: 0,
            greatest: 5,
            pmod: 2,
            ceil: 2n,
            floor: 1n,
            hex: "FF",
            conv: "255",
            factorial: 120n,
            signum: -1,
        });
        expect(row.pi).toBeCloseTo(Math.PI, 10);
    });

    it("evaluates date functions", async () => {
        expect(await alice({
            year: F.year("d"),
            month: F.month("d"),
            day: F.dayofmonth("d"),
            plus20: F.date_add("d", 20),
            minus15: F.date_sub("d", 15),
            diff: F.datediff(F.to_date(lit("2024-03-20")), "d"),
            formatted: F.date_format("d", "yyyy/MM/dd"),
            parsed: F.to_date(lit("15-03-2024"), "dd-MM-yyyy"),
            lastDay: F.last_day("d"),
            made: F.make_date(lit(2024), lit(2), lit(29)),
            truncated: F.trunc("d", "month"),
            nextMonth: F.add_months("d", 1),
            nextMonday: F.next_day("d", "Mon"),
            weekday: F.dayofweek("d"),
            week: F.weekofyear("d"),
            quarter: F.quarter("d"),
            months: F.months_between(F.to_date(lit("2024-05-15")), "d"),
        })).toEqual({
            year: 2024,
            month: 3,
            day: 15,
            plus20: "2024-04-04",
            minus15: "2024-02-29",
            diff: 5,
            formatted: "2024/03/15",
            parsed: "2024-03-15",
            lastDay: "2024-03-31",
            made: "2024-02-29",
            truncated: "2024-03-01",
            nextMonth: "2024-04-15",
            nextMonday: "2024-03-18",
            weekday: 6,
            week: 11,
            quarter: 1,
            months: 2,
        });
    });

    it("evaluates array and map functions", async () => {
        const row = await alice({
            size: F.size("xs"),
            contains: F.array_contains("xs", 2),
            elementAt: F.element_at("xs", 1),
            mapElement: F.element_at("m", "b"),
            distinct: F.array_distinct(F.array(lit(1), lit(1), lit(2))),
            sortedDesc: F.sort_array(F.array(lit(3), lit(1), lit(2)), false),
            slice: F.slice("xs", 2, 2),
            joined: F.array_join("xs", "|"),
            sequence: F.sequence(lit(1), lit(4)),
            flat: F.flatten(F.array(F.array(lit(1)), F.array(lit(2), lit(3)))),
            max: F.array_max("xs"),
            position: F.array_position("xs", 3),
            removed: F.array_remove("xs", 2),
            appended: F.array_append("xs", 4),
            get: F.get("xs", 0),
            keys: F.array_sort(F.map_keys("m")),
            hasKey: F.map_contains_key("m", "a"),
            created: F.create_map(lit("x"), lit(1)),
            fromArrays: F.map_from_arrays(F.array(lit("k")), F.array(lit(9))),
            parsed: F.str_to_map(lit("a:1,b:2")),
        });
        expect(row).toEqual({
            size: 3,
            contains: true,
            elementAt: 1,
            mapElement: 2,
            distinct: [1, 2],
            sortedDesc: [3, 2, 1],
            slice: [2, 3],
            joined: "1|2|3",
            sequence: [1, 2, 3, 4],
            flat: [1, 2, 3],
            max: 3,
            position: 3n,
            removed: [1, 3],
            appended: [1, 2, 3, 4],
            get: 1,
            keys: ["a", "b"],
            hasKey: true,
            created: new Map([["x", 1]]),
            fromArrays: new Map([["k", 9]]),
            parsed: new Map([["a", "1"], ["b", "2"]]),
        });
        const exploded = await people().filter(col("id").eq(1)).select(F.explode("xs").alias("x")).collect();
        expect(exploded.map(r => r.x)).toEqual([1, 2, 3]);
    });

    it("evaluates higher-order functions with JS lambdas", async () => {
        expect(await alice({
            times10: F.transform("xs", x => x.multiply(10)),
            plusIndex: F.transform("xs", (x, i) => x.plus(i)),
            odd: F.filter("xs", x => x.mod(2).eq(1)),
            afterFirst: F.filter("xs", (_x, i) => i.gt(0)),
            anyAbove2: F.exists("xs", x => x.gt(2)),
            allPositive: F.forall("xs", x => x.gt(0)),
            sum: F.aggregate("xs", lit(0), (acc, x) => acc.plus(x)),
            sumTimes10: F.reduce("xs", lit(0), (acc, x) => acc.plus(x), acc => acc.multiply(10)),
            squares: F.zip_with("xs", "xs", (x, y) => x.multiply(y)),
            descending: F.array_sort("xs", (l, r) => r.minus(l)),
            bigValues: F.map_filter("m", (_k, v) => v.gt(1)),
            upperKeys: F.transform_keys("m", k => F.upper(k)),
            hundreds: F.transform_values("m", (_k, v) => v.multiply(100)),
            doubled: F.map_zip_with("m", "m", (_k, v1, v2) => v1.plus(v2)),
            nested: F.transform(
                F.array(F.array(lit(1), lit(2)), F.array(lit(3))),
                row => F.transform(row, cell => cell.multiply(2)),
            ),
            // The inner lambda reads the outer lambda's variable.
            scaledSums: F.transform("xs", x => F.aggregate("xs", lit(0), (acc, y) => acc.plus(y.multiply(x)))),
        })).toEqual({
            times10: [10, 20, 30],
            plusIndex: [1, 3, 5],
            odd: [1, 3],
            afterFirst: [2, 3],
            anyAbove2: true,
            allPositive: true,
            sum: 6,
            sumTimes10: 60,
            squares: [1, 4, 9],
            descending: [3, 2, 1],
            bigValues: new Map([["b", 2]]),
            upperKeys: new Map([["A", 1], ["B", 2]]),
            hundreds: new Map([["a", 100], ["b", 200]]),
            doubled: new Map([["a", 2], ["b", 4]]),
            nested: [[2, 4], [6]],
            scaledSums: [6, 12, 18],
        });
    });

    it("evaluates JSON functions and forwards their options", async () => {
        expect(await alice({
            parsed: F.from_json("js", "k INT, tags ARRAY<STRING>"),
            json: F.to_json(F.struct(lit(1).alias("a"))),
            path: F.get_json_object("js", "$.k"),
            inferred: F.schema_of_json(lit('{"a": 1}')),
        })).toEqual({
            parsed: { k: 7, tags: ["x", "y"] },
            json: '{"a":1}',
            path: "7",
            inferred: "STRUCT<a: BIGINT>",
        });
        // FAILFAST only fails if the options map reaches the parser.
        await expect(
            people().select(F.from_json("js", "k INT", { mode: "FAILFAST" }).alias("v")).collect(),
        ).rejects.toThrow(/MALFORMED|malformed/);
    });

    it("evaluates conditional, hash and misc functions", async () => {
        const row = await alice({
            nullif: F.nullif("id", lit(1)),
            nvl2: F.nvl2("score", lit("has"), lit("none")),
            md5: F.md5(lit("Alice")),
            sha256: F.sha2(lit("Alice"), 256),
            crc: F.crc32(lit("Alice")),
            type: F.typeof("id"),
            uuid: F.uuid(),
            version: F.version(),
        });
        expect(row).toMatchObject({
            nullif: null,
            nvl2: "has",
            md5: createHash("md5").update("Alice").digest("hex"),
            sha256: createHash("sha256").update("Alice").digest("hex"),
            crc: BigInt(crc32("Alice")),
            type: "int",
        });
        expect(row.uuid).toMatch(/^[0-9a-f-]{36}$/);
        expect(row.version).toMatch(/^4\.\d+\.\d+/);
        expect(await perPerson(F.nvl("score", lit(0)))).toEqual([10.5, 0, 7]);
        expect(await perPerson(F.coalesce("score", lit(-1)))).toEqual([10.5, -1, 7]);
    });

    it("aggregates with the library functions", async () => {
        const rows = await people()
            .groupBy("country")
            .agg({
                rows: F.count("*"),
                scores: F.count("score"),
                names: F.array_sort(F.collect_list("name")),
                best: F.max_by("name", "score"),
                worst: F.min_by("name", "score"),
                big: F.count_if(col("score").gt(8)),
                allPositive: F.bool_and(col("id").gt(0)),
                distinctIds: F.approx_count_distinct("id"),
                idProduct: F.product("id"),
                median: F.median("score"),
                sd: F.stddev("score"),
            })
            .orderBy("country")
            .collect();
        expect(rows).toHaveLength(2);
        expect(rows[0]).toMatchObject({
            country: "ar",
            rows: 2n,
            scores: 2n,
            names: ["Alice", "Carol"],
            best: "Alice",
            worst: "Carol",
            big: 1n,
            allPositive: true,
            distinctIds: 2n,
            idProduct: 3,
            median: 8.75,
        });
        expect(rows[0]!.sd).toBeCloseTo(3.5 / Math.SQRT2, 10);
        expect(rows[1]).toMatchObject({ country: "us", rows: 1n, scores: 0n, best: null, sd: null });

        const [totals] = await people()
            .groupBy()
            .agg({
                countries: F.count_distinct("country"),
                parities: F.sum_distinct(F.pmod("id", 2)),
                mostCommon: F.mode("country"),
            })
            .collect();
        expect(totals).toEqual({ countries: 2n, parities: 1n, mostCommon: "ar" });
    });

    it("aggregates through the typed factory", async () => {
        const Sales = schema({ region: "string", amount: "double", note: "string?" });
        const rows = await session
            .createDataFrame(
                [
                    { region: "ar", amount: 10, note: "x" },
                    { region: "ar", amount: 30, note: null },
                    { region: "us", amount: 5, note: "y" },
                ],
                Sales,
            )
            .groupBy("region")
            .agg(a => [
                a.countDistinct("amount").as("distinct"),
                a.first("amount").as("first"),
                a.last("note", true).as("lastNote"),
                a.collectList("note").as("notes"),
                a.collectSet("region").as("regions"),
                a.stddev("amount").as("sd"),
                a.variance("amount").as("var"),
                a.approxCountDistinct("amount").as("approx"),
                a.countIf(c => c.amount.gt(6)).as("big"),
            ])
            .orderBy("region")
            .collect();

        expect(rows[0]).toMatchObject({
            region: "ar",
            distinct: 2n,
            lastNote: "x",
            notes: ["x"],
            regions: ["ar"],
            var: 200,
            approx: 2n,
            big: 2n,
        });
        expect([10, 30]).toContain(rows[0]!.first);
        expect(rows[0]!.sd).toBeCloseTo(Math.sqrt(200), 10);
        expect(rows[1]).toMatchObject({ region: "us", distinct: 1n, first: 5, sd: null, var: null, big: 0n });
    });

    it("evaluates window functions over partitions, frames and the whole result", async () => {
        const byCountry = Window.partitionBy("country").orderBy("id");
        const rows = await people()
            .select(
                col("id"),
                F.row_number().over(byCountry).alias("rn"),
                F.lag("id", 1, 0).over(byCountry).alias("prev"),
                F.lead("id").over(byCountry).alias("next"),
                F.dense_rank().over(Window.orderBy(col("country").desc())).alias("dr"),
                F.sum("id").over(Window.orderBy("id").rowsBetween(-1, 0)).alias("pair"),
                F.sum("id").over(Window.orderBy("id").rangeBetween(-Infinity, 0)).alias("running"),
                F.sum("id").over().alias("total"),
                F.count("*").over(Window.partitionBy("country")).alias("inCountry"),
            )
            .orderBy("id")
            .collect();
        expect(rows).toEqual([
            { id: 1, rn: 1, prev: 0, next: 3, dr: 2, pair: 1n, running: 1n, total: 6n, inCountry: 2n },
            { id: 2, rn: 1, prev: 0, next: null, dr: 1, pair: 3n, running: 3n, total: 6n, inCountry: 1n },
            { id: 3, rn: 2, prev: 1, next: null, dr: 2, pair: 5n, running: 6n, total: 6n, inCountry: 2n },
        ]);
    });
});
