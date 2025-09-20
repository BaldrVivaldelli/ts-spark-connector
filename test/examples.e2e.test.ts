// test/examples.e2e.test.ts
import {describe, it, expect, beforeAll} from 'vitest';
import {explode, lit, posexplode, split, to_json, from_json, struct} from "../src";
import {SparkSession} from "../src";
import {ReadChainedDataFrame} from "../src/read/readChainedDataFrame";
let spark: any;
let col: any, isNull: any, isNotNull: any, when: any;

beforeAll(async () => {
    // si no viene del entorno, usa el local
    process.env.SPARK_CONNECT_URL ??= 'sc://localhost:15002';

    ({spark} = await import('../src/client/session'));
    ({col, isNull, isNotNull, when} = await import('../src/engine/column'));
});

const session = SparkSession.builder()
    .withAuth({ type: "token", token: "my-token" }) // opcional
    .enableTLS({
        keyStorePath: "./spark-server/certs/keystore.p12",
        keyStorePassword: "password",
        trustStorePath: "./spark-server/certs/cert.crt",
        trustStorePassword: "password"
    })
    .getOrCreate();
// helpers para obtener DF frescos en cada test
const people = () =>

    session.read.option('delimiter', '\t').option('header', 'true').csv('/data/people.tsv');

const purchases = () =>
    session.read.option('delimiter', '\t').option('header', 'true').csv('/data/purchases.tsv');

describe('examples (E2E)', () => {
    it('join + select + filter + groupBy + agg + show', async () => {
        const p = people();
        const pu = purchases();
        await p
            .join(pu, col('id').eq(col('user_id')))
            .select('name', 'product', 'amount')
            .filter(col('amount').gt(100))
            .groupBy('name')
            .agg({total_spent: 'sum(amount)', items_purchased: 'count(product)'})
            .show();
        expect(true).toBe(true);
    }, 90_000);

    it('select + sort + limit', async () => {
        await purchases().select('user_id', 'amount')
            .sort('user_id', col('amount').ascNullsFirst())
            .limit(5)
            .show();
        expect(true).toBe(true);
    }, 90_000);

    it('groupBy + agg + orderBy + limit', async () => {
        await purchases()
            .groupBy('user_id')
            .agg({total_spent: 'sum(amount)'})
            .orderBy(col('total_spent').descNullsLast())
            .limit(10)
            .show();
        expect(true).toBe(true);
    }, 90_000);

    it('dropDuplicates', async () => {
        await purchases().dropDuplicates('user_id', 'product').show();
        expect(true).toBe(true);
    }, 90_000);

    it('distinct country', async () => {
        await people().select('country').distinct().show();
        expect(true).toBe(true);
    }, 90_000);

    it('withColumnRenamed + select + limit', async () => {
        await purchases()
            .withColumnRenamed('user_id', 'customer_id')
            .select('customer_id', 'product', 'amount')
            .limit(3)
            .show();
        expect(true).toBe(true);
    }, 90_000);

    it('union y unionByName', async () => {
        const pu = purchases();
        const p2024 = pu.filter(col('year').eq(2024));
        const p2025 = pu.filter(col('year').eq(2025));
        await p2024.union(p2025).limit(5).show();
        await p2024.unionByName(p2025).limit(5).show();
        expect(true).toBe(true);
    }, 90_000);

    it('withColumn (gte) + when/otherwise', async () => {
        const pu = purchases();
        await pu
            .withColumn('amount_x10', col('amount').gte(10))
            .select('user_id', 'product', 'amount_x10')
            .limit(3)
            .show();

        await pu.withColumn(
            'spending_category',
            when(col('amount').gt(1000), 'VIP')
                .when(col('amount').gt(500), 'Premium')
                .when(col('amount').gt(100), 'Regular')
                .otherwise('Low')
        ).show();

        expect(true).toBe(true);
    }, 90_000);

    it('filters isNull / isNotNull (métodos y funciones)', async () => {
        await people().select('id', 'name', 'age').filter(col('age').isNull()).limit(5).show();
        await people().select('id', 'name', 'age').filter(col('age').isNotNull()).limit(5).show();
        await people().filter(isNull(col('age'))).select('id', 'name', 'age').limit(5).show();
        await people().filter(isNotNull(col('age'))).select('id', 'name', 'age').limit(5).show();
        expect(true).toBe(true);
    }, 90_000);

    it('coalesce + select + limit', async () => {
        await purchases()
            .coalesce('year', 'amount', 0)
            .select('user_id', 'product', 'amount')
            .limit(5)
            .show();
        expect(true).toBe(true);
    }, 90_000);

    // ------- writes -------
    it('write: save resumen', async () => {
        await purchases()
            .select('user_id', 'product', 'amount')
            .write
            .mode('overwrite')
            .save('/data/dest/purchases_summary');
        expect(true).toBe(true);
    }, 90_000);

    it('write: parquet / csv / json / saveAsTable', async () => {
        const pu = purchases();
        await pu.write.parquet().save('/data/dest/purchases_parquet');
        await pu.write.csv().option('header', true).save('/data/dest/purchases_csv');
        await pu.write.json().save('/data/dest/purchases_json');
        await pu.write.mode('overwrite').saveAsTable('purchases_tbl');
        expect(true).toBe(true);
    }, 90_000);

    it('write: partitionBy(year) parquet overwrite', async () => {
        await purchases().write
            .partitionBy('year')
            .parquet()
            .mode('overwrite')
            .save('/data/dest/purchases_by_year');
        expect(true).toBe(true);
    }, 90_000);

    it('write: bucketBy + sortBy + parquet + saveAsTable', async () => {
        await purchases().write
            .bucketBy(2, 'user_id')
            .sortBy('product')
            .parquet()
            .mode('overwrite')
            .saveAsTable('purchases_bucketed');
        expect(true).toBe(true);
    }, 90_000);

    it('pipeline topSpenders + write parquet', async () => {
        const pu = purchases();
        const topSpenders = pu
            .groupBy('user_id')
            .agg({total_spent: 'sum(amount)'})
            .orderBy(col('total_spent').descNullsLast());

        await topSpenders.write.parquet().mode('overwrite').save('/data/dest/top_spenders');
        expect(true).toBe(true);
    }, 90_000);

    it('write: orc + avro (option)', async () => {
        const pu = purchases();
        await pu.write.orc().mode('overwrite').save('/data/dest/purchases_orc');
        await pu.write.avro().option('compression', 'snappy').mode('append')
            .save('/data/dest/purchases_avro');
        expect(true).toBe(true);
    }, 90_000);
    it('describe: id,name,age,country,year', async () => {
        const df = people();
        await df
            .describe(["id", "name", "age", "country", "year"])
            .show();
        expect(true).toBe(true);
    }, 90_000);
    it('summary: count/min/50%/75%/max sobre age', async () => {
        const df = people();
        await df
            .summary(["count", "min", "50%", "75%", "max"], ["age"])
            .show();
        expect(true).toBe(true);
    }, 90_000);


    it('explode sin alias funciona (1 columna)', async () => {
        await purchases()
            .withColumn("tags_arr", split(col("tags"), lit(",")))  // dividir en array
            .select(
                col("user_id"),
                posexplode(col("tags_arr"))
            )
            .show();
        expect(true).toBe(true);
    }, 90_000);

    it('explode sin alias funciona (1 columna)', async () => {
        await purchases()
            .withColumn("tags_arr", split(col("tags"), lit(",")))
            .select(
                col("user_id"),
                explode(col("tags_arr"))
            )
            .limit(5)
            .show();
        expect(true).toBe(true);
    }, 90_000);
    it('from_json + to_json roundtrip (logical)', async () => {
        await purchases()
            .withColumn("jsonified", to_json(struct(col("product"))))
            .withColumn("parsed", from_json(col("jsonified"), "struct<product:string>"))
            .select("user_id", "product", "jsonified", "parsed")
            .limit(5)
            .show();

    }, 90_000);

    it('repartition and coalescePartitions (logical)', async () => {
        await purchases()
            .coalescePartitions(3)        // <-- 🧊 reduce particiones sin shuffle
            .select("tags", "product") // <-- 🧹 selecciona columnas deseadas
            .limit(5)
            .show();
    }, 90_000);

    it('repartition test', async () => {
        await purchases()
            .repartition(4) // cambia el número de particiones a 4 (con shuffle)
            .select("tags", "product")
            .limit(5)
            .show();
    }, 90_000);

    it("should support SQL over an existing DataFrame", async () => {
        const df = purchases().sql("SHOW TABLES");
        await df.explain();
    });

    it("should allow creating a temp view from a DataFrame", async () => {
        const df = purchases().select("user_id", "product", "amount").limit(5);
        await df.write.createTempView("temp_view_test");

        await df.sql("SELECT * FROM temp_view_test").show();
        const rows = await df.collectRaw();

        expect(rows.length).toBeGreaterThan(0);
    }, 90_000);

    it("should replace an existing temp view with createOrReplaceTempView", async () => {
        const df1 = purchases().select("user_id", "product", "amount").limit(5);
        await df1.write.createOrReplaceTempView("temp_view_test");

        const rows1 = await df1.sql("SELECT * FROM temp_view_test").collectRaw();
        expect(rows1.length).toBeGreaterThan(1);

        // Segundo DataFrame con solo 2 filas
        const df2 = purchases().select("user_id", "product", "amount").limit(2);
        await df2.write.createOrReplaceTempView("temp_view_test_new"); // debería reemplazar

        await df2.sql("SELECT * FROM temp_view_test_new").show();
    }, 90_000);

    it('toClientASTJSON() devuelve JSON legible', async () => {
        const df = purchases()
            .select('user_id', 'product', 'amount')
            .filter(col('amount').gt(100))
            .orderBy(col('user_id').descNullsLast())
            .limit(5);

        const json = df.toClientASTJSON();
        console.log('\n=== toClientASTJSON ===\n', json, '\n=======================\n');

        expect(typeof json).toBe('string');
        expect(json.length).toBeGreaterThan(10);
        expect(json.trim().startsWith('{')).toBe(true);

        const parsed = JSON.parse(json);
        expect(parsed).toBeTruthy();
        // chequeo suave para no acoplar tests
        expect(json).toMatch(/relation|select|filter|orderBy/i);
    }, 90_000);

    it('toClientASTMermaid() devuelve un diagrama Mermaid', async () => {
        const df = purchases()
            .select('user_id', 'product', 'amount')
            .filter(col('amount').gt(50))
            .orderBy(col('user_id').descNullsLast())
            .limit(3);

        const mermaid = df.toClientASTMermaid();
        console.log('\n=== toClientASTMermaid ===\n', mermaid, '\n==========================\n');
        expect(typeof mermaid).toBe('string');
        expect(mermaid.length).toBeGreaterThan(10);
        // formato Mermaid típico
        expect(mermaid.startsWith('flowchart')).toBe(true);
        expect(mermaid).toMatch(/-->/);
    }, 90_000);

    it('toSparkLogicalPlanJSON() devuelve JSON válido del logical plan cliente', async () => {
        const df = purchases()
            .select('user_id', 'product', 'amount')
            .filter(col('amount').gt(200))
            .limit(2);

        const json = df.toSparkLogicalPlanJSON();
        console.log('\n=== toSparkLogicalPlanJSON ===\n', json, '\n==============================\n');
        expect(typeof json).toBe('string');
        const obj = JSON.parse(json);
        expect(obj).toBeTruthy();
        // pista de que hay nodos comunes
        expect(json).toMatch(/relation|project|filter|csv/i);
    }, 90_000);

    it('toProtoJSON() devuelve JSON válido del proto', async () => {
        const df = purchases()
            .select('user_id', 'product')
            .limit(1);

        const json = df.toProtoJSON();
        console.log('\n=== toProtoJSON ===\n', json, '\n===================\n');
        expect(typeof json).toBe('string');
        const obj = JSON.parse(json);
        expect(obj).toBeTruthy();
    }, 90_000);

    it("emite nodo Hint en el LogicalPlan cliente (JSON)", () => {
        const right = session.read.csv("/data/purchases.tsv").broadcast(); // encadenable
        const left  = session.read.csv("/data/people.tsv");
        const df    = left.join(right, col("id").eq(col("user_id")), "INNER");

        const json = df.toSparkLogicalPlanJSON();
        expect(json).toMatch(/"type":\s*"Hint"/);
        expect(json).toMatch(/"name":\s*"broadcast"/);
    });

    it("sample(0.1) compila como Sample y ejecuta show()", async () => {
        const df = purchases().select("user_id", "product", "amount");

        const s1 = df.sample(0.1);
        const json1 = s1.toSparkLogicalPlanJSON();
        console.log("🔎 sample(0.1) LogicalPlan:", json1);

        expect(json1).toMatch(/"type":\s*"Sample"/);
        expect(json1).toMatch(/"upperBound":\s*0\.1/);

        await s1.limit(3).show();
        expect(true).toBe(true);
    }, 90_000);

    it("sample(1.5, true, 42) setea withReplacement y seed, y ejecuta show()", async () => {
        const df = purchases().select("user_id", "product", "amount");

        const s2 = df.sample(1.5, true, 42);
        const json2 = s2.toSparkLogicalPlanJSON();
        console.log("🔎 sample(1.5, true, 42) LogicalPlan:", json2);

        expect(json2).toMatch(/"type":\s*"Sample"/);
        expect(json2).toMatch(/"withReplacement":\s*true/);
        expect(json2).toMatch(/"seed":\s*42/);

        await s2.limit(3).show();
        expect(true).toBe(true);
    }, 90_000);

    it('randomSplit([0.8, 0.2], 7) añade rand, filtra por rangos y luego dropea la col temporal', async () => {
        const df = purchases().select("user_id", "product", "amount");
        const [train, test] = df.randomSplit([0.8, 0.2], 7);

        const trainJson = train.toSparkLogicalPlanJSON();
        const testJson  = test.toSparkLogicalPlanJSON();

        console.log("🔎 randomSplit train LogicalPlan:", trainJson);
        console.log("🔎 randomSplit test  LogicalPlan:", testJson);

        // raíz Drop (se elimina la col temporal en la salida)
        expect(trainJson).toMatch(/"type":\s*"Drop"/);
        expect(testJson).toMatch(/"type":\s*"Drop"/);

        // se agregó rand(seed=7) con alias __rand_split__
        expect(trainJson).toMatch(/"type":\s*"UnresolvedFunction"[\s\S]*"name":\s*"rand"[\s\S]*"value":\s*7/);
        expect(testJson).toMatch(/"type":\s*"UnresolvedFunction"[\s\S]*"name":\s*"rand"[\s\S]*"value":\s*7/);
        expect(trainJson).toMatch(/"alias":\s*"__rand_split__"/);
        expect(testJson).toMatch(/"alias":\s*"__rand_split__"/);

        // rangos: train [0.0, 0.8)  /  test [0.8, 1.0]
        expect(trainJson).toMatch(/"op":\s*">="\s*[\s\S]*"name":\s*"__rand_split__"[\s\S]*"value":\s*0(\.0+)?/);
        expect(trainJson).toMatch(/"op":\s*"<"\s*[\s\S]*"name":\s*"__rand_split__"[\s\S]*"value":\s*0\.8/);

        expect(testJson).toMatch(/"op":\s*">="\s*[\s\S]*"name":\s*"__rand_split__"[\s\S]*"value":\s*0\.8/);
        expect(testJson).toMatch(/"op":\s*"<="\s*[\s\S]*"name":\s*"__rand_split__"[\s\S]*"value":\s*1(\.0+)?/);

        // que ejecuten sin reventar
        await train.limit(3).show();
        await test.limit(3).show();
        expect(true).toBe(true);
    }, 90_000);

    it("streaming: readStream + watermark + trigger + outputMode aparecen en el AST de cliente", () => {
        const s = ReadChainedDataFrame
            .readStream<any, any, any>("rate", session, { rowsPerSecond: "1" })
            .withWatermark(col("timestamp"), "10 minutes")
            .select("value");

        // no ejecutamos show() (es streaming); verificamos el AST de cliente (trazas)
        const trace = s.toClientASTJSON();
        expect(trace).toMatch(/readStream/i);
        expect(trace).toMatch(/withWatermark/i);
    });

});
