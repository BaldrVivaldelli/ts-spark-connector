// test/examples.e2e.test.ts
import {describe, it, expect, beforeAll} from 'vitest';
import {explode, lit, posexplode, split, to_json, from_json, struct} from "../src/engine/column";


let spark: any;
let col: any, isNull: any, isNotNull: any, when: any;

beforeAll(async () => {
    // si no viene del entorno, usa el local
    process.env.SPARK_CONNECT_URL ??= 'sc://localhost:15002';

    ({spark} = await import('../src/client/session'));
    ({col, isNull, isNotNull, when} = await import('../src/engine/column'));
});

// helpers para obtener DF frescos en cada test
const people = () =>
    spark.read.option('delimiter', '\t').option('header', 'true').csv('/data/people.tsv');

const purchases = () =>
    spark.read.option('delimiter', '\t').option('header', 'true').csv('/data/purchases.tsv');

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
    it("should default to 'simple' mode when no argument is given", async () => {
        const df = purchases().select("user_id", "product");
        await df.explain();

    });
});
