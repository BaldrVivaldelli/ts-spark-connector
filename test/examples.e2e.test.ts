// test/examples.e2e.test.ts
import {afterAll, describe, it, expect} from 'vitest';
import {
    SparkSession,
    col,
    explode,
    from_json,
    isNotNull,
    isNull,
    lit,
    posexplode,
    split,
    struct,
    to_json,
    when,
} from "../src";
import {ReadChainedDataFrame} from "../src/read/readChainedDataFrame";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const sessionBuilder = SparkSession.builder().config("spark.connect.url", connectUrl);

if (connectUrl.startsWith("scs://")) {
    sessionBuilder.enableTLS({
        trustStorePath: process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt",
        serverNameOverride: process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect",
    });
}

const session = sessionBuilder.getOrCreate();
afterAll(async () => session.close());
// helpers para obtener DF frescos en cada test

const DEST_BASE =
    process.env.SPARK_TEST_DEST ?? '/tmp/ts-spark-connector-tests';

const dest = (name: string) => `${DEST_BASE}/${name}`;

const people = () =>
    session.read
        .option('delimiter', '\t')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv('/data/people.tsv');

const purchases = () =>
    session.read
        .option('delimiter', '\t')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv('/data/purchases.tsv');

// ------- fixtures (example_data/*.tsv, mounted at /data in the server) -------
// With inferSchema the TSV integers are IntegerType, so they decode as JS
// numbers; Spark aggregates (sum/count) are LongType and decode as bigint.
type Row = Record<string, unknown>;
type Purchase = { user_id: number; product: string; amount: number; year: number; tags: string };
type Person = { id: number; name: string; age: number | null; country: string; year: number };

const PURCHASES: Purchase[] = [
    { user_id: 1, product: 'Macbook', amount: 1200, year: 2024, tags: 'electronics,laptop,apple' },
    { user_id: 1, product: 'Mouse', amount: 25, year: 2024, tags: 'electronics,peripheral' },
    { user_id: 2, product: 'Monitor', amount: 300, year: 2025, tags: 'electronics,display' },
    { user_id: 3, product: 'Keyboard', amount: 80, year: 2024, tags: 'electronics,peripheral,input' },
    { user_id: 3, product: 'Chair', amount: 200, year: 2025, tags: 'furniture,ergonomic' },
];
const PEOPLE: Person[] = [
    { id: 1, name: 'Alice', age: 30, country: 'USA', year: 2024 },
    { id: 2, name: 'Bob', age: 25, country: 'Canada', year: 2024 },
    { id: 3, name: 'Carol', age: 40, country: 'Argentina', year: 2025 },
    { id: 3, name: 'Carol', age: null, country: 'Argentina', year: 2025 },
];
const PURCHASE_BY_PRODUCT = new Map(PURCHASES.map(purchase => [purchase.product, purchase]));
const PRODUCTS = PURCHASES.map(purchase => purchase.product).sort();
const TOTAL_AMOUNT = PURCHASES.reduce((sum, purchase) => sum + purchase.amount, 0); // 1805

const compareValues = (left: unknown, right: unknown): number => {
    // bigint/number/string compare naturally with < and >; null sorts first.
    if (left === right) return 0;
    if (left == null) return -1;
    if (right == null) return 1;
    return (left as number) < (right as number) ? -1 : 1;
};
const sorted = <T extends Row>(rows: readonly T[], ...keys: string[]): T[] =>
    [...rows].sort((left, right) => {
        for (const key of keys) {
            const order = compareValues(left[key], right[key]);
            if (order !== 0) return order;
        }
        return 0;
    });
const columnsOf = (rows: readonly Row[]): string[] => Object.keys(rows[0] ?? {});
const pick = <T extends Row>(row: T, ...keys: string[]): Row =>
    Object.fromEntries(keys.map(key => [key, row[key]]));

describe('examples (E2E)', () => {
    it('uses the TLS compatibility endpoint', () => {
        expect(connectUrl).toMatch(/^scs:\/\//);
        expect(process.env.SPARK_TLS_CA ?? './spark-server/certs/ca.crt').toMatch(/ca\.crt$/);
    });

    it('validates join type/condition, sort/null ordering and WithColumns behaviorally', async () => {
        const joined = await people()
            .join(purchases(), col('id').eq(col('user_id')), 'LEFT')
            .filter(col('product').eq('Macbook'))
            .select('name', 'product', 'amount')
            .collect() as Array<{ name: string; product: string; amount: number }>;
        expect(joined).toEqual([{ name: 'Alice', product: 'Macbook', amount: 1200 }]);

        const sorted = await people()
            .select('name', 'age')
            .orderBy(col('age').ascNullsFirst())
            .collect() as Array<{ name: string; age: number | null }>;
        expect(sorted[0]).toEqual({ name: 'Carol', age: null });

        const replaced = await purchases()
            .withColumn('amount', col('amount').gte(100))
            .select('product', 'amount')
            .orderBy('product')
            .collect() as Array<{ product: string; amount: boolean }>;
        expect(replaced.find(row => row.product === 'Macbook')?.amount).toBe(true);
        expect(replaced.find(row => row.product === 'Mouse')?.amount).toBe(false);
    }, 90_000);

    it('reads every path in a multipath relation', async () => {
        const rows = await session.read
            .option('delimiter', '\t')
            .option('header', 'true')
            .csv('/data/people.tsv', '/data/people.tsv')
            .collect() as Array<{ name: string }>;
        expect(rows).toHaveLength(8);
        // Both paths are read in full: every fixture row appears exactly twice.
        expect(rows.map(row => row.name).sort()).toEqual(
            [...PEOPLE, ...PEOPLE].map(person => person.name).sort(),
        );
    }, 90_000);

    it('round-trips a batch write through Spark', async () => {
        const path = dest('roundtrip_parquet');
        await purchases().write().parquet().mode('overwrite').save(path);
        const rows = await session.read.parquet(path).collect() as Purchase[];
        expect(sorted(rows, 'product')).toEqual(sorted(PURCHASES, 'product'));
    }, 90_000);

    it('join + select + filter + groupBy + agg + show', async () => {
        const p = people();
        const pu = purchases();
        const report = p
            .join(pu, col('id').eq(col('user_id')))
            .select('name', 'product', 'amount')
            .filter(col('amount').gt(100))
            .groupBy('name')
            .agg({total_spent: 'sum(amount)', items_purchased: 'count(product)'});

        // The only remaining show(): it covers the Arrow printer on real batches.
        await report.show();

        // Inner join: Carol (id 3) appears twice in people.tsv, so each of her
        // purchases joins twice. Only amounts > 100 survive the filter.
        const rows = await report.collect() as Array<{ name: string; total_spent: bigint; items_purchased: bigint }>;
        expect(sorted(rows, 'name')).toEqual([
            { name: 'Alice', total_spent: 1200n, items_purchased: 1n },
            { name: 'Bob', total_spent: 300n, items_purchased: 1n },
            { name: 'Carol', total_spent: 400n, items_purchased: 2n },
        ]);
    }, 90_000);

    it('select + sort + limit', async () => {
        const rows = await purchases().select('user_id', 'amount')
            .sort('user_id', col('amount').ascNullsFirst())
            .limit(5)
            .collect();
        expect(rows).toEqual([
            { user_id: 1, amount: 25 },
            { user_id: 1, amount: 1200 },
            { user_id: 2, amount: 300 },
            { user_id: 3, amount: 80 },
            { user_id: 3, amount: 200 },
        ]);
    }, 90_000);

    it('orderBy sorts globally across partitions', async () => {
        const rows = await session.sql('SELECT id FROM range(0, 200)')
            .repartition(5)
            .orderBy(col('id').desc())
            .collect() as Array<{ id: bigint }>;
        expect(rows.map(row => row.id)).toEqual(Array.from({ length: 200 }, (_, i) => BigInt(199 - i)));
    }, 90_000);

    it('groupBy + agg + orderBy + limit', async () => {
        const rows = await purchases()
            .groupBy('user_id')
            .agg({total_spent: 'sum(amount)'})
            .orderBy(col('total_spent').descNullsLast())
            .limit(10)
            .collect();
        expect(rows).toEqual([
            { user_id: 1, total_spent: 1225n },
            { user_id: 2, total_spent: 300n },
            { user_id: 3, total_spent: 280n },
        ]);
    }, 90_000);

    it('dropDuplicates', async () => {
        // purchases.tsv has no duplicate (user_id, product) pair: nothing is dropped.
        const uniquePairs = await purchases().dropDuplicates('user_id', 'product').collect() as Purchase[];
        expect(sorted(uniquePairs, 'product')).toEqual(sorted(PURCHASES, 'product'));

        // Restricting the key to user_id keeps exactly one purchase per user.
        const perUser = await purchases().dropDuplicates('user_id').collect() as Purchase[];
        expect(perUser.map(row => row.user_id).sort()).toEqual([1, 2, 3]);
        for (const row of perUser) expect(row).toEqual(PURCHASE_BY_PRODUCT.get(row.product));

        // people.tsv duplicates Carol (id 3) with and without an age.
        const uniquePeople = await people().dropDuplicates('id', 'name').collect() as Person[];
        expect(sorted(uniquePeople, 'id').map(row => pick(row, 'id', 'name'))).toEqual([
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
            { id: 3, name: 'Carol' },
        ]);
    }, 90_000);

    it('distinct country', async () => {
        const rows = await people().select('country').distinct().collect() as Array<{ country: string }>;
        expect(columnsOf(rows)).toEqual(['country']);
        expect(rows.map(row => row.country).sort()).toEqual(['Argentina', 'Canada', 'USA']);
    }, 90_000);

    it('withColumnRenamed + select + limit', async () => {
        const rows = await purchases()
            .withColumnRenamed('user_id', 'customer_id')
            .select('customer_id', 'product', 'amount')
            .limit(3)
            .collect() as Array<{ customer_id: number; product: string; amount: number }>;
        expect(rows).toHaveLength(3);
        expect(columnsOf(rows)).toEqual(['customer_id', 'product', 'amount']);
        expect(new Set(rows.map(row => row.product)).size).toBe(3);
        for (const row of rows) {
            const source = PURCHASE_BY_PRODUCT.get(row.product)!;
            expect(row).toEqual({ customer_id: source.user_id, product: source.product, amount: source.amount });
        }
    }, 90_000);

    it('union y unionByName', async () => {
        const pu = purchases();
        const p2024 = pu.filter(col('year').eq(2024));
        const p2025 = pu.filter(col('year').eq(2025));

        const unioned = await p2024.union(p2025).collect() as Purchase[];
        expect(sorted(unioned, 'product')).toEqual(sorted(PURCHASES, 'product'));

        // unionByName resolves columns by name, so a right side with a different
        // column order still lands every value in the right column.
        const reordered = p2025.select('tags', 'year', 'amount', 'product', 'user_id');
        const byName = await p2024.unionByName(reordered).collect() as Purchase[];
        expect(columnsOf(byName)).toEqual(['user_id', 'product', 'amount', 'year', 'tags']);
        expect(sorted(byName, 'product')).toEqual(sorted(PURCHASES, 'product'));
        expect(byName.reduce((sum, row) => sum + row.amount, 0)).toBe(TOTAL_AMOUNT);
    }, 90_000);

    it('withColumn (gte) + when/otherwise', async () => {
        const pu = purchases();
        const flagged = await pu
            .withColumn('amount_x10', col('amount').gte(10))
            .select('user_id', 'product', 'amount', 'amount_x10')
            .collect() as Array<{ user_id: number; product: string; amount: number; amount_x10: boolean }>;
        expect(flagged).toHaveLength(5);
        for (const row of flagged) {
            expect(row.amount).toBe(PURCHASE_BY_PRODUCT.get(row.product)!.amount);
            expect(row.amount_x10).toBe(row.amount >= 10);
        }

        const categorized = await pu.withColumn(
            'spending_category',
            when(col('amount').gt(1000), 'VIP')
                .when(col('amount').gt(500), 'Premium')
                .when(col('amount').gt(100), 'Regular')
                .otherwise('Low')
        ).collect() as Array<Purchase & { spending_category: string }>;
        expect(columnsOf(categorized)).toEqual(['user_id', 'product', 'amount', 'year', 'tags', 'spending_category']);
        expect(Object.fromEntries(categorized.map(row => [row.product, row.spending_category]))).toEqual({
            Macbook: 'VIP',
            Mouse: 'Low',
            Monitor: 'Regular',
            Keyboard: 'Low',
            Chair: 'Regular',
        });
    }, 90_000);

    it('filters isNull / isNotNull (métodos y funciones)', async () => {
        const withoutAge = [{ id: 3, name: 'Carol', age: null }];
        const withAge = [
            { id: 1, name: 'Alice', age: 30 },
            { id: 2, name: 'Bob', age: 25 },
            { id: 3, name: 'Carol', age: 40 },
        ];

        const methodNull = await people().select('id', 'name', 'age').filter(col('age').isNull()).limit(5).collect();
        const methodNotNull = await people().select('id', 'name', 'age').filter(col('age').isNotNull()).limit(5).collect();
        const functionNull = await people().filter(isNull(col('age'))).select('id', 'name', 'age').limit(5).collect();
        const functionNotNull = await people().filter(isNotNull(col('age'))).select('id', 'name', 'age').limit(5).collect();

        expect(methodNull).toEqual(withoutAge);
        expect(functionNull).toEqual(withoutAge);
        expect(sorted(methodNotNull, 'id')).toEqual(withAge);
        expect(sorted(functionNotNull, 'id')).toEqual(withAge);
    }, 90_000);

    it('coalesce + select + limit', async () => {
        // coalesce(age, 0) fills the one missing age in people.tsv.
        const rows = await people()
            .coalesce('age_filled', 'age', 0)
            .select('id', 'name', 'age_filled')
            .limit(5)
            .collect() as Array<{ id: number; name: string; age_filled: number }>;
        expect(sorted(rows, 'id', 'age_filled')).toEqual([
            { id: 1, name: 'Alice', age_filled: 30 },
            { id: 2, name: 'Bob', age_filled: 25 },
            { id: 3, name: 'Carol', age_filled: 0 },
            { id: 3, name: 'Carol', age_filled: 40 },
        ]);
    }, 90_000);

    // ------- writes -------
    it('write: save resumen', async () => {
        const path = dest('purchases_summary');
        await purchases()
            .select('user_id', 'product', 'amount')
            .write()                        // <-- antes: .write
            .mode('overwrite')
            .save(path);

        // save() without a format writes parquet.
        const rows = await session.read.parquet(path).collect() as Array<{ user_id: number; product: string; amount: number }>;
        expect(columnsOf(rows)).toEqual(['user_id', 'product', 'amount']);
        expect(sorted(rows, 'product')).toEqual(
            sorted(PURCHASES, 'product').map(row => pick(row, 'user_id', 'product', 'amount')),
        );
    }, 90_000);

    it('write: parquet / csv / json / saveAsTable', async () => {
        const pu = purchases();
        await pu.write().mode('overwrite').parquet().save(dest('purchases_parquet'));
        await pu.write().mode('overwrite').csv().option('header', true).save(dest('purchases_csv'));
        await pu.write().mode('overwrite').json().save(dest('purchases_tbl_vTest'));
        await pu.write().mode('overwrite')
            .saveAsTable('purchases_as_table_tbl');

        const expected = sorted(PURCHASES, 'product');

        const parquetRows = await session.read.parquet(dest('purchases_parquet')).collect() as Purchase[];
        expect(sorted(parquetRows, 'product')).toEqual(expected);

        // The tags column contains commas, so the CSV writer must quote it.
        const csvRows = await session.read
            .option('header', 'true')
            .option('inferSchema', 'true')
            .csv(dest('purchases_csv'))
            .collect() as Purchase[];
        expect(sorted(csvRows, 'product')).toEqual(expected);

        // JSON inference yields LongType, hence bigint values for the integers.
        const jsonRows = await session.read.json(dest('purchases_tbl_vTest')).collect() as Array<Record<string, unknown>>;
        expect(sorted(jsonRows, 'product').map(row => ({
            ...row,
            user_id: Number(row.user_id),
            amount: Number(row.amount),
            year: Number(row.year),
        }))).toEqual(expected);

        const tableRows = await session.table('purchases_as_table_tbl').collect() as Purchase[];
        expect(sorted(tableRows, 'product')).toEqual(expected);
    }, 90_000);

    it('write: partitionBy(year) parquet overwrite', async () => {
        const path = dest('purchases_by_year');
        await purchases().write()
            .partitionBy('year')
            .parquet()
            .mode('overwrite')
            .save(path);

        // The partition column is recovered from the directory layout.
        const rows = await session.read.parquet(path).collect() as Purchase[];
        expect(sorted(rows, 'product')).toEqual(sorted(PURCHASES, 'product'));

        // Reading one partition directory directly yields only that year's rows
        // (and, without basePath, no year column).
        const only2025 = await session.read.parquet(`${path}/year=2025`).collect() as Array<Omit<Purchase, 'year'>>;
        expect(columnsOf(only2025)).toEqual(['user_id', 'product', 'amount', 'tags']);
        expect(only2025.map(row => row.product).sort()).toEqual(['Chair', 'Monitor']);
    }, 90_000);


    it('write: bucketBy + sortBy + parquet + saveAsTable', async () => {
        await purchases().write()
            .bucketBy(2, 'user_id')
            .sortBy('product')
            .parquet()
            .mode('overwrite')
            .saveAsTable('purchases_bucketed_vTest_table');

        const tableRows = await session.table('purchases_bucketed_vTest_table').collect() as Purchase[];
        expect(sorted(tableRows, 'product')).toEqual(sorted(PURCHASES, 'product'));

        // The catalog records the bucket spec that the writer declared.
        const described = await session.sql('DESCRIBE EXTENDED purchases_bucketed_vTest_table')
            .collect() as Array<{ col_name: string; data_type: string }>;
        const detail = Object.fromEntries(described.map(row => [row.col_name, row.data_type]));
        expect(detail).toMatchObject({
            Provider: 'parquet',
            'Num Buckets': '2',
            'Bucket Columns': '[`user_id`]',
            'Sort Columns': '[`product`]',
        });
    }, 90_000);

    it('pipeline topSpenders + write parquet', async () => {
        const pu = purchases();
        const topSpenders = pu
            .groupBy('user_id')
            .agg({total_spent: 'sum(amount)'})
            .orderBy(col('total_spent').descNullsLast());

        await topSpenders.write().parquet().mode('overwrite').save(dest("top_spenders"));

        const rows = await session.read.parquet(dest('top_spenders')).collect() as Array<{ user_id: number; total_spent: bigint }>;
        expect(sorted(rows, 'user_id')).toEqual([
            { user_id: 1, total_spent: 1225n },
            { user_id: 2, total_spent: 300n },
            { user_id: 3, total_spent: 280n },
        ]);
    }, 90_000);

    it('write: orc + avro (option)', async () => {
        const pu = purchases();
        await pu.write().orc().mode('overwrite').save(dest('/purchases_orc'));
        const orcRows = await session.read.orc(dest('/purchases_orc')).collect() as Purchase[];
        expect(sorted(orcRows, 'product')).toEqual(sorted(PURCHASES, 'product'));

        // Start from a known state, then append: the second write doubles the rows.
        const avroPath = dest('purchases_avro');
        await pu.write().avro().option('compression', 'snappy').mode('overwrite').save(avroPath);
        await pu.write().avro().option('compression', 'snappy').mode('append')
            .save(avroPath);
        const avroRows = await session.read.avro(avroPath).collect() as Purchase[];
        expect(avroRows).toHaveLength(10);
        expect(sorted(avroRows, 'product')).toEqual(
            sorted([...PURCHASES, ...PURCHASES], 'product'),
        );
    }, 90_000)

    it('describe: id,name,age,country,year', async () => {
        const df = people();
        const rows = await df
            .describe(["id", "name", "age", "country", "year"])
            .collect() as Array<Record<string, string | null>>;
        expect(columnsOf(rows)).toEqual(['summary', 'id', 'name', 'age', 'country', 'year']);
        const stats = Object.fromEntries(rows.map(row => [row.summary, row]));
        expect(Object.keys(stats).sort()).toEqual(['count', 'max', 'mean', 'min', 'stddev']);

        expect(stats.count).toEqual({ summary: 'count', id: '4', name: '4', age: '3', country: '4', year: '4' });
        expect(stats.min).toEqual({ summary: 'min', id: '1', name: 'Alice', age: '25', country: 'Argentina', year: '2024' });
        expect(stats.max).toEqual({ summary: 'max', id: '3', name: 'Carol', age: '40', country: 'USA', year: '2025' });

        // Non-numeric columns have no mean/stddev; numeric ones match the fixture.
        expect(pick(stats.mean!, 'name', 'country')).toEqual({ name: null, country: null });
        expect(Number(stats.mean!.id)).toBeCloseTo(2.25, 10);
        expect(Number(stats.mean!.age)).toBeCloseTo(95 / 3, 10);
        expect(Number(stats.mean!.year)).toBeCloseTo(2024.5, 10);
        expect(pick(stats.stddev!, 'name', 'country')).toEqual({ name: null, country: null });
        expect(Number(stats.stddev!.id)).toBeCloseTo(Math.sqrt(2.75 / 3), 10);
        expect(Number(stats.stddev!.age)).toBeCloseTo(Math.sqrt((350 / 3) / 2), 10);
        expect(Number(stats.stddev!.year)).toBeCloseTo(Math.sqrt(1 / 3), 10);
    }, 90_000);
    it('summary: count/min/50%/75%/max sobre age', async () => {
        const df = people();
        const rows = await df
            .summary(["count", "min", "50%", "75%", "max"], ["age"])
            .collect() as Array<{ summary: string; age: string }>;
        expect(columnsOf(rows)).toEqual(['summary', 'age']);
        expect(rows.map(row => row.summary)).toEqual(['count', 'min', '50%', '75%', 'max']);

        // ages: 30, 25, 40 and one null
        const stats = Object.fromEntries(rows.map(row => [row.summary, row.age]));
        expect(stats.count).toBe('3');
        expect(stats.min).toBe('25');
        expect(stats.max).toBe('40');
        expect(Number(stats['50%'])).toBe(30);
        expect(Number(stats['75%'])).toBe(40);
    }, 90_000);


    it('posexplode sin alias funciona (pos + col)', async () => {
        const rows = await purchases()
            .withColumn("tags_arr", split(col("tags"), lit(",")))  // dividir en array
            .select(
                col("user_id"),
                col("product"),
                posexplode(col("tags_arr"))
            )
            .collect() as Array<{ user_id: number; product: string; pos: number; col: string }>;

        const expected = PURCHASES.flatMap(purchase =>
            purchase.tags.split(',').map((tag, pos) => ({
                user_id: purchase.user_id,
                product: purchase.product,
                pos,
                col: tag,
            })),
        );
        expect(rows).toHaveLength(12);
        expect(columnsOf(rows)).toEqual(['user_id', 'product', 'pos', 'col']);
        expect(sorted(rows, 'product', 'pos')).toEqual(sorted(expected, 'product', 'pos'));
    }, 90_000);

    it('explode sin alias funciona (1 columna)', async () => {
        const rows = await purchases()
            .withColumn("tags_arr", split(col("tags"), lit(",")))
            .select(
                col("user_id"),
                col("product"),
                explode(col("tags_arr"))
            )
            .collect() as Array<{ user_id: number; product: string; col: string }>;

        const expected = PURCHASES.flatMap(purchase =>
            purchase.tags.split(',').map(tag => ({
                user_id: purchase.user_id,
                product: purchase.product,
                col: tag,
            })),
        );
        expect(rows).toHaveLength(12);
        expect(columnsOf(rows)).toEqual(['user_id', 'product', 'col']);
        expect(sorted(rows, 'product', 'col')).toEqual(sorted(expected, 'product', 'col'));
    }, 90_000);
    it('from_json + to_json roundtrip (logical)', async () => {
        const rows = await purchases()
            .withColumn("jsonified", to_json(struct(col("product"))))
            .withColumn("parsed", from_json(col("jsonified"), "struct<product:string>"))
            .select("user_id", "product", "jsonified", "parsed")
            .limit(5)
            .collect() as Array<{ user_id: number; product: string; jsonified: string; parsed: { product: string } }>;

        expect(rows).toHaveLength(5);
        for (const row of rows) {
            expect(row.user_id).toBe(PURCHASE_BY_PRODUCT.get(row.product)!.user_id);
            expect(row.jsonified).toBe(JSON.stringify({ product: row.product }));
            expect(row.parsed).toEqual({ product: row.product });
        }
    }, 90_000);

    it('repartition and coalescePartitions (logical)', async () => {
        const rows = await purchases()
            .coalescePartitions(3)        // <-- 🧊 reduce particiones sin shuffle
            .select("tags", "product") // <-- 🧹 selecciona columnas deseadas
            .limit(5)
            .collect() as Array<{ tags: string; product: string }>;
        expect(columnsOf(rows)).toEqual(['tags', 'product']);
        expect(sorted(rows, 'product')).toEqual(
            sorted(PURCHASES, 'product').map(row => pick(row, 'tags', 'product')),
        );
    }, 90_000);

    it('repartition test', async () => {
        const rows = await purchases()
            .repartition(4) // cambia el número de particiones a 4 (con shuffle)
            .select("tags", "product")
            .limit(5)
            .collect() as Array<{ tags: string; product: string }>;
        // The shuffle may reorder rows, but never loses or duplicates one.
        expect(columnsOf(rows)).toEqual(['tags', 'product']);
        expect(sorted(rows, 'product')).toEqual(
            sorted(PURCHASES, 'product').map(row => pick(row, 'tags', 'product')),
        );
    }, 90_000);

    it("should support SQL over an existing DataFrame", async () => {
        const df = purchases().sql("SHOW TABLES");
        const plan = await df.explain();
        expect(plan).toContain('== Physical Plan ==');
        expect(plan).toContain('ShowTables');

        const rows = await df.collect() as Array<{ namespace: string; tableName: string; isTemporary: boolean }>;
        expect(columnsOf(rows)).toEqual(['namespace', 'tableName', 'isTemporary']);
        for (const row of rows) {
            expect(typeof row.tableName).toBe('string');
            expect(typeof row.isTemporary).toBe('boolean');
        }
    });

    it("should allow creating a temp view from a DataFrame", async () => {
        const df = purchases().select("user_id", "product", "amount").limit(5);
        await df.write().createTempView("temp_view_test");

        const viewRows = await session.sql("SELECT * FROM temp_view_test").collect() as Array<{ user_id: number; product: string; amount: number }>;
        expect(columnsOf(viewRows)).toEqual(['user_id', 'product', 'amount']);
        expect(sorted(viewRows, 'product')).toEqual(
            sorted(PURCHASES, 'product').map(row => pick(row, 'user_id', 'product', 'amount')),
        );

        const rows = await df.collectRaw();
        expect(rows.length).toBeGreaterThan(0);

        // Views are visible to the catalog as temporary.
        const tables = await session.sql("SHOW TABLES").collect() as Array<{ tableName: string; isTemporary: boolean }>;
        expect(tables).toContainEqual(expect.objectContaining({ tableName: 'temp_view_test', isTemporary: true }));
    }, 90_000);

    it("should replace an existing temp view with createOrReplaceTempView", async () => {
        const df1 = purchases().select("user_id", "product", "amount").limit(5);
        await df1.write().createOrReplaceTempView("temp_view_replace");

        const rows1 = await session.sql("SELECT * FROM temp_view_replace").collect();
        expect(rows1).toHaveLength(5);

        // Segundo DataFrame con solo 2 filas: debe reemplazar la vista anterior.
        const df2 = purchases().select("user_id", "product", "amount").limit(2);
        await df2.write().createOrReplaceTempView("temp_view_replace");

        const rows2 = await session.sql("SELECT * FROM temp_view_replace").collect() as Array<{ product: string }>;
        expect(rows2).toHaveLength(2);
        for (const row of rows2) expect(PURCHASE_BY_PRODUCT.has(row.product)).toBe(true);
        expect(await session.sql("SELECT count(*) AS n FROM temp_view_replace").collect()).toEqual([{ n: 2n }]);
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

    it("sample(0.1) compila como Sample y ejecuta collect()", async () => {
        const df = purchases().select("user_id", "product", "amount");

        const s1 = df.sample(0.1);
        const json1 = s1.toSparkLogicalPlanJSON();
        console.log("🔎 sample(0.1) LogicalPlan:", json1);

        expect(json1).toMatch(/"type":\s*"Sample"/);
        expect(json1).toMatch(/"upperBound":\s*0\.1/);

        // A 10% sample of 5 rows yields between 0 and 5 rows, always fixture rows.
        const rows = await s1.limit(3).collect() as Array<{ user_id: number; product: string; amount: number }>;
        expect(rows.length).toBeLessThanOrEqual(3);
        for (const row of rows) {
            expect(row).toEqual(pick(PURCHASE_BY_PRODUCT.get(row.product)!, 'user_id', 'product', 'amount'));
        }
    }, 90_000);

    it("sample(1.5, true, 42) setea withReplacement y seed, y ejecuta collect()", async () => {
        const df = purchases().select("user_id", "product", "amount");

        const s2 = df.sample(1.5, true, 42);
        const json2 = s2.toSparkLogicalPlanJSON();
        console.log("🔎 sample(1.5, true, 42) LogicalPlan:", json2);

        expect(json2).toMatch(/"type":\s*"Sample"/);
        expect(json2).toMatch(/"withReplacement":\s*true/);
        expect(json2).toMatch(/"seed":\s*42/);

        const rows = await s2.limit(3).collect() as Array<{ user_id: number; product: string; amount: number }>;
        expect(rows.length).toBeLessThanOrEqual(3);
        for (const row of rows) {
            expect(row).toEqual(pick(PURCHASE_BY_PRODUCT.get(row.product)!, 'user_id', 'product', 'amount'));
        }
        // The explicit seed makes the sample reproducible.
        expect(await s2.limit(3).collect()).toEqual(rows);
    }, 90_000);

    it('randomSplit([0.8, 0.2], 7) usa rangos Sample disjuntos sin columna temporal', async () => {
        const df = purchases().select("user_id", "product", "amount");
        const [train, test] = df.randomSplit([0.8, 0.2], 7);

        const trainJson = train.toSparkLogicalPlanJSON();
        const testJson  = test.toSparkLogicalPlanJSON();

        console.log("🔎 randomSplit train LogicalPlan:", trainJson);
        console.log("🔎 randomSplit test  LogicalPlan:", testJson);

        expect(trainJson).toMatch(/"type":\s*"Sample"/);
        expect(trainJson).toMatch(/"lowerBound":\s*0/);
        expect(trainJson).toMatch(/"upperBound":\s*0\.8/);
        expect(testJson).toMatch(/"lowerBound":\s*0\.8/);
        expect(testJson).toMatch(/"upperBound":\s*1/);
        expect(trainJson).toMatch(/"seed":\s*7/);
        expect(testJson).toMatch(/"seed":\s*7/);
        expect(trainJson).not.toContain("__rand_split__");
        expect(testJson).not.toContain("__rand_split__");

        // Disjoint Sample ranges over a deterministic order partition the input:
        // every fixture row lands in exactly one split.
        const trainRows = await train.collect() as Array<{ product: string }>;
        const testRows = await test.collect() as Array<{ product: string }>;
        const trainProducts = trainRows.map(row => row.product);
        const testProducts = testRows.map(row => row.product);
        expect(trainProducts.length + testProducts.length).toBe(5);
        expect([...trainProducts, ...testProducts].sort()).toEqual(PRODUCTS);
        expect(trainProducts.filter(product => testProducts.includes(product))).toEqual([]);
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

    it('streaming: writeStream serializa sink + trigger + outputMode en el AST', () => {
        const s = ReadChainedDataFrame
            .readStream<any, any, any>('rate', session, { rowsPerSecond: '1' })
            .withWatermark(col('timestamp'), '10 minutes')
            .select('value');

        const w = s.writeStream()
            .format('console')
            .outputMode('append')
            .trigger({ processingTime: '1 second' })
            .checkpoint('/chk/rate_stream')
            .queryName('rate_q');

        const trace = w.toClientASTJSON();

        expect(trace).toMatch(/"kind"\s*:\s*"streamingWrite"/i);

        expect(trace).toMatch(/"format"\s*:\s*"console"/i);
        expect(trace).toMatch(/"outputMode"\s*:\s*"append"/i);
        expect(trace).toMatch(/"trigger"/i);
        expect(trace).toMatch(/"checkpointLocation"/i);
        expect(trace).toMatch(/"queryName"\s*:\s*"rate_q"/i);

    });

    it('streaming: starts, times out, stops and exposes rows through a memory sink', async () => {
        const queryName = `rate_e2e_${Date.now()}`;
        const stream = ReadChainedDataFrame
            .readStream<any, any, any>('rate', session, {
                rowsPerSecond: '5',
                numPartitions: '1',
            })
            .select('value');

        const handle = await stream.writeStream()
            .format('memory')
            .outputMode('append')
            .trigger({ processingTime: '500 milliseconds' })
            .queryName(queryName)
            .start();

        expect(await handle.awaitTermination(2_000)).toBe(false);
        await handle.stop();
        expect(await handle.awaitTermination(30_000)).toBe(true);

        const rows = await session.table(queryName).collect() as Array<{ value: bigint }>;
        expect(rows.length).toBeGreaterThan(0);
        // The rate source emits a strictly increasing bigint counter from 0.
        const values = rows.map(row => row.value).sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
        expect(values[0]).toBe(0n);
        expect(values).toEqual(values.map((_value, index) => BigInt(index)));
    }, 120_000);


});
