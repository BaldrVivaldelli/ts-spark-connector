// src/read/DataFrameReaderTF.ts
import { ReadChainedDataFrame } from "./readChainedDataFrame";
import { SparkSession } from "../client/session";
import {DFProgram} from "../algebra/read";
import {DeclaredSchema, InferSchema, SchemaDef} from "../schema/schema";
import { UnknownSchema } from "../schema/schema-model";
import { validateTableIdentifier } from "../utils/identifiers";
import { SqlCap } from "../algebra/read/batch-capabilities";

/** @public */
export type Opts = Record<string, string>;
/** @public */
export type ReaderOptionValue = string | number | boolean | bigint;

/**
 * Partitioning and connection settings of `read.jdbc`, following PySpark's
 * `DataFrameReader.jdbc` keyword arguments.
 * @public
 */
export type JdbcReadOptions = {
    /** Numeric, date or timestamp column used to split the reads; requires the three bounds below. */
    column?: string;
    lowerBound?: number | bigint | string;
    upperBound?: number | bigint | string;
    numPartitions?: number;
    /** One partition per WHERE clause; cannot be combined with `column`. */
    predicates?: readonly string[];
    /** Connection properties such as `driver`, `user` or `password`, sent as data source options. */
    properties?: Record<string, ReaderOptionValue>;
};

function nonEmpty(value: string, label: string): string {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${label} must be a non-empty string.`);
    }
    return value;
}

function optionValue(value: ReaderOptionValue, label: string): string {
    if (value === null || value === undefined) {
        throw new TypeError(`${label} cannot be null or undefined.`);
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
        throw new RangeError(`${label} must be a finite number.`);
    }
    return String(value);
}

function oneOrManyPaths(paths: string[]): string | string[] {
    return paths.length === 1 ? paths[0]! : paths;
}

/** @public */
export class DataFrameReaderTF<R = unknown, E = unknown, G = unknown>  {
    private readonly fmt?: string;
    private readonly opts: Opts;

    constructor(private readonly session: SparkSession, fmt?: string, opts: Opts = {}) {
        this.fmt = fmt;
        this.opts = opts;
    }

    format(fmt: string): DataFrameReaderTF<R,E,G> {
        nonEmpty(fmt, "format()");
        return new DataFrameReaderTF<R,E,G>(this.session, fmt, this.opts);
    }

    option(key: string, value: ReaderOptionValue): DataFrameReaderTF<R,E,G> {
        nonEmpty(key, "option() key");
        return new DataFrameReaderTF<R,E,G>(this.session, this.fmt, {
            ...this.opts,
            [key]: optionValue(value, `option(${JSON.stringify(key)}) value`),
        });
    }

    options(kv: Record<string, ReaderOptionValue>): DataFrameReaderTF<R,E,G> {
        const normalized = Object.fromEntries(
            Object.entries(kv).map(([key, value]) => [
                nonEmpty(key, "options() key"),
                optionValue(value, `options()[${JSON.stringify(key)}]`),
            ])
        );
        return new DataFrameReaderTF<R,E,G>(this.session, this.fmt, { ...this.opts, ...normalized });
    }

    // Atajos con multipath
    csv(...paths: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.make("csv", oneOrManyPaths(paths));
    }
    json(...paths: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.make("json", oneOrManyPaths(paths));
    }
    parquet(...paths: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.make("parquet", oneOrManyPaths(paths));
    }
    orc(...paths: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.make("orc", oneOrManyPaths(paths));
    }
    text(...paths: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.make("text", oneOrManyPaths(paths));
    }
    avro(...paths: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.make("avro", oneOrManyPaths(paths));
    }

    /**
     * Reads a table or a query through JDBC. Spark reads `table` with a single
     * partition unless `column` with its bounds, or `predicates`, splits it.
     */
    jdbc(url: string, table: string, options: JdbcReadOptions = {}): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        nonEmpty(url, "jdbc() url");
        nonEmpty(table, "jdbc() table");
        if (options === null || typeof options !== "object") {
            throw new TypeError("jdbc() options must be an object.");
        }
        const { column, lowerBound, upperBound, numPartitions, predicates, properties } = options;
        const partitioning: Record<string, string> = {};
        if (column !== undefined) {
            nonEmpty(column, "jdbc() column");
            if (lowerBound === undefined || upperBound === undefined || numPartitions === undefined) {
                throw new TypeError("jdbc() column requires lowerBound, upperBound and numPartitions.");
            }
            if (!Number.isSafeInteger(numPartitions) || numPartitions < 1) {
                throw new RangeError("jdbc() numPartitions must be a positive safe integer.");
            }
            partitioning.partitionColumn = column;
            partitioning.lowerBound = optionValue(lowerBound, "jdbc() lowerBound");
            partitioning.upperBound = optionValue(upperBound, "jdbc() upperBound");
            partitioning.numPartitions = String(numPartitions);
        }
        if (predicates !== undefined) {
            if (column !== undefined) {
                throw new TypeError("jdbc() predicates cannot be combined with a partition column.");
            }
            if (!Array.isArray(predicates) || predicates.length === 0
                || predicates.some(predicate => typeof predicate !== "string" || !predicate.trim())) {
                throw new TypeError("jdbc() predicates must be a non-empty array of non-empty strings.");
            }
        }
        const normalizedProperties = Object.fromEntries(
            Object.entries(properties ?? {}).map(([key, value]) => [
                nonEmpty(key, "jdbc() property name"),
                optionValue(value, `jdbc() properties[${JSON.stringify(key)}]`),
            ])
        );
        const opts = { ...this.opts, ...normalizedProperties, ...partitioning, url, dbtable: table };
        const prog: DFProgram<R, E, G> = (DF) =>
            DF.relation("jdbc", [], opts, undefined, predicates === undefined ? undefined : [...predicates]);
        return new ReadChainedDataFrame<UnknownSchema, R, E, G>(prog, this.session);
    }

    load(...paths: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        const fmt = this.fmt ?? "parquet";
        return this.make(fmt, oneOrManyPaths(paths), true);
    }

    table(name: string): ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown> {
        const identifier = validateTableIdentifier(name);
        const prog: DFProgram<R, E, G, SqlCap<R>, unknown> = (DF) =>
            DF.relation("table", identifier, this.opts);
        return new ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown>(prog, this.session);
    }

    sql(query: string): ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown> {
        this.assertPaths("sql", query);
        const prog: DFProgram<R, E, G, SqlCap<R>, unknown> = (DF) =>
            DF.relation("sql", query, this.opts);
        return new ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown>(prog, this.session);
    }

    /**
     * Entrada de lectura tipada con declaración única de schema (Requirement 4.2).
     *
     * A partir de un único `DeclaredSchema` (creado con `schema({...})`):
     *
     *  1. RUNTIME: inyecta `declared.toDDL()` en `Read.DataSource.schema` del plan,
     *     de modo que Spark aplique **exactamente** ese schema en lugar de inferirlo.
     *     Esto elimina el drift entre el tipo que se programa y el schema que el
     *     servidor aplica.
     *
     *  2. COMPILE TIME: tipa el `ReadChainedDataFrame` resultante como
     *     `InferSchema<D>`, habilitando el camino tipado sobre la misma superficie
     *     pública (la misma fuente de verdad token→tipo / token→DDL).
     *
     * Es la vía recomendada para tipar un DataFrame (promesa *verificada*), frente
     * a `.as<T>()` (promesa *no verificada* para fuentes dinámicas).
     *
     * @example
     *   const People = schema({ id: "int", name: "string", age: "int?" });
     *   const df = session.read.readWith(People, "csv", "people.csv", { header: "true" });
     *   // df: ReadChainedDataFrame<{ id: number; name: string; age: number | null }, ...>
     */
    readWith<D extends SchemaDef>(
        declared: DeclaredSchema<D>,
        format: string,
        path: string | string[],
        options?: Record<string, ReaderOptionValue>,
    ): ReadChainedDataFrame<InferSchema<D>, R, E, G> {
        const provider = nonEmpty(format, "readWith() format").trim();
        const normalizedFormat = provider.toLowerCase();
        if (normalizedFormat === "table" || normalizedFormat === "sql") {
            throw new TypeError(
                `readWith() cannot enforce a declared schema for ${normalizedFormat}; ` +
                `use session.${normalizedFormat}(...) and .as<T>() only as an explicit unchecked assertion.`
            );
        }
        this.assertPaths(provider, path);
        const normalizedOptions = Object.fromEntries(
            Object.entries(options ?? {}).map(([key, value]) => [
                nonEmpty(key, "readWith() option key"),
                optionValue(value, `readWith() options[${JSON.stringify(key)}]`),
            ])
        );
        const opts = { ...this.opts, ...normalizedOptions };
        const ddl = declared.toDDL();
        const prog: DFProgram<R, E, G> = (DF) => DF.relation(provider, path, opts, ddl);
        return new ReadChainedDataFrame<InferSchema<D>, R, E, G>(
            prog,
            this.session,
            false,
            declared.def,
        );
    }


    // ---------- interno ----------
    private make(format: string, path: string | string[], allowNoPath = false): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        nonEmpty(format, "read format");
        this.assertPaths(format, path, allowNoPath);
        const prog: DFProgram<R,E,G> = (DF) => DF.relation(format, path, this.opts);
        return new ReadChainedDataFrame<UnknownSchema, R,E,G>(prog, this.session);
    }

    private assertPaths(format: string, path: string | string[], allowNoPath = false): void {
        if (format === "table" || format === "sql") {
            if (Array.isArray(path) || typeof path !== "string" || !path.trim()) {
                throw new TypeError(`${format} requires exactly one non-empty string.`);
            }
            return;
        }
        const paths = Array.isArray(path) ? path : [path];
        if (allowNoPath && paths.length === 0) return;
        if (paths.length === 0 || paths.some(value => typeof value !== "string" || !value.trim())) {
            throw new TypeError(`${format} read requires at least one non-empty path.`);
        }
    }
}
