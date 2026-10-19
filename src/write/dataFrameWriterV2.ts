import type { SparkSession } from "../client/session";
import { applyPendingAnalyzeActions, ProtoExprAlg, ProtoRel } from "../engine/compilerRead";
import type { EBuilder } from "../engine/column";
import type { WriterOptionValue } from "./dataFrameWriterTF";


/**
 * How `WriteOperationV2` treats the target table.
 * @public
 */
export type WriterV2Mode =
    | "create"
    | "replace"
    | "createOrReplace"
    | "append"
    | "overwrite"
    | "overwritePartitions";

const PROTO_MODES: Record<WriterV2Mode, string> = {
    create: "MODE_CREATE",
    replace: "MODE_REPLACE",
    createOrReplace: "MODE_CREATE_OR_REPLACE",
    append: "MODE_APPEND",
    overwrite: "MODE_OVERWRITE",
    overwritePartitions: "MODE_OVERWRITE_PARTITIONS",
};

type WriterV2Spec = {
    readonly provider?: string;
    readonly options: Readonly<Record<string, string>>;
    readonly tableProperties: Readonly<Record<string, string>>;
    readonly partitioning: readonly (string | EBuilder)[];
    readonly clustering: readonly string[];
    readonly schemaEvolution: boolean;
};

function requireNonEmpty(value: string, label: string): string {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${label} must be a non-empty string.`);
    }
    return value;
}

function optionValue(value: WriterOptionValue, label: string): string {
    if (value === null || value === undefined) {
        throw new TypeError(`${label} cannot be null or undefined.`);
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
        throw new RangeError(`${label} must be a finite number.`);
    }
    return String(value);
}

function isBuildable(value: unknown): value is EBuilder {
    return value !== null && typeof value === "object" && typeof (value as EBuilder).build === "function";
}

/**
 * Spark's DataFrameWriterV2 (`df.writeTo(table)`): configure the table, then
 * run one of `create`, `replace`, `createOrReplace`, `append`, `overwrite` or
 * `overwritePartitions`. Builders are immutable; every call returns a new
 * writer.
 * @public
 */
export class DataFrameWriterV2 {
    private spec: WriterV2Spec = {
        options: {},
        tableProperties: {},
        partitioning: [],
        clustering: [],
        schemaEvolution: false,
    };

    /**
     * @internal Obtained from `df.writeTo(table)`. `relation` compiles the
     * source DataFrame to its Spark Connect relation.
     */
    constructor(
        private readonly session: SparkSession,
        private readonly relation: () => unknown,
        private readonly table: string,
    ) {
        requireNonEmpty(table, "writeTo() table");
    }

    private with(patch: Partial<WriterV2Spec>): DataFrameWriterV2 {
        const next = new DataFrameWriterV2(this.session, this.relation, this.table);
        next.spec = { ...this.spec, ...patch };
        return next;
    }

    /** Table provider, e.g. `delta`, `iceberg` or `parquet`. */
    using(provider: string): DataFrameWriterV2 {
        return this.with({ provider: requireNonEmpty(provider, "using() provider") });
    }

    /** One write option. */
    option(key: string, value: WriterOptionValue): DataFrameWriterV2 {
        requireNonEmpty(key, "option() key");
        return this.with({
            options: { ...this.spec.options, [key]: optionValue(value, `option(${JSON.stringify(key)}) value`) },
        });
    }

    /** Several write options. */
    options(options: Readonly<Record<string, WriterOptionValue>>): DataFrameWriterV2 {
        const normalized = Object.fromEntries(
            Object.entries(options).map(([key, value]) => [
                requireNonEmpty(key, "options() key"),
                optionValue(value, `options()[${JSON.stringify(key)}]`),
            ]),
        );
        return this.with({ options: { ...this.spec.options, ...normalized } });
    }

    /** One table property, applied when the table is created or replaced. */
    tableProperty(property: string, value: string): DataFrameWriterV2 {
        requireNonEmpty(property, "tableProperty() property");
        if (typeof value !== "string") throw new TypeError("tableProperty() value must be a string.");
        return this.with({ tableProperties: { ...this.spec.tableProperties, [property]: value } });
    }

    /**
     * Partitions a created or replaced table by columns or partition
     * transforms (`call("years", [col("ts")])`, `call("bucket", [lit(4), col("id")])`).
     * Like PySpark, a later call replaces the earlier list.
     */
    partitionedBy(...cols: Array<string | EBuilder>): DataFrameWriterV2 {
        if (cols.length === 0) throw new TypeError("partitionedBy() requires at least one column.");
        for (const column of cols) {
            if (typeof column === "string") requireNonEmpty(column, "partitionedBy() column");
            else if (!isBuildable(column)) {
                throw new TypeError("partitionedBy() columns must be names or column expressions.");
            }
        }
        return this.with({ partitioning: [...cols] });
    }

    /** Clusters a created or replaced table by columns; a later call replaces the list. */
    clusterBy(...cols: string[]): DataFrameWriterV2 {
        if (cols.length === 0) throw new TypeError("clusterBy() requires at least one column.");
        for (const column of cols) requireNonEmpty(column, "clusterBy() column");
        return this.with({ clustering: [...cols] });
    }

    /**
     * Lets the write add columns the table lacks. Needs Spark Connect 4.2 or
     * newer: older servers reject the write with `UnsupportedServerVersionError`
     * before anything is sent.
     */
    withSchemaEvolution(): DataFrameWriterV2 {
        return this.with({ schemaEvolution: true });
    }

    /** Creates the table; fails if it already exists. */
    create(): Promise<void> {
        return this.run("create");
    }

    /** Replaces the table's contents and definition; fails if it does not exist. */
    replace(): Promise<void> {
        return this.run("replace");
    }

    /** Creates the table, or replaces it when it exists. */
    createOrReplace(): Promise<void> {
        return this.run("createOrReplace");
    }

    /** Appends to an existing table. */
    append(): Promise<void> {
        return this.run("append");
    }

    /** Replaces the rows matching `condition` with this DataFrame. */
    async overwrite(condition: EBuilder): Promise<void> {
        if (!isBuildable(condition)) throw new TypeError("overwrite() requires a condition expression.");
        return this.run("overwrite", condition);
    }

    /** Dynamically overwrites the partitions this DataFrame writes to. */
    overwritePartitions(): Promise<void> {
        return this.run("overwritePartitions");
    }

    /**
     * @internal The `write_operation_v2` command for `mode`, as sent to Spark;
     * used by plan-inspection tests.
     */
    buildCommand(mode: WriterV2Mode, condition?: EBuilder): Record<string, unknown> {
        return { write_operation_v2: this.operation(this.relation() as ProtoRel, mode, condition) };
    }

    private operation(input: ProtoRel, mode: WriterV2Mode, condition?: EBuilder): Record<string, unknown> {
        const EX = ProtoExprAlg;
        return {
            input,
            table_name: this.table,
            ...(this.spec.provider !== undefined ? { provider: this.spec.provider } : {}),
            partitioning_columns: this.spec.partitioning.map(column =>
                typeof column === "string" ? EX.col(column) : column.build(EX)
            ),
            options: { ...this.spec.options },
            table_properties: { ...this.spec.tableProperties },
            mode: PROTO_MODES[mode],
            ...(condition ? { overwrite_condition: condition.build(EX) } : {}),
            clustering_columns: [...this.spec.clustering],
            ...(this.spec.schemaEvolution ? { with_schema_evolution: true } : {}),
        };
    }

    private async run(mode: WriterV2Mode, condition?: EBuilder): Promise<void> {
        if (this.spec.schemaEvolution) {
            // Spark 4.1 already defines WriteOperationV2.with_schema_evolution
            // and accepts it, but the 4.1 server (with the Delta runtime the
            // E2E image bundles) still writes without evolving the schema and
            // fails with DELTA_METADATA_MISMATCH; the first line verified to
            // act on the flag is 4.2, so that is the behavioural gate.
            await this.session.requireServerVersion("4.2.0", "withSchemaEvolution()");
        }
        const input = this.relation() as ProtoRel;
        const executor = await applyPendingAnalyzeActions(input, this.session);
        await executor.runCommand({
            write_operation_v2: this.operation(input, mode, condition),
        });
    }
}
