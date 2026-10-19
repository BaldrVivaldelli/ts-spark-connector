import type { SparkSession } from "../client/session";
import { applyPendingAnalyzeActions, ProtoExpr, ProtoExprAlg, ProtoRel } from "../engine/compilerRead";
import type { EBuilder } from "../engine/column";


type MergeActionType = "DELETE" | "INSERT" | "INSERT_STAR" | "UPDATE" | "UPDATE_STAR";

type MergeActionSpec = {
    readonly type: MergeActionType;
    readonly condition?: EBuilder;
    readonly assignments: ReadonlyArray<readonly [string, EBuilder]>;
};

type MergeSpec = {
    readonly matched: readonly MergeActionSpec[];
    readonly notMatched: readonly MergeActionSpec[];
    readonly notMatchedBySource: readonly MergeActionSpec[];
    readonly schemaEvolution: boolean;
};

function isBuildable(value: unknown): value is EBuilder {
    return value !== null && typeof value === "object" && typeof (value as EBuilder).build === "function";
}

function optionalCondition(label: string, condition: EBuilder | undefined): EBuilder | undefined {
    if (condition !== undefined && !isBuildable(condition)) {
        throw new TypeError(`${label} condition must be a column expression.`);
    }
    return condition;
}

function assignmentList(label: string, assignments: Readonly<Record<string, EBuilder>>): Array<[string, EBuilder]> {
    const entries = Object.entries(assignments);
    if (entries.length === 0) throw new TypeError(`${label} requires at least one column assignment.`);
    for (const [column, value] of entries) {
        if (!column.trim()) throw new TypeError(`${label} column names must be non-empty.`);
        if (!isBuildable(value)) {
            throw new TypeError(`${label} value for ${JSON.stringify(column)} must be a column expression; wrap constants in lit().`);
        }
    }
    return entries;
}

function protoMergeAction(action: MergeActionSpec): ProtoExpr {
    const EX = ProtoExprAlg;
    return {
        merge_action: {
            action_type: `ACTION_TYPE_${action.type}`,
            ...(action.condition ? { condition: action.condition.build(EX) } : {}),
            assignments: action.assignments.map(([column, value]) => ({
                key: EX.col(column),
                value: value.build(EX),
            })),
        },
    };
}

/**
 * `MERGE INTO` builder returned by `df.mergeInto(table, condition)`, mirroring
 * PySpark's `MergeIntoWriter`. Clauses apply in the order they are added;
 * `merge()` runs the command. Builders are immutable.
 * @public
 */
export class MergeIntoWriter {
    private spec: MergeSpec = {
        matched: [],
        notMatched: [],
        notMatchedBySource: [],
        schemaEvolution: false,
    };

    /**
     * @internal Obtained from `df.mergeInto(table, condition)`. `source`
     * compiles the source DataFrame to its Spark Connect relation.
     */
    constructor(
        private readonly session: SparkSession,
        private readonly source: () => unknown,
        private readonly table: string,
        private readonly condition: EBuilder,
    ) {
        if (typeof table !== "string" || !table.trim()) {
            throw new TypeError("mergeInto() table must be a non-empty string.");
        }
        if (!isBuildable(condition)) {
            throw new TypeError("mergeInto() requires a merge condition expression.");
        }
    }

    private with(patch: Partial<MergeSpec>): MergeIntoWriter {
        const next = new MergeIntoWriter(this.session, this.source, this.table, this.condition);
        next.spec = { ...this.spec, ...patch };
        return next;
    }

    /** @internal Appends an action to one clause list; used by the `when*` builders. */
    addAction(kind: "matched" | "notMatched" | "notMatchedBySource", action: unknown): MergeIntoWriter {
        return this.with({ [kind]: [...this.spec[kind], action as MergeActionSpec] });
    }

    /** Actions for target rows that match a source row (optionally only when `condition` holds). */
    whenMatched(condition?: EBuilder): WhenMatched {
        return new WhenMatched(this, optionalCondition("whenMatched()", condition));
    }

    /** Actions for source rows without a matching target row. */
    whenNotMatched(condition?: EBuilder): WhenNotMatched {
        return new WhenNotMatched(this, optionalCondition("whenNotMatched()", condition));
    }

    /** Actions for target rows without a matching source row. */
    whenNotMatchedBySource(condition?: EBuilder): WhenNotMatchedBySource {
        return new WhenNotMatchedBySource(this, optionalCondition("whenNotMatchedBySource()", condition));
    }

    /** Lets `updateAll` / `insertAll` add source columns that the target lacks. */
    withSchemaEvolution(): MergeIntoWriter {
        return this.with({ schemaEvolution: true });
    }

    /** Runs the `MERGE INTO` command. */
    async merge(): Promise<void> {
        const source = this.source() as ProtoRel;
        const command = this.command(source);
        const executor = await applyPendingAnalyzeActions(source, this.session);
        await executor.runCommand(command);
    }

    /** @internal The `merge_into_table_command` sent to Spark; used by plan-inspection tests. */
    buildCommand(): Record<string, unknown> {
        return this.command(this.source() as ProtoRel);
    }

    private command(source: ProtoRel): Record<string, unknown> {
        const { matched, notMatched, notMatchedBySource, schemaEvolution } = this.spec;
        if (matched.length + notMatched.length + notMatchedBySource.length === 0) {
            throw new TypeError("mergeInto() requires at least one whenMatched/whenNotMatched/whenNotMatchedBySource action.");
        }
        return {
            merge_into_table_command: {
                target_table_name: this.table,
                source_table_plan: source,
                merge_condition: this.condition.build(ProtoExprAlg),
                match_actions: matched.map(protoMergeAction),
                not_matched_actions: notMatched.map(protoMergeAction),
                not_matched_by_source_actions: notMatchedBySource.map(protoMergeAction),
                with_schema_evolution: schemaEvolution,
            },
        };
    }
}

/**
 * `whenMatched(...)` clause of a `MergeIntoWriter`.
 * @public
 */
export class WhenMatched {
    /** @internal */
    constructor(private readonly writer: MergeIntoWriter, private readonly condition?: EBuilder) {}

    /** Updates every target column from the source column of the same name. */
    updateAll(): MergeIntoWriter {
        return this.writer.addAction("matched", { type: "UPDATE_STAR", condition: this.condition, assignments: [] });
    }

    /** Updates the given target columns. */
    update(assignments: Readonly<Record<string, EBuilder>>): MergeIntoWriter {
        return this.writer.addAction("matched", {
            type: "UPDATE",
            condition: this.condition,
            assignments: assignmentList("whenMatched().update()", assignments),
        });
    }

    /** Deletes the matched target row. */
    delete(): MergeIntoWriter {
        return this.writer.addAction("matched", { type: "DELETE", condition: this.condition, assignments: [] });
    }
}

/**
 * `whenNotMatched(...)` clause of a `MergeIntoWriter`.
 * @public
 */
export class WhenNotMatched {
    /** @internal */
    constructor(private readonly writer: MergeIntoWriter, private readonly condition?: EBuilder) {}

    /** Inserts the source row, column by column. */
    insertAll(): MergeIntoWriter {
        return this.writer.addAction("notMatched", { type: "INSERT_STAR", condition: this.condition, assignments: [] });
    }

    /** Inserts a row with the given target column values. */
    insert(assignments: Readonly<Record<string, EBuilder>>): MergeIntoWriter {
        return this.writer.addAction("notMatched", {
            type: "INSERT",
            condition: this.condition,
            assignments: assignmentList("whenNotMatched().insert()", assignments),
        });
    }
}

/**
 * `whenNotMatchedBySource(...)` clause of a `MergeIntoWriter`.
 * @public
 */
export class WhenNotMatchedBySource {
    /** @internal */
    constructor(private readonly writer: MergeIntoWriter, private readonly condition?: EBuilder) {}

    /** Updates the given target columns. */
    update(assignments: Readonly<Record<string, EBuilder>>): MergeIntoWriter {
        return this.writer.addAction("notMatchedBySource", {
            type: "UPDATE",
            condition: this.condition,
            assignments: assignmentList("whenNotMatchedBySource().update()", assignments),
        });
    }

    /** Deletes the unmatched target row. */
    delete(): MergeIntoWriter {
        return this.writer.addAction("notMatchedBySource", { type: "DELETE", condition: this.condition, assignments: [] });
    }
}
