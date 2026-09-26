// Intérprete "final": del programa TF -> Protobuf directo (sin AST)
import {
    toProtoJoinType,
    DEFAULT_JOIN_TYPE,
    toProtoGroupType,
    toProtoSortDirection,
    toProtoNullsOrder,
    toProtoSetOpType, ExplainModeInput, GroupTypeInput,
} from "./sparkConnectEnums";
import {SparkSession} from "../client/session";
import {SparkConnectExecutor} from "../client/sparkConnectExecutor";
import {SortOrder, WindowSpec} from "../types";
import {DFExec} from "../executables";
import {DFAlg, ExprAlg, LiteralValue} from "../algebra/read";
import {StreamingCaps} from "../algebra/read/streaming-dataframe";

type CDF = StreamingCaps<ProtoRel, ProtoExpr>;


// ======================== EXPRESIONES (E = ProtoExpr) ========================

/**
 * Minimal structural protobuf representation used by the direct compiler.
 *
 * The runtime objects are plain values consumed by `@grpc/proto-loader`; they
 * are not generated message classes. Keeping the recursive shape explicit
 * prevents `any` from leaking through the interpreter while still allowing
 * every Spark Connect message field emitted below.
 */
export type ProtoScalar = string | number | boolean | null;
export type ProtoValue = ProtoScalar | ProtoMessage | ProtoValue[];

export interface ProtoMessage {
    [field: string]: ProtoValue | undefined;
}

interface ProtoUnresolvedAttribute extends ProtoMessage {
    unparsed_identifier?: string | string[];
    unparsedIdentifier?: string | string[];
    plan_id?: number;
}

interface ProtoSortKeyMarker extends ProtoMessage {
    input: ProtoExpr;
    direction: "asc" | "desc";
    nulls?: "nullsFirst" | "nullsLast";
}

export interface ProtoExpr extends ProtoMessage {
    unresolved_attribute?: ProtoUnresolvedAttribute;
    unresolvedAttribute?: ProtoUnresolvedAttribute;
    sort_key_marker?: ProtoSortKeyMarker;
}

export interface ProtoRel extends ProtoMessage {
    common?: ProtoMessage;
}

export type ProtoAnalyzeAction =
    | { kind: "persist"; relation: ProtoRel; level: string }
    | { kind: "unpersist"; relation: ProtoRel; blocking?: boolean };

const PENDING_ANALYZE_ACTIONS = Symbol("ts-spark-connector.pendingAnalyzeActions");

function withAnalyzeAction(relation: ProtoRel, action: ProtoAnalyzeAction): ProtoRel {
    const next = { ...relation };
    Object.defineProperty(next, PENDING_ANALYZE_ACTIONS, {
        value: [
            ...((relation as { [PENDING_ANALYZE_ACTIONS]?: ProtoAnalyzeAction[] })[PENDING_ANALYZE_ACTIONS] ?? []),
            action,
        ],
        enumerable: false,
        configurable: false,
        writable: false,
    });
    return next;
}

function preserveAnalyzeActions(source: ProtoRel, target: ProtoRel): ProtoRel {
    const actions = (source as { [PENDING_ANALYZE_ACTIONS]?: ProtoAnalyzeAction[] })[
        PENDING_ANALYZE_ACTIONS
    ];
    if (!actions) return target;
    Object.defineProperty(target, PENDING_ANALYZE_ACTIONS, {
        value: actions,
        enumerable: false,
        configurable: false,
        writable: false,
    });
    return target;
}

/** Returns deferred AnalyzePlan actions embedded in a proto relation tree. */
export function getPendingAnalyzeActions(root: unknown): ProtoAnalyzeAction[] {
    const actions: ProtoAnalyzeAction[] = [];
    const visited = new Set<object>();
    const visit = (value: unknown) => {
        if (!value || typeof value !== "object" || visited.has(value)) return;
        visited.add(value);
        const carrier = value as { [PENDING_ANALYZE_ACTIONS]?: ProtoAnalyzeAction[] };
        for (const child of Object.values(value)) visit(child);
        if (carrier[PENDING_ANALYZE_ACTIONS]) actions.push(...carrier[PENDING_ANALYZE_ACTIONS]!);
    };
    visit(root);
    return actions;
}

/** Applies deferred cache/persist/unpersist actions before any execution path. */
export async function applyPendingAnalyzeActions(
    root: unknown,
    session: SparkSession,
): Promise<SparkConnectExecutor> {
    const executor = SparkConnectExecutor.for(session);
    for (const action of getPendingAnalyzeActions(root)) {
        await executor.runAnalyzeAction(action);
    }
    return executor;
}

function protoExprToColumnName(expr: ProtoExpr): string | undefined {
    const unresolvedAttribute = expr?.unresolved_attribute ?? expr?.unresolvedAttribute;
    const parts = unresolvedAttribute?.unparsed_identifier ?? unresolvedAttribute?.unparsedIdentifier;
    if (typeof parts === "string") {
        return parts.length > 0 ? parts : undefined;
    }

    // Accept the legacy in-memory representation while plans created after
    // this change use the proto-correct scalar string.
    return Array.isArray(parts) && parts.length > 0 ? parts.join(".") : undefined;
}

function protoExprsToColumnNames(exprs?: ProtoExpr[]): string[] | undefined {
    if (!exprs || exprs.length === 0) {
        return [];
    }

    const columnNames = exprs.map(protoExprToColumnName);
    return columnNames.every((name): name is string => typeof name === "string" && name.length > 0)
        ? columnNames
        : undefined;
}

const INT32_MIN = -2_147_483_648;
const INT32_MAX = 2_147_483_647;
const INT64_MIN = -9_223_372_036_854_775_808n;
const INT64_MAX = 9_223_372_036_854_775_807n;

function protoLiteral(value: LiteralValue): ProtoExpr {
    if (value === null) {
        // Expression.Literal.null contains a DataType, whose own `null` oneof
        // selects DataType.NULL. An empty DataType would leave the kind unset.
        return {literal: {null: {null: {}}}};
    }

    if (typeof value === "bigint") {
        if (value < INT64_MIN || value > INT64_MAX) {
            throw new RangeError(`BigInt literal ${value} is outside Spark's signed int64 range.`);
        }
        return {literal: {long: value.toString()}};
    }

    if (typeof value === "number") {
        if (!Number.isInteger(value)) {
            return {literal: {double: value}};
        }
        if (!Number.isSafeInteger(value)) {
            throw new RangeError(
                `Integer literal ${value} is not exactly representable as a JavaScript number; use a bigint instead.`,
            );
        }
        if (value >= INT32_MIN && value <= INT32_MAX) {
            return {literal: {integer: value}};
        }
        return {literal: {long: String(value)}};
    }

    if (typeof value === "boolean") return {literal: {boolean: value}};
    return {literal: {string: value}};
}

function protoHintParameter(value: unknown): ProtoExpr {
    if (value !== null && typeof value === "object") return value as ProtoExpr;
    if (["string", "number", "boolean", "bigint"].includes(typeof value) || value === null) {
        return protoLiteral(value as LiteralValue);
    }
    throw new TypeError(`Unsupported hint parameter type: ${typeof value}.`);
}

function protoSortOrder(
    child: ProtoExpr,
    direction: "asc" | "desc",
    nulls?: "nullsFirst" | "nullsLast",
) {
    return {
        child,
        direction: toProtoSortDirection(direction),
        null_ordering: toProtoNullsOrder(nulls, direction),
    };
}

type ProtoWindowFrame = NonNullable<WindowSpec<ProtoExpr>["frame"]>;
type ProtoWindowBoundary = ProtoWindowFrame["start"];

function protoWindowValue(
    frameType: ProtoWindowFrame["type"],
    value: number,
    preceding: boolean,
): ProtoExpr {
    const signedValue = preceding ? -Math.abs(value) : Math.abs(value);
    if (!Number.isSafeInteger(signedValue)) {
        throw new RangeError("Window frame boundaries must be safe integers.");
    }

    if (frameType === "rows") {
        if (signedValue < INT32_MIN || signedValue > INT32_MAX) {
            throw new RangeError("Row window frame boundaries must fit in a signed int32.");
        }
        return {literal: {integer: signedValue}};
    }

    // Spark Connect represents finite range-frame offsets as long literals,
    // even when the value would also fit in int32.
    return {literal: {long: String(signedValue)}};
}

function protoWindowBoundary(
    boundary: ProtoWindowBoundary,
    frameType: ProtoWindowFrame["type"],
): ProtoExpr {
    switch (boundary.type) {
        case "UnboundedPreceding":
        case "UnboundedFollowing":
            return {unbounded: true};
        case "CurrentRow":
            return {current_row: true};
        case "ValuePreceding":
            return {value: protoWindowValue(frameType, boundary.value, true)};
        case "ValueFollowing":
            return {value: protoWindowValue(frameType, boundary.value, false)};
    }
}

function protoWindowFrame(frame: ProtoWindowFrame) {
    return {
        frame_type: frame.type === "rows" ? "FRAME_TYPE_ROW" : "FRAME_TYPE_RANGE",
        lower: protoWindowBoundary(frame.start, frame.type),
        upper: protoWindowBoundary(frame.end, frame.type),
    };
}

export const ProtoExprAlg: ExprAlg<ProtoExpr> = {
    col: (name, planId) => ({
        unresolved_attribute: {
            unparsed_identifier: name,
            ...(planId === undefined ? {} : { plan_id: planId }),
        },
    }),
    lit: protoLiteral,
    bin: (op, left, right) => ({
        unresolved_function: {function_name: op, arguments: [left, right]},
    }),
    logical: (op, left, right) => ({
        unresolved_function: {function_name: op, arguments: [left, right]},
    }),
    alias: (input, name) => ({
        alias: {expr: input, name: [name]},
    }),
    call: (name, args) => ({
        unresolved_function: {function_name: name, arguments: args},
    }),
    sortKey: (input, direction, nulls) => ({
        // No es expresión ejecutable; el DFAlg.orderBy lo "desenvuelve".
        sort_key_marker: {input, direction, nulls},
    }),
    star: () => ({unresolved_star: {}}),
    caseWhen: (branches, otherwise) => {
        if (otherwise == null) {
            throw new Error("caseWhen requires an 'otherwise' branch to generate nested ifs.");
        }
        // arranca por el else y va envolviendo: if(whenN, thenN, acc)
        let acc: ProtoExpr = otherwise;
        for (let i = branches.length - 1; i >= 0; i--) {
            const b = branches[i];
            if (!b) {
                throw new Error("caseWhen received a sparse branch list.");
            }
            acc = {
                unresolved_function: {
                    function_name: "if",
                    arguments: [b.when, b.then, acc],
                },
            };
        }
        return acc;
    },
    win: (func, spec: WindowSpec<ProtoExpr>) => ({
        window: {
            window_function: func,
            partition_spec: spec.partitionBy,
            order_spec: spec.orderBy.map(order =>
                protoSortOrder(order.input, order.direction, order.nulls)
            ),
            ...(spec.frame ? {frame_spec: protoWindowFrame(spec.frame)} : {}),
        },
    }),
    isNull: (input) => ({
        unresolved_function: {
            function_name: "isnull",
            arguments: [input],
        },
    }),

    isNotNull: (input) => ({
        unresolved_function: {
            function_name: "isnotnull",
            arguments: [input],
        },
    }),

    coalesce: (args) => ({
        unresolved_function: {
            function_name: "coalesce",
            arguments: args,
        },
    }),
    explode: (input) => ({
        unresolved_function: {
            function_name: "explode",
            arguments: [input],
        }
    }),

    posexplode: (input) => ({
        unresolved_function: {
            function_name: "posexplode",
            arguments: [input],
        }
    }),
    getField: (structExpr, fieldName) => ({
        unresolved_function: {
            function_name: "getfield",
            arguments: [
                structExpr,
                {literal: {string: fieldName}}
            ]
        }
    }),
    map_keys: (mapExpr) => ({
        unresolved_function: {
            function_name: "map_keys",
            arguments: [mapExpr]
        }
    }),
    map_values: (mapExpr) => ({
        unresolved_function: {
            function_name: "map_values",
            arguments: [mapExpr]
        }
    }),
    elementAt: (mapExpr, key) => ({
        unresolved_function: {
            function_name: "element_at",
            arguments: [mapExpr, typeof key === "object" ? key : {literal: {string: String(key)}}]
        }
    }),
    getItem: (collectionExpr, key) => ({
        unresolved_function: {
            function_name: "element_at",
            arguments: [
                collectionExpr,
                typeof key === "object"
                    ? key
                    : typeof key === "number"
                        ? protoLiteral(key)
                        : {literal: {string: String(key)}}
            ]
        }
    }),
    split: (input, delimiter) => ({
        unresolved_function: {
            function_name: "split",
            arguments: [
                input,
                typeof delimiter === "object"
                    ? delimiter
                    : {literal: {string: String(delimiter)}}
            ]
        }
    }),
    from_json: (jsonExpr, schema) => ({
        unresolved_function: {
            function_name: "from_json",
            arguments: [
                jsonExpr,
                {literal: {string: schema}},
            ],
        },
    }),

    to_json: (expr) => ({
        unresolved_function: {
            function_name: "to_json",
            arguments: [expr],
        },
    }),

};

export type ProtoGroup = {
    __group__: { input: ProtoRel; keys: ProtoExpr[]; groupType?: GroupTypeInput };
};

function protoSortedRelation(
    input: ProtoRel,
    orders: SortOrder<ProtoExpr>[],
): ProtoRel {
    return {
        sort: {
            input,
            order: orders.map(order => {
                const marker = order.expr.sort_key_marker;
                return protoSortOrder(
                    marker?.input ?? order.expr,
                    marker?.direction ?? order.direction,
                    marker?.nulls ?? order.nulls,
                );
            }),
        },
    };
}

export const ProtoDFAlg: DFAlg<ProtoRel, ProtoExpr, ProtoGroup,CDF> = {
    relation: (format, path, options, schema) => {
        if (format === "table") {
            if (Array.isArray(path)) throw new TypeError("table reads accept exactly one identifier.");
            return {
                read: {
                    named_table: {
                        unparsed_identifier: path,
                        options: options ?? {},
                    },
                },
            };
        }
        if (format === "sql") {
            if (Array.isArray(path)) throw new TypeError("SQL reads accept exactly one query.");
            return { sql: { query: path } };
        }
        return {
            read: {
                data_source: {
                    format,
                    paths: Array.isArray(path) ? path : [path],
                    options: options ?? {},
                    ...(schema ? { schema } : {}),
                },
            },
        };
    },
    withPlanId: (input, planId) => preserveAnalyzeActions(input, {
        ...input,
        common: {
            ...(input.common ?? {}),
            plan_id: planId,
        },
    }),
    select: (input, columns) => ({
        project: {
            input,
            expressions: columns,
        },
    }),

    filter: (input, condition) => ({
        filter: {
            input,
            condition,
        },
    }),

    withColumn: (input, name, column) => ({
        with_columns: {
            input,
            aliases: [
                {expr: column, name: [name]},
            ],
        },
    }),

    join: (left, right, on, joinType = "INNER") => ({
        join: {
            left,
            right,
            join_condition: on,
            join_type: toProtoJoinType(joinType ?? DEFAULT_JOIN_TYPE),
        },
    }),

    groupBy: (input, cols) => ({__group__: {input, keys: cols}}),

    agg: (g, aggregations, groupType) => ({
        aggregate: {
            input: g.__group__.input,
            grouping_expressions: g.__group__.keys,
            group_type: toProtoGroupType(groupType ?? "groupby"),
            aggregate_expressions: Object.entries(aggregations).map(([alias, expr]) => ({
                alias: {expr, name: [alias]},
            })),
        },
    }),

    orderBy: protoSortedRelation,

    sort: protoSortedRelation,

    limit: (input, n) => ({
        limit: {input, limit: n},
    }),

    distinct: (input) => ({
        deduplicate: {
            input,
            all_columns_as_keys: true,
        },
    }),

    dropDuplicates: (input, cols) => {
        if (!cols || cols.length === 0) {
            return { deduplicate: { input, all_columns_as_keys: true } };
        }

        const columnNames = protoExprsToColumnNames(cols);
        if (!columnNames) {
            throw new Error("dropDuplicates(...) currently only supports plain column references.");
        }

        return {
            deduplicate: {
                input,
                column_names: columnNames,
            },
        };
    },

    union: (left, right, opts) => ({
        set_op: {
            left_input: left,
            right_input: right,
            set_op_type: toProtoSetOpType("union"),
            is_all: true,
            ...(opts?.byName !== undefined ? {by_name: opts.byName} : {}),
            ...(opts?.allowMissingColumns !== undefined
                ? {allow_missing_columns: opts.allowMissingColumns}
                : {}),
        },
    }),

    intersect: (left, right, opts) => ({
        set_op: {
            left_input: left,
            right_input: right,
            set_op_type: toProtoSetOpType("intersect"),
            is_all: !!opts?.all,
        },
    }),

    except: (left, right, opts) => ({
        set_op: {
            left_input: left,
            right_input: right,
            set_op_type: toProtoSetOpType("except"),
            is_all: !!opts?.all,
        },
    }),

    withColumnRenamed: (input, oldName, newName) => ({
        with_columns_renamed: {
            input,
            rename_columns_map: {
                [oldName]: newName,
            },
        },
    }),

    withColumnsRenamed: (input, mapping) => ({
        with_columns_renamed: {
            input,
            rename_columns_map: { ...mapping },
        },
    }),
    describe: (input, columns) => {
        const names = protoExprsToColumnNames(columns);
        if (!names) throw new TypeError("describe() only accepts plain column references.");
        return { describe: { input, cols: names } };
    },
    summary: (input, metrics, _columns) => ({
        summary: {
            input,
            statistics: metrics.map(metric => {
                const name = protoExprToColumnName(metric);
                if (!name) throw new TypeError("summary() metrics must be plain names.");
                return name;
            }),
        },
    }),
    cache: input => withAnalyzeAction(input, {
        kind: "persist",
        relation: input,
        level: "MEMORY_AND_DISK",
    }),

    persist: (input, level) => withAnalyzeAction(input, {
        kind: "persist",
        relation: input,
        level: level ?? "MEMORY_AND_DISK",
    }),

    unpersist: (input, blocking) => withAnalyzeAction(input, {
        kind: "unpersist",
        relation: input,
        blocking,
    }),
    repartition: (
        input,
        numPartitions: number,
        shuffle = true
    ) => ({
        repartition: {
            input,
            num_partitions: numPartitions,
            shuffle
        }
    }),

    coalesce: (input, numPartitions) => ({
        repartition: {
            input,
            num_partitions: numPartitions,
            shuffle: false
        }
    }),
    sql: (query: string) => ({
        sql: {
            query
        }
    }),
    hint: (input: ProtoRel, name: string, params?: unknown[]) => ({
        hint: {
            input,
            name,
            parameters: (params ?? []).map(protoHintParameter),
        }
    }),
    sample: (input, lower, upper, withReplacement, seed, deterministicOrder) => ({
        sample: {
            input,
            lower_bound: lower,
            upper_bound: upper,
            ...(withReplacement !== undefined ? { with_replacement: withReplacement } : {}),
            ...(seed !== undefined ? { seed } : {}),
            ...(deterministicOrder !== undefined ? { deterministic_order: deterministicOrder } : {}),
        }
    }),
    drop: (input, columnNames) => ({
        drop: {
            input,
            column_names: columnNames, // usamos nombres; también podrías usar 'columns' (expr)
        }
    }),
    withWatermark(input, eventTimeCol, delay) {
        const eventTimeColumn = protoExprToColumnName(eventTimeCol);
        if (!eventTimeColumn) {
            throw new Error("withWatermark(...) currently only supports a plain event-time column reference.");
        }

        return {
            with_watermark: {
                input,
                event_time: eventTimeColumn,
                delay_threshold: delay,
            },
        };
    },
    readStream: (format: string, options?: Record<string, string>) => ({
        read: {
            data_source: {
                format,
                paths: [],                 // p.ej. "rate" no requiere paths
                options: options ?? {},
            },
            is_streaming: true,          // ← va ACÁ (en Read), no en data_source
        },
    }),
};

export const ProtoExec: DFExec<unknown> = {
    async collect(root, session) {
        const executor = await applyPendingAnalyzeActions(root, session);
        // SparkConnectExecutor is shared with the client-side LogicalPlan
        // interpreter, but at this boundary it transports an already-compiled
        // protobuf relation. The executor never inspects the plan shape.
        return executor.execute(
            root as unknown as Parameters<SparkConnectExecutor["execute"]>[0]
        );
    },

    async explain(root: ProtoRel, session: SparkSession, mode: ExplainModeInput = "simple"): Promise<string> {
        const executor = await applyPendingAnalyzeActions(root, session);
        return executor.explain(
            root as unknown as Parameters<SparkConnectExecutor["explain"]>[0],
            mode,
        );
    }
};

export function programToProtobufRoot(root: unknown) {
    return {plan: {root}};
}
