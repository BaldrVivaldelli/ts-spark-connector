// Intérprete "final": del programa TF -> Protobuf directo (sin AST)
import {
    toProtoJoinType,
    DEFAULT_JOIN_TYPE,
    toProtoGroupType,
    toProtoSortDirection,
    // toProtoNullsOrder, // si tu proto lo necesita explícito en vez de boolean
    toProtoSetOpType, toProtoExplainMode, ExplainModeInput,
    JoinHintName,
} from "./sparkConnectEnums";
import {DFAlg, DFExec, ExprAlg, SortOrder, WindowSpec} from "../read/readDataframe";
import {SparkSession} from "../client/session";
import {SparkConnectExecutor} from "../client/sparkConnectExecutor";

// ======================== EXPRESIONES (E = ProtoExpr) ========================

type ProtoExpr = any; // ajustá al tipo real si lo tenés tipado

export const ProtoExprAlg: ExprAlg<ProtoExpr> = {
    col: (name) => ({
        unresolved_attribute: {unparsed_identifier: [name]},
    }),
    lit: (v) => {
        if (typeof v === "number") {
            return Number.isInteger(v)
                ? {literal: {integer: v}}
                : {literal: {double: v}};
        }
        if (typeof v === "boolean") return {literal: {boolean: v}};
        return {literal: {string: String(v)}};
    },
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
            throw new Error("caseWhen requiere 'otherwise' para generar if anidados.");
        }
        // arranca por el else y va envolviendo: if(whenN, thenN, acc)
        let acc: any = otherwise;
        for (let i = branches.length - 1; i >= 0; i--) {
            const b = branches[i];
            acc = {
                unresolved_function: {
                    function_name: "if",
                    arguments: [b.when, b.then, acc],
                },
            };
        }
        return acc;
    },
    win: (func, _spec: WindowSpec<ProtoExpr>) => ({
        win: {func},
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
    map_keys: (mapExpr: any) => ({
        unresolved_function: {
            function_name: "map_keys",
            arguments: [mapExpr]
        }
    }),
    map_values: (mapExpr: any) => ({
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
                        ? {literal: {integer: key}}
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

// ========================= DATAFRAME (R = ProtoRel) =========================

type ProtoRel = any;

type ProtoGroup = { __group__: { input: ProtoRel; keys: ProtoExpr[]; groupType?: any } };

export const ProtoDFAlg: DFAlg<ProtoRel, ProtoExpr, ProtoGroup> = {
    relation: (format, path, options) => ({
        read: {
            data_source: {
                format,
                paths: [path],
                options: options ?? {},
            },
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
        project: {
            input,
            // Alias primero + star (o al revés, según cómo lo quieras)
            expressions: [
                {alias: {expr: column, name: [name]}},
                {unresolved_star: {}},
            ],
        },
    }),

    join: (left, right, on, joinType = "INNER") => ({
        join: {
            left,
            right,
            condition: on,
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

    orderBy: (input, orders: SortOrder<ProtoExpr>[]) => ({
        sort: {
            input,
            order: orders.map(o => {
                // “desenvolvé” sortKey si vino de EX.sortKey(...)
                const expr = o.expr && o.expr.sort_key_marker ? o.expr.sort_key_marker.input : o.expr;
                return {
                    child: expr,
                    direction: toProtoSortDirection(o.direction), // "ASCENDING" | "DESCENDING"


                    // En tu compiler usabas boolean nulls_first; mantenemos ese contrato:
                    nulls_first: o.nulls === "nullsFirst"
                        ? true
                        : o.nulls === "nullsLast"
                            ? false
                            : undefined,
                };
            }),
        },
    }),

    sort: (input, orders) =>
        // mismo mapping que orderBy
        (ProtoDFAlg.orderBy as any)(input, orders),

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
        // Opción robusta: compilar a Aggregate (groupBy + agg vacía),
        // así no dependemos de soporte específico de “column_names”
        if (!cols || cols.length === 0) {
            return {deduplicate: {input, all_columns_as_keys: true}};
        }
        return {
            aggregate: {
                input,
                grouping_expressions: cols,
                group_type: toProtoGroupType("groupby"),
                aggregate_expressions: [], // sin medidas
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

    withColumnRenamed: (input, oldName, newName) => ({
        project: {
            input,
            expressions: [
                {
                    alias: {
                        expr: {unresolved_attribute: {unparsed_identifier: [oldName]}},
                        name: [newName]
                    }
                },
                {unresolved_star: {}},
            ],
        },
    }),

    withColumnsRenamed: (input, mapping) => ({
        project: {
            input,
            expressions: Object.entries(mapping).map(([from, to]) => ({
                alias: {
                    expr: {unresolved_attribute: {unparsed_identifier: [from]}},
                    name: [to],
                },
            })),
        },
    }),
    describe: (plan, columns) => ({
        project: {
            plan,
            expressions: columns,
        },
    }),
    summary: (plan, metrics, columns) => ({
        extension: {
            value: {
                input: plan,
                metrics,
                columns,
            },
        }
    }),
    cache: (input) => ({
        extension: {
            cache: {
                input
            }
        }
    }),

    persist: (input, level) => ({
        extension: {
            persist: {
                input,
                ...(level ? {storage_level: level} : {})
            }
        }
    }),

    unpersist: (input, blocking) => ({
        extension: {
            unpersist: {
                input,
                ...(blocking !== undefined ? {blocking} : {})
            }
        }
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
    hint: (plan: ProtoRel, name: string, params?: any[]) => ({
        hint: {
            input: plan,
            name: name,
            parameters: params ,
        }
    }),
};

export const ProtoExec: DFExec<ProtoRel> = {
    async collect(root, session) {
        return SparkConnectExecutor.for(session).execute(root);
    },

    async explain(root: any, session: SparkSession, mode: ExplainModeInput = "simple"): Promise<any[]> {
        return SparkConnectExecutor.for(session).execute(root);
    }
};

export function programToProtobufRoot(root: ProtoRel) {
    return {plan: {root}};
}
