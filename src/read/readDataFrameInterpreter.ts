import { Expression, GroupBy, LogicalPlan } from "../engine/logicalPlan";
import { DEFAULT_JOIN_TYPE, GroupTypeInput, JoinTypeInput } from "../engine/sparkConnectEnums";
import { DFAlg, ExprAlg } from "../algebra/read";
import { StreamingCaps } from "../algebra/read/streaming-dataframe";
import { SortOrder, WindowSpec } from "../types";

function exprToColumnName(expr: Expression): string | undefined {
    return expr.type === "Column" ? expr.name : undefined;
}

function exprsToColumnNames(exprs?: Expression[]): string[] | undefined {
    if (!exprs || exprs.length === 0) {
        return [];
    }

    const columnNames = exprs.map(exprToColumnName);
    return columnNames.every((name): name is string => typeof name === "string" && name.length > 0)
        ? columnNames
        : undefined;
}

export const SparkExprAlg: ExprAlg<Expression> = {
    col: (name) => ({ type: "Column", name }),
    lit: (v) => ({ type: "Literal", value: v }),
    bin: (op, left, right) => ({ type: "Binary", op, left, right }),
    logical: (op, left, right) => ({ type: "Logical", op, left, right }),
    alias: (input, alias) => ({ type: "Alias", input, alias }),
    call: (name, args) => ({ type: "UnresolvedFunction", name, args }),
    sortKey: (input, direction, nulls) => ({ type: "SortKey", input, direction, nulls }),
    star: () => ({ type: "UnresolvedStar" }),
    caseWhen: (branches, elze) => ({ type: "CaseWhen", branches, else: elze }),
    win: (func, spec: WindowSpec<Expression>) => ({
        type: "Window",
        func,
        spec: {
            partitionBy: spec.partitionBy,
            orderBy: spec.orderBy.map(o => ({
                type: "SortKey",
                input: o.input,
                direction: o.direction,
                nulls: o.nulls,
            })),
            frame: spec.frame && { ...spec.frame },
        },
    }),
    isNull: (input) => ({
        type: "UnresolvedFunction",
        name: "isnull",
        args: [input],
    }),
    isNotNull: (input) => ({
        type: "UnresolvedFunction",
        name: "isnotnull",
        args: [input],
    }),
    coalesce: (input) => ({
        type: "UnresolvedFunction",
        name: "coalesce",
        args: input,
    }),
    explode: (input) => ({
        type: "UnresolvedFunction",
        name: "explode",
        args: [input],
    }),
    posexplode: (input) => ({
        type: "UnresolvedFunction",
        name: "posexplode",
        args: [input],
    }),
    getField: (input, field) => ({
        type: "UnresolvedFunction",
        name: "getfield",
        args: [
            input,
            { type: "Literal", value: field },
        ],
    }),
    map_keys: (input) => ({
        type: "UnresolvedFunction",
        name: "map_keys",
        args: [input],
    }),
    map_values: (input) => ({
        type: "UnresolvedFunction",
        name: "map_values",
        args: [input],
    }),
    elementAt: (map, key) => ({
        type: "UnresolvedFunction",
        name: "element_at",
        args: [map, key],
    }),
    getItem: (collectionExpr, key) => ({
        type: "UnresolvedFunction",
        name: "element_at",
        args: [
            collectionExpr,
            typeof key === "object"
                ? key
                : typeof key === "number"
                    ? { type: "Literal", value: key }
                    : { type: "Literal", value: String(key) },
        ],
    }),
    split: (input, delimiter) => ({
        type: "UnresolvedFunction",
        name: "split",
        args: [
            input,
            typeof delimiter === "object"
                ? delimiter
                : { type: "Literal", value: delimiter },
        ],
    }),
    from_json: (jsonExpr, schema) => ({
        type: "UnresolvedFunction",
        name: "from_json",
        args: [jsonExpr, { type: "Literal", value: schema }],
    }),
    to_json: (expr) => ({
        type: "UnresolvedFunction",
        name: "to_json",
        args: [expr],
    }),
};

export const SparkDFAlg: DFAlg<LogicalPlan, Expression, GroupBy, StreamingCaps<LogicalPlan, Expression>> = {
    relation: (format, path, options) => ({ type: "Relation", format, path, options }),
    select: (plan, columns) => ({ type: "Project", input: plan, columns }),
    filter: (plan, condition) => ({ type: "Filter", input: plan, condition }),
    withColumn: (plan, name, column) => ({
        type: "Project",
        input: plan,
        columns: [{ type: "UnresolvedStar" }, { type: "Alias", input: column, alias: name }],
    }),
    join: (left, right, on, joinType = DEFAULT_JOIN_TYPE) => ({
        type: "Join",
        left,
        right,
        on,
        joinType: joinType.toUpperCase() as JoinTypeInput,
    }),
    groupBy: (plan, cols) => ({ type: "GroupBy", input: plan, expressions: cols }),
    agg: (group, aggregations, groupType?: GroupTypeInput) => ({
        type: "Aggregate",
        input: group,
        aggregations,
        groupType,
    }),
    orderBy: (plan, orders: SortOrder<Expression>[]) => ({
        type: "Sort",
        input: plan,
        orders: orders.map(o => o.expr.type === "SortKey"
            ? { expr: o.expr.input, direction: o.expr.direction, nulls: o.expr.nulls }
            : { expr: o.expr, direction: "asc" as const }
        ),
    }),
    sort: (plan, orders) => ({
        type: "Sort",
        input: plan,
        orders: orders.map(o => o.expr.type === "SortKey"
            ? { expr: o.expr.input, direction: o.expr.direction, nulls: o.expr.nulls }
            : { expr: o.expr, direction: "asc" as const }
        ),
    }),
    limit: (plan, n) => ({ type: "Limit", input: plan, limit: n }),
    distinct: (plan) => ({ type: "Distinct", input: plan }),
    dropDuplicates: (plan, cols) => {
        if (!cols || cols.length === 0) {
            return { type: "Distinct", input: plan };
        }

        const columnNames = exprsToColumnNames(cols);
        if (!columnNames) {
            throw new Error("dropDuplicates(...) currently only supports plain column references.");
        }

        return { type: "Deduplicate", input: plan, columnNames };
    },
    union: (left, right, opts) => ({
        type: "Union",
        inputs: [left, right],
        byName: !!opts?.byName,
        allowMissingColumns: !!opts?.allowMissingColumns,
    }),
    withColumnRenamed: (plan, oldName, newName) => ({
        type: "WithColumnsRenamed",
        input: plan,
        mapping: { [oldName]: newName },
    }),
    withColumnsRenamed: (plan, mapping) => ({
        type: "WithColumnsRenamed",
        input: plan,
        mapping: { ...mapping },
    }),
    describe: (plan, columns) => ({ type: "Describe", input: plan, columns }),
    summary: (plan, metrics, columns) => ({ type: "Summary", input: plan, metrics, columns }),
    cache: (plan) => ({ type: "Cache", input: plan }),
    persist: (plan, level) => ({ type: "Persist", input: plan, level }),
    unpersist: (plan, blocking) => ({ type: "Unpersist", input: plan, blocking }),
    repartition: (plan, numPartitions, shuffle) => ({
        type: "Repartition",
        input: plan,
        numPartitions,
        shuffle,
    }),
    coalesce: (plan, numPartitions) => ({
        type: "Coalesce",
        input: plan,
        numPartitions,
    }),
    sql: (query) => ({ type: "Sql", query }),
    hint: (plan, name, params) => ({
        type: "Hint",
        name,
        params: params ?? [],
        child: plan,
    }),
    sample: (plan, lowerBound, upperBound, withReplacement, seed, deterministicOrder) => ({
        type: "Sample",
        input: plan,
        lowerBound,
        upperBound,
        ...(withReplacement !== undefined ? { withReplacement } : {}),
        ...(seed !== undefined ? { seed } : {}),
        ...(deterministicOrder !== undefined ? { deterministicOrder } : {}),
    }),
    drop: (plan, columnNames) => ({
        type: "Drop",
        input: plan,
        columnNames,
    }),
    readStream: (format, options) => ({
        type: "Read",
        data_source: {
            format,
            paths: [],
            options: options ?? {},
            streaming: true,
        },
        is_streaming: true,
    }),
    withWatermark: (plan, eventTimeCol, delay) => {
        const eventTimeColumn = exprToColumnName(eventTimeCol);
        if (!eventTimeColumn) {
            throw new Error("withWatermark(...) currently only supports a plain event-time column reference.");
        }

        return {
            type: "EventTimeWatermark",
            input: plan,
            eventTimeColumn,
            delay,
        };
    },
};
