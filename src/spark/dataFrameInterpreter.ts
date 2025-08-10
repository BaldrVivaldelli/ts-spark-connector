import { Expression, GroupBy, LogicalPlan } from "../engine/logicalPlan";
import {DEFAULT_JOIN_TYPE, GroupTypeInput, JoinTypeInput} from "../engine/sparkConnectEnums";
import {DFAlg, ExprAlg, SortOrder, WindowSpec} from "./dataframe";

export const SparkExprAlg: ExprAlg<Expression> = {
    col: (name) => ({type: "Column", name}),
    lit: (v) => ({type: "Literal", value: v}),
    bin: (op, left, right) => ({type: "Binary", op, left, right}),
    logical: (op, left, right) => ({type: "Logical", op, left, right}),
    alias: (input, alias) => ({type: "Alias", input, alias}),
    call: (name, args) => ({type: "UnresolvedFunction", name, args}),
    sortKey: (input, direction, nulls) => ({type: "SortKey", input, direction, nulls}),
    star: () => ({type: "UnresolvedStar"}),
    caseWhen: (branches, elze) => ({type: "CaseWhen", branches, else: elze}),
    win: (func, spec: WindowSpec<Expression>) => ({
        type: "Window",
        func,
        spec: {
            partitionBy: spec.partitionBy,
            orderBy: spec.orderBy.map(o => ({
                type: "SortKey",
                input: o.input,
                direction: o.direction,
                nulls: o.nulls
            })),
            frame: spec.frame && {...spec.frame}
        }
    }),
    isNull: (input) => ({
        type: "UnresolvedFunction",
        name: "isnull",
        args: [input]
    }),

    isNotNull: (input) => ({
        type: "UnresolvedFunction",
        name: "isnotnull",
        args: [input]
    }),

    coalesce: (args) => ({
        type: "UnresolvedFunction",
        name: "coalesce",
        args
    })
};

export const SparkDFAlg: DFAlg<LogicalPlan, Expression, GroupBy> = {
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
        joinType: (joinType.toUpperCase() as JoinTypeInput),
    }),

    groupBy: (plan, cols) => ({ type: "GroupBy", input: plan, expressions: cols }),

    agg: (group, aggregations, groupType?: GroupTypeInput) => ({
        type: "Aggregate",
        input: group,
        aggregations,
        groupType
    }),

    orderBy: (plan, orders: SortOrder<Expression>[]) => ({
        type: "Sort",
        input: plan,
        orders: orders.map(o =>
            o.expr.type === "SortKey"
                ? { expr: o.expr.input, direction: o.expr.direction, nulls: o.expr.nulls }
                : { expr: o.expr, direction: "asc" as const }
        ),
    }),

    sort: (plan, orders) => ({
        type: "Sort",
        input: plan,
        orders: orders.map(o =>
            o.expr.type === "SortKey"
                ? { expr: o.expr.input, direction: o.expr.direction, nulls: o.expr.nulls }
                : { expr: o.expr, direction: "asc" as const }
        ),
    }),

    limit: (plan, n) => ({ type: "Limit", input: plan, limit: n }),

    distinct: (plan) => ({ type: "Distinct", input: plan }),

    dropDuplicates: (plan, cols) => {
        if (!cols || cols.length === 0) return { type: "Distinct", input: plan };
        const gb: GroupBy = { type: "GroupBy", input: plan, expressions: cols };
        return { type: "Aggregate", input: gb, aggregations: {} };
    },

    union: (left, right, opts) => ({
        type: "Union",
        inputs: [left, right],
        byName: !!opts?.byName,
        allowMissingColumns: !!opts?.allowMissingColumns,
    }),

    withColumnRenamed: (plan, oldName, newName) => ({
        type: "Project",
        input: plan,
        columns: [
            { type: "UnresolvedStar" },
            { type: "Alias", input: { type: "Column", name: oldName }, alias: newName }
        ],
    }),

    withColumnsRenamed: (plan, mapping) => ({
        type: "Project",
        input: plan,
        columns: Object.entries(mapping).map(([from, to]) => ({
            type: "Alias",
            input: { type: "Column", name: from },
            alias: to,
        })),
    }),
};
