import { DFAlg } from "../algebra/read/dataframe";
import { ExprAlg, LiteralValue } from "../algebra/read/expressions";
import { EventTimeWatermarkCap, StreamingReadCap } from "../algebra/read/streaming-capabilities";
import { GroupTypeInput, JoinHintName, JoinTypeInput } from "../engine/sparkConnectEnums";
import { FrameBoundary, NullsOrder, SortDirection, SortOrder, WindowSpec } from "../types";

export type TraceNode = {
    id: string;
    label: string;
    children: TraceNode[];
};

let _id = 0;
const nid = (pfx: string) => `${pfx}_${++_id}`;
const node = (pfx: string, label: string, children: TraceNode[] = []): TraceNode =>
    ({ id: nid(pfx), label, children });

const boundaryLabel = (boundary: FrameBoundary): string =>
    "value" in boundary ? `${boundary.type}(${boundary.value})` : boundary.type;

const sortLabel = (direction: SortDirection, nulls?: NullsOrder): string =>
    nulls ? `${direction},${nulls}` : direction;

/**
 * Expression interpreter that builds the client-side AST shown by
 * `toClientASTJSON()` / `toClientASTMermaid()`.
 *
 * It is typed against `ExprAlg` on purpose: every builder the public column
 * helpers can emit must have a trace counterpart, otherwise plan inspection
 * throws at runtime for expressions that execute fine against Spark.
 */
export const TraceExprAlg: ExprAlg<TraceNode> = {
    col: (name, planId) => node("col", `col(${name}${planId === undefined ? "" : `@${planId}`})`),
    lit: (v: LiteralValue) => node("lit", `lit(${typeof v === "bigint" ? `${v}n` : JSON.stringify(v)})`),
    bin: (op, l, r) => node("bin", op, [l, r]),
    logical: (op, l, r) => node("logical", op, [l, r]),
    alias: (input, ...names) => node("alias", `alias(${names.join(",")})`, [input]),
    call: (fn, args) => node("call", fn, args),
    sortKey: (input, direction, nulls) => node("sortKey", `sortKey(${sortLabel(direction, nulls)})`, [input]),
    star: () => node("star", "*"),
    caseWhen: (branches, elze) =>
        node("case", "case_when", [
            ...branches.flatMap(branch => [branch.when, branch.then]),
            ...(elze === undefined ? [] : [elze]),
        ]),
    win: (func, spec: WindowSpec<TraceNode>) =>
        node(
            "win",
            `window(partitionBy[${spec.partitionBy.length}],orderBy[${spec.orderBy.length}]${
                spec.frame
                    ? `,${spec.frame.type}[${boundaryLabel(spec.frame.start)}..${boundaryLabel(spec.frame.end)}]`
                    : ""
            })`,
            [func, ...spec.partitionBy, ...spec.orderBy.map(order => order.input)],
        ),
    isNull: input => node("isNull", "isNull", [input]),
    isNotNull: input => node("isNotNull", "isNotNull", [input]),
    coalesce: exprs => node("coalesce", "coalesce", exprs),
    explode: input => node("explode", "explode", [input]),
    posexplode: input => node("posexplode", "posexplode", [input]),
    getField: (input, field) => node("getField", `getField(${field})`, [input]),
    map_keys: input => node("mapKeys", "map_keys", [input]),
    map_values: input => node("mapValues", "map_values", [input]),
    elementAt: (map, key) => node("elementAt", "element_at", [map, key]),
    getItem: (collectionExpr, key) =>
        typeof key === "object"
            ? node("getItem", "getItem", [collectionExpr, key])
            : node("getItem", `getItem(${JSON.stringify(key)})`, [collectionExpr]),
    split: (input, delimiter) =>
        typeof delimiter === "string"
            ? node("split", `split(${JSON.stringify(delimiter)})`, [input])
            : node("split", "split", [input, delimiter]),
    from_json: (jsonExpr, schema) => node("fromJson", `from_json(${schema})`, [jsonExpr]),
    to_json: expr => node("toJson", "to_json", [expr]),
};

type TraceDFAlgebra = DFAlg<
    TraceNode,
    TraceNode,
    TraceNode,
    StreamingReadCap<TraceNode> & EventTimeWatermarkCap<TraceNode, TraceNode>
>;

const orderLabels = (orders: SortOrder<TraceNode>[]): TraceNode[] => orders.map(order => order.expr);

/**
 * DataFrame interpreter for the client-side AST. Typed against the full read
 * algebra (batch capabilities plus the streaming read capabilities) so a new
 * relation added to `DFAlg` cannot silently break plan inspection.
 */
export const TraceDFAlg: TraceDFAlgebra = {
    relation: (format, _path, _opts, schema) =>
        node("rel", `relation(${format}${schema ? ",schema" : ""})`),

    catalog: op => node("rel", `catalog(${op.method})`),

    localRelation: (_ddl, _data, rowCount) => node("rel", `localRelation[${rowCount}]`),

    withPlanId: (df, _planId) => df,

    select: (df, cols) => node("select", `select[${cols.length}]`, [df, ...cols]),

    filter: (df, cond) => node("filter", "filter", [df, cond]),

    withColumn: (df, name, e) => node("withCol", `withColumn(${name})`, [df, e]),

    join: (l, r, on, jt?: JoinTypeInput) => node("join", `join(${jt ?? "INNER"})`, [l, r, on]),

    groupBy: (df, keys) => node("groupBy", `groupBy[${keys.length}]`, [df, ...keys]),

    agg: (dfOrG, pairs, groupType?: GroupTypeInput) =>
        node("agg", `agg[${Object.keys(pairs).length}]${groupType ? `(${groupType})` : ""}`, [
            dfOrG,
            ...Object.entries(pairs).map(([k, v]) => node("alias", `as(${k})`, [v])),
        ]),

    orderBy: (df, orders) => node("orderBy", `orderBy[${orders.length}]`, [df, ...orderLabels(orders)]),

    sort: (df, orders) => node("sort", `sort[${orders.length}]`, [df, ...orderLabels(orders)]),

    limit: (df, n) => node("limit", `limit(${n})`, [df]),

    distinct: df => node("distinct", "distinct", [df]),

    dropDuplicates: (df, exprs) =>
        node("dropDup", `dropDuplicates[${exprs?.length ?? 0}]`, [df, ...(exprs ?? [])]),

    union: (l, r, opts) =>
        node("union", opts?.byName ? `unionByName${opts.allowMissingColumns ? "(allowMissing)" : ""}` : "union", [l, r]),

    intersect: (l, r, opts) => node("intersect", opts?.all ? "intersectAll" : "intersect", [l, r]),

    except: (l, r, opts) => node("except", opts?.all ? "exceptAll" : "except", [l, r]),

    withColumnRenamed: (df, oldN, newN) => node("rename", `rename(${oldN}→${newN})`, [df]),

    withColumnsRenamed: (df, mapping) =>
        node("renameMany", `renameMany(${Object.entries(mapping).map(([from, to]) => `${from}→${to}`).join(",")})`, [df]),

    drop: (df, columnNames) => node("drop", `drop(${columnNames.join(",")})`, [df]),

    repartition: (df, n, shuffle) => node("repartition", `repartition(${n}${shuffle ? ",shuffle" : ""})`, [df]),

    coalesce: (df, n) => node("coalesce", `coalesce(${n})`, [df]),

    cache: df => node("cache", "cache", [df]),

    persist: (df, level) => node("persist", `persist(${level ?? "MEMORY_AND_DISK"})`, [df]),

    unpersist: (df, _blocking) => node("unpersist", "unpersist", [df]),

    sql: _query => node("sql", "sql(query)"),

    hint: (df, name: JoinHintName | string, params) =>
        node("hint", `hint(${name}${params && params.length > 0 ? `,${params.length} params` : ""})`, [df]),

    sample: (df, lowerBound, upperBound, withReplacement, seed, deterministicOrder) =>
        node(
            "sample",
            `sample(${lowerBound},${upperBound}${withReplacement ? ",withReplacement" : ""}${
                seed === undefined ? "" : `,seed=${seed}`
            }${deterministicOrder ? ",deterministic" : ""})`,
            [df],
        ),

    describe: (df, columns) => node("describe", `describe[${columns.length}]`, [df, ...columns]),

    summary: (df, metrics, columns) =>
        node("summary", `summary[${metrics.length}x${columns.length}]`, [df, ...metrics, ...columns]),

    readStream: (format, options) =>
        node("readStream", `readStream(${format}${options ? ",opts" : ""})`),

    withWatermark: (df, eventTimeCol, delay) =>
        node("watermark", `withWatermark(${delay})`, [df, eventTimeCol]),
};
