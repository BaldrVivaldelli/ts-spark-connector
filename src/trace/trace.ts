export type TraceNode = {
    id: string;
    label: string;
    children: TraceNode[];
};

let _id = 0;
const nid = (pfx: string) => `${pfx}_${++_id}`;

export const TraceExprAlg = {
    col: (name: string): TraceNode => ({ id: nid("col"), label: `col(${name})`, children: [] }),
    lit: (v: any): TraceNode => ({ id: nid("lit"), label: `lit(${JSON.stringify(v)})`, children: [] }),
    bin: (op: string, l: TraceNode, r: TraceNode): TraceNode =>
        ({ id: nid("bin"), label: op, children: [l, r] }),
    call: (fn: string, args: TraceNode[]): TraceNode =>
        ({ id: nid("call"), label: fn, children: args }),
    alias: (e: TraceNode, name: string): TraceNode =>
        ({ id: nid("alias"), label: `alias(${name})`, children: [e] }),
    caseWhen: (branches: { when: TraceNode; then: TraceNode }[], elseE: TraceNode): TraceNode =>
        ({ id: nid("case"), label: "case_when", children: [...branches.flatMap(b => [b.when, b.then]), elseE] }),
    coalesce: (exprs: TraceNode[]): TraceNode =>
        ({ id: nid("coalesce"), label: "coalesce", children: exprs }),
} as const;

export const TraceDFAlg = {
    relation: (format: string, path: string | string[], opts?: Record<string,string>): TraceNode =>
        ({ id: nid("rel"), label: `relation(${format})`, children: [] }),

    select: (df: TraceNode, cols: TraceNode[]): TraceNode =>
        ({ id: nid("select"), label: `select[${cols.length}]`, children: [df, ...cols] }),

    filter: (df: TraceNode, cond: TraceNode): TraceNode =>
        ({ id: nid("filter"), label: "filter", children: [df, cond] }),

    withColumn: (df: TraceNode, name: string, e: TraceNode): TraceNode =>
        ({ id: nid("withCol"), label: `withColumn(${name})`, children: [df, e] }),

    join: (l: TraceNode, r: TraceNode, on: TraceNode, jt: string): TraceNode =>
        ({ id: nid("join"), label: `join(${jt})`, children: [l, r, on] }),

    groupBy: (df: TraceNode, keys: TraceNode[]): TraceNode =>
        ({ id: nid("groupBy"), label: `groupBy[${keys.length}]`, children: [df, ...keys] }),

    agg: (dfOrG: TraceNode, pairs: Record<string, TraceNode>): TraceNode =>
        ({ id: nid("agg"), label: `agg[${Object.keys(pairs).length}]`, children: [dfOrG, ...Object.entries(pairs).map(([k,v]) =>
                ({ id: nid("alias"), label: `as(${k})`, children: [v] }))]}),

    orderBy: (df: TraceNode, orders: {expr: TraceNode; direction: "asc" | "desc"}[]): TraceNode =>
        ({ id: nid("orderBy"), label: `orderBy[${orders.length}]`, children: [df, ...orders.map(o => o.expr)] }),

    sort: (df: TraceNode, orders: {expr: TraceNode; direction: "asc" | "desc"}[]): TraceNode =>
        ({ id: nid("sort"), label: `sort[${orders.length}]`, children: [df, ...orders.map(o => o.expr)] }),

    limit: (df: TraceNode, n: number): TraceNode =>
        ({ id: nid("limit"), label: `limit(${n})`, children: [df] }),

    distinct: (df: TraceNode): TraceNode =>
        ({ id: nid("distinct"), label: "distinct", children: [df] }),

    dropDuplicates: (df: TraceNode, exprs?: TraceNode[]): TraceNode =>
        ({ id: nid("dropDup"), label: `dropDuplicates[${exprs?.length ?? 0}]`, children: [df, ...(exprs ?? [])] }),

    union: (l: TraceNode, r: TraceNode, _opts?: any): TraceNode =>
        ({ id: nid("union"), label: "union", children: [l, r] }),

    withColumnRenamed: (df: TraceNode, oldN: string, newN: string): TraceNode =>
        ({ id: nid("rename"), label: `rename(${oldN}→${newN})`, children: [df] }),

    withColumnsRenamed: (df: TraceNode, _m: Record<string,string>): TraceNode =>
        ({ id: nid("renameMany"), label: "renameMany", children: [df] }),

    repartition: (df: TraceNode, n: number, _shuf: boolean): TraceNode =>
        ({ id: nid("repartition"), label: `repartition(${n})`, children: [df] }),

    coalesce: (df: TraceNode, n: number): TraceNode =>
        ({ id: nid("coalesce"), label: `coalesce(${n})`, children: [df] }),

    cache: (df: TraceNode): TraceNode =>
        ({ id: nid("cache"), label: "cache", children: [df] }),

    persist: (df: TraceNode, level: string): TraceNode =>
        ({ id: nid("persist"), label: `persist(${level})`, children: [df] }),

    unpersist: (df: TraceNode, _blocking?: boolean): TraceNode =>
        ({ id: nid("unpersist"), label: "unpersist", children: [df] }),

    sql: (_: string): TraceNode =>
        ({ id: nid("sql"), label: "sql(query)", children: [] }),
} as const;
