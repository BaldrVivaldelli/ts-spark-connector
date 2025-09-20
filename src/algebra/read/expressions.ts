import { NullsOrder, SortDirection, WindowSpec } from "../../types";


export interface ExprAlg<E> {
    col(name: string): E;
    lit(v: string | number | boolean): E;
    bin(op: string, left: E, right: E): E;
    logical(op: "AND" | "OR", left: E, right: E): E;
    alias(input: E, ...names: string[]): E;
    call(name: string, args: E[]): E;
    sortKey(input: E, direction: SortDirection, nulls?: NullsOrder): E;
    star(): E;
    caseWhen(branches: Array<{ when: E; then: E }>, elze?: E): E;
    win(func: E, spec: WindowSpec<E>): E;
    isNull(input: E): E;
    isNotNull(input: E): E;
    coalesce(args: E[]): E;
    explode(input: E): E;
    posexplode(input: E): E;
    getField(input: E, field: string): E;
    map_keys(input: E): E;
    map_values(input: E): E;
    elementAt(map: E, key: E): E;
    getItem(collectionExpr: E, key: E | string | number): E;
    split(input: E, delimiter: E | string): E;
    from_json(jsonExpr: E, schema: string): E;
    to_json(expr: E): E;
}