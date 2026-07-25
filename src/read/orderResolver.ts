import type { ExprAlg } from "../algebra/read";
import type { ColumnType, Schema } from "../schema/schema-model";
import { SortKey, TypedColumn, makeColumns } from "../typed/typed-column";
import type { SortOrder } from "../types";

type ExpressionBuilder = { build<E>(algebra: ExprAlg<E>): E };

export function resolveOrderInput<E>(input: unknown, algebra: ExprAlg<E>): SortOrder<E> {
    if (typeof input === "string") {
        return { expr: algebra.col(input), direction: "asc" };
    }
    if (typeof input !== "function") {
        return { expr: (input as ExpressionBuilder).build(algebra), direction: "asc" };
    }

    // Typed order callbacks and legacy SortKeyBuilder are both functions. Try
    // the typed accessor first; legacy builders require ExprAlg and therefore
    // fall back to the existing invocation below.
    try {
        const typed = (input as (
            columns: ReturnType<typeof makeColumns<Schema, E>>
        ) => SortKey<E> | TypedColumn<ColumnType, E>)(makeColumns<Schema, E>());
        if (typed instanceof SortKey) return typed.toSortOrder(algebra);
        if (typed instanceof TypedColumn) {
            return { expr: typed.build(algebra), direction: "asc" };
        }
    } catch {
        // Legacy builder: evaluate it with the real expression algebra.
    }
    return (input as (exprAlgebra: ExprAlg<E>) => SortOrder<E>)(algebra);
}
