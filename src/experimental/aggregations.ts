/**
 * EXPERIMENTAL — typed aggregation builders for groupBy().agg().
 *
 * Each builder carries, in the type, both the output alias name and the output
 * value type, so the result schema of an aggregation can be computed statically:
 *
 *   count("id").as("n")        -> { n: number }
 *   sum(c => c.amount).as("t") -> { t: number }
 *
 * At runtime it produces the same proto expression the untyped API would, via
 * `ProtoExprAlg`.
 */

import { ProtoExprAlg } from "../engine/compilerRead";
import type { Columns, ColumnType, Schema, TypedColumn } from "./typed-dataframe";

type ProtoExpr = unknown;

/**
 * An aggregation with a known output alias `Name` and output type `T`. The
 * `__out` phantom carries `{ [Name]: T }` so the result schema can be inferred.
 */
export class Aggregation<Name extends string, T extends ColumnType> {
    /** @internal phantom: never read at runtime, carries the output shape. */
    declare readonly __out: { [P in Name]: T };

    /** @internal */ constructor(
        readonly alias: Name,
        readonly expr: ProtoExpr
    ) {}
}

/** An aggregation whose alias has not been chosen yet. */
class PendingAggregation<T extends ColumnType> {
    /** @internal */ constructor(private readonly expr: ProtoExpr) {}

    /** Names the aggregation output column. */
    as<Name extends string>(name: Name): Aggregation<Name, T> {
        return new Aggregation<Name, T>(name, this.expr);
    }
}

// A column selector accepts either a column name (string) or a typed-column
// accessor function, mirroring the ergonomics of filter/withColumn.
type ColRef<S extends Schema> = (keyof S & string) | ((c: Columns<S>) => TypedColumn<ColumnType>);

function resolve<S extends Schema>(ref: ColRef<S>, cols: Columns<S>): ProtoExpr {
    return typeof ref === "function"
        ? (ref(cols) as TypedColumn<ColumnType>).expr
        : ProtoExprAlg.col(ref);
}

/**
 * Aggregation factory bound to a schema `S`. `groupBy` provides the column
 * accessor so the helpers can validate column references against the schema.
 *
 * @internal — used by GroupedData; not part of the public surface.
 */
export type AggFactory<S extends Schema> = {
    count(): PendingAggregation<number>;
    countDistinct(ref: ColRef<S>): PendingAggregation<number>;
    sum(ref: ColRef<S>): PendingAggregation<number | null>;
    avg(ref: ColRef<S>): PendingAggregation<number | null>;
    min(ref: ColRef<S>): PendingAggregation<number | null>;
    max(ref: ColRef<S>): PendingAggregation<number | null>;
};

export function makeAggFactory<S extends Schema>(cols: Columns<S>): AggFactory<S> {
    const nullableFn = <R extends ColumnType>(name: string, ref: ColRef<S>) =>
        new PendingAggregation<R>(ProtoExprAlg.call(name, [resolve(ref, cols)] as never[]));

    return {
        // count(*) is never null.
        count: () => new PendingAggregation<number>(
            ProtoExprAlg.call("count", [ProtoExprAlg.lit(1)] as never[])
        ),
        countDistinct: (ref) => new PendingAggregation<number>(
            ProtoExprAlg.call("count", [resolve(ref, cols)] as never[])
        ),
        // sum/avg/min/max can be null over empty groups or all-null inputs.
        sum: (ref) => nullableFn<number | null>("sum", ref),
        avg: (ref) => nullableFn<number | null>("avg", ref),
        min: (ref) => nullableFn<number | null>("min", ref),
        max: (ref) => nullableFn<number | null>("max", ref),
    };
}

export { PendingAggregation };
