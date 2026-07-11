/**
 * Agregaciones tipadas para `groupBy().agg()`, **agnósticas del intérprete**.
 *
 * Promueve y reescribe el prototipo experimental
 * (`src/experimental/aggregations.ts`), que construía proto directamente vía
 * `ProtoExprAlg`, a builders **parametrizados por el álgebra `E`**: cada
 * agregación guarda un *thunk* `(EX: ExprAlg<E>) => E` y solo lo evalúa cuando
 * se la interpreta, conservando la parametricidad tagless-final
 * (Requirement 3.2) y la equivalencia de plan con el camino laxo, igual que los
 * builders de columna de `./typed-column`.
 *
 * Cada agregación lleva, en el tipo, **tanto** el alias de salida como el tipo
 * de valor de salida, de modo que el schema resultante de una agregación puede
 * computarse de forma estática (Requirement 8.5):
 *
 *   count().as("n")            -> { n: number }
 *   sum("amount").as("total")  -> { total: number | bigint | null }
 *
 * Alineación con `schema-transforms` (Requirement 8.5, task 2.2): la clase
 * `Aggregation` aquí definida `implements` la interfaz estructural
 * `Aggregation<Alias, Out>` de `src/schema/schema-transforms.ts` (importada como
 * `AggregationShape`). Esa interfaz solo exige un phantom
 * `readonly __out: { [P in Alias]: Out }`, que es justamente lo que `Aggregated<S, K, A>`
 * consume vía `A[number]["__out"]`. Al implementarla explícitamente garantizamos
 * que ambas definiciones permanezcan consistentes y que la salida de estos
 * builders sea asignable a lo que espera la inferencia de agregación.
 */

import { ExprAlg } from "../algebra/read";
import {
    ColumnType,
    NonNull,
    SchemaShape,
    UsableColumnKey,
    UsableColumnType,
} from "../schema/schema-model";
import type { Aggregation as AggregationShape } from "../schema/schema-transforms";
import { Columns, makeColumns, NumericValue } from "./typed-column";

/** Un *thunk* que construye la expresión `E` del intérprete a partir del álgebra. */
type ExprThunk<E> = (EX: ExprAlg<E>) => E;

// ---------------------------------------------------------------------------
// Aggregation<Alias, T, E>
// ---------------------------------------------------------------------------

/**
 * Una agregación con alias de salida `Alias` conocido y tipo de salida `T`. El
 * phantom `__out` carga `{ [Alias]: T }` para que el schema resultante de
 * `groupBy().agg()` pueda inferirse (lo consume `Aggregated` vía
 * `A[number]["__out"]`).
 *
 * Implementa la forma estructural mínima `AggregationShape<Alias, T>` de
 * `schema-transforms`, de modo que esta clase concreta y la interfaz que usa la
 * inferencia de schema no puedan divergir.
 */
export class Aggregation<Alias extends string, T extends ColumnType, E>
    implements AggregationShape<Alias, T>
{
    /** @internal phantom: nunca se lee en runtime, carga la forma de salida. */
    declare readonly __out: { [P in Alias]: T };

    /**
     * @internal Construye una agregación nombrada a partir de su alias y del
     * thunk del álgebra. No suele instanciarse directamente: se obtiene de
     * `PendingAggregation.as(...)`.
     */
    constructor(
        readonly alias: Alias,
        private readonly run: ExprThunk<E>
    ) {}

    /** Produce la expresión del intérprete (mismo contrato que `EBuilder.build`). */
    build(EX: ExprAlg<E>): E {
        return this.run(EX);
    }
}

// ---------------------------------------------------------------------------
// PendingAggregation<T, E>
// ---------------------------------------------------------------------------

/**
 * Una agregación cuyo alias todavía no se eligió. Se materializa nombrándola con
 * `.as(alias)`, que produce la `Aggregation<Alias, T, E>` final.
 */
export class PendingAggregation<T extends ColumnType, E> {
    /** @internal */
    constructor(private readonly run: ExprThunk<E>) {}

    /** Nombra la columna de salida de la agregación. */
    as<const Alias extends string>(
        alias: Alias extends "" ? never : Alias,
    ): Aggregation<Alias, T, E> {
        if (typeof alias !== "string" || !alias.trim()) {
            throw new TypeError("Aggregation alias must be a non-empty string.");
        }
        return new Aggregation<Alias, T, E>(alias, this.run);
    }
}

// ---------------------------------------------------------------------------
// Referencia de columna y resolución a `E`
// ---------------------------------------------------------------------------

/**
 * Referencia a una columna a agregar: o bien un nombre de columna del schema
 * (validado contra `keyof S`), o bien una `TypedColumn<T, E>` ya construida
 * (p. ej. la que produce el accesor `Columns<S, E>` del camino tipado).
 */
type BuildableColumn<E> = { build(EX: ExprAlg<E>): E };

type NumericColumnKey<S extends SchemaShape> = {
    [K in UsableColumnKey<S>]: NonNull<UsableColumnType<S, K>> extends NumericValue
        ? K
        : never;
}[UsableColumnKey<S>];

type NumericResult<S extends SchemaShape, K extends NumericColumnKey<S>> = Extract<
    NonNull<UsableColumnType<S, K>>,
    NumericValue
>;

type SumResult<S extends SchemaShape, K extends NumericColumnKey<S>> =
    NumericResult<S, K> extends bigint ? bigint : number | bigint;

type ColRef<
    S extends SchemaShape,
    E,
    K extends UsableColumnKey<S> = UsableColumnKey<S>,
> = K | BuildableColumn<E> | ((columns: Columns<S, E>) => BuildableColumn<E>);

/** Resuelve una `ColRef` a la expresión `E` del intérprete, difiriendo al álgebra. */
function resolve<S extends SchemaShape, E>(ref: ColRef<S, E>, EX: ExprAlg<E>): E {
    if (typeof ref === "function") {
        return ref(makeColumns<S, E>()).build(EX);
    }
    return typeof ref === "string" ? EX.col(ref) : ref.build(EX);
}

// ---------------------------------------------------------------------------
// Factory de agregaciones
// ---------------------------------------------------------------------------

/**
 * Factory de agregaciones ligado a un schema `S` y a un álgebra `E`. Es lo que
 * recibirá el callback de `groupBy(...).agg(a => [...])` (task 5.4): valida las
 * referencias de columna contra `S` y produce `PendingAggregation` que se
 * nombran con `.as(...)`.
 *
 * Nullabilidad de salida (Requirement 8.5):
 * - `count` y `countDistinct` son BIGINT no-null: nunca devuelven null.
 *   sobre un grupo vacío (devuelve 0).
 * - `sum`, `avg`, `min`, `max` son nullables: pueden ser null sobre grupos
 *   vacíos o de entradas todo-null.
 *
 * COUNT/COUNT DISTINCT se modelan como `bigint`, acorde al BIGINT de Spark. Se
 * usa la función canónica `count_distinct`, no `count(col)`.
 */
export type AggFactory<S extends SchemaShape, E> = {
    count(): PendingAggregation<bigint, E>;
    countDistinct(ref: ColRef<S, E>): PendingAggregation<bigint, E>;
    sum<K extends NumericColumnKey<S>>(ref: ColRef<S, E, K>): PendingAggregation<SumResult<S, K> | null, E>;
    avg(ref: ColRef<S, E, NumericColumnKey<S>>): PendingAggregation<number | null, E>;
    min<K extends NumericColumnKey<S>>(ref: ColRef<S, E, K>): PendingAggregation<NumericResult<S, K> | null, E>;
    max<K extends NumericColumnKey<S>>(ref: ColRef<S, E, K>): PendingAggregation<NumericResult<S, K> | null, E>;
};

/**
 * Crea un `AggFactory<S, E>`. Las agregaciones nullables comparten la misma
 * forma (una función agregada de un solo argumento), por lo que se factoriza en
 * `nullableFn`.
 */
export function makeAggFactory<S extends SchemaShape, E>(): AggFactory<S, E> {
    const nullableNumberFn = (
        name: string,
        ref: ColRef<S, E, NumericColumnKey<S>>
    ): PendingAggregation<number | null, E> =>
        new PendingAggregation<number | null, E>(EX => EX.call(name, [resolve(ref, EX)]));

    const preservingNumericFn = <K extends NumericColumnKey<S>>(
        name: string,
        ref: ColRef<S, E, K>,
    ): PendingAggregation<NumericResult<S, K> | null, E> =>
        new PendingAggregation<NumericResult<S, K> | null, E>(EX => EX.call(name, [resolve(ref, EX)]));

    const sumFn = <K extends NumericColumnKey<S>>(
        ref: ColRef<S, E, K>,
    ): PendingAggregation<SumResult<S, K> | null, E> =>
        new PendingAggregation<SumResult<S, K> | null, E>(EX => EX.call("sum", [resolve(ref, EX)]));

    return {
        // Spark COUNT devuelve BIGINT y nunca null, incluso para cero filas.
        count: () => new PendingAggregation<bigint, E>(EX => EX.call("count", [EX.lit(1)])),
        countDistinct: ref =>
            new PendingAggregation<bigint, E>(EX =>
                EX.call("count_distinct", [resolve(ref, EX)])
            ),
        // Agregaciones nullables sobre grupos vacíos / todo-null.
        // INT widens to BIGINT, while FLOAT/DOUBLE remain JS number. Since the
        // public input schema collapses those families to `number`, their sound
        // SUM result must include both runtime representations.
        sum: ref => sumFn(ref),
        avg: ref => nullableNumberFn("avg", ref),
        min: ref => preservingNumericFn("min", ref),
        max: ref => preservingNumericFn("max", ref),
    };
}
