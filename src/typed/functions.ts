/**
 * Funciones escalares tipadas + `caseWhen` (`when().otherwise()`),
 * **agnósticas del intérprete**.
 *
 * Promueven y reescriben las funciones del prototipo experimental
 * (`src/experimental/functions.ts`), que construían proto directamente vía
 * `ProtoExprAlg`, para que ahora construyan su expresión `E` a través de
 * `ExprAlg<E>` (Requirement 8.7, 3.2). Cada función declara el/los tipo(s) de
 * columna de entrada y el tipo de salida, de modo que el tipo (y su
 * nullabilidad) fluye por `withColumn`/`select` igual que en el prototipo
 * (Requirement 8.4).
 *
 * Como los builders de `./typed-column`, estas funciones no tocan proto: cada
 * una devuelve un `TypedColumn`/`NumericColumn` que guarda un *thunk*
 * `(EX: ExprAlg<E>) => E` y solo lo evalúa al interpretarse, conservando la
 * parametricidad tagless-final y la equivalencia de plan con el camino laxo
 * (mismas llamadas que `call(...)` de `src/engine/column.ts`).
 *
 * Alcance (Requirement 8.4/8.6): `length`, `upper`, `lower`, `concat`, `abs`,
 * `round` y `caseWhen`. Las funciones fuera de este set (ventanas, long-tail de
 * Spark SQL) degradan al camino laxo (`EBuilder`).
 */

import { ExprAlg } from "../algebra/read";
import { ColumnType, ScalarType } from "../schema/schema-model";
import { Condition, NumericColumn, NumericValue, TypedColumn } from "./typed-column";

// ---------------------------------------------------------------------------
// Funciones de string (string -> ...)
// ---------------------------------------------------------------------------

/**
 * `length(col)` — longitud de una columna de string como columna numérica. El
 * resultado se modela como nullable (`length(null)` es `null` en Spark), igual
 * que en el prototipo.
 */
export function length<E>(col: TypedColumn<string | null, E>): NumericColumn<number | null, E> {
    return new NumericColumn<number | null, E>(EX => EX.call("length", [col.build(EX)]));
}

/**
 * `upper(col)` — pasa a mayúsculas una columna de string. La nullabilidad de la
 * entrada se **preserva** en la salida (el parámetro `T` viaja sin cambios).
 */
export function upper<T extends string | null, E>(col: TypedColumn<T, E>): TypedColumn<T, E> {
    return new TypedColumn<T, E>(EX => EX.call("upper", [col.build(EX)]));
}

/**
 * `lower(col)` — pasa a minúsculas una columna de string, preservando la
 * nullabilidad de la entrada.
 */
export function lower<T extends string | null, E>(col: TypedColumn<T, E>): TypedColumn<T, E> {
    return new TypedColumn<T, E>(EX => EX.call("lower", [col.build(EX)]));
}

/**
 * `concat(a, b, ...)` — concatena columnas de string y/o literales en una
 * columna de string. El resultado se modela como nullable (cualquier parte nula
 * propaga `null`), igual que en el prototipo.
 */
export function concat<E>(
    ...parts: Array<TypedColumn<string | null, E> | string>
): TypedColumn<string | null, E> {
    return new TypedColumn<string | null, E>(EX =>
        EX.call(
            "concat",
            parts.map(p => (typeof p === "string" ? EX.lit(p) : p.build(EX)))
        )
    );
}

// ---------------------------------------------------------------------------
// Funciones numéricas (number -> number)
// ---------------------------------------------------------------------------

/**
 * `abs(col)` — valor absoluto, **preservando la nullabilidad** de la entrada
 * (el parámetro `T` viaja sin cambios).
 */
export function abs<T extends NumericValue | null, E>(col: TypedColumn<T, E>): NumericColumn<T, E> {
    return new NumericColumn<T, E>(EX => EX.call("abs", [col.build(EX)]));
}

/**
 * `round(col, scale)` — redondea una columna numérica, preservando la
 * nullabilidad de la entrada. `scale` es la cantidad de decimales (0 por
 * defecto) y se promueve a literal.
 */
export function round<T extends NumericValue | null, E>(
    col: TypedColumn<T, E>,
    scale = 0
): NumericColumn<T, E> {
    return new NumericColumn<T, E>(EX => EX.call("round", [col.build(EX), EX.lit(scale)]));
}

// ---------------------------------------------------------------------------
// caseWhen — when(...).when(...).otherwise(...)
// ---------------------------------------------------------------------------

/**
 * Ensancha un literal escalar a su tipo base (`"high"` -> `string`, `1` ->
 * `number`), de modo que `when(cond, "high")` produzca una cadena de `string`
 * (no de `"high"`) y las ramas siguientes admitan otros valores del mismo tipo.
 */
type Widen<V> = V extends string
    ? string
    : V extends number
      ? number
      : V extends bigint
        ? bigint
      : V extends boolean
        ? boolean
        : V;

/** Una rama `when(cond) -> value` aún sin interpretar. */
interface Branch<T extends ScalarType, E> {
    readonly cond: Condition<E>;
    readonly value: T | TypedColumn<T, E>;
}

/**
 * `when(cond, value)` tipado. Todas las ramas y el valor de `otherwise` deben
 * compartir el mismo tipo escalar `T`, por lo que la columna resultante es
 * `TypedColumn<T | null, E>` (la rama `else` puede ser nula).
 *
 * `T` se ensancha desde el literal del primer valor a su tipo base (ver
 * `Widen`), igual que en el prototipo.
 */
export function when<V extends ScalarType, E>(
    cond: Condition<E>,
    value: V | TypedColumn<Widen<V>, E>
): CaseChain<Widen<V>, E> {
    return new CaseChain<Widen<V>, E>([{ cond, value: value as Widen<V> | TypedColumn<Widen<V>, E> }]);
}

/**
 * Cadena de ramas `case when` en construcción. Es inmutable: cada `when`
 * agregado devuelve una nueva cadena. Solo al llamar `otherwise` se materializa
 * el `TypedColumn` con su *thunk*.
 */
class CaseChain<T extends ScalarType, E> {
    /** @internal Las cadenas se obtienen de `when(...)`, no se construyen a mano. */
    constructor(private readonly branches: ReadonlyArray<Branch<T, E>>) {}

    /** Agrega otra rama `when(cond) -> value` del mismo tipo `T`. */
    when(cond: Condition<E>, value: T | TypedColumn<T, E>): CaseChain<T, E> {
        return new CaseChain<T, E>([...this.branches, { cond, value }]);
    }

    /**
     * Cierra la cadena con la rama `else`. El resultado es nullable porque la
     * rama `else` puede ser `null`.
     */
    otherwise(value: T | TypedColumn<T, E> | null): TypedColumn<T | null, E> {
        const branches = this.branches;
        return new TypedColumn<T | null, E>(EX =>
            EX.caseWhen(
                branches.map(b => ({ when: b.cond.build(EX), then: valueExpr(b.value, EX) })),
                valueExpr(value, EX)
            )
        );
    }
}

/**
 * Convierte el valor de una rama (o de `otherwise`) en una expresión del
 * intérprete: una columna se interpreta con su thunk; un literal se promueve con
 * `EX.lit`; `null` se emite como literal nulo en la rama `else` (mismo
 * comportamiento que el prototipo).
 */
function valueExpr<T extends ScalarType, E>(
    value: T | TypedColumn<ColumnType, E> | null,
    EX: ExprAlg<E>
): E {
    if (value === null) {
        // Literal nulo de la rama `else`, igual que el prototipo experimental.
        return EX.lit(null as never);
    }
    return value instanceof TypedColumn ? value.build(EX) : EX.lit(value);
}
