/**
 * Builders de columna tipados, **agnósticos del intérprete**.
 *
 * Reemplazan a las columnas tipadas del prototipo experimental
 * (`src/experimental/typed-dataframe.ts`), que construían proto directamente,
 * por builders **parametrizados por el álgebra `E`**. Cada builder lleva el tipo
 * de valor `T` en el tipo (igual que el prototipo) **y** construye su expresión
 * `E` a través de `ExprAlg<E>` (a diferencia del prototipo), de modo que
 * funcionan con cualquier intérprete (`SparkDFAlg`, `ProtoExprAlg`,
 * `TraceExprAlg`, …) — exactamente el mismo patrón que el `EBuilder` no tipado
 * de `src/engine/column.ts` (`build<E>(EX): E`).
 *
 * Esta es la diferencia clave con el prototipo: aquí **no** se toca proto. La
 * columna guarda un *thunk* `(EX: ExprAlg<E>) => E` y solo lo evalúa cuando se
 * la interpreta, conservando así la parametricidad tagless-final
 * (Requirement 3.2) y garantizando la equivalencia de plan con el camino laxo.
 *
 * Decisión de implementación: se modelan como **clases** (no interfaces puras)
 * porque (a) `NumericColumn` extiende `TypedColumn`, (b) se necesitan chequeos
 * `instanceof TypedColumn` para distinguir un argumento columna de un literal, y
 * (c) el accesor `Columns<S, E>` las instancia vía `Proxy`. La forma pública de
 * estas clases coincide con las interfaces descritas en el diseño
 * (Componente 3).
 */

import { ExprAlg } from "../algebra/read";
import {
    ColumnType,
    NonNull,
    ScalarType,
    SchemaShape,
    UsableColumnKey,
    UsableColumnType,
} from "../schema/schema-model";
import { NullsOrder, SortDirection, SortOrder } from "../types";

/** Un *thunk* que construye la expresión `E` del intérprete a partir del álgebra. */
type ExprThunk<E> = (EX: ExprAlg<E>) => E;

/**
 * Accesor de columnas de un schema conocido. Las marcas de ambigüedad creadas
 * por un join se excluyen del mapped type: existen en el schema resultante para
 * informar el conflicto, pero no se pueden usar como si fueran columnas
 * resueltas. Las columnas numéricas exponen además las operaciones aritméticas.
 */
export type Columns<S extends SchemaShape, E> = {
    readonly [K in UsableColumnKey<S>]: [NonNull<UsableColumnType<S, K>>] extends [NumericValue]
        ? NumericColumn<UsableColumnType<S, K> & (NumericValue | null), E>
        : TypedColumn<UsableColumnType<S, K>, E>;
};

export type NumericValue = number | bigint;
type NumericBase<T> = Extract<NonNull<T>, NumericValue>;

type NumericOperand<E> = NumericValue | TypedColumn<NumericValue | null, E>;

type NumericOperandValue<O> = O extends TypedColumn<infer V, infer _E>
    ? Extract<NonNull<V>, NumericValue>
    : O extends bigint
      ? bigint
      : O extends number
        ? number extends O
            ? number | bigint
            : `${O}` extends `${bigint}`
              ? bigint
              : number
        : never;

type PromotedNumeric<L, O> = number extends NumericBase<L>
    ? number
    : NumericOperandValue<O> extends bigint
      ? bigint
      : NumericOperandValue<O> extends number
        ? number
        : number | bigint;

type Coalesced<T extends ColumnType, U extends ColumnType> = null extends T
    ? null extends U
        ? NonNull<T> | null
        : NonNull<T>
    : NonNull<T>;

/**
 * Crea perezosamente el accesor tipado. En runtime se instancia siempre la
 * variante numérica, que es un superset de `TypedColumn`; el mapped type es el
 * que oculta aritmética para columnas no numéricas.
 */
export function makeColumns<S extends SchemaShape, E>(planId?: number): Columns<S, E> {
    return new Proxy(
        {},
        {
            get: (_target, prop) =>
                new NumericColumn<NumericValue | null, E>(EX => EX.col(String(prop), planId)),
        }
    ) as Columns<S, E>;
}

// ---------------------------------------------------------------------------
// Condition<E>
// ---------------------------------------------------------------------------

/**
 * Una expresión booleana usable en `filter`, componible con `and`/`or`. Como el
 * resto de builders, no construye nada hasta que se la interpreta con un
 * `ExprAlg<E>` concreto.
 */
export class Condition<E> {
    /**
     * @internal Construye una condición a partir de un thunk del álgebra. No es
     * parte de la API pública: las condiciones se obtienen de las comparaciones
     * de `TypedColumn` o de las funciones tipadas.
     */
    constructor(private readonly run: ExprThunk<E>) {}

    /** Produce la expresión del intérprete (mismo contrato que `EBuilder.build`). */
    build(EX: ExprAlg<E>): E {
        return this.run(EX);
    }

    /** Conjunción lógica con otra condición. */
    and(other: Condition<E>): Condition<E> {
        return new Condition<E>(EX => EX.logical("AND", this.build(EX), other.build(EX)));
    }

    /** Disyunción lógica con otra condición. */
    or(other: Condition<E>): Condition<E> {
        return new Condition<E>(EX => EX.logical("OR", this.build(EX), other.build(EX)));
    }
}

// ---------------------------------------------------------------------------
// SortKey<E>
// ---------------------------------------------------------------------------

/**
 * Clave de orden producida por `TypedColumn.asc`/`desc`, consumida por
 * `orderBy`. Lleva el thunk de la expresión más la dirección y el orden de
 * nulos elegidos.
 */
export class SortKey<E> {
    /** @internal */
    constructor(
        private readonly run: ExprThunk<E>,
        readonly direction: SortDirection,
        readonly nulls?: NullsOrder
    ) {}

    /** Produce la expresión de columna sobre la que se ordena. */
    build(EX: ExprAlg<E>): E {
        return this.run(EX);
    }

    /** Materializa la clave como un `SortOrder<E>` listo para `DFAlg.orderBy`. */
    toSortOrder(EX: ExprAlg<E>): SortOrder<E> {
        return { expr: this.run(EX), direction: this.direction, nulls: this.nulls };
    }
}

// ---------------------------------------------------------------------------
// TypedColumn<T, E>
// ---------------------------------------------------------------------------

/**
 * Una referencia tipada a una columna de tipo de valor `T` (que puede incluir
 * `null`). Los helpers de comparación aceptan el tipo escalar **no nulo** de la
 * columna (o bien otra columna de tipo compatible), de modo que, por ejemplo,
 * `c.age.gt("x")` se rechaza en compilación cuando `age` es numérica
 * (Requirement 8.3).
 */
export class TypedColumn<T extends ColumnType, E> {
    /**
     * @internal Construye una columna a partir de un thunk del álgebra. Se usa
     * desde el accesor `Columns<S, E>` y las funciones tipadas; no suele
     * invocarse directamente desde código de usuario.
     */
    constructor(protected readonly run: ExprThunk<E>) {}

    /** Produce la expresión del intérprete (mismo contrato que `EBuilder.build`). */
    build(EX: ExprAlg<E>): E {
        return this.run(EX);
    }

    /**
     * Comparación genérica. Si el operando derecho es otra columna se usa su
     * expresión; si es un valor escalar se promueve a literal (semántica de
     * PySpark: `col.eq("x")` compara contra el *valor* `"x"`).
     */
    private cmp(
        op: string,
        other:
            | Extract<NonNull<T>, ScalarType>
            | TypedColumn<NonNull<T>, E>
            | TypedColumn<NonNull<T> | null, E>
    ): Condition<E> {
        return new Condition<E>(EX => {
            const rhs =
                other instanceof TypedColumn
                    ? other.build(EX)
                    : EX.lit(other as ScalarType);
            return EX.bin(op, this.build(EX), rhs);
        });
    }

    /** Igualdad (`=`). */
    eq(other: Extract<NonNull<T>, ScalarType> | TypedColumn<NonNull<T>, E> | TypedColumn<NonNull<T> | null, E>): Condition<E> {
        return this.cmp("=", other);
    }
    /** Mayor que (`>`). */
    gt(other: Extract<NonNull<T>, ScalarType> | TypedColumn<NonNull<T>, E> | TypedColumn<NonNull<T> | null, E>): Condition<E> {
        return this.cmp(">", other);
    }
    /** Mayor o igual (`>=`). */
    gte(other: Extract<NonNull<T>, ScalarType> | TypedColumn<NonNull<T>, E> | TypedColumn<NonNull<T> | null, E>): Condition<E> {
        return this.cmp(">=", other);
    }
    /** Menor que (`<`). */
    lt(other: Extract<NonNull<T>, ScalarType> | TypedColumn<NonNull<T>, E> | TypedColumn<NonNull<T> | null, E>): Condition<E> {
        return this.cmp("<", other);
    }
    /** Menor o igual (`<=`). */
    lte(other: Extract<NonNull<T>, ScalarType> | TypedColumn<NonNull<T>, E> | TypedColumn<NonNull<T> | null, E>): Condition<E> {
        return this.cmp("<=", other);
    }

    /** Chequeo de nulo — disponible en toda columna, sea nullable o no. */
    isNull(): Condition<E> {
        return new Condition<E>(EX => EX.isNull(this.build(EX)));
    }
    /** Chequeo de no-nulo — disponible en toda columna, sea nullable o no. */
    isNotNull(): Condition<E> {
        return new Condition<E>(EX => EX.isNotNull(this.build(EX)));
    }

    /**
     * Reemplaza `null` por un valor de respaldo, produciendo una columna **no
     * nullable**. El respaldo debe ser el tipo escalar no nulo de la columna (o
     * bien otra columna del mismo tipo).
     */
    coalesce(fallback: Extract<NonNull<T>, ScalarType>): TypedColumn<NonNull<T>, E>;
    coalesce<U extends NonNull<T> | null>(fallback: TypedColumn<U, E>): TypedColumn<Coalesced<T, U>, E>;
    coalesce(
        fallback: Extract<NonNull<T>, ScalarType> | TypedColumn<NonNull<T> | null, E>
    ): TypedColumn<ColumnType, E> {
        return new TypedColumn<ColumnType, E>(EX => {
            const fb =
                fallback instanceof TypedColumn
                    ? fallback.build(EX)
                    : EX.lit(fallback as ScalarType);
            return EX.coalesce([this.build(EX), fb]);
        });
    }

    /** Da un alias a la columna (usado dentro de `select`/`withColumn`). */
    as(name: string): TypedColumn<T, E> {
        return new TypedColumn<T, E>(EX => EX.alias(this.build(EX), name));
    }

    /** Clave de orden ascendente (opcionalmente eligiendo el orden de nulos). */
    asc(nulls?: NullsOrder): SortKey<E> {
        return new SortKey<E>(this.run, "asc", nulls);
    }

    /** Clave de orden descendente (opcionalmente eligiendo el orden de nulos). */
    desc(nulls?: NullsOrder): SortKey<E> {
        return new SortKey<E>(this.run, "desc", nulls);
    }
}

// ---------------------------------------------------------------------------
// NumericColumn<T, E>
// ---------------------------------------------------------------------------

/**
 * Una columna numérica tipada que agrega aritmética. El resultado de operar
 * entre dos operandos posiblemente nulos es a su vez nullable (`null` se propaga
 * en Spark). Además conserva/promueve la familia numérica que realmente devuelve
 * Spark; la división siempre produce `number`.
 */
export class NumericColumn<T extends NumericValue | null, E> extends TypedColumn<T, E> {
    /**
     * Operación aritmética genérica. El operando derecho puede ser un `number`
     * (promovido a literal) u otra columna numérica.
     */
    private arith<O extends NumericOperand<E>>(
        op: string,
        other: O
    ): NumericColumn<PromotedNumeric<T, O> | null, E> {
        return new NumericColumn<PromotedNumeric<T, O> | null, E>(EX => {
            const rhs =
                other instanceof TypedColumn ? other.build(EX) : EX.lit(other);
            return EX.bin(op, this.build(EX), rhs);
        });
    }

    /** Suma (`+`). El resultado propaga nullabilidad. */
    plus<O extends NumericOperand<E>>(other: O): NumericColumn<PromotedNumeric<T, O> | null, E> {
        return this.arith("+", other);
    }
    /** Resta (`-`). El resultado propaga nullabilidad. */
    minus<O extends NumericOperand<E>>(other: O): NumericColumn<PromotedNumeric<T, O> | null, E> {
        return this.arith("-", other);
    }
    /** Multiplicación (`*`). El resultado propaga nullabilidad. */
    times<O extends NumericOperand<E>>(other: O): NumericColumn<PromotedNumeric<T, O> | null, E> {
        return this.arith("*", other);
    }
    /** División (`/`). El resultado propaga nullabilidad. */
    div(other: NumericOperand<E>): NumericColumn<number | null, E> {
        return new NumericColumn<number | null, E>(EX => {
            const rhs = other instanceof TypedColumn ? other.build(EX) : EX.lit(other);
            return EX.bin("/", this.build(EX), rhs);
        });
    }
}
