/**
 * SchemaTransforms — transformaciones de schema a nivel de tipos.
 *
 * Cada operación del DataFrame tipado (`select`, `withColumnRenamed`,
 * `withColumn`, `drop`, `groupBy().agg()`, `join`) tiene aquí su transformación
 * de schema correspondiente: una función *pura a nivel de tipos* que, dado el
 * schema de entrada, computa el schema de salida.
 *
 * Este módulo es una **capa pura de tipos**: no contiene ningún valor de
 * runtime. Reusa el vocabulario base y los helpers definidos en
 * `./schema-model` (`Schema`, `ColumnType`, `Prettify`, `UnionToIntersection`);
 * no los redefine.
 *
 * Las especificaciones formales (pre/postcondiciones) de cada transformación
 * están en el documento de diseño, secciones "Especificación de `select`
 * tipado", "Especificación de `filter` / `withColumn` tipados", la inferencia
 * de agregación y "Decisión: política de colisión de nombres en `join`".
 */

import type {
    AmbiguousColumn,
    ColumnType,
    Prettify,
    SchemaShape,
    UnionToIntersection,
} from "./schema-model";

export type { AmbiguousColumn } from "./schema-model";

// ---------------------------------------------------------------------------
// select — proyección
// ---------------------------------------------------------------------------

/**
 * Schema resultante de proyectar el subconjunto de claves `K` desde `S`.
 *
 * Postcondición: el schema resultante contiene *exactamente* las claves `K`
 * con sus tipos originales. Como `K extends keyof S`, cualquier nombre que no
 * pertenezca a `S` no compila en el llamador (Requirement 5.1, 5.2).
 */
export type Selected<S extends SchemaShape, K extends keyof S> = {
    [P in K]: S[P];
};

// ---------------------------------------------------------------------------
// withColumnRenamed — renombrado
// ---------------------------------------------------------------------------

/**
 * Schema tras renombrar la columna `From` a `To`, conservando su tipo de valor.
 *
 * Se elimina la clave de origen y se agrega la de destino con el mismo tipo;
 * el resto de columnas queda intacto (Requirement 5.4).
 */
export type Renamed<
    S extends SchemaShape,
    From extends keyof S,
    To extends string,
> = Prettify<Omit<S, From> & { [P in To]: S[From] }>;

// ---------------------------------------------------------------------------
// withColumn — agregar / reemplazar columna
// ---------------------------------------------------------------------------

/**
 * Schema tras agregar o reemplazar la columna `Name` con el tipo `T`.
 *
 * Si `Name` ya existía, su tipo se reemplaza por `T`; si no, se agrega. El
 * `Omit` previo garantiza el reemplazo limpio incluso cuando la clave ya estaba
 * presente (Requirement 5.3).
 */
export type WithColumn<
    S extends SchemaShape,
    Name extends string,
    T extends ColumnType,
> = Prettify<Omit<S, Name> & { [P in Name]: T }>;

// ---------------------------------------------------------------------------
// drop — eliminación de columnas
// ---------------------------------------------------------------------------

/**
 * Schema tras eliminar las columnas nombradas en `K` (Requirement 5.5).
 */
export type Dropped<S extends SchemaShape, K extends keyof S> = Prettify<
    Omit<S, K>
>;

// ---------------------------------------------------------------------------
// groupBy().agg() — agregación
// ---------------------------------------------------------------------------

/**
 * Forma estructural mínima de una agregación nombrada.
 *
 * Lleva, en el phantom `__out`, el par alias→tipo de salida (`{ [Alias]: Out }`)
 * para que el schema resultante de una agregación pueda computarse de forma
 * estática. Es el contrato que `Aggregated` consume vía `A[number]["__out"]`.
 *
 * Nota de alineación: esta es una definición *estructural* y mínima. El builder
 * concreto que la implementa (`count`/`sum`/`avg`/… con su `.as(alias)`) se
 * crea en `src/typed/aggregations.ts` (task 4.3). Cualquier clase que exponga un
 * `readonly __out: { [Alias]: Out }` —como el `Aggregation` del prototipo en
 * `src/experimental/aggregations.ts`— es asignable a este tipo, de modo que las
 * dos definiciones permanecen consistentes.
 */
export interface Aggregation<
    Alias extends string = string,
    Out extends ColumnType = ColumnType,
> {
    /** @internal phantom: nunca se lee en runtime, carga la forma de salida. */
    readonly __out: { [P in Alias]: Out };
}

/**
 * Schema resultante de `groupBy(...keys).agg(...aggs)`: las claves de
 * agrupación (con su tipo original) más los alias de salida de cada agregación
 * (con su tipo de salida).
 *
 * Postcondición: `dom(resultado) = K ∪ { alias | agg ∈ A }`. El conjunto de
 * claves no depende del orden de las claves de agrupación (Requirement 5.6).
 */
export type Aggregated<
    S extends SchemaShape,
    K extends keyof S,
    A extends readonly { readonly __out: object }[],
> = Prettify<Pick<S, K> & UnionToIntersection<A[number]["__out"]>>;

// ---------------------------------------------------------------------------
// join — política de colisión de nombres
// ---------------------------------------------------------------------------

/**
 * Marca de error de tipo para una columna *ambigua* tras un join: una columna
 * que existe en ambos lados. No es un `ColumnType` válido, de modo que
 * cualquier operación posterior que la requiera como columna (`select`,
 * `filter`, …) no compila, con un mensaje que nombra la columna conflictiva.
 *
 * El join en sí compila; el error aparece solo *donde se usa mal* la columna
 * ambigua. El usuario lo resuelve renombrando antes del join
 * (`withColumnRenamed`) o accediendo por lado (Requirement 6.2, 6.3).
 */
type NullableColumn<T> = T extends ColumnType ? T | null : T;
type JoinName<J extends string> = Uppercase<J>;
type IsLeftNullable<J extends string> = JoinName<J> extends
    | "RIGHT"
    | "RIGHT_OUTER"
    | "OUTER"
    | "FULL"
    | "FULL_OUTER"
    ? true
    : false;
type IsRightNullable<J extends string> = JoinName<J> extends
    | "LEFT"
    | "LEFT_OUTER"
    | "OUTER"
    | "FULL"
    | "FULL_OUTER"
    ? true
    : false;

/**
 * Schema de salida de un join, incluyendo la nullabilidad introducida por el
 * tipo de join. `LEFT_SEMI`/`LEFT_ANTI` solo devuelven las columnas del lado
 * izquierdo, tal como hace Spark. Las colisiones de los demás joins siguen
 * marcadas como ambiguas y nunca se vuelven columnas utilizables.
 */
type JoinedForSingle<
    L extends SchemaShape,
    R extends SchemaShape,
    J extends string,
> = JoinName<J> extends "LEFT_SEMI" | "LEFT_ANTI"
    ? Prettify<L>
    : Prettify<
          {
              [K in Exclude<keyof L, keyof R>]: IsLeftNullable<J> extends true
                  ? NullableColumn<L[K]>
                  : L[K];
          } & {
              [K in Exclude<keyof R, keyof L>]: IsRightNullable<J> extends true
                  ? NullableColumn<R[K]>
                  : R[K];
          } & {
              [K in keyof L & keyof R]: AmbiguousColumn<K & string>;
          }
      >;

/**
 * Distribute over a runtime join-type union. Without this outer naked
 * conditional, `JoinTypeInput` was treated as a single value and both sides
 * incorrectly remained non-null even though the runtime value could be an
 * outer join (or could omit the right side for semi/anti joins).
 */
export type JoinedFor<
    L extends SchemaShape,
    R extends SchemaShape,
    J extends string,
> = J extends unknown ? JoinedForSingle<L, R, J> : never;

/** INNER join por defecto, conservado como alias de compatibilidad. */
export type Joined<L extends SchemaShape, R extends SchemaShape> = JoinedFor<
    L,
    R,
    "INNER"
>;
