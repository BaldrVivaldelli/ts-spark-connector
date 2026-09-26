/**
 * SchemaModel — modelo de tipos del schema.
 *
 * Define el vocabulario de tipos que representa un schema en el sistema de
 * tipos. Es la única fuente de verdad de la *forma* del schema: tanto el camino
 * laxo (sin tipar) como el camino tipado dependen de estos tipos.
 *
 * Este módulo es una **capa pura de tipos**: no contiene valores de runtime más
 * allá de lo que los propios tipos requieren (p. ej. el `unique symbol` que
 * marca `UnknownSchema`, que nunca se materializa). Por eso agregar el schema
 * `S` no introduce overhead de runtime alguno.
 *
 * Las transformaciones de schema por operación (`Selected`, `Renamed`,
 * `Joined`, `Aggregated`, …) viven en `./schema-transforms`, no aquí.
 */

// ---------------------------------------------------------------------------
// Tipos base del schema
// ---------------------------------------------------------------------------

/**
 * Escalares públicos devueltos por `collect()`.
 *
 * `long` usa `bigint` para no perder precisión; fechas, timestamps y decimales
 * se normalizan a strings canónicos (ISO/decimal) en la frontera Arrow.
 */
export type ScalarType = string | number | bigint | boolean;

/** Valores complejos públicos devueltos por `collect()`. */
export type ComplexType =
    | readonly ColumnType[]
    | ReadonlyMap<ScalarType, ColumnType>
    | ({ readonly [field: string]: ColumnType } & { readonly __ambiguous?: never });

/** Un tipo de valor de columna, posiblemente nulo y recursivo. */
export type ColumnType = ScalarType | ComplexType | null;

/** Un schema es un mapa nombre de columna -> tipo de valor (TS). */
export type Schema = Record<string, ColumnType>;

/**
 * Marca una columna cuyo nombre existe en ambos lados de un join.  Vive en el
 * modelo base (y no en el DataFrame) para que las transformaciones posteriores
 * puedan conservar el resto del schema sin convertir una columna ambigua en
 * una columna utilizable por accidente.
 */
export type AmbiguousColumn<Name extends string = string> = {
    readonly __ambiguous: Name;
};

/** Schema que puede contener marcas de ambigüedad producidas por un join. */
export type SchemaShape = Record<string, ColumnType | AmbiguousColumn>;

/** Nombres que todavía representan columnas utilizables. */
export type UsableColumnKey<S> = {
    [K in keyof S]-?: S[K] extends ColumnType ? K : never;
}[keyof S] & string;

/** Extrae el tipo de valor de una columna utilizable. */
export type UsableColumnType<S, K extends keyof S> = Extract<S[K], ColumnType>;

// ---------------------------------------------------------------------------
// Schema desconocido (compatibilidad con el camino laxo)
// ---------------------------------------------------------------------------

/**
 * Marca de "schema desconocido". Es el valor por defecto del parámetro de
 * schema de la superficie pública y preserva el comportamiento actual (laxo):
 * cuando `S` es `UnknownSchema` se activan las sobrecargas laxas
 * (`string`/`EBuilder`) equivalentes a la API actual.
 *
 * El `unique symbol` garantiza que `UnknownSchema` no colisione estructuralmente
 * con ningún `Schema` real declarado por el usuario.
 */
export type UnknownSchema = { readonly __unknownSchema: unique symbol };

/**
 * Decide si un schema `S` es *conocido* (un `Schema` real) o *desconocido*
 * (la marca `UnknownSchema`). Es el predicado que elige qué sobrecarga aplica:
 * `false` -> camino laxo; `true` -> camino tipado.
 *
 * El envoltorio en tupla (`[S] extends [UnknownSchema]`) evita la distribución
 * sobre uniones, de modo que la comparación se hace sobre `S` como un todo.
 */
export type IsKnownSchema<S> = [S] extends [UnknownSchema]
    ? false
    : S extends Schema
      ? true
      : false;

// ---------------------------------------------------------------------------
// Maquinaria de tipos clave (helpers)
// ---------------------------------------------------------------------------

/** Quita `| null` para obtener el tipo escalar subyacente de una columna. */
export type NonNull<T> = T extends null ? never : T;

/**
 * Aplana una intersección de tipos objeto en un único tipo objeto legible.
 * No cambia la semántica del tipo; solo mejora cómo lo muestran las
 * herramientas (hover, errores), lo que es clave para que los mensajes de las
 * transformaciones de schema sean comprensibles.
 */
export type Prettify<T> = { [K in keyof T]: T[K] } & {};

/**
 * Convierte una unión `U` en la intersección de sus miembros. Helper estándar
 * usado, entre otras cosas, para fusionar las salidas de varias agregaciones en
 * un único tipo objeto.
 */
export type UnionToIntersection<U> = (
    U extends unknown ? (k: U) => void : never
) extends (k: infer I) => void
    ? I
    : never;
