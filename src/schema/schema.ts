/**
 * Declaración única de schema (`schema({...})`) con inferencia de tipos.
 *
 * Declaraciones públicas de schema y sus utilidades de inferencia.
 *
 * Una sola declaración `schema({...})` cumple dos propósitos a la vez, a partir
 * de la **misma fuente de verdad** token→tipo (`TokenToTs`) y token→DDL
 * (`TOKEN_TO_DDL`):
 *
 *  1. RUNTIME: serializa a un string DDL de Spark (p. ej. "id INT, name STRING")
 *     que se envía en `Read.DataSource.schema`, de modo que Spark use exactamente
 *     este schema en lugar de inferirlo. Esto elimina el drift de schema en la
 *     fuente.
 *
 *  2. COMPILE TIME: el tipo TS de las columnas declaradas se infiere de los
 *     literales de token vía mapped types (`InferSchema<D>`), de modo que fluye
 *     al DataFrame tipado.
 *
 * Los mapeos token→tipo-TS y token→DDL son la única fuente de verdad compartida
 * por ambos caminos.
 */

// ---------------------------------------------------------------------------
// Tokens y fuente de verdad token -> tipo TS / token -> DDL
// ---------------------------------------------------------------------------

/**
 * Tokens de tipo de columna admitidos.
 * @public
 */
export type TypeToken =
    | "int"
    | "long"
    | "double"
    | "float"
    | "string"
    | "boolean"
    | "date"
    | "timestamp";

/**
 * Mapea cada token al tipo de valor TS usado en el schema.
 * @internal
 */
export interface TokenToTs {
    int: number;
    long: bigint;
    double: number;
    float: number;
    string: string;
    boolean: boolean;
    date: string;
    timestamp: string;
}

/**
 * Decimal exacto: se expone como string para no introducir redondeo IEEE-754.
 * @public
 */
export interface DecimalSpec {
    readonly kind: "decimal";
    readonly precision: number;
    readonly scale: number;
    readonly nullable?: boolean;
}

/** @public */
export interface ArraySpec<Element extends FieldSpec = FieldSpec> {
    readonly kind: "array";
    readonly element: Element;
    readonly nullable?: boolean;
}

/** @public */
export interface MapSpec<Key extends FieldSpec = FieldSpec, Value extends FieldSpec = FieldSpec> {
    readonly kind: "map";
    readonly key: Key;
    readonly value: Value;
    readonly nullable?: boolean;
}

/** @public */
export interface StructSpec<Fields extends SchemaDef = SchemaDef> {
    readonly kind: "struct";
    readonly fields: Fields;
    readonly nullable?: boolean;
}

/**
 * Mapea cada token a su nombre de tipo DDL de Spark. Es la otra mitad de la
 * fuente de verdad única (junto con `TokenToTs`): el tipo y el DDL derivan de
 * la misma declaración, evitando el drift.
 * @internal
 */
export const TOKEN_TO_DDL: Record<TypeToken, string> = {
    int: "INT",
    long: "BIGINT",
    double: "DOUBLE",
    float: "FLOAT",
    string: "STRING",
    boolean: "BOOLEAN",
    date: "DATE",
    timestamp: "TIMESTAMP",
};

// ---------------------------------------------------------------------------
// Declaración de campos y schema
// ---------------------------------------------------------------------------

/**
 * Un token sufijado con `?` que marca la columna como nullable.
 * @public
 */
export type NullableToken = `${TypeToken}?`;

/**
 * Una declaración de campo: un token de tipo, opcionalmente nullable (`?`).
 * @public
 */
export type ComplexSpec = DecimalSpec | ArraySpec | MapSpec | StructSpec;

/**
 * Declaración de campo primitivo o complejo, opcionalmente nullable.
 * @public
 */
export type FieldSpec = TypeToken | NullableToken | ComplexSpec;

/**
 * Alias histórico conservado para consumidores que solo usan primitivas.
 * @public
 */
export type FieldToken = TypeToken | NullableToken;

/**
 * Una declaración de schema: mapa nombre de columna -> token de campo.
 * @public
 */
export interface SchemaDef {
    readonly [name: string]: FieldSpec;
}

/**
 * Infiere el tipo TS de un campo, agregando `| null` para los tokens nullables
 * (sufijo `?`). El `?` final marca nullabilidad y se propaga al tipo.
 * @internal
 */
export type InferNonNullable<F extends FieldSpec> = F extends `${infer B}?`
    ? B extends TypeToken
        ? TokenToTs[B]
        : never
    : F extends TypeToken
      ? TokenToTs[F]
      : F extends DecimalSpec
        ? string
        : F extends ArraySpec<infer Element>
          ? Array<InferField<Element> | null>
          : F extends MapSpec<infer Key, infer Value>
            ? Map<
                  Extract<InferNonNullable<Key>, string | number | bigint | boolean>,
                  InferField<Value> | null
              >
            : F extends StructSpec<infer Fields>
              ? InferNestedSchema<Fields>
              : never;

/** @public */
export type InferField<F extends FieldSpec> = F extends `${TypeToken}?`
    ? InferNonNullable<F> | null
    : F extends { readonly nullable: true }
      ? InferNonNullable<F> | null
      : InferNonNullable<F>;

/**
 * Spark's DDL string does not carry `StructField.nullable`,
 * `ArrayType.containsNull` or `MapType.valueContainsNull` for nested values.
 * Model nested values conservatively so the TypeScript row contract never
 * promises non-null where Spark's parsed DDL defaults to nullable.
 * @internal
 */
export type InferNestedSchema<D extends SchemaDef> = {
    [K in keyof D]: InferField<D[K]> | null;
};

/**
 * Infiere el schema de runtime (nombre -> tipo TS) de una declaración de tokens.
 * @public
 */
export type InferSchema<D extends SchemaDef> = { [K in keyof D]: InferField<D[K]> };

// ---------------------------------------------------------------------------
// DeclaredSchema y la fábrica `schema()`
// ---------------------------------------------------------------------------

/**
 * Un schema declarado. Lleva la definición de tokens original (para generar el
 * DDL) y, en el sistema de tipos, los tipos de columna inferidos vía el phantom
 * `__schema`.
 * @public
 */
export class DeclaredSchema<D extends SchemaDef> {
    /** @internal phantom: nunca se lee en runtime, solo lleva el tipo. */
    declare readonly __schema: InferSchema<D>;

    /** @internal */ constructor(readonly def: D) {}

    /**
     * String DDL de Spark, p. ej. "id INT NOT NULL, name STRING".
     *
     * - El sufijo `?` marca nullabilidad y se propaga a la *ausencia* de
     *   `NOT NULL` en el DDL.
     * - Las columnas no nullables emiten `NOT NULL` para que Spark haga cumplir
     *   la misma nullabilidad que promete el tipo.
     * - Los identificadores no triviales se citan con backticks (duplicando
     *   backticks internos).
     */
    toDDL(): string {
        return Object.entries(this.def)
            .map(([name, spec]) => {
                const nullable = isNullableSpec(spec);
                return `${ddlIdentifier(name)} ${ddlType(spec)}${nullable ? "" : " NOT NULL"}`;
            })
            .join(", ");
    }
}

/**
 * Declara un schema a partir de un mapa de tokens. No requiere `as const`
 * porque los valores ya son una unión finita de tokens.
 *
 * @example
 *   const People = schema({ id: "int", name: "string", age: "int" });
 *   // DeclaredSchema con tipo inferido { id: number; name: string; age: number }
 * @public
 */
export function schema<D extends SchemaDef>(def: D): DeclaredSchema<D> {
    return new DeclaredSchema<D>(def);
}

/**
 * Marca de nulabilidad que agregan `decimalType`/`arrayType`/`mapType`/`structType` cuando `nullable` es `true`.
 * @public
 */
export type NullableFlag<N extends boolean> = N extends true ? { readonly nullable: true } : object;

/**
 * Crea un descriptor DECIMAL exacto, validando los límites de Spark.
 * @public
 */
export function decimalType<
    const P extends number,
    const S extends number,
    const N extends boolean = false,
>(
    precision: P,
    scale: S,
    nullable?: N,
): DecimalSpec & { readonly precision: P; readonly scale: S } & NullableFlag<N> {
    if (!Number.isSafeInteger(precision) || precision < 1 || precision > 38) {
        throw new RangeError("decimalType(): precision must be an integer in [1, 38]");
    }
    if (!Number.isSafeInteger(scale) || scale < 0 || scale > precision) {
        throw new RangeError("decimalType(): scale must be an integer in [0, precision]");
    }
    return { kind: "decimal", precision, scale, ...(nullable ? { nullable: true } : {}) } as
        DecimalSpec & { readonly precision: P; readonly scale: S } & NullableFlag<N>;
}

/** @public */
export function arrayType<const Element extends FieldSpec, const N extends boolean = false>(
    element: Element,
    nullable?: N,
): ArraySpec<Element> & NullableFlag<N> {
    return { kind: "array", element, ...(nullable ? { nullable: true } : {}) } as
        ArraySpec<Element> & NullableFlag<N>;
}

/** @public */
export function mapType<
    const Key extends FieldSpec,
    const Value extends FieldSpec,
    const N extends boolean = false,
>(
    key: Key,
    value: Value,
    nullable?: N,
): MapSpec<Key, Value> & NullableFlag<N> {
    if (isNullableSpec(key)) throw new TypeError("mapType(): map keys cannot be nullable");
    return { kind: "map", key, value, ...(nullable ? { nullable: true } : {}) } as
        MapSpec<Key, Value> & NullableFlag<N>;
}

/** @public */
export function structType<const Fields extends SchemaDef, const N extends boolean = false>(
    fields: Fields,
    nullable?: N,
): StructSpec<Fields> & NullableFlag<N> {
    return { kind: "struct", fields, ...(nullable ? { nullable: true } : {}) } as
        StructSpec<Fields> & NullableFlag<N>;
}

// ---------------------------------------------------------------------------
// Citado de identificadores para DDL
// ---------------------------------------------------------------------------

/**
 * Cita un identificador de columna para el DDL si contiene algo más allá de un
 * identificador simple (Spark DDL usa backticks, duplicando los backticks
 * embebidos).
 */
function ddlIdentifier(name: string): string {
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name)
        ? name
        : "`" + name.replace(/`/g, "``") + "`";
}

function isNullableSpec(spec: FieldSpec): boolean {
    return typeof spec === "string" ? spec.endsWith("?") : spec.nullable === true;
}

/**
 * Spark DDL type of one field spec (`INT`, `ARRAY<STRING>`, `DECIMAL(10,2)`,
 * `STRUCT<a: INT>`); the nullability marker is not part of a type string.
 * @internal
 */
export function fieldSpecToDDL(spec: FieldSpec): string {
    return ddlType(spec);
}

function ddlType(spec: FieldSpec): string {
    if (typeof spec === "string") {
        const base = spec.endsWith("?") ? spec.slice(0, -1) : spec;
        return TOKEN_TO_DDL[base as TypeToken];
    }
    switch (spec.kind) {
        case "decimal":
            return `DECIMAL(${spec.precision},${spec.scale})`;
        case "array":
            return `ARRAY<${ddlType(spec.element)}>`;
        case "map":
            return `MAP<${ddlType(spec.key)},${ddlType(spec.value)}>`;
        case "struct":
            return `STRUCT<${Object.entries(spec.fields)
                .map(([name, field]) => `${ddlIdentifier(name)}: ${ddlType(field)}`)
                .join(", ")}>`;
    }
    throw new TypeError("Unsupported Spark schema descriptor");
}
