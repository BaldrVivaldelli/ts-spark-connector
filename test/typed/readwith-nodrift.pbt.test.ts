import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { SparkSession } from "../../src";
import {
  schema,
  TOKEN_TO_DDL,
  type FieldToken,
  type InferField,
  type InferSchema,
  type TokenToTs,
  type TypeToken,
} from "../../src/schema/schema";

/**
 * Property 8: Sin drift en `readWith`
 *
 * **Validates: Requirements 4.1, 4.2**
 *
 * Una única declaración (`schema({...})`) alimenta a la vez:
 *   - el TIPO del DataFrame (`InferSchema<D>`, vía la fuente de verdad token→TS
 *     `TokenToTs`), y
 *   - el DDL que `readWith` inyecta en `Read.DataSource.schema` del plan proto
 *     (vía la fuente de verdad token→DDL `TOKEN_TO_DDL`, dentro de `toDDL()`).
 *
 * Como ambos lados derivan de la MISMA declaración y de los MISMOS mapas
 * token→{TS,DDL}, no pueden divergir ("drift"). Esta suite ancla esa propiedad:
 *
 *   - Lado de tipos (estático, verificado por `tsc -p tsconfig.test.json`):
 *     `InferSchema<D>` se deriva exactamente de `TokenToTs` (+ `| null` para los
 *     tokens nullables). Si alguien cambiara `TokenToTs` y no el inferidor, estas
 *     aserciones dejarían de compilar.
 *
 *   - Lado de runtime (fast-check):
 *       (P8.a) El DDL que `readWith(schema(def), ...)` inyecta en
 *              `proto.read.data_source.schema` es EXACTAMENTE `schema(def).toDDL()`.
 *              Es decir, lo que viaja al servidor es la misma declaración que tipa
 *              el DataFrame: una sola fuente conduce ambos caminos (Requirement 4.2).
 *       (P8.b) Cada columna del DDL es función pura de `TOKEN_TO_DDL[baseToken]`
 *              con `NOT NULL` sii la columna NO es nullable (Requirement 4.1):
 *              el DDL no inventa tipos, solo proyecta el mismo mapa token→DDL que
 *              el tipo proyecta como token→TS.
 */

// ---------------------------------------------------------------------------
// Lado de tipos: InferSchema<D> deriva de TokenToTs (la misma fuente de verdad)
// ---------------------------------------------------------------------------
//
// Igualdad de tipos estricta: solo compila si A y B son exactamente iguales.
type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B
  ? 1
  : 2
  ? true
  : false;
type Expect<T extends true> = T;

// Cada token base infiere EXACTAMENTE el tipo de `TokenToTs` (token→TS).
type _IntFromTokenToTs = Expect<Equal<InferField<"int">, TokenToTs["int"]>>;
type _LongFromTokenToTs = Expect<Equal<InferField<"long">, TokenToTs["long"]>>;
type _DoubleFromTokenToTs = Expect<Equal<InferField<"double">, TokenToTs["double"]>>;
type _StringFromTokenToTs = Expect<Equal<InferField<"string">, TokenToTs["string"]>>;
type _BoolFromTokenToTs = Expect<Equal<InferField<"boolean">, TokenToTs["boolean"]>>;
type _DateFromTokenToTs = Expect<Equal<InferField<"date">, TokenToTs["date"]>>;

// Los tokens nullables proyectan el mismo tipo de `TokenToTs` más `| null`.
type _NullableIntFromTokenToTs = Expect<
  Equal<InferField<"int?">, TokenToTs["int"] | null>
>;
type _NullableStringFromTokenToTs = Expect<
  Equal<InferField<"string?">, TokenToTs["string"] | null>
>;

// El schema completo se deriva de `TokenToTs`, igual que el DDL se deriva de
// `TOKEN_TO_DDL`: una sola declaración conduce ambos.
type _SchemaDerivesFromTokenToTs = Expect<
  Equal<
    InferSchema<{ id: "int"; name: "string"; age: "int?" }>,
    { id: TokenToTs["int"]; name: TokenToTs["string"]; age: TokenToTs["int"] | null }
  >
>;

// ---------------------------------------------------------------------------
// Generadores (fast-check)
// ---------------------------------------------------------------------------

const TOKENS: readonly TypeToken[] = [
  "int",
  "long",
  "double",
  "float",
  "string",
  "boolean",
  "date",
  "timestamp",
];

/** Un token de campo: base opcionalmente sufijado con `?` (nullable). */
const fieldTokenArb: fc.Arbitrary<FieldToken> = fc
  .tuple(fc.constantFrom(...TOKENS), fc.boolean())
  .map(([base, nullable]) => (nullable ? (`${base}?` as FieldToken) : base));

/**
 * Nombres de columna como identificadores simples (`[A-Za-z_][A-Za-z0-9_]*`):
 * no requieren citado con backticks, lo que mantiene el parseo del DDL trivial
 * (partir por ", " y luego por espacios). Se evitan comas y backticks a
 * propósito para que `, ` siga siendo un separador inequívoco entre columnas.
 */
const simpleNameArb: fc.Arbitrary<string> = fc
  .tuple(
    fc.constantFrom(..."abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_".split("")),
    fc.stringMatching(/^[A-Za-z0-9_]{0,7}$/),
  )
  .map(([head, tail]) => head + tail);

/** Una `SchemaDef` con identificadores simples y claves únicas. */
const schemaDefArb: fc.Arbitrary<Record<string, FieldToken>> = fc.dictionary(
  simpleNameArb,
  fieldTokenArb,
  { minKeys: 1, maxKeys: 8 },
);

// Sesión perezosa: `getOrCreate()` no abre conexión; `toProtoJSON()` serializa
// el plan sin contactar al servidor (igual que en test/readWith.proto.test.ts).
const sess = SparkSession.builder().getOrCreate();

// ---------------------------------------------------------------------------
// P8.a — El DDL inyectado en el plan es exactamente `toDDL()` (Requirement 4.2)
// ---------------------------------------------------------------------------

describe("Property 8: sin drift en readWith — el plan lleva la misma declaración que el tipo", () => {
  it("inyecta en proto.read.data_source.schema exactamente schema(def).toDDL()", () => {
    fc.assert(
      fc.property(schemaDefArb, (def) => {
        const declared = schema(def);
        const df = sess.read.readWith(declared, "csv", "/tmp/x.csv");

        const proto = JSON.parse(df.toProtoJSON());

        // El DDL que viaja al servidor === el DDL que produce la declaración que
        // también tipa el DataFrame. No hay dos fuentes: hay una.
        expect(proto.read.data_source.schema).toBe(declared.toDDL());
      }),
    );
  });

  it("conserva format/paths junto al schema declarado en el plan", () => {
    fc.assert(
      fc.property(schemaDefArb, (def) => {
        const declared = schema(def);
        const df = sess.read.readWith(declared, "csv", "/tmp/x.csv");

        const proto = JSON.parse(df.toProtoJSON());

        expect(proto.read.data_source).toMatchObject({
          format: "csv",
          paths: ["/tmp/x.csv"],
          schema: declared.toDDL(),
        });
      }),
    );
  });

  // -------------------------------------------------------------------------
  // P8.b — Cada columna del DDL es función pura de TOKEN_TO_DDL[base] + nullab.
  //         (Requirement 4.1: misma fuente de verdad token→DDL que token→TS)
  // -------------------------------------------------------------------------
  it("cada columna del DDL deriva de TOKEN_TO_DDL[base] con NOT NULL sii no-nullable", () => {
    fc.assert(
      fc.property(schemaDefArb, (def) => {
        const declared = schema(def);
        const df = sess.read.readWith(declared, "csv", "/tmp/x.csv");

        const injected: string = JSON.parse(df.toProtoJSON()).read.data_source.schema;

        // `toDDL()` une columnas con ", " preservando el orden de Object.entries.
        // Con identificadores simples (sin comas), partir por ", " da una entrada
        // por columna, alineada índice a índice con Object.entries(def).
        const entries = Object.entries(def);
        const parts = injected.split(", ");
        expect(parts.length).toBe(entries.length);

        entries.forEach(([name, token], i) => {
          const nullable = token.endsWith("?");
          const base = (nullable ? token.slice(0, -1) : token) as TypeToken;
          const ddlType = TOKEN_TO_DDL[base];

          // El tipo DDL es exactamente TOKEN_TO_DDL[base] (no se inventa nada),
          // y la nullabilidad se proyecta como presencia/ausencia de NOT NULL,
          // espejo de cómo el tipo proyecta `| null`.
          const expected = `${name} ${ddlType}${nullable ? "" : " NOT NULL"}`;
          expect(parts[i]).toBe(expected);
        });
      }),
    );
  });
});
