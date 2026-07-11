import { describe, expect, it } from "vitest";
import fc from "fast-check";
import {
  schema,
  TOKEN_TO_DDL,
  type FieldToken,
  type InferField,
  type InferSchema,
  type TypeToken,
} from "../../src/schema/schema";

/**
 * Property 9: Nullabilidad consistente
 *
 * **Validates: Requirements 4.3, 4.4**
 *
 * Para cualquier token de campo:
 *   - `'?' ∈ token  ⟺  el tipo inferido incluye null`   (Requirement 4.3)
 *   - `'?' ∉ token  ⟺  el DDL emite NOT NULL`            (Requirement 4.4)
 *
 * El lado de tipos (`'?' ⟺ incluye null`) se ancla con aserciones estáticas de
 * igualdad de tipos (verificadas por `tsc -p tsconfig.test.json`). El lado de
 * runtime (`'?' ⟺ ausencia/presencia de NOT NULL`) se valida con fast-check
 * sobre `schema(def).toDDL()`.
 */

// ---------------------------------------------------------------------------
// Lado de tipos: '?' ∈ token ⟺ el tipo inferido incluye null (Requirement 4.3)
// ---------------------------------------------------------------------------
//
// Igualdad de tipos estricta: solo compila si A y B son exactamente iguales.
type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B
  ? 1
  : 2
  ? true
  : false;
type Expect<T extends true> = T;

// Token base (sin `?`) ⟹ el tipo NO incluye null.
type _NonNullExcludesNull = Expect<Equal<InferField<"int">, number>>;
type _StringExcludesNull = Expect<Equal<InferField<"string">, string>>;
type _BoolExcludesNull = Expect<Equal<InferField<"boolean">, boolean>>;

// Token nullable (con `?`) ⟹ el tipo SÍ incluye null.
type _NullableIncludesNull = Expect<Equal<InferField<"int?">, number | null>>;
type _NullableStringIncludesNull = Expect<Equal<InferField<"string?">, string | null>>;
type _NullableBoolIncludesNull = Expect<Equal<InferField<"boolean?">, boolean | null>>;

// La biconditional se propaga al schema completo: la clave nullable incluye null,
// la no-nullable no.
type _SchemaProjectsNullability = Expect<
  Equal<
    InferSchema<{ id: "int"; note: "string?" }>,
    { id: number; note: string | null }
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

/** Un token de campo: base opcionalmente sufijado con `?`. */
const fieldTokenArb: fc.Arbitrary<FieldToken> = fc
  .tuple(fc.constantFrom(...TOKENS), fc.boolean())
  .map(([base, nullable]) => (nullable ? (`${base}?` as FieldToken) : base));

/**
 * Nombres de columna. Se incluyen caracteres que fuerzan el citado con
 * backticks (espacios, guiones, dígitos iniciales) para ejercitar el parseo
 * de identificadores citados, pero se excluyen comas y backticks para que el
 * separador `, ` del DDL siga siendo inequívoco al partir por columna.
 */
const nameCharArb = fc.constantFrom(
  ..."abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_ -".split("")
);
const nameArb = fc
  .array(nameCharArb, { minLength: 1, maxLength: 8 })
  .map((cs) => cs.join(""));

/** Una SchemaDef: mapa nombre de columna -> token de campo (claves únicas). */
const schemaDefArb: fc.Arbitrary<Record<string, FieldToken>> = fc.dictionary(nameArb, fieldTokenArb, {
  minKeys: 1,
  maxKeys: 8,
});

// ---------------------------------------------------------------------------
// Lado de runtime: '?' ∉ token ⟺ el DDL emite NOT NULL (Requirements 4.3, 4.4)
// ---------------------------------------------------------------------------

describe("Property 9: nullabilidad consistente — DDL", () => {
  it("'?' ∈ token ⟺ ausencia de NOT NULL; '?' ∉ token ⟺ NOT NULL", () => {
    fc.assert(
      fc.property(schemaDefArb, (def) => {
        const ddl = schema(def).toDDL();

        // `toDDL()` une las columnas con ", " (Object.entries preserva el
        // orden). Como los nombres no contienen comas, partir por ", " devuelve
        // exactamente una entrada por columna, alineada índice a índice con
        // Object.entries(def).
        const entries = Object.entries(def);
        const parts = ddl.split(", ");
        expect(parts.length).toBe(entries.length);

        entries.forEach(([, token], i) => {
          const part = parts[i];
          const nullable = token.endsWith("?");
          const base = (nullable ? token.slice(0, -1) : token) as TypeToken;
          const ddlType = TOKEN_TO_DDL[base];

          // Biconditional central: NOT NULL presente ⟺ token NO nullable.
          expect(part.endsWith(" NOT NULL")).toBe(!nullable);

          if (nullable) {
            // Nullable: la entrada termina en el tipo DDL, sin NOT NULL.
            expect(part.endsWith(ddlType)).toBe(true);
          } else {
            // No nullable: la entrada termina en "<TIPO> NOT NULL".
            expect(part.endsWith(`${ddlType} NOT NULL`)).toBe(true);
          }
        });
      })
    );
  });
});
