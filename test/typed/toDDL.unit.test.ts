import { describe, expect, it } from "vitest";
import {
  arrayType,
  decimalType,
  mapType,
  schema,
  structType,
  TOKEN_TO_DDL,
  type InferSchema,
  type TypeToken,
} from "../../src/schema/schema";

type Equal<X, Y> =
  (<T>() => T extends X ? 1 : 2) extends
  (<T>() => T extends Y ? 1 : 2) ? true : false;
type Expect<T extends true> = T;

// Unit tests de `DeclaredSchema.toDDL()`. Verifican la serialización del DDL a
// partir de la declaración de tokens: emisión de `NOT NULL` para columnas no
// nullables, su ausencia para nullables (sufijo `?`), el mapeo token→DDL y el
// citado con backticks de identificadores no triviales (duplicando los
// backticks internos). Requirements 4.4, 4.5.

describe("DeclaredSchema.toDDL()", () => {
  it("emite NOT NULL en no nullables y lo omite en nullables (sufijo ?)", () => {
    const People = schema({
      id: "int",
      name: "string",
      age: "int?",
      active: "boolean",
    });

    expect(People.toDDL()).toBe(
      "id INT NOT NULL, name STRING NOT NULL, age INT, active BOOLEAN NOT NULL",
    );
  });

  it("mapea correctamente todos los tokens de tipo a su DDL de Spark", () => {
    const All = schema({
      a: "int",
      b: "long",
      c: "double",
      d: "float",
      e: "string",
      f: "boolean",
      g: "date",
      h: "timestamp",
    });

    expect(All.toDDL()).toBe(
      [
        "a INT NOT NULL",
        "b BIGINT NOT NULL",
        "c DOUBLE NOT NULL",
        "d FLOAT NOT NULL",
        "e STRING NOT NULL",
        "f BOOLEAN NOT NULL",
        "g DATE NOT NULL",
        "h TIMESTAMP NOT NULL",
      ].join(", "),
    );
  });

  it("mapea cada token nullable al mismo DDL pero sin NOT NULL", () => {
    const tokens = Object.keys(TOKEN_TO_DDL) as TypeToken[];
    for (const token of tokens) {
      const nonNull = schema({ col: token });
      const nullable = schema({ col: `${token}?` as const });
      const ddlType = TOKEN_TO_DDL[token];

      expect(nonNull.toDDL()).toBe(`col ${ddlType} NOT NULL`);
      expect(nullable.toDDL()).toBe(`col ${ddlType}`);
    }
  });

  it("no cita identificadores simples (letras, dígitos y guion bajo)", () => {
    const Simple = schema({
      user_id: "int",
      _private: "string",
      col2: "int",
    });

    expect(Simple.toDDL()).toBe(
      "user_id INT NOT NULL, _private STRING NOT NULL, col2 INT NOT NULL",
    );
  });

  it("cita con backticks identificadores no triviales (espacios y caracteres especiales)", () => {
    const Weird = schema({
      "first name": "string",
      "weird-col": "int",
      "2cents": "double",
      "with.dot": "boolean",
    });

    expect(Weird.toDDL()).toBe(
      [
        "`first name` STRING NOT NULL",
        "`weird-col` INT NOT NULL",
        "`2cents` DOUBLE NOT NULL",
        "`with.dot` BOOLEAN NOT NULL",
      ].join(", "),
    );
  });

  it("duplica los backticks internos dentro del identificador citado", () => {
    const Backtick = schema({
      "we`ird": "string",
      "a``b": "int?",
    });

    // `we`ird` -> we``ird ; a``b -> a````b ; el nullable omite NOT NULL.
    expect(Backtick.toDDL()).toBe("`we``ird` STRING NOT NULL, `a````b` INT");
  });

  it("serializa decimal y colecciones anidadas sin perder su tipo TS", () => {
    const Complex = schema({
      id: "long",
      amount: decimalType(18, 4),
      tags: arrayType("string"),
      attributes: mapType("string", "long?", true),
      address: structType({ city: "string", zip: "int?" }),
    });

    expect(Complex.toDDL()).toBe([
      "id BIGINT NOT NULL",
      "amount DECIMAL(18,4) NOT NULL",
      "tags ARRAY<STRING> NOT NULL",
      "attributes MAP<STRING,BIGINT>",
      "address STRUCT<city: STRING, zip: INT> NOT NULL",
    ].join(", "));

    type Row = InferSchema<typeof Complex.def>;
    type _ArrayElementsAreConservative = Expect<Equal<Row["tags"], Array<string | null>>>;
    type _MapValuesAreConservative = Expect<
      Equal<Row["attributes"], Map<string, bigint | null> | null>
    >;
    type _StructFieldsAreConservative = Expect<
      Equal<Row["address"], { readonly city: string | null; readonly zip: number | null }>
    >;
    const row: Row = {
      id: 9n,
      amount: "12.3400",
      // Nested nullability is conservative because Spark DDL defaults array
      // elements, map values and struct fields to nullable.
      tags: ["a", null],
      attributes: new Map([["x", null]]),
      address: { city: null, zip: null },
    };
    const soundnessChecks: true[] = [
      true satisfies _ArrayElementsAreConservative,
      true satisfies _MapValuesAreConservative,
      true satisfies _StructFieldsAreConservative,
    ];
    expect(soundnessChecks.every(Boolean)).toBe(true);
    expect(row.id).toBe(9n);
  });

  it("valida precisión, escala y claves de map", () => {
    expect(() => decimalType(0, 0)).toThrow(RangeError);
    expect(() => decimalType(10, 11)).toThrow(RangeError);
    expect(() => mapType("string?", "int")).toThrow(/cannot be nullable/);
  });
});
