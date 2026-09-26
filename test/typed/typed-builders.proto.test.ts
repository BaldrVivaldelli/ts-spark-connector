import { describe, expect, it } from "vitest";

import { ProtoExprAlg } from "../../src/engine/compilerRead";
import { col, lit, call, coalesce, when } from "../../src/engine/column";
import { NumericColumn, TypedColumn } from "../../src/typed/typed-column";
import { abs, concat, length, lower, round, upper, when as twhen } from "../../src/typed/functions";
import { makeAggFactory } from "../../src/typed/aggregations";
import { ColumnType } from "../../src/schema/schema-model";

/**
 * Plan-assertion tests de los builders tipados (task 4.5).
 *
 * **Validates: Requirements 3.2, 11.3**
 *
 * Garantía de "pure type layer": los builders tipados de `src/typed/*`
 * (`typed-column`, `functions`, `aggregations`) construyen su expresión `E` a
 * través de `ExprAlg<E>` y, por tanto, deben producir una expresión
 * **estructuralmente idéntica** a la del camino no tipado equivalente
 * (`col`/`lit`/`call`/... de `src/engine/column.ts`, el `EBuilder`).
 *
 * Estrategia: se interpreta **tanto** el builder tipado **como** el equivalente
 * no tipado contra el **mismo** intérprete concreto (`ProtoExprAlg`, de
 * `src/engine/compilerRead.ts`, que produce los objetos proto), y se compara el
 * proto resultante con `toEqual`. Si ambos caminos generan el mismo plan proto,
 * la capa tipada no altera el plan (Requirement 3.2 / Plan_Equivalence 11.3).
 *
 * Nota sobre la aritmética: el `EBuilder` no tipado **no** expone
 * `plus/minus/times/div` (solo comparaciones y lógicos), por lo que el
 * "equivalente no tipado" de la aritmética es la expresión canónica construida
 * directamente con el álgebra (`ProtoExprAlg.bin(...)`), que es exactamente lo
 * que cualquier camino produciría para esa operación.
 */

// `E` es el tipo de expresión del intérprete proto (interno = any).
type E = ReturnType<typeof ProtoExprAlg.col>;

const EX = ProtoExprAlg;

// ---------------------------------------------------------------------------
// Helpers: construir columnas tipadas como lo haría el accesor `Columns<S, E>`
// (un thunk `EX => EX.col(name)`), sin depender de la superficie unificada.
// ---------------------------------------------------------------------------

const tcol = <T extends ColumnType>(name: string): TypedColumn<T, E> =>
    new TypedColumn<T, E>(ex => ex.col(name));

const ncol = (name: string): NumericColumn<number | null, E> =>
    new NumericColumn<number | null, E>(ex => ex.col(name));

/**
 * Afirma que un builder tipado y su equivalente no tipado producen el mismo
 * proto al interpretarse con `ProtoExprAlg`.
 */
const sameProto = (
    typed: { build(ex: typeof EX): E },
    untyped: { build(ex: typeof EX): E }
): void => {
    expect(typed.build(EX)).toEqual(untyped.build(EX));
};

describe("Plan-assertion: builders de columna tipados vs EBuilder no tipado", () => {
    it("referencia de columna (col) produce el mismo proto", () => {
        sameProto(tcol("age"), col("age"));
    });

    it("comparación eq produce el mismo proto", () => {
        // typed: c.name.eq("bob")  ~  untyped: col("name").eq("bob")
        sameProto(tcol<string>("name").eq("bob"), col("name").eq("bob"));
    });

    it("comparación gt produce el mismo proto", () => {
        sameProto(ncol("age").gt(18), col("age").gt(18));
    });

    it("comparaciones gte/lt/lte producen el mismo proto", () => {
        sameProto(ncol("age").gte(18), col("age").gte(18));
        sameProto(ncol("age").lt(65), col("age").lt(65));
        sameProto(ncol("age").lte(65), col("age").lte(65));
    });

    it("condiciones compuestas (and/or) producen el mismo proto", () => {
        const typed = ncol("age").gt(18).and(tcol<string>("name").eq("bob"));
        const untyped = col("age").gt(18).and(col("name").eq("bob"));
        sameProto(typed, untyped);
    });

    it("isNull / isNotNull producen el mismo proto", () => {
        sameProto(tcol<string | null>("note").isNull(), col("note").isNull());
        sameProto(tcol<string | null>("note").isNotNull(), col("note").isNotNull());
    });

    it("coalesce produce el mismo proto", () => {
        // El fallback escalar es un literal en ambos caminos: lit("n/a").
        sameProto(
            tcol<string | null>("note").coalesce("n/a"),
            coalesce(col("note"), lit("n/a"))
        );
    });

    it("alias (as) produce el mismo proto", () => {
        sameProto(tcol<string>("name").as("who"), col("name").alias("who"));
    });
});

describe("Plan-assertion: aritmética numérica tipada vs álgebra canónica", () => {
    it("plus produce el mismo proto", () => {
        sameProto(ncol("amount").plus(10), {
            build: ex => ex.bin("+", ex.col("amount"), ex.lit(10)),
        });
    });

    it("minus produce el mismo proto", () => {
        sameProto(ncol("amount").minus(10), {
            build: ex => ex.bin("-", ex.col("amount"), ex.lit(10)),
        });
    });

    it("times produce el mismo proto", () => {
        sameProto(ncol("amount").times(2), {
            build: ex => ex.bin("*", ex.col("amount"), ex.lit(2)),
        });
    });

    it("div produce el mismo proto", () => {
        sameProto(ncol("amount").div(2), {
            build: ex => ex.bin("/", ex.col("amount"), ex.lit(2)),
        });
    });

    it("aritmética entre columnas produce el mismo proto", () => {
        sameProto(ncol("amount").minus(ncol("discount")), {
            build: ex => ex.bin("-", ex.col("amount"), ex.col("discount")),
        });
    });
});

describe("Plan-assertion: funciones escalares tipadas vs call(...) no tipado", () => {
    it("length produce el mismo proto", () => {
        sameProto(length(tcol<string>("product")), call("length", [col("product")]));
    });

    it("upper produce el mismo proto", () => {
        sameProto(upper(tcol<string>("product")), call("upper", [col("product")]));
    });

    it("lower produce el mismo proto", () => {
        sameProto(lower(tcol<string>("product")), call("lower", [col("product")]));
    });

    it("concat produce el mismo proto", () => {
        // En concat, las partes string son literales en el camino tipado: lit("!").
        sameProto(
            concat(tcol<string>("product"), "!"),
            call("concat", [col("product"), lit("!")])
        );
    });

    it("abs produce el mismo proto", () => {
        sameProto(abs(ncol("amount")), call("abs", [col("amount")]));
    });

    it("round produce el mismo proto", () => {
        sameProto(round(ncol("amount"), 2), call("round", [col("amount"), lit(2)]));
    });
});

describe("Plan-assertion: caseWhen tipado vs when().otherwise() no tipado", () => {
    it("when().when().otherwise() produce el mismo proto", () => {
        const typed = twhen(ncol("amount").gt(100), "high")
            .when(ncol("amount").gt(10), "mid")
            .otherwise("low");
        const untyped = when(col("amount").gt(100), "high")
            .when(col("amount").gt(10), "mid")
            .otherwise("low");
        sameProto(typed, untyped);
    });
});

describe("Plan-assertion: agregaciones tipadas vs call(...) no tipado", () => {
    const agg = makeAggFactory<{ amount: number; product: string }, E>();

    it("count produce el mismo proto", () => {
        sameProto(agg.count().as("n"), call("count", [lit(1)]));
    });

    it("countDistinct produce el mismo proto", () => {
        sameProto(
            agg.countDistinct("amount").as("d"),
            call("count_distinct", [col("amount")])
        );
    });

    it("sum produce el mismo proto", () => {
        sameProto(agg.sum("amount").as("total"), call("sum", [col("amount")]));
    });

    it("avg produce el mismo proto", () => {
        sameProto(agg.avg("amount").as("mean"), call("avg", [col("amount")]));
    });

    it("min / max producen el mismo proto", () => {
        sameProto(agg.min("amount").as("lo"), call("min", [col("amount")]));
        sameProto(agg.max("amount").as("hi"), call("max", [col("amount")]));
    });
});
