import type { LiteralValue } from "../algebra/read";
import type { UnknownSchema } from "../schema/schema-model";
import type {
    ColumnName,
    EBuilder,
    ReadChainedDataFrame,
} from "./readChainedDataFrame";

/**
 * Value accepted by `fillna` / `na.fill`. Spark fills numeric columns with a
 * number (integers travel as longs), string columns with a string and boolean
 * columns with a boolean; columns of another type are left untouched.
 * @public
 */
export type NAFillValue = string | number | boolean | bigint;

/**
 * Value accepted by `replace` / `na.replace`. Numbers are compared as doubles,
 * as in Spark; `null` may appear on either side.
 * @public
 */
export type NAReplaceValue = string | number | boolean | bigint | null;

/**
 * `dropna` mode: `any` drops a row with any null in the considered columns,
 * `all` only rows where every considered column is null.
 * @public
 */
export type NAHow = "any" | "all";

/**
 * Mirrors PySpark's `df.na`: every method delegates to the DataFrame method of
 * the same meaning (`fillna`, `dropna`, `replace`).
 * @public
 */
export class DataFrameNaFunctions<S, R, E, G, CDF, CEX> {
    /** @internal Obtained from `df.na`. */
    constructor(private readonly df: ReadChainedDataFrame<S, R, E, G, CDF, CEX>) {}

    /** See `ReadChainedDataFrame.fillna`. */
    fill(
        value: NAFillValue | Readonly<Record<string, NAFillValue>>,
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.df.fillna(value, subset);
    }

    /** See `ReadChainedDataFrame.dropna`. */
    drop(
        how?: NAHow,
        thresh?: number,
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.df.dropna(how, thresh, subset);
    }

    /** See `ReadChainedDataFrame.replace`. */
    replace(
        toReplace: NAReplaceValue | readonly NAReplaceValue[],
        value: NAReplaceValue | readonly NAReplaceValue[],
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    replace(
        mapping: Readonly<Record<string, NAReplaceValue>> | ReadonlyMap<NAReplaceValue, NAReplaceValue>,
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    replace(...args: unknown[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return (this.df.replace as (...input: unknown[]) => ReadChainedDataFrame<S, R, E, G, CDF, CEX>)
            .apply(this.df, args);
    }
}

/**
 * Mirrors PySpark's `df.stat`: every method delegates to the DataFrame method
 * of the same name.
 * @public
 */
export class DataFrameStatFunctions<S, R, E, G, CDF, CEX> {
    /** @internal Obtained from `df.stat`. */
    constructor(private readonly df: ReadChainedDataFrame<S, R, E, G, CDF, CEX>) {}

    /** See `ReadChainedDataFrame.corr`. */
    corr(col1: ColumnName<S>, col2: ColumnName<S>, method?: "pearson"): Promise<number> {
        return this.df.corr(col1, col2, method);
    }

    /** See `ReadChainedDataFrame.cov`. */
    cov(col1: ColumnName<S>, col2: ColumnName<S>): Promise<number> {
        return this.df.cov(col1, col2);
    }

    /** See `ReadChainedDataFrame.crosstab`. */
    crosstab(col1: ColumnName<S>, col2: ColumnName<S>): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        return this.df.crosstab(col1, col2);
    }

    /** See `ReadChainedDataFrame.freqItems`. */
    freqItems(
        cols: readonly ColumnName<S>[],
        support?: number,
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        return this.df.freqItems(cols, support);
    }

    /** See `ReadChainedDataFrame.approxQuantile`. */
    approxQuantile(col: ColumnName<S>, probabilities: readonly number[], relativeError: number): Promise<number[]>;
    approxQuantile(
        cols: readonly ColumnName<S>[],
        probabilities: readonly number[],
        relativeError: number,
    ): Promise<number[][]>;
    approxQuantile(
        cols: ColumnName<S> | readonly ColumnName<S>[],
        probabilities: readonly number[],
        relativeError: number,
    ): Promise<number[] | number[][]> {
        return (this.df.approxQuantile as (
            input: ColumnName<S> | readonly ColumnName<S>[],
            p: readonly number[],
            e: number,
        ) => Promise<number[] | number[][]>).call(this.df, cols, probabilities, relativeError);
    }

    /** See `ReadChainedDataFrame.sampleBy`. */
    sampleBy(
        col: ColumnName<S> | EBuilder,
        fractions: Readonly<Record<string, number>> | ReadonlyMap<LiteralValue, number>,
        seed?: number,
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.df.sampleBy(col, fractions, seed);
    }
}
