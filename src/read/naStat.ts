import type { LiteralValue } from "../algebra/read";
import type { NAReplacement, SampleFraction } from "../algebra/read/batch-capabilities";
import type { NAFillValue, NAHow, NAReplaceValue } from "./dataFrameFunctions";
import { assertInteger, assertNonEmptyString } from "./dataframeValidation";

/**
 * Client-side validation and normalisation for the `na` / `stat` relations.
 * Spark would reject most of these inputs only at analysis time, with a less
 * precise message, after a round trip.
 */

/** Normalises an optional column or column list into a validated list. */
export function columnList(label: string, subset: string | readonly string[] | undefined): string[] {
    if (subset === undefined) return [];
    const columns = typeof subset === "string" ? [subset] : [...subset];
    for (const column of columns) assertNonEmptyString(`${label} column`, column);
    return columns;
}

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

function assertFillValue(label: string, value: NAFillValue): void {
    const kind = typeof value;
    if (kind !== "string" && kind !== "number" && kind !== "boolean" && kind !== "bigint") {
        throw new TypeError(`${label} must be a string, number, boolean or bigint.`);
    }
    if (kind === "number" && !Number.isFinite(value)) {
        throw new RangeError(`${label} must be a finite number.`);
    }
    if (kind === "bigint" && ((value as bigint) < INT64_MIN || (value as bigint) > INT64_MAX)) {
        throw new RangeError(`${label} is outside Spark's signed int64 range.`);
    }
}

/**
 * `fillna(value, subset)` or `fillna({ column: value })`. A mapping fills each
 * named column with its own value, so it cannot be combined with `subset`.
 */
export function fillSpec(
    value: NAFillValue | Readonly<Record<string, NAFillValue>>,
    subset: string | readonly string[] | undefined,
): { columns: string[]; values: LiteralValue[] } {
    if (value !== null && typeof value === "object") {
        if (subset !== undefined) {
            throw new TypeError("fillna() takes no subset when the value is a column mapping.");
        }
        const entries = Object.entries(value);
        if (entries.length === 0) {
            throw new TypeError("fillna() requires at least one column in the mapping.");
        }
        for (const [column, fill] of entries) {
            assertNonEmptyString("fillna() column", column);
            assertFillValue(`fillna() value for ${JSON.stringify(column)}`, fill);
        }
        return { columns: entries.map(([column]) => column), values: entries.map(([, fill]) => fill) };
    }
    assertFillValue("fillna() value", value);
    return { columns: columnList("fillna()", subset), values: [value] };
}

/**
 * `min_non_nulls` for `dropna`: an explicit threshold wins; otherwise `all`
 * keeps rows with at least one value and `any` (the default) requires every
 * considered column.
 */
export function dropNaMinimum(how: NAHow | undefined, thresh: number | undefined): number | undefined {
    if (how !== undefined && how !== "any" && how !== "all") {
        throw new RangeError(`dropna() how must be "any" or "all"; received ${String(how)}.`);
    }
    if (thresh !== undefined) {
        assertInteger("dropna() thresh", thresh, 0);
        return thresh;
    }
    return how === "all" ? 1 : undefined;
}

type ReplaceKind = "numeric" | "boolean" | "string";

function replaceKind(value: NAReplaceValue): ReplaceKind | undefined {
    if (value === null) return undefined;
    switch (typeof value) {
        case "number":
            if (!Number.isFinite(value)) {
                throw new RangeError("replace() numbers must be finite.");
            }
            return "numeric";
        case "bigint":
            return "numeric";
        case "boolean":
            return "boolean";
        case "string":
            return "string";
        default:
            throw new TypeError("replace() values must be strings, numbers, booleans, bigints or null.");
    }
}

function isReplaceMapping(
    value: unknown,
): value is Readonly<Record<string, NAReplaceValue>> | ReadonlyMap<NAReplaceValue, NAReplaceValue> {
    return value instanceof Map || (value !== null && typeof value === "object" && !Array.isArray(value));
}

/**
 * Normalises the PySpark `replace` call shapes: `(mapping, subset?)`,
 * `(old, new, subset?)` and `(olds[], news[] | new, subset?)`. Like PySpark
 * it refuses to mix value kinds, because Spark picks the columns to touch from
 * the type of the replaced values.
 */
export function replaceSpec(args: readonly unknown[]): { columns: string[]; replacements: NAReplacement[] } {
    const [first, second, third] = args;
    let pairs: Array<[NAReplaceValue, NAReplaceValue]>;
    let subset: unknown;

    if (isReplaceMapping(first)) {
        pairs = first instanceof Map ? [...first.entries()] : Object.entries(first);
        subset = second;
    } else {
        if (args.length < 2) {
            throw new TypeError("replace() requires a replacement value or a mapping.");
        }
        const olds = Array.isArray(first) ? first as NAReplaceValue[] : [first as NAReplaceValue];
        const news = Array.isArray(second)
            ? second as NAReplaceValue[]
            : olds.map(() => second as NAReplaceValue);
        if (olds.length !== news.length) {
            throw new RangeError("replace() needs as many replacement values as values to replace.");
        }
        pairs = olds.map((old, index) => [old, news[index] as NAReplaceValue]);
        subset = third;
    }

    if (pairs.some(([from]) => from === null)) {
        // Spark picks the replacement column type from the replaced value, so a
        // null there fails at execution with a bare MatchError.
        throw new TypeError("replace() cannot replace null values; use fillna() instead.");
    }
    const kinds = new Set(pairs.flat().map(replaceKind).filter(kind => kind !== undefined));
    if (kinds.size > 1) {
        if (!(first instanceof Map) && isReplaceMapping(first)) {
            throw new TypeError(
                "replace() mapping keys are always strings; use a Map to replace numeric or boolean values."
            );
        }
        throw new TypeError(`replace() cannot mix ${[...kinds].join(" and ")} values.`);
    }
    return {
        columns: columnList("replace()", subset as string | readonly string[] | undefined),
        replacements: pairs.map(([from, to]) => ({ from, to })),
    };
}

/** Strata for `sampleBy`, from a string-keyed record or a literal-keyed map. */
export function sampleFractions(
    fractions: Readonly<Record<string, number>> | ReadonlyMap<LiteralValue, number>,
): SampleFraction[] {
    const entries: Array<[LiteralValue, number]> = fractions instanceof Map
        ? [...fractions.entries()]
        : Object.entries(fractions);
    if (entries.length === 0) {
        throw new TypeError("sampleBy() requires at least one stratum fraction.");
    }
    return entries.map(([stratum, fraction]) => {
        if (!Number.isFinite(fraction) || fraction < 0 || fraction > 1) {
            throw new RangeError(`sampleBy() fraction for ${String(stratum)} must be between 0 and 1.`);
        }
        return { stratum, fraction };
    });
}

/** Validates `approxQuantile` probabilities and relative error. */
export function assertQuantileArguments(probabilities: readonly number[], relativeError: number): void {
    if (probabilities.length === 0) {
        throw new TypeError("approxQuantile() requires at least one probability.");
    }
    for (const probability of probabilities) {
        if (!Number.isFinite(probability) || probability < 0 || probability > 1) {
            throw new RangeError("approxQuantile() probabilities must be between 0 and 1.");
        }
    }
    if (!Number.isFinite(relativeError) || relativeError < 0) {
        throw new RangeError("approxQuantile() relativeError must be a finite number >= 0.");
    }
}

/** Validates the `freqItems` support, which Spark requires in [1e-4, 1]. */
export function assertSupport(support: number | undefined): void {
    if (support !== undefined && !(support >= 1e-4 && support <= 1)) {
        throw new RangeError("freqItems() support must be between 1e-4 and 1.");
    }
}
