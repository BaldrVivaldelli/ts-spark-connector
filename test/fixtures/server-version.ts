import { expect } from "vitest";
import type { SparkSession } from "../../src";
import { compareSparkVersions } from "../../src/client/serverVersion";

/** Whether the E2E server runs Spark `minimum` or newer. */
export async function serverAtLeast(session: SparkSession, minimum: string): Promise<boolean> {
    return compareSparkVersions(await session.version(), minimum) >= 0;
}

/**
 * True when the server supports `feature`. On an older server it asserts that
 * `attempt` is refused with UnsupportedServerVersionError before reaching
 * Spark, and returns false so the caller skips the behavioural checks.
 */
export async function supportedOrRefused(
    session: SparkSession,
    minimum: string,
    feature: string,
    attempt: () => Promise<unknown>,
): Promise<boolean> {
    if (await serverAtLeast(session, minimum)) return true;
    await expect(attempt()).rejects.toMatchObject({
        name: "UnsupportedServerVersionError",
        feature,
        requiredVersion: minimum,
    });
    return false;
}

/** Functions whose optional arguments Spark added in 4.0 (`mode(col, deterministic)`). */
export const OPTIONAL_ARGUMENTS_ADDED_IN_SPARK_4 = new Set(["mode"]);

/**
 * Functions the library exposes that Spark added in 4.0: a 3.5 server answers
 * them with UNRESOLVED_ROUTINE.
 */
export const FUNCTIONS_ADDED_IN_SPARK_4 = new Set([
    "collate", "collation", "dayname", "from_xml", "is_valid_utf8", "is_variant_null", "listagg",
    "make_valid_utf8", "monthname", "nullifzero", "parse_json", "randstr", "schema_of_variant",
    "schema_of_variant_agg", "schema_of_xml", "session_user", "string_agg", "to_variant_object",
    "to_xml", "try_make_interval", "try_make_timestamp", "try_make_timestamp_ltz",
    "try_make_timestamp_ntz", "try_mod", "try_parse_json", "try_parse_url", "try_reflect",
    "try_url_decode", "try_validate_utf8", "try_variant_get", "uniform", "validate_utf8",
    "variant_get", "zeroifnull",
]);
