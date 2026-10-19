// Sample arguments for every generated function, shared by the unit suite
// (which checks the encoded expression) and the E2E suite (which has Spark
// resolve each call).

import { ProtoExprAlg } from "../../src/engine/compilerRead";
import type { EBuilder } from "../../src/engine/column";
import * as F from "../../src/functions";
import type { CatalogFunction, CatalogParam } from "./function-catalog";

export type Proto = Record<string, any>;
export type Library = Record<string, (...args: unknown[]) => EBuilder>;
export const library = F as unknown as Library;

const SAMPLE_LITERAL: Record<Exclude<CatalogParam["kind"], "column" | "cn" | "opts">, unknown> = {
    int: 3,
    num: 0.25,
    str: "s",
    bool: true,
    any: 7,
};

/** Columns referenced by the samples: `c0` ... `c19`. */
export const SAMPLE_COLUMNS = Array.from({ length: 20 }, (_, index) => `c${index}`);

/** A distinct sample per parameter, plus the proto it must encode to. */
function sample(param: CatalogParam, index: number): { value: unknown; proto: Proto } {
    switch (param.kind) {
        case "column":
            return { value: `c${index}`, proto: { unresolved_attribute: { unparsed_identifier: `c${index}` } } };
        case "cn":
            // Exercise both shapes: a column name and a number literal.
            return index % 2 === 0
                ? { value: `c${index}`, proto: { unresolved_attribute: { unparsed_identifier: `c${index}` } } }
                : { value: 2, proto: { literal: { integer: 2 } } };
        case "opts":
            return {
                value: { mode: "PERMISSIVE" },
                proto: {
                    unresolved_function: {
                        function_name: "map",
                        arguments: [{ literal: { string: "mode" } }, { literal: { string: "PERMISSIVE" } }],
                    },
                },
            };
        default: {
            const value = SAMPLE_LITERAL[param.kind];
            return { value, proto: ProtoExprAlg.lit(value as never) };
        }
    }
}

/**
 * The arguments to call `fn` with — only the required ones, or all of them —
 * and the protos Spark Connect must receive for them, in order.
 */
export function sampleCall(fn: CatalogFunction, includeOptional: boolean): { values: unknown[]; protos: Proto[] } {
    const values: unknown[] = [];
    const protos: Proto[] = [];
    fn.params.forEach((param, index) => {
        if (param.variadic) {
            for (const offset of [0, 1]) {
                const { value, proto } = sample(param, index + offset * 10);
                values.push(value);
                protos.push(proto);
            }
            return;
        }
        if (param.optional && !includeOptional) return;
        if (param.default !== undefined && !includeOptional) {
            // Omitted defaulted parameters are still sent, with their default.
            values.push(undefined);
            protos.push(ProtoExprAlg.lit(JSON.parse(param.default.replace(/'/g, '"')) as never));
            return;
        }
        const { value, proto } = sample(param, index);
        values.push(value);
        protos.push(proto);
    });
    return { values, protos };
}

/** Calls the exported function of a catalog entry. */
export function callFunction(fn: CatalogFunction, values: unknown[]): EBuilder {
    const exported = library[fn.name];
    if (!exported) throw new Error(`functions.${fn.name} is not exported`);
    return exported(...values);
}
