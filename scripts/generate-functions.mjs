// Generates src/functions/generated.ts (and the unit-test fixture describing
// it) from scripts/functions/catalog.mjs.
//
//   node scripts/generate-functions.mjs           rewrite the generated files
//   node scripts/generate-functions.mjs --check   fail when they are stale

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { SECTIONS } from "./functions/catalog.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const SOURCE = join(root, "src", "functions", "generated.ts");
const FIXTURE = join(root, "test", "fixtures", "function-catalog.ts");

// Exported by src/functions/special.ts; the catalog must not redefine them.
const HAND_WRITTEN = new Set([
    "col", "column", "lit", "expr", "when", "count", "count_distinct", "sum_distinct", "log",
    "transform", "filter", "exists", "forall", "aggregate", "reduce", "zip_with", "array_sort",
    "map_filter", "transform_keys", "transform_values", "map_zip_with", "window",
    "asc", "desc", "asc_nulls_first", "asc_nulls_last", "desc_nulls_first", "desc_nulls_last",
    "trim", "ltrim", "rtrim",
]);

// Names that cannot be function identifiers; they are exported under their
// Spark name through an alias.
const RESERVED = new Set(["typeof"]);

const KINDS = {
    column: { type: "ColumnOrName", coerce: "c" },
    int: { type: "number | EBuilder", coerce: "l" },
    num: { type: "number | EBuilder", coerce: "l" },
    str: { type: "string | EBuilder", coerce: "l" },
    bool: { type: "boolean | EBuilder", coerce: "l" },
    any: { type: "LiteralValue | EBuilder", coerce: "l" },
    cn: { type: "ColumnOrNumber", coerce: "cn" },
    opts: { type: "Readonly<Record<string, string>>", coerce: "opts" },
};

const SIGNATURE = /^([a-z_][a-z0-9_]*)\((.*)\)$/;
const PARAM = /^(\.\.\.)?([A-Za-z_][A-Za-z0-9_]*)(?::([a-z]+))?(\?|=(.+))?$/;

function parseSignature(signature) {
    const match = SIGNATURE.exec(signature);
    if (!match) throw new Error(`Invalid signature: ${signature}`);
    const [, name, list] = match;
    const params = list.trim() === "" ? [] : list.split(",").map(raw => {
        const token = raw.trim();
        const parsed = PARAM.exec(token);
        if (!parsed) throw new Error(`Invalid parameter "${token}" in ${signature}`);
        const [, rest, paramName, kind = "column", suffix, defaultValue] = parsed;
        if (!(kind in KINDS)) throw new Error(`Unknown kind "${kind}" in ${signature}`);
        return {
            name: paramName,
            kind,
            variadic: rest === "...",
            optional: suffix === "?",
            ...(defaultValue === undefined ? {} : { default: defaultValue }),
        };
    });
    return { name, params };
}

function parseCatalog() {
    const seen = new Set();
    return SECTIONS.map(section => ({
        title: section.title,
        functions: section.functions.map(([signature, doc, extra = {}]) => {
            const { name, params } = parseSignature(signature);
            if (seen.has(name)) throw new Error(`Duplicate function ${name}`);
            if (HAND_WRITTEN.has(name)) throw new Error(`${name} is hand-written in special.ts`);
            seen.add(name);
            params.forEach((param, index) => {
                const later = params.slice(index + 1);
                if (param.variadic && later.length > 0) {
                    throw new Error(`${name}: the variadic parameter must be last`);
                }
                if (param.optional && later.some(next => !next.optional)) {
                    throw new Error(`${name}: optional parameters must be trailing`);
                }
                if (param.kind === "opts" && !param.optional) {
                    throw new Error(`${name}: options are always optional`);
                }
            });
            return { name, sql: extra.sql ?? name, doc, params };
        }),
    }));
}

function tsParam(param) {
    const { type } = KINDS[param.kind];
    if (param.variadic) {
        return `...${param.name}: ${type.includes("|") ? `Array<${type}>` : `${type}[]`}`;
    }
    if (param.default !== undefined) return `${param.name}: ${type} = ${param.default}`;
    return `${param.name}${param.optional ? "?" : ""}: ${type}`;
}

function renderFunction(fn) {
    const identifier = RESERVED.has(fn.name) ? `${fn.name}_` : fn.name;
    const required = [];
    const optional = [];
    for (const param of fn.params) {
        const { coerce } = KINDS[param.kind];
        if (param.variadic) required.push(`...${param.name}.map(${coerce})`);
        else if (param.optional) {
            optional.push(coerce === "opts" ? `opts(${param.name})` : `maybe(${param.name}, ${coerce})`);
        } else required.push(`${coerce}(${param.name})`);
    }
    const args = [`"${fn.sql}"`, `[${required.join(", ")}]`];
    if (optional.length > 0) args.push(`[${optional.join(", ")}]`);
    const exported = RESERVED.has(fn.name) ? "" : "export ";
    return [
        "/**",
        ` * ${fn.doc}`,
        " *",
        ` * Spark SQL: \`${fn.sql}\`.`,
        " * @public",
        " */",
        `${exported}function ${identifier}(${fn.params.map(tsParam).join(", ")}): EBuilder {`,
        `    return invoke(${args.join(", ")});`,
        "}",
    ].join("\n");
}

function renderSource(catalog) {
    const aliases = catalog
        .flatMap(section => section.functions)
        .filter(fn => RESERVED.has(fn.name))
        .map(fn => `export { ${fn.name}_ as ${fn.name} };`);
    const blocks = catalog.map(section => [
        `// ${section.title} ${"-".repeat(Math.max(4, 74 - section.title.length))}`,
        ...section.functions.map(renderFunction),
    ].join("\n\n"));
    return [
        "// Generated by scripts/generate-functions.mjs from scripts/functions/catalog.mjs.",
        "// Do not edit: change the catalog and run `npm run functions:generate`.",
        "",
        'import type { LiteralValue } from "../algebra/read";',
        'import type { EBuilder } from "../engine/column";',
        'import { c, cn, invoke, l, maybe, opts, type ColumnOrName, type ColumnOrNumber } from "./args";',
        "",
        blocks.join("\n\n"),
        "",
        ...aliases,
        "",
    ].join("\n");
}

function renderFixture(catalog) {
    const entries = catalog.flatMap(section => section.functions).map(fn => JSON.stringify({
        name: fn.name,
        sql: fn.sql,
        params: fn.params,
    }));
    return [
        "// Generated by scripts/generate-functions.mjs from scripts/functions/catalog.mjs.",
        "// Describes every generated function so the unit tests can call each one.",
        "",
        "export type CatalogParam = {",
        "    name: string;",
        '    kind: "column" | "int" | "num" | "str" | "bool" | "any" | "cn" | "opts";',
        "    variadic: boolean;",
        "    optional: boolean;",
        "    default?: string;",
        "};",
        "",
        "export type CatalogFunction = { name: string; sql: string; params: CatalogParam[] };",
        "",
        "export const FUNCTION_CATALOG: CatalogFunction[] = [",
        ...entries.map(entry => `    ${entry},`),
        "];",
        "",
    ].join("\n");
}

const catalog = parseCatalog();
const outputs = [[SOURCE, renderSource(catalog)], [FIXTURE, renderFixture(catalog)]];

if (process.argv.includes("--check")) {
    const stale = outputs.filter(([file, content]) => {
        try {
            return readFileSync(file, "utf8").replace(/\r\n/g, "\n") !== content;
        } catch {
            return true;
        }
    });
    if (stale.length > 0) {
        process.stderr.write(`Stale generated files: ${stale.map(([file]) => file).join(", ")}\n`);
        process.stderr.write("Run `npm run functions:generate` and commit the result.\n");
        process.exit(1);
    }
    process.stdout.write(`Function library is up to date (${catalog.flatMap(s => s.functions).length} generated functions).\n`);
} else {
    for (const [file, content] of outputs) writeFileSync(file, content);
    process.stdout.write(`Generated ${catalog.flatMap(s => s.functions).length} functions.\n`);
}
