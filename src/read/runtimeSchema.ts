import type { JoinTypeInput } from "../engine/sparkConnectEnums";
import type { FieldSpec, SchemaDef } from "../schema/schema";
import { assertNonEmptyString } from "./dataframeValidation";

export function schemaDefsEqual(left?: SchemaDef, right?: SchemaDef): boolean {
    if (!left || !right) return left === right;
    return JSON.stringify(left) === JSON.stringify(right);
}

function nullableFieldSpec(spec: FieldSpec): FieldSpec {
    if (typeof spec === "string") {
        return spec.endsWith("?") ? spec : `${spec}?` as FieldSpec;
    }
    return { ...spec, nullable: true } as FieldSpec;
}

export function joinRuntimeSchema(
    left: SchemaDef | undefined,
    right: SchemaDef | undefined,
    joinType: JoinTypeInput,
): SchemaDef | undefined {
    if (!left || !right) return undefined;
    const normalized = String(joinType).toUpperCase();
    if (normalized === "LEFT_SEMI" || normalized === "LEFT_ANTI") return left;

    // Row objects cannot represent duplicate output names. Keep the descriptor
    // absent and let the Arrow boundary report a precise duplicate-field error.
    if (Object.keys(left).some(name => Object.prototype.hasOwnProperty.call(right, name))) {
        return undefined;
    }

    const leftNullable = normalized === "RIGHT" || normalized === "RIGHT_OUTER" ||
        normalized === "OUTER" || normalized === "FULL" || normalized === "FULL_OUTER";
    const rightNullable = normalized === "LEFT" || normalized === "LEFT_OUTER" ||
        normalized === "OUTER" || normalized === "FULL" || normalized === "FULL_OUTER";

    return {
        ...Object.fromEntries(Object.entries(left).map(([name, spec]) => [
            name,
            leftNullable ? nullableFieldSpec(spec) : spec,
        ])),
        ...Object.fromEntries(Object.entries(right).map(([name, spec]) => [
            name,
            rightNullable ? nullableFieldSpec(spec) : spec,
        ])),
    };
}

export function selectRuntimeSchema(
    schema: SchemaDef | undefined,
    names: readonly string[],
): SchemaDef | undefined {
    if (!schema) return undefined;
    const selected: Record<string, FieldSpec> = {};
    for (const name of names) {
        if (!Object.prototype.hasOwnProperty.call(schema, name) ||
            Object.prototype.hasOwnProperty.call(selected, name)) {
            return undefined;
        }
        selected[name] = schema[name]!;
    }
    return selected;
}

export function dropRuntimeSchema(
    schema: SchemaDef | undefined,
    names: readonly string[],
): SchemaDef | undefined {
    if (!schema) return undefined;
    const removed = new Set(names);
    return Object.fromEntries(Object.entries(schema).filter(([name]) => !removed.has(name)));
}

export function statisticsRuntimeSchema(
    schema: SchemaDef | undefined,
    names: readonly string[],
): SchemaDef | undefined {
    if (!selectRuntimeSchema(schema, names)) return undefined;
    return {
        summary: "string",
        ...Object.fromEntries(names.map(name => [name, "string?"] as const)),
    };
}

export function assertRenameMapping(mapping: Record<string, string>): void {
    const targets = new Set<string>();
    for (const [source, target] of Object.entries(mapping)) {
        assertNonEmptyString("rename source", source);
        assertNonEmptyString("rename target", target);
        if (targets.has(target)) {
            throw new TypeError(`Multiple columns cannot be renamed to ${JSON.stringify(target)}.`);
        }
        targets.add(target);
    }
}

export function assertUniqueColumnNames(label: string, names: readonly string[]): void {
    const seen = new Set<string>();
    for (const name of names) {
        if (seen.has(name)) {
            throw new TypeError(`${label} does not allow duplicate column ${JSON.stringify(name)}.`);
        }
        seen.add(name);
    }
}

export function renameRuntimeSchema(
    schema: SchemaDef | undefined,
    mapping: Record<string, string>,
): SchemaDef | undefined {
    if (!schema) return undefined;
    for (const source of Object.keys(mapping)) {
        if (!Object.prototype.hasOwnProperty.call(schema, source)) {
            throw new TypeError(`Cannot rename missing column ${JSON.stringify(source)}.`);
        }
    }
    const renamed: Record<string, FieldSpec> = {};
    for (const [name, spec] of Object.entries(schema)) {
        const target = mapping[name] ?? name;
        if (Object.prototype.hasOwnProperty.call(renamed, target)) {
            throw new TypeError(
                `Rename would create duplicate column ${JSON.stringify(target)}.`
            );
        }
        renamed[target] = spec;
    }
    return renamed;
}
