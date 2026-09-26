// Validation helpers for Spark multipart identifiers carried in the Connect
// NamedTable message. Spark accepts qualified names (catalog.schema.table) and
// backtick-quoted identifiers. We only accept either:
//   - one or more dot-separated unquoted identifiers ([A-Za-z_][A-Za-z0-9_]*), or
//   - backtick-quoted identifiers, where an embedded backtick is escaped as ``.

const UNQUOTED_PART = /^[A-Za-z_][A-Za-z0-9_]*$/;
// A backtick-quoted part: opening/closing backticks with any chars inside,
// where literal backticks are doubled (Spark's escaping rule).
const QUOTED_PART = /^`(?:[^`]|``)*`$/;

function isValidPart(part: string): boolean {
    return UNQUOTED_PART.test(part) || QUOTED_PART.test(part);
}

/** Splits on qualifier dots while preserving dots inside quoted identifiers. */
function splitIdentifierParts(name: string): string[] | undefined {
    const parts: string[] = [];
    let part = "";
    let quoted = false;

    for (let index = 0; index < name.length; index += 1) {
        const char = name[index];
        if (char === "`") {
            part += char;
            if (quoted && name[index + 1] === "`") {
                part += "`";
                index += 1;
            } else {
                quoted = !quoted;
            }
            continue;
        }

        if (char === "." && !quoted) {
            if (!part) return undefined;
            parts.push(part);
            part = "";
            continue;
        }

        part += char;
    }

    if (quoted || !part) return undefined;
    parts.push(part);
    return parts;
}

/**
 * Validates a (possibly qualified) table/view identifier and returns it
 * unchanged. Dots inside a quoted identifier are data, while dots outside
 * backticks delimit qualifiers.
 */
export function validateTableIdentifier(name: string): string {
    if (typeof name !== "string" || name.trim().length === 0) {
        throw new Error("table(): identifier must be a non-empty string.");
    }

    const parts = splitIdentifierParts(name);
    if (!parts || !parts.every(isValidPart)) {
        throw new Error(
            `table(): invalid identifier "${name}". ` +
            "Expected a qualified name like `db.table` (use backticks for special characters)."
        );
    }

    return name;
}
