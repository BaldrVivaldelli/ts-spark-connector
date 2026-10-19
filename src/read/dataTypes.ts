/**
 * Decoding of Spark Connect `DataType` messages (AnalyzePlan `schema`) into
 * Spark's own `simpleString` notation (`int`, `array<string>`,
 * `struct<a:int,b:string>`, `decimal(10,2)`, ...).
 */

/**
 * One column of a DataFrame schema as resolved by Spark.
 * @public
 */
export interface SparkStructField {
    /** Column name. */
    readonly name: string;
    /** Spark's `simpleString` of the field type, e.g. `bigint` or `array<string>`. */
    readonly dataType: string;
    /** Whether Spark allows nulls in the column. */
    readonly nullable: boolean;
}

/**
 * A DataFrame schema as resolved by Spark (`df.schema()`).
 * @public
 */
export interface SparkStructType {
    /** Columns, in order. */
    readonly fields: readonly SparkStructField[];
}

type ProtoDataType = Record<string, unknown>;

const SIMPLE_KINDS: Record<string, string> = {
    null: "void",
    binary: "binary",
    boolean: "boolean",
    byte: "tinyint",
    short: "smallint",
    integer: "int",
    long: "bigint",
    float: "float",
    double: "double",
    date: "date",
    timestamp: "timestamp",
    timestamp_ntz: "timestamp_ntz",
    calendar_interval: "interval",
    variant: "variant",
};

const YEAR_MONTH_FIELDS = ["year", "month"];
const DAY_TIME_FIELDS = ["day", "hour", "minute", "second"];

const COMPLEX_KINDS = [
    "decimal",
    "string",
    "char",
    "var_char",
    "year_month_interval",
    "day_time_interval",
    "array",
    "struct",
    "map",
    "udt",
    "unparsed",
];

function asMessage(value: unknown, label: string): ProtoDataType {
    if (value === null || typeof value !== "object") {
        throw new TypeError(`Invalid Spark DataType: ${label} is missing.`);
    }
    return value as ProtoDataType;
}

/** The populated `kind` oneof member, preferring proto-loader's virtual field. */
function kindOf(dataType: ProtoDataType): string {
    if (typeof dataType.kind === "string") return dataType.kind;
    const present = [...Object.keys(SIMPLE_KINDS), ...COMPLEX_KINDS].find(
        key => dataType[key] !== null && typeof dataType[key] === "object",
    );
    if (!present) throw new TypeError("Invalid Spark DataType: no type kind is set.");
    return present;
}

function intervalName(message: ProtoDataType, names: readonly string[], defaultEnd: number): string {
    const start = typeof message.start_field === "number" ? message.start_field : 0;
    const end = typeof message.end_field === "number" ? message.end_field : defaultEnd;
    const startName = names[start] ?? String(start);
    const endName = names[end] ?? String(end);
    return start === end ? `interval ${startName}` : `interval ${startName} to ${endName}`;
}

/** Spark's `simpleString` for a Spark Connect `DataType` message. */
export function sparkDataTypeString(input: unknown): string {
    const dataType = asMessage(input, "data type");
    const kind = kindOf(dataType);
    const simple = SIMPLE_KINDS[kind];
    if (simple) return simple;

    const message = asMessage(dataType[kind], kind);
    switch (kind) {
        case "decimal": {
            const precision = typeof message.precision === "number" ? message.precision : 10;
            const scale = typeof message.scale === "number" ? message.scale : 0;
            return `decimal(${precision},${scale})`;
        }
        case "string": {
            const collation = typeof message.collation === "string" ? message.collation : "";
            return collation && collation.toUpperCase() !== "UTF8_BINARY"
                ? `string collate ${collation}`
                : "string";
        }
        case "char":
            return `char(${String(message.length)})`;
        case "var_char":
            return `varchar(${String(message.length)})`;
        case "year_month_interval":
            return intervalName(message, YEAR_MONTH_FIELDS, 1);
        case "day_time_interval":
            return intervalName(message, DAY_TIME_FIELDS, 3);
        case "array":
            return `array<${sparkDataTypeString(message.element_type)}>`;
        case "map":
            return `map<${sparkDataTypeString(message.key_type)},${sparkDataTypeString(message.value_type)}>`;
        case "struct":
            return `struct<${structFields(message).map(field => `${field.name}:${field.dataType}`).join(",")}>`;
        case "udt":
            return message.sql_type ? sparkDataTypeString(message.sql_type) : String(message.type);
        case "unparsed":
            return String(message.data_type_string);
        default:
            throw new TypeError(`Unsupported Spark DataType kind: ${kind}.`);
    }
}

function structFields(struct: ProtoDataType): SparkStructField[] {
    const fields = Array.isArray(struct.fields) ? struct.fields as unknown[] : [];
    return fields.map(raw => {
        const field = asMessage(raw, "struct field");
        return {
            name: String(field.name),
            dataType: sparkDataTypeString(field.data_type),
            nullable: field.nullable === true,
        };
    });
}

/** Decodes the top-level row type of a DataFrame schema. */
export function sparkStructType(input: unknown): SparkStructType {
    const dataType = asMessage(input, "schema");
    if (kindOf(dataType) !== "struct") {
        throw new TypeError(`Expected a struct schema but Spark returned ${sparkDataTypeString(dataType)}.`);
    }
    return { fields: structFields(asMessage(dataType.struct, "struct")) };
}
