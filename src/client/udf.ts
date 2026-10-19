import type { SparkSession } from "./session";
import { SparkConnectExecutor } from "./sparkConnectExecutor";
import { unparsedDataType } from "../engine/compilerRead";
import { fieldSpecToDDL, type FieldSpec } from "../schema/schema";

function requireIdentifier(value: unknown, label: string): string {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${label} must be a non-empty string.`);
    }
    return value;
}

/**
 * Registers user-defined functions implemented on the server's classpath,
 * mirroring `spark.udf`. JavaScript functions cannot run inside Spark; use
 * `session.addArtifact()` to ship a jar first.
 * @public
 */
export class SparkUdfRegistration {
    /** @internal Obtained from `session.udf`. */
    constructor(private readonly session: SparkSession) {}

    /**
     * Registers a Java UDF class (`org.apache.spark.sql.api.java.UDF0`..`UDF22`)
     * under `name`, like PySpark's `spark.udf.registerJavaFunction`. The
     * return type is a schema token or spec (`"string"`, `arrayType("int")`)
     * or a Spark type string; when omitted, Spark infers it from the class.
     */
    async registerJavaFunction(name: string, className: string, returnType?: FieldSpec | string): Promise<void> {
        await this.register(name, className, {
            ...(returnType !== undefined ? { output_type: unparsedDataType(returnTypeString(returnType)) } : {}),
            aggregate: false,
        });
    }

    /** Registers a Java UDAF class (`org.apache.spark.sql.expressions.UserDefinedAggregateFunction`). */
    async registerJavaUDAF(name: string, className: string): Promise<void> {
        await this.register(name, className, { aggregate: true });
    }

    private async register(name: string, className: string, javaUdf: Record<string, unknown>): Promise<void> {
        await SparkConnectExecutor.for(this.session).runCommand({
            register_function: {
                function_name: requireIdentifier(name, "UDF name"),
                deterministic: true,
                arguments: [],
                java_udf: {
                    class_name: requireIdentifier(className, "UDF class name"),
                    ...javaUdf,
                },
            },
        });
    }
}

function returnTypeString(returnType: FieldSpec | string): string {
    if (typeof returnType === "string") {
        return requireIdentifier(returnType, "UDF return type");
    }
    return fieldSpecToDDL(returnType);
}
