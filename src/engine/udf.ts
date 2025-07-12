// src/engine/udf.ts
import { Expression } from "./logicalPlan";

let udfCounter = 0;

export function udf(fn: (value: any) => any): (input: Expression) => Expression {
    const udfName = `udf_${udfCounter++}`;
    return (arg: Expression): Expression => ({
        type: "UDF",
        name: udfName,
        function: fn,
        argument: arg
    });
}
