import { Expression } from "./logicalPlan";

export class Column {
    constructor(public expr: Expression) {}

    gt(value: number): Column {
        return new Column({
            type: "Binary",
            op: ">",
            left: this.expr,
            right: { type: "Literal", value }
        });
    }
}

export function col(name: string): Column {
    return new Column({ type: "Column", name });
}
