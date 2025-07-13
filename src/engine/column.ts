import { Expression } from "./logicalPlan";

export class Column {
    constructor(public expr: Expression) {}

    eq(value: any): Column {
        return new Column({
            type: "Binary",
            op: "=",
            left: this.expr,
            right: literal(value),
        });
    }

    gt(value: any): Column {
        return new Column({
            type: "Binary",
            op: ">",
            left: this.expr,
            right: literal(value),
        });
    }

    gte(value: any): Column {
        return new Column({
            type: "Binary",
            op: ">=",
            left: this.expr,
            right: literal(value),
        });
    }

    lt(value: any): Column {
        return new Column({
            type: "Binary",
            op: "<",
            left: this.expr,
            right: literal(value),
        });
    }

    lte(value: any): Column {
        return new Column({
            type: "Binary",
            op: "<=",
            left: this.expr,
            right: literal(value),
        });
    }

    and(other: Column): Column {
        return new Column({
            type: "Logical",
            op: "AND",
            left: this.expr,
            right: other.expr,
        });
    }

    or(other: Column): Column {
        return new Column({
            type: "Logical",
            op: "OR",
            left: this.expr,
            right: other.expr,
        });
    }

    // Alias
    alias(name: string): Column {
        return new Column({
            type: "Alias",
            input: this.expr,
            alias: name,
        });
    }
}

function literal(value: any): Expression {
    return { type: "Literal", value };
}

export function col(name: string): Column {
    return new Column({ type: "Column", name });
}
