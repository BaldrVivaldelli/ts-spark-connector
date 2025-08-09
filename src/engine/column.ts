import {Expression} from "./logicalPlan";

export class Column {
    constructor(public expr: Expression) {
    }

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

    alias(name: string): Column {
        return new Column({
            type: "Alias",
            input: this.expr,
            alias: name,
        });
    }

    asc(): Column {
        return new Column({type: "SortKey", input: this.expr, direction: "asc"});
    }

    desc(): Column {
        return new Column({type: "SortKey", input: this.expr, direction: "desc"});
    }

    ascNullsFirst(): Column {
        return new Column({type: "SortKey", input: this.expr, direction: "asc", nulls: "nullsFirst"});
    }

    ascNullsLast(): Column {
        return new Column({type: "SortKey", input: this.expr, direction: "asc", nulls: "nullsLast"});
    }

    descNullsFirst(): Column {
        return new Column({type: "SortKey", input: this.expr, direction: "desc", nulls: "nullsFirst"});
    }

    descNullsLast(): Column {
        return new Column({type: "SortKey", input: this.expr, direction: "desc", nulls: "nullsLast"});
    }

}

function literal(value: any): Expression {
    return {type: "Literal", value};
}

export function col(name: string): Column {
    return new Column({type: "Column", name});
}

export function lit(value: any): Column {
    return new Column(literal(value));
}

export function when(condition: Column, value: any) {
    const pairs: Expression[] = [condition.expr, literal(value)];

    const chain = {
        when(cond: Column, val: any) {
            pairs.push(cond.expr, literal(val));
            return chain;
        },
        otherwise(val: any): Column {
            let elseExpr: Expression = literal(val);
            for (let i = pairs.length - 2; i >= 0; i -= 2) {
                const cond = pairs[i];
                const thenVal = pairs[i + 1];
                elseExpr = {
                    type: "UnresolvedFunction",
                    name: "if",
                    args: [cond, thenVal, elseExpr],
                };
            }
            return new Column(elseExpr);
        },
    };

    return chain;
}