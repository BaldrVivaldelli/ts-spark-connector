import { DataFrameDSLFactory} from "./dataframe";
import {Column} from "../engine/column";
import {LogicalPlan} from "../engine/logicalPlan";
import {dataframeInterpreter} from "./dataFrameInterpreter";
import {SparkSession} from "./session";
export class ChainedDataFrame {
    constructor(
        private plan: LogicalPlan,
        private session: SparkSession
    ) {}

    private get dsl(): DataFrameDSLFactory<LogicalPlan> {
        return dataframeInterpreter(this.getPlan(), this.getSession());
    }
    private wrap(plan: LogicalPlan,sparkSession:SparkSession): ChainedDataFrame {
        return new ChainedDataFrame(plan, sparkSession);
    }
    select(...cols: (string | Column)[]): ChainedDataFrame {
        const nextPlan = this.dsl.select(this.getPlan(),cols);
        return this.wrap(nextPlan,this.getSession());
    }


    join(right: ChainedDataFrame, condition: Column): ChainedDataFrame {
        const nextPlan = this.dsl.join(this.plan, right.plan, condition);
        return this.wrap(nextPlan,this.getSession());
    }

    filter(condition: Column): ChainedDataFrame {
        const nextPlan = this.dsl.filter(this.getPlan(),condition);
        return this.wrap(nextPlan,this.getSession());
    }

    withColumn(name: string, column: Column): ChainedDataFrame {
        const nextPlan = this.dsl.withColumn(this.getPlan(),name, column);
        return this.wrap(nextPlan,this.getSession());
    }

    groupBy(...columns: (string | Column)[]) {
        const grouped = this.dsl.groupBy(this.getPlan(), columns);
        return {
            agg: (aggregations: Record<string, string>) => {
                const plan = grouped.agg(aggregations);
                return this.wrap(plan,this.getSession());
            }
        };
    }

    orderBy(...columns: (string | Column)[]): ChainedDataFrame {
        const nextPlan = this.dsl.orderBy(this.getPlan(), columns);
        return this.wrap(nextPlan, this.getSession());
    }

    sort(...columns: (string | Column)[]): ChainedDataFrame {
        const nextPlan = this.dsl.sort(this.getPlan(), columns);
        return this.wrap(nextPlan, this.getSession());
    }


    union(right: ChainedDataFrame): ChainedDataFrame {
        const nextPlan = this.dsl.union(this.getPlan(), right.getPlan());
        return this.wrap(nextPlan, this.getSession());
    }

    dropDuplicates(...columns: (string | Column)[]): ChainedDataFrame {
        const nextPlan = this.dsl.dropDuplicates(this.getPlan(), columns.length ? columns : undefined);
        return this.wrap(nextPlan, this.getSession());
    }
    withColumnRenamed(oldName: string, newName: string): ChainedDataFrame {
        const nextPlan = this.dsl.withColumnRenamed(this.getPlan(), oldName, newName);
        return this.wrap(nextPlan, this.getSession());
    }

    withColumnsRenamed(mapping: Record<string, string>): ChainedDataFrame {
        const nextPlan = this.dsl.withColumnsRenamed(this.getPlan(), mapping);
        return this.wrap(nextPlan, this.getSession());
    }
    unionByName(right: ChainedDataFrame, allowMissingColumns = false): ChainedDataFrame {
        const nextPlan = this.dsl.union(this.getPlan(), right.getPlan(), {
            byName: true,
            allowMissingColumns,
        });
        return this.wrap(nextPlan, this.getSession());
    }


    distinct(): ChainedDataFrame {
        const nextPlan = this.dsl.distinct(this.getPlan());
        return this.wrap(nextPlan, this.getSession());
    }

    limit(n: number): ChainedDataFrame {
        const nextPlan = this.dsl.limit(this.getPlan(), n);
        return this.wrap(nextPlan, this.getSession());
    }


    show() {
        this.dsl.show(this.getPlan());
    }

    async collect(){
        return await this.dsl.collect(this.getPlan());
    }

    getSession(){
        return this.session;
    }
    getPlan() {
        return this.plan;
    }
}
