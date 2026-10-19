import { SparkSession } from "../client/session";
import { ExplainModeInput } from "../engine/sparkConnectEnums";
import type { ResolvedExecution } from "../client/executionOptions";

export interface DFExec<R> {
    collect(root: R, session: SparkSession, execution?: ResolvedExecution): Promise<any[]>;
    explain(root: R, session: SparkSession, mode: ExplainModeInput): Promise<string>;
}
