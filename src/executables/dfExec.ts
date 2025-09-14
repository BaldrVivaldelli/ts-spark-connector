import { SparkSession } from "../client/session";
import { ExplainModeInput } from "../engine/sparkConnectEnums";


export interface DFExec<R> {
    collect(root: R, session: SparkSession): Promise<any[]>;
    explain(root: R, session: SparkSession, mode: ExplainModeInput): Promise<any[]>;
}




