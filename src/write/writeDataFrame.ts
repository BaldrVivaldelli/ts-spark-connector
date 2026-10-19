import type { SparkSession } from "../client/session";
import type { ResolvedExecution } from "../client/executionOptions";
import {StreamingQueryHandle} from "../client/sparkClient";

/** @internal */
export interface DFWritingExec<W> {
    /** `execution` carries per-run controls such as the DataFrame's observations. */
    run(root: W, session: SparkSession, execution?: ResolvedExecution): Promise<void>;
    runStream(root: W, session: SparkSession): Promise<StreamingQueryHandle>;
}