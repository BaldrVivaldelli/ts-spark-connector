import type { SparkSession } from "../client/session";

export interface DFWritingExec<W> {
    run(root: W, session: SparkSession): Promise<void>;
}