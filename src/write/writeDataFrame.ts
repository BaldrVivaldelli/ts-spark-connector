import type { SparkSession } from "../client/session";
import {StreamingQueryHandle} from "../client/sparkClient";

export interface DFWritingExec<W> {
    run(root: W, session: SparkSession): Promise<void>;
    runStream(root: W, session: SparkSession): Promise<StreamingQueryHandle>;
}