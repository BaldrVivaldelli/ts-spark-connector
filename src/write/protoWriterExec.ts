// src/engine/writer/protoWriterExec.ts

import {DFWritingExec} from "./writeDataFrame";
import {ProtoWriteRoot} from "./compilerWrite";
import {SparkSession} from "../client/session";
import {SparkConnectExecutor} from "../client/sparkConnectExecutor";

export const ProtoWritingExec: DFWritingExec<ProtoWriteRoot> = {
    async run(root: ProtoWriteRoot, session: SparkSession): Promise<void> {
        await SparkConnectExecutor.for(session).runWrite(root)
    }
};