// src/engine/writer/protoWriterExec.ts

import {DFWritingExec} from "./writeDataFrame";
import {ProtoWriteRoot} from "./compilerWrite";
import {SparkSession} from "../client/session";
import {SparkConnectExecutor} from "../client/sparkConnectExecutor";
import {StreamingQueryHandle} from "../client/sparkClient";

export const ProtoWritingExec: DFWritingExec<ProtoWriteRoot> = {
    async run(root: ProtoWriteRoot, session: SparkSession): Promise<void> {
        await SparkConnectExecutor.for(session).runWrite(root)
    },
    async runStream(root: ProtoWriteRoot, session: SparkSession): Promise<StreamingQueryHandle> {
        return await SparkConnectExecutor.for(session).runStream(root)
    },

};