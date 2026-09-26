// src/engine/writer/protoWriterExec.ts

import {DFWritingExec} from "./writeDataFrame";
import {ProtoWriteRoot} from "./compilerWrite";
import {SparkSession} from "../client/session";
import {SparkConnectExecutor} from "../client/sparkConnectExecutor";
import {StreamingQueryHandle} from "../client/sparkClient";

export const ProtoWritingExec: DFWritingExec<ProtoWriteRoot> = {
    async run(root: ProtoWriteRoot, session: SparkSession): Promise<void> {
        if (root.writerKind === "stream") {
            throw new TypeError("Streaming writes must be started with start(), start(path), or toTable().");
        }
        await SparkConnectExecutor.for(session).runWrite(root)
    },
    async runStream(root: ProtoWriteRoot, session: SparkSession): Promise<StreamingQueryHandle> {
        if (root.writerKind !== "stream") {
            throw new TypeError("Batch writes must be executed with save() or saveAsTable().");
        }
        return await SparkConnectExecutor.for(session).runStream(root)
    },

};
