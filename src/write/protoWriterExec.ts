// src/engine/writer/protoWriterExec.ts

import {DFWritingExec} from "./writeDataFrame";
import {ProtoWriteRoot} from "./compilerWrite";
import {SparkSession} from "../client/session";
import {SparkConnectExecutor} from "../client/sparkConnectExecutor";
import {StreamingQueryHandle} from "../client/sparkClient";
import type {ResolvedExecution} from "../client/executionOptions";

export const ProtoWritingExec: DFWritingExec<ProtoWriteRoot> = {
    async run(root: ProtoWriteRoot, session: SparkSession, execution?: ResolvedExecution): Promise<void> {
        if (root.writerKind === "stream") {
            throw new TypeError("Streaming writes must be started with start(), start(path), or toTable().");
        }
        const executor = SparkConnectExecutor.for(session);
        await (execution === undefined ? executor.runWrite(root) : executor.runWrite(root, execution));
    },
    async runStream(root: ProtoWriteRoot, session: SparkSession): Promise<StreamingQueryHandle> {
        if (root.writerKind !== "stream") {
            throw new TypeError("Batch writes must be executed with save() or saveAsTable().");
        }
        return await SparkConnectExecutor.for(session).runStream(root)
    },

};
