// src/engine/writer/protoWriterExec.ts

import {DFWritingExec} from "./writeDataFrame";
import {ProtoWriteRoot, protoWriteRootToPlan} from "./compilerWrite";
import {SparkSession} from "../client/session";
import { sparkGrpcClient } from "../client/sparkClient";

export const ProtoWritingExec: DFWritingExec<ProtoWriteRoot> = {
    async run(root: ProtoWriteRoot, session: SparkSession): Promise<void> {
        const plan = protoWriteRootToPlan(root);
        await sparkGrpcClient.executePlan({
            plan,
            session_id: session.getSessionId(),
            user_context: session.getUserContext(),
        });
    }
};