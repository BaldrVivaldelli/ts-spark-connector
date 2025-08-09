// src/spark/session.ts
import crypto from "crypto";
import { DataFrameReaderTF } from "./reader";

export class SparkSession {
    private readonly sessionId: string;
    private userContext: Record<string, any> = {};

    constructor(sessionId?: string) {
        this.sessionId = sessionId ?? crypto.randomUUID();
    }

    get read(): DataFrameReaderTF {
        return new DataFrameReaderTF(this);
    }

    getSessionId(): string {
        return this.sessionId;
    }

    getUserContext(): Record<string, any> {
        return this.userContext;
    }

    setUserContext(context: Record<string, any>) {
        this.userContext = context;
    }
}

export function createSparkSession(sessionId?: string): SparkSession {
    return new SparkSession(sessionId);
}

export const spark = createSparkSession();
