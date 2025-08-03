// src/spark/session.ts
import { DataFrameReader } from "./reader";
import crypto from "crypto";

export class SparkSession {
    private readonly sessionId: string;
    private userContext: Record<string, any> = {};

    read = new DataFrameReader();

    constructor(sessionId?: string) {
        this.sessionId = sessionId ?? crypto.randomUUID();
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

// instancia única global
export const spark = new SparkSession();
