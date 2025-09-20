// src/read/session.ts
import crypto from "crypto";
import { DataFrameReaderTF } from "../read/dataFrameReaderTF";
import { SessionAlgebra } from "./sessionAlgebra";
import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import {DFProgram, StreamingMark} from "../algebra/read";
import { SqlCap } from "../algebra/read/batch-capabilities";
import { StreamingReadCap } from "../algebra/read/streaming-capabilities";

// ⬇️ (opcional) si querés exponer writeStream desde la sesión:
import { DataFrameWriterTF } from "../write/dataFrameWriterTF";
import { WStream } from "../algebra/write"; // o desde donde declares WStream

/** TYPES **/

export type TLSConfig = {
    keyStorePath: string;
    keyStorePassword: string;
    trustStorePath?: string;
    trustStorePassword?: string;
};

export type AuthConfig =
    | { type: "basic"; username: string; password: string }
    | { type: "token"; token: string };

/** SPARK SESSION **/

export class SparkSession implements SessionAlgebra {
    private readonly sessionId: string;
    private userContext: Record<string, any> = {};

    constructor(sessionId?: string) {
        this.sessionId = sessionId ?? crypto.randomUUID();
    }

    // ===== Streaming read: idéntico a tu implementación actual =====
    readStream<R, E, G>(
        format: string,
        options?: Record<string, string>
    ): ReadChainedDataFrame<R, E, G, StreamingReadCap<R> & StreamingMark<R>, unknown> {
        return ReadChainedDataFrame.readStream<R, E, G>(format, this, options);
    }

    // ===== (Opcional) Atajo de streaming write desde la sesión =====
    // Delegamos en df.writeStream() para mantener una sola fuente de verdad.
    // Si tenés el brand StreamingMark<R>, podés tiparlo más fuerte.
    writeStream<
        R = unknown,
        E = unknown,
        G = unknown,
        CDF = unknown,
        CEX = unknown
    >(
        df: ReadChainedDataFrame<R, E, G, CDF, CEX>
    ): DataFrameWriterTF<R, E, G, WStream, CDF, CEX, any> {
        return (df as any).writeStream();
    }

    // ===== SQL helpers (batch) =====
    sql<R = unknown, E = unknown, G = unknown>(
        query: string
    ): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown> {
        const prog: DFProgram<R, E, G, SqlCap<R>, unknown> = (DF) => DF.sql(query);
        return new ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>(prog, this);
    }

    table<R = unknown, E = unknown, G = unknown>(
        name: string
    ): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown> {
        const prog: DFProgram<R, E, G, SqlCap<R>, unknown> = (DF) =>
            DF.sql(`SELECT * FROM ${name}`);
        return new ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>(prog, this);
    }

    static builder(): SparkSessionBuilder {
        return new SparkSessionBuilder();
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

/** BUILDER **/

class SparkSessionBuilder {
    private configMap: Record<string, any> = {};

    config(key: string, value: any): this {
        this.configMap[key] = value;
        return this;
    }

    configs(configs: Record<string, any>): this {
        this.configMap = { ...this.configMap, ...configs };
        return this;
    }

    enableTLS(tls: TLSConfig): this {
        this.config("spark.ssl.enabled", "true");
        this.config("spark.ssl.keyStore", tls.keyStorePath);
        this.config("spark.ssl.keyStorePassword", tls.keyStorePassword);

        if (tls.trustStorePath) {
            this.config("spark.ssl.trustStore", tls.trustStorePath);
        }

        if (tls.trustStorePassword) {
            this.config("spark.ssl.trustStorePassword", tls.trustStorePassword);
        }

        return this;
    }

    withAuth(auth: AuthConfig): this {
        if (auth.type === "basic") {
            this.config("spark.auth.type", "basic");
            this.config("spark.auth.username", auth.username);
            this.config("spark.auth.password", auth.password);
        } else {
            this.config("spark.auth.type", "token");
            this.config("spark.auth.token", auth.token);
        }

        return this;
    }

    withAuthAndTLS(auth: AuthConfig, tls: TLSConfig): this {
        return this.withAuth(auth).enableTLS(tls);
    }

    getOrCreate(): SparkSession {
        const session = new SparkSession();
        session.setUserContext(this.configMap);
        return session;
    }
}

export const spark = createSparkSession();
