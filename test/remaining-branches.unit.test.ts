import { describe, expect, it } from "vitest";
import {
    ProtoWritingAlg,
    protoWriteRootToPlan,
    type ProtoWriteRoot,
} from "../src/write/compilerWrite";
import {
    TraceWriterAlg,
    writerToClientASTMermaid,
    type TWNode,
} from "../src/trace/traceWriterAlg";

describe("remaining writer compiler branches", () => {
    it("merges writers whose optional collections are absent", () => {
        const empty = {
            child: {},
            writerKind: "batch",
            spec: {},
        } as unknown as ProtoWriteRoot;
        const runtime = ProtoWritingAlg as unknown as {
            option(writer: ProtoWriteRoot, key: string, value: string): ProtoWriteRoot;
            partitionBy(writer: ProtoWriteRoot, ...columns: string[]): ProtoWriteRoot;
            sortBy(writer: ProtoWriteRoot, column: string): ProtoWriteRoot;
        };
        const withOption = runtime.option(empty, "key", "value");
        const partitioned = runtime.partitionBy(empty, "day");
        const sorted = runtime.sortBy(empty, "day");
        expect(withOption.spec.options).toEqual({ key: "value" });
        expect(partitioned.spec.partitionBy).toEqual(["day"]);
        expect(sorted.spec.sortBy).toEqual(["day"]);
    });

    it("uses wire defaults when an externally supplied writer omits spec fields", () => {
        const noSpecBatch = {
            child: {},
            writerKind: "batch",
            spec: undefined,
        } as unknown as ProtoWriteRoot;
        const noSpecStream = {
            child: {},
            writerKind: "stream",
            spec: undefined,
        } as unknown as ProtoWriteRoot;
        expect(protoWriteRootToPlan(noSpecBatch)).toMatchObject({
            command: {
                write_operation: {
                    mode: 3,
                    options: {},
                    partitioning_columns: [],
                    sort_column_names: [],
                },
            },
        });
        expect(protoWriteRootToPlan(noSpecStream)).toMatchObject({
            command: {
                write_stream_operation_start: {
                    options: {},
                    partitioning_column_names: [],
                },
            },
        });
        expect(protoWriteRootToPlan({
            child: {},
            writerKind: "batch",
            spec: {
                options: undefined,
                partitionBy: undefined,
                sortBy: undefined,
            },
        } as unknown as ProtoWriteRoot)).toHaveProperty("command.write_operation");
        expect(protoWriteRootToPlan({
            child: {},
            writerKind: "stream",
            spec: {
                options: undefined,
                partitionBy: undefined,
            },
        } as unknown as ProtoWriteRoot)).toHaveProperty("command.write_stream_operation_start");
    });

    it("rejects malformed low-level trigger variants after every guard", () => {
        for (const trigger of [
            {},
            { kind: "ProcessingTime" },
            { kind: "ProcessingTime", intervalMs: Number.NaN },
            { kind: "Continuous" },
            { kind: "Continuous", checkpointIntervalMs: Number.NaN },
        ]) {
            expect(() => protoWriteRootToPlan({
                child: {},
                writerKind: "stream",
                spec: { trigger: trigger as never },
            } as unknown as ProtoWriteRoot)).toThrow(/invalid streaming trigger/i);
        }
    });

    it("handles trace nodes whose optional arrays and maps are absent", () => {
        const node: TWNode = {
            kind: "batchWrite",
            child: {},
            target: { kind: "none" },
        };
        expect(TraceWriterAlg.option(node, "key", "value").options)
            .toEqual({ key: "value" });
        expect(TraceWriterAlg.options(node, { other: "value" }).options)
            .toEqual({ other: "value" });
        expect(TraceWriterAlg.partitionBy(node, "day").partitionBy).toEqual(["day"]);
        expect(writerToClientASTMermaid(node)).toContain("partitionBy=-");
    });
});
