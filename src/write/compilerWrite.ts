// src/write/compilerWrite.ts
import type {
    SaveMode,
    BatchWriterFormat,
    StreamWriterFormat,
    WriterSpec,
    WBatchBrand,
    WStreamBrand,
    Trigger,
} from "../algebra/write";
import type { BatchWriterAlg, StreamWriterAlg } from "../algebra/write/dataframe";

type ProtoScalar = string | number | boolean | null;
type ProtoMessage = Record<string, unknown>;
type ProtoRel = ProtoMessage;

type StreamTriggerShape = NonNullable<WriterSpec["trigger"]> | Trigger;

export interface ProtoCreateDataFrameViewPlan {
    command: {
        create_dataframe_view: {
            name: string;
            input: ProtoRel;
            replace?: boolean;
            is_global?: boolean;
        };
    };
}

export interface ProtoWriteOperationPlan {
    command: {
        write_operation: ProtoMessage;
    };
}

export interface ProtoWriteStreamOperationPlan {
    command: {
        write_stream_operation_start: ProtoMessage;
    };
}

export type ProtoPlan =
    | ProtoCreateDataFrameViewPlan
    | ProtoWriteOperationPlan
    | ProtoWriteStreamOperationPlan;

export interface ProtoWriteRoot {
    child: ProtoRel;
    spec: WriterSpec;
    tempViewName?: string;
    writerKind: "batch" | "stream";

    start?: boolean;
    awaitTermination?: boolean;
    createStreamingView?: string;
}

type ProtoBatchWriteNode = ProtoWriteRoot & WBatchBrand;
type ProtoStreamWriteNode = ProtoWriteRoot & WStreamBrand;
type ProtoWriterNode = ProtoBatchWriteNode | ProtoStreamWriteNode;

type ExtendedStreamWriterAlg = {
    start(w: ProtoStreamWriteNode): ProtoStreamWriteNode;
    awaitTermination(w: ProtoStreamWriteNode): ProtoStreamWriteNode;
    fromTempView(w: ProtoStreamWriteNode, name: string): ProtoStreamWriteNode;
};

function createBaseWriter(child: ProtoRel, writerKind: ProtoWriteRoot["writerKind"]): ProtoWriteRoot {
    return {
        child,
        spec: { options: {}, partitionBy: [], sortBy: [] },
        writerKind,
    };
}

function asBatch(writer: ProtoWriteRoot): ProtoBatchWriteNode {
    return { ...writer, writerKind: "batch" } as ProtoBatchWriteNode;
}

function asStream(writer: ProtoWriteRoot): ProtoStreamWriteNode {
    return { ...writer, writerKind: "stream" } as ProtoStreamWriteNode;
}

function preserveWriterFlavor<T extends ProtoWriterNode>(
    writer: T,
    patch: Partial<ProtoWriteRoot>
): T {
    const next = { ...writer, ...patch };
    return (writer.writerKind === "stream" ? asStream(next) : asBatch(next)) as T;
}

function updateWriterSpec<T extends ProtoWriterNode>(
    writer: T,
    patch: Partial<WriterSpec>
): T {
    return preserveWriterFlavor(writer, {
        spec: {
            ...writer.spec,
            ...patch,
        },
    });
}

function appendWriterOptions<T extends ProtoWriterNode>(
    writer: T,
    options: Record<string, string>
): T {
    return updateWriterSpec(writer, {
        options: {
            ...(writer.spec.options ?? {}),
            ...options,
        },
    });
}

const protoWritingAlg = {
    fromChild(child: ProtoRel): ProtoBatchWriteNode {
        return asBatch(createBaseWriter(child, "batch"));
    },

    writeStream(child: ProtoRel): ProtoStreamWriteNode {
        return asStream(createBaseWriter(child, "stream"));
    },

    format<T extends ProtoWriterNode>(writer: T, fmt: BatchWriterFormat | StreamWriterFormat): T {
        return updateWriterSpec(writer, { format: fmt });
    },

    option<T extends ProtoWriterNode>(writer: T, key: string, value: string): T {
        return appendWriterOptions(writer, { [key]: String(value) });
    },

    options<T extends ProtoWriterNode>(writer: T, options: Record<string, string>): T {
        const normalized = Object.fromEntries(
            Object.entries(options).map(([key, value]) => [key, String(value)])
        );
        return appendWriterOptions(writer, normalized);
    },

    partitionBy<T extends ProtoWriterNode>(writer: T, ...columns: string[]): T {
        return updateWriterSpec(writer, {
            partitionBy: [...(writer.spec.partitionBy ?? []), ...columns],
        });
    },

    targetPath<T extends ProtoWriterNode>(writer: T, path: string): T {
        return updateWriterSpec(writer, { target: { path } });
    },

    targetTable<T extends ProtoWriterNode>(writer: T, table: string): T {
        return updateWriterSpec(writer, { target: { table } });
    },

    createOrReplaceTempView<T extends ProtoWriterNode>(writer: T, name: string): T {
        return preserveWriterFlavor(writer, { tempViewName: name });
    },

    createTempView<T extends ProtoWriterNode>(writer: T, name: string): T {
        return updateWriterSpec(writer, { registerView: { name, replace: false } });
    },

    mode(writer: ProtoBatchWriteNode, mode: SaveMode): ProtoBatchWriteNode {
        return asBatch({ ...writer, spec: { ...writer.spec, mode } });
    },

    bucketBy(writer: ProtoBatchWriteNode, numBuckets: number, col: string, ...cols: string[]): ProtoBatchWriteNode {
        return asBatch({
            ...writer,
            spec: {
                ...writer.spec,
                bucketBy: { numBuckets, columns: [col, ...cols] },
            },
        });
    },

    sortBy(writer: ProtoBatchWriteNode, col: string, ...cols: string[]): ProtoBatchWriteNode {
        return asBatch({
            ...writer,
            spec: {
                ...writer.spec,
                sortBy: [...(writer.spec.sortBy ?? []), col, ...cols],
            },
        });
    },

    outputMode(writer: ProtoStreamWriteNode, mode: "append" | "complete" | "update"): ProtoStreamWriteNode {
        return asStream({ ...writer, spec: { ...writer.spec, outputMode: mode } });
    },

    trigger(writer: ProtoStreamWriteNode, trigger: Trigger): ProtoStreamWriteNode {
        return asStream({ ...writer, spec: { ...writer.spec, trigger } });
    },

    queryName(writer: ProtoStreamWriteNode, name: string): ProtoStreamWriteNode {
        return asStream({ ...writer, spec: { ...writer.spec, queryName: name } });
    },

    start(writer: ProtoStreamWriteNode): ProtoStreamWriteNode {
        return asStream({ ...writer, start: true });
    },

    awaitTermination(writer: ProtoStreamWriteNode): ProtoStreamWriteNode {
        return asStream({ ...writer, awaitTermination: true });
    },

    fromTempView(writer: ProtoStreamWriteNode, name: string): ProtoStreamWriteNode {
        return asStream({
            ...writer,
            createStreamingView: name,
        });
    },
};

export const ProtoWritingAlg = protoWritingAlg as unknown as BatchWriterAlg<ProtoRel> & StreamWriterAlg<ProtoRel> & ExtendedStreamWriterAlg;

function toSaveModeV1(
    mode?: "append" | "overwrite" | "ignore" | "error" | "errorifexists"
): number {
    switch (mode) {
        case "append":
            return 1;
        case "overwrite":
            return 2;
        case "error":
        case "errorifexists":
            return 3;
        case "ignore":
            return 4;
        default:
            return 0;
    }
}

function isStreamingRoot(root: ProtoWriteRoot): boolean {
    return root.writerKind === "stream";
}

function toOutputModeV1(mode?: WriterSpec["outputMode"]): string | undefined {
    const normalized = (mode ?? "").toLowerCase();
    return normalized === "append" || normalized === "complete" || normalized === "update"
        ? normalized
        : undefined;
}

function toStreamTriggerV1(trigger?: StreamTriggerShape): ProtoMessage | undefined {
    if (!trigger) return undefined;

    if ("processingTime" in trigger && typeof trigger.processingTime === "string") {
        return { processing_time_interval: trigger.processingTime };
    }
    if ("once" in trigger && trigger.once) {
        return { once: true };
    }
    if ("availableNow" in trigger && trigger.availableNow) {
        return { available_now: true };
    }

    const kind = ("kind" in trigger ? trigger.kind : undefined)?.toLowerCase();
    if (kind === "processingtime") {
        const intervalMs = Number((trigger as { intervalMs?: number }).intervalMs ?? 0);
        if (Number.isFinite(intervalMs) && intervalMs > 0) {
            return { processing_time_interval: `${intervalMs} milliseconds` };
        }
    }
    if (kind === "once") {
        return { once: true };
    }
    if (kind === "continuous") {
        const checkpointIntervalMs = Number(
            (trigger as { checkpointIntervalMs?: number }).checkpointIntervalMs ?? 0
        );
        if (Number.isFinite(checkpointIntervalMs) && checkpointIntervalMs > 0) {
            return { continuous_checkpoint_interval: `${checkpointIntervalMs} milliseconds` };
        }
    }

    return undefined;
}

function appendIfDefined(
    message: ProtoMessage,
    key: string,
    value: ProtoScalar | ProtoMessage | string[] | undefined
): void {
    if (value !== undefined) {
        message[key] = value;
    }
}

function buildStreamingWriteOperation(root: ProtoWriteRoot, input: ProtoRel): ProtoMessage {
    const spec = root.spec ?? { options: {}, partitionBy: [], sortBy: [] };
    const operation: ProtoMessage = {
        input,
        options: spec.options ?? {},
        partitioning_column_names: spec.partitionBy ?? [],
    };

    appendIfDefined(operation, "format", spec.format);

    const outputMode = toOutputModeV1(spec.outputMode);
    appendIfDefined(operation, "output_mode", outputMode);

    const trigger = toStreamTriggerV1(spec.trigger);
    if (trigger) {
        Object.assign(operation, trigger);
    }

    appendIfDefined(operation, "query_name", spec.queryName);
    appendIfDefined(operation, "path", spec.target?.path);
    appendIfDefined(operation, "table_name", spec.target?.table);

    return operation;
}

function buildBatchWriteOperation(root: ProtoWriteRoot): ProtoMessage {
    const spec = root.spec ?? { options: {}, partitionBy: [], sortBy: [] };
    const operation: ProtoMessage = {
        input: root.child,
        source: spec.format ?? "parquet",
        mode: toSaveModeV1(spec.mode),
        options: spec.options ?? {},
        partitioning_columns: spec.partitionBy ?? [],
    };

    if (spec.bucketBy) {
        operation.bucket_by = {
            bucket_column_names: spec.bucketBy.columns,
            num_buckets: spec.bucketBy.numBuckets,
        };
        if (spec.sortBy?.length) {
            operation.sort_column_names = spec.sortBy;
        }
    }

    if (spec.target?.path) {
        operation.path = spec.target.path;
    } else if (spec.target?.table) {
        operation.table = {
            table_name: spec.target.table,
            save_method: 1,
        };
    } else {
        throw new Error("V1 batch: falta destino path o table.");
    }

    return operation;
}

export function protoWriteRootToPlan(root: ProtoWriteRoot): ProtoPlan | ProtoPlan[] {
    const spec = root.spec ?? { options: {}, partitionBy: [], sortBy: [] };

    if (spec.registerView) {
        return {
            command: {
                create_dataframe_view: {
                    name: spec.registerView.name,
                    is_global: false,
                    replace: spec.registerView.replace,
                    input: root.child,
                },
            },
        };
    }

    if (root.tempViewName) {
        return {
            command: {
                create_dataframe_view: {
                    name: root.tempViewName,
                    is_global: false,
                    replace: true,
                    input: root.child,
                },
            },
        };
    }

    if (isStreamingRoot(root)) {
        if (root.createStreamingView) {
            const createViewCmd: ProtoCreateDataFrameViewPlan = {
                command: {
                    create_dataframe_view: {
                        name: root.createStreamingView,
                        input: root.child,
                        is_global: false,
                        replace: true,
                    },
                },
            };

            const writeCmd: ProtoWriteStreamOperationPlan = {
                command: {
                    write_stream_operation_start: buildStreamingWriteOperation(root, {
                        unresolved_relation: {
                            parts: [root.createStreamingView],
                        },
                    }),
                },
            };

            return [createViewCmd, writeCmd];
        }

        return {
            command: {
                write_stream_operation_start: buildStreamingWriteOperation(root, root.child),
            },
        };
    }

    return {
        command: {
            write_operation: buildBatchWriteOperation(root),
        },
    };
}
