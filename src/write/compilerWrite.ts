// src/write/compilerWrite.ts
import type {
    SaveMode,
    BatchWriterFormat,
    StreamWriterFormat,
    WriterSpec,
    WBatchBrand,
    WStreamBrand, Trigger,
} from "../algebra/write";
import type { BatchWriterAlg, StreamWriterAlg } from "../algebra/write/dataframe";

type ProtoRel = any;

export interface ProtoWriteRoot {
    child: ProtoRel;
    spec: WriterSpec;
    tempViewName?: string;

    start?: boolean;
    awaitTermination?: boolean;
    createStreamingView?: string;
}

function asBatch(w: ProtoWriteRoot): WBatchBrand {
    return w as unknown as WBatchBrand;
}
function asStream(w: ProtoWriteRoot): WStreamBrand {
    return w as unknown as WStreamBrand;
}

export const ProtoWritingAlg = {
    // ------- constructores -------
    fromChild(child: any): any {
        const w: ProtoWriteRoot = {
            child,
            spec: { options: {}, partitionBy: [], sortBy: [] } as WriterSpec,
        };
        return asBatch(w);
    },

    writeStream(child: any): any {
        const w: ProtoWriteRoot = {
            child,
            spec: { options: {}, partitionBy: [], sortBy: [] } as WriterSpec,
        };
        return asStream(w);
    },

    // ------- comunes (batch + stream) -------
    format(w: any, fmt: BatchWriterFormat | StreamWriterFormat): any {
        const root = w as unknown as ProtoWriteRoot;

        // usamos un spec temporal en any para evitar el choque de tipos (Batch vs Stream)
        const specAny: any = { ...root.spec };
        specAny.format = fmt as any; // puede ser batch o stream; lo guardamos igual

        const next: ProtoWriteRoot = { ...root, spec: specAny as WriterSpec };

        // preservamos el "brand" de entrada (batch/stream)
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },
    option(w: any, k: string, v: string): any {
        const root = w as unknown as ProtoWriteRoot;
        const next: ProtoWriteRoot = {
            ...root,
            spec: { ...root.spec, options: { ...(root.spec.options ?? {}), [k]: String(v) } },
        };
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },
    options(w: any, o: Record<string, string>): any {
        const norm = Object.fromEntries(Object.entries(o).map(([k, v]) => [k, String(v)]));
        const root = w as unknown as ProtoWriteRoot;
        const next: ProtoWriteRoot = {
            ...root,
            spec: { ...root.spec, options: { ...(root.spec.options ?? {}), ...norm } },
        };
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },
    partitionBy(w: any, ...cs: string[]): any {
        const root = w as unknown as ProtoWriteRoot;
        const next: ProtoWriteRoot = {
            ...root,
            spec: { ...root.spec, partitionBy: [...(root.spec.partitionBy ?? []), ...cs] },
        };
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },
    targetPath(w: any, path: string): any {
        const root = w as unknown as ProtoWriteRoot;
        const next: ProtoWriteRoot = { ...root, spec: { ...root.spec, target: { path } } };
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },
    targetTable(w: any, tab: string): any {
        const root = w as unknown as ProtoWriteRoot;
        const next: ProtoWriteRoot = { ...root, spec: { ...root.spec, target: { table: tab } } };
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },
    createOrReplaceTempView(w: any, name: string): any {
        const root = w as unknown as ProtoWriteRoot;
        const next: ProtoWriteRoot = { ...root, tempViewName: name };
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },
    createTempView(w: any, name: string): any {
        const root = w as unknown as ProtoWriteRoot;
        const next: ProtoWriteRoot = { ...root, spec: { ...root.spec, registerView: { name, replace: false } } };
        return (w as WStreamBrand) ? asStream(next) : asBatch(next);
    },

    // ------- batch-only -------
    mode(w: any, m: SaveMode): any {
        const root = w as unknown as ProtoWriteRoot;
        return asBatch({ ...root, spec: { ...root.spec, mode: m } });
    },
    bucketBy(w: any, n: number, c: string, ...cs: string[]): any {
        const root = w as unknown as ProtoWriteRoot;
        return asBatch({ ...root, spec: { ...root.spec, bucketBy: { numBuckets: n, columns: [c, ...cs] } } });
    },
    sortBy(w: any, c: string, ...cs: string[]): any {
        const root = w as unknown as ProtoWriteRoot;
        return asBatch({ ...root, spec: { ...root.spec, sortBy: [...(root.spec.sortBy ?? []), c, ...cs] } });
    },

    // ------- streaming-only -------
    outputMode(w: any, m: "append" | "complete" | "update"): any {
        const root = w as unknown as ProtoWriteRoot;
        return asStream({ ...root, spec: { ...root.spec, outputMode: m } as WriterSpec });
    },
    trigger(w: any, t: Trigger): any {
        const root = w as unknown as ProtoWriteRoot;
        return asStream({ ...root, spec: { ...root.spec, trigger: t } as WriterSpec });
    },
    queryName(w: any, name: string): any {
        const root = w as unknown as ProtoWriteRoot;
        return asStream({ ...root, spec: { ...root.spec, queryName: name } as WriterSpec });
    },
    start(w: any): any {
        const root = w as ProtoWriteRoot;
        const next: ProtoWriteRoot = { ...root, start: true };
        return asStream(next);
    },

    awaitTermination(w: any): any {
        const root = w as ProtoWriteRoot;
        const next: ProtoWriteRoot = { ...root, awaitTermination: true };
        return asStream(next);
    },
    fromTempView(w: ProtoWriteRoot, name: string): ProtoWriteRoot {
        return {
            ...w,
            createStreamingView: name
        };
    }
} as unknown as BatchWriterAlg<ProtoRel> & StreamWriterAlg<ProtoRel>; // 👈 cast final, sin sobrecargas


// (igual a tu helper previo)
function toSaveModeV1(
    m?: "append" | "overwrite" | "ignore" | "error" | "errorifexists"
) {
    switch (m) {
        case "append":        return 1;
        case "overwrite":     return 2;
        case "error":
        case "errorifexists": return 3;
        case "ignore":        return 4;
        default:              return 0;
    }
}

function isStreamingSpec(s: any): boolean {
    const fmt = (s?.format ?? "").toString().toLowerCase();
    // heurística suficiente: si tiene outputMode/trigger/queryName o es un sink clásico de streaming
    return !!(s?.outputMode || s?.trigger || s?.queryName ||
        fmt === "console" || fmt === "kafka" || fmt === "memory");
}

// helpers para enums/oneof de streaming
function toOutputModeV1(m?: "append" | "complete" | "update") {
    switch ((m ?? "").toLowerCase()) {
        case "append":   return 1; // OUTPUT_MODE_APPEND
        case "complete": return 2; // OUTPUT_MODE_COMPLETE
        case "update":   return 3; // OUTPUT_MODE_UPDATE
        default:         return 0; // OUTPUT_MODE_UNSPECIFIED
    }
}

function toStreamTriggerV1(t: any) {
    // Aceptamos tus dos estilos:
    // 1) { processingTime: "2 seconds" }  (el que usás hoy)
    // 2) { kind: "ProcessingTime", intervalMs: 2000 }  (si alguna vez migrás)
    if (!t) return undefined;

    // estilo #1
    if (typeof t.processingTime === "string") {
        // Connect acepta string con unidad, en snake_case
        return { processing_time: t.processingTime };
    }
    if (t.once) return { once: true };
    if (t.availableNow) return { available_now: true };

    // estilo #2 (discriminado)
    const kind = (t.kind ?? "").toLowerCase();
    if (kind === "processingtime") {
        const ms = Number(t.intervalMs ?? 0);
        if (Number.isFinite(ms) && ms > 0) {
            return { processing_time: `${ms} milliseconds` };
        }
    }
    if (kind === "once") return { once: true };
    if (kind === "availablenow") return { available_now: true };

    // fallback: nada
    return undefined;
}


export function protoWriteRootToPlan(rootAny: any) {
    const root = rootAny as ProtoWriteRoot;
    const s = root.spec ?? {};

    // vistas (válido para batch y stream)
    if (s.registerView) {
        return {
            command: {
                create_dataframe_view: {
                    name: s.registerView.name,
                    is_local: false,
                    replace: s.registerView.replace,
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
                    is_local: false,
                    replace: true,
                    input: root.child,
                },
            },
        };
    }

    // --- STREAMING ---
    if (isStreamingSpec(s)) {
        if (root.createStreamingView) {
            // 1️⃣ Primer comando: crear la vista temporal
            const createViewCmd = {
                command: {
                    create_dataframe_view: {
                        name: root.createStreamingView,
                        input: root.child,
                        replace: true,
                    }
                }
            };

            // 2️⃣ Segundo comando: escribir usando unresolved_relation
            const write_stream_operation: any = {
                input: {
                    unresolved_relation: {
                        parts: [root.createStreamingView],
                    }
                },
                source: s.format ?? "console",
                options: s.options ?? {},
            };

            const om = toOutputModeV1(s.outputMode);
            if (om !== 0) write_stream_operation.output_mode = om;

            const trig = toStreamTriggerV1(s.trigger);
            if (trig) write_stream_operation.trigger = trig;

            if (s.queryName) write_stream_operation.query_name = s.queryName;
            if (s.target?.path) {
                write_stream_operation.path = s.target.path;
            }
            if (s.target?.table) {
                write_stream_operation.table = {
                    table_name: s.target.table,
                    save_method: 1,
                };
            }
            if (root.start) write_stream_operation.start = true;
            if (root.awaitTermination) write_stream_operation.await_termination = true;

            const writeCmd = { command: { write_stream_operation } };

            // 🔁 Devolvés una lista de comandos
            return [createViewCmd, writeCmd];
        }

        // 🚨 Caso sin createStreamingView: se genera normalmente
        const write_stream_operation: any = {
            input: root.child,
            source: s.format ?? "console",
            options: s.options ?? {},
        };

        const om = toOutputModeV1(s.outputMode);
        if (om !== 0) write_stream_operation.output_mode = om;

        const trig = toStreamTriggerV1(s.trigger);
        if (trig) write_stream_operation.trigger = trig;

        if (s.queryName) write_stream_operation.query_name = s.queryName;
        if (s.target?.path) {
            write_stream_operation.path = s.target.path;
        }
        if (s.target?.table) {
            write_stream_operation.table = {
                table_name: s.target.table,
                save_method: 1,
            };
        }
        if (root.start) write_stream_operation.start = true;
        if (root.awaitTermination) write_stream_operation.await_termination = true;

        return { command: { write_stream_operation } };
    }



    // --- BATCH ---
    const write_operation: any = {
        input: root.child,
        source: s.format ?? "parquet",
        mode: toSaveModeV1(s.mode),
        options: s.options ?? {},
        partitioning_columns: s.partitionBy ?? [],
    };

    if (s.bucketBy) {
        write_operation.bucket_by = {
            bucket_column_names: s.bucketBy.columns,
            num_buckets: s.bucketBy.numBuckets,
        };
        if (s.sortBy?.length) write_operation.sort_column_names = s.sortBy;
    }

    if (s.target?.path) {
        write_operation.path = s.target.path;
    } else if (s.target?.table) {
        write_operation.table = { table_name: s.target.table, save_method: 1 };
    } else {
        throw new Error("V1 batch: falta destino path o table.");
    }

    return { command: { write_operation } };
}