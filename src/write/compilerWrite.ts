// src/write/compilerWrite.ts

import {DFWritingAlg, SaveMode, WriterFormat, WriterSpec} from "../algebra/write";

type ProtoRel = any;

export interface ProtoWriteRoot {
    child: ProtoRel;
    spec: WriterSpec;
    tempViewName?: string;
}

export const ProtoWritingAlg: DFWritingAlg<ProtoRel, ProtoWriteRoot> = {
    fromChild(child: ProtoRel): ProtoWriteRoot {
        return { child, spec: { options: {}, partitionBy: [], sortBy: [] } };
    },

    format(w: ProtoWriteRoot, fmt: WriterFormat): ProtoWriteRoot {
        return { ...w, spec: { ...w.spec, format: fmt } };
    },

    mode(w: ProtoWriteRoot, m: SaveMode): ProtoWriteRoot {
        return { ...w, spec: { ...w.spec, mode: m } };
    },

    option(w: ProtoWriteRoot, k: string, v: string): ProtoWriteRoot {
        return {
            ...w,
            spec: { ...w.spec, options: { ...w.spec.options, [k]: String(v) } },
        };
    },

    options(w: ProtoWriteRoot, o: Record<string, string>): ProtoWriteRoot {
        const norm = Object.fromEntries(
            Object.entries(o).map(([k, v]) => [k, String(v)])
        );
        return {
            ...w,
            spec: { ...w.spec, options: { ...w.spec.options, ...norm } },
        };
    },

    partitionBy(w: ProtoWriteRoot, ...cs: string[]): ProtoWriteRoot {
        return {
            ...w,
            spec: { ...w.spec, partitionBy: [...w.spec.partitionBy, ...cs] },
        };
    },

    bucketBy(w: ProtoWriteRoot, n: number, c: string, ...cs: string[]): ProtoWriteRoot {
        return {
            ...w,
            spec: { ...w.spec, bucketBy: { numBuckets: n, columns: [c, ...cs] } },
        };
    },

    sortBy(w: ProtoWriteRoot, c: string, ...cs: string[]): ProtoWriteRoot {
        return {
            ...w,
            spec: { ...w.spec, sortBy: [...w.spec.sortBy, c, ...cs] },
        };
    },

    targetPath(w: ProtoWriteRoot, path: string): ProtoWriteRoot {
        return { ...w, spec: { ...w.spec, target: { path } } };
    },

    targetTable(w: ProtoWriteRoot, tab: string): ProtoWriteRoot {
        return { ...w, spec: { ...w.spec, target: { table: tab } } };
    },

    createOrReplaceTempView(w: ProtoWriteRoot, name: string): ProtoWriteRoot {
        return { ...w, tempViewName: name };
    },

    createTempView(w: ProtoWriteRoot, name: string): ProtoWriteRoot {
        return {
            ...w,
            spec: { ...w.spec, registerView: { name, replace: false } },
        };
    },
};

function toSaveModeV1(
    m?: "append" | "overwrite" | "ignore" | "error" | "errorifexists"
) {
    switch (m) {
        case "append":
            return 1; // SAVE_MODE_APPEND
        case "overwrite":
            return 2; // SAVE_MODE_OVERWRITE
        case "error":
        case "errorifexists":
            return 3; // SAVE_MODE_ERROR_IF_EXISTS
        case "ignore":
            return 4; // SAVE_MODE_IGNORE
        default:
            return 0; // SAVE_MODE_UNSPECIFIED
    }
}

export function protoWriteRootToPlan(root: ProtoWriteRoot) {
    const s = root.spec;

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

    const write_operation: any = {
        input: root.child,
        source: s.format ?? "parquet",
        mode: toSaveModeV1(s.mode),
        options: s.options,
        partitioning_columns: s.partitionBy,
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
        throw new Error("V1: falta destino path o table.");
    }

    return { command: { write_operation } };
}
