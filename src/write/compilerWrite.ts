// src/engine/compiler-writer.ts

import {DFWritingAlg, WriterSpec} from "./writeDataFrame";

type ProtoRel = any;

export interface ProtoWriteRoot {
    child: ProtoRel;
    spec: WriterSpec;
}

export const ProtoWritingAlg: DFWritingAlg<ProtoRel, ProtoWriteRoot> = {
    fromChild(child) {
        return { child, spec: { options: {}, partitionBy: [], sortBy: [] } };
    },
    format: (w, fmt) => ({ ...w, spec: { ...w.spec, format: fmt } }),
    mode:   (w, m)   => ({ ...w, spec: { ...w.spec, mode: m } }),
    option: (w, k, v) => ({ ...w, spec: { ...w.spec, options: { ...w.spec.options, [k]: String(v) } } }),
    options:(w, o)    => {
        const norm = Object.fromEntries(Object.entries(o).map(([k,v]) => [k, String(v)]));
        return { ...w, spec: { ...w.spec, options: { ...w.spec.options, ...norm } } };
    },
    partitionBy:(w, ...cs) => ({ ...w, spec: { ...w.spec, partitionBy: [...w.spec.partitionBy, ...cs] } }),
    bucketBy:(w, n, c, ...cs) => ({ ...w, spec: { ...w.spec, bucketBy: { numBuckets: n, columns: [c, ...cs] } } }),
    sortBy:(w, c, ...cs) => ({ ...w, spec: { ...w.spec, sortBy: [...w.spec.sortBy, c, ...cs] } }),
    targetPath:(w, path) => ({ ...w, spec: { ...w.spec, target: { path } } }),
    targetTable:(w, tab) => ({ ...w, spec: { ...w.spec, target: { table: tab } } }),
};


function toSaveModeV1(m?: "append"|"overwrite"|"ignore"|"error"|"errorifexists") {
    switch (m) {
        case "append":        return 1; // SAVE_MODE_APPEND
        case "overwrite":     return 2; // SAVE_MODE_OVERWRITE
        case "error":
        case "errorifexists": return 3; // SAVE_MODE_ERROR_IF_EXISTS
        case "ignore":        return 4; // SAVE_MODE_IGNORE
        default:              return 0; // SAVE_MODE_UNSPECIFIED
    }
}

/*function toModeV2(m?: "append"|"overwrite"|"ignore"|"error"|"errorifexists") {
    switch (m) {
        case "append":    return 4; // MODE_APPEND
        case "overwrite": return 2; // MODE_OVERWRITE
        // el resto no tiene mapping directo; elegí política o expone nuevos modos
        default:          return 0; // MODE_UNSPECIFIED
    }
}*/

export function protoWriteRootToPlan(root: ProtoWriteRoot) {
    const s = root.spec;

    const write_operation: any = {
        input: root.child,
        source: s.format ?? "parquet",
        mode: toSaveModeV1(s.mode),                // 1..4
        options: s.options,                         // map<string,string>
        partitioning_columns: s.partitionBy,        // string[]
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
        write_operation.table = {
            table_name: s.target.table,
            save_method: 1, // SAVE_AS_TABLE
        };
    } else {
        throw new Error("V1: falta destino path o table.");
    }

    return { command: { write_operation } };
}