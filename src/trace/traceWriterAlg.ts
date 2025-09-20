// ============================================================
// TraceWriterAlg (shim local) — genera un árbol de traza del WR
// ============================================================

type TWKind =
    | "batchWrite"
    | "streamingWrite";

type TWTarget =
    | { kind: "path"; path: string }
    | { kind: "table"; table: string }
    | { kind: "tempView"; name: string; replace?: boolean }
    | { kind: "none" };

type TWNode = {
    kind: TWKind;
    // child es el AST del DF/EX ya construido con TraceDFAlg/TraceExprAlg
    child: any;

    // comunes
    format?: string;
    options?: Record<string, string>;
    partitionBy?: string[];

    // batch-only
    mode?: string;
    bucketBy?: { n: number; cols: string[] };
    sortBy?: string[];

    // streaming-only
    outputMode?: "append" | "complete" | "update";
    trigger?: { processingTime?: string; once?: true; availableNow?: true };
    queryName?: string;

    // destino/acción
    target: TWTarget;
};

export const TraceWriterAlg = {
    // levantar writer batch desde DF
    fromChild(child: any): TWNode {
        return {
            kind: "batchWrite",
            child,
            options: {},
            partitionBy: [],
            target: { kind: "none" },
        };
    },

    // levantar writer streaming desde DF
    writeStream(child: any): TWNode {
        return {
            kind: "streamingWrite",
            child,
            options: {},
            partitionBy: [],
            target: { kind: "none" },
        };
    },

    // comunes
    format(w: TWNode, fmt: string): TWNode {
        return { ...w, format: fmt };
    },
    option(w: TWNode, k: string, v: string): TWNode {
        return { ...w, options: { ...(w.options ?? {}), [k]: v } };
    },
    options(w: TWNode, kv: Record<string, string>): TWNode {
        return { ...w, options: { ...(w.options ?? {}), ...kv } };
    },
    partitionBy(w: TWNode, ...cols: string[]): TWNode {
        return { ...w, partitionBy: [...(w.partitionBy ?? []), ...cols] };
    },

    // batch-only
    mode(w: TWNode, m: string): TWNode {
        return { ...w, mode: m };
    },
    bucketBy(w: TWNode, n: number, col: string, ...cols: string[]): TWNode {
        return { ...w, bucketBy: { n, cols: [col, ...cols] } };
    },
    sortBy(w: TWNode, col: string, ...cols: string[]): TWNode {
        return { ...w, sortBy: [col, ...cols] };
    },

    // streaming-only
    outputMode(w: TWNode, m: "append" | "complete" | "update"): TWNode {
        return { ...w, outputMode: m };
    },
    trigger(
        w: TWNode,
        t: { processingTime?: string; once?: true; availableNow?: true }
    ): TWNode {
        return { ...w, trigger: { ...t } };
    },
    queryName(w: TWNode, name: string): TWNode {
        return { ...w, queryName: name };
    },

    // destinos / acciones (para save/saveAsTable/createTempView)
    targetPath(w: TWNode, path: string): TWNode {
        return { ...w, target: { kind: "path", path } };
    },
    targetTable(w: TWNode, table: string): TWNode {
        return { ...w, target: { kind: "table", table } };
    },
    createTempView(w: TWNode, name: string): TWNode {
        return { ...w, target: { kind: "tempView", name, replace: false } };
    },
    createOrReplaceTempView(w: TWNode, name: string): TWNode {
        return { ...w, target: { kind: "tempView", name, replace: true } };
    },
} as const;

// Serializadores (reusan tus traceSerializers para el hijo DF)
export function writerToClientAST(wn: TWNode) {
    return {
        node: wn.kind,
        format: wn.format,
        options: wn.options,
        partitionBy: wn.partitionBy,
        mode: wn.mode,
        bucketBy: wn.bucketBy,
        sortBy: wn.sortBy,
        outputMode: wn.outputMode,
        trigger: wn.trigger,
        queryName: wn.queryName,
        target: wn.target,
        // embed del AST del DF (ya serializado por tus serializers)
        child: wn.child,
    };
}

export function writerToClientASTJSON(wn: TWNode) {
    // si el hijo DF ya es un árbol de trace (objeto), dejamos tal cual;
    // si es una estructura propia, podrías envolverlo con traceToJSON(wn.child) antes
    return JSON.stringify(writerToClientAST(wn), null, 2);
}

export  function writerToClientASTMermaid(wn: TWNode) {
    // Mermaid simple: un nodo write -> child
    const id = wn.kind === "batchWrite" ? "Write" : "WriteStream";
    const target =
        wn.target.kind === "path"
            ? `path:${wn.target.path}`
            : wn.target.kind === "table"
                ? `table:${wn.target.table}`
                : wn.target.kind === "tempView"
                    ? `${wn.target.replace ? "createOrReplace" : "create"}:${wn.target.name}`
                    : "no-target";

    const caption =
        `${id}\\nformat=${wn.format ?? "-"}\\nmode=${wn.mode ?? "-"}\\n` +
        `outputMode=${wn.outputMode ?? "-"}\\ntrigger=${wn.trigger?.processingTime ?? wn.trigger?.once ? "once" : wn.trigger?.availableNow ? "availableNow" : "-"}` +
        `\\ncheckpoint=${wn.options?.checkpointLocation ?? "-"}` +
        `\\nqueryName=${wn.queryName ?? "-"}` +
        `\\npartitionBy=${(wn.partitionBy ?? []).join(",") || "-"}` +
        `\\ntarget=${target}`;

    // podés enriquecer usando traceToMermaid(wn.child) si tu DF ya genera diagramas
    return `flowchart TD
  W(["${caption}"])
  C["DF pipeline"]
  W --> C
`;
}
