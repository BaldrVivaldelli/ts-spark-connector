// Capacidades nuevas (abiertas) para streaming
export interface StreamingReadCap<R> {
    // similar a relation, pero para fuentes continuas
    readStream(format: string, options?: Record<string, string>): R;
}

// Watermark sobre una columna de tiempo de evento
export interface WatermarkCap<R, E> {
    withWatermark(plan: R, eventTimeCol: E, delay: string): R; // p.ej. "10 minutes"
}

// (si más adelante querés crecer)
export interface TriggerCap<R> {
    withTrigger(plan: R, trigger: { once?: boolean; processingTimeMs?: number }): R;
}

export interface OutputModeCap<R> {
    withOutputMode(plan: R, mode: "append" | "update" | "complete"): R;
}
