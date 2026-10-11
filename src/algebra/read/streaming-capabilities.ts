// streaming-capabilities.ts
/** @internal */
export interface StreamingReadCap<R> {
    readStream(format: string, options?: Record<string, string>): R;
}
/** @internal */
export interface EventTimeWatermarkCap<R, E> {
    withWatermark(plan: R, eventTimeCol: E, delay: string): R;
}

export declare const STREAMING_BRAND: unique symbol;

/** @internal */
export type StreamingMark<R> = {
    [STREAMING_BRAND]: (r: R) => R;   // propiedad REQUERIDA, nominal
};

export type StreamingCaps<R, E> =
    & StreamingReadCap<R>
    & EventTimeWatermarkCap<R, E>;
