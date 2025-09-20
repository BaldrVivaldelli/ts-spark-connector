// streaming-capabilities.ts
export interface StreamingReadCap<R> {
    readStream(format: string, options?: Record<string, string>): R;
}
export interface EventTimeWatermarkCap<R, E> {
    withWatermark(plan: R, eventTimeCol: E, delay: string): R;
}
export type StreamingCaps<R, E> =
    & StreamingReadCap<R>
    & EventTimeWatermarkCap<R, E>;
