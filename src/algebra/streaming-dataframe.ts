// agrupás las caps de streaming que quieras exigir:
import {OutputModeCap, StreamingReadCap, TriggerCap, WatermarkCap} from "./streaming-capabilities";
import {DFAlg} from "./dataframe";

export type StreamingCaps<R, E> =
    & StreamingReadCap<R>
    & WatermarkCap<R, E>
    & TriggerCap<R>
    & OutputModeCap<R>;

// alias cómodo cuando el programa **sí** usa streaming:
export type DFStreamingAlg<R, E, G> = DFAlg<R, E, G, StreamingCaps<R, E>>;
