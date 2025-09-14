// agrupás las caps de streaming que quieras exigir:

import {EventTimeWatermarkCap, StreamingReadCap} from "./streaming-capabilities";
import {DFAlg} from "./dataframe";

export type StreamingCaps<R, E> =
    & EventTimeWatermarkCap<R, E>
    & StreamingReadCap<R>

// alias cómodo cuando el programa **sí** usa streaming:
export type DFStreamingAlg<R, E, G> = DFAlg<R, E, G, StreamingCaps<R, E>>;
