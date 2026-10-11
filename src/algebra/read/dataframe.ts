import { DFCore } from "./df-core";
import {CacheCap, DescribeCap, HintCap, RepartitionCap, SamplingCap, SqlCap, SummaryCap} from "./batch-capabilities";

// Caps "batch" por defecto (lo que ya tenías)
/** @internal */
export type DFBatchCaps<R, E, G> =
    & DFCore<R, E, G>
    & CacheCap<R>
    & RepartitionCap<R>
    & SqlCap<R>
    & HintCap<R>
    & SamplingCap<R>
    & DescribeCap<R, E>
    & SummaryCap<R, E>;

// DFAlg abierto: le podés inyectar categorías nuevas sin tocar este archivo
/** @internal */
export type DFAlg<R, E, G = unknown, ExtraCaps = {}> =
    DFBatchCaps<R, E, G> & ExtraCaps;
