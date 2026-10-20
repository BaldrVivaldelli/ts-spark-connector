import { DFCore } from "./df-core";
import {
    CacheCap,
    DescribeCap,
    HintCap,
    NullHandlingCap,
    AsOfJoinCap,
    CachedRelationCap,
    LateralJoinCap,
    MetadataCap,
    ObserveCap,
    OffsetCap,
    PartitionSortCap,
    RepartitionByExpressionCap,
    SchemaCap,
    WatermarkDedupCap,
    RepartitionCap,
    ReshapeCap,
    SamplingCap,
    SqlCap,
    StatCap,
    SummaryCap,
} from "./batch-capabilities";

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
    & SummaryCap<R, E>
    & NullHandlingCap<R>
    & StatCap<R, E>
    & ReshapeCap<R, E>
    & OffsetCap<R>
    & RepartitionByExpressionCap<R, E>
    & PartitionSortCap<R, E>
    & WatermarkDedupCap<R, E>
    & LateralJoinCap<R, E>
    & AsOfJoinCap<R, E>
    & ObserveCap<R, E>
    & CachedRelationCap<R>
    & SchemaCap<R>
    & MetadataCap<R, E>;

// DFAlg abierto: le podés inyectar categorías nuevas sin tocar este archivo
/** @internal */
export type DFAlg<R, E, G = unknown, ExtraCaps = {}> =
    DFBatchCaps<R, E, G> & ExtraCaps;
