// Main entry point - export public API
export {
    SparkRuntimeConfig,
    SparkSession,
    SparkSessionBuilder,
    createSparkSession,
    spark,
} from "./client/session";
export type {
    AuthConfig, TLSConfig, RetryConfig, RetryEvent,
    SparkConnectionConfig, SparkLogger, SparkTelemetryEvent,
    SparkMetric, SparkMetricObserver,
    SessionConfigMap, SessionConfigValue, SparkUserContext,
} from "./client/session";
export { SparkConnectError } from "./client/errors";
export type { SparkConnectErrorOptions, SparkErrorDetails, SparkGrpcErrorInfo } from "./client/errors";
export {
    Window,
    call,
    coalesce,
    col,
    elementAt,
    explode,
    from_json,
    getField,
    getItem,
    isNotNull,
    isNull,
    lit,
    map_keys,
    map_values,
    posexplode,
    split,
    struct,
    to_json,
    when,
} from "./engine/column";
export type {
    CaseChain as ExpressionCaseChain,
    EBuilder,
    FrameBoundary,
    SortKeyBuilder,
} from "./engine/column";

// Unified schema-aware DataFrame surface. The same classes retain the lax path
// when no schema is declared; these exports do not introduce a parallel engine.
export { GroupedDataFrameTF, ReadChainedDataFrame } from "./read/readChainedDataFrame";
export type {
    AddedColumnSchema,
    AggregatedSchema,
    ColumnName,
    DuplicateRenameTargets,
    DroppedSchema,
    EBuilder as DataFrameExpressionBuilder,
    JoinSchema,
    RenamedColumnsSchema,
    RenamedSchema,
    RowOf,
    SelectedSchema,
    StatisticsSchema,
    TypedAggBuilder,
    TypedColumnFactory,
    TypedJoinPredicate,
    TypedOrderFactory,
    TypedPredicate,
    UniqueAggregationTuple,
    ValidRenameMap,
    ValidRenameTarget,
} from "./read/readChainedDataFrame";
export { DataFrameReaderTF } from "./read/dataFrameReaderTF";
export type {
    Opts as ReaderOptions,
    ReaderOptionValue,
} from "./read/dataFrameReaderTF";
export { DataFrameWriterTF } from "./write/dataFrameWriterTF";
export type {
    DefaultR as DefaultDataFrameRepresentation,
    Impl as WriterBackendImplementation,
    ReadDFAny,
    ReadDFPrivate,
    ReadDFPublic,
    StreamTriggerInput,
    WriterOptionValue,
} from "./write/dataFrameWriterTF";
export type { StreamingQueryHandle } from "./client/sparkClient";
export type {
    BatchWriterFormat,
    SaveMode,
    StreamWriterFormat,
} from "./algebra/write/write-core";
export type { OutputMode, Trigger } from "./algebra/write/write-stream-capabilities";
export type {
    CheckpointCap,
    OutputModeCap,
    QueryNameCap,
    StreamFormatCap,
    StreamLiftCap,
    TriggerCap,
} from "./algebra/write/write-stream-capabilities";
export type {
    BatchFormatCap,
    BucketSortCap,
    ViewsCap,
} from "./algebra/write/write-batch-capabilities";
export type {
    BatchWProgram,
    StreamWProgram,
    WBatch,
    WBatchBrand,
    WProgram,
    WStream,
    WStreamBrand,
} from "./algebra/write";
export type { WriteCore } from "./algebra/write/write-core";
export type { BatchWriterAlg, StreamWriterAlg } from "./algebra/write/dataframe";
export type {
    CacheCap,
    DescribeCap,
    HintCap,
    RepartitionCap,
    SamplingCap,
    SqlCap,
    SummaryCap,
} from "./algebra/read/batch-capabilities";
export type { DFCore } from "./algebra/read/df-core";
export type {
    EventTimeWatermarkCap,
    StreamingMark,
    StreamingReadCap,
} from "./algebra/read/streaming-capabilities";
export type {
    DFAlg,
    DFBatchCaps,
    DFProgram,
    ExprAlg,
    LiteralValue,
} from "./algebra/read";
export type {
    ExplainModeInput,
    GroupTypeInput,
    JoinHintName,
    JoinTypeInput,
    JoinTypeName,
} from "./engine/sparkConnectEnums";
export type { SessionAlgebra } from "./client/sessionAlgebra";
export type { DFWritingExec } from "./write/writeDataFrame";
export type {
    NullsOrder,
    SortDirection,
    SortOrder,
    WindowSpec,
} from "./types";
export type {
    FrameBoundary as WindowFrameBoundary,
    FrameType,
} from "./types/window";

export {
    DeclaredSchema,
    TOKEN_TO_DDL,
    arrayType,
    decimalType,
    mapType,
    schema,
    structType,
} from "./schema/schema";
export type {
    ArraySpec,
    ComplexSpec,
    DecimalSpec,
    FieldToken,
    FieldSpec,
    InferField,
    InferNonNullable,
    InferNestedSchema,
    InferSchema,
    MapSpec,
    NullableToken,
    NullableFlag,
    SchemaDef,
    StructSpec,
    TokenToTs,
    TypeToken,
} from "./schema/schema";
export type {
    AmbiguousColumn,
    ColumnType,
    ComplexType,
    IsKnownSchema,
    NonNull,
    ScalarType,
    Schema,
    SchemaShape,
    UnknownSchema,
    UsableColumnKey,
    UsableColumnType,
    Prettify,
    UnionToIntersection,
} from "./schema/schema-model";
export type {
    Aggregated,
    Aggregation as SchemaAggregation,
    Dropped,
    Joined,
    JoinedFor,
    JoinedForSingle,
    IsLeftNullable,
    IsRightNullable,
    JoinName,
    NullableColumn,
    Renamed,
    Selected,
    WithColumn,
} from "./schema/schema-transforms";

export { Condition, NumericColumn, SortKey, TypedColumn } from "./typed/typed-column";
export type { Columns, NumericValue } from "./typed/typed-column";
export type {
    Coalesced,
    ExprThunk as TypedColumnExprThunk,
    NumericOperand,
    NumericBase,
    NumericOperandValue,
    PromotedNumeric,
} from "./typed/typed-column";
export {
    abs,
    concat,
    length,
    lower,
    round,
    upper,
    when as typedWhen,
} from "./typed/functions";
export type {
    CaseChain as TypedCaseChain,
    Branch as TypedCaseBranch,
    Widen,
} from "./typed/functions";
export {
    Aggregation as TypedAggregation,
    PendingAggregation,
    makeAggFactory,
} from "./typed/aggregations";
export type {
    AggFactory,
    BuildableColumn,
    ColRef,
    ExprThunk as AggregationExprThunk,
    NumericColumnKey,
    NumericResult,
    SumResult,
} from "./typed/aggregations";
