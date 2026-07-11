// Main entry point - export public API
export { SparkRuntimeConfig, SparkSession, createSparkSession, spark } from "./client/session";
export type {
    AuthConfig, TLSConfig, RetryConfig, RetryEvent,
    SparkConnectionConfig, SparkLogger, SparkTelemetryEvent,
    SessionConfigMap, SparkUserContext,
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
export type { EBuilder, FrameBoundary, SortKeyBuilder } from "./engine/column";

// Unified schema-aware DataFrame surface. The same classes retain the lax path
// when no schema is declared; these exports do not introduce a parallel engine.
export { GroupedDataFrameTF, ReadChainedDataFrame } from "./read/readChainedDataFrame";
export type { RowOf, EBuilder as DataFrameExpressionBuilder } from "./read/readChainedDataFrame";
export { DataFrameReaderTF } from "./read/dataFrameReaderTF";
export type { ReaderOptionValue } from "./read/dataFrameReaderTF";
export { DataFrameWriterTF } from "./write/dataFrameWriterTF";
export type {
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
export type { NullsOrder, SortDirection, SortOrder, WindowSpec } from "./types";

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
    InferSchema,
    MapSpec,
    NullableToken,
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
} from "./schema/schema-model";
export type {
    Aggregated,
    Dropped,
    Joined,
    JoinedFor,
    Renamed,
    Selected,
    WithColumn,
} from "./schema/schema-transforms";

export { Condition, NumericColumn, SortKey, TypedColumn } from "./typed/typed-column";
export type { Columns, NumericValue } from "./typed/typed-column";
export {
    abs,
    concat,
    length,
    lower,
    round,
    upper,
    when as typedWhen,
} from "./typed/functions";
export {
    Aggregation as TypedAggregation,
    PendingAggregation,
    makeAggFactory,
} from "./typed/aggregations";
export type { AggFactory } from "./typed/aggregations";
