# PySpark parity tracker

What the connector covers of the PySpark Spark Connect API, area by area.
Every unchecked item is a self-contained contribution: the
[architecture guide](ARCHITECTURE.md) lists the steps, and the "Protocol"
column names the Spark Connect message to start from. Items that need a
server newer than 4.0 must be gated on `session.requireServerVersion()`.

Legend: ✅ available · ⬜ open · ➖ not applicable to a TypeScript client.

## Session

| PySpark | Status | Notes / protocol |
| --- | --- | --- |
| `SparkSession.builder` (remote, config, auth, TLS) | ✅ | `SparkSession.builder()`, `withAuth`, `enableTLS`, `withRetry`, `withRpcTimeout`, `withExecuteTimeout` |
| `spark.sql`, `spark.table`, `spark.read`, `spark.readStream` | ✅ | |
| `spark.createDataFrame` | ✅ | primitive and complex column types (decimal, array, map, struct) over the `LocalRelation` Arrow encoding |
| `spark.range` | ✅ | `Relation.range`, typed `{ id: bigint }` |
| `spark.catalog` | ✅ | see Catalog below |
| `spark.conf` | ✅ | `Config` RPC |
| `spark.version` | ✅ | `session.version()` |
| `spark.stop` | ✅ | `close()` / `stop()` |
| `addTag`, `removeTag`, `getTags`, `clearTags` | ✅ | |
| `interruptAll`, `interruptTag`, `interruptOperation` | ✅ | plus per-call `ExecutionOptions.signal` |
| `cloneSession` | ✅ | Spark 4.1+ |
| `newSession` | ✅ | same connection config and user, no RPC |
| `addArtifact(s)` (jars, files, archives), `cacheArtifact` / `isArtifactCached` | ✅ | `AddArtifacts` (batched / chunked, CRC-32 per chunk); `ArtifactStatus` reports only `cache/<hash>` blobs |
| `udf.registerJavaFunction`, `udf.registerJavaUDAF` | ✅ | `CommonInlineUserDefinedFunction.java_udf` in a `register_function` command |
| Python / pandas UDFs, `mapInPandas`, `applyInPandas` | ➖ | need a Python worker on the server |
| `spark.streams` (StreamingQueryManager) | ✅ | `session.streams` |
| `spark.dataSource.register` | ➖ | Python data sources |
| Operation status (`session.getStatus`) | ✅ | Spark 4.2+ `GetStatus` RPC, gated on the server version |

## DataFrame transformations

| PySpark | Status | Notes / protocol |
| --- | --- | --- |
| `select`, `filter`/`where`, `withColumn`, `withColumnRenamed(s)`, `drop` | ✅ | |
| `join` (all join types, hints), `union`, `unionByName`, `intersect(All)`, `exceptAll`/`subtract` | ✅ | |
| `groupBy().agg()` | ✅ | typed and untyped |
| `rollup`, `cube`, `groupBy().pivot()`, `groupingSets` | ✅ | `Aggregate.group_type`, `pivot`, `grouping_sets`; results are untyped |
| `orderBy`/`sort`, `limit`, `distinct`, `dropDuplicates` | ✅ | |
| `sortWithinPartitions` | ✅ | `Sort.is_global = false` |
| `sample`, `randomSplit` | ✅ | |
| `repartition(n)`, `coalesce(n)`, `hint` | ✅ | |
| `describe`, `summary` | ✅ | |
| `cache`, `persist`, `unpersist` | ✅ | |
| `withWatermark` | ✅ | |
| `dropDuplicatesWithinWatermark` | ✅ | `Deduplicate.within_watermark` |
| `na.fill/drop/replace`, `fillna/dropna`, `replace` | ✅ | `NAFill`, `NADrop`, `NAReplace` |
| `stat.corr/cov/crosstab/freqItems/approxQuantile/sampleBy` | ✅ | `corr`, `cov` and `approxQuantile` execute and return numbers |
| `unpivot`/`melt`, `toDF`, `alias`, `offset`, `transpose`, `withColumns`, `crossJoin` | ✅ | reshaping operations fall back to the untyped schema |
| `repartition(n?, ...cols)`, `repartitionByRange` | ✅ | `RepartitionByExpression` |
| `selectExpr`, `filter("sql")`, `where` | ✅ | `ExpressionString` |
| `colRegex`, `metadataColumn` | ✅ | `UnresolvedRegex`, `UnresolvedAttribute.is_metadata_column` |
| `lateralJoin`, `asOfJoin` | ✅ | `LateralJoin`, `AsOfJoin` (PySpark `_joinAsOf` parameters) |
| `observe` | ✅ | `CollectMetrics`; `Observation.get()` resolves with `ExecutePlanResponse.observed_metrics` |
| `checkpoint`, `localCheckpoint` | ✅ | `checkpoint_command`, `CachedRemoteRelation`; `releaseCheckpoint()` or `close()` send `remove_cached_remote_relation_command` |
| `transform(func)` | ✅ | client-side helper |
| `to(schema)` | ✅ | `ToSchema` with an unparsed DDL type; typed when given a `schema({...})` |
| `withMetadata` | ✅ | `WithColumns` alias with JSON metadata |

## DataFrame actions and introspection

| PySpark | Status | Notes / protocol |
| --- | --- | --- |
| `collect`, `show`, `toLocalIterator` (`toRows`), Arrow batches | ✅ | per-execution `signal`, `tags`, `operationId` |
| `explain` | ✅ | |
| `count`, `first`, `head`, `take`, `tail`, `isEmpty` | ✅ | `count()` returns a `bigint`, like every Spark long |
| `schema`, `printSchema`/`treeString`, `columns`, `dtypes` | ✅ | AnalyzePlan `schema` / `tree_string` |
| `isLocal`, `isStreaming`, `inputFiles`, `sameSemantics`, `semanticHash` | ✅ | AnalyzePlan |
| `storageLevel` | ✅ | AnalyzePlan `get_storage_level` |
| `createTempView`, `createOrReplaceTempView` | ✅ | through the writer |
| `createGlobalTempView`, `createOrReplaceGlobalTempView` | ✅ | `CreateDataFrameViewCommand.is_global`; also on the writer |
| `toJSON` | ✅ | `to_json(struct(*))` projection into a `value` column |

## Writers

| PySpark | Status | Notes / protocol |
| --- | --- | --- |
| `write.format/mode/option(s)/partitionBy/bucketBy/sortBy/save/saveAsTable` | ✅ | csv, json, parquet, orc, avro, text, Delta, Iceberg |
| `write.insertInto` | ✅ | `WriteOperation.table.save_method = INSERT_INTO` |
| `write.jdbc`, `read.jdbc` (properties, partition column, predicates) | ✅ | `jdbc` data source, `DataSource.predicates`; verified against the Derby jars shipped with Spark |
| `writeTo` (DataFrameWriterV2: `using`, `tableProperty`, `partitionedBy`, `clusterBy`, `create`, `replace`, `createOrReplace`, `append`, `overwrite`, `overwritePartitions`, `withSchemaEvolution` on Spark 4.2+) | ✅ | `WriteOperationV2`; 4.1 defines `with_schema_evolution` but does not act on it (verified: `DELTA_METADATA_MISMATCH`), hence the 4.2 gate |
| `mergeInto` (`whenMatched`, `whenNotMatched`, `whenNotMatchedBySource`, `withSchemaEvolution`) | ✅ | `MergeIntoTableCommand` |
| `writeStream` (start, toTable, triggers, output modes, sinks) | ✅ | |
| `writeStream.foreach`, `foreachBatch` | ➖ | need Python on the server |

## Columns and functions

| PySpark | Status | Notes / protocol |
| --- | --- | --- |
| `col`, `lit`, `when/otherwise`, `coalesce`, `isNull/isNotNull`, comparisons, `and/or`, `alias`, `asc/desc` | ✅ | |
| Window specs: `partitionBy`, `orderBy`, `rowsBetween`, `rangeBetween`, partial specs, `over()` without a spec | ✅ | |
| Column methods: arithmetic, `neq`, `eqNullSafe`, `cast`/`tryCast`, `isin`, `between`, `like`/`ilike`/`rlike`, `startsWith`/`endsWith`/`contains`, `substr`, `getItem`/`getField`, `withField`/`dropFields`, bitwise ops | ✅ | |
| `pyspark.sql.functions` | ✅ | 438 entries in `functions` (407 generated from `scripts/functions/catalog.mjs`); `call(name, args)` still reaches anything else |
| Higher-order functions (`transform`, `filter`, `exists`, `forall`, `aggregate`, `zip_with`, `array_sort`, `map_filter`, `transform_keys`/`values`, `map_zip_with`) with JS lambdas | ✅ | `LambdaFunction` |
| `expr` | ✅ | `ExpressionString` |
| Partition transforms for `writeTo` (`years`, `months`, `days`, `hours`, `bucket`) | ✅ | `functions.years(...)` etc.; `UnresolvedFunction` in `WriteOperationV2.partitioning_columns` |
| `call_function` | ✅ | `functions.call_function(name, ...cols)`; `call(name, args)` when arguments must be literals |
| Typed (schema-aware) builders | ✅ | `length`, `upper`, `lower`, `concat`, `abs`, `round`, `typedWhen`; typed aggregates `count`, `countDistinct`, `sum`, `avg`, `min`, `max`, `first`, `last`, `collectList`, `collectSet`, `stddev`, `variance`, `approxCountDistinct`, `countIf` |

## Catalog

| PySpark | Status | Notes / protocol |
| --- | --- | --- |
| current/set catalog and database, list catalogs/databases/tables/columns, exists checks, drop temp views | ✅ | |
| `listFunctions`, `getTable`, `getDatabase`, `getFunction` | ✅ | `Catalog` relation |
| `createTable`, `createExternalTable` | ✅ | `Catalog` relation; schemas travel as unparsed DataType strings |
| `cacheTable`, `uncacheTable`, `isCached`, `clearCache` | ✅ | `Catalog` relation |
| `refreshTable`, `refreshByPath`, `recoverPartitions` | ✅ | `Catalog` relation |

## Streaming

| PySpark | Status | Notes / protocol |
| --- | --- | --- |
| `readStream` sources, `writeStream` sinks, triggers, `awaitTermination`, `stop` | ✅ | |
| `StreamingQuery.id/runId/isActive/status/lastProgress/recentProgress/explain/exception/processAllAvailable` | ✅ | `streaming_query_command` |
| `StreamingQueryManager` (`session.streams`: `active`, `get`, `awaitAnyTermination`, `resetTerminated`) | ✅ | `streaming_query_manager_command` |
| Query listeners (`addListener`/`removeListener`, started/progress/idle/terminated events) | ✅ | `streaming_query_listener_bus_command` plus the start event on `WriteStreamOperationStartResult` |

## Not planned yet

- MLlib over Connect (`ml_command`, `MlRelation`): large surface; needs a design first.
- Spark Declarative Pipelines (`pipelines.proto`, Spark 4.1+).
- Dual ESM/CJS build: tracked in issue #71.
