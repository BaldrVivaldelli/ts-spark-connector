# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [1.1.0] - 2025-07-13

### Added
- Introduced Tagless Final architecture for the DataFrame DSL
- Implemented support for column expressions: `gt`, `lt`, `and`, `or`, `alias`, etc.
- Added `withColumn()` with column name conflict resolution and schema preservation
- Introduced `compileExpression()` support for `UnresolvedFunction` and `Alias` types
- Added support for expression aliasing with proper Spark Connect identifier array
- New `extractColumns()` utility to recover schema across transformations
- DSL `select()` now accepts both `string` and `Column` arguments

### Changed
- Refactored `compileRelation()` using functional pattern dispatch per LogicalPlan node
- Improved `DataFrame.show()` output based on Arrow batches

### Fixed
- Prevented schema loss on chained transformations (e.g., `withColumn().select()`)
- Corrected column resolution errors with better expression validation

### Developer Experience
- DSL now supports PySpark-like chaining with autocompletion
- Updated `README.md` with full working examples and Spark Connect integration notes
- Added `.gitignore` covering Node, WebStorm, build, and env files


## [1.2.0] - 2025-08-03

### Added
- Support for `Join` in `LogicalPlan` with configurable join types: `"inner"`, `"left"`, `"right"`, `"outer"`, etc.
- New `JoinTypeInput` and `toProtoJoinType(...)` helper to map user-friendly strings to Spark Connect enums
- `join()` method in the DSL (`ReadChainedDataFrame`) supporting both condition and join type
- `sparkConnectEnums.ts` module to centralize Spark Connect enum constants

### Changed
- `ReadChainedDataFrame` now carries its `SparkSession` context internally across transformations
- `dataframeInterpreter()` refactored to be session-aware instead of relying on global singleton
- `DataFrameReader` constructs session-bound `ReadChainedDataFrame` instances at creation time

### Developer Experience
- Cleaner DX: no need to pass or manage sessions explicitly across transformations
- Default join type set to `"inner"` if not specified
- Added `.wrap()` helper to simplify ReadChainedDataFrame instantiation
- Improved debug logging potential via centralized wrap function

### Fixed
- Resolved issue where executing `df.show()` after `join()` resulted in `JOIN_TYPE_UNSPECIFIED`