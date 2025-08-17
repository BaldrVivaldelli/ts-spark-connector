# 1.0.0 (2025-08-17)


### Bug Fixes

* correct repository metadata for npm ([c86de55](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/c86de55a1caccc05f698a92f8b0887468b39f59e))
* update repository metadata for npm ([48302c4](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/48302c441d61f16ab89bd7d96fd92b0798a9bd58))
* withColumns retrieve context just like pyspark ([4638fcf](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/4638fcf4b3b43b8f3dd03b9e1a6610ecfd9415bc))


### Features

* add null handling, window functions, and caseWhen support to DF API ([bf82d9b](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/bf82d9b5d86e03bd1291ca81307c007472e85c16))
* add roadmap ([a8700b0](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/a8700b0fcdeb47e0ab1e0b9e18c59acb467d62ad))
* add roadmap ([1c73c95](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/1c73c953b3b0a367db73bc7384aa26bd2a84e2fd))
* add SetOperation support, enums, updated datasets and examples runner ([f4cac99](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/f4cac9962f9cbf005ae04e11c066bb2c09fe53c6))
* add SetOperation support, enums, updated datasets and examples runner ([7b63ab7](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/7b63ab7809a3d4ca6d10c224fd41a23f416b3cc0))
* add SetOperation support, proto enums mapping, and new usage examples ([ffa79db](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/ffa79db8e4a3de8f858a0e91064bfb75e29469be))
* **engine:** implement Tagless Final DSL and full Spark expression support ([b563bd8](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/b563bd832f200c534f3c7fa3d3d2f429d127909b))
* **logical-plan:** add support for JOIN in LogicalPlan and compiler ([57c0115](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/57c01150dc6174fa2398a0dea8dedeafc3ca4034))
* **reader:** add multi-path support for parquet and json formats ([b127717](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/b1277176f1b302c3fc6ad93758a65199f2972145))
* soporte completo tagless-final + API tipo PySpark ([27f7c5c](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/27f7c5c1d151e8c95b879874c1bcb507e89e2442))
* **write:** add DataFrameWriter support to ts-spark-connector ([8c7fe26](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/8c7fe261f229ae9e8c58e902eebb132f85eb411b))
* **write:** add DataFrameWriterTF with PySpark-like API, format shortcuts, and examples ([81d382c](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/81d382cdfdf8f76744b2650c75be04133de2bfe6))

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
