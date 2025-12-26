## [1.12.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.11.2...v1.12.0) (2025-12-26)

### Features

* add Agents ([e65e4aa](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/e65e4aa8a4885cac62a2d8d9c2798ec82057f5ec))
* add Agents ([34f5c20](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/34f5c20c2aed78deef9fafafbe8b55d1a4f611a8))
* add Agents ([705f744](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/705f74480763c5fb9a0d359a2e4705f42107407a))
* change readme & run ([517190b](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/517190ba3640ecd8686ba400353db6621f48389e))

## [1.11.2](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.11.1...v1.11.2) (2025-11-23)

### Bug Fixes

* add dest in type test ([f969e48](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/f969e48058cbf6f379089c953bb8cd66513a30f7))
* fix test ([6cd8029](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/6cd8029dc79e36def5f8b735541ce7d2a8a4009b))
* typo in index ([9b8fe0b](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/9b8fe0b8a95d4bdbbf3489ee9d281408029a803a))

## [1.11.1](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.11.0...v1.11.1) (2025-10-22)

### Bug Fixes

* add spark-default.conf ([4531d2f](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/4531d2f12062383116d1e8d0523bce8e7ce4ef3e))
* fix ci ([fe58de3](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/fe58de343762ff010d721183ea8d1552821d917e))
* fix docker and entrypoint ([aa84aa6](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/aa84aa61de7ee696838299526eaa995d6984fc2d))
* fix workflows ([dfc3e39](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/dfc3e3996c0bf8ab1b77551cbbde8e6d050e276f))
* test ([d44aba8](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/d44aba8511a956aed7dd8f0438dc69b1e6658ec8))
* test remove save test ([a219e61](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/a219e61e6dad7ced7819b686e8d0dbae43e9ada5))
* update docker compose ([ba694f5](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/ba694f518af409a0ab5e48ead5482531f188dbb8))
* update spark-default.conf ([ed14443](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/ed144430db0b5aba507d0112816de902020a7d7b))
* use official docker image ([2d45241](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/2d452411c6e8cbc712a90ccb2e7f59e68b5932ac))

## [1.11.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.10.0...v1.11.0) (2025-09-21)

### Features

* add fromTempView syntax ([d2f5500](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/d2f55008e400b398e1b8e72abc0b7c1ff4dd536b))
* add fromTempView syntax ([2eb9362](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/2eb9362b87d198d05fad267e01c15f18318fba78))
* add fromTempView syntax ([fa54796](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/fa54796012a2a73fdb3ee5e4fd3aea5bc2d4f082))

## [1.10.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.9.0...v1.10.0) (2025-09-20)

### Features

* refactor write streaming ([03a0e86](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/03a0e86d99adf3361ab1c4138e9c98e0263a5cd2))

### Bug Fixes

* fix workflow ([3a65441](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/3a65441d4929d2aecbb67867b3ee98585912f48a))

## [1.9.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.8.0...v1.9.0) (2025-09-15)

### Features

* add Docker-based test environment with complete Spark setup ([402377d](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/402377d7e27b6b611b418aaf8ba11619a8cb68b1))
* docker test environment ([afea97b](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/afea97bce4e9d23c44422036dd6c2cd45802b44f)), closes [#28](https://github.com/BaldrVivaldelli/ts-spark-connector/issues/28)

### Bug Fixes

* lazy Spark client creation and exclude E2E tests from unit test runs ([7d90ec6](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/7d90ec6fc091f7ecc229bfc1a306152eb8e17030))
* update to Docker Compose v2 commands for CI compatibility ([8a686fa](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/8a686fa5acc5b0a97e0c157ed9ffaad4110daae8))
* use getClient() in explain method and remove debug logging ([99a47b4](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/99a47b41a577512c04dbce6400508c14db5c38c4))

## [1.8.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.7.0...v1.8.0) (2025-09-14)

### Features

* refactor for Streaming Algebra so it dont colide with batch algebra ([7174081](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/71740811502a8912e8695f14a5fa1bca072d68f1))
* spark stream part 4 ([da6a88f](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/da6a88fd289ccff963fca2d562a1d757b143c2f9))

## [1.7.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.6.0...v1.7.0) (2025-09-07)

### Features

* se agrega nueva feature sample(...), randomSplit(...) ([1463140](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/1463140504b9d7f0235c1cb5aca2fbe609b8fc53))

### Bug Fixes

* add correct email ([de7ead9](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/de7ead9267d5ebd1a492961dc6c020e74fb952b3))
* fix typo ([5def24f](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/5def24fd2b621b236a5ff16408556be4962e6a9c))

## [1.6.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.5.0...v1.6.0) (2025-09-07)

### Features

* add broadcast and hints, with test e2e ([bfc6e60](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/bfc6e602f66ee95ab18fcaffe95babd296e8b53f))
* modifiy readme, add trace feature and configs ([649766a](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/649766a5859cfdf47909217d5be03a7e3c060fbe))

### Bug Fixes

* add type impl ([b713ba7](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/b713ba7f930122f6577aa8142152337db1642778))
* refactor condition ([73b7ac2](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/73b7ac2ce57a382d51eb1cf011455be89b023bd4))
* refactor condition ([92093d1](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/92093d15924fede122b1e96dbeea18f846c0f1a8))
* test condition ([8593b67](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/8593b675fa375f0c4aa00a9ca3f3c589fe4ce487))

## [1.5.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.4.0...v1.5.0) (2025-08-31)

### Features

* add table temp create and .sql creation ([1bddf77](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/1bddf778970b98cc0a0b79002a395d45cb23ee0c))

## [1.4.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.3.0...v1.4.0) (2025-08-30)

### Features

* add from_json & to_json ([8e3786e](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/8e3786e68f7f19a248e0505d10e5e4c87cc71f58))
* finish part 2 ([4fc466f](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/4fc466f210332b48c872f79d3201177bd1440555))

### Bug Fixes

* add explain ([f154843](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/f154843a3952e36e22fd01a11dc6a4a5149120c8))

## [1.3.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.2.0...v1.3.0) (2025-08-30)

### Features

* add complex types, unionByName ([cf576a5](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/cf576a5ea45a3559bef8ae06f6f5a32dd2190c4c))
* **read:** implementa describe() y summary() en ReadChainedDataFrame. Se agregan combinadores tagless-final sobre Spark Connect sin extensiones. Un solo Aggregate global + Projects y UNION ALL. Guard numérico con WHEN/ELSE para evitar CAST_INVALID_INPUT. Soporte de percentiles en summary. Proyección temprana de columnas. Ejemplos y tests e2e añadidos. ([64a6184](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/64a6184452dab14732a6c03506ae3d1516b80c36))

## [1.2.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.1.0...v1.2.0) (2025-08-17)

### Features

* **fix:** add release rules ([d7ea363](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/d7ea363c339faa249508c4c6a5517525416c58b1))

# [1.1.0](https://github.com/BaldrVivaldelli/ts-spark-connector/compare/v1.0.0...v1.1.0) (2025-08-17)


### Features

* **fix:** update readme ([7e78808](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/7e7880827b8b83ebe930c7fb0e25f3df9d2bd8b2))
* **refactor:** format package json ([ccd35c8](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/ccd35c8db2db86d5efcae6596dda62796b92578a))
* **refactor:** format package json ([9c13b5e](https://github.com/BaldrVivaldelli/ts-spark-connector/commit/9c13b5e94c85b5c1b7c8a11644a028cd5e502a4e))

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
