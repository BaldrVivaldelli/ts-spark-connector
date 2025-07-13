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
