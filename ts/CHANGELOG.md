# Changelog

All notable changes to the Antfly TypeScript SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [@antfly/sdk@0.0.13] - 2026-03-09

### Added
- Step lifecycle SSE events for Answer Agent streaming
- AI Elements adapters and replication route types
- Embedding, DenseEmbedding, SparseEmbedding type exports
- API key and bearer token auth support
- MergeConfig and updated index types
- Sparse bool and consolidated index types

### Changed
- Rename `onAnswer` to `onGeneration` in Answer Agent streaming
- Standardize followup SSE event name and rename `onFollowUpQuestion` to `onFollowup`
- Update generated SDK types
- Cleanup text and audio chunking options
- Update dependencies

### Fixed
- Address code review findings across SDK

## [@antfly/components@0.0.11] - 2026-03-09

### Added
- ChatBar component library and chat playground
- AI Elements adapters integration
- Tabbed Pipeline/Response view using AnswerResults component
- Step lifecycle SSE events in Answer Agent streaming

### Changed
- Rename `onAnswer` to `onGeneration` in Answer Agent streaming
- Standardize followup SSE event name and rename `onFollowUpQuestion` to `onFollowup`
- Consolidate Vite chunks to single file
- Update dependencies

### Fixed
- Prevent stale closure from overwriting widget state in Listener
- Address code review findings across components

## [@antfly/sdk@0.0.12] - 2025-12-29

## [@antfly/sdk@0.0.11] - 2025-12-22

### Added
- Add eval streaming support for Answer Agent with `onEvalResult` callback
- Export eval-related types: `EvalConfig`, `EvalResult`, `EvalScores`, `EvaluatorScore`, `EvalSummary`, `EvaluatorName`

### Changed
- Update autogen from chat agent API updates
- Migrate biome config to 2.3.10 and apply lint fixes
- Update dependencies and fix peer dependency mismatches

## [@antfly/components@0.0.9] - 2025-12-22

### Added
- Add eval streaming support for Answer Agent in AnswerResults component
- Add tests for eval result handling

### Changed
- Remove redundant `showEval` prop from AnswerResults (eval results now shown when `evalConfig` is provided)
- Migrate biome config to 2.3.10 and apply lint fixes
- Update dependencies and fix peer dependency mismatches

## [Previous]

### Added
- GitHub issue templates for bug reports and feature requests
- Changelog file to track version history

## [0.1.0] - 2024-01-XX

### Added
- Initial release of the Antfly TypeScript SDK
- Full TypeScript support with auto-generated types from OpenAPI spec
- Support for both Node.js and browser environments
- Basic authentication support
- Table operations (list, create, drop, query, batch, backup, restore, lookup)
- Index management (list, get, create, drop)
- User management (create, delete, update password, manage permissions)
- Global query support
- Examples for browser and Node.js usage
- Comprehensive test suite
- CI/CD pipeline with GitHub Actions

### Features
- Type-safe API client using `openapi-fetch`
- Automatic type generation from OpenAPI specification
- Tree-shakeable ES modules
- CommonJS and ESM dual package support
- Zero runtime dependencies (only `openapi-fetch`)

[Unreleased]: https://github.com/antfly/antfly-sdk-ts/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/antfly/antfly-sdk-ts/releases/tag/v0.1.0
