# LangChain.js validation

Review fixes were validated on 2026-10-28 with Node 24, LangChain core 1.3.0,
and LangGraph 1.5.3. The Python suite is the behavioral reference.

Current live profile: **62 LangChain tests and 7 LangGraph-store tests passed, zero skips**.
Builds and TypeScript checks pass; lint passes with warnings for dynamic graph
and test-backend types.

The tests exercise live Antfly retrieval, codecs, graph scopes, ownership, paths,
counts, memory runtime isolation and multimodal integrity. Added regressions
cover interrupted multimodal replacement at summary writes, cleanup and final
commit; cached read metadata; bounded concurrent embeddings; native scoped
single-field memory search and sticky exact multi-field fallback.

Raw command logs are local under `.state/langchain/` and are excluded from npm.
Package contents are limited to dist, README, LICENSE and package metadata.
The independent consumer verifies ESM/CommonJS, types and live persistence using
local tarballs. A separate registry-only release gate checks real npm dependency
availability. SDK 0.2.0 is still unavailable in the registry, so release remains
blocked until the SDK foundations ship, followed by LangChain and LangGraph.

Review suggestions to change `[id]` citations, to replace exact multi-field
pooling with vector-child records, and to change `not_submitted` commitment
semantics were declined to retain Python compatibility. The exact abstention
phrase remains part of the TS generation contract. Creation/update timestamps
now use the same atomic transform strategy as Python.

Native vision model quality and hosted CI remain unverified. No npm artifacts
were released during this review. Historical restart evidence remains in the
path-free JSON files under validation; previous baseline was 57+6 live tests.
