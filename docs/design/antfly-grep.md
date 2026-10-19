# Antfly-grep: indexed query planning

## Decision (2026-10-19)

Use Antfly's existing query API for indexed grep. Reuse its full-text predicates,
filters, projections, ascending-ID pagination, retained snapshots and authorization.
Do not add a grep-specific `grep_scan` request or per-document miss-proof protocol.
The CLI performs final matching with the same matcher used for local files.

The earlier response-elision experiment reduced a synthetic miss's response bytes,
but still read and validated its content on the server. Existing indexed substring
and regex queries can exclude nonmatching documents before transfer. A separate
API, source-contract dependency in the server, SDK fields and snapshot certificates
are unnecessary for this capability. End-to-end latency and cost still need
representative indexed workload measurements; reduced bytes alone do not prove
lower total cost.

## Table contract and configuration

Documents retain the versioned source contract described in
[the CLI design and usage](../../zig/pkg/antfly-grep/README.md): content, path, URI,
revision, timestamps, metadata, regions and permissions. Providers such as S3,
GCS, R2, Box, Dropbox and GitHub are ingestion concerns. Query planning uses the
stored document and its indexed revision, independently of the original provider.

Tables can declare an existing built-in substring companion:

```json
{
  "name": "sources",
  "document_field": "grep",
  "content_index": {
    "field": "grep.content._substring"
  }
}
```

Map `grep.content` with `"x-antfly-types": ["text", "substring"]` to obtain this
companion. An optional `content_index.full_text_index` selects an existing named
full-text index. Omit it for the active schema index. The configured companion
must cover the complete stored content at the same revision with the built-in
substring analyzer, without custom normalization, truncation or partial backfills.
Missing/misconfigured indexes return ordinary query errors; they must not silently
be treated as empty results. The existing optional raw-trigram `candidate_index`
remains supported, but the two index declarations are mutually exclusive.

Without an index declaration, the CLI uses the same query API with `match_all`
and any user filter, then matches each returned document locally. This is a full
traversal of a query result, not a separate scan API or remote-object reader.

## Conservative predicate planning

Every pattern alternative must supply a proven necessary case-sensitive byte
literal from the native matcher. Each literal must contain a contiguous ASCII
alphanumeric run of at least three bytes. Choose at most 32 bytes of that run and
lowercase it. The built-in substring analyzer indexes all normalized word
suffixes up to 32 bytes, so a raw match implies a candidate hit. Punctuation and
normalization can introduce false positives, which local verification removes.

For fixed strings, use the existing substring `match` predicate. For regex
patterns, use an existing `regexp` prefix (`fragment.*`) over the same suffix
field. This prefix contains only ASCII letters/digits; it does not translate or
approximate the user's entire PCRE/ripgrep expression. Combine alternatives with
OR. If any branch lacks a safe literal/run, omit the entire candidate predicate.
Bound alternative count and predicate size through the matcher's existing limits.

The ordinary request uses `full_text_search` for those candidates,
`filter_query` for the user's explicit scope, and optional `full_text_index`.
Keep ascending `_id` ordering from page one, replay the same predicate and scope
for every retained-snapshot page, and fetch only required source-contract fields.
Apply document permission/path checks and the complete local matcher afterward,
including case, line/word boundaries, captures, context and replacements.

Index normalization does not establish equivalence with original file bytes.
The current automatic path therefore uses conservative candidates even when a
particular literal could have an exact server-side equivalent. Further exact
translations require differential tests for their analyzer and regex semantics.
There is no approximate semantic retrieval in an exact grep plan.

## Exhaustive modes and accounting

Do not prune candidates when output needs nonmatching documents: inversion,
files-without-match, file discovery, include-zero, passthrough, or unrestricted
natural-language/jevgrep exploration. JSON and statistics modes preserve their
existing exhaustive source validation and file/byte totals by omitting candidate
pushdown. Explicit lexical decision prefilters can use candidates when ordinary
output semantics allow it. Unsafe encodings and unprovable pattern branches
retain the full query traversal as well.

The optimization does not manufacture corpus-wide counts from a candidate page.
A future API improvement should expose generic examined/returned/transfer metrics
and index capabilities through existing query diagnostics. Any change to the
meaning of JSON scan totals must be explicit and versioned, independently of
candidate filtering. Server query errors, authorization failures and snapshot
expiry retain the existing explicit failure/partial-result behavior.

## Verification and further work

Compare filtered and ordinary queries for literals, regexes, alternatives,
normalization/case false positives, Unicode neighbors, punctuation and line
boundaries. Verify fallback for unsupported patterns and exhaustive modes,
explicit filter composition, named-index forwarding and stable pagination.
Run real-server checks with one and multiple shards alongside concurrent-update,
snapshot-expiry, authorization and large-document tests.

Measure representative repositories and indexed corpora before extending the
planner: first-result latency, server/client CPU, response bytes, index footprint
and ingestion cost. Prefer existing index/query capabilities and shared query
observability. Extend safe predicate extraction only with no-false-negative
properties; avoid a new grep-specific server protocol.
