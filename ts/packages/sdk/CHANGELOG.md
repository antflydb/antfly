# Changelog

## 0.2.0 — unreleased

### LangChain integration foundations

- **Breaking behavioral change:** `tables.batch` and `tables.batchWithOptions`
  now throw `AntflyResponseError` when the server omits a response body. Earlier
  versions synthesized successful insert/delete counts from the request. An
  absent response does not establish whether a mutation committed: inspect
  stored data and visibility before retrying; do not automatically replay it.
- Table, index and document lookups expose `AntflyHTTPError` with HTTP status
  and response body, allowing missing records to be distinguished from other
  failures.
- Export `EmbedOptions`, including task type, dimensions, instruction,
  error policy and cancellation signal. Dense embedding and reranking calls
  propagate cancellation.

This work belongs to the SDK's already-declared, unreleased 0.2.0 version. Publish
and validate that SDK release before the LangChain and LangGraph packages; the
registry-only release checks intentionally reject an unavailable dependency.
