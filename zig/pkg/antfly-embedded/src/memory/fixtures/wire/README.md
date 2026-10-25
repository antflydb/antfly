# Native memory read-response fixtures

These JSON responses come from native `Service.execute` calls in the policy test
`memory discovery and proposal wire fixtures are actual typed service responses`.
The test creates the `wire` space with table identity 7, admits two grounded
entities, discovers candidates, and submits one open merge proposal. It checks
the snapshots against the typed candidate-page, proposal-page and proposal-detail
contracts on every run.

Go, Python, TypeScript and Rust SDK tests decode the same files. This checks the
native producer and generated consumers together, including entity views,
proposal input revisions, null fields and the separation from mutation receipts.
When intentionally changing these contracts, refresh snapshots from the native
service and regenerate the public schema and clients with `make generate`.
