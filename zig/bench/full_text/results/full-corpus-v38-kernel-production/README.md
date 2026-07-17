# Full-corpus v38 production kernel qualification

This directory is the checked-in raw result bundle from the final direct
Antfly-v38 versus Tantivy-0.25 kernel comparison. Both persistent adapters used
the same 5,032,105-document corpus, V1 query grammar, simple analyzer, BM25
`k1=1.2`/`b=0.75`, and their declared production segment states. The runner
performed analyzer and `VERIFY_TOP_N_COUNT` correctness gates before warmup or
timing. All five correctness queries matched strictly and neither adapter
reported an unsupported query.

The run used five shuffled timing repetitions after at least one second and
2,468 shared warmup queries, followed by independent three-second CPU/RSS
profiles per engine and query class. Median Antfly/Tantivy latency ratios were
1.121 term, 0.793 union, 0.662 intersection, and 1.070 phrase. CPU-per-query
ratios were 1.188, 0.796, 0.649, and 1.216 respectively. Thus the former 1.8x
term/phrase CPU gap is not present: Antfly is within about 19--22% for those two
classes and wins both boolean classes on this accepted sample.

`indexing.json` preserves build-time measurements from the reused index
manifests. The comparator's historical manifest records elapsed time but not
indexing CPU or peak RSS; the accepted same-index full rebuild memory result is
retained separately in `../full-corpus-v27-production-query-resources.json`.
No comparator rebuild was performed merely to duplicate that evidence.

The copied Antfly index manifest lacked a terminal newline; the checked-in copy
normalizes that single textual detail. Its parsed JSON content is unchanged.
Every other file is byte-identical to the runner output.
