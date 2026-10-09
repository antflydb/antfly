# Native SQL regular expressions

SQL regex functions use the capture-capable native Zig interface in
`lib/regex/src/captures.zig`, exported as `antfly_regex.captures`. This is
separate from the byte-oriented matcher and FST automaton: those retain their
existing syntax, matching guarantees and search-index pruning contracts.
There are no vendored C sources, C bridge, host-libc or host-locale dependencies.

## Execution architecture

- Immutable Unicode-scalar programs expose typed syntax, case and newline
  options, character spans, caller allocators and explicit work/cancellation
  budgets. Errors are generic engine errors, translated at the SQL boundary.
- Regular matching merges equivalent Thompson states, including substring
  starts. Match extent is selected independently of capture dissection.
- Capture extraction follows ordered subtree preferences and positive-minimum
  repetition's final-copy binding. Width bounds and cached reverse-reachability
  frontiers avoid restarting a matcher for every candidate split. Small
  frontiers are inline and do not populate an execution-sized heap cache.
- Backreferences use an explicit bounded continuation stack. Regular capture
  subtrees bind deterministically before subsequent backreferences; alternative
  extents are admitted lazily from reusable forward frontiers. Nonregular
  patterns do not inherit a linear-time guarantee.
- Lookaround retains the original subject/anchor domain. Bounded local probes
  handle small assertions; execution-owned forward/reverse assertion frontiers
  prevent repeated failed unbounded assertions from rescanning each suffix.
- The SQL adapter retains bounded pattern/replacement LRUs and reusable
  allocation size classes. Live and idle scratch jointly obey actual-byte
  admission. Failed allocation, quota or cancellation exposes no partial spans
  or replacement output. No pattern retains a request budget or mutable matcher.

Classes and case folding deliberately follow PostgreSQL C collation (ASCII
classification/case, Unicode subjects and offsets). Other collations are not
implemented. Work and memory limits may reject expensive expressions; no claim
is made that every backreference or capture shape has linear complexity.

## Validation

Run `zig build sql-regex-test` from `zig/`, or `zig build test` here.
`zig build test -Doptimize=ReleaseFast` also runs a checked warm-owner benchmark.
Scaling regressions cover 4,096/16,384-character captures, ambiguous repetition,
late failures and unbounded assertions; fourfold input must stay within fivefold
charged work.

The independent PostgreSQL 18+ C-collation fixtures include 37 original span,
962 capture/syntax, 10 global-occurrence and 16 replacement contracts. They are
not proofs of all PostgreSQL syntax or original SQL corpus cases. The native
gate also sweeps allocation faults and cancellation checkpoints, tests warm
cache reuse, and shares immutable patterns across independent std.Io workers.

Use `scripts/generate_sql_regex_reference.py --capture-campaign --check
zig/lib/sql_regex/src/testdata/capture-postgres.json` in the psycopg environment
to recheck the expanded oracle. The existing span, global and replacement
fixtures have their corresponding generator modes. Freestanding WASM tests
verify PostgreSQL results without any host imports.

SQL binding, NULLs, overloads, diagnostics and statement/cursor ownership retain
their separate integration gates. This backend replacement does not award
additional original corpus-case credit.
