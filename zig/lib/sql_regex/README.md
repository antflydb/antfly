# Native SQL regular expressions

The required full-parity target and unfinished qualification work are tracked
in [the regex parity design](../../../docs/design/sql-regex-parity.md).
The validation below is a baseline, not the release-completion criterion.

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

## Bounded differential campaigns

Build the offline witness runner here with `zig build parity-probe
-Doptimize=ReleaseFast`. From the repository root, run:

```sh
uv run --no-project --with 'psycopg[binary]==3.3.6' python \
  scripts/check_sql_regex_parity.py --oracle-major 18 \
  --max-subject 3 --fuzz 5000 --seed 19 \
  --output /tmp/regex-witness.json --report /tmp/regex-report.json
```

That qualified 24,575-case C/UTF8 campaign passes against PostgreSQL 18.6.
Omit the major override to require PostgreSQL 19; use `ANTFLY_PG_BIN` to select
its installed binaries. There is no fallback or successful missing-oracle skip.
Use `--suite regressions` for the 41 permanent selection/rejection witnesses.
The regular native and WASM gates replay those witnesses without PostgreSQL;
WASM now executes 1,066 component contracts twice.

Artifacts record the exact reference build/binary and campaign bounds. The
native runner returns the exact witness digest and complete outcome count;
reports also pin its binary. An incorrect-witness positive control is mandatory.
Quota refusals are not parity successes. The campaign covers its declared
selection grammar, not every ARE shape or the complete public SQL surface.
See the design for remaining upstream, shrinking, collation and activation work.
