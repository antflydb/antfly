# PostgreSQL regular-expression backend

This is an in-progress SQL backend, not the byte/FST matcher used by search.
It is not yet wired into public SQL and does not establish corpus-case coverage.

The vendored Henry Spencer/PostgreSQL ARE core is pinned to upstream commit
`1370a7832a2ab7bda5625fd1f0448808b534cd50` (REL_18_STABLE). C files originate
in `src/backend/regex`; headers originate in `src/include/regex`. Original
copyright notices and the PostgreSQL COPYRIGHT file are retained.

Adaptations are intentionally isolated and documented:

- `regcustom.h` selects the caller-owned portability layer; the original server
  configuration remains present under a disabled conditional.
- `regex.h` omits host regex types and PostgreSQL server headers in standalone
  mode. Exported engine symbols are namespaced by the portability header.
- `regcomp.c` selects a deterministic C-collation adapter. Locale classes and
  case handling are ASCII as in PostgreSQL C, but subjects, literals, dot,
  captures and offsets operate on Unicode codepoints. Other collations are not
  implemented and must not silently inherit host locale behavior.
- `regc_nfa.c` unwinds cancellation through each function's native return type.
- `rege_dfa.c` checks work on cached character transitions and backreference
  loops, not just cache misses. Sticky request errors prohibit exposing partial
  successful results. Upstream parsing and match precedence are retained.

Native allocations use a bounded caller allocator with complete ownership
tracking, including failed compilations. Class caches are per compilation and
freed before publishing the pattern. Matching uses separate scratch owners, so
compiled patterns do not retain a request budget or mutable matching state.
Execution-owned scratch reuses power-of-two size classes between rows and
occurrences. Physical cached and live blocks together obey the heap limit;
admission reclaims idle classes, and failures reset all borrowed request state.
No per-pattern mutable execution state is shared across concurrent callers.
Character-to-byte maps use a 32-bit checkpoint per 64 codepoints, rather than a
machine-word offset per character. Conversion examines at most 63 decoded
codepoints per boundary. Searches retain the original subject for anchor and
lookbehind semantics.

Run `zig build sql-regex-test` from `zig/`, or `zig build test` here, with Zig
0.17. Regenerate/check the independent fixture
from the repository root using `scripts/generate_sql_regex_reference.py --check
zig/lib/sql_regex/src/testdata/postgres.json` in the existing psycopg environment.
The oracle requires PostgreSQL 18+ and explicitly validates its C locale.
The additional `--global-matches --check
zig/lib/sql_regex/src/testdata/global-postgres.json` fixture verifies ten global
occurrence contracts (26 matches), including empty matches, Unicode positions,
anchors, lookbehind and unmatched captures, using PostgreSQL count/substr/instr.

The backend has no host-libc dependency. Memory/string helpers and allocation-free
heapsort are compiled freestanding without builtin libc substitution. Run
`zig build test -Dtarget=wasm32-freestanding` here to execute all 32 PostgreSQL
contracts twice in a WASM module with no host imports. Native concurrency uses
thread-local call context; single-threaded WASM saves/restores per-instance
context for nested synchronous invocations. The C call must never yield.

Remaining activation requirements include prepared/dynamic pattern admission, complete work
charging for complex DFA/backreference paths, SQL functions/NULLs/error mapping,
replacement expansion, and original mounted corpus campaigns.
The standalone test gate is not a claim that those layers are complete.
