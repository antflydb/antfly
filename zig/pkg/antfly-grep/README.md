# antfly-grep

A standalone native Zig CLI for regex/literal search and natural-language source
search. It uses Antfly's regex, HTTP, and inference interfaces. Building and
running the CLI requires no Rust toolchain, ripgrep executable, shell subprocess,
database, or persistent index. Regex execution, PCRE-style extensions, decompression,
transcoding, discovery, source analysis, and output are implemented in Zig in this
monorepo. There is no libpcre2 or libarchive dependency. Explicit `--pre` and
`--hostname-bin` options run the user-specified program.

This implements local and table-backed search for
[#1151](https://github.com/antflydb/antfly/issues/1151). Table profiles select
indexed documents using the versioned schema below. Connector ingestion and
remote-source retrieval remain separate work.

## Build and test

Use the repository's pinned Zig 0.17.0, from `zig/`:

```sh
zig build -Dgrep-only=true grep grep-test
./zig-out/bin/antfly-grep --help
```

The `grep`, `grep-run -- ARGS`, and `grep-test` targets also exist in the regular
build graph. `-Dgrep-only=true` avoids server, GPU, model runtime, database, and
OpenAPI build dependencies. Tests require Python 3 and use local HTTP and native
inference fixtures; no model download or provider key is required.

## Exact search

```sh
antfly-grep -n 'Retry[A-Za-z]*' ./src
antfly-grep -Fi 'context canceled' .
antfly-grep -w '\p{Greek}+' .
antfly-grep -e timeout -e retry -t go .
antfly-grep -nC 2 budget ./src
antfly-grep -o '(?P<name>retry)\w*' -r '${name}' ./src
antfly-grep -U 'begin\nend' ./src
antfly-grep --files -0 .
printf 'hello\nworld\n' | antfly-grep hello
antfly-grep --json retry ./src
```

Without paths, nonterminal stdin is searched; otherwise the current directory is
searched recursively. Use `.` to force file search in a pipeline, `-` for stdin,
and `--` to end options. Non-multiline exact search and semantic stdin processing
stream results before EOF. There is no default 16 MiB input ceiling.
`--max-filesize 16M` skips oversized discovered files; `--max-file-size BYTES`
sets a hard read ceiling. Multiline and semantic regular-file search buffer the file.

Discovery supports ancestor and nested `.gitignore`/`.ignore`, Git worktree
indirection, `.git/info/exclude`, global Git excludes including `core.excludesFile`,
explicit ignore files, positive/negative globs, case-insensitive globs, type filters,
depth limits, and optional symlink following. `.ignore` rules take precedence over
Git ignore rules. Ignored directories are pruned. Explicit paths bypass discovery
filters. `--hidden`, `--no-ignore`, and repeated `-u` relax filters. Cyclic symlinks
are detected. `--sort path` gives stable traversal/output ordering; otherwise
multiple files are scanned concurrently through a bounded discovery queue. Workers
share immutable compiled patterns. Ordinary line search uses 64–256 KiB buffered
reads; automatic mmap is reserved for multiline workloads. `--mmap` requests
private read-only mappings and `--no-mmap` forces reads. Explicit mmap can increase
resident memory by the size of the pages touched.

The native ordered Thompson/Pike regex engine supports Unicode 16 literals,
properties, Script_Extensions, categories, age and break properties, simple case folding, word boundaries, captures,
named captures, scoped flags, lazy/greedy repetition, alternation, assertions,
class intersection/subtraction, and ASCII POSIX classes. `--no-unicode` selects
byte matching. The existing FST/schema regex dialect remains unchanged.
`-P` / `--engine=pcre2` selects a separate, bounded native backtracking engine.
`--engine=auto` falls back to it for nonregular patterns. It supports lookaround,
numeric/named backreferences, atomic groups, possessive quantifiers, branch-reset
groups, recursive/subroutine calls, capture/recursion conditionals, `\K`, `\G`,
`\Q…\E`, `\R`, `\X`, duplicate-name mode, numeric/string callouts, and
FAIL/ACCEPT/SKIP/PRUNE/COMMIT/THEN/MARK control verbs. The library callout API can
continue, reject a branch, or abort; CLI callouts continue without callbacks.
`\X` reuses Antfly's Unicode grapheme iterator. This is an Antfly implementation
of PCRE-style semantics, not a binding to upstream PCRE2. Automatic engine
selection applies to the whole pattern set, including empty-match UTF behavior.
Unsupported extensions fail explicitly. Compiled states, branch memory, recursion, and match steps are
bounded; `--pcre2-match-limit` controls the per-candidate-start step budget.
Backtracking capture buffers reuse a bounded per-worker cache. Lookbehind compiles branch-specific character bounds, preserves alternative
priority, and rejects unbounded/oversized assertions. Single-predicate repeats
use native scanning kernels with constant-size retry state; subroutine calls
share recyclable immutable snapshots instead of copying return stacks. The default engine remains linear-time. A bounded lazy DFA accelerates supported boolean searches; the VM
handles unsupported assertions and capture recovery. Direct literals bypass regex
compilation, with SIMD exact/ASCII-folded search and a bounded-work two-way
fallback. Multiple mandatory literals share a bounded Aho-Corasick prefilter.
Per-worker VM scratch is allocated on demand, and output modes request only the
spans or captures they need. Count-only and plain statistics searches accumulate
totals without retaining match lists; one-byte literals use SIMD counting with
Unicode folding equivalents. DFA/prefilter allocation quotas include array/hash
capacities and temporary growth buffers.

The backtracking engine has a native ARM64/x86_64 JIT for consume chains,
with a 1 MiB code cap and W^X memory. ASCII predicates are emitted directly;
Unicode uses shared native helpers within the generated chain. Control flow
continues through the bounded VM.
Single-predicate subroutines are proven free of visible state changes and
inlined into those chains; other calls retain capture restoration. It preserves
the VM step budget. This is not upstream PCRE2's JIT and does not
establish full dialect parity. `--debug` reports compiled JIT code usage;
`--pcre2-version` identifies the implementation.

`-z` searches gzip, bzip2, XZ, LZMA, Zstandard, LZ4, Brotli, lzip, and Unix-compress files
using native decoders. Checksums and concatenated frames are validated where the
format defines them. Archive containers are not extracted. Decoded content is
streamed for ordinary searches through bounded buffers and codec history windows.
Multiline and semantic searches require random access. On macOS/Linux, decoded or
preprocessed streams larger than approximately 1 MiB spill to a private, immediately
unlinked temporary file and are mapped for searching; `TMPDIR` selects the directory.
This bounds snapshot heap use, though accessed mapped pages still contribute to RSS.
Windows currently retains the in-memory snapshot fallback.
`--dictionary PATH` supplies raw Brotli/LZ4 dictionaries or raw/formatted Zstandard
dictionaries. `--max-decompression-window SIZE` bounds history allocation (64 MiB
by default), including Brotli large-window streams. Serialized/transformed Brotli
shared dictionaries support RFC 9841 word/transform lists, context maps, and
LZ77 prefixes. Dictionary bytes are loaded once per search and prepared metadata
is shared by workers. `--dictionary-format auto` recognizes the RFC signature;
`raw` bypasses recognition, `brotli` requires RFC serialization, and
`brotli-google` explicitly accepts Google's experimental transform-triplet order.
The two serialized orders can be ambiguous, so they are never guessed.
`--max-dictionary-size` bounds the payload separately from the decoder window. Bzip2 uses the monorepo’s native bulk
CRC-32/BZIP2, batched output, a bit reservoir, minimum-length canonical Huffman
decoding, and packed inverse-BWT links. XZ supports bounded LZMA2, delta chains,
and x86/PowerPC/IA64/ARM/Thumb/SPARC/ARM64/RISC-V branch filters.
The Brotli decoder is adapted from the pinned pure-Zig
zig-parquet implementation with its MIT notices and format dictionary preserved;
see `zig/lib/compress/src/brotli/decode.zig`. `--encoding` accepts the WHATWG encoding labels, including
UTF, Windows/ISO single-byte encodings and Japanese, Korean, and Chinese encodings.
BOM detection is automatic; `--encoding none` preserves bytes. Transcoded stdin
streams complete records before EOF. `--pre PROGRAM` / `--pre-glob GLOB` supports
explicit preprocessing.

Output includes match spans (`-o`), capture replacements (`-r`), color, multiline,
context, columns, byte offsets, headings, counts, filenames, NUL separators, JSON,
and quiet mode. `-n` enables line numbers; `-H`/`-I` control filenames; `-h` shows help. Defaults
adapt to terminal output. `--help` shows help. Context/capture offsets refer to
searched bytes (UTF-8 after transcoding). Replacements change output only.
`--colors`, `--hyperlink-format`, `--vimgrep`, `--pretty`, `--include-zero`,
custom separators, timestamp sorting, `--debug`, and `--trace` are available.
`--generate man|complete-bash|complete-zsh|complete-fish|complete-powershell`
emits local tooling assets.

## Natural-language search

```sh
export ANTFLY_GREP_MODEL=your-decision-model
export ANTFLY_GREP_ENDPOINT=http://127.0.0.1:8090/decisions
antfly-grep --ask 'Where is the retry budget enforced?' ./src
antfly-grep --ask 'Reports a failed payment' --unit line server.log
antfly-grep --ask 'Where are retries stopped?' -e 'retry|attempt' ./src
antfly-grep ask 'Where are permissions checked?' --threshold 0.7 --json .
antfly-grep cache clear
```

The default HTTP backend uses the public [decision contract](../../../docs/guides/decisions.md).
`--endpoint` accepts the full decision URL; `--base-url` appends `/decisions`.
`ANTFLY_GREP_TOKEN` supplies the bearer token. `--timeout-ms` bounds requests.
Redirects and cookies are disabled. Malformed, duplicate, missing, out-of-range,
or similarity-only responses fail the search rather than becoming nonmatches.

For in-process inference, use the existing native Antfly inference library:

```sh
antfly-grep --ask 'Where are retries stopped?' --backend local \
  --library ./libantfly.dylib --models-dir ./models --model your-model ./src
```

The library is loaded through Antfly's native C ABI. No model server or Rust
component is required. `ANTFLY_GREP_LIBRARY` and `ANTFLY_GREP_MODELS_DIR` can provide
the paths. The CLI does not download models. The test fixture verifies ABI
integration; real-model quality and hardware support require a configured model.

`ask QUERY` explores directory listings, file paths/previews, declarations, and
individual lines. Native source analysis masks comments/strings, tracks balanced
syntax and nesting, and records declaration names/ranges and call locations.
Zig analysis uses the compiler's native AST. JavaScript, JSX, TypeScript, TSX,
and declaration files use the pinned native Zig Yuku grammar, including generics,
decorators, overloads and optional calls. Python, Go and Rust use complete pinned grammars with native Zig GLR parsing,
lexical automata, and Python/Rust scanners. The runtime shares immutable tables,
merges ambiguous parse paths at each input position, and caps parser allocations.
Go generic invocations retain call syntax; conversion and macro candidates carry
explicit kinds in JSON. Macro expansion and semantic type resolution require
information beyond syntax. See `zig/lib/syntax/README.md` for pins and conformance. Unsupported or incomplete syntax falls back to bounded chunks.

Line discovery evaluates lines even inside rejected blocks. Completed line judgments
control selection even when every line is rejected; accepted region scores remain
separate evidence. `--no-refine`
retains accepted regions instead. `--broad` bypasses directory/file pruning and
asks about broader relationships. Rich `ask` output reports separate preview,
region, and line scores, declarations, calls, reading leads, and explicit excerpt
omissions. A qualified file preview can be a reading lead without a matching line.
The `--ask QUERY` spelling retains grep-style output and regex prefilters.

Files and requests run concurrently (`-j`/`--concurrency`). Results rank by file
relevance; `--no-rank` preserves source order. Repeated `-e` questions and
`-f`/`--filter`, `--only`, and `--not` policies in `ask` mode share a single source
upload with a question matrix. In `--ask` mode, `-e` remains a regex prefilter and
`-f` a pattern file. `ask -n` limits reported files, `-T` sets the file threshold,
and `-t` the region threshold. Defaults in `ask` mode are 150 chunk lines, 12 model
context lines, file threshold 0.6, 15 result files, 5 displayed regions per file,
and 32 request lanes. Use `--help` for the separate grep-style defaults.

Transient requests retry with exponential backoff and jitter, respect numeric and HTTP-date
Retry-After (including obsolete HTTP-date formats), and reduce concurrent request lanes until the provider recovers.
Oversized batches split, including newline-based subdivision of a rejected single
input. Each attempt consumes the request budget. Valid partial results survive
terminal failures and incomplete searches exit 2.

`--batch-size` defaults to 8 (maximum 128). Cache keys include endpoint/model,
backend, authorization scope, question, and rendered input. Decision records
contain scores, not source bodies or tokens. The separate private source-preparation
cache contains line offsets, declaration/call names and ranges. Use `--no-cache`,
`--cache-dir`, or `cache clear`. Pin `--model-revision ID`, or clear the cache after
replacing a model behind an unchanged identifier.

`--max-requests` and `--max-output-bytes` bound work. In `--ask` mode,
`--max-regions` and `--max-source-bytes` are evaluation/input budgets; in `ask`
mode they limit displayed regions per file and source bytes per excerpt. Exhaustion retains completed results and exits 2. `-m` limits printed
matches; it is separate from model budgets. `-c` counts selected lines.

## Output, compatibility, and limits

Exit codes: **0** matches, **1** complete search with no matches, **2** invalid
arguments, failures, or incomplete search. Diagnostics use stderr. Closed output
pipes terminate work. JSON emits per-file `begin`, `match`/`context`, `end`, and
final `summary` records, with ripgrep-shaped `data` and Antfly extensions for
probabilities and completion/request statistics. Invalid UTF-8 uses base64.

Configure an Antfly decision endpoint or the TypeSafe provider explicitly:

```sh
printf '%s' "$ANTFLY_GREP_TOKEN" | antfly-grep auth --provider antfly \
  --endpoint http://127.0.0.1:8090/decisions --model MODEL --stdin
printf '%s' "$TYPESAFE_API_KEY" | antfly-grep auth --provider typesafe --stdin
antfly-grep doctor
antfly-grep skill --agent codex
```

Running `antfly-grep auth` in a terminal prompts for a provider, any required
endpoint/model, and a hidden token. Presets include Antfly, TypeSafe, Vercel AI
Gateway, OpenRouter, OpenCode Zen, local inference, and custom TypeSafe gateways.
Custom gateways require HTTPS or loopback HTTP. Configuration is written atomically
with owner-only permissions on POSIX. Terminal echo is restored after entry and
on POSIX SIGINT/SIGTERM. Shell completions include value choices, file types,
option arity, paths, and setup commands.
`doctor` uses a synthetic request. Agent installation supports Codex, Claude,
and OpenCode; `--global` chooses the home directory. Model inference can also
use the monorepo native library described above. No model is downloaded implicitly.

Table-backed searches use the same matcher, decision pipeline and renderers as
local files. Configure an endpoint and tables using
[schema/tables.example.json](schema/tables.example.json), then run:

```sh
export ANTFLY_GREP_TABLE_TOKEN=... # independent of the decision-provider token
antfly-grep --table-config tables.json --table sources -n 'retry|backoff'
antfly-grep ask 'Where is the retry budget enforced?' --table-config tables.json \
  --model MODEL --endpoint http://127.0.0.1:8090/decisions --json
# Explicit filesystem paths combine local and indexed sources.
antfly-grep --table-config tables.json --sort path -l 'retry' ./src
```

`ANTFLY_GREP_TABLE_CONFIG` can supply the configuration path. With no explicit
filesystem paths, table searches do not read stdin or implicitly walk the current
directory. Repeated `--table` selects configured aliases; omitted selectors scan
all configured tables. The default `prefetch: true` overlaps one retained-snapshot page
with matching and unlimited-budget decisions using one persistent producer. At most two decoded pages are
live (each bounded by `max_page_memory`, plus bounded HTTP response buffers). Set
`prefetch: false` for sequential retrieval. Quiet searches and finite per-file match
limits disable speculation. Decision searches with any finite request budget (including the default 1,000)
retrieve sequentially so speculative pages cannot displace foreground evaluations.
Use `--max-requests=unlimited` to opt into overlapping retrieval and decisions. Document/request limits and
cancellation remain enforced.
`--table-filter JSON` passes an additional Antfly query
filter before pagination. Filters, globs, file types, counts, replacements,
context, quiet mode and lexical/decision output share their local implementations.
Git ignore files describe local worktrees; indexed searches use explicit path/type
filters and the indexed document's permission metadata. Indexed timestamp sorting
requires the corresponding stored timestamp; missing values produce an explicit
partial-result error rather than an invented ordering.

Each source document follows [document-v1.json](schema/document-v1.json): a `grep`
object contains `version: 1`, canonical source `uri`, logical `path`, extracted UTF-8
`content`, nonempty `revision`, `indexed_at` in UTC Unix seconds, and
`permissions: {public: boolean, readers: [principal, ...]}`. Optional `metadata`,
`regions` (one-based inclusive line ranges), and source timestamps preserve
provider details. The configured `document_field` can point to a different dotted
object path with the same contract. S3, Box, Dropbox, GCS, R2 and GitHub ingestion
all feed this contract; the CLI does not download provider files or implement
connector synchronization. Content and metadata must describe the same revision.

The Antfly endpoint must enforce its authenticated table/document access policies.
Configured `principals` additionally intersect source permission metadata before
any document reaches matching, decision evaluation or output. Without principals,
only public documents pass that check. This client-side intersection does not
replace server authorization. CLI credentials never enter source provenance, fixtures
or query-cache records, and credentialed requests do not follow redirects.

Exact searches enumerate projected documents in stable ID order and replay
`remote_snapshot` with each typed `_sort` cursor. Missing, changed or expired
snapshots fail explicitly and retain earlier results; scans never restart silently
or switch to an approximate candidate set. Transport/429/5xx retries use bounded
backoff and Retry-After, including HTTP dates. Oversized/rejected pages shrink around the same cursor and recover gradually
without duplicating processed documents. Compiled matchers, scratch, models and
HTTP connections are reused across documents. Pages are released after processing; only continuation state and at most one prefetched page survive. `max_page_memory` separately caps JSON expansion and
page allocations (64 MiB by default), including array capacities and temporary
growth. Response bytes, requests, scanned documents, source
bytes and output have explicit limits. `--max-index-documents` defaults to 100000;
`--max-requests` is shared by index and decision requests. Ranked/sorted output
has a 64 MiB retained-output cap, adjustable with `--max-pending-output BYTES`.
Budget exhaustion produces partial results and exit 2. Exact enumeration can be
expensive on large tables; an explicit indexed filter narrows its scope without
changing the requested regex semantics.

JSON result events retain grep-compatible data fields and add
`result_schema_version: 1` and a common `source` object. Local provenance is live;
indexed provenance reports table/document ID, canonical URI, stored revision,
index and observation times, snapshot, metadata and optional source regions.
Rich decision output exposes file/region/line scores, declaration and call ranges,
selection thresholds and whether line refinement was used. Human decision output
also labels indexed revisions and index time. Probabilities explain the selection
rule; the CLI does not manufacture model rationales. Exit codes remain 0 for a
qualifying result, 1 for a complete search with none, and 2 for incomplete/error
results (quiet match success follows grep behavior). POSIX SIGINT/SIGTERM cooperatively
cancel provider/table HTTP requests and produce an incomplete summary. Idle stdin can be canceled without waiting for its producer to close. A second
interrupt forces exit. Windows signal handling remains the platform default.

The [compatibility matrix](COMPATIBILITY.md) records tested behavior and explicit
limits; `tests/check_compatibility.py --check` guards its evidence and fixture counts.
Local search has 292 additional checked-in differential cases covering both
engines, Unicode/output combinations, binary delimiters and malformed encoding.
Generate them with `tests/generate_local_fixtures.py` and the pinned test-only oracle.

A labeled coding-search corpus covers retry bounds, input validation, atomic cache
publication, permission checks, and a query with no relevant source. Run `zig build -Dgrep-only=true grep-eval` for
fixture-backed precision/recall/nDCG and request/token accounting. The fixture
intentionally rejects declaration regions so independent line discovery is tested.
These scores measure pipeline behavior, not real-model relevance. For an explicitly
authorized provider measurement, run `bench/evaluate.py BINARY --endpoint URL
--model MODEL --output REPORT`; `--token-env` chooses the credential environment
variable. Provider runs are opt-in; ordinary tests contact only local fixtures.
Decision score caches expire after 24 hours by default; `--cache-ttl SECONDS`
sets their lifetime and zero bypasses them. Cache keys include complete inputs,
questions, policies, model, endpoint and credential scope. Malformed, expired or
incomplete cache records are misses. Batch records use atomic replacement; the
fixed-size score table validates checksummed records and rejects torn writes.

Ordinary parallel searches flush completed files so a slow file does not hold
back earlier results. Explicit `--block-buffered` retains block buffering; ranked
and sorted results retain their required ordering.

Performance measurements now include `startup` and `first-result` cases. Each case
records first stdout-byte latency in separate drained runs alongside throughput,
CPU, RSS and output hashes. `--repository PATH` additionally measures file discovery
and regex file selection on an existing repository, recording its corpus identity.
For example:

```sh
zig build -Dgrep-only=true -Doptimize=ReleaseFast grep-bench -- \
  --mib 4 --rounds 5 --repository pkg/antfly-grep \
  --case startup --case first-result --case repository-files --case repository-search \
  --output /tmp/antfly-grep-latency.json
```

The [unified-search benchmark](bench/results/macos-arm64-unified-search.json)
records five paired rounds over 4 MiB inputs, a 1,000-file synthetic repository,
and the actual 29-file native source tree. Output hashes agree with ripgrep.
Completed-file flushing reduced first-result latency in the 1,000-file case from
21.7 ms to 9.7 ms versus the saved pre-flush binary; native peak RSS was about
4.5 MiB. Startup measured 8.3 ms in this run. Elapsed time and first-result latency
use separate alternating rounds, so their medians need not describe the same
execution. Host load and every run are preserved; these are measurements on a
busy shared host, not general performance guarantees. Explicit block buffering
remains available for bulk-output workloads.

The [labeled fixture report](bench/results/macos-arm64-relevance-fixtures.json)
records all five queries passing file and line precision/recall/nDCG, including
the no-result query. It identified and guards against empty rejected files being
reported as reading leads. The report explicitly identifies fixture scoring and
provider-reported token-counter limits; it does not establish live-model quality.

The integration suite exercises actual executable behavior with HTTP and native
inference fixtures, and uses installed ripgrep as an optional development oracle.
The differential target is ripgrep 15.2.0 with PCRE2 10.45; older versions differ
in some Unicode-property and discovery behavior.
Cross-compilation covers Linux and Windows; platform runtime checks are separate.

**Compatibility is still under development.** Tests cover the implemented PCRE
control verbs/callouts, duplicate names, late binary detection, absolute-anchor
replacement context, unterminated-line empty matches, mmap count limits and JSON
statistics, Unicode namespaces, Git configuration continuations, and remote-URL
conditional includes. This coverage does not establish every upstream
dialect, flag combination, or gitwildmatch edge case. The 76 pinned PCRE2 10.45 golden fixtures additionally cover non-atomic assertions,
script runs, substring scans, recursion conditions, extended classes, Unicode
POSIX classes and lookbehind branch priority. Generated grammar conformance
compares 328 valid upstream named trees. These checks establish specific behavior,
not universal PCRE2 or JIT parity. Provider checks currently use fixtures;
live-provider testing is deferred at the user’s request. Windows validation uses
cross-compilation; macOS and Linux have runtime checks.
Regenerate the synthetic PCRE fixtures with a test-only ripgrep 15.2.0 /
PCRE2 10.45 executable:

```sh
python3 pkg/antfly-grep/tests/generate_pcre2_fixtures.py /path/to/rg > pkg/antfly-grep/tests/pcre2_dialect_fixtures.json
```

Use the reproducible benchmark to assess performance:

```sh
zig build -Dgrep-only=true -Doptimize=ReleaseFast grep-bench -- \
  --mib 32 --rounds 3 --output /tmp/antfly-grep-benchmark.json
```

The [original macOS ARM64 baseline](bench/results/macos-arm64.json) and
[full 32 MiB benchmark](bench/results/macos-arm64-final.json) preserve binary hashes,
versions, sizes, host load, every timed run, child RSS/CPU/page faults, and output
hashes checked against ripgrep outside the timed measurements. That full run
uses a 32 MiB corpus and five measured runs after warmup:

| Case | Throughput | Peak RSS | Time / ripgrep |
|---|---:|---:|---:|
| Literal miss | 3.12 GiB/s | 2.97 MiB | 1.10× |
| Regex prefix miss | 2.58 GiB/s | 3.02 MiB | 1.02× |
| Regex prefix hit | 2.72 GiB/s | 3.06 MiB | 0.87× |
| Case-insensitive literal | 959 MiB/s | 3.22 MiB | 0.63× |
| Unicode word / `-o` | 196 MiB/s | 3.28 MiB | 0.90× |
| Boolean lazy DFA | 379 MiB/s | 3.28 MiB | 1.00× |
| Small files, unsorted | — | 4.62 MiB | 1.01× |
| Streaming gzip | 1.33 GiB/s | 3.08 MiB | 0.62× |
| Streaming bzip2 | 88 MiB/s | 9.53 MiB | 2.05× |
| Streaming XZ | 574 MiB/s | 12.86 MiB | 0.42× |

These paired runs alternate executable order. Host load was elevated (14–21),
so elapsed-time ratios should be read with the recorded CPU times and individual
runs. Codec comparisons use ripgrep’s external decompressors. These are local
warm-cache measurements, not a throughput/RSS parity guarantee.
The original literal-miss baseline was approximately 2.2 GiB/s with 35 MiB RSS;
the default now uses buffered I/O. Explicit mmap still reaches approximately
34.5 MiB RSS on this corpus. Backend flags are requests: a reference executable
may fall back to streaming. Compare the recorded binaries and actual backends
before drawing conclusions about mmap itself. Regex patterns with different
structure, larger repositories, cold storage, codecs, and model providers need
separate measurements.

The [verified 4 MiB follow-up](bench/results/macos-arm64-count-codec-verified.json)
compares the installed binary, an earlier Antfly binary, and ripgrep in nine
alternating rounds after our compiler and test jobs finished. Dense
case-insensitive counts fell from 52.1 ms / 183.7 MiB RSS to 6.0 ms / 8.5 MiB;
ripgrep measured 33.4 ms / 10.8 MiB in that case. Counting no longer retains one
match object per occurrence. The same experiment records bzip2 logs at 38.9 ms
versus ripgrep’s 29.2 ms, and incompressible data at 145.1 ms versus 171.4 ms.
Host load remained elevated from other work; CPU times and all runs are preserved.
These results describe those inputs, not general parity. Codec references launch
external decoders; recorded RSS is child accounting rather than summed simultaneous
process-tree memory. The separate [32 MiB bulk-CRC experiment](bench/results/macos-arm64-bzip-bulk.json)
records 119 MiB/s for bzip2, compared with 88 MiB/s in the earlier full benchmark.
Use `--extended`, repeatable `--case NAME`, and `--baseline PATH` to reproduce
focused comparisons without rerunning unrelated search cases.

The [native PCRE follow-up](bench/results/macos-arm64-native-pcre.json) records
seven alternating rounds over 1 MiB inputs with output hashes checked against
ripgrep. Compared with the earlier Antfly binary, single-predicate repeats fell
from 32.3 to 5.7 ms, Unicode chains from 20.1 to 12.4 ms, and the simple-subroutine
case from 16.3 to 8.7 ms. Ripgrep measured 5.5, 9.2 and 16.1 ms respectively.
Native peak RSS was approximately 3 MiB in these cases. Host load was about 11;
these warm-cache cases establish specific improvements, not general JIT parity.
Reproduce with `--extended --mib 1 --rounds 7 --case pcre-repeat --case
pcre-unicode-chain --case pcre-subroutine --baseline PATH`.

The [native grammar sample](bench/results/macos-arm64-native-grammars.json)
records all 75 deterministically selected repository files parsing successfully
in each of three rounds. It includes source/table hashes, parser work, elapsed
time, and requested allocator peaks. The largest parser peak was approximately
23.2 MiB under the default 64 MiB cap. That allocation metric is distinct from
process RSS. Full syntax support follows the pinned grammars and documented Go
builtin extension; future language revisions need grammar updates.

Linux in-process inference uses the operating system’s dynamic library loader.
Build for a dynamically linked libc (for example, `x86_64-linux-gnu`); a static
musl executable supports lexical/HTTP searches and reports
`DynamicInferenceRequiresDynamicLibc` for local inference. `--version` records
the compilation target and linkage.

## Generated conformance, hardening and profiling

`tests/differential.py` generates reproducible PCRE2 patterns, malformed syntax,
Unicode subjects and option combinations from a seed. It compares syntax
acceptance, selected output, JSON byte spans and capture replacements against
**ripgrep 15.2.0 with runtime PCRE2 10.45**, recording executable hashes. Mismatches
are reduced by deleting argument units, pattern fragments and subject characters
under an explicit evaluation budget; UTF-8 subjects remain UTF-8 during reduction.
Malformed JSON and process failures become replayable failures. Timeouts fail the
campaign. This oracle is a test dependency only.

```sh
# From zig/; run the oracle and native binary on the same platform.
python3 pkg/antfly-grep/tests/differential.py zig-out/bin/antfly-grep /path/to/rg \
  --seed 1151 --cases 1500 --output .zig-cache/differential.json \
  --goldens pkg/antfly-grep/tests/generated_fixtures.json
# Fast repeat using frozen outputs, after verifying the oracle executable hash.
python3 pkg/antfly-grep/tests/differential.py zig-out/bin/antfly-grep /path/to/rg \
  --replay pkg/antfly-grep/tests/generated_fixtures.json --frozen-oracle \
  --output .zig-cache/replay.json
```

The generated fixtures add 1,500 contracts to the existing 368. The complete
corpus runs on Linux x86_64; portable ASCII inputs also replay on other hosts.
Upstream empty UTF matching differs between Linux x86_64 and ARM64 references,
so non-ASCII generated inputs retain their oracle-platform scope. Six categories
of fixes cover empty config paths, multiline counting/grouping, duplicate capture
names, empty-match rendering, line-anchor EOF semantics and bounded source context
for capture rendering. Context is retained only for patterns/output that need it,
with explicit 1 MiB admission; counting does not retain output spans.

`grep-harden` runs 1,024 deterministic malformed-input mutations, coverage seed
replays, and allocation-failure injection. Native regex/parser/dictionary modules
use ReleaseSafe. Regex compilation/execution has a 1 MiB allocator quota and a
4,096-step match budget; pinned Python/Go/Rust and JavaScript/TypeScript parsers
and dictionaries have 512 KiB allocator quotas in the harness. Decoder output is
limited to 4 KiB. The suite checks leaks, source ranges and VM/JIT agreement for
captures, spans, marks, error outcomes and step accounting. `grep-harden-build`
builds a portable test executable for runtime checks on another platform.

```sh
zig build -Dgrep-only=true grep-harden
# Each target is selected explicitly; otherwise the build fuzzer may run only
# its first discovered fuzz test. Keep each campaign finite and retain reports.
python3 pkg/antfly-grep/tests/fuzz.py --zig /path/to/zig --iterations 50000 \
  --output pkg/antfly-grep/bench/results/macos-arm64-hardening.json
```

`grep-profile` builds `antfly-grep-regex-profile`. It measures VM dispatch counts
outside timing, then runs the VM and JIT over identical in-memory subjects with
reused scratch. `bench/profile.py` alternates executables and execution modes,
checks result checksums and records corpus/executable hashes, samples and host
load. Compilation, source reads and warmups are excluded from kernel timing;
`bench/compare.py` measures the actual CLI including startup, I/O, output and RSS.
The new `pcre-captures` and `pcre-reset` extended cases exercise the measured
capture-save/reset bottleneck. Native ARM64/x86_64 JIT chains now handle capture
saves and `\K`, with the same step accounting and W^X/code-size limits. Entry
remains on consuming states; saves/resets execute inside native chains. Dispatch
counters and their pointer are compiled out of production executables; only the
profiling utility explicitly opts in. Complex
control flow continues through the bounded VM. CPU-stack sampling of late-prefix
log searches also identified capture-buffer cloning at every failed candidate.
The runner now initializes a pooled, owned frame directly, removing that copy and
the separate initial capture allocation; branch snapshots retain their ownership
and memory limits.

```sh
zig build -Dgrep-only=true -Doptimize=ReleaseFast grep grep-profile
python3 pkg/antfly-grep/bench/profile.py zig-out/bin/antfly-grep-regex-profile \
  --output .zig-cache/kernels.json
python3 pkg/antfly-grep/bench/compare.py zig-out/bin/antfly-grep --extended \
  --mib 32 --rounds 7 --repository pkg/antfly-grep/src \
  --case pcre-captures --case pcre-reset --case literal-miss \
  --case repository-files --case repository-search --output .zig-cache/cli.json
```

The preceding hardening/profiling milestone passed **112 CLI tests on macOS and Linux** (two and one
platform-specific skips), all **1,500 live generated oracle comparisons** with no
failures/timeouts, and **153,992 additional coverage fuzz executions** across the
three targets. Windows x86_64 cross-compilation passed; runtime checks remain
scoped to macOS/Linux. Reports retain executable identities and hardening source
hashes: [live differential](bench/results/linux-x86_64-generated-differential.json),
[fuzz campaigns](bench/results/macos-arm64-hardening.json).

The earlier [paired CLI measurements](bench/results/macos-arm64-profile-local.json)
compare the original PR binary with the updated binary, using seven alternating
rounds. Times include startup and I/O. These warm-cache macOS ARM64 measurements
record host load and raw runs; they establish gains for specific workloads.

| Workload | Original PR ms | Updated ms | ripgrep ms | Original / updated |
|---|---:|---:|---:|---:|
| Capture count, 32 MiB | 154.92 | 70.97 | 127.62 | 2.18× |
| Capture/reset count, 32 MiB | 698.98 | 558.12 | 84.92 | 1.25× |
| Repeat count, 32 MiB | 62.64 | 59.91 | 38.02 | 1.05× |
| Monorepo file discovery | 193.88 | 192.61 | 22.95 | 1.01× |
| Monorepo file search | 1048.29 | 1060.07 | 183.49 | 0.99× |

Capture/reset counts peak at approximately 3.3 MiB RSS. Startup, literal, and
repository workloads remain broadly unchanged. The complete
[monorepo run](bench/results/macos-arm64-profile-monorepo.json) covers 10,119 files
and approximately 534 MiB; [Go SDK](bench/results/macos-arm64-profile-go-sdk.json)
and [TypeScript SDK](bench/results/macos-arm64-profile-ts-sdk.json) runs cover
additional source trees. Those earlier measurements identified repository
discovery/search and late-prefix reset search as the next bottlenecks; the
optimization results below supersede them for those workloads.

[VM/JIT kernel profiles](bench/results/macos-arm64-capture-jit.json) record dispatch
mixes and matching result checksums. Raw CLI CPU traces accompany the
[capture log](bench/results/macos-arm64-cli-capture-sample.json),
[reset log before owned-frame reuse](bench/results/macos-arm64-cli-reset-before.json),
[final reset log](bench/results/macos-arm64-cli-reset-sample.json), and
[repository search](bench/results/macos-arm64-cli-monorepo-sample.json) reports.
The repository trace exposes ignore/glob matching during discovery and parallel
file scanning; further broad JIT expansion would not address that entire cost.
The sampling adapter supports directories, count/file modes and the default or
PCRE-style engine, collects symbols before short-lived processes exit, and
records failed/time-limited runs explicitly.

```sh
python3 pkg/antfly-grep/bench/sample_cli.py zig-out/bin/antfly-grep /path/to/repo \
  'TODO|FIXME|retry' --engine default --files-with-matches \
  --output .zig-cache/repository-sample.json
```

Fuzz campaigns and generated suites are finite evidence. They do not establish
universal PCRE2/JIT parity, parser correctness, or performance parity.

### Native search speedups

Local discovery uses up to twelve workers for file lists and four workers for
file-search discovery, with an opportunistic twelve-job queue:
active workers continue depth-first traversal, sharing subtrees with idle
workers. Jobs retain immutable compiled ignore rules. A full queue falls back
to inline traversal, and completion/cancellation drains owned jobs before
releasing configuration. Decision, tracing and symlink-following walks
remain serial; sorted output still collects and sorts its inventory.

A bounded directory lookahead (128 entries / 8 KiB of names) avoids opening
missing ignore files without a second directory scan. Overflow retains the
original ignore loader and streams the remaining entries. Path prefixes and
64–256 KiB file buffers are reused; ordinary discovered files avoid redundant
metadata calls. Automatic literal and literal-union scans use at
most eight scan workers to limit filesystem contention; explicit `-j` values
remain authoritative. Plain file lists bypass file scans and batch up to 64 names /
8 KiB per worker. Line buffering, JSON, sorted lists, metadata filters and finite
output budgets retain their per-file behavior.

Bare literal alternations bypass regex compilation while preserving alternative
order. Literal prefilters skip complete nonmatching records before the first
candidate, leaving the candidate record on the normal matching/rendering path.
SIMD candidate searches reduce four vectors together; PCRE-style matching uses
safe literal start filters and vectorized ASCII repeats with scalar-equivalent
captures and step budgets. Binary-checked full reader windows can be borrowed
without an extra copy. Single-literal misses fuse NUL detection and literal
rejection in those windows; proofs expire at refill, and cross-window records
stay on the normal matching path. Late-NUL classification windows remain unchanged.

An intermediate [directory stack sample](bench/results/macos-arm64-discovery-speedups-sample.json)
identified directory I/O and waiting; the [file-worker sweep](bench/results/macos-arm64-file-worker-tuning.json)
motivated the automatic scan-worker cap. Stack samples include blocked time.

The [final paired run](bench/results/macos-arm64-search-speedups.json) compares
pre-optimization commit `01d5749023`, this implementation, and ripgrep over eleven
alternating rounds. All five original cases are faster than ripgrep on this
macOS ARM64 host. Times include startup and I/O; output digests and exit codes
agree. The repository contains 10,133 files / 534.1 MiB.

| Workload | Before ms | Native ms | ripgrep ms | rg / native |
|---|---:|---:|---:|---:|
| Repository search | 1493.86 | 157.84 | 196.53 | 1.25× |
| File discovery | 191.57 | 20.72 | 22.87 | 1.10× |
| Capture/reset count, 32 MiB | 677.09 | 46.55 | 84.95 | 1.83× |
| Repeat count, 32 MiB | 59.44 | 16.22 | 36.79 | 2.27× |
| Literal miss, 32 MiB | 8.48 | 6.85 | 7.38 | 1.08× |

Repository search uses 28% less median total CPU time and 46% less peak
recorded RSS than ripgrep (7.5 vs 14.0 MiB). Discovery also uses less CPU and
RSS. The [128 MiB check](bench/results/macos-arm64-search-speedups-large.json)
keeps literal-miss RSS near 3 MiB and records literal misses at
13.82 vs 15.67 ms, capture/reset counts at
171.15 vs 324.78 ms, and repeats at
53.28 vs 133.06 ms.

Seven-round [Go SDK](bench/results/macos-arm64-search-speedups-go-sdk.json) and
[TypeScript SDK](bench/results/macos-arm64-search-speedups-ts-sdk.json) checks also
beat ripgrep for discovery and file search. These are workload-specific warm-cache
measurements, not universal performance parity. Reports retain binary/corpus
hashes, versions, host load, first-result latency, CPU/RSS and individual runs.

Optimization regression tests compare compiled globs with the original VM,
vector repeats with scalar execution, and literal alternations/prefix skips
with grouped regex execution across case, word, context, replacement and count
options. They also cover directory-cache overflow, shared nested ignore scopes,
wide frontiers, null-separated lists, finite output limits, line buffering,
cancellation and broken-pipe cleanup.

[Final validation](bench/results/search-speedups-validation.json) passes **118 CLI
tests on macOS and Linux** (two and one platform-specific skips), native unit
properties, all **1,500 live pinned-oracle comparisons** with no failures/timeouts,
and the Linux hardening suite. Three refreshed [coverage campaigns](bench/results/macos-arm64-search-speedups-hardening.json)
add **107,363 executions** across regex, parsers and dictionaries. Windows x86_64
cross-compilation passes; runtime validation remains scoped to macOS/Linux.
The validation report records source and executable hashes. Decision checks use
local fixtures.


## Release qualification and indexed acceleration

Build a versioned standalone release with the native toolchain, then package it:

```sh
zig build -Dgrep-only=true -Doptimize=ReleaseFast -Dstrip=true -Dantfly-version=VERSION grep
python3 ../scripts/packaging/package_grep_release.py --binary zig-out/bin/antfly-grep --output zig-out/antfly-grep.tar.gz
```

The archive includes the executable, its SHA-256 manifest, Apache license,
third-party notices, all four shell completion files, a man page, connection and
document schemas, and compatibility documentation. Server archives also carry
these grep assets. Cross-target archives copy generated tooling without executing
the target binary. CLI regression tests compare every generated asset with the
current CLI output. Regenerate `generated/` using `--generate` after flag changes.
The server E2E artifact includes grep so real-server tests use the same checkout.

[`zig/e2e/antfly/test_grep_indexed.py`](../../e2e/antfly/test_grep_indexed.py) covers real native-server
pagination, large source-region lists, concurrent updates, one-second cursor
expiry with retained partial output, authentication and key revocation. Run it
with `ANTFLY_BIN` and `ANTFLY_GREP_BIN` pointing at the desired native builds.

Optional indexed acceleration uses an explicitly configured, revision-atomic
raw-byte trigram index. For example:

```json
{"name":"sources","candidate_index":{"version":1,"field":"grep_trigrams","version_field":"grep_trigrams_version"}}
```

The producer must index **every overlapping three-byte window of the exact stored
UTF-8 content** as a six-character lowercase hexadecimal keyword term. No analyzer
stemming, normalization, or truncation is permitted. Deduplicating terms is safe.
Write the term array, content, source metadata and revision atomically; set the
keyword certificate field to `raw-byte-trigrams-v1` only after generating the
complete array. Empty and short content has an empty term array. The certificate
is an ingestion guarantee, not a user-supplied relevance score. Without a
certificate, documents remain candidates, allowing online incremental backfills.
Do not certify partial or stale term arrays.

The CLI selects at most eight necessary grams per proven case-sensitive literal
or mandatory regex substring. Up to 64 alternatives are combined with OR, then
intersected with any user filter. Every alternative must supply a substring of
at least three bytes; otherwise the entire query falls back to an exact scan.
All returned content is verified with the shared matcher. Missing certificates
use the exact-scan fallback. Queries needing nonmatching files, inverted selection,
passthrough, zero counts, or full JSON/statistics bypass candidate filtering.
Decision searches can use a proven explicit lexical prefilter; unrestricted
natural-language searches and jevgrep preview/reading-lead searches retain the
full scan. Unprovable case-insensitive, short, advanced-PCRE and
oversized pattern sets fall back too. Existing tables without
`candidate_index` keep the original exact scan. Approximate semantic retrieval is
not substituted for an exact grep query.

Automatic local workers respect Linux scheduler affinity and hierarchical cgroup
v2 CPU quotas; explicit `-j` remains authoritative. Storage-sensitive defaults
retain the measured caps. Automatic file scans start with at most two workers
and grow only when the bounded queue backs up; directory workers start only
when subtrees can be shared. Explicit worker counts retain eager startup. Use benchmark worker sweeps on the
deployment's actual storage rather than assuming one cap is optimal everywhere.

`bench/compare.py` accepts `--threads N`, Linux `--cpus N`, and
`--cache-mode cold-advice`. Cold advice fsyncs each corpus file and calls
`POSIX_FADV_DONTNEED` before each measured child. It does not drop global caches,
and storage/controller caches can remain warm; it is best-effort eviction, not a
physical cold-storage guarantee. Extended cases include dense output, capture
replacement, Unicode output and preprocessing; `--codecs` adds decompression.
The admitted native Linux qualification CI records paired runs, CPU, RSS,
first-result latency, output hashes and a 5,000-case differential campaign,
including worker sweeps and a one-CPU/cold-advice configuration. CI evidence is
available only after the job has actually completed on that platform.

`tests/relevance-repository.json` pins labeled production-source snapshots in
addition to the small synthetic relevance corpus. Run `bench/evaluate.py` with
that manifest and `--fixture` for deterministic pipeline checks. It records
requests and provider-reported tokens; optional `--input-usd-per-million` and
`--output-usd-per-million` calculate cost when usage is available. Unknown usage
remains unknown. Fixture precision/recall does not establish live-model quality.
Provider evaluation remains opt-in, with an explicit endpoint/model and request
budget. No provider pricing or credentials are embedded in the corpus.

Linux memory measurements use separate GNU `time` runs outside elapsed/CPU timing.
Install `/usr/bin/time` to collect target RSS; without it, Linux RSS is reported as
unavailable. Direct `wait4` can include the benchmark parent's inherited pre-exec
high-water mark, so those values are deliberately omitted. A regression holds
96 MiB in the parent and verifies that a small target does not inherit that peak.
Cold-advice eviction is repeated before the independent memory runs too.

Timing reports retain every trial and flag relative median absolute deviation
above 20% as high variance. Treat such timings as inconclusive for small speedups;
the Linux VM can share CPU and storage with other host workloads. No slow trials
are removed. Dedicated admitted CI remains the next source of independent x86-64
Linux measurements. Source and executable identities remain attached to results.

GNU `time` reports the command's accounted RSS maximum, including waited
children by maximum rather than sum. Codec/preprocessor comparisons therefore
do not establish concurrent process-tree peak memory; external rg helper
processes can overlap. Single-process cases avoid that ambiguity.

Decision refinement batches independent line targets across each bounded region
window, including lines in rejected regions. Target prompts are materialized only
for the current request window, and partial budget exhaustion retains completed
line evidence. Typesafe requests share each immutable region context once in
`state`. Decision API serialization preserves the existing `inputs` wire format
and writes context directly into JSON without a temporary joined string. JSON
serialization uses a 32 MiB allocation budget and a 16 MiB payload limit; oversized
batches split through the existing retry path. `--decision-context shared-v1` opts
ordinary Decision API requests into the versioned `context_version: 1` format: each
immutable context is sent once in `contexts`, with `context_index` on targets.
Supporting Antfly decision servers validate references and bound expanded inputs to 1
MiB each and 16 MiB total; effective input bytes and answer identities equal legacy
requests. The default `legacy` mode supports existing endpoints; an unsupported shared
version fails explicitly. This reduces transport bytes, not model input tokens.
Per-target cache identities hash each immutable region once and include its digest
alongside target text, credential/question scope, and broad/direct mode. Cache v6
records bounded ordered batches with one stat/read per warm batch and atomic
publication. Valid partial scores survive failed evaluations; missing scores are
reevaluated. Existing v5 per-target entries remain readable and migrate on use.
Changing batch grouping can cause a safe miss. Warm hits do not refresh expiry;
corrupt, expired, or differently scoped entries cannot supply evidence. Concurrent
writers may replace one another’s partial cache records, reducing hits but never
changing score identity.
Fixture request bytes are recorded
separately from provider token usage; fixture results do not establish model
quality or actual provider cost.

`bench/decision_transport.py` compares a saved baseline with the current binary
using synthetic source and a local constant-score Typesafe fixture. The recorded
512-line case sends the same 513 questions in 65 requests and produces identical
output, while request-body bytes fall from 10,719,928 to 1,609,144 (85% less).
See `bench/results/followup-typesafe-transport.json`. The labeled repository
fixture separately reduces requests from 52 to 45 per question while preserving
all 304 evaluated targets and expected results. These are transport/orchestration
measurements, not provider token costs or model-quality evidence.

The bzip2 decoder reuses block workspaces with precise link-buffer capacity,
separates inverse-BWT traversal from RLE output, and uses SIMD to locate RLE
escapes before copying ordinary bytes in bulk. Randomized legacy blocks, CRCs,
output limits and streaming backpressure retain their checked paths.

The follow-up qualification and exact executable/source identities are in
[`followup-validation.json`](bench/results/followup-validation.json). The native
macOS runs below compare the saved pre-change binary, current binary, and pinned
rg on the same corpus. Timings are milliseconds; each report retains all trials.

| Workload | Previous | Current | rg |
|---|---:|---:|---:|
| 75-file generated repository: discovery | 4.70 | 4.28 | 7.67 |
| 75-file generated repository: search | 5.68 | 5.45 | 9.52 |
| Python SDK: discovery, default workers | 8.65 | 9.43 | 14.02 |
| Python SDK: search, default workers | 30.75 | 32.78 | 38.95 |
| Python SDK: search, explicit eight workers | 31.42 | 31.26 | 35.71 |
| Buffered regex prefix miss | 8.06 | 6.89 | 8.42 |
| Buffered regex | 6.37 | 6.34 | 6.32 |
| Unicode folded output | 38.88 | 36.25 | 35.01 |
| Preprocessing | 29.00 | 29.53 | 29.79 |
| Repetitive bzip2 (noisy) | 200.13 | 173.32 | 220.12 |
| High-entropy bzip2 | 591.06 | 598.22 | 678.84 |

The default SDK search is about 7% slower than the saved baseline; the paired
explicit-eight-worker run is approximately even. Adaptive scheduling is a
startup/resource tradeoff, not a guarantee that every repository becomes faster.
The generated 75-file case uses 21 paired rounds, the SDK/output/codec run nine,
and the explicit-worker run 15. Small differences should be rechecked on the
intended host and storage.

Bzip2 peak RSS falls from roughly 9.7 to 7.7 MiB on macOS (about 20%) and
8.6–8.8 to 8.3 MiB on native ARM64 Linux (about 4–6%) versus the saved baseline.
Throughput is mixed and several Linux timings are highly contended; they do not
establish general Linux speed guarantees. The Linux final report uses a
container-local copy of the SDK and two-CPU affinity. The earlier shared-mount
report, earlier macOS reports, and their original hashes/trials are retained with
`before-precise`/`overlapped` names to show the allocation regression and timing
noise that motivated the final workspace shape. No additional JIT expansion was
justified by these measurements.


The next review follow-up fixes completed negative line refinement (including
inversion and inclusion-policy rejection) and makes output-budget exhaustion
cancel indexed work, including blocked prefetched requests. Shared Decision API
contexts are opt-in through `--decision-context shared-v1`; native server parsing,
SDK models, completions, and the public guide cover the versioned format.

Automatic file-worker growth reserves under the queue mutex, enqueues before
spawning outside it, and falls back to existing workers if growth fails. Literal
and literal-union scans use the eight-worker automatic cap; explicit worker
counts remain authoritative. Indexed scans build candidate filters once per
table and reuse the HTTP pool across tables. One persistent producer overlaps at
most one page; continuation storage is independent of page lifetimes. Quiet and
finite match-limit searches remain sequential. `prefetch: false` disables overlap.

Final paired native macOS measurements (11 alternating rounds, warm cache,
16 MiB codec corpus) compare the reviewed binary with this follow-up:

| Workload | Reviewed (ms) | Current (ms) | rg (ms) |
|---|---:|---:|---:|
| Repetitive bzip2 | 107.34 | 107.42 | 64.91 |
| High-entropy bzip2 | 490.49 | 450.83 | 549.22 |
| Python SDK search | 31.73 | 31.39 | 38.13 |
| Python SDK discovery | 8.09 | 8.07 | 12.26 |

Repository search and discovery are approximately unchanged versus the reviewed
binary. The adaptive 10-bit Huffman table accelerates longer codes while retaining
the cheaper canonical probe for short codes and fallback through 20 bits. The
move-to-front vector-copy experiment regressed and was removed; its report and
the pre-tuning report remain checked in. The short stripped-binary CPU sample
retains raw callsite offsets; unknown offsets are not attributed to Zig functions.
See `next-review-compression-profile.json` and `next-review-macos*.json` under
`bench/results/`. Host load is recorded; these are single-host measurements,
not portable performance guarantees. Repetitive bzip2 still trails rg here.

Synthetic Decision API transport sends the same 513 target/question evaluations
and identical output with 10,548,999 → 1,412,265 request bytes
(86.6% less). Warm disk-cache search over 2,048 lines
uses zero provider requests in both binaries and takes 104.91 →
39.44 ms. The eight-document indexed latency fixture takes
970.21 → 518.25 ms while preserving all 17 requests and identical output;
each simulated HTTP/provider request has 40 ms latency. These fixtures establish
transport/cache/orchestration behavior, not live model quality or billed tokens.
The five labeled repository fixtures retain precision/recall 1.0 with 304 target
evaluations and 45 requests per question.

`bench/results/next-review-validation.json` records the implementation commit,
source hashes, and qualified macOS/Linux executable identities. Qualification
includes 133 CLI tests on each platform (two macOS skips, one Linux skip), five
real native-server indexed scenarios, 15 native decision-contract tests,
canonical/lookup Huffman equivalence and malformed-input checks, 1,000 generated
differential cases, and 10,167 codec fuzz runs. Linux executes ARM64 instructions
inside a Linux VM/container on the ARM64 host with two CPUs; it is not independent
Linux hardware. No live decision provider was contacted. Main PR CI still needs
its commit-specific approval.


### Bounded output, cache and preparation follow-up

Column-only output retains the first span on each selected record; statistics
count occurrences without allocating one span per occurrence. JSON, color,
replacement, only-matching and vimgrep retain their required complete match data.
Rendering borrows the scanner's stable spans instead of copying them again.
Directory workers grow outside the discovery queue mutex, with one growth in
flight and inline traversal if spawning fails. No additional unbounded queue or
thread pool is introduced.

GLiNER decision preparation reuses exact task encodings and exact normalized text
tokenization within one request. Separate bounded maps admit up to 128 tasks and
128 texts with at most 4 MiB of retained token arrays. New unique entries beyond
the caps use the same uncached encoder. Full original rows, attention masks,
marker positions, task packing, logits and token accounting are preserved.
Bidirectional encoder activations are recomputed for each row: this optimization
reduces preparation work, without assuming shared text has context-independent
model activations. It applies to the GLiNER decision pipeline; other model
architectures retain their existing preparation paths.

A symbolized repetitive-bzip2 sample identifies inverse-BWT pointer traversal and
block construction as the dominant decoder work. Frequency accumulation now
happens during MTF/RLE symbol decoding, counting whole runs at once and removing
the subsequent histogram scan. The four-byte BWT links and bounded streaming
buffers are preserved. Further encoder/JIT or workspace expansion requires a
measured benefit and equivalent results.


The follow-up qualification records executable identities and source hashes in
[`bounded-review-validation.json`](bench/results/bounded-review-validation.json).
All 137 CLI tests pass on macOS (two platform skips) and ARM64 Linux (one skip),
alongside five existing real-server indexed tests, 15 decision-contract tests,
the native preparation-equivalence fixture, 1,000 generated differential cases,
and 10,138 codec fuzz runs. The legacy-cache migration fixture also verifies
credential isolation. No live provider or model assets were used.

Paired medians against PR head `02c9684b96`:

| Workload | Before | After | Scope |
|---|---:|---:|---|
| Dense column output, 1 MiB line | 40.24 ms | 8.08 ms | Native macOS; peak RSS 100.3 → 4.9 MiB |
| Dense column output with statistics | 39.97 ms | 7.75 ms | Native macOS; exact counters checked |
| Warm decision cache, 2,048 lines | 82.30 ms | 29.95 ms | Loopback fixture; zero provider requests |
| Repetitive bzip2, 16 MiB | 196.34 ms | 144.08 ms | Native macOS; rg 206.68 ms on this loaded run |
| Repetitive bzip2, 16 MiB | 140.70 ms | 107.13 ms | Linux VM, one CPU/worker; rg 105.76 ms |
| High-entropy bzip2, 16 MiB | 1,264.04 ms | 824.92 ms | Linux VM, one CPU/worker; CPU 965.41 → 815.90 ms |

At the prior head, default-budget indexed overlap took 530.79 ms versus 952.45 ms sequentially
with identical output and all 17 requests (40 ms simulated latency). Finite
request budgets now prioritize foreground decisions; the two-request regression
returns the same 1,000 matching lines whether prefetch is configured on or off.

Raw trials, output hashes, load and RSS appear in `bounded-review-*.json`.
These are shared-host measurements with unrelated builds active. The initial
Linux two-CPU run has mixed elapsed results, including slower repository search
and repetitive bzip2 despite lower decoder CPU time; the one-CPU codec rerun
makes the repetitive decoder improvement clearer. High-entropy one-CPU timings
are flagged as high variance for every binary, so elapsed-time gains there are
provisional. A 31-round macOS repository-only rerun measures search at 32.02 → 31.97 ms
and discovery at 8.15 → 8.13 ms, approximately unchanged (rg 40.26/12.39 ms).
Discovery is modestly faster in the first Linux run. No universal
repository speedup, independent-Linux-hardware result, live encoder speedup,
model quality or billed-token improvement is claimed.


### Prepared discovery, replayed output and persistent decision workers

The current implementation prepares and deduplicates file-type and exclusion
globs once per discovery root. Literal basenames and dot suffixes use shared
lookup sets; complex patterns retain the canonical compiled-glob fallback.
Traversal workers borrow those immutable predicates until they join. Type includes can admit matching hidden filenames, matching
ripgrep's precedence; hidden parent directories and ignore-file exclusions
remain effective. Local and indexed filtering use the same prepared predicates.

Ordinary single-record JSON, color and match-only output preflight matches in
constant memory and replay spans into buffered output. Literal preflight uses
the bulk counter. Regex preflight validates all occurrences before replay and caches up to 64
spans for sparse rows, so resource failures are detected before streaming a JSON match record. Output no
longer retains one match object or a second JSON submatch object per occurrence.
Adjacent positive matches share highlight ranges; empty matches retain their
metadata and match-only row behavior without empty color escape sequences.
Capture-dependent, contextual and multiline rendering retain their required
existing match data paths.

Unbounded JSON output streams through a 64 KiB writer. Finite output budgets and
retained/sorted output use an atomic record collector capped by the remaining
output or pending-output allowance. Oversized records stop serialization before
allocating the full expanded record, and no partial JSON record is published.
The collector performs one bounded record allocation instead of serializing and
copying the entire record twice.

Parallel decision batches use a lazily grown, client-owned worker pool. Workers
and their HTTP connection pools survive region/refinement waves. At most the
configured number of workers exist; a wave borrows its input until every selected
worker acknowledges completion. Partial failures preserve completed scores and
join safely; spawning failures fall back to available workers or sequential work.
No queue scales with source or repository size.

All finite request budgets, including the implicit default, keep indexed
retrieval behind foreground decisions. A request's remaining provider work can
include refinement, retries and batch splitting, so speculative retrieval cannot
safely claim its last request slots. `--max-requests=unlimited` explicitly enables
retrieval/decision overlap, while document, output, timeout and cancellation
limits still apply. This trades default-budget network overlap for deterministic
foreground evidence. Generated help and shell tooling include the new value.

`bench/output_pipeline.py` compares match output, every JSON match span, file
sets, startup-to-first-byte time, CPU and process RSS against a saved binary and
optional ripgrep. `bench/decision_transport.py --jobs 4` covers repeated worker
waves; `--keep-alive` also records connection reuse. These are loopback fixtures,
not measurements of model relevance, encoder speed or billed provider cost.


The latest qualification compares the implementation with PR head `f36d2c4b47`.
Paired medians and executable hashes are in `bench/results/stream-review-*.json`:

| Workload | Before | After | Additional evidence |
|---|---:|---:|---|
| All-type discovery, macOS, 1,786 SDK files | 191.10 ms | 8.27 ms | rg 22.03 ms; identical file sets |
| All-type discovery, Linux VM, two CPUs | 405.72 ms | 32.65 ms | rg 36.28 ms; identical file sets |
| Dense JSON, macOS, 512 KiB line | 31.37 ms | 24.77 ms | Peak RSS 70.0 → 3.7 MiB; rg 51.12 ms |
| Dense JSON, Linux VM, two CPUs | 59.52 ms | 43.67 ms | Peak RSS 71.0 → 3.4 MiB; rg 83.30 ms |
| Dense color, macOS | 14.59 ms | 10.35 ms | Peak RSS 40.0 → 3.6 MiB; rg 17.03 ms |
| Dense match-only, macOS | 13.03 ms | 9.63 ms | Peak RSS 28.9 → 3.6 MiB; rg 11.77 ms |
| Dense regex color, macOS | 13.53 ms | 14.26 ms | Peak RSS 40.1 → 3.6 MiB; rg 20.29 ms |
| Dense regex color, Linux VM, two CPUs | 33.27 ms | 38.83 ms | Peak RSS 42.3 → 3.6 MiB; rg 55.32 ms |
| Decision HTTP keep-alive, four workers | 237.86 ms | 53.02 ms | Same 513 questions / 65 requests; connections 65 → 5 |
| Warm decision cache, four workers | 12.42 ms | 10.58 ms | No network requests |

Dense regex replay deliberately exchanges a second matching pass for bounded
span storage: this run is about 5% slower on macOS and 17% slower in the Linux
VM than the previous allocation-heavy renderer. Linux match-only measures
16.21 ms versus rg's 15.20 ms despite improving from 28.96 ms. These results
do not establish universal throughput superiority. First-result latency, CPU,
RSS, raw trials and match-equivalence checks are retained in the reports.

The keep-alive fixture also measures request bytes at 10,548,999 → 1,412,265,
using the existing Typesafe shared-state transport. This is wire payload, not
provider token billing. HTTP/1 direct, pooled and TLS connections now request
TCP_NODELAY, matching the existing HTTP/2 socket policy and avoiding delayed-ACK
stalls across persistent worker waves. Two native socket regressions verify the
direct and reused pooled connections.

With explicit `--max-requests=unlimited`, the indexed overlap fixture measures
962.00 → 515.79 ms against sequential retrieval, with identical output and all
17 requests. Finite budgets deliberately run retrieval behind decisions. The
1,000-request default regression now returns 50,000 lines with 500 retrievals
and 500 decisions, regardless of the prefetch setting; the previous speculative
path consumed 501 retrievals and left only 499 decisions, returning 49,900 lines.

Qualification includes 142 CLI tests on macOS (two skips) and ARM64 Linux (one
skip), full native grep unit/hardening/tooling targets, five real-server indexed
integration tests, and 1,000 generated comparisons against ripgrep 15.2.0 /
PCRE2 10.45 with no differences or timeouts. The five labeled fixture questions
also retain complete expected results. Fixture scores do not establish model
relevance or real-provider reliability. License headers, the Apache dependency
boundary and its 35 checker tests pass.

The Linux measurements execute ARM64 instructions in a two-CPU Linux VM on the
same ARM64 Mac; they are not independent Linux hardware results. Filesystem
caches were warm and unrelated builds were active. Full upstream PCRE2/ripgrep
parity, independent-host cold-cache performance and live-provider quality remain
outside this qualification. Source and executable identities are recorded in
[`stream-review-validation.json`](bench/results/stream-review-validation.json).


### Cached-only execution and incremental preparation

```sh
# Warm evidence with a stable model generation, then prohibit inference.
antfly-grep --ask 'retry enforcement' --model MODEL --model-revision REV src/
antfly-grep --ask 'retry enforcement' --model MODEL --model-revision REV \
  --cached-only --max-requests=0 src/
```

`--cached-only` reads existing evidence without provider HTTP calls or native
inference startup. Missing, evicted or expired scores produce `DecisionCacheMiss`,
preserve completed evidence, and exit 2 with `complete: false` (quiet mode
retains ripgrep-style successful-match precedence). It never converts
missing evidence into a negative result or falls back to a paid provider. It
rejects table reads, preprocessors, hostname commands, `--no-cache` and zero cache
TTL. It operates on local files or stdin; operating-system access to mounted
filesystems remains outside the CLI's network policy. Existing endpoint/model,
credential and policy identities must still match the records being read.

Native local inference now has a shared lazy owner. Creating search or decision
workers does not open a library or model. The first uncached invocation starts
the runtime under a mutex; subsequent calls use the concurrent native ABI, and
all workers finish before that owner is destroyed. Mixed parallel waves retain resolved scores and dispatch only unresolved inputs,
reusing derived cache keys and preserving original timestamps. Entirely cached parallel waves
also bypass worker dispatch. A regression removes the library after warming
scores and verifies that both normal and cached-only searches still succeed.

Decision batch cache v8 records each score's original creation time. Re-batching,
legacy migration and partial-cache repairs cannot rejuvenate old evidence. V6/V7
batches and scoped v5 scalar files remain readable with their original file age;
new records use a separate versioned namespace. V8 uses packed little-endian
score/timestamp pairs in three size classes: 256-byte slots (4,096 buckets,
16 ways), 1-KiB slots (2,048 buckets, 8 ways), and 4-KiB slots (512 buckets,
8 ways), capped at 48 MiB of logical record bytes. Filesystem blocks and inode
overhead are additional. Small batches do not consume maximum-size slots.
Successful legacy reads migrate during writable searches;
pre-existing legacy files remain until `cache clear`, avoiding startup sweeps
and loss of evidence if migration writes fail. A second-level table
holds 16,384 four-way buckets of checksummed 64-byte records (4 MiB).
One positional read retrieves an entire bucket. It retains individual
input identities when batch shapes change. Collisions are evictions and cause
misses, never score substitution; concurrent torn writes also cause misses. The
exact-batch tier keeps the ordinary one-record warm-hit path. Pinning
`--model-revision` prevents reuse after a model generation changes.

Source preparation is independent of questions. An artifact stores line offsets,
declarations and calls for exact content, path/language, delimiter/CRLF settings,
and authorization/revision scope. Its identity includes the projection source,
Zig version and every pinned parser/grammar source byte; parser updates invalidate
artifacts automatically. The private, atomically replaced artifacts occupy at
most 1,024 four-way buckets of 256 KiB per slot (1 GiB total).
V2 stores portable binary offset/index records rather than JSON and evicts the
oldest written artifact in a full bucket. Old V1 source artifacts are disposable
and no longer read; `cache clear` removes them. Full digest and checksum
validation precedes reuse;
collisions, oversized preparations or invalid records fall back to parsing. Source
cache entries contain identifier names and ranges and are created with mode 0600.
`stats.source_cache_hits` exposes actual reuse. All source bytes are still read
and hashed; there is no unsafe mtime-only shortcut. `--no-cache` bypasses both
preparation and decision caches, and `cache clear` removes both.

Indexed scans recheck authorization before using cached preparation or evidence.
Their identity includes endpoint, table credential, table/document ID, URI,
revision, permission data and configured principals. A new retained snapshot alone
does not invalidate otherwise identical evidence; changed revisions or permission
scope do. Content hashes independently validate prepared source data.

Indexed `--files` requests only identity, permission and timestamp fields when no
content-size restriction or JSON/statistics contract requires content. Ordinary
lexical text output also omits unused metadata and region arrays. Decision and
JSON/statistics scans preserve the full document projection. These projections
are verified against the real native server, including nested document fields,
large documents and denied readers. Certified trigram pushdown can now precede
explicit lexical decision prefilters; uncertified documents remain candidates.

Dense regex preflight now compresses repeated span lengths/strides into at most
64 run records. Both JSON and color rendering can consume the verified runs
without matching again. Irregular rows that exceed the run budget retain bounded
memory and replay from source. This preserves preflight failure semantics and
individual JSON occurrences without introducing unbounded match storage.

These changes can eliminate inference calls for warm evidence; they cannot
eliminate CPU, source reads, storage or output cost. Cached-only mode has zero
provider requests even when evidence is absent. Normal cache use may reevaluate
expired or evicted records. Local inference has no remote API bill but still
uses local compute. Fixture wire-byte and question-count reductions are not
measurements of billed tokens or live relevance.


Qualification against previous PR head `d5d13ece2d`:

| Workload | Before | After | Scope |
|---|---:|---:|---|
| Dense regex color, macOS | 21.46 ms | 18.12 ms | rg 30.02 ms |
| Dense regex color, Linux VM / two CPUs | 20.49 ms | 15.11 ms | rg 29.06 ms |
| Dense JSON, macOS | 31.92 ms | 30.72 ms | rg 62.40 ms |
| Dense match-only, macOS | 15.08 ms | 18.14 ms | rg 16.86 ms |
| Dense match-only, Linux VM / two CPUs | 9.38 ms | 10.71 ms | rg 16.36 ms |
| Warm Python auth-test source | 11.56 ms | 9.46 ms | Alternating paired runs; zero provider requests |
| Warm decisions, 2,048 lines / four workers | 20.82 ms | 20.28 ms | Zero provider requests |

The re-batching fixture sends 0 requests / 0 questions instead of 11 requests /
32 questions, with identical output. A one-line edit evaluates 1 question instead
of 8. Narrow indexed listing returns the same path using 261 response bytes
instead of 4,981,035 bytes. These are fixture counts and wire sizes, not provider
billing measurements. Raw source hashes and paired trials are retained in
`bench/results/incremental-review-*.json`.

The dense-regex speedup does not establish that every path is faster: match-only
regresses on both final runs, and discovery/color results are mixed. All runs use
warm filesystem caches on a shared host with unrelated builds active. Linux uses
a two-CPU ARM64 VM on that same Mac. CPU, first-byte latency and peak RSS appear
beside elapsed trials; independent hardware and cold-cache results remain open.
The current JIT helps existing literal capture chains; repeat and control-flow
profiles do not justify another emission path in this pass. The verified compact
span representation improves rendering without altering regex execution.

Final validation: 154 CLI tests on macOS (two skips) and ARM64 Linux (one skip),
all native grep unit/hardening/tooling targets on macOS, six real-server indexed
scenarios, 1,000 generated oracle comparisons and the 392-case output-option grid
with no differences, plus five labeled relevance fixtures. These fixtures retain
expected results but do not measure real model relevance. License headers,
the Apache boundary and all 35 boundary-checker tests pass. Exact source and
executable identities are in
[`incremental-review-validation.json`](bench/results/incremental-review-validation.json).


### Associative cache and output qualification

This revision fixes match-only width limits and NUL-delimited long-line omissions,
including preview coloring. Finite width limits preserve `--vimgrep` repetitions.
Selection avoids a complete preliminary match count when only output spans are
needed. Plain one-byte ASCII literal matches with no prefixes, trimming or color
use a bounded bulk output path; other output retains ordinary span rendering.
The tests compare bytes against ripgrep and cover full/partial output blocks.

Mixed decision waves preserve resolved scores and their original timestamps,
reuse derived identities, and dispatch only unresolved inputs. Workers return
actual score creation times; waiting for a slow sibling cannot refresh earlier
evidence. A staggered-response regression compares both cache tiers across a
two-second completion gap. Cached-only misses
retain evidence in the failed wave and stop without visiting subsequent waves.
Indexed lexical/listing scans skip unused cache-scope serialization and hashing;
decision preparation remains authorization-scoped even with score TTL zero.

The source cache now uses packed, four-way associative artifacts, and the batch
cache uses packed size classes. Capacity replaces the oldest written artifact in
a full bucket; complete identities and checksums prevent substituted evidence.
Size classes matter for cost: giving every small batch a maximum-size slot would
waste capacity and cause avoidable model calls. The large fixture verifies zero
warm inference calls as well as preparation reuse and output equivalence.

Final paired results and binary/source identities are recorded in
[`associative-review-validation.json`](bench/results/associative-review-validation.json).
The output runs, repository-scale cache fixture, incremental HTTP fixture, and
generated comparisons are recorded in the adjacent `associative-review-*.json`
reports. The baseline is PR head `344de9822e`; these are warm-filesystem results
on a shared Mac and a Linux ARM64 VM on that same host. They do not establish
independent-hardware or cold-cache parity, real-model relevance, billed token
savings, or full upstream PCRE2/JIT/CLI conformance.

| Paired workload | Previous PR head | This revision | ripgrep |
|---|---:|---:|---:|
| Dense ASCII literal match-only, macOS | 14.25 ms | 7.50 ms | 18.11 ms |
| Dense JSON, macOS | 33.46 ms | 34.91 ms | 81.69 ms |
| Dense color, macOS | 16.29 ms | 15.83 ms | 23.59 ms |
| Dense regex color, macOS | 17.78 ms | 17.61 ms | 30.76 ms |
| Dense ASCII literal match-only, Linux VM | 20.14 ms | 10.31 ms | 29.63 ms |
| Warm 2,048-file Python fixture, macOS | 9.41 s | 5.89 s | n/a |

The cache fixture retains 1,754 source preparations versus 293, with zero warm
inference calls for both binaries. Allocated cache storage is 45.8 MB versus
45.0 MB, and peak RSS remains approximately 66 MiB. This is improved reuse and
bounded growth, not a claim of smaller allocation in every workload. Some
dense-output timings regress slightly; the reports retain every measured run.

Validation includes 160 CLI tests on each platform (two macOS skips, one Linux
skip), native unit/hardening/tooling checks, six real-server indexed scenarios,
1,000 generated differential cases, five labeled fixture questions and all 35
Apache-boundary checker tests. Full upstream conformance remains unproven.
