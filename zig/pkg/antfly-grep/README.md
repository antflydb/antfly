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

Line discovery evaluates lines even inside rejected blocks. `--no-refine`
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
backend, authorization scope, question, and rendered input. Cached files contain
scores, not source text or tokens. Use `--no-cache`, `--cache-dir`, or `cache clear`.
Clear the cache after replacing a model behind an unchanged model identifier.

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
all configured tables. `--table-filter JSON` passes an additional Antfly query
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
HTTP connections are reused across documents. Pages are released after processing;
only cursor state survives. `max_page_memory` separately caps JSON expansion and
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
incomplete cache records are misses; writes use atomic replacement.

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
