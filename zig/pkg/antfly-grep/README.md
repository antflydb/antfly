# antfly-grep

A standalone native Zig CLI for regex/literal search and natural-language source
search. It uses Antfly's regex, HTTP, and inference interfaces. Building and
running the CLI requires no Rust toolchain, ripgrep executable, shell subprocess,
database, or persistent index. Regex execution, PCRE-style extensions, decompression,
transcoding, discovery, source analysis, and output are implemented in Zig in this
monorepo. There is no libpcre2 or libarchive dependency. Explicit `--pre` and
`--hostname-bin` options run the user-specified program. Cloud source selection
invokes the public `antfly-cloud context --json` command without a shell.

This implements local and table-backed search for
[#1151](https://github.com/antflydb/antfly/issues/1151). Table profiles select
indexed documents using the versioned schema below. Colony owns provider
connections and background ingestion; the Cloud integration below discovers its
authorized canonical sources.

## Connected Cloud sources

After `antfly-cloud login` and active instance selection, compatible Colony servers
provide source discovery without hand-authored table JSON or copied data-plane tokens:

```sh
antfly-grep --cloud --files
antfly-grep --source engineering -n 'retry|budget'
antfly-grep --source engineering --source work-drive 'deadline'
antfly-grep ask 'Why was this retry policy chosen?' --source engineering --model MODEL
```

`--cloud` selects all authorized sources; repeat `--source` to select aliases or
stable source IDs across repositories/providers. Exact IDs take precedence over
aliases; an ambiguous alias fails before querying. Repeated selectors select each
resolved source once. `--cloud-cli PATH` selects the CLI
executable. Cloud selection cannot be mixed with explicit local paths or manual table
credentials. Identical connection/URI/revision/content from overlapping selections is
reported once after authorization and path filters. Deduplication memory is bounded by
`max_page_memory`; independent sources retain independent query snapshots.

Discovery reads the ordinary `antfly_sources` table using bounded snapshot pages.
Cloud identity and current catalog policy are verified through trusted query
response headers; continuation requests pin the policy proof. Incomplete or
changed discovery never starts content search. Limits are 1 MiB per response,
32 MiB total wire bytes, 64 MiB parsing memory, 1,000 sources and 1,024 pages.
Cloud uses current server-enforced document permissions, with no separate reader
list API. File catalogs and self-hosted search retain additional local reader hints.

The session stays in memory. A 401 refreshes through the public CLI and resumes the
same cursor/snapshot; changing the active instance or authenticated identity fails
explicitly. Redirects are disabled. Source revocation, stale/pending sync, expired
snapshots and other source failures preserve earlier results and exit 2. Inference
endpoint/model/credentials remain independently configured through existing provider
setup; source discovery never supplies provider refresh tokens or signing keys.

Colony's source-management and data-plane protocols, provider setup and current
format limits are tracked in [Colony #813](https://github.com/antflydb/colony/issues/813)
and its `docs/dev/connected-source-search.md`. This integration does not itself
implement provider synchronization, OAuth consent screens or document rendering.

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

`--json` preserves the version 1 event format. `--json-compact` opts into
version 2 JSON Lines for lower serialization, transfer, and consumer costs:

```sh
antfly-grep --json-compact retry ./src
antfly-grep --ask 'retry enforcement' --model MODEL --json-compact ./src
```

A `begin` event contains the encoded path in `data.path`, the complete `source`
provenance, record delimiter (`lf` or `nul`), CRLF mode, and an opaque `file_id`
for that observation. `match`, `context`, `file` overview and `end` events refer
back to it with `file_id`; they do not repeat provenance. Match/context `data`
contains `lines`, `line_number`, `absolute_offset`, and `submatches`, retaining
binary base64 values and byte coordinates. The repeated top-level path/text/offset
aliases are omitted; `probability` and `scope` remain. `end.data` retains statistics
and binary offset. Every version 2 event declares `result_schema_version: 2`.
Consumers must retain identities until their `end` event and may receive an
incomplete stream on cancellation or failure. Errors before a file begins retain
their own path/provenance; summary and inventory events need no file reference.
The last output-mode flag wins. Output and pending-output budgets still apply
atomically to complete records. Regular JSON remains unchanged.

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

The default HTTP backend uses the public [decision contract](../../../docs/reference/decisions.md).
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
input. A subdivided input becomes cacheable only when every half completes;
partial scores cannot be cached under the whole input's identity. Inputs that can
be subdivided use a new evidence-key generation, so pre-fix incomplete scores
cannot reenter through legacy migration; other legacy identities remain reusable.
Each attempt consumes the request budget. Valid partial results survive
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

Decision search uses the OpenAI Decisions named-question contract, without a
provider SDK or private Jev wire adapter:

```sh
export OPENAI_API_KEY=...  # inherited only by the canonical OpenAI destination
antfly-grep ask 'Where are retries bounded?' --provider openai ./src
# Antfly gateway routes to a configured decider (including provider: jev).
export ANTFLY_API_KEY=...
antfly-grep auth --provider antfly --base-url http://localhost:8080/ai/v1 \
  --model deciders/triage
antfly-grep doctor
antfly-grep skill --agent codex
```

`--provider openai` defaults to `https://api.openai.com/v1/decisions` and
`gpt-6-luna`; pass `--model` to choose a supported Decisions model explicitly.
`ANTFLY_GREP_PROVIDER`, `ANTFLY_GREP_ENDPOINT`, and `ANTFLY_GREP_MODEL` configure
nonsecret defaults. Explicit endpoint, model, and transport options override
provider defaults regardless of argument order. A custom Decisions service uses `--provider custom --endpoint
URL --model MODEL`; its key comes from `ANTFLY_GREP_TOKEN`. That explicitly
supplied key is bound to the selected destination. OpenAI environment keys are
never sent to custom endpoints; Antfly keys apply only to the Antfly profile.
Changing a saved endpoint or provider clears its saved token. Redirects are
not followed. HTTPS or loopback HTTP is required.

The baseline sends one `input` with multiple questions. Requests share a persistent
HTTP client and use bounded provider concurrency; `--decision-concurrency` controls
lanes independently of file workers. `--batch-size` affects Antfly batches only.
Antfly and local profiles retain the native batch capability. A custom service may
explicitly opt into `--decision-transport antfly-batch`; `--decision-context
shared-v1` additionally interns repeated contexts. Neither extension is probed by
sending unsupported requests. Single-input mode rejects shared-v1. Successful
cache hits require no provider request; request budgets count actual attempts,
including retries and splits. Refusal, abstention, similarity/generated answers,
and malformed/missing responses remain incomplete evidence (exit 2), never cached
negative results. Typed predicates may omit Antfly's `decision_method` metadata.

Antfly's public gateway requires `public_inference.enabled: true`; authenticated
execution requires `inference:write`. Use its canonical `deciders/NAME` model ID.
An unauthenticated local gateway needs only its URL and configured model. Direct
TypeSafe/Vercel/OpenRouter/OpenCode presets were removed: configure Jev on the
Antfly gateway and select its decider instead. No chat/Structured Outputs fallback
manufactures probability scores. See [the gateway contract](../../../docs/reference/inference-gateway.md).

`auth` saves nonsecret provider configuration atomically with owner-only permissions
on POSIX. Keys come from the environment; `--stdin` returns
`UseEnvironmentCredential` instead of persisting a new token. Old credential
profiles can be read during migration; remove their `token` after exporting an
appropriate environment key. `doctor` uses a synthetic request. Agent installation
supports Codex, Claude, and OpenCode; `--global` chooses the home directory.
Shell completions include provider/transport choices, file types, option arity,
paths, and setup commands. Local inference needs the explicit model/library setup
above. No model is downloaded implicitly.

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
all configured tables. A table with an alias is selected by that alias; physical
names remain selectable for tables without aliases. Cloud source selectors use
source ID precedence instead. The default `prefetch: true` overlaps one retained-snapshot page
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

The [unified-search benchmark](bench/results/README.md)
records five paired rounds over 4 MiB inputs, a 1,000-file synthetic repository,
and the actual 29-file native source tree. Output hashes agree with ripgrep.
Completed-file flushing reduced first-result latency in the 1,000-file case from
21.7 ms to 9.7 ms versus the saved pre-flush binary; native peak RSS was about
4.5 MiB. Startup measured 8.3 ms in this run. Elapsed time and first-result latency
use separate alternating rounds, so their medians need not describe the same
execution. Host load and every run are preserved; these are measurements on a
busy shared host, not general performance guarantees. Explicit block buffering
remains available for bulk-output workloads.

The [labeled fixture report](bench/results/README.md)
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

The [original macOS ARM64 baseline](bench/results/README.md) and
[full 32 MiB benchmark](bench/results/README.md) preserve binary hashes,
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

The [verified 4 MiB follow-up](bench/results/README.md)
compares the installed binary, an earlier Antfly binary, and ripgrep in nine
alternating rounds after our compiler and test jobs finished. Dense
case-insensitive counts fell from 52.1 ms / 183.7 MiB RSS to 6.0 ms / 8.5 MiB;
ripgrep measured 33.4 ms / 10.8 MiB in that case. Counting no longer retains one
match object per occurrence. The same experiment records bzip2 logs at 38.9 ms
versus ripgrep’s 29.2 ms, and incompressible data at 145.1 ms versus 171.4 ms.
Host load remained elevated from other work; CPU times and all runs are preserved.
These results describe those inputs, not general parity. Codec references launch
external decoders; recorded RSS is child accounting rather than summed simultaneous
process-tree memory. The separate [32 MiB bulk-CRC experiment](bench/results/README.md)
records 119 MiB/s for bzip2, compared with 88 MiB/s in the earlier full benchmark.
Use `--extended`, repeatable `--case NAME`, and `--baseline PATH` to reproduce
focused comparisons without rerunning unrelated search cases.

The [native PCRE follow-up](bench/results/README.md) records
seven alternating rounds over 1 MiB inputs with output hashes checked against
ripgrep. Compared with the earlier Antfly binary, single-predicate repeats fell
from 32.3 to 5.7 ms, Unicode chains from 20.1 to 12.4 ms, and the simple-subroutine
case from 16.3 to 8.7 ms. Ripgrep measured 5.5, 9.2 and 16.1 ms respectively.
Native peak RSS was approximately 3 MiB in these cases. Host load was about 11;
these warm-cache cases establish specific improvements, not general JIT parity.
Reproduce with `--extended --mib 1 --rounds 7 --case pcre-repeat --case
pcre-unicode-chain --case pcre-subroutine --baseline PATH`.

The [native grammar sample](bench/results/README.md)
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

## Releases and installation

Grep has independent versions and release tags: `antfly-grep/vX.Y.Z`, including
prereleases such as `antfly-grep/v0.1.0-rc.1`. It is distributed separately from
the Antfly server and embedded library. See the repository's
[release design](../../../RELEASE.md#independent-grep-releases) for the operator
workflow and native qualification matrix.

Download the matching `antfly-grep_<version>_<OS>_<arch>[_gnu].tar.gz` archive and
`SHA256SUMS` from that tag's GitHub Release. Linux x86-64 and ARM64 have portable
musl and glibc 2.28+ (`_gnu`) builds; Darwin ARM64 requires macOS 15+. Use GNU
Linux builds for optional local dynamic inference. Verify the archive's entry
in `SHA256SUMS` before extracting it, then install under a user-owned prefix:

```sh
# After downloading the archive and SHA256SUMS:
awk '$2 == "antfly-grep_0.1.0-rc.1_Darwin_arm64.tar.gz"' SHA256SUMS | shasum -a 256 --check
mkdir -p /path/to/extracted-release
tar -xzf antfly-grep_0.1.0-rc.1_Darwin_arm64.tar.gz -C /path/to/extracted-release
/path/to/extracted-release/install.sh --prefix "$HOME/.local"
"$HOME/.local/bin/antfly-grep" --version
```

The archive includes the binary, installer, completions, manpage, schemas and
license notices. Installation does not require an Antfly server or libantfly.
Server-backed search remains an optional integration with its own endpoint and
credentials. Grep releases do not change Antfly's stable release channel.

## Continuous integration

The shared Zig workflow runs one native Linux x86-64 grep qualification job for
its admitted suite:

- `zig-base / grep` runs `grep-test` (unit, CLI, hardening, compatibility and
  integration fixtures), a seeded 500-case differential campaign against pinned
  ripgrep, and standalone packaging checks.
- `zig-full / grep` runs the same checks with 5,000 differential cases and adds
  the extended/codecs, single-CPU, cold-cache-advice, and 1/2/4/8-worker repository
  benchmarks.

Each job is required by its `zig-base` or `zig-full` aggregate. Full runs select
only the full profile, reusing its build for correctness and benchmarks. Base
runs follow the Zig change filter; inference-only validation skips grep. Reports
and the standalone archive are uploaded as `zig-base-grep` or `zig-full-grep`.
Server-backed indexed-search tests continue in the Antfly E2E base/full suites.

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
  --output ../output/grep-hardening/macos-arm64-hardening.json
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
hashes: [live differential](bench/results/README.md),
[fuzz campaigns](bench/results/README.md).

The earlier [paired CLI measurements](bench/results/README.md)
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
[monorepo run](bench/results/README.md) covers 10,119 files
and approximately 534 MiB; [Go SDK](bench/results/README.md)
and [TypeScript SDK](bench/results/README.md) runs cover
additional source trees. Those earlier measurements identified repository
discovery/search and late-prefix reset search as the next bottlenecks; the
optimization results below supersede them for those workloads.

[VM/JIT kernel profiles](bench/results/README.md) record dispatch
mixes and matching result checksums. Raw CLI CPU traces accompany the
[capture log](bench/results/README.md),
[reset log before owned-frame reuse](bench/results/README.md),
[final reset log](bench/results/README.md), and
[repository search](bench/results/README.md) reports.
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

An intermediate [directory stack sample](bench/results/README.md)
identified directory I/O and waiting; the [file-worker sweep](bench/results/README.md)
motivated the automatic scan-worker cap. Stack samples include blocked time.

The [final paired run](bench/results/README.md) compares
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
RSS. The [128 MiB check](bench/results/README.md)
keeps literal-miss RSS near 3 MiB and records literal misses at
13.82 vs 15.67 ms, capture/reset counts at
171.15 vs 324.78 ms, and repeats at
53.28 vs 133.06 ms.

Seven-round [Go SDK](bench/results/README.md) and
[TypeScript SDK](bench/results/README.md) checks also
beat ripgrep for discovery and file search. These are workload-specific warm-cache
measurements, not universal performance parity. Reports retain binary/corpus
hashes, versions, host load, first-result latency, CPU/RSS and individual runs.

Optimization regression tests compare compiled globs with the original VM,
vector repeats with scalar execution, and literal alternations/prefix skips
with grouped regex execution across case, word, context, replacement and count
options. They also cover directory-cache overflow, shared nested ignore scopes,
wide frontiers, null-separated lists, finite output limits, line buffering,
cancellation and broken-pipe cleanup.

[Final validation](bench/results/README.md) passes **118 CLI
tests on macOS and Linux** (two and one platform-specific skips), native unit
properties, all **1,500 live pinned-oracle comparisons** with no failures/timeouts,
and the Linux hardening suite. Three refreshed [coverage campaigns](bench/results/README.md)
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

The historical TypeSafe transport qualification used synthetic source and a
local constant-score fixture. The recorded
512-line case sends the same 513 questions in 65 requests and produces identical
output, while request-body bytes fall from 10,719,928 to 1,609,144 (85% less).
See the archived `followup-typesafe-transport.json` report in the [historical results archive](bench/results/README.md). The labeled repository
fixture separately reduces requests from 52 to 45 per question while preserving
all 304 evaluated targets and expected results. These are transport/orchestration
measurements, not provider token costs or model-quality evidence.

The bzip2 decoder reuses block workspaces with precise link-buffer capacity,
separates inverse-BWT traversal from RLE output, and uses SIMD to locate RLE
escapes before copying ordinary bytes in bulk. Randomized legacy blocks, CRCs,
output limits and streaming backpressure retain their checked paths.

The follow-up qualification and exact executable/source identities are in
[`followup-validation.json`](bench/results/README.md). The native
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

The archived `next-review-validation.json` report records the implementation commit,
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
[`bounded-review-validation.json`](bench/results/README.md).
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
Paired medians and executable hashes are in the archived `stream-review-*.json` reports:

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
[`stream-review-validation.json`](bench/results/README.md).


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

Decision batch cache v9 records each score's original creation time. Re-batching,
legacy migration and partial-cache repairs cannot rejuvenate old evidence. V6/V7/V8
batches and scoped v5 scalar files remain readable with their original file age;
new records use a separate versioned namespace. V9 uses packed little-endian
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
exact-batch tier keeps the ordinary warm-hit path. V9 coalesces each bucket's
headers into one file and one read, then reads the matching body. No per-way
open/stat loop or mmap is required. The header checksum includes the full key,
length, age and body, so in-place racing writes cannot alias identities. Readers
retry previously absent scalar tables at the next wave. Scalar-only hits are
promoted into batch records; mixed waves repair only changed shapes and avoid
rewriting worker batches that already have the original shape. Legacy presence
is inventoried lazily once per client, avoiding repeated negative probes on new
caches. Pinning
`--model-revision` prevents reuse after a model generation changes.

Source preparation is independent of questions. An artifact stores line offsets,
declarations and calls for exact content, path/language, delimiter/CRLF settings,
and authorization/revision scope. Its identity includes the projection source,
Zig version and every pinned parser/grammar source byte; parser updates invalidate
artifacts automatically. The private, atomically replaced artifacts occupy at
most 1,024 four-way buckets of 256 KiB per slot (1 GiB total).
V3 stores portable binary offset/index records and evicts the oldest written
artifact in a full bucket. A bucket contains one contiguous header directory and
compact bodies, published by atomic rename. Unlike fixed large offsets, this
layout does not allocate empty slot gaps on filesystems without sparse writes;
rewrites are bounded by the four-slot bucket size. V2 per-way artifacts migrate
after validation during writable searches, preserving their exact identities. Old V1 source artifacts are disposable
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
the archived `incremental-review-*.json` reports.

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
[`incremental-review-validation.json`](bench/results/README.md).


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
[`associative-review-validation.json`](bench/results/README.md).
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


### Coalesced cache buckets and compact JSON qualification

The follow-up fixes incomplete subdivision caching and previously absent score
tables, promotes scalar-only waves, and repairs only changed batch shapes. It
replaces per-way artifact probes with coalesced bucket headers. Source buckets
pack their bodies rather than allocating fixed large gaps. `--json-compact`
provides the opt-in version 2 event contract documented above; packaged man pages
and shell completions include the option.

Five paired warm runs over 2,048 generated Python files (65,536 selected source
lines), using the same native inference fixture and checking reconstructed match
payloads, probabilities, and scopes:

| Measurement | Previous PR head | Coalesced buckets |
|---|---:|---:|
| Median elapsed, regular JSON | 1092 ms | 732 ms |
| Median system CPU | 767 ms | 450 ms |
| Allocated cache bytes (`st_blocks * 512`) | 45,801,472 | 26,210,304 |
| Cache files | 10,159 | 4,438 |
| Warm inference calls | 0 | 0 |
| Source-preparation hits | 1,754 | 1,754 |
| Median peak RSS | 66.8 MiB | 66.9 MiB |

The separately paired compact run emits 19,070,307 bytes versus
49,065,982 bytes with regular JSON (61% fewer), with identical
reconstructed results and no warm inference. Its median elapsed is
724 ms versus 967 ms for the previous head.
These are fixture-only results on a shared macOS ARM64 host with warm filesystem
caches. They do not establish independent-host, cold-cache, provider latency,
billing, or model-quality improvements. Memory remains approximately unchanged.

The 15-run dense-output matrix remains workload-dependent: regular dense JSON
measured 25.9 ms versus 25.5 ms for the previous head, while literal `-o`
measured 6.1 ms versus 5.8 ms and regex color 12.7 ms versus 12.8 ms.
The separate compact matrix measured 25.4 ms for compact candidate JSON versus
25.6 ms for standard baseline JSON on the single dense record. A one-file warm
source case measured 11.0 ms versus 10.8 ms. These small workloads include startup and
shared-host scheduling; the repository-scale cache gain is not a blanket latency
guarantee.

Evidence: [cache scaling](bench/results/README.md),
[compact scaling](bench/results/README.md),
[output matrix](bench/results/README.md),
[compact output matrix](bench/results/README.md), and
[incremental checks](bench/results/README.md). Binary/source
identities and validation limits are in
[bucket-review-validation.json](bench/results/README.md).


### Bounded top-K evidence and shared score shards

Finite relevance ranking (`--max-result-files=K`, including the default for
`ask`) now selects evidence in a synchronized worst-first heap before rendering.
All eligible candidates are evaluated, while only K current winners retain typed
rows, declaration/call evidence, and owned provenance. Known losers need no
output allocation. Winners are serialized once through the existing renderers.
Excerpts describe the bytes originally searched: local files are never reread,
and indexed pages can be released before final output. Score/path/discovery-order
ties are deterministic. Retained arenas count their actual allocated capacity
against `--max-pending-output`; worker scratch, temporary replacement candidates,
parser/model memory and output buffers are additional. Metadata nesting beyond
512 levels reports `ResultNestingTooDeep` rather than using an unbounded stack.

This path applies to relevance-ranked file/table results with finite K. Explicit
sorting, unlimited ranking, stdin, inventory, count/list/quiet and multiline modes
keep their existing execution paths. Limiting displayed files does not limit the
search, inference budget, or final summary's evaluated-source counters. Successful
evidence survives errors; finite JSON output remains record-atomic, and budget
exhaustion flushes completed records and exits 2 without claiming completion.

Batch cache V10 uses 16 packed shards for each of three size classes, shared by
file and batch workers. At most 48 handles stay open until workers join. Positive
bucket offsets remain stable, so warm hits take one bounded page read without
open/stat/close operations. The 4 KiB shard directory allocates only occupied
4/8/32 KiB bucket pages. A best-effort cross-process writer lock protects page
allocation and updates; checksums reject torn/racing reads. Crash-orphaned pages
are reused. Batch payload capacity remains 48 MiB, plus 192 KiB of shard
directories, filesystem overhead and retained legacy formats. V9/V8 and earlier
records migrate without refreshing score ages; `--cached-only` performs no writes.
Source V3 and scalar-table V2 formats remain unchanged.

Compact JSON observation IDs are random opaque 64-character hex strings. Their
creation does not encode provenance; provenance is serialized only by the
budgeted record writer. A 2 MiB escaped-metadata fixture with a 1 KiB output limit
uses 19.3 MiB peak RSS versus
34.4 MiB at the prior PR head, with identical output lengths and exit status.

Paired warm results against `6d91fa53fdc0`, over 2,048 generated Python files:

| Measurement | Prior PR head | Current |
|---|---:|---:|
| Top 15, one worker, median elapsed | 564 ms | 348 ms |
| Top 15, one worker, median peak RSS | 66.7 MiB | 6.3 MiB |
| Top 15, one worker, median system CPU | 333 ms | 182 ms |
| Top 15, four workers, median elapsed | 235 ms | 135 ms |
| Top 15, four workers, median peak RSS | 71.0 MiB | 9.1 MiB |
| Unlimited results, median elapsed | 575 ms | 432 ms |
| One-worker cache files | 4,438 | 904 |
| One-worker allocated cache bytes | 26,210,304 | 26,824,704 |

All three comparisons reconstruct identical matches, probabilities and scopes,
retain 1,754 warm source-preparation hits and make zero warm inference calls.
Packed shard allocation costs about 2.3% more disk space in the one-worker fixture;
it removes thousands of files and bounds open handles. Unlimited output still
retains all rendered results for ranking, so its approximately 67 MiB RSS is
unchanged. Dense-output controls remain mixed: JSON 26.1 versus 26.0 ms, literal
match-only 6.1 versus 6.0 ms, and regex color 12.9 versus 13.2 ms.

These measurements use warm caches on one shared macOS ARM64 host and a constant
native inference fixture. They establish neither cold-cache/independent-hardware
speed nor lower cold-query model cost. Linux correctness runs in a two-CPU ARM64
VM; Windows runtime and live-provider/model quality remain unqualified.

Validation: **170 CLI tests on macOS and Linux** (two/one skips), native grep
unit/hardening/tooling checks, ten focused ReleaseSafe heap/cache tests, six Linux
hardening tests, Windows cross-compilation, 1,000 differential cases with no
mismatches/timeouts, packaging checks, Apache boundaries and 35 checker tests.
Regression fixtures cover ranking equivalence, ties/concurrency, bounded pending
storage, searched-byte ownership, released-page provenance, output budgets,
heading/color/context/trim parity, V9 migration and original score expiry.

Evidence: [single-worker ranking](bench/results/README.md),
[four-worker ranking](bench/results/README.md),
[unlimited ranking](bench/results/README.md),
[escaped provenance](bench/results/README.md),
[dense output controls](bench/results/README.md), and
[validation and source/binary identities](bench/results/README.md).
Reproduce ranking comparisons with `bench/cache_scale.py --top-k 15 --threads 1`
or `--threads 4`, passing the candidate/baseline binaries and native fixture
library; omit `--top-k` for unlimited output. `bench/provenance_budget.py` runs the
escaped-provenance HTTP fixture without a provider or model.


### Prepared-source reuse, bounded discovery and external ranking

Block-region construction now borrows the revision-validated source index from
source preparation. Cold files parse once; valid warm preparations do not parse
again. The additive `stats.source_parses` JSON summary counter records remaining
preparation parses. Standalone callers can still use the parsing region-builder
wrapper. Prepared/parsing equivalence tests cover nested Python, Go and Rust.

Source V3 reads share immutable bucket snapshots across file workers, with a
4 MiB total cap distributed over 16 locks. A positive key can reuse its verified,
content-addressed bytes; absent keys reopen the current bucket, observing atomic
replacements and migrations. Returned bodies belong to the file arena, so later
snapshot eviction cannot invalidate evidence. Buckets above a stripe's 256 KiB
allowance use bounded header/selected-record reads on the already-open descriptor
rather than reading or retaining an oversized whole bucket. Score V10 and source
V3 on-disk formats and authorization/revision scopes remain unchanged.

Decision discovery now feeds the bounded worker queue. Explicit triage policies,
unlimited file-count thresholds and a known single regular file start immediately.
When automatic path triage depends on the total file count, eligible paths go to
a private disk inventory until discovery finishes or crosses the threshold. The
queue then consumes that inventory with the same policy for every file. A known
single file runs on the calling thread while decision batches retain their own
bounded concurrency. Default terminal searches of `.` also use bounded ranking;
piped stdin retains its existing behavior. Worker
errors/cancellation close the queue, waking blocked discovery. Finite region
budgets keep serial file admission; final relevance ordering remains deterministic.

Excerpt probabilities use an ordered cursor over disjoint coarse regions.
Before/after context is a union of ordered intervals, with each mask position
written at most once. Ordinary full-content rows and retained excerpts share this
implementation. Large context settings and dense evidence no longer multiply
line-by-region or line-by-context work. Independent line evidence is preserved.

Per-file JSON elapsed time freezes when file processing finishes, then adds only
that winner's actual rendering work. Waiting for other files and sorting no longer
inflates per-file timing. A paired fixture exercises a fast file alongside a
separate file delayed by 2.2 seconds.

Unlimited relevance ranking keeps a bounded 256 KiB payload prefix and a
512 KiB key chunk in memory (allocator capacity is additional). Small inputs need
no scratch files. Larger payloads move once to disk, and key sets exceeding the
chunk target merge through binary levels with at most 64 run slots. Disk payloads
replay through a 64 KiB buffer; the complete output is not retained in RAM. Per-file serialization buffers still obey the shared
`--max-pending-output` allowance; source/model/parser scratch and the bounded
snapshot/key caches are additional. Explicit sorting, stdin and unranked modes
retain their existing execution paths.

`--max-spill-bytes` caps ordering payloads, a conservative allowance for merge
keys, and any live discovery inventory together. The default is 1 GiB; integer
bytes and K/M/G suffixes are accepted, and zero removes the cap. Filesystem block
allocation overhead is additional. Scratch exhaustion preserves completed results,
marks the search partial and exits 2. Finite JSON output stays record-atomic,
including an oversized record after an emitted prefix. Cached-only mode still
makes no provider calls or cache-artifact writes; ordering may use temporary
scratch. POSIX scratch files are private and immediately unlinked, including on
interruption; Windows files are removed when closed (runtime remains unqualified).

All changes remain native Zig, using the monorepo and standard filesystem APIs.

Final paired measurements against PR head `82ea997ae33a`, using warm filesystem
caches and the same fixture inference library on this shared ARM64 Mac:

| Workload | Median elapsed, before → after | Median peak RSS, before → after |
|---|---:|---:|
| Top 15, one worker / 2,048 files | 375.17 → 258.32 ms | 6.39 → 10.38 MiB |
| Top 15, four workers / 2,048 files | 132.47 → 109.12 ms | 9.23 → 12.08 MiB |
| Unlimited ranking / 2,048 files | 460.41 → 376.87 ms | 67.06 → 10.81 MiB |
| 9,000-line evidence overview | 46.27 → 15.44 ms | 7.95 → 8.17 MiB |
| Single small file | 8.44 → 7.75 ms | 4.91 → 4.27 MiB |

The source snapshot cache trades a few MiB for fewer reads in finite-K workloads;
unlimited ranking removes output growth from memory. Cached preparation hits do
not parse, but bounded bucket collisions still produce misses (294 reparses in
the one-worker 2,048-file fixture). Measured warm runs make zero inference calls.
The small-file result is only a sub-millisecond difference and needs broader
measurement. Dense-output controls are approximately flat: JSON 25.56 → 25.34 ms,
color 11.95 → 12.08 ms, match-only 6.04 → 5.92 ms, regex color 12.64 → 12.42 ms.

Evidence includes [single-worker ranking](bench/results/README.md),
[four-worker ranking](bench/results/README.md),
[unlimited ranking](bench/results/README.md),
[line evidence](bench/results/README.md),
[small files](bench/results/README.md),
[dense output controls](bench/results/README.md), and
[the timing regression](bench/results/README.md). Reports retain
binary/library hashes, versions, host details and per-run CPU/RSS observations.
Source/binary identities and qualification limits are in
[the validation record](bench/results/README.md).

Qualification: 175 CLI tests on each of macOS and Linux ARM64 (two/one skips),
native unit/hardening/tooling checks, seven focused ReleaseSafe cache/sort tests,
six Linux hardening checks, 1,000 generated ripgrep/PCRE2 comparisons with no
mismatches or timeouts, Windows cross-compilation, release packaging, formatting,
license headers, Apache boundaries and 35 boundary-checker tests. These fixtures
establish orchestration and output contracts, not live-model relevance or billing.
Linux runs in a two-CPU VM on this Mac; independent Linux hardware/cold-cache
performance, Windows runtime and full upstream PCRE2/JIT/CLI parity remain
unqualified. Existing real-server scenarios were not rerun in this follow-up.

### Sharded preparations and cross-file decision microbatches

This section records the f63d647a90 implementation and measurements. Source V5
below supersedes its preparation storage layout; the output and batching
contracts remain in effect.

Indexed ranked search now preserves complete staged JSON records when a later
record, error or end event exceeds the output budget. Version 1 and compact JSON
share this behavior. Ranked text inserts file-heading separators at replay time,
respecting `--no-filename` in finite and unlimited modes.

Preparations up to 4,016 encoded bytes use source V4 packed shards: 16 persistent
handles, 1,024 eight-way buckets and 4 KiB slots. Pages are allocated only for
occupied buckets, with a maximum logical allocation of 32 MiB plus 64 KiB of
shard directories. Pages serialize contiguous headers and live payloads;
unused slots are not written. Filesystem block allocation is measured separately.
Reads fetch headers and only the selected payload through
shared handles, avoiding per-file open/stat/close calls. Writes use the existing
best-effort process locks, packed page directory and checksum validation. Torn
records remain disposable misses. Eight-way retention reduces collision reparses
without increasing the RAM snapshot cap. Larger preparations still use bounded
source V3 buckets and the existing 4 MiB shared views. Retained V2/V3 files and
filesystem allocation overhead are additional to the new tier's disk cap.
Large V3 entries remain unchanged on normal warm reads, including bytes and mtime.
Validated old entries promote during normal searches; cached-only searches do
not promote, repair or write. Complete source bytes, parser generation, document
revision and authorization scope remain part of identity; this is not a
metadata-only shortcut around content validation. Score V10 is unchanged.

Directory exploration owns a separate decision client and HTTP pool. It borrows
the shared cache, model runtime and request limits, then merges its counters only
after file workers join. Directory I/O and provider latency no longer hold the
result-publication mutex. A blocked-directory fixture checks that queued file
work continues to drain and that request totals include both clients.

With multiple file workers and `--max-requests=unlimited`, underfilled region,
line and target requests can share a provider call. `--decision-batch-wait-ms`
sets a bounded gather window (default 1 ms; 0 disables; maximum 1,000 ms). Full
batches, single-file searches, serial indexed scans and finite request/region
budgets retain their existing paths. Each caller lends at most one request and
waits for completion; the oldest caller gathers compatible jobs and uses its
existing connection pool. Detached groups can execute concurrently. Pending
memory is bounded by file-worker admission, and each merged request respects
`--batch-size` (at most 128 inputs). No extra service thread or repository-sized
request queue is introduced.

Only clients sharing options, stage, question and global limits combine. Cache
lookup and persistence stay with each originating client and document scope.
Temporary unique wire IDs route answers back to the original inputs; shared
context remains borrowed. HTTP request/token totals count each provider call
once, including split/retry calls. Cancellation wakes through the bounded gather
window and existing HTTP cancellation. Completed scores survive a later split
failure under the existing partial-evidence rules. Tests compare scores and
ordering, change batch shapes for cached-only reuse, split rejected groups,
and exercise cancellation while jobs are waiting.

Cross-file batching reduces requests for underfilled work; it cannot reduce the
request count of a workload already filling every configured batch. Gathering
can add latency when provider calls are cheap. The opt-out permits direct
measurement for each provider rather than assuming request savings imply a
throughput or billed-token improvement.

Final paired warm-cache measurements against PR head `3f9bf90f2a40`:

| Workload | Median elapsed, before → after | Median peak RSS, before → after |
|---|---:|---:|
| Top 15, one worker / 2,048 files | 234.67 → 122.21 ms | 10.38 → 4.59 MiB |
| Top 15, four workers / 2,048 files | 103.23 → 61.55 ms | 12.19 → 5.12 MiB |
| Unlimited ranking / 2,048 files | 331.46 → 216.27 ms | 10.78 → 5.03 MiB |
| Single small file | 7.87 → 8.34 ms | 4.25 → 4.28 MiB |
| 9,000-line overview | 15.00 → 14.26 ms | 8.28 → 8.31 MiB |

One-worker system CPU falls 171.18 → 82.14 ms; preparation parses 294 → 0, and cache files 904 → 33. Allocated cache disk changes 25.53 → 45.27 MiB. Measured warm provider calls remain zero.

The bounded tier trades more allocated disk on this filesystem for fewer inodes/syscalls, lower RAM and better retention. Packing reduces writes and small-cache allocation; the filesystem still allocates much of the page address span in larger caches. Single-file startup remains slightly slower; see controls above. These measurements do not justify claiming every workload is faster or that allocated disk is smaller.

The 128-file HTTP fixture uses eight file workers, two provider lanes, 20 ms per request, no cache and the default 1 ms gather window. Median requests fall 128 → 16 and elapsed time 1986.21 → 555.40 ms. Each run sends the same 128 targets and reconstructs equivalent ordered evidence. Already-full batches retain their request count.

These are synthetic, warm-cache measurements on one shared macOS host. The provider fixture models request overhead and concurrency limits; it does not establish billed-token savings or live-model relevance.

Evidence: [one-worker search](bench/results/README.md), [four-worker search](bench/results/README.md), [unlimited ranking](bench/results/README.md), [single-file control](bench/results/README.md), [overview control](bench/results/README.md), [provider microbatches](bench/results/README.md), [generated comparisons](bench/results/README.md), and [source/binary hashes and validation](bench/results/README.md). Reports retain per-run timing, CPU, memory, cache allocation and/or request counts.

Qualification: 180 CLI tests each on macOS and Linux ARM64 (two/one skips), native unit/hardening/tooling checks, four focused ReleaseSafe artifact tests, six Linux hardening tests, 1,000 generated comparisons without mismatches or timeouts, Windows cross-compilation, packaging, Apache boundaries, staged license checks and formatting. Linux correctness uses a two-CPU VM on this Mac. Independent Linux hardware/cold-cache performance, Windows runtime, live providers and full upstream PCRE2/JIT/CLI parity remain unqualified; existing real-server scenarios were not rerun in this follow-up.

### Dense preparations and independent provider tuning

Source V5 retains eight-way lookup and 8,192 records across 16 private shard
files. Records pack contiguously at their actual encoded size; partially occupied
buckets no longer reserve 32 KiB and small preparations avoid 4 KiB slot padding. A compact, process-local routing index is bounded to
128 KiB total. Fingerprints only select candidate pages: the full key, metadata
and payload checksum must verify before decoding. Warm hits use one positional read of the selected header and payload. Payload publication precedes directory publication; readers
refresh missing or evicted keys, writers use nonblocking process locks, and
orphan extents are reclaimed at the hard file ceiling. Maximum logical V5 storage is
32 MiB plus 704 KiB of directories. Retained older namespaces and decision
score caches are additional. V2/V3/V4 preparations promote only after source
validation during ordinary reads; `--cached-only` never migrates or writes.

JSON workers share an immutable invocation working directory for provenance.
Score-key construction reuses the invariant SHA prefix and empty-context digest while preserving existing V5 score identities, credential
scopes, stages, revisions and document permissions.

`--decision-concurrency N` independently caps active provider calls (1..256;
0 follows `--threads`). Single-file batch pools use this limit too, and JSON
serialization/response scratch is held within provider lanes rather than
allocated by every waiting file worker. Provider
lane release signals waiting callers immediately; bounded timed waits still
observe cancellation and retry cooldowns. Guaranteed oversized batches split
before allocating their JSON bodies. Both ranked and unranked file-worker
paths share the bounded microbatcher; single-worker paths bypass it.
The JSON summary reports `request_bytes`, `request_inputs`, and `peak_request_bytes` for attempted provider calls, counted
once across batching, retries and workers. Cache-only runs report zero. These
measure submitted JSON payloads, excluding HTTP/TLS overhead; provider-reported `input_tokens`/`output_tokens`
remain separate and neither implies billed-cost or relevance guarantees.

Provider tuning can be saved explicitly, and CLI flags override saved values:

```sh
antfly-grep auth --provider antfly --model MODEL --stdin \
  --decision-concurrency 4 --batch-size 32 --decision-batch-wait-ms 1 \
  --decision-context shared-v1
```

Use shared-v1 only with supporting endpoints. It preserves effective target
text while sharing repeated context in the request. Larger batches and fewer
HTTP requests do not by themselves establish lower model token usage or equal
model relevance; retain a labeled evaluation for each provider/model change.

The tuning adapter measures the actual supplied repository/storage/provider
combination rather than assigning new host-specific automatic worker caps:

```sh
python3 zig/pkg/antfly-grep/bench/tune.py \
  --workers 1,4,8 --providers 1,2,4 --wait-ms 0,1,5 --batch-sizes 8,32 \
  --rounds 5 --output tuning.json -- \
  ./antfly-grep --ask retry --model MODEL --json --no-cache ./repository
```

It checks result evidence across configurations, alternates measurement order,
records CPU/RSS and timing variance, and measures first output separately.
Use a fixed labeled fixture for model comparisons; cached runs measure a warm
retrieval workload and `--no-cache` runs measure provider work. It does not drop
OS caches. Native Linux/cold-storage and real-provider qualification still
require those environments.

Paired warm measurements against f63d647a90 on the same shared macOS host:

| Workload | Median elapsed, before → after | Median peak RSS, before → after |
|---|---:|---:|
| Top 15, one worker / 2,048 files | 123.45 → 84.40 ms | 4.62 → 4.62 MiB |
| Top 15, four workers / 2,048 files | 63.46 → 40.95 ms | 5.14 → 5.38 MiB |
| Unlimited ranking / 2,048 files | 225.59 → 194.04 ms | 5.06 → 5.11 MiB |
| Single small file | 8.39 → 7.58 ms | 4.17 → 4.16 MiB |
| 9,000-line overview | 14.10 → 13.81 ms | 8.30 → 8.23 MiB |

One-worker system CPU falls 82.89 → 53.43 ms. The 2,048-file cache's logical
size changes 45.26 → 24.17 MiB and observed allocated space 45.27 → 24.20 MiB;
filesystem allocation remains platform dependent. Both binaries reparse zero
sources and make zero provider calls in these warm runs. RSS is roughly flat,
with a small increase in the four-worker case. The single-file cache has more
metadata overhead (observed allocated space 3.99 → 4.03 MiB).

The unranked tuning fixture uses 32 small files, eight file workers, two provider
lanes, a 10 ms constant-score provider delay and no cache. Changing the gather
window from 0 to 1 ms reduces requests 32 → 4 and request bytes 14,224 → 4,320;
median elapsed changes 293.97 → 109.69 ms. Separate repeated first-output
measurements change 34.35 → 35.82 ms: gathering trades a small initial delay for
lower overall work. Every configuration returns equivalent evidence. This
fixes the previously unconnected unranked worker path; ranked search already
used cross-file batching. Full native batches retain their original call count.
The 128-file ranked fixture retains 16 requests (498.68 → 495.86 ms).

The supporting shared-v1 transport fixture uploads 412,521 rather than
10,531,047 bytes with equivalent effective target text and output. This validates
an existing opt-in protocol and the saved provider configuration path, not a
new model-quality or billing claim. Local lexical/regex controls remain in the
report; the profiles do not justify additional JIT complexity in this pass.

Qualification: 184 CLI tests on macOS and ARM64 Linux, ten ReleaseSafe
cache/options tests on each platform, native unit/hardening/tooling checks,
1,000 generated ripgrep 15.2 / PCRE2 10.45 comparisons without mismatches or
timeouts, Windows x86-64 cross-compilation, packaging, license and formatting
checks. Linux uses a two-CPU VM on this Mac and a GNU/JIT-enabled oracle;
container-local executable temporary storage supports invalid-UTF8 filenames
and preprocessor fixtures. The host's filesystem bridge and noexec temporary
mounts do not provide those same contracts. Independent Linux hardware,
cold-storage performance, Windows runtime, live-model relevance and billed
cost remain unqualified. Repository CI still requires human approval.

Evidence: [one-worker search](bench/results/README.md),
[four-worker search](bench/results/README.md),
[unlimited ranking](bench/results/README.md),
[single-file control](bench/results/README.md),
[overview control](bench/results/README.md),
[local lexical/regex controls](bench/results/README.md),
[ranked HTTP control](bench/results/README.md),
[provider tuning and first-output trials](bench/results/README.md),
[shared transport](bench/results/README.md),
[generated comparisons](bench/results/README.md),
[oracle build differences](bench/results/README.md), and
[validation and hashes](bench/results/README.md).

### Budgeted batching and pinned workspace sessions

Cross-file gathering now applies with the default finite request budget and
includes file-path triage and file previews. One shared atomic reservation is
charged per actual provider attempt after gathering, including retries and
oversize splits. Queued targets do not consume request slots. Use
`--max-regions` to limit evaluated targets independently. Exhaustion still
returns exit 2 and retains confirmed partial evidence; batching can evaluate
more targets within the same request budget. Directory pruning remains ahead
of traversal and is not gathered with file evaluations. A zero gather window
retains individual requests for latency-sensitive or differential controls.

Unranked, unsorted decision searches use the existing bounded discovery queue.
Unlimited-file or explicit-triage searches overlap discovery and evaluation.
When automatic path triage depends on exceeding `--max-files`, the initial
inventory spills to bounded scratch until the threshold or EOF determines the
same triage policy. Explicit sorting and jevgrep overview presentation retain
their inventory requirements. Source/output/spill/request budgets and
cancellation remain shared across workers.

For editors and agents issuing repeated searches, start a private stdio session:

```sh
antfly-grep workspace serve --glob '*.go' ./repository
```

The service emits `workspace_ready` with protocol `version: 1`, a SHA-256
`revision`, file/byte counts, retained allocation, and preparation build/reuse
counts. Send one JSON object per line, with a numeric request ID:

```json
{"version":1,"id":1,"op":"search","revision":"COPY_READY_REVISION","args":["retry","--fixed-strings"]}
{"version":1,"id":2,"op":"invalidate"}
{"version":1,"id":3,"op":"refresh"}
{"version":1,"id":4,"op":"trim"}
{"version":1,"id":5,"op":"close"}
```

Search emits `workspace_begin`, ordinary grep JSON events, and `workspace_end`
with the request ID, snapshot revision, and ordinary 0/1/2 exit code. Validation
errors after begin still have an end envelope. Decision arguments such as
`--ask`, `--model`, `--no-rank`, budgets, thresholds and provider concurrency use
the ordinary search engine. Provider defaults/credentials are inherited from
normal configuration and environment. Sources are captured using ordinary
ignore/glob/type filtering at startup; discovery filters and filesystem input
transforms cannot be changed by a query. Snapshot queries currently reject
explicit sorting, file listings, table inputs, preprocessors, compressed-file decoding,
pattern files, path arguments and directory triage. Capture rejects the recursive `--follow` option; explicit file paths keep
their ordinary native input behavior. Configure discovery when starting the session.

Each result records its content revision and workspace snapshot with
`freshness: "snapshot"`. Queries search owned, immutable captured bytes, even
when the original files change or disappear. They perform no source-file
opens or source-content digest/preparation-cache validation; UTF-8 declaration
and call indexes are reused for ordinary newline mode. Decoding, CRLF and NUL
modes retain their ordinary preparation paths. Query execution and JSON output
still have CPU and memory costs, and score-cache accesses can still do I/O.

This is an explicit snapshot contract, not an automatically fresh filesystem
view. A capture reads each file consistently with before/after metadata checks;
it is not an atomic filesystem-wide snapshot. Integrations can report any
change, watcher overflow, disconnect or lost event using `invalidate`; searches
then fail closed until a successful `refresh`. Default refresh rereads discovery
rules and source bytes, verifies content identities, and reuses preparations for
unchanged path/content pairs. Opt-in `--workspace-watch` can certify unchanged
namespace/configuration/source state and skip redundant work; `force_rescan`
always uses a conservative capture.
It publishes only a complete generation. Failed refresh preserves the old
allocation but leaves it invalidated; it never silently searches stale data.
Content revisions remain deterministic for unchanged captures. No chain of old
generations is retained.

Requests execute serially and borrow the generation until all query workers
join. SIGINT cancels an active search using the existing cancellation path;
EOF/close releases the session. Frames are limited to 64 KiB; malformed JSON
gets a protocol error, and an oversized frame terminates the stream. There is
no network listener or persisted source copy. The default capture limits are
100,000 files and 256 MiB of allocator-accounted source/index/capture memory,
configurable with `--workspace-max-files` and `--workspace-memory-limit`.
Transactional refresh can temporarily retain two bounded generations, plus
per-query/provider/runtime memory governed by their existing limits.

`bench/workspace.py` checks evidence against ordinary invocations, measures
capture/refresh separately from warm queries, and can sample the exact tested
macOS process. `bench/decision_microbatch.py --file-previews` and `--unranked`
cover default-budget request gathering against a supplied baseline. Reports
retain binary/fixture hashes and per-run results. These fixtures establish
orchestration and evidence routing, not live-model relevance or billing.

Final qualification of the budgeted batching/workspace implementation: **194
CLI tests on macOS and ARM64 Linux** (two skips each), native unit/hardening and
tooling checks, 1,000 generated ripgrep 15.2 / PCRE2 10.45 comparisons without
mismatches/timeouts, Windows x86-64 cross-compilation, package asset checks,
Apache/license boundaries and formatting. Regressions include finite merged
request budgets/retries, previews, explicit-dot cache identity, saved provider
tuning and overrides, explicit binary input, CRLF/UTF-16 and anchor replacements,
invalidated/failed refreshes, parser-index ownership, capture quotas and framing.

Final warm measurements on this shared macOS host:

| Workload | Median elapsed, ordinary/baseline → optimized | Provider calls |
|---|---:|---:|
| Default-budget file previews / 64 files | 1,152.62 → 321.07 ms | 128 → 16 |
| Default-budget unranked search / 64 files | 666.71 → 201.74 ms | 64 → 8 |
| Repeated top-15 query / 1,024 synthetic Python files | 35.68 → 6.36 ms | 0 in both warm paths |
| Repeated query / actual Go SDK, 75 files / 5.9 MB | 80.52 → 26.63 ms | 0 in both warm paths |

HTTP fixtures use eight file workers, two fixture provider lanes and 10 ms
service delay; the candidate explicitly caps provider concurrency at two while
the baseline follows its file workers. Both use the default 1,000-request
budget. File-preview first-output medians change 1,122.37 → 300.15 ms; unranked
first output changes 33.81 → 36.35 ms. Gathering therefore still has a latency
tradeoff despite reducing total work. No HTTP throughput case crosses the adapter's
20% relative-MAD high-variance threshold.

Snapshot times compare ordinary warm invocations of the same final binary with
pinned in-process queries, including consumer JSON decoding. Initial capture
is measured separately: 60.73 ms / 117.85 ms for synthetic/SDK sources;
unchanged refresh takes 19.08 ms / 6.16 ms and reparses zero sources. Retained
source/preparation allocations are 5.52 MiB / 14.09 MiB. They exclude process,
allocator, model and per-query memory; the SDK process sample reports an 80.1M
physical footprint. Persistent snapshots trade resident memory for repeated
query latency, and are opt-in. Both warm paths use the same selected evidence
and scan statistics. The SDK fixture is a successful negative search, not a
model relevance evaluation.

Local controls against 03917b05bf retain broadly similar performance: repository
search 7.05 → 6.77 ms, discovery 6.12 → 6.51 ms, capture/reset counting
91.38 → 93.63 ms, repeat counting 29.50 → 29.67 ms, and 64 MiB literal misses
10.35 → 10.40 ms. These measurements do not establish an additional general
lexical/regex speedup. The same final-binary repository/large-file controls and
SDK process sample are retained; no new JIT coverage was added. Independent
Linux hardware/cold-storage performance, Windows runtime, automatic OS watcher
integration, live-model relevance and billed cost remain unqualified.

Evidence: [default-budget previews](bench/results/README.md),
[unranked decisions](bench/results/README.md),
[synthetic sessions](bench/results/README.md),
[SDK sessions](bench/results/README.md),
[exact production-binary sample](bench/results/README.md),
[local controls](bench/results/README.md),
[generated comparisons](bench/results/README.md),
[release archives](bench/results/README.md), and
[validation/source hashes](bench/results/README.md).

### Workspace resource reuse

Workspace requests use the same provider finalization as ordinary CLI searches:
base URLs receive `/decisions`, and Typesafe defaults and environment credentials
are resolved after request arguments. Full endpoint paths retain ordinary CLI
semantics.

A session retains separate file and provider worker pools, with at most 32 idle
threads in each pool. Higher explicitly requested concurrency can use temporary
threads. Every query joins its leases before releasing request options, buffers,
cancellation state or the source generation. HTTP connections and the lazily opened
native inference handle survive compatible queries and refreshes. Changing the
endpoint, credential, backend, model directory, library, timeout or cache directory
retires that provider scope after its workers finish. Request budgets, cancellation
and statistics remain per query; no model/library is opened for a pure cache hit.

Unchanged files share reference-counted source/index blobs across refreshes.
References point directly to independent blobs, never to previous generations.
Each generation counts the full allocation of every referenced blob against its
capture quota; failed capture releases acquired references. Identifiers retain offsets into captured bytes when possible; derived names
are owned independently and source kinds are interned within each blob. Refresh still rereads and hashes source bytes
to establish content identity, and still follows the explicit snapshot contract.

A disposable region-plan cache retains at most 16 MiB of allocator-accounted plans,
including construction and evicted plans still borrowed by active queries. Each
plan is limited to 4 MiB; allocation failure falls back to ordinary evaluation.
Plans retain region metadata and compatible decision keys, not copies of formatted
provider text or source bytes. Listings are materialized from the active generation
only when needed for a new key or an actual provider request. Raw UTF-8 preparations
also certify validation, so unchanged input is not rescanned for UTF-8/NUL validation
in the decision content path. Decoding and CRLF/NUL modes preserve their original paths.

A separate 4 MiB payload cache retains checked batch records within the current
provider/cache scope. Cache timestamps and scores are validated on every use;
reading never renews a timestamp. Query/policy/model/document changes invalidate
derived keys using the ordinary cache identity. `--no-cache`, `--cached-only` and
zero TTL retain their existing meanings. These caches are resident-memory tradeoffs,
not additional provider-result guarantees or a zero-cost execution claim.

Qualification of resource reuse compares the saved `6fa6dbfb` binary with the
candidate using 101 alternating, evidence-equivalent queries per binary and
workload. Native macOS, a warm filesystem and a constant-score inference fixture
are used; this is not a live-provider relevance or billing evaluation.

| Pinned workload | Baseline median | Candidate median | CPU per query, baseline → candidate |
|---|---:|---:|---:|
| SDK, 75 files / 5.9 MB | 29.28 ms | 20.68 ms | 29.60 → 20.79 ms |
| Synthetic, 1,024 files | 9.88 ms | 8.00 ms | 8.32 → 5.84 ms |

The median paired speedups are 1.42× / 1.25× for SDK/synthetic sources,
with relative MAD 2.8% / 3.3% and 9.6% / 11.5%, respectively. Both campaigns
show approximately 30% less CPU per warm query. CPU is cumulative process time
sampled using Darwin `ps` (0.01-second resolution) across each entire campaign,
divided by its query count. Both services stay resident and timed queries alternate.
Capture/refresh are excluded from those warm-query timings.

SDK samples report physical footprint **79.9M → 97.8M**, with the same reported
peaks in those samples. Retained source/index bytes, sampled footprint and peak
footprint are distinct measurements. This implementation retains bounded plans,
records, threads and provider resources in exchange for less repeated work; it
does **not** establish lower RSS or a lower startup/capture peak. Source/parser
arenas use the ordinary search allocator and resource accounting to preserve
bounded parser behavior. Sequential reports on a heavily contended shared host
are retained too and are not used as headline speedup evidence. Local lexical
controls are mixed; no additional general lexical/JIT performance win is claimed.

Reproduce the paired SDK campaign with:

```sh
python3 pkg/antfly-grep/bench/session_compare.py BASELINE CANDIDATE \
  --library NATIVE_FIXTURE --repository ../go/pkg/sdk --rounds 101 \
  --output sdk-paired.json
```

Evidence: [paired SDK](bench/results/README.md),
[paired synthetic](bench/results/README.md),
[baseline sample](bench/results/README.md),
[candidate sample](bench/results/README.md),
[local controls](bench/results/README.md),
and [generated differential cases](bench/results/README.md).

Final qualification: **199 CLI tests on macOS** (two skips) and **199 on ARM64
Linux** (one skip), native unit/hardening/tooling checks, 1,000 generated pinned
ripgrep/PCRE2 comparisons without mismatches/timeouts, Windows x86-64
cross-compilation, three real target archive/hash checks, packaging, license
boundaries, generated compatibility and formatting. Linux runtime checks use
an isolated two-CPU Docker VM; independent hardware performance, Windows runtime
and live-provider quality remain outside this qualification. The source/binary
identities and scope limits are in [validation](bench/results/README.md).


### Complete-query and per-file decision reuse

Workspace sessions retain two additional bounded, typed caches. A 4 MiB cache
reuses complete finite-top-k ranked decision results for identical query options
and snapshot content. It retains selected evidence, ranking/omission counts and
logical scan statistics. Hits skip file traversal and evaluation, then render
through the normal budgeted output path with fresh timing, observation metadata,
compact file IDs, request envelopes and transport counters. Cached logical
`regions_evaluated` counts describe the result's evidence; `query_cache_hits: 1`
and zero transport counters identify the work avoided by this invocation.
Unranked/unlimited-output searches keep their existing streaming/spill paths.

An independent 8 MiB cache retains completed per-file decision outcomes, including
rejected previews/regions and files excluded by top-k. After refresh, unchanged
content reuses those judgments while changed files are evaluated normally and
all qualifying results are ranked again. It retains no source bytes, source
indexes, generation leases or provider instances. Repeated selected/probability/
line-score states use lossless runs preserving floating-point and NaN bits;
highly variable outcomes use the smaller dense representation. Optional run construction uses a reclaiming allocator bounded to 2 MiB; failed admission leaves the completed search outcome intact. The
`file_evaluation_cache_hits` counter reports this reuse. Finite global region
budgets deliberately use ordinary per-file evaluation so partial-prefix behavior
is preserved when another file grows after refresh.

Complete-query identity hashes all effective options, including provider credentials,
model/revision, policies, thresholds, rendering and budgets. File-evaluation identity
excludes only an explicit presentation-field allowlist (context shown, top-k/output
caps, styling, IDs and separators); context used by the model, evaluation/refinement
controls, policies, credentials and budgets remain part of that identity. Content
and paths scope file entries, and the snapshot scopes complete queries.
Both caches expire at the earliest contributing judgment timestamp, including
negative evidence and omitted files, and reject excessive future clock skew.
Reading does not extend expiry. Interrupted/failed/incomplete evaluations never
become complete hits. `--no-cache`, zero TTL, cache-only provider restrictions,
changed identities, eviction and allocation failures keep their ordinary
fallback behavior. Both caches are four-way associative with per-set LRU replacement.
Admission constructs an independent candidate before committing any eviction;
failed/oversized admissions preserve existing entries, including same-key entries.
Immutable leases pin values during rendering, avoiding deep copies, heap rebuilding
and repeated sorting. Active evicted leases and construction share the physical
cap. Each entry's construction budget is at most one quarter of the cache cap;
permanent construction headroom reserves that quarter plus one entry header.
Lookup/render scratch and allocator metadata remain additional memory.

Region-key preparation now runs only after a batch reserves its region budget,
and observes cancellation between regions. A new query with `--max-regions=0`
therefore never formats or hashes unadmitted listings. Cancellation propagates
instead of silently switching to uncached formatting.

Send `{"version":1,"id":4,"op":"trim"}` to release optional caches, native/HTTP
provider resources and idle file/provider threads. The `workspace_trimmed`
response reports released cache-entry bytes, zero remaining cache-entry bytes, and retained source memory. Explicit trim also requests best-effort allocator pressure relief on macOS and glibc Linux; other targets safely omit that hint.
The snapshot and invalidation state remain intact; later queries can reuse disk
scores and rebuild disposable state. Released allocation and OS RSS differ:
allocator arenas and the OS can retain freed pages, and captured sources remain
resident until close. Combined retained cache caps are now 16 + 8 + 4 + 4 MiB
(plans, file outcomes, complete queries, batch payloads), versus the previous
32 + 4 MiB. Lexical-only sessions no longer initialize provider resources.

Refresh defaults to conservative discovery, reads and hashing. Opt in with
`antfly-grep workspace serve --workspace-watch PATH` to let native kqueue (macOS)
or inotify (Linux) witnesses skip unchanged content reads. Witness state is
allocated only when explicitly enabled. A bounded discovery journal covers
visited directories, their ancestors, optional/missing ignore/config inputs, Git
includes/HEAD/gitdir dependencies, and canonical symlink-target ancestors. While
that coverage remains complete and clean, refresh can reuse discovery membership
and skip opens for intact source witnesses. With no source or dependency events,
it republishes the same owned generation without an O(file-count) inventory walk.
Otherwise uncertified paths use open/stat/device/inode/timestamp checks and normal
reads; incomplete namespace coverage uses ordinary discovery. Subscriptions
are bounded (at most 4096 inode witnesses per owner, further capped at one
quarter of the file-descriptor limit). Linux named directory events can cover
additional regular files without individual inode subscriptions; macOS always
requires an inode witness to certify source contents. Missing subscriptions,
admission failures, overflow,
watch loss, unsupported targets and errors fall back to ordinary reads. Linux
event dispatch uses a bounded handle map. Capture retries without watches if
watch resources cause a capture failure. `workspace_ready` reports
`source_bytes_read`, `source_bytes_reused`, `sources_opened`, `discovery_reused`
and `watch_fallback`. The dependency journal has a 1 MiB allocation cap and its
own bounded native watch table. The Linux directory event owner has a separate
2 MiB allocation cap. macOS uses strict kqueue source/directory witnesses and
directory membership certificates; asynchronous FSEvents is not a certification
mechanism. Linux directory-prefix routing uses the generation's sorted inventory
rather than a second per-file pathname table. Resource-related capture failure closes both
watch owners and retries conservatively without watch allocations during capture.

Use watching only where kernel events cover the writers/filesystem in use.
Linux mmap writes, remote filesystem changes and writes through out-of-tree
hard-link aliases may bypass directory notifications. Known multi-link files
outside inode coverage cannot receive directory-only content certificates.
Send `{"version":1,"id":5,"op":"refresh","force_rescan":true}` for a conservative
read/hash capture that also rebuilds witnesses. Trim drops witnesses and the
next refresh reads sources normally. Watches accelerate explicit refresh; they
do not update the published snapshot or make captures globally atomic.

Complete, admitted ranked results publish an optional 64-character `result_token`
after the summary. Repeat the same search request with `if_result_token` to
receive `workspace_unchanged`, a fresh summary and normal begin/end framing,
avoiding repeated file/match output. Tokens are random cache-entry identities,
not content or credential hashes. Acknowledgements require a valid revision,
matching effective query/provider/policy identity, live unexpired evidence and
an uncanceled query. Misses/eviction/trim/expired evidence take the normal search
path; changed bytes produce a different snapshot. Conditional acknowledgements
refer to the requested pinned snapshot, not the live filesystem. Clients must
consume through `workspace_end`, require successful exit and complete summary,
and retain their original result data. Optional token metadata is omitted when
it would exceed the output budget; required responses use normal budget errors.

Search cleanup preserves sentinel allocation length; canceled empty cache hits
now finish incomplete with exit 2. Regressions cover these findings, transactional
admission/collisions, lease lifetime, fresh compact IDs, conditional responses,
presentation reuse and watched discovery/edits/replacements/forced rescans.
Cache hits avoid repeated evaluation/provider billing while valid. Request
parsing, lookup, validation, output, retained snapshots and refresh still consume
resources; this is not zero-cost execution.


The final native macOS campaign compares `c213e061` with this implementation,
using the same saved inference fixture and 101 alternating resident queries per
binary. Both paths verify identical selected evidence and logical scan counts;
all timed queries make zero provider requests and perform zero source parses.

| Workload | Baseline → candidate median | CPU/query, baseline → candidate |
|---|---:|---:|
| SDK, 75 files / 5.9 MB, **no selected results** | 17.95 → 0.14 ms | 18.12 → 0.10 ms |
| Synthetic, 1,024 files, selected top-15 output | 4.96 → 2.89 ms | 3.47 → 0.59 ms |
| 4,096 lines / 500 context lines, new query, zero-region budget | 89.38 → 0.21 ms | not sampled |

The synthetic median paired speedup is 1.71× with approximately 83% less CPU.
The empty-result SDK fast path is approximately 131× faster by paired median;
its tiny elapsed times have 24% relative MAD, so this is a measured empty-result
optimization, not an output-heavy or general search guarantee. The budget case
is approximately 428× faster with zero evaluated regions and zero provider calls.
Reports retain per-trial data, counters, host load, fixture/binary hashes and
scope limits. CPU uses cumulative Darwin process time at 0.01-second resolution
across each 101-query campaign. Capture and refresh are excluded from warm timings.

The final SDK sample reports **92.1M physical footprint**, versus **97.8M** in the
previous resource-reuse sample. Explicit trim releases **4.69 MB** of cache-entry
allocation but the same process sample remains **92.1M** (92.2M recorded peak),
so allocator relief is best effort and no proportional RSS reduction is claimed.
The earlier `6fa6dbfb` sample was 79.9M; the current feature set still retains
more memory than that older baseline. Source/index retention is approximately
14.47 MB in this SDK capture. These samples do not establish cross-host or cold-
capture memory guarantees.

Evidence: [paired SDK](bench/results/README.md),
[paired selected output](bench/results/README.md),
[budget admission](bench/results/README.md),
[profile campaign](bench/results/README.md),
[active sample](bench/results/README.md),
[trimmed sample](bench/results/README.md), and
[generated comparisons](bench/results/README.md).


Final qualification passes 204 CLI tests on macOS (two skips) and ARM64 Linux
(one skip), native unit/hardening/tooling checks, 1,000 generated comparisons
without mismatches/timeouts, Windows x86-64 cross-compilation, license boundaries,
formatting and three actual release archive/hash checks. Linux runs in a two-CPU
isolated Docker VM; Windows runtime, independent Linux hardware performance and
live-model relevance/billing remain unqualified. See
[validation/source hashes](bench/results/README.md) and
[release archive contents/hashes](bench/results/README.md).


### Conditional replay qualification

The current native macOS fixture campaign compares PR head `557b5f6b` with
immutable leased replay. Alternating 51 paired queries preserve matched evidence,
logical scan statistics, zero provider requests and zero source parses. Full
replay is effectively unchanged: paired median speedup **1.02×**, process CPU
about **0.78 ms/query** for each binary (coarse cumulative CPU sampling). Separate
elapsed medians were 2.33/2.32 ms. This does not establish a substantial new
full-output throughput win.

A 128-file selected-result campaign measures the protocol savings separately:

| Warm operation | Client elapsed median | Producer elapsed median | Response bytes median |
|---|---:|---:|---:|
| Full cached result | 2.25 ms | 1.75 ms | 419,059 |
| Matching conditional token | 0.143 ms | 0.00283 ms | 1,212 |
| New presentation, reused file judgments | 2.54 ms | 1.99 ms | 419,061 |

Conditional requests transfer **99.7% fewer bytes** and reduce observed client
elapsed time about **15.7×** on this fixture. Producer elapsed includes pipe
backpressure and excludes option parsing/identity setup and summary serialization;
client elapsed includes framing and Python JSON decoding. Neither timer is CPU
time. Presentation variations miss complete-query reuse and hit all 128 file
outcomes with zero provider calls. Repeated queries on unchanged inputs avoid
provider evaluation only while evidence remains valid.

On the same 128-file corpus, unchanged watched refresh reads **zero source bytes**
(reusing 47,122 bytes), taking median **4.47 ms** versus **8.16 ms** for alternating
forced conservative rescans. This is a small warm corpus, not a large-repository,
cold-cache, native Linux hardware or remote-filesystem result. Measurements run
on a shared macOS host and are not performance guarantees. The exact production
binary sample reports **13.3 MB** physical footprint for the separate synthetic
1024-file profile; no cross-workload RSS improvement is claimed.

Evidence: [paired full replay](bench/results/README.md),
[conditional/presentation/refresh](bench/results/README.md),
[production-binary profile campaign](bench/results/README.md),
[sample](bench/results/README.md),
[generated comparisons](bench/results/README.md),
[validation/source hashes](bench/results/README.md), and
[release archives](bench/results/README.md).


### Registered queries and exact incremental delivery

For an editor or agent retaining results, register the query once. Registrations
bind immutable effective options, provider/authentication, policy and model
identity to an opaque process-local handle. Compact JSON is the default for this
new operation; set `compact:false` during registration for legacy JSON. Ordinary
`search` requests keep their existing output defaults.

```json
{"version":1,"id":1,"op":"register","args":["--ask=retry","--model=MODEL","--max-result-files=15"]}
```

Retain the `query_registered.query_handle`. Searches using it supply no `args`:

```json
{"version":1,"id":2,"op":"search","revision":"REVISION","query_handle":"HANDLE"}
{"version":1,"id":3,"op":"search","revision":"REVISION","query_handle":"HANDLE","if_result_token":"TOKEN"}
```

Matching conditional hits check the bound identity, current valid revision,
oldest-evidence TTL, output budget and final cancellation without parsing options,
compiling/cloning matchers, hashing option JSON, looking up cwd or configuring a
provider. Misses use the registered compiled program with ordinary search
semantics. Handles never authorize a different model, credential or policy.
`forget` retires one handle; `trim` retires all; LRU eviction reports
`UnknownQueryHandle` and clients can register again. Failed registration preserves
existing handles.

Each eligible registration maintains a bounded exact index of **all** completed
positive and negative file outcomes, plus an ordered treap of qualifying scores.
The index owns compressed/dense selection evidence and declaration labels, not
source bytes. It accounts for each file's logical scan statistics. A refresh
publishes changed paths and the preceding distinct content revision. When that
revision matches, only added/changed files enter the existing bounded worker
queue/provider batching; deletions simply retire records. A lone changed file
bypasses the cross-worker batching wait. Selecting new winners walks the score
index, including previously losing candidates. Unchanged winning payloads borrow
an immutable prior-result lease through rendering and transactional admission.
Changed/promoted winners materialize from owned outcomes and current source
preparations, with current URI, revision, snapshot and observation metadata.

This path requires complete, live evidence and compatible raw UTF-8/newline
inputs. Finite global region budgets, transformed/unsupported inputs, skipped
revision chains, expired evidence or memory admission loss use ordinary search.
Optional index admission failure disables rebuilding for that content revision,
so complete-query hits remain cheap. No approximation substitutes for missing
state. Cache hits reset per-invocation work counters; `incremental_cache_hits`
and `incremental_files_evaluated` expose actual incremental execution separately
from logical corpus/evidence counts.

After a refresh, a client retaining the previous complete result can request:

```json
{"version":1,"id":4,"op":"search","revision":"NEW_REVISION","query_handle":"HANDLE","if_result_token":"PREVIOUS_TOKEN","delta":true}
```

A matching retained baseline can produce `workspace_delta` with `base_token`,
current `revision`/`observed_at`, the complete ordered selected-path list, and
removed paths. Only changed/promoted selected files emit ordinary begin/result/end
payloads; unchanged files keep their retained view and rebase snapshot observation
metadata from the delta. Unknown/missing baselines return a full response.
Stage changes and commit only after a complete summary and
`workspace_end` with exit 0 or 1; errors or partial results preserve the previous view as an old
snapshot. Replace the baseline only with a `result_token` actually received. If
none is supplied, clear conditional state and request full results next time.

Use editor-owned file-change events to request refresh/search when needed; the
service blocks on stdin while idle and has no polling timer or live subscription
claim. Kernel-invisible changes still require `force_rescan`. Valid conditional
hits make zero provider calls, but retained memory, validation, I/O and edits have
resource costs.

Registrations are optional and bounded to eight LRU plans. Each plan has a 2 MiB
cap covering owned options, compiled patterns and retained delivery metadata, and
a separate 4 MiB file-index cap. Admission constructs one extra plan before
replacing a victim: at most 50 MiB plus fixed owner headers across retained plans
and registration construction, additional to existing caches/source/runtime/query
memory. These are ceilings, not eagerly allocated reservations. Large programs
can use ordinary requests if registration admission fails. Typed-cache and region
plan leases now return physical quota **after** backing deallocation; the checked
adversarial allocator interleaving stays below its cap.

#### Registered-query qualification

The final binary was tested with 215 CLI cases on native macOS and Linux ARM64
under a two-CPU Docker VM, 1,000 seeded differential cases (zero differences or
timeouts), package tests, hardening tests, and Windows cross-compilation. Windows
runtime behavior and native Linux hardware performance remain unverified.

A paired, warm, synthetic 128-file macOS fixture compared ordinary compact search
with registered search using the **same production binary**. Every trial checks
that the reconstructed delta view matches full results, including source URI and
content revision. Medians include framing and client JSON decoding:

| Operation | Ordinary | Registered | Observation |
|---|---:|---:|---|
| Full replay | 1.388 ms | 1.291 ms | Same 140,374-byte response |
| Conditional hit | 69.75 µs | 34.17 µs | 2.04× faster; zero provider calls |
| One-file edit | 3.089 ms | 0.314 ms | 9.83× faster; one provider evaluation each |
| Edit response | 140,381 bytes | 1,927 bytes | 98.63% fewer bytes |
| Refresh | 9.594 ms | 0.507 ms | Forced rescan versus opt-in certified journal |

The refresh comparison intentionally uses different capture guarantees: the
conservative side opens 128 files, while the certified side opens one changed
file. It is not a same-mode I/O speedup. Full replay improves modestly; retained
state primarily benefits conditional hits and edits. Sample RSS after warming was
9,200/9,456 KiB (ordinary/registered), and after edits 12,288/11,408 KiB. Trim
released 1,731,556/1,938,844 accounted cache bytes; allocator RSS need not fall by
that amount. This is retained-memory work, not a zero-footprint service.

Seven-round warm local controls on the same shared host measured 12.42/12.12 ms
for literal misses (antfly-grep/ripgrep), 6.78/9.71 ms for SDK file discovery, and
7.40/10.78 ms for SDK search. These small, noisy controls do not establish a
universal win over ripgrep. No new JIT optimization or provider billing reduction
is inferred from the registered-query fixture.

Evidence: [paired protocol trials](bench/results/README.md),
[local controls](bench/results/README.md),
[differential results](bench/results/README.md),
[validation and source identities](bench/results/README.md), and
[verified release archives](bench/results/README.md).


### Incremental refresh and shared evidence follow-up

Registered indexes may advance their revision only after their own file outcomes
have been populated or updated. A complete-result cache hit from another request
cannot certify an older file index. This has a cross-request A → B → C regression;
rendering a mismatched indexed file now fails explicitly instead of silently
omitting it from a complete response.

Capture no longer parses every source file. Decision workers lazily prepare
compatible files, with independently owned immutable memos and bounded peak
reservations against the workspace quota (2–32 MiB per active preparation,
limited by remaining quota, with unused capacity returned after parsing). Different files can prepare concurrently;
failed optional memo admission uses ordinary query preparation. Lexical captures
and searches require no declaration/call parsing. Prepared memos survive compatible
refreshes and remain owned independently of the generation that first built them.

Opt-in certified refresh routes dirty watch slots to source entries (including
hard-link aliases) and stages only those files. With an exclusively owned generation,
publication replaces staged blobs and updates a canonical path-keyed Merkle tree in O(D log N)
work, without rebuilding/sorting the full inventory. No fallible operation follows
publication. Retained readers, uncertain witnesses, new events during capture,
configuration/topology changes or insufficient admission use ordinary capture.
Opaque workspace revisions now use the canonical version-3 inventory root; forced
rescans and incremental refresh produce identical revisions for identical captures.
The protocol version remains 1. `refresh_entries_visited` distinguishes inventory
work from source opens; cached retained-byte totals keep unchanged ready responses
independent of corpus size. Certified file-membership updates use affected-directory
reconciliation; uncertain membership still performs ordinary discovery.

Complete-result entries now share immutable file payloads. Metadata and all shared
payload allocations, construction, growth and evicted active leases participate in
the existing 4 MiB query-cache physical cap. Payload quota retires after backing
memory is freed. A new revision copies its bounded metadata and new/promoted file
payloads, while unchanged winners retain existing payloads. A unit regression checks
pointer identity, negligible metadata growth and charge retention through eviction.
Delta membership, winner lookup and change checks use path maps, eliminating the
previous quadratic top-k comparisons. Delivery-map memory remains in the existing
2 MiB registered-plan budget.

These paths reduce work for repeated and incremental queries. They do not promise
zero memory, free cold inference, or fresh evidence after TTL expiry. Clients can
avoid unchanged requests using their editor revision and retained complete result;
misses and expired evidence still require evaluation or explicit partial cached-only
results. The service continues blocking on stdin while idle.

#### Follow-up measurements and verification

The frozen follow-up binary passes **218 CLI cases on macOS and Linux ARM64**,
71/72 native package tests (one platform skip), all 72 Linux package tests, six
Linux hardening tests, 1,000 generated differential cases without differences or
timeouts, and Windows cross-compilation. Linux runtime checks use a two-CPU Docker
VM; Windows runtime and independent native-Linux hardware throughput remain unverified.

Paired controls against the preceding PR binary use 64 Python functions per file,
seven alternating new-process captures after warmup, and eleven paired dirty
refreshes. The filesystem is warm; this does not measure cold disk caches.

| Corpus | Capture before → after | Dirty refresh before → after | Accounted capture bytes before → after |
|---|---:|---:|---:|
| 64 files | 17.58 → 8.45 ms | 0.410 → 0.176 ms | 1,227,882 → 327,736 |
| 256 files | 51.99 → 16.77 ms | 0.427 → 0.192 ms | 5,064,004 → 1,470,218 |

Every optimized dirty refresh visits and opens one source entry. Each fixture's
final incremental revision equals its forced-rescan revision. Capture allocations
are 71–73% lower here; these are allocator-accounted bytes, not an RSS guarantee.
macOS now shares watch subscriptions by device/inode, preserving per-capture
sequence checks, so replacement reads need no extra subscription at full capacity.
Ordinary unwatched captures allocate no watch-routing arrays.

A separate 19-pair comparison edits a **selected** file and reconstructs both
binaries' complete result views after every delta. Refresh plus search falls from
0.814 to 0.570 ms at 15 winners, and 0.940 to 0.666 ms at 128 winners (about 1.4×).
Search alone is slightly slower: 0.426 → 0.448 ms and 0.477 → 0.523 ms. Lazy
preparation moves parsing from refresh into search, so end-to-end latency is the
appropriate comparison. No isolated search-speed improvement is claimed.

The same-binary ordinary/registered fixture edits an **unselected** file. At 15
winners, full delivery is 3.210 ms / 140,381 bytes versus 0.350 ms / 1,925 bytes for
a delta; at 128 winners, it is 14.786 ms / 1,177,508 bytes versus 0.557 ms / 3,457
bytes. These are delivery-mode comparisons, not improvements against the previous
binary. Both modes make one fixture evaluation per edit. Valid warm hits make zero
provider requests. Sample RSS after edits is 13,104/10,368 KiB for ordinary/registered
services in the 15-winner fixture; retained state and allocator behavior still cost
memory. No live-model cost, relevance, universal ripgrep-speed or literal zero-cost
claim follows from these shared-host synthetic measurements.

Evidence: [paired startup/refresh](bench/results/README.md),
[paired selected-file edits](bench/results/README.md),
[15-winner delivery](bench/results/README.md),
[128-winner delivery](bench/results/README.md),
[differential](bench/results/README.md),
[validation/source identities](bench/results/README.md), and
[release archives](bench/results/README.md).


### Strict refresh and active evidence follow-up

Refresh fallback revalidates the namespace certificate after a rejected dirty
capture. An invalidation observed while reading a source cannot be carried into
trusted-membership fallback. Atomic saves verify the affected directory's names,
kinds, inode and permissions, then stage only changed source entries. Ancestor
notifications revalidate tracked pathname identities and absent configuration
targets; unrelated siblings do not require reopening all sources. Watch rearming
uses validated sequence numbers, preserving events arriving after validation.
Healthy directory-event subscriptions survive rediscovery; namespace/configuration
certificates are rebuilt, and events racing that rebuild remain pending. Directory
loss, overflow, alias uncertainty and admission failure use conservative
discovery and reads. The original follow-up rebuilt inventory metadata on additions/deletions;
the subsequent membership implementation below removes that rebuild when
source and directory coverage remain complete.

Linux uses synchronous named inotify directory events beyond the individual-file
watch budget. macOS uses bounded strict kqueue inode and directory witnesses without
an asynchronous event layer. Captures beyond the macOS witness budget explicitly report
`watch_fallback` and read unwitnessed sources. Tests mutate every file in a
512-file macOS corpus, sample a 4352-file Linux corpus, and verify conservative
macOS reads above its witness budget. Optimized revisions are checked against
forced rescans. Windows runtime conformance remains unverified.

Complete-result cache entries contain payload pointers and a path lookup built
once at admission. Full rediscovery batches witness polling, validates retained
witnesses before publication, and reads conservatively if a retained witness changed.
Incremental queries borrow unchanged payloads without rebuilding
a map of large candidate structs. Payloads, lookup metadata, constructors and
active leases remain charged to the existing 4 MiB physical cap. Per-file evidence
times are scoped before capture. Two removable ordered time indexes track oldest
and newest evidence across active positive and negative outcomes; deleting or
replacing a file retires its expiry witnesses in logarithmic time. Validity checks
exclude pending changed/deleted records before mutation, so an expired removed
record does not force rebuilding fresh surviving outcomes. Active rejected
or unselected outcomes still constrain TTL; reuse never renews evidence timestamps.

This does not make search resource-free. Unchanged conditional results can avoid
provider calls and payload delivery; changed sources, expiry and uncertain
coverage require work. Client-held handles/revisions/tokens can avoid an RPC only
while their own freshness conditions remain valid.

Final paired controls against the rebuilt `16fa2b9184` PR binary (identical
baseline SHA-256 to the original frozen artifact):

| Corpus / refresh | Before | After | Source opens before → after |
|---|---:|---:|---:|
| 512 files / unchanged | 7.050 ms | 0.061 ms | 256 → 0 |
| 512 files / inplace | 7.211 ms | 0.147 ms | 257 → 1 |
| 512 files / atomic | 15.258 ms | 2.171 ms | 512 → 1 |
| 512 files / add_delete | 14.882 ms | 3.471 ms | 513 → 1 |

Eleven alternating pairs per case, warm shared host; every mode ends with a
forced-rescan revision comparison. In that binary, file additions/deletions still
visited the inventory: those measurements removed repeated polling and source
opens, before the subsequent incremental membership implementation. At 64 files, unchanged/in-place controls are
essentially unchanged; atomic saves improve and add/delete rediscovery improves.

Nineteen-pair native-inference controls show selected-file refresh + search
at 0.573 vs 0.544 ms for 15 winners and 0.667 vs 0.680 ms for 128 winners.
No broad decision-search latency gain is claimed. Both binaries make one
fixture evaluation per edit and reconstruct identical results. The deterministic
128-winner ownership regression requires new shared-query metadata below 32 KiB,
including its admission-owned path lookup, under the unchanged physical cache cap.

Evidence: [refresh controls](bench/results/README.md),
[selected-file controls](bench/results/README.md),
[differential replay](bench/results/README.md),
[validation/source identities](bench/results/README.md), and
[release archives](bench/results/README.md).

### Incremental membership and payload-free delta reuse

Workspace inventory uses a canonical, path-keyed Cartesian Merkle tree whose
priorities derive from SHA-256 of paths. Cold sorted discovery builds it in linear
time; updates copy only affected tree paths and share the other nodes. Dense entry
slots and intrusive directory/watch lists permit constant-time removal/relocation;
slot positions do not affect revision identities. Opaque revisions use a new v3
hash domain. Old process-local tokens are not portable across processes/builds;
the stdio protocol version remains 1. Forced rebuilds use the same v3 identity.
Tree depth is capped at 128, allocations participate in the workspace budget,
and failed staging preserves published contents. Entry paths and retired tree
nodes are reclaimed rather than accumulating across membership changes.

Certified file additions, deletions and renames stage a complete transaction before
publication, including simultaneous edits elsewhere. Directory topology, ignore or
configuration changes, lost events, incomplete source coverage, retained readers,
and failed admission retain conservative discovery. New size-excluded inputs
receive eligibility witnesses before their exclusion is accepted. Policy reloads
preserve the original root, ancestor-ignore controls, depth and filesystem rules.
Linux uses complete named inotify events to reconcile changed paths without
walking the directory's other files. A bounded event index groups paths by directory
and direct membership lookups avoid scanning the full event set per changed
directory during rename bursts. macOS kqueue has no child names, so it checks
the affected directory's membership; it does not rebuild the repository inventory.
Unwitnessed macOS source contents still require conservative reads. Namespace
invalidations route to affected paths and dependencies, including descendants of
invalidated ancestors; an ancestor replacement cannot hide pathname changes.

Delta delivery checks result tags before allocating line masks, score arrays or
excerpts. If the prior excerpt cache was evicted, unchanged members retain small
metadata-only descriptors. Such entries can acknowledge matching conditional
tokens but cannot satisfy full-payload replay; full delivery reconstructs missing
payloads from immutable sources/evidence. Partial searches never gain complete
result status through this mechanism. All payloads, descriptors and active leases
remain under the existing physical query-cache cap.

`namespace_paths_checked` and `namespace_directories_scanned` report refresh
validation work. `incremental_results_materialized` counts file payloads rebuilt
by incremental search. Idle service blocks on stdin; valid conditional queries
avoid scanning, parsing, inference and payload delivery. Freshness checks and
changed content still require resources. Ordinary one-shot search remains
independent of workspace inventories and event subscriptions.

Possible future work: benchmark broader directory churn and consider extending
incremental discovery to directory topology and ignore-policy changes only if
measurements justify that complexity. Include 1K/10K/100K-file cases, rename bursts,
retained memory, first-result latency and forced-rebuild equivalence. macOS cannot
infer child names from kqueue notifications, and kernel-invisible writers still
require forced capture. The earlier warm shared-host reports above describe their
original binaries, not performance guarantees for later revisions.

Final paired warm add/delete controls against the preceding PR binary:

| Host / corpus | Before | After | Entries visited before → after | Retained workspace MiB before → after |
|---|---:|---:|---:|---:|
| macOS / 2,048 files, 32 directories | 12.750 ms | 0.937 ms | 2,049 → 65 | 3.50 → 2.14 |
| Linux VM / 100,000 files, 64 directories | 1,864.748 ms | 8.867 ms | 100,001 → 1 | 203.06 → 95.93 |
| Linux VM / 100,000 files, one directory | 1,523.255 ms | 2.305 ms | 100,001 → 1 | 203.05 → 94.78 |

macOS uses eleven alternating pairs per case; the Linux VM uses five pairs and a
two-CPU quota. Each mode ends with a forced-rebuild revision comparison. Linux
named-event cases perform no directory membership enumerations, including the
100,000-name directory whose optional bounded certificate cannot hold all names.
Accounted retained workspace bytes are distinct from process RSS: sampled RSS
improves on several workloads but is roughly unchanged at 100K Linux files.
Initial capture still does full discovery and builds the inventory. Its single
bootstrap samples were slower on large Linux corpora (for example 3.47 s versus
2.29 s at 100K/64 directories); these are not paired cold-start qualifications.
The results do not establish a universal startup, throughput, or RSS win.

Final validation covers 227 CLI tests on each of macOS and Linux (two skips each),
86/87 package tests on each (one platform skip), 328 grammar cases, all six Linux
hardening tests/four fuzz targets, 1,000 generated comparisons with zero
mismatches/timeouts, and Windows cross-compilation. Linux package tests run on the
VM's native filesystem so bind-mount limitations cannot silently skip membership
coverage. Put the pinned GNU ripgrep 15.2 / PCRE2 10.45 oracle on `PATH` when
running Linux CLI qualification; two skips are expected here, and a higher skip
count can indicate missing oracle coverage. Run package unit executables from
local VM storage rather than a host bind mount. Native fixtures establish
orchestration, not real model quality or cost.

Evidence: [macOS controls](bench/results/README.md),
[Linux controls](bench/results/README.md),
[wide-directory controls](bench/results/README.md),
[differential replay](bench/results/README.md), and
[source/binary/archive identities](bench/results/README.md).


### Startup allocation and hashing qualification

Inventory nodes and their immutable pathname now share one aligned allocation,
including persistent update copies. Allocation size arithmetic is checked and
charged to the existing workspace budget. Cold Cartesian construction computes
subtree hashes only after installing children, avoiding the earlier redundant
leaf-shaped subtree hash. Inventory v3 identities and retained-byte accounting
are unchanged. Cold and persistent construction agree under exhaustive allocation
failure injection, including empty and longer pathnames.

`bench/capture.py CURRENT BASELINE OUTPUT` alternates fresh processes over one
immutable corpus, discards a warmup pair, includes spawn-to-ready latency, samples
ready-state RSS, and checks revision equality across every run. Data is warm in
the filesystem cache; this is not a cold-storage benchmark. Against the preceding
PR binary (`752e0a8b7f`), seven pairs on macOS and five pairs in the two-CPU ARM64
Linux VM measured:

| Host / corpus (64 directories) | Startup before → after | Ready RSS before → after |
|---|---:|---:|
| macOS / 1,000 files | 57.73 → 57.26 ms | 6.36 → 6.31 MiB |
| macOS / 10,000 files | 478.57 → 440.56 ms | 17.39 → 17.36 MiB |
| Linux VM / 1,000 files | 19.34 → 15.46 ms | 6.20 → 6.18 MiB |
| Linux VM / 10,000 files | 147.60 → 132.09 ms | 15.42 → 15.27 MiB |
| Linux VM / 100,000 files | 1,399.78 → 1,311.90 ms | 107.28 → 106.13 MiB |

The paired startup results supersede the earlier unpaired bootstrap samples as
startup qualification against this immediate baseline. They do not compare
against the pre-membership inventory revision format, establish native Linux
hardware performance, or establish cold-cache performance. Separate refresh
controls retain zero source reads/entries visited for unchanged workspaces and
forced-rebuild revision equality after changes. Submillisecond timing and RSS
results vary across cases; no universal improvement is claimed. Initial capture
still reads every eligible source: validation, changed content and native watch
subscriptions have real CPU, I/O and memory costs even when provider calls are
zero.

Evidence: [paired macOS startup](bench/results/README.md),
[paired Linux startup](bench/results/README.md),
[macOS refresh controls](bench/results/README.md),
[Linux refresh controls](bench/results/README.md),
[differential replay](bench/results/README.md), and
[qualification identities](bench/results/README.md).


### Compact captures, bounded readers and restart preparation reuse

Workspace decision preparation now consults the same content-addressed, validated
parser-artifact cache as ordinary decision search before parsing. Parser identity,
pathname, content, line mode and authorization scope remain part of its identity;
loaded offsets and ranges are validated against the captured bytes. The retained
preparation owns its decoded metadata. `--no-cache` bypasses disk artifacts;
`--cached-only` prohibits provider calls, while still allowing source validation
and parsing when no preparation artifact exists. Decision evidence TTL is unchanged.

Each raw source blob now occupies one checked, aligned allocation containing its
header and exact source bytes. A compact stamp retains only the fields needed for
source certification and alias policy. Entries keep one preparation-owner pointer
instead of duplicating its descriptor. Blobs remain independently reference counted
so deletion releases their storage without waiting for unrelated source segments
or requiring segment compaction. Captures retain immutable owned bytes, including
when original paths disappear. Inventory v3 revisions remain unchanged.

Initial capture can read, validate and hash a bounded batch in parallel. The
coordinator alone arms witnesses, allocates buffers, updates budgets
and publishes entries; read workers touch disjoint preallocated buffers. The later
opt-in metadata stage described below also batches opens and stats. File sizes and
stamps are checked before and after reading, including EOF/growth checks. Failed
batches join every worker and release their descriptors, blobs and reservations
before returning; no partial generation is published. Discovery remains serial.

The queue holds at most 32 descriptors and 8 MiB of source payloads. POSIX descriptor
quotas lower its capacity; pending reads drain before an open is retried. Capture
uses at most eight workers for explicit `--threads`, or four in automatic mode,
subject to the existing automatic job budget. Automatic mode leaves reads of files below 64 KiB and
files exceeding the queue byte bound on the serial path. `--threads=1` disables
capture concurrency. `workspace_ready.capture_workers` reports the largest launched
capture stage, including the coordinator (zero when no helper is launched);
`parallel_sources_read` counts files in batches
which used workers. Incremental refresh remains selective and reports zero capture
workers. The existing separate search/provider worker policies are unchanged.

Final alternating paired measurements against `ea35e73fb8`, using seven pairs on
the shared macOS host and five in a two-CPU ARM64 Linux Docker VM:

| Workload | Before → after | Retained workspace MiB before → after |
|---|---:|---:|
| Linux VM startup, 100K tiny files | 1,328 → 1,268 ms | 96.14 → 53.00 |
| Linux VM startup, 128 × 1 MiB files | 373.3 → 204.7 ms | 288.17 → 128.12 |
| macOS startup, 128 × 1 MiB files | 94.3 → 47.1 ms | 288.17 → 128.12 |
| macOS startup, 10K tiny files | 662.5 → 686.3 ms | 10.41 → 5.89 |
| Linux VM warm-artifact query after restart | 10.19 → 4.77 ms | Not measured |
| macOS warm-artifact query after restart | 5.51 → 1.88 ms | Not measured |

At 100K tiny Linux files (1.34 MiB source), sampled ready RSS drops from 105.70 to
61.31 MiB. Retained accounting includes reserved backing allocations, while RSS
measures touched resident pages; their reductions differ, particularly on the
large-file corpus. Isolating worker concurrency within the new binary gives
376.5 → 197.9 ms for two versus one reader in Linux and 99.5 → 56.3 ms for four
versus one on macOS. Warm-artifact query trials use 32 synthetic Python files,
128 call lines each, a native constant-score fixture, and one output match per
file; capture time is recorded separately. Every recorded query makes zero
provider calls, returns identical match evidence, and changes 32 source parses
into zero. These fixtures do not establish live-provider quality or billing.

All timings use warm filesystem data, including fresh-process startup. Small-file
latency and submillisecond refresh results vary; the macOS 10K case regresses in
this run. No universal throughput, cold-storage, or independent Linux hardware
claim is made. Unchanged refresh controls retain zero source reads/entries visited
and changed cases verify revision equality against forced capture.

Source snapshots, inventory and complete-result memos remain process-local RAM.
The configured disk cache contains derived parser metadata/names and decision
scores/timestamps; this change adds no durable source mirror or workspace catalog.
Ranked output may separately use temporary spill files containing excerpts.
Persistence cannot make restart validation free, and changed source data still
requires reading and evaluation. Clients can reuse an existing result locally
when query, pinned revision and evidence validity permit, eliminating a redundant
request without claiming freshness for kernel-invisible writes.

Validation: 229 CLI cases per macOS/Linux (two skips each); 89/90 package tests per
platform (one platform skip), including allocation-failure capture, worker failure,
serial/parallel revision equivalence and low-descriptor tests. Native package
checks also pass in ReleaseSafe mode. Windows x86_64 cross-compilation, 1,000 seeded
differential comparisons without mismatches/timeouts, license boundaries and three
release archives are qualified. Windows runtime and live-provider testing remain
outside this qualification.

Evidence: [Linux capture](bench/results/README.md),
[Linux I/O](bench/results/README.md),
[isolated workers](bench/results/README.md),
[restart queries](bench/results/README.md),
[macOS capture](bench/results/README.md), and
[all source/binary/report/archive identities](bench/results/README.md).

### Lexical completion certificates and bounded file reuse (qualification at b261a10f84)

Registered literal and regex searches can return `result_token` after a complete
exit-0 or exit-1 result, including a complete miss. Repeating that handle, pinned
workspace revision and token returns `workspace_unchanged` and a fresh summary,
without cloning the matcher or visiting source entries. Each of the eight bounded
registrations owns one scalar certificate. Unknown or superseded tokens take the
ordinary search path; changed revisions require searching the new snapshot.
`--no-cache`, quiet mode, debugging/tracing, cancellation, errors and partial
results do not publish lexical certificates. This is exact reuse of immutable
local data, independent of decision-provider evidence lifetimes. Ranked decision
certificates retain their existing TTL and evidence checks.

Full lexical delivery can reuse per-file outcomes across workspace revisions.
Keys bind the complete effective query identity, displayed path, content revision
and file/render flags. Negative outcomes are reusable too. The shared cache is
process-local, four-way associative, limited to 4,096 entries and 8 MiB of physical
retained memory including active leases. Per-file staging is capped at 256 KiB;
large/dense outputs fall back to scanning without changing the result. Files below
4 KiB match directly: a tiny-file probe showed that hashing, leasing and decoding
cached JSON could cost more than scanning their contents. Count/file-list/quiet
modes and finite output budgets use their existing paths. Whole-query completion
certificates still cover these complete non-quiet searches.

Replay regenerates source envelopes, snapshot IDs, observation times, compact
file IDs and end-record timing/statistics. It preserves match/context data,
submatches, binary offsets and logical counts. `lexical_cache_hits` and
`lexical_files_evaluated` report actual cache and evaluation work; a conditional
acknowledgement reports neither. Cancellation and ordinary output/pending-memory
limits remain active during replay. Optional payload capture uses a separate JSON
writer callback; the general output writer has no cache-capture branch. `trim`, `forget`, registration eviction and
process exit release their respective optional state. Cache eviction is an
ordinary miss and cannot suppress results.

Watch admission uses two levels of free-slot bitsets instead of a linear scan.
Both supported watcher platforms reuse device/inode identities before attempting
registration, including when all slots are occupied. Linux upgrades existing
masks by union when the same inode needs both file and directory notifications.
A full table makes no new registration/removal calls for a distinct identity;
released slots become immediately available. Lost identities are retired without
removing a newer subscription for the same inode. Safe tests cover full-table
rejection, hard links, reference-counted release, slot recycling and Linux mask
upgrades. Watch loss and incomplete coverage still force conservative capture.

Workspace preparation passes its already captured SHA-256 to the source-artifact
identity calculation instead of hashing the entire source again. Ordinary search
computes that digest itself. The v2 identity binds the digest to parser identity,
path, line mode and authorization scope. Older preparation keys become disposable
cache misses; decision-score identities and inventory-v3 revisions are unchanged.
Line-boundary/range validation, UTF-8 validation and binary checks remain in place.
No additional per-file digest or descriptor is retained. Further metadata packing
or combining those validation passes needs evidence of a worthwhile gain; a
scalar loop replacing vectorized validation is not an assumed optimization.

Source snapshots, lexical payloads, certificates and inventory metadata live in
RAM. This change creates no persistent source-content or lexical-result index.
The configured disk cache still contains bounded derived parser metadata and
decision evidence; existing temporary ranked spills may contain excerpts. Idle
services block on stdin. Unchanged conditional queries avoid source-sized work,
but framing, freshness/invalidation, output, memory and changed files still have
cost. These mechanisms do not make search literally resource-free.

Paired controls against the preceding PR binary `328bca376b` use alternating
trials, warm filesystem data and synthetic files. Capture is outside lexical
search timing; full/edit deliveries compare exact match/context data outside
timing. The conditional baseline delivers a full result because it has no lexical
certificate. Preparation controls prewarm both preparation-key versions and
require zero parser/provider calls from both binaries. The Linux environment is
an ARM64 Docker VM capped at two CPUs; macOS is a shared 14-CPU host. Builds and
this task's tests were stopped before the final serial benchmark pass, but other
host workloads remain uncontrolled. Each report records load and binary hashes.

| Linux VM workload | Before → after (median elapsed) |
|---|---:|
| Initial capture, 100K tiny files | 1,320 → 832 ms |
| Full lexical delivery, 64 files / roughly 64 MiB | 4.324 → 0.889 ms |
| One-file lexical edit, same fixture | 4.402 → 0.899 ms |
| Conditional lexical acknowledgement | 4.497 → 0.083 ms |
| Warm preparation after restart, 32 files with 64 KiB comments | 21.579 → 15.859 ms |
| Full lexical delivery, 1K tiny files / dense JSON output | 11.868 → 14.011 ms |

The tiny-file full-output regression is retained in the qualification. macOS tiny
full/edit controls also regress about 5–7%; these paths deliberately scan directly
and still serialize all output. Clients retaining a result should send its token:
conditional acknowledgements avoid both that scan and payload delivery. There is
no claim of universal throughput or memory improvement. At 100K Linux files,
accounted retained source bytes remain 55,579,336 on both binaries; sampled ready
RSS increases 62,676→63,340 KiB as bounded witness/cache structures are added.
Further packing must justify its complexity against measured working-set costs.

The final binaries pass 234 CLI tests each on macOS and Linux (two expected skips
each), 91/92 package tests on each platform (one expected skip), macOS ReleaseSafe
package tests, native grep conformance/hardening targets and 1,000 generated
ripgrep/PCRE2 comparisons without mismatches or timeouts. Windows x86_64
cross-compilation, three release archives with exact binary/documentation/schema/
notice/completion/man-page identities, license boundaries and release checks pass.
Full PR CI still requires repository admission; native Linux hardware, Windows
runtime and live-model quality/billing remain unqualified.

Evidence: [Linux lexical reuse](bench/results/README.md),
[tiny-file controls](bench/results/README.md),
[Linux capture](bench/results/README.md),
[preparation controls](bench/results/README.md),
[macOS lexical reuse](bench/results/README.md), and
[all source/binary/report/archive identities](bench/results/README.md).
Reproduce lexical controls with `python3 bench/lexical_reuse.py CURRENT BASELINE
OUTPUT`; use `bench/restart.py` with `--prewarm-both --padding-bytes=65536` and a
local fixture library to isolate preparation work across cache-key versions.

The branch also merges `origin/main` through `bdd7bc0113`. Its embedded SQL and
source-catalog changes do not enter the grep-only dependency graph. Native macOS,
Linux ARM64 and Windows x86_64 product binaries are checked byte-for-byte against
the qualified artifacts after that merge; the native conformance target and
license boundary are rerun on the merged tree.

### Typed lexical replay and changed-path delivery

The follow-up to `b261a10f84` fixes finite-budget acknowledgements: the entire
conditional response is staged before publication. If it cannot fit, search
falls back to ordinary delivery, preserving complete misses that fit the budget.
Repeated full deliveries retain the same registered-query token while the pinned
revision is unchanged. `lexical_files_evaluated` includes tiny-file scans;
`lexical_entries_visited` separately counts inventory paths visited during the
request, including deleted paths in a delta. Logical file/match totals describe
the complete result, while these counters describe work actually performed.

Per-file payloads now contain typed rows, owned line bytes and match spans,
rather than serialized JSON. Spans are collected during their original encoding;
replay needs neither JSON parsing nor another regex pass. Envelopes, source IDs,
observation times and end statistics are still regenerated. The shared 8 MiB
cache and per-worker 256 KiB staging bound remain; failed admission only causes
ordinary scanning. Sources smaller than 4 KiB still bypass payload caching.

Registered lexical clients can opt into changed-path delivery by bootstrapping
with `delta: true` and an unknown valid `if_result_token` (64 hexadecimal digits,
for example all zeroes). That first request returns a full result and builds an
optional, 16 MiB maximum per-registration summary index. Subsequent requests with
`delta: true` and the last completed token can receive:

```json
{"type":"workspace_lexical_delta","base_token":"…","base_revision":"…","revision":"…","observed_at":0,"removed":[{"text":"src/old.txt"}],"ordering":"path"}
```

Removed paths use the same `{ "text": "…" }` or base64 `{ "bytes": "…" }`
representation as `begin.data.path`, preserving non-UTF8 Linux names.
Apply this as a transaction: remove the listed prior result paths, replace each
path introduced by a new `begin` and its following match/context rows, and
retain other paths. Apply the new revision/observation time to the retained
view's snapshot provenance; per-file content revisions remain unchanged. Commit
only after a complete summary, a result token and `workspace_end` with exit code
0 or 1. An error, cancellation or incomplete response must leave the prior view
intact. Sort the combined view by original path bytes (UTF-8 encode `text`
or base64 decode `bytes`); patch event order is not a complete result ordering. `workspace_unchanged` requires no changes. A response without
either delta/unchanged event replaces the entire view.

The index owns paths and compact logical totals, not source generations or
cached result payloads. A single edit visits only the generation's changed paths,
even if payload caching was skipped or evicted. Miss-to-hit, hit-to-miss,
additions and deletions update aggregate counts. A separate visibility bit tracks
context-only `--passthru` files, so deletion removes displayed context even when
the file has zero logical matches. Index admission failure,
unknown/stale tokens and skipped generations fall back to complete delivery.
Count and file-list modes and finite output/input budgets retain their normal
delivery paths. Finite `--max-source-bytes` disables per-file/delta reuse so cached
files cannot evade the global allowance or change exhaustion into success. Ordinary lexical queries do not populate this optional index. `forget`,
registration eviction, `trim` and process exit release the corresponding state.
This metadata remains in RAM; no durable source or lexical index is introduced.

On 64-bit targets, workspace entry metadata is reduced from 168 to 120 bytes by
storing SHA-256 in binary and reading content through its existing blob owner.
External hexadecimal revisions and canonical inventory-v3 hashes are unchanged.
`workspace_ready.entry_bytes` exposes this structure size; retained allocation
accounting and RSS remain separate measurements.

A symbolized macOS profile of repeated tiny-file JSON delivery showed output
writes/serialization dominating, rather than regex execution. Parallel lexical
pipe delivery now flushes the first completed file. Startup batches flush after
eight files or 16 KiB at a file boundary so decoding can overlap encoding. After
128 KiB of output, bulk batches flush after 64 files or at the general 64 KiB
threshold. Large records retain that general flush threshold. Explicit line buffering,
interactive terminals and decision-provider output retain their flush behavior.
No additional regex JIT path is justified by that profile. First-event and total
latency must both be considered when measuring this change.

`bench/lexical_reuse.py` now binds compact match IDs to path/source identities
and compares provenance, match/context data, binary offsets and logical end
counts. Swapping a match's file ID is a regression even when its text is equal.
`bench/lexical_delta.py` applies patches to a materialized client view and compares
it with full baseline results, while asserting only one path is visited for a
one-file edit. Both record first-event latency. Refresh is outside search timing;
these controls use warm synthetic data, not cold-storage or independent native
Linux hardware. Changes and output still cost resources; avoiding an unnecessary
request in the consuming client remains the cheapest unchanged-result path.

This branch merges `origin/main` through `dfc6007c5d`. The final main update
changes workflow action pins and an unrelated guide. All three product binaries
are rebuilt and checked byte-for-byte against the qualified `18ec53341f`
artifacts; native unit/hardening checks also pass on the merged tree.

Final controls against `b261a10f84` use alternating serial trials, warm synthetic
files and the same client for each binary. This task's builds/tests were stopped
before measurement; other shared-host activity remains uncontrolled. The Linux
ARM64 Docker VM is capped at two CPUs. Refresh/capture is outside query timing.
Delta results are compared with full results after reconstructing the entire
client view, including per-file content identity and current snapshot provenance.

| Workload | Linux VM before → after | macOS before → after |
|---|---:|---:|
| One-file delta delivery, 10K files (baseline delivers full view) | 202.024 → 0.158 ms | 98.206 → 0.124 ms |
| Full delivery, 64 files / roughly 64 MiB | 0.780 → 0.743 ms | 0.616 → 0.580 ms |
| Full delivery after one edit, same fixture | 1.039 → 0.806 ms | 0.687 → 0.635 ms |
| Full delivery, 1K tiny files / dense JSON | 9.322 → 9.270 ms | 8.133 → 8.511 ms |
| Initial capture, 100K tiny files | 761 → 725 ms | 5,078 → 4,973 ms |
| Ready RSS, 100K tiny files | 63,592 → 58,388 KiB | 62,208 → 58,032 KiB |

The 10K-file delta transfers 2,857 bytes instead of 11,849,220 in Linux, and
2,909 instead of 12,369,196 in macOS. Each edit visits/evaluates one path; summary
file/match counts still describe the whole result. Per-request producer time for
64-file full delivery drops 0.634→0.451 ms in Linux and 0.371→0.175 ms in macOS.
Producer time includes output backpressure and is not a CPU-time measurement.
Accounted retained capture allocations at 100K files drop 55,579,336→50,457,496
bytes on both platforms. RSS reflects touched resident pages, not just ownership
accounting. Optional delta metadata adds its own separately bounded RAM.

Small controls are mixed: macOS tiny-file full delivery regresses about 4.6%,
while tiny-file edit delivery improves about 3.4%; Linux conditional tiny-query
latency also regresses in this run. Their raw trials and first-event latencies
remain in the reports. The larger delta gain comes from avoiding unchanged work
and output, not from making a cold scan or identical output arbitrarily cheap.
No universal throughput, cold-cache, native Linux hardware, or zero-cost claim
is made.

Final binaries pass **239 CLI cases each on macOS/Linux** (two expected skips
each), **92/93 package cases each** (one expected skip), macOS ReleaseSafe package
checks, native regex/grammar/dictionary/hardening targets and benchmark/differential
tool tests. Another **1,000 generated ripgrep/PCRE2 comparisons** find no mismatch
or timeout. Regressions cover finite output/input budgets, stable tokens,
compact-file binding, skipped revision chains, byte-path removals, cached
context-only passthrough files and all-miss deltas. The empty/trailing-root watch
fixture verifies every result against forced capture and requires the fast path
within bounded fresh additions, allowing the documented conservative event-race
fallback. Windows x86_64 cross-compilation and release contents are qualified;
Windows runtime, independent Linux hardware and live-provider quality remain
outside this fixture qualification. Full PR CI remains subject to admission.

Evidence: [Linux deltas](bench/results/README.md),
[Linux full delivery](bench/results/README.md),
[dense controls](bench/results/README.md),
[Linux capture](bench/results/README.md),
[preparation controls](bench/results/README.md),
[refresh controls](bench/results/README.md),
[macOS deltas](bench/results/README.md),
[macOS capture](bench/results/README.md), and
[source/binary/report/archive identities](bench/results/README.md).

### Admission backoff, minimal acknowledgements and retained clients

The next follow-up uses `7a19e40c9a` as its qualification baseline. Failed optional
lexical-index admission no longer repeats the same allocation work on every
bootstrap request. Cold admission reserves the hash table once and checks owned
path bytes against the remaining 16 MiB allowance before scanning. A separate
negative admission record survives payload clearing and content-only edits.
Its constant-time cost estimate uses file count and total path bytes, maintained
transactionally by capture and changed-path membership updates. Admission retries
after an estimated footprint reduction of at least 25%, a larger allowance, or a
new registration. Backing-allocator failures instead use exponential retry
backoff of 1–64 requests, allowing recovery without an inventory shrink. Failure
remains an exact full-delivery fallback; `forget` and
`trim` release the record. Summary counters distinguish
`lexical_index_admissions`, `lexical_index_admission_failures` and
`lexical_index_admission_skips`. A real 24K-file long-path fixture exceeds the
budget, verifies backoff across an edit, shrinks the inventory, and verifies
successful admission and subsequent changed-path delivery.

Registered conditional searches may opt into a smaller response:

```json
{"version":1,"id":7,"op":"search","revision":"…","query_handle":"…","if_result_token":"…","acknowledgement":"minimal"}
```

A matching complete lexical certificate can return exactly one terminal frame:

```json
{"type":"workspace_not_modified","id":7,"revision":"…","result_token":"…","observed_at":0,"complete":true,"exit_code":0}
```

This response has no `workspace_begin`, summary or `workspace_end`. Exit code 1
is also a successful completed transaction for a query with no matches. The
query handle and snapshot bind the result; an unknown token, changed revision,
unsupported query or insufficient output allowance uses ordinary delivery.
Invalidated workspaces still require refresh. Cancellation is checked before
publication. The opt-in lexical fast path constructs no matcher, provider client,
heap-backed summary or result payload. Default acknowledgements keep the existing
protocol. Small workspace control frames use stack storage and one write.
`workspace_ready.observed_at` exposes the current capture observation time, and
`query_registered.supports_lexical_delta` advertises the supported row contract;
index admission remains optional and bounded.

[retained.py](bench/retained.py) is a stdlib-only reference **consumer and benchmark
tool**, not a Python dependency of the Zig binary. It retains one live compact
lexical view, skips a request when that handle and the last validated pinned
revision are already materialized, and can explicitly revalidate using the
minimal acknowledgement. Refresh is explicit: a skipped request makes no new
claim about live filesystem freshness. Invalidation and failed refresh prohibit
reuse; registration changes, forget, trim and close discard the retained view.

The client validates complete transactions and file-ID bindings before applying
changes. Delta publication changes only affected paths without copying the
unchanged payload map or retaining prior generation chains. It stores serialized
file payloads, overlays the current snapshot/observation metadata when explicitly
materializing rows, and supports UTF-8 and base64 byte paths. The default 8 MiB
allowance bounds retained serialized payload/path bytes, not Python object RSS.
A separate configurable response-wire allowance (by default twice that allowance
plus 64 KiB, at least 64 KiB) is checked before JSON decoding and while reading
partial frames. Exhaustion stops the owned producer and invalidates the client
without publishing a partial view. A local allocation failure invalidates the view certificate; a full refetch is
required. Provider/ranked searches and finite-budget/non-row registrations are
excluded from this reference client's request-omission API.

```python
# Run with zig/pkg/antfly-grep/bench on PYTHONPATH.
from pathlib import Path
from retained import RetainedClient

client = RetainedClient(Path("/path/to/antfly-grep"), Path.cwd(), "src", [], {})
try:
    _, rows = client.exchange("register", args=["TODO"])
    handle = rows[0]["query_handle"]
    view, work = client.search_view(handle)       # full bootstrap
    view, work = client.search_view(handle)       # zero requests/bytes
    view, work = client.search_view(handle, revalidate=True)  # minimal ack
    client.exchange("refresh")                   # explicit freshness work
    view, work = client.search_view(handle)        # changed paths or full fallback
finally:
    client.close()
```

Full output still requires encoding and transfer. Worker rendering now reuses at
most 64 KiB of scratch between rows, releases exceptional larger records, and
borrows pinned workspace paths for queued tasks; live discovery retains explicit
string ownership. Per-file elapsed labels use stack storage, and compact end
records avoid unused path encoding. After 128 KiB of output, typed JSON envelopes pre-quote static ASCII
schema keys; small non-delta records keep the standard encoder to bound fixed work. Both
paths produce identical envelopes and submatches; dynamic keys, value escaping, numeric formatting and custom
serializers remain with `std.json`, with byte-equivalence tests. Cold captures
read directly into their
immutable content owner and use the existing size/ctime/identity barrier to reject
short reads or growth, avoiding a separate EOF read and buffering copy. Full
source-watch tables still reuse known inode identities, but reject new identities
without polling the kernel for each remaining file. Capture/refresh retains the
final event/publication barriers and conservative fallback on lost coverage.

These changes avoid repeated work; they do not make freshness, cold reads,
changed results, full output or retained RAM free. No additional regex JIT path is
justified by the output/capture profiles. Independent native Linux hardware and
cold-storage qualification remain separate from the warm controls below.

Qualification against `7a19e40c9a` uses nine alternating trials of 256 dense
full deliveries per trial and 8,192 acknowledgements per variant, with exact
result comparisons outside timing. CPU counters cover the server process and
its workers; their 10 ms precision is amortized over each batch. They are separate
from producer wall time, which includes output backpressure. Capture controls
use seven alternating fresh-process trials on warm filesystem data. Delta
controls use 31 edits, each checked against the complete materialized view.

| Control | macOS before → after | Linux VM before → after |
|---|---:|---:|
| Dense 1K-file JSON, server CPU per request | 5.273 → 4.766 ms | 7.813 → 6.758 ms |
| Same full delivery, client elapsed | 10.123 → 10.131 ms | 10.588 → 10.500 ms |
| Full acknowledgement → opt-in minimal, same final binary | 28.24 → 13.93 µs | 51.49 → 38.92 µs |
| Acknowledgement bytes, same final binary | about 1,510 → 256 | about 1,510 → 256 |
| Capture 100K tiny files | 3.979 → 3.240 s | 715.5 → 675.6 ms |
| One-file delta in 10K-file inventory, client elapsed | 124.0 → 128.0 µs | 140.2 → 218.4 µs |

The retained reference client sends **zero requests and zero response bytes** for
an already materialized pinned revision. Its local call still uses CPU and the
view occupies RAM. The default full-output controls are essentially flat in
client elapsed time while using less server CPU. Small-source and delta controls
remain mixed; the Linux delta regression is retained explicitly. The optional
`bench/lexical_delta.py --cpu-schedstat` samples Linux main-thread CPU/runqueue
counters outside request timing. Its separate probe records about 97.8 versus
66.8 µs of main-thread CPU per delta, so the remaining fixed overhead should not
be attributed solely to scheduler noise. The execution path still visits and
evaluates exactly one changed file and makes no provider requests.

An earlier macOS capture control had large timing spikes in both binaries:
separate medians were 8.206→11.444 s, although six of seven paired trials favored
the implementation. A repeat recorded 3.911→2.450 s. The preceding qualified controls
remain under `overhead-pre-adaptive-*`, including regressions; the qualification
record identifies their earlier binary/source revision separately. Those raw trials and their limitations remain available.
Ready retained capture allocations are effectively unchanged at roughly 50.46 MB
for 100K files; entries remain 120 bytes. No zero-RAM or universal speed claim is
made, and source freshness/capture is outside query timing.

Final qualification passes **242 CLI cases each on macOS/Linux** (two expected
skips each), **96/97 package cases each** (one expected skip), ReleaseSafe native
conformance/hardening, another **1,000 generated oracle comparisons** without
mismatches/timeouts, Windows x86_64 cross-compilation, release-license/toolchain/
format/compatibility checks and verified release contents. Providers remain local
fixtures. Independent native Linux hardware, cold storage, Windows runtime and
live-provider quality/billing remain unqualified; full PR CI requires admission.

Evidence: [macOS CPU/output](bench/results/README.md),
[Linux CPU/output](bench/results/README.md),
[macOS capture](bench/results/README.md),
[Linux capture](bench/results/README.md),
[macOS deltas](bench/results/README.md),
[Linux deltas](bench/results/README.md),
[Linux delta CPU probe](bench/results/README.md),
[large-file reuse](bench/results/README.md),
[oracle results](bench/results/README.md), and
[source/binary/report/archive identities](bench/results/README.md).

### Incremental admission recovery and streaming view consumption

Lexical index admission distinguishes a final footprint that cannot fit from
old/new hash-table overlap during growth. After removing changed entries, the
coordinator reserves the final map and path footprint before producing a patch.
A quota failure on an existing index releases that index and attempts one cold
reservation. Success delivers a complete replacement and restores changed-path
searches on the next revision; only a failed cold reservation records negative
admission. Backing-allocator failures retain bounded retry backoff. Path-byte
accounting excludes keys already charged to the index. The regression crosses a
real 16 MiB growth boundary and checks the following edit visits one file.

The serial workspace parser reuses at most 16 KiB of request scratch, released
on EOF, without constructing a second parser arena. Registrations and source
generations own everything they retain beyond the request. Small transaction
records use stack storage and one buffered write; large removal lists retain
the allocating, record-atomic budget fallback. Local URI construction uses one
exact allocation, with direct encoding for normalized POSIX paths and the usual
path resolver for dot components, repeated separators and Windows. URI encoding
is tested against the previous resolver/formatter across escaped and byte paths.
Generated compact file IDs avoid rescanning their known hexadecimal alphabet;
other strings retain ordinary JSON escaping.

Cold capture stores stamp validity in existing stamp padding, reducing its
source header from 160 to 144 bytes on the qualified 64-bit targets. It retains
full inode, size, kind, link-count and nanosecond timestamps. Fresh-read witness
certification does not perform an additional event drain per file: the
coordinator's existing final drain/publication barrier observes pending writes
and invalidates affected sequences. No namespace or identity barrier is removed.
A regression queues a write before deferred certification and verifies that the
publication drain still rejects the earlier token.

The reference retained consumer stages validated original JSON frames per file
and releases each decoded row after validating its binding. It retains control
records until the terminal frame, then publishes only a complete transaction.
It avoids retaining a decoded result tree and serializing that tree again;
explicit `materialize()` still decodes the complete view. Duplicate identities,
unbound rows, missing file ends and payloads hidden in minimal acknowledgements
are rejected. A consumer failure retires the owned connection so unread frames
cannot become another response. Response and retained-payload allowances remain
independent; Python allocation overhead is additional. `bench/retained_delivery.py`
compares this path with the unchanged decoded-tree application path against the
same binary, retaining all paired timings and a separate traced-allocation probe.

Qualification against the previous PR head (`5e0b230939`) retains the exact
binary hashes and paired raw trials. Full-output controls use nine alternating
batches of 256 requests; the consumer control uses nine alternating batches of
32 complete view transactions against one binary. Capture uses seven fresh-process
pairs. Data and processes are warm; capture/refresh is excluded from query timing.
The Linux controls use the same two-CPU ARM64 VM, not independent Linux hardware.

| Control | macOS before → after | Linux VM before → after |
|---|---:|---:|
| Dense 1K-file JSON, server CPU/request | 5.742 → 5.078 ms | 6.914 → 6.250 ms |
| Same full delivery, client elapsed | 11.469 → 11.619 ms | 11.608 → 11.383 ms |
| Decoded-tree → streaming consumer, elapsed | 16.994 → 9.420 ms | 20.218 → 12.770 ms |
| Same consumer, client CPU | 16.869 → 9.375 ms | 17.111 → 9.614 ms |
| Same consumer, traced peak allocation | 8.33 → 1.54 MB | 8.24 → 1.50 MB |
| Capture 100K files, repeat without concurrent validation | 2.482 → 2.334 s | 686.7 → 673.5 ms |
| Ready retained source allocation, 100K files | 50.46 → 48.86 MB | 50.46 → 48.86 MB |
| One-file delta, paired client elapsed | 121.2 → 126.8 µs | 281.1 → 223.3 µs |

The first macOS capture control overlapped other validation and recorded
11.620 → 11.905 s; it remains alongside the repeat. Small 1K-file capture controls
are mixed across hosts. Source-header memory savings are deterministic, but these
shared-host capture times do not establish a general storage-speed improvement.
The traced consumer probe is Python allocation, not process RSS or native source
memory. Pinned retained reads still send zero requests/bytes.

Delta timing now collects garbage before each pair and completes both requests
before copying either full oracle view. Query order is recorded. Earlier controls
performed that O(N) verification between requests and showed strong order effects;
they remain available rather than being relabeled as the improved harness. The
corrected Linux probe records 126.3 → 90.3 µs of main-thread CPU, with zero median
runqueue delay. A separate comparison against the older `7a19e40c9a` binary remains
slower: 158.4 → 199.7 µs elapsed and 82.8 → 109.8 µs CPU. Tiny-delta overhead is
therefore not eliminated, and no universal delta speedup is claimed. A bounded
transport-coalescing experiment also regressed complete-request latency and was
reverted; its negative controls identify the experimental binary separately.

The `3c25321f51` source snapshot passed the 243-case CLI suites on macOS/Linux (two expected
skips each), 100/101 package tests each (one expected skip), ReleaseSafe native
conformance/hardening, 1,000 generated oracle comparisons without failures or
timeouts, Windows cross-compilation, and release/license/toolchain/compatibility
checks. Decision providers remain local fixtures. The rejected transport build
has separate 244-case diagnostic logs and is not the distributed product.

Evidence: [qualification and source/binary/archive identities](bench/results/README.md),
[macOS CPU/output](bench/results/README.md),
[Linux CPU/output](bench/results/README.md),
[macOS capture repeat](bench/results/README.md),
[Linux capture](bench/results/README.md),
[macOS consumer](bench/results/README.md),
[Linux consumer](bench/results/README.md),
[paired macOS deltas](bench/results/README.md),
[paired Linux deltas](bench/results/README.md),
[older-reference Linux deltas](bench/results/README.md),
[oracle results](bench/results/README.md), and
[pre-optimization diagnostic stacks and driver](bench/results/README.md).

### Bounded capture metadata and dense consumer staging

`--workspace-parallel-open` opts cold capture into batching open/stat work
separately from read/hash work. The default keeps serial opens. Each open
batch owns at most 16 source descriptors and copies transient discovery names
into quota-accounted storage. The two stages share a persistent worker pool and
a total allowance of at most 32 source descriptors, reduced by the process's
open-file limit. A one-descriptor allowance keeps serial opens. Allocation,
source subscriptions, quota reservations and publication remain on the
coordinator. Read workers still verify full stamps after reading; capture still
performs its final namespace/event barriers. Failed parallel opens retire the
batch and retry serially after draining pending reads. `--threads=1` retains
serial capture. Metadata concurrency trades CPU for latency and is intended for
measured latency-bound storage. Refresh continues to use the existing changed-path path.

`workspace_ready.parallel_sources_opened` counts paths in concurrent
open/stat batches; `parallel_sources_read` continues to count concurrent reads.
`capture_workers` reports the largest actual stage width, which may be smaller
than `--threads` under descriptor pressure. Pending oversized sources are resolved
before applying the eligible-file limit. Regressions cover that boundary, failed
opens, allocation failures and descriptor headroom.

The reference consumer now writes each file's original frames into one growable
byte buffer and publishes immutable bytes at its end frame. It avoids retaining
one allocation per match followed by a join and whole-file concatenations.
Identity checks and transactional publication are unchanged. The consumer benchmark
retains the old list collector as an explicit `listed` baseline, rotates three
consumer modes, and accepts `--matches-per-file` to qualify dense individual files.
The traced probe measures Python allocations separately from request timings and
is not an RSS measurement.

Workspace deltas also use the existing byte-verified static JSON field encoder
for small records, and summary elapsed formatting uses stack storage. Dynamic
keys, values and custom serializers keep their existing escaping and formatting.
Output flushing and cancellation behavior are unchanged.

Per-request profiling is a compile-time diagnostic option:

```sh
zig build --build-file zig/build.zig -Dgrep-only=true -Doptimize=ReleaseFast \
  -Dgrep-workspace-profile=true grep
python3 zig/pkg/antfly-grep/bench/workspace_phases.py \
  zig/zig-out/bin/antfly-grep phases.json --files=1000 --iterations=101
```

It records parse/validation, conditional checks, initialization, admission,
cancellation setup, search phases, completion and teardown. Each request stages a
fixed number of samples and writes one bounded diagnostic record to stderr after
timing ends. Main-thread CPU excludes workers; wall time includes backpressure.
Clock calls perturb short phases, so uninstrumented paired trials determine
performance gains. Normal builds compile out both timers and profiling storage.
No new JIT path is justified by these output/capture measurements.


Qualification of `5b7a05d424` compares uninstrumented binaries against the prior
PR head `3c25321f51`. Delta controls use 51 alternating pairs, exclude refresh,
and finish both requests before updating either complete oracle view. Consumer
controls rotate decoded, frozen-list and buffered modes across nine trials,
using one exact binary. The dense fixture contains one file with 20,000 matching
lines and a 6.02 MB retained payload. Allocation tracing is a separate untimed
probe. Capture uses seven alternating pairs on warm filesystem data. Linux is
an ARM64 VM constrained to two CPUs; macOS and Linux share the host with other
work. These results do not establish cold-storage or hardware-independent wins.

| Control | macOS before → after | Linux VM before → after |
|---|---:|---:|
| Dense file, list → buffered traced peak | 14.51 → 6.77 MB | 14.49 → 6.77 MB |
| Same delivery, elapsed | 53.67 → 54.48 ms | 53.60 → 53.57 ms |
| One-file delta, client elapsed | 129.96 → 129.17 µs | 452.04 → 296.67 µs |
| Same delta, main-thread CPU | Not sampled | 156.38 → 146.58 µs |
| Default 10K-file capture, process CPU | 0.22 → 0.20 s | 0.06 → 0.06 s |
| Opt-in parallel 10K capture, elapsed | 260.42 → 236.46 ms | 69.84 → 77.45 ms |
| Same opt-in capture, process CPU | 0.21 → 0.44 s | 0.06 → 0.07 s |

Dense staging reduces traced peak allocation by about 53%; it does not establish
a latency improvement over the previous list collector. Many-small-file consumer
controls remain approximately flat. Delta CPU falls about 6% in the Linux control;
the larger elapsed-time difference is sensitive to shared-host scheduling, and
macOS latency is flat. Default Linux 100K capture is similarly flat (709.9 →
718.3 ms, 0.70 s CPU for both). Capture CPU counters have 10 ms precision.

An earlier unconditional-parallel-open prototype regressed the loaded macOS
100K-file control (13.43 → 14.53 s, 4.14 → 9.66 s CPU). Its negative report and
binary identity remain available, separate from the distributed binary. Final
opt-in Linux 100K capture also costs more CPU (0.69 → 0.82 s) without improving
elapsed time. Consequently parallel opens remain explicit opt-in, intended for
storage where a measured latency benefit justifies the CPU cost. Automatic
metadata concurrency and independent storage/hardware qualification remain future
work. The default preserves serial opens, and retained source accounting differs
by only one eight-byte generation counter on these 64-bit targets.

The final sources pass both 243-case CLI suites (two expected skips each),
102/103 package tests on each host (one expected skip), ReleaseSafe conformance
and hardening, 1,000 generated oracle comparisons without failures/timeouts,
Windows cross-compilation, six consumer regressions and release/license/toolchain
checks. Packaged completions and the man page include the new flag. Profiling
builds are separate from all distributed/tested performance binaries. Pinned
retained reads still omit requests; freshness checks, changed output and retained
RAM retain their inherent costs. Provider checks remain local fixtures.

Evidence: [source/binary/report/archive qualification](bench/results/README.md),
[dense macOS consumer](bench/results/README.md),
[dense Linux consumer](bench/results/README.md),
[macOS deltas](bench/results/README.md),
[Linux deltas and CPU](bench/results/README.md),
[default macOS capture](bench/results/README.md),
[default Linux capture](bench/results/README.md),
[opt-in macOS metadata capture](bench/results/README.md),
[opt-in Linux metadata capture](bench/results/README.md),
[rejected automatic prototype](bench/results/README.md),
[pre-optimization macOS phases](bench/results/README.md),
[final diagnostic Linux phases](bench/results/README.md), and
[generated oracle results](bench/results/README.md).

### Bounded workspace transport and larger strict witness coverage

Workspace registrations can explicitly negotiate result schema v3:

```json
{"version":1,"id":1,"op":"register","args":["needle"],"compact":true,"batches":true}
```

The reply advertises `result_schema_version: 3`. Ordinary CLI JSON and default
registrations retain their v1/v2 formats. V3 retains ordinary begin/end/source
identity envelopes and can replace consecutive match/context events with a
`batch` frame carrying one `file_id` and an `events` array. Bodies contain at
most 64 events and 32 KiB of encoded event bytes; oversized events stream as
ordinary v3 rows. The first event is unbatched, and foreground output flushes
one early row per request unless block buffering was requested. Batches never
cross file boundaries or control events. This mode rejects decision queries,
finite output limits and line buffering at registration. Each active encoder
lazily allocates at most one 32 KiB batch body. Cancellation, failed output and
incomplete terminal framing cannot certify a complete result.

The reference retained consumer validates envelopes and owns the original bytes
in its per-file serialized payload. Explicit materialization expands batches;
ordinary pinned-view reuse still omits requests. Frame decoding uses one cached
stdlib JSON decoder with explicit UTF-8, matching the JSONL protocol. Invalid
UTF-8, BOM/UTF-16 input, malformed batches and incomplete file streams fail
before a view is published. The request and batch schemas are shipped in release
archives, alongside existing schemas.

Opt-in `--workspace-watch` now uses a source table that grows geometrically on
demand, up to 65,536 slots on macOS and at most one quarter of the process
descriptor quota. Linux keeps its 4,096-slot ceiling because synchronous
directory queues already extend regular-file coverage beyond that table. Namespace/configuration journals keep their independent 4,096-slot limit.
A free-list gives constant-time slot admission/recycling; dirty-event clearing
visits recorded slots instead of clearing a maximum-sized array. Stable numeric
tokens survive backing-table growth. Smaller workspaces allocate smaller tables,
while larger macOS workspaces can retain strict kqueue inode witnesses instead
of rereading sources above the former 4,096-source ceiling. More admitted
witnesses consume descriptors, kernel resources and table/map memory; source
snapshot memory is accounted separately. Allocation failure, low descriptor
limits, overflow, lost coverage, unsupported platforms and publication races
retain conservative fallback. This does not change the event visibility contract
or make kernel-invisible writes observable. Flat-directory membership changes
still require reading that directory's names; topology and ignore-policy changes
can require broader discovery.

Indexed retrieval projects every field used by the v1 document contract for
JSON, statistics and decision search, including metadata and source regions.
Unrelated sibling blobs are excluded without changing full-scan counters,
validation, permissions or provenance. Conservative trigram filtering remains
disabled for JSON/statistics workflows: skipping documents would otherwise
change their logical totals and diagnostics. Extending pruning to those modes
requires snapshot-bound, revision-certified input/validation summaries and an
explicit server contract for accounting skipped documents. Narrowing projection
is available now without that change in semantics.

Indexed prefetch shutdown publishes terminal cancellation for an undequeued
request and wakes its consumer. A consumer awaiting a page observes atomic
signal cancellation through a bounded 10 ms condition timeout. Successful
handoffs remain signaled immediately; idle producers do not periodically wake.

The final transport qualification uses product source `46f721cb9a`, the preceding
PR binary (`ecde7aa255`), and measurement driver `cf673e4e43`. Nine rotated warm
trials verify complete logical summaries, exact match/context data, provenance
and file statistics outside timing. The same optimized consumer serves every
mode. A separate conservative oracle process forces a fresh capture after each
mixed refresh trial; it never resets either timed client's generation or caches.
The fixtures contain one matching line, 20,000 matching lines, or 6,000 small
files in one flat directory with rotating edits/additions/deletions.

| Dense delivery control | Previous binary v2 | Current binary v2 | Current opt-in v3 |
|---|---:|---:|---:|
| macOS elapsed | 97.33 ms | 108.50 ms | 71.87 ms |
| macOS consumer CPU | 72.24 ms | 73.92 ms | 46.33 ms |
| Linux VM elapsed | 149.42 ms | 158.91 ms | 129.71 ms |
| Linux VM consumer CPU | 148.22 ms | 155.06 ms | 117.71 ms |

V3 reduces dense response bytes by about 33% (6.08 → 4.07 MB). Against the
same-binary v2 control it reduces consumer CPU about 37% on macOS and 24% in the
Linux VM. First dense rows arrive earlier in these controls. One-line timings
vary: macOS baseline/v2/v3 measure 1.119/0.837/1.278 ms, and Linux measures
0.254/0.282/0.215 ms. An early foreground flush can cost tiny requests; v3 is
explicit opt-in. These are dense transport improvements, not universal startup
or tiny-request speedups.

Watched macOS refresh plus full delivery falls from 211.79 to 134.95 ms in the
mixed 6,000-file control. Content-only edits measure 150.08→59.38 ms; flat-directory
additions/deletions still scan names (180.76→157.35 and 294.51→170.15 ms). Current
refreshes read only 14–27 changed source bytes versus >228 KiB of unwitnessed
reads in the baseline. A single startup sample costs 747 versus 660 ms when
admitting additional witnesses; its resident-set sample is 8,224 versus 10,128
KiB. These single setup/RSS observations are neither cold-cache nor peak-memory
measurements. More coverage trades setup/kernel resources for cheaper repeated
freshness checks.

Linux already has selective reads through synchronous directory queues and
retains its smaller witness ceiling. Mixed refresh plus delivery measures
136.66→131.11 ms, with substantial per-trial variation; no general Linux freshness
speedup is established. A prior expanded-witness experiment is retained as a
resource-policy negative control, labeled with its older asymmetric oracle
methodology and used only diagnostically. It is not a distributed binary.

The host was shared with other work, and load increased between runs. Final
reports preserve every trial, the prior-binary and same-binary controls,
versions, hashes, work counters and startup/RSS samples. No independent Linux
hardware, cold-storage or hardware-independent performance guarantee follows.
No new regex JIT path was warranted by these transport/freshness measurements.

Final checks cover 248 CLI cases on macOS/Linux (two/three expected skips),
106 package passes with one skip each, all 16 ReleaseSafe conformance/hardening
steps, 1,000 generated ripgrep comparisons without mismatches/timeouts, 20 tooling
cases (one skip), Windows cross-compilation, release packaging and
license/toolchain/compatibility checks. Seven real-server indexed scenarios
collect, including the new JSON projection regression; they were not executed
locally. Provider checks remain local fixtures. JSON candidate pruning still
requires the accounting/validation contract described above.

Evidence: [final source/binary/report/archive qualification](bench/results/README.md),
[macOS paired trials](bench/results/README.md),
[Linux paired trials](bench/results/README.md),
[historical Linux witness experiment](bench/results/README.md), and
[generated oracle comparisons](bench/results/README.md).

### Linear framing and bounded encoder/capture work

The reference consumer now tracks a newline-search watermark independently of
its consumed-record cursor. Fragmented oversized JSON records require one scan
of each buffered byte; compaction adjusts both positions. Large records copy
directly from a temporary buffer view, avoiding an intermediate bytearray copy.
Small records retain their cheaper slice operation. Canonical JSON objects use
the cached stdlib raw decoder with an explicit end-of-input check; legal
surrounding whitespace retains the ordinary decoder path. UTF-8 validation,
malformed/trailing-data rejection and response allowances remain enforced.

V3 encodes typed events directly into one reusable, precisely allocated 32 KiB
body, using the existing byte-equivalent static-field serializer. An overflow
can only modify an unpublished suffix. The encoder flushes its complete prefix
and retries once; an oversized event uses ordinary streaming output. This
removes the per-row 32 KiB scratch array and encoded-row copy. Batch count,
body bounds, file/control boundaries and opt-in negotiation stay unchanged.

On macOS, capture amortizes empty source-watcher polls across 64 admissions only
for never-used slots. Watches are still registered before reads. Identity reuse
and recycled slots drain events before admission; Linux retains its existing
drain behavior. Capture still drains the kernel queue at its final publication
barrier. Deferred writes invalidate their tokens, and overflow/failure still
uses conservative fallback. A regression covers writes during admission and
slot reuse. This optimization does not reduce descriptor coverage or weaken
freshness certification.

Qualification compares product source `687cf045a2` with PR head `168a2b04e1`.
The transport driver rotates old/new binaries in both v2 and v3 modes for nine
warm trials, verifies complete decoded results outside timing, records consumer
profiles, and repeats watched process startup nine times. Separate native
encoding controls drain buffered bytes and decode only control records during
timing; each of three rotated controls runs 128 requests, after full decoded
result verification. Producer CPU includes workers and amortizes the 10 ms
process-counter resolution. Raw-drain elapsed is a diagnostic, not application
latency. The framing control mocks transport with identical 64 KiB chunks and
uses each revision's reader/decoder; its CPU is entirely consumer work.

Reproduce from the repository root (use the pinned Zig compiler to build the
two product binaries first):

```sh
python3 zig/pkg/antfly-grep/bench/framing.py --baseline=168a2b04e1 --output=framing.json
python3 zig/pkg/antfly-grep/bench/encoding.py BASELINE CURRENT encoding.json
python3 zig/pkg/antfly-grep/bench/transport_compare.py CURRENT BASELINE transport.json --profile --capture-rounds=9
```

| Control, previous → current | macOS | Two-CPU ARM64 Linux VM |
|---|---:|---:|
| Watched 6K-file startup, median elapsed | 300.29 → 175.74 ms | 687.87 → 663.59 ms |
| Same startup, producer CPU | 210 → 170 ms | 160 → 150 ms |
| Native v3 encoding CPU/request, 20K matches | 8.203 → 7.344 ms | 7.813 → 6.875 ms |
| Native v2 encoding CPU/request, same fixture | 8.281 → 8.438 ms | 8.594 → 8.750 ms |
| Decoded dense v2 delivery, median elapsed | 72.20 → 74.42 ms | 47.32 → 45.92 ms |
| Decoded dense v3 delivery, median elapsed | 41.34 → 40.20 ms | 29.89 → 30.29 ms |

The isolated macOS reader control measures 20K small synthetic frames at
23.82 → 20.70 ms CPU. A 16 MiB record costs 60.05 → 19.57 ms CPU, and a 64 MiB
record costs 752.13 → 67.68 ms CPU. Oversized inputs must still fit the configured
response allowance. Native v3 encoding uses about 10–12% less CPU in the raw
controls; decoded delivery remains largely consumer-bound. Default-v2 results
are mixed, with approximately 2% higher CPU in the raw controls and 6% higher
producer CPU in the macOS decoded control. There is no general v2 speedup claim.
An encoder-function-splitting prototype did not establish a consistent v2
benefit and was discarded. No new regex/JIT path was justified by these profiles.

The host is shared, filesystem caches are warm, and the Linux VM shares that
host. Startup is a cold-process measurement, not cold storage. RSS snapshots
exclude kernel watch storage and do not establish peak-memory improvement.
All trials are retained. Independent Linux hardware, cold caches and Windows
runtime remain unqualified. Checks pass 249 CLI cases per platform (two/three
expected skips), 107 package tests with one skip each, all 16 ReleaseSafe
conformance/hardening steps, 23 tooling cases (one skip), Windows cross-compilation,
release archive exactness, licensing and toolchain/compatibility checks.

Exact JSON candidate pruning remains **unimplemented**. Its server contract
must bind the authorized, path-filtered logical input set and validation outcome
to a retained snapshot, document revision and validator/schema version. Totals
must account for excluded documents without suppressing malformed-document
errors, source/file budgets, permissions, or ordering. Certificates must be
server-owned and atomically tied to source revisions; caller-provided trigram
markers alone cannot provide that proof. Missing support, incomplete coverage,
expired snapshots or incompatible options require a full exact scan or an
explicit error. This pass preserves the existing guard; it adds no server
protocol or indexed-pruning performance claim.

Evidence: [qualification and release identities](bench/results/README.md),
[macOS transport/profiles](bench/results/README.md),
[Linux transport/profiles](bench/results/README.md),
[macOS native encoding](bench/results/README.md),
[Linux native encoding](bench/results/README.md), and
[fragmented reader controls](bench/results/README.md).


## Dynamic namespace coverage and tuple delivery

Namespace coverage grows lazily instead of stopping at 4,096 route slots.
`workspace serve --workspace-watch` admits up to 65,536 namespace witnesses,
subject to descriptor headroom. `--workspace-watch-limit N` lowers or raises
that admission limit (maximum 1,048,576); it also caps strict source witnesses,
whose platform-specific ceiling can be lower. Namespace maps, watch tables,
routes and native directory events share `--workspace-watch-memory-limit SIZE`
(default 32 MiB): three quarters for metadata/strict namespace witnesses and
one quarter for native directory event subscriptions. These are admission
ceilings, not startup allocations. Source blobs, strict source-watch tables
and retained query evidence have their own existing bounds. Allocation failure,
watch exhaustion and lost events preserve conservative discovery/source reads.
No journal or extra source index is written to disk.

Registered lexical consumers can opt into tuple batches:

```json
{"version":1,"id":1,"op":"register","args":["STOP"],"compact":true,"batches":true,"row_format":"tuples"}
```

This advertises result schema v4. Batch `rows` contain
`[kind, line_number, absolute_offset, lines, submatches]`, where kind 0 means
match and 1 means context. Probability is null and scope is line. The existing
byte-preserving lines and submatch representations are retained. First and
oversized rows remain ordinary events, with the same 64-row/32-KiB batch-body
bounds. Default v1/v2/v3 formats are unchanged. A retried encoding attempt
resets row-local span capture, preventing duplicate spans in later cache replay.

The reference consumer retains original frames and offers `View.iter_file(path)`
and `materialize_file(path)`. Applications can decode only a visible file or stop after its first few frames.
Versioned private retained framing bounds decoding to the consumed frame,
including when a later line in that file is enormous; legacy array payloads
remain readable. `materialize()` remains an explicit whole-view operation. Validation and atomic
view publication still precede reuse. Unknown files raise `KeyError`. Iteration
pins a file's serialized payload and observation before yielding its rows.

## Indexed search uses the existing query API

The design decision and safety contract are recorded in
[the indexed query planning design](../../../docs/design/antfly-grep.md).
Configure table names and the document root, without index companion names:

```json
{"name":"sources","document_field":"grep"}
```

The CLI sends proven necessary literals as ordinary logical `contains` queries on
`grep.content`. Antfly chooses compatible indexed representations and verifies
original values before pagination. Mapping the content field with
`"x-antfly-types": ["text", "substring"]` enables indexed candidate selection.
The CLI explicitly permits scan fallback; optional `execution.value_verification` in
the table configuration controls `allow_scan`, `max_documents`, `max_bytes`, and
`max_regex_work`. Server verification budgets apply and
failures are reported. See the [shared logical-field query design](../../../docs/design/logical-field-queries.md).

The complete local matcher still decides final regex/PCRE matches. Unprovable
branches, unsafe encodings, inversion and modes requiring nonmatching documents
retain exhaustive query traversal. JSON/statistics retain their existing
file/byte totals. The older raw-trigram `candidate_index` remains compatible.

[Historical query-planning validation](bench/results/README.md)
records the earlier companion-routing binary and its exact validation results;
it is not qualification of the logical-field server planner.

[Initial logical-field qualification](bench/results/README.md) records
its implementation commit, source and artifact hashes, and macOS checks:
253 CLI cases (two skipped), 21 native logical-query checks without leaks,
143 existing query contracts, compiled error identities, generated SDKs, and 11
real-server HTTP cases across one/three shards. The HTTP suite covers original-value
semantics, policy errors, authorization, retained updates/expiry, narrow projections,
capability associations and logical keyword sorting. It includes immutable-storage
checkpoint reads for retained remote views. This is correctness qualification;
new indexed throughput and broader provider/platform qualification remain separate.

### Reusing logical value verification

The server resolves named autocomplete representations through declared source
associations. Observed physical fields with an unknown source do not advertise
invented logical paths. Lossless keyword equality can use literal postings directly;
its proof excludes synonym expansion and requires compatible, complete mappings.

Retained pages can reuse exact predicate membership in a process-local 16 MiB
cache with 128 slots, 2 MiB per entry, and the admitted cut's expiry. The cache
stores ID bitmaps and a scalar completeness flag; no source bodies, credentials,
snapshot leases, output, or authorization decisions are retained. Every request
still passes authorization, cut admission/expiry and projection readiness. Current
candidate constraints apply before using partial facts. Full membership also
includes the conservative index's proof of absence outside its candidates.
A different cut or predicate misses; cache refusal and eviction preserve exact
verification. These cache entries are never written to disk.

Within one execution, two to 64 direct original-value OR siblings evaluate the
same streamed document, retaining compact membership rather than a decoded corpus.
AND/nested branches keep their candidate scope and cheap literal fusion; indexed
siblings retain their selective iterator rather than expanding regex work.
Regex, equality and different-field clauses preserve compatible literal groups;
each group uses a bounded multi-output automaton. Demanded leaf errors propagate;
optional sibling errors remain unknown until that leaf is demanded. Regex work
is budgeted and cancellation/deadlines remain active. Separate execution components
can additionally share originals under an 8 MiB / 4,096-entry limit. Single-leaf
queries retain streaming scratch. Cache readers pin immutable entries; bitmap
copies occur outside the directory lock, completed hits copy only the result,
and oversized entries are rejected before eviction. Retired pinned entries remain
charged against the same 16 MiB allocation budget. Verification skips tombstoned physical slots before loading source and checks cancellation
while advancing over deleted runs. Query output hydration still has its own work;
zero verification loads does not imply a zero-cost API request.

The [reuse qualification report](bench/results/README.md)
records the exact source and binary identities, before/after workload records,
validation commands, and measured verification work. Qualification passes 31
native logical/storage checks without leaks, 143 query contracts, 14 real-server
HTTP cases, and 253 CLI cases (two skipped), plus generation and SDK checks.

| Workload | Verification loads before → after | Median HTTP time before → after |
| --- | ---: | ---: |
| Resumed page, 128 documents | 128 → 0 | 39.71 → 21.55 ms |
| Three-literal OR, exact count | 384 → 128 | 28.38 → 12.48 ms |
| Mixed literal/regex OR, exact count | 256 → 128 | 20.44 → 12.78 ms |

Each row has seven trials. Verification bytes fall from 1,052,416 to zero on
resumed pages, from 3,157,248 to 1,052,416 for literal OR, and from 2,104,832 to
1,052,416 for mixed OR. These counters exclude response hydration.
 Its bounded synthetic
workloads use Debug servers on a shared macOS host with warm caches. They do not
establish native Linux hardware, cold-storage, production-scale latency or RSS
parity. The design and memory/ownership rules are in
[logical field queries](../../../docs/design/logical-field-queries.md).

### Mixed predicate streaming and immutable cache ownership

Direct OR siblings now evaluate the same streamed original and retain compact
membership, including mixed regex/equality/different-field queries. Wholly literal
cohorts keep their direct automaton path. AND/nested branches and indexed siblings
retain selective candidate scopes. Optional sibling failures stay unknown until
demanded; work budgets and cancellation remain enforced.

Cache readers pin immutable entries, and bitmap copying occurs outside the
cache directory lock. Completed hits copy only their result; constrained hits
copy the smaller operand. Oversized or unreclaimable insertions are rejected
before eviction. Retired pinned entries and in-flight construction stay within
the same 16 MiB allocation cap. Named autocomplete uses whole query tokens for
both built-in analyzer aliases rather than matching shared short index prefixes.

The [streaming qualification](bench/results/README.md)
compares seven trials per workload/binary over 4,096 documents / 16.9 MB of
canonical source. All results have exact counts and matching corpus identities.

| Workload | Source loads before → after | Median HTTP time before → after |
| --- | ---: | ---: |
| Three literals | 4,096 → 4,096 | 142.79 → 138.17 ms |
| Three literals + regex | 14,293 → 4,096 | 440.99 → 152.19 ms |
| Three literals + regex + another field | 17,692 → 4,096 | 524.64 → 155.87 ms |

Mixed workloads reduce verification bytes to 16.9 MB per query. The regular HTTP
regression permits exactly 4,096 source loads and 20 MiB of verification, proving
one-pass behavior beyond the decoded-source cache ceiling. A narrow immutable
intersection succeeds under a 256-byte allocator even against millions of cached
IDs. Native tests also cover expiry/eviction with concurrent pins, admission under
pin pressure, deferred sibling failures and pruned AND branches doing no regex work.

The report retains the initial control observation, including its slower literal
case, before restoring the direct literal path and hoisting coverage ownership.
Qualification passes 43 native logical/storage cases without leaks, 143 query
contracts, eight schema cases, runtime/decision wire checks, 15 real-server HTTP
cases (one optional profile skipped there and run separately), and 253 CLI cases
(two skipped). SDK, source-ownership, generation and license checks pass.

Timings use Debug servers, warm caches and a shared macOS host, and include a
substantial main merge. They do not isolate every optimization or establish
production RSS, cold-storage or native Linux hardware performance guarantees.

### Qualification of namespace and tuple changes

The final-artifact controls compare code at `8f1a78f679` with the preceding
`687cf045a2` binary; the retained consumer is at `95126ee24a`. Each report records
binary hashes, all trials and verification outside the timed interval. Namespace
mutations compare revisions against forced rescans. Tuple delivery compares
expanded match/context data, provenance and logical statistics against v2/v3.

| Workload | macOS | Two-CPU Linux VM |
| --- | ---: | ---: |
| Unchanged refresh, 5,000 directories | 410.23 → 0.089 ms | 81.56 → 0.087 ms |
| One-file edit, 5,000 directories | 405.81 → 0.310 ms | 81.27 → 0.161 ms |
| Dense v3 → v4 consumer CPU/request, 20K lines | 28.13 → 23.24 ms | 27.82 → 20.19 ms |
| Dense v3 → v4 producer CPU/request | 7.031 → 5.391 ms | 8.437 → 5.781 ms |

The wide-repository gain removes conservative full discovery above the old
coverage ceiling; unchanged refresh visits zero source entries and a regular
one-file edit visits one. It is conditional on intact native-event coverage,
not a zero-cost filesystem guarantee. Forced rescans and membership/policy
changes retain their explicit work. The retained reader's memory regression
checks that consuming a small first frame does not materialize a later 2 MiB
line, with less than 128 KiB of additional traced Python heap in that control.

These are shared-host, warm-cache measurements. Linux runs in an ARM64 Docker VM
with namespace fixtures on tmpfs, not independent native Linux hardware or cold
storage. Raw v2 encoding repeats vary with contention and do not establish a
general default-v2 speedup. A nine-round Linux repeat did not reproduce the
earlier v2 CPU gap: median producer CPU was 8.438 → 8.359 ms, with paired
per-round ratios of 0.965–1.048. Both reports are retained. No additional
regex/JIT path was justified. Native
Windows runtime and independent storage/CPU qualification remain future work.

Evidence: [macOS namespace](bench/results/README.md),
[Linux namespace](bench/results/README.md),
[macOS tuple delivery/profiles](bench/results/README.md),
[Linux tuple delivery/profiles](bench/results/README.md),
[macOS native encoding control](bench/results/README.md),
[Linux native encoding control](bench/results/README.md),
[Linux nine-round repeat](bench/results/README.md).

### Scaling original-value verification

Named autocomplete roots now retain whole lowercase Unicode words. The separate
2–20 character prefix accelerator does not limit root-token length. Real-server
regressions cover one-character words, 20/21-character prefixes, complete long
words and near misses. Named representations built with the prior prefix-only
analyzer, including earlier development builds of this PR, need reindexing to
recreate omitted root terms.

Retained predicate membership uses a byte-budgeted hash directory with hot/cold
queues instead of a fixed 128-entry array. The old directory reproduced zero
hits in a second sequential pass over 129 small entries despite using only
54,180 bytes. Native stress checks now reuse all 129 and 4,096 entries; the latter
peaks at 2,318,360 charged bytes. Directory capacity, immutable facts, retired
pins and builders share the existing 16 MiB limit. This remains memory-only;
authorization, snapshot admission and response hydration still do work.

Indexed verification uses the existing source multi-read callback in adaptive
windows, with an 8 MiB allocation ceiling and a conservative allocator-headroom
check. Small sources retain the standard parser. Larger sources use a compiled
field-path projection that validates all JSON while decoding only selected
strings and path structure. It preserves array boundaries, duplicate-key
rejection and matcher behavior. Unused deep structures use iterative validation;
unsupported selected depth falls back to the ordinary parser. Refused windows
shrink or use scalar reads, and speculative reads consume shared work budgets.
Full-document caches never hold sparse projections.

The opt-in HTTP benchmark records nine trials for tiny sources, large selected
strings and large unused escaped arrays, checking exact results and verification
counts in every trial. It records first-result latency, server-process CPU,
throughput, sampled RSS/physical footprint and read/parse/window telemetry.

```sh
mkdir -p output
ANTFLY_BIN=/path/to/optimized/antfly \
ANTFLY_LOGICAL_SCALABLE_REPORT=output/logical-scalable.json \
uv run --project zig/e2e/antfly pytest -q \
  zig/e2e/antfly/test_grep_indexed.py -k logical_scalable_work_profile
```

Process environment controls `ANTFLY_BENCH_VALUE_SOURCE_SCALAR=1` and
`ANTFLY_BENCH_VALUE_SOURCE_FULL_DOM=1` disable batching and projection independently
for attribution. `ANTFLY_BENCH_VALUE_SOURCE_ROWS=2..64` holds window size fixed.
These are internal benchmark controls, not public query or CLI options. The
[design](../../../docs/design/logical-field-queries.md) specifies ownership,
admission and profile-metric coverage.

The [scalable verification report](bench/results/README.md)
pins the implementation, compiler, both optimized server binaries, driver, baseline
patch and every trial. Protocol 3 records process-CPU quiescence before timing,
with elapsed-time and Linux CPU-tick tolerance. Earlier observations and the
initial overly strict Linux settling failure are retained separately.

Shared-host macOS, nine trials per workload, preserved optimized baseline versus
the updated binary (the baseline already includes the initial cache/root fixes):

| Workload | Before | After |
| --- | ---: | ---: |
| Tiny sources | 7.46 ms | 6.57 ms |
| Large selected strings | 20.12 ms | 19.71 ms |
| Large unused escaped arrays | 53.08 ms | 48.16 ms |

Native ARM64 Linux in a two-CPU Docker VM uses within-binary attribution,
not a historical Linux baseline:

| Workload | Batching/projection disabled | Default |
| --- | ---: | ---: |
| Tiny sources | 11.83 ms | 9.42 ms |
| Large selected strings | 26.23 ms | 28.36 ms |
| Large unused escaped arrays | 67.66 ms | 48.77 ms |

The Linux selected-string case is slower under the default than under the
scalar and fixed-window controls; those controls and all initial observations
remain in the report. The small macOS selected-string difference is comparable
to variation across repeats. Process CPU and RSS are mixed, rather than uniform
gains. The default source windows peak below 0.5 MiB in these corpora; that
measures individual windows, not process RSS. Independent physical Linux, cold
storage and larger production workloads remain unqualified.

Profiles identify JSON decoding/validation as the main cost of these indexed
workloads. This change extends no regex/JIT path; it makes no additional
ripgrep/PCRE2 compatibility or local-search throughput claim.

### Enforcing analyzer upgrades and reducing verification costs

Named-autocomplete semantics now participate in the physical generation's durable
text-projection witness. Existing nonempty prefix-only generations are quarantined
with `TextProjectionProvenanceMismatch` until explicitly rebuilt; reopening cannot
silently relabel their postings. This also applies to unversioned named-analyzer
generations already built with complete words: the old witness cannot prove
which implementation produced their postings. Unaffected analyzers keep their
existing witness. The native upgrade regression creates real legacy terms, checks refusal, then
builds and reopens a private replacement containing complete short and long words.

Retained membership expiry uses an indexed min-heap within the same 16 MiB budget.
Replacement and eviction remove their heap nodes immediately, so stale deadline
records cannot accumulate. A 4,096-entry staggered-expiry test requires fewer than
122,880 heap comparisons while preserving active reader pins. This removes the
full-directory walk at each distinct expiration; it does not eliminate the lock
or allocation cost of maintaining retained facts.

Source-read adaptation now measures scalar and bounded batch provider costs on
required reads. It samples power-of-two classes from two through 64 rows, keeps a bounded decayed
history, periodically probes alternatives and may choose scalar reads. Learned
byte limits round up to size classes to avoid boundary cliffs; allocator admission, document/byte budgets and
scalar fallback retain their existing authority. Calibration does no duplicate I/O.

The JSON adapter borrows plain selected ASCII strings using a vector-assisted
scan and validates skipped escaped/Unicode strings without producing a token for
every fragment. All source syntax and duplicate keys are still checked. Standard
scanner paths remain responsible for keys, diagnostics, streaming and decoding
escaped selected strings. Generated mutations and truncations compare rejection
and structural continuation with the standard scanner. Allocation-failure tests
force deterministic growth so Linux remapping differences cannot change their
failure enumeration.

`zig/lib/json/bench_value_strings.zig` isolates the two string paths with nine
alternating standard/native samples and checked results. On the shared Apple
host, with this task's compilers paused, median standard/native ratios were
14.83× for selected plain 4 KiB strings, 3.88× for skipped strings with 16 escapes,
and 5.68× for skipped 128 KiB escaped strings. These ratios describe scanner
throughput; complete-query timings must also include storage reads,
matching, authorization and response work. The shared
[design](../../../docs/design/logical-field-queries.md) documents the migration and
dataflow contracts, including requirements for a future versioned stored-field
layout that could avoid validating unrelated source bytes.


The [follow-up qualification report](bench/results/README.md)
pins the merged implementation, compiler, source and final binary hashes. It
retains all 16 primary configurations, a paired Linux repeat, initial variable
observations, an idle-threshold rejection, all timed trials and CPU/memory data.
Real process-upgrade tests on macOS and Linux reject the historical witness,
rebuild through the public repair endpoint, then verify restart durability.

Optimized macOS medians use nine timed queries per workload on the shared host,
with fresh predicate work for every query and warm code/source caches:

| Workload | Preserved baseline | Updated default |
| --- | ---: | ---: |
| Tiny sources (4,096 documents) | 6.89 ms | 7.00 ms |
| Selected 4 KiB strings (4,096 documents) | 20.08 ms | 9.53 ms |
| Unused escaped arrays (1,024 documents) | 48.48 ms | 17.61 ms |

Selected strings improve about 2.1× and escaped arrays about 2.8× in that macOS
run; tiny-source latency is effectively unchanged. The two native ARM64 Linux VM
comparisons observe 1.6–2.5× and 3.1–4.5× respectively, with substantial absolute
timing variation. The VM has a two-CPU quota and shares the Apple host. An
unchanged tiny-source decoder control also varied, so those observations do not
establish a physical Linux speed guarantee or isolate every control's causal
cost. Fixed windows sometimes have lower latency than the learned choice;
CPU and process RSS improvements are not uniform. Default-window allocation
peaks stay below 1 MiB on these corpora, compared with roughly 0.5 MiB in the
preserved baseline; they are higher despite the latency gains. Every individual
window stays within the existing 8 MiB allocation ceiling. Independent physical Linux,
cold-storage and broader production concurrency qualification remain outstanding.

Reproduce one configuration from the repository root with the optimized server
and matching native CLI paths:

```sh
ANTFLY_BIN=/path/to/antfly ANTFLY_GREP_BIN=/path/to/antfly-grep \
ANTFLY_LOGICAL_SCALABLE_REPORT=/path/to/run.json \
uv run --project zig/e2e/antfly pytest -q \
  zig/e2e/antfly/test_grep_indexed.py -k logical_scalable_work_profile
```

The report records internal scalar, fixed-16/32/64, full-DOM and combined controls.
Profile counters and source hashes accompany each observation; scanner-only
ratios above remain separate from complete-query latency.


Logical verification uses the document store's revision-bound authority, upgraded
to `DPD2` object paths with legacy `DPD1` fallback. This handles the default nested
`grep.content` schema without decoding unrelated `grep.regions`. Parent/kind flags
fit the existing per-entry space; metadata remains capped at 32 KiB, 512 nodes and
one sixteenth of original size. Construction has a fixed linear work allowance.
Arrays and objects exceeding a limit stay opaque and are fetched as complete
subtrees. Selected hashes are checked, original-byte budgets remain authoritative,
and projected JSON never populates the complete-document cache. Missing/stale
or legacy authority wakes bounded maintenance only on writable owners. Read-only
retained cuts preserve their immutable fallback. Source matching and the public
document contract remain unchanged.

Table capabilities group known logical sources by `field`, with `kind`,
`value_operations`, resolved sort availability and nested `representations`.
Concrete names, analyzers, index-relative query modes and coverage belong to
those representations. Antfly resolves compatible views for logical operations
and sorting. Reserved `_id`, conditional computed `_score`, templates and unknown
source associations remain explicit. SDKs use this grouped response; migration
details are in the [logical-field design](../../../docs/design/logical-field-queries.md).

The source tuner now attributes scalar fallbacks correctly, excludes truncated
batches from full-arm samples, and measures batch preparation/allocation as well
as provider time. Representation changes restart learning without attributing
one-time snapshot setup to an arm. Read windows retain at most 512 KiB of arena
capacity between batches, within the existing live-allocation/request limits.

Required full-source parsing also supplies selected-string byte counts. A small
query-local estimate avoids opening a projection reader when those values fill
most of the original, and keeps observing full reads as document shapes change.
Unproductive metadata probes back off for 32–1,024 rows. After a reader is pinned,
backoff, small sources and root-wide predicates use complete bodies from that
same snapshot. Deliberately skipped metadata does not trigger repair. This adds
no persisted cache and no second source scan.

`bench/logical_sources.py` preserves pinned artifacts, raw samples and alternating
baseline/candidate order. `--controls` includes full-original reads, scalar reads
and fixed 16/32/64-row windows. The concurrent fixture records a cold-process
query after restart and nine synchronized groups per client count. It measures
request/first-byte latency, throughput, process CPU and endpoint memory, and
records container CPU/memory quotas where available. Restarting a process is not
cold-storage qualification.

```sh
zig/e2e/antfly/.venv/bin/python zig/pkg/antfly-grep/bench/logical_sources.py \
  --baseline /path/to/baseline/antfly --candidate /path/to/candidate/antfly \
  --grep /path/to/antfly-grep --output output/logical-source-comparison \
  --rounds 2 --clients 1,4 --controls
```

On Linux, `--advise-file-cache` optionally advises eviction of the fixture's own
files before restart. The report records advised files and startup/query process
I/O. It leaves global caches alone; advice and restart do not guarantee cold
storage, and startup can warm data again. Use this with the concurrent or both
suites, including inside CPU/memory constrained Linux containers.

The [source-reader qualification](bench/results/README.md)
pins implementation `cf71262234`, merged main `f322acc4de`, compiler and binary
identities, all 60 final configurations, and the preceding diagnostic runs.
Each scalable comparison below combines 18 warm HTTP trials across two rounds
with reversed configuration order. Every request verifies the complete corpus;
predicate membership is fresh. The Linux VM has a one-CPU quota and 1 GiB limit.

| Corpus | macOS baseline | macOS default | Linux baseline | Linux default |
|---|---:|---:|---:|---:|
| Tiny values, 4,096 documents | 6.78 ms | 7.98 ms | 8.52 ms | 9.13 ms |
| Selected 4 KiB strings, 4,096 documents | 9.27 ms | 10.15 ms | 14.38 ms | 14.48 ms |
| Dominant selected 16 KiB strings, 1,024 documents | 5.57 ms | 6.82 ms | 7.43 ms | 7.47 ms |
| Unused escaped arrays, 1,024 documents | 16.82 ms | 7.58 ms | 18.91 ms | 8.98 ms |
| Nested content and 1,024 regions per document, 1,024 documents | 107.47 ms | 8.60 ms | 105.19 ms | 10.52 ms |

The nested-source case improves about **12.5× on macOS and 10.0× in the VM**;
unused arrays improve **2.2× and 2.1×**. The same-binary full-original control
isolates the reader path: nested medians are 104.19/105.87 ms and unused-array
medians 16.73/18.69 ms respectively. Nested mean process CPU falls from
110.3 to 7.4 ms on macOS and 106.1 to 11.1 ms in the VM.

The initial implementation repeatedly inspected metadata when selected values
filled the original. Required-parse feedback and bounded backoff remove that
penalty: final dominant-value latency is close to the same-binary complete
reader (6.69 ms on macOS, 6.85 ms in the VM). Small and selected-full cases have
higher macOS medians than the preserved baseline. Their full-original controls
show similar increases; unchanged parser timings and pre-ingestion idle CPU
also vary between rounds. The report preserves that variation, including the
earlier regression. These measurements establish gains on the shown workloads.

Concurrent request medians, with one and four simultaneous clients, are:

| Environment | One client, baseline → default | Four clients, baseline → default |
|---|---:|---:|
| macOS | 16.29 → 5.63 ms | 18.02 → 9.60 ms |
| ARM64 Linux VM, one CPU / 1 GiB | 23.41 → 10.98 ms | 118.78 → 86.80 ms |
| ARM64 Linux VM, two CPUs / 1 GiB | 22.01 → 10.83 ms | 69.38 → 41.65 ms |

Default admitted windows peak below 0.8 MiB in both scalable suites, within the
8 MiB hard ceiling and 512 KiB retained-arena cap. Window and process-memory
changes are mixed: nested sampled RSS changes from 676.7 to 662.5 MiB on macOS
and 381.4 to 371.1 MiB in the VM; the tiny macOS case rises slightly. Process
samples include ingestion state, caches and mappings and are not allocation
bounds. Raw first-byte, CPU, throughput, window, RSS and process-I/O observations
and scalar/fixed-window controls accompany every configuration.

Both final optimized servers pass 18 HTTP/CLI integration and process-upgrade
cases, including grouped capabilities, logical sorting, retained revisions,
original-byte budgets, DPD1 migration, rollback writes and re-upgrade. Four
optional benchmark cases are exercised separately. Native logical tests pass
63 cases in Debug and ReleaseFast, alongside 144 query contracts. The complete
Linux correctness suite uses 4 GiB: its repetitive substring-index fixture
exceeds automatic build budgets at 1/2 GiB; the preserved baseline also fails
that fixture at 1 GiB. Those failures are retained separately from the successful
1 GiB query benchmarks.

The Linux runs execute native ARM64 instructions in a VM on the same shared
Apple host. File-cache advice is limited to isolated fixture files; process
restarts and advice do not establish device-cold or independent-hardware results.

### Bounded substring indexing and source startup

Substring index construction consumes the exact built-in word/lowercase/shingle/
suffix pipeline through a borrowed-token stream. Two sequential source passes
preserve unigram/bigram order using a fixed normalized window. Each distinct term
is interned once, the first two positions stay inline, and larger vectors and the
dictionary use reclaimable, quota-preserving scratch. Scratch is released after
the posting builder copies its inputs. Inputs below 128 bytes retain the small
owned-token path. Other analyzers and highlight analysis keep their existing
owned-token behavior. No analyzer generation, disk cache, or index migration is
introduced.

Logical original-value verification now bootstraps with two full documents,
seeds its size estimate from the first required payload, and skips clock reads
for rows already buffered. Projection admission, snapshot ownership, work budgets
and adaptive-window limits retain their existing contracts.

The [substring/startup report](bench/results/README.md)
records production code at `34091b088b`, following the merge of main
`dd49f55102`. Its component benchmark rotates the old owned accumulator, current
owned control and borrowed stream over nine trials on three 180 KiB corpora.
All variants must produce identical complete postings digests, including field
norms, frequencies and positions. Construction timings exclude digest validation
and teardown; whole-server batch-write measurements separately include cleanup
and background work. These component results do not measure local CLI search.

| Corpus | macOS construction, old → stream | Linux VM construction, old → stream | Peak tracked allocation, old → stream |
|---|---:|---:|---:|
| Repetitive | 9.87 → 6.13 ms | 11.58 → 6.81 ms | 218–219 → 2 MiB |
| Source-like | 10.68 → 8.05 ms | 11.82 → 10.44 ms | 101 → 6 MiB |
| Distinct long word | 12.98 → 10.31 ms | 14.85 → 18.76 ms | 108 → 23 MiB |

The distinct-word Linux case trades about 26% more construction time for about
79% less tracked allocation. The report preserves every trial and the owned
control rather than treating all inputs as a throughput win. Instrumented peak
allocation is distinct from sampled process RSS. Both optimized servers now pass
all 18 HTTP integration/upgrade cases, with Linux constrained to 1 GiB; this
supersedes the preceding 4 GiB correctness qualification for the expanded
substring fixture.

The same-main macOS server comparison alternates baseline/candidate order over
two rounds and retains 18 fresh-verification trials per corpus:

| Verification workload | Baseline → candidate HTTP median | Largest observed read window, baseline → candidate |
|---|---:|---:|
| Tiny | 6.94 → 7.20 ms | 17.5 → 17.4 KiB |
| Fully selected 4 KiB value | 9.92 → 10.37 ms | 717 → 598 KiB |
| Dominant selected 16 KiB value | 5.93 → 6.03 ms | 428 → 720 KiB |
| Unused escaped arrays | 6.94 → 7.04 ms | 479 → 109 KiB |
| Nested content and regions | 9.15 → 8.27 ms | 776 → 219 KiB |

The bootstrap replaces 16 original reads with two before useful projection;
the 1,024-document nested/unused-array cases now use 1,022 stored projections
rather than 1,008. Nested parse time falls, while small/full-selected latency and
some window sizes rise. One/four-client request medians change from 5.74/9.37
to 5.85/10.36 ms; cold-process results and ingestion CPU are also mixed. This
qualification establishes lower expanded-token allocation and bounded initial
reads, not a universal server speedup. All timings, first-byte samples, work
counters, ingestion batches and memory observations remain in the report.

To reproduce the component benchmark, build the implementation test owner and
run it with strict selection (from `zig/`):

```sh
zig build introducer-test-build -Doptimize=ReleaseFast
ANTFLY_BENCH_SUBSTRING_STREAM=1 zig-out/bin/antfly-introducer-tests \
  --suite-filter introducer.substring
```

For native ARM64 Linux, add `-Dtarget=aarch64-linux-musl -Dmetal=false
-Dsystem-blas=false`, then run the resulting executable on Linux. The build-only
target installs the implementation test partition after its imports finalize;
installing only the consumer test binary would omit named-dependency tests.


The incremental-execution follow-up adds a separate ingestion workload to the
logical-source benchmark driver. It exercises repetitive text, source-like text
and seeded distinct words through the real substring index. Requests and typed
profiles use `execution.value_verification` and `profile.value_verification`.
For frozen binaries from before that API cleanup, the benchmark-only adapter
translates those two wire spellings; functional tests always use the public API.

```sh
python3 zig/pkg/antfly-grep/bench/logical_sources.py \
  --baseline /path/to/frozen-antfly --candidate /path/to/current-antfly \
  --grep /path/to/current-antfly-grep --output output/new-qualification \
  --suite all --rounds 2 --baseline-legacy-value-wire
```

Use a new output directory for each qualification. The report records artifact
hashes, fixture hashes, raw HTTP trials, ingestion CPU and wall time, and
source-reader work. Process restarts do not establish cold-storage performance.
The execution design and cache-proof limitations are recorded in
[logical field queries](../../../docs/design/logical-field-queries.md).


The [incremental execution qualification](bench/results/README.md)
records the frozen baseline and candidate binaries, raw trials, source hashes,
work counters, integration checks and unsuccessful admission attempts. Both
servers pass 19 integration/upgrade cases, with five opt-in profiling cases
skipped during correctness runs. Linux correctness and verification/concurrency
profiles use a 1 GiB limit. The CLI suites run 253 cases on each platform, with
two macOS skips and 32 Linux platform skips. Debug and ReleaseFast each pass 75
logical-query and 144 query-contract tests.

These are warm-filesystem measurements on one shared Apple Silicon host; Linux
runs native ARM64 instructions in a VM. Two alternating rounds retain 18 fresh
verification trials per corpus and 18/72 request samples for one/four clients.
HTTP medians in milliseconds are:

| Workload | macOS baseline → candidate | Linux, two CPUs/1 GiB, baseline → candidate |
|---|---:|---:|
| Tiny values | 9.42 → 9.73 | 8.99 → 8.44 |
| Selected 4 KiB values | 11.47 → 11.79 | 13.34 → 13.46 |
| Dominant selected 16 KiB values | 7.84 → 8.16 | 7.68 → 7.91 |
| Unselected escaped arrays | 9.41 → 9.41 | 8.56 → 7.68 |
| Nested source contract | 10.37 → 7.63 | 9.06 → 9.86 |
| One concurrent client | 6.35 → 5.89 | 9.47 → 9.42 |
| Four concurrent clients | 9.24 → 8.43 | 39.43 → 38.86 |

The one-CPU Linux control changes one/four-client medians from 9.57/89.05 to
9.84/88.04 ms. These results are mixed; they do not establish a general latency
win. Required source-reader exploration is bounded to ten completed provider
calls under stable caps, and scalar read counts fall from about 16 to 3–4 in
these workloads. Refused/truncated reads remain untrained, and later probes
continue to adapt. Direct warm retained counts consume an immutable bitmap lease
and subtract deletions without allocating a result bitmap or reading sources;
zero-byte allocator tests and HTTP verification counters cover this component,
not complete request cost.

For 180 KiB high-distinctness input, matched streamed-token construction changes
from 9.35 to 8.65 ms on macOS and 11.91 to 11.00 ms on Linux. Maximum tracked
construction allocation changes from 24.32 to 22.65 MB and 24.18 to 23.48 MB,
respectively. Other construction cases change little. Every variant preserves
identical postings digests. These construction-only counters exclude the rest
of index publication and are distinct from process RSS.

Full substring ingestion remains mixed: macOS repetitive/source-like/distinct
HTTP medians change from 136.78/119.93/1181.88 to 131.23/122.64/1174.97 ms;
Linux changes from 204.54/208.57/1418.74 to 231.94/228.53/1467.42 ms.
The Linux ingestion stress comparison uses equal 8 GiB node envelopes. The
frozen baseline exhausted its default full-text build slice at 1 and 2 GiB;
the candidate also failed that stress case at 1 GiB. Both small-node envelopes
keep this slice at its 128 MiB floor. Raising the node envelope admits a larger
build slice for timing; it does not establish that the process requires 8 GiB
RSS or fix low-memory ingestion admission. All failed attempts remain recorded.

The API review retains `remote_snapshot`, including hierarchy-child resumption,
and `autocomplete`. Verification controls now use `execution.value_verification`
and profiles use `profile.value_verification`; freshness uses `read_requirements`.
Removed public lake aliases and `value_search_execution` are rejected. SDKs and
MCP schemas are regenerated. Complete cached predicate facts remain memory-only;
validated whole-generation owner proofs allow reuse across equivalent cuts.
Writes invalidate those proofs. Finer reuse across writes requires maintained
per-segment source-binding certificates.


### Review follow-up: bounded counts and prepared indexing

The [review follow-up qualification](bench/results/grep-review-followup-summary.json)
supersedes the preceding qualification for the server fixes below. It records
frozen artifacts, paired trials, exact build commands, source hashes, unsuccessful
diagnostic attempts, and remaining limits. Raw trials and logs are local ignored
artifacts, not published downloads. The reference server is unstripped and the
candidate is stripped; these comparisons do not isolate algorithm-only latency,
executable-size, or RSS changes.

The Python SDK preserves closed query variants through union decoding, including
nested Boolean predicates and explicit `minimum_should_match` values. Public
Boolean lowering preserves those thresholds through distributed execution.
Supported complete cached Boolean counts borrow immutable membership facts and
avoid allocating result bitmaps. Sparse negation uses cached cardinality and
exceptions instead of traversing absent document words: the maximum-domain
regression falls from 67,108,864 word-program steps to zero for one input and
three for the nested example. This is bounded component work, not zero request
cost. Authorization, query parsing, read-cut validation, and requested output
still run.

Lookup expiry maintenance retires at most eight heap entries plus the requested
expired key, and frees retired ownership after releasing the cache mutex.
Optional admission bounds its eviction search. Provider timing hints have a fixed
capacity and TTL, influence batching only, and never certify predicate membership.
The distinct-predicate fixture requires source verification on every request.

Prepared text publication now enables the same private postings spill path as
direct sink construction. Repeated physical-field contributions are grouped in
stable order, analyzed one group at a time, and spilled before the next group.
An explicit reclaiming scratch allocator owns transient maps and position
vectors; encoded results retain separate ownership. The native preparation
regression uses about 62.6 MiB of tracked build allocation in ReleaseFast under
a 128 MiB allowance. Its opaque-region allocator regression reduces tracked
construction peak from 36.8 to 21.6 MiB. These counters exclude process RSS and
the rest of publication. Private runs use temporary disk scratch; no persistent
query-result cache is added.

The final server passes 21 integration/upgrade cases on each platform, with six
opt-in skips. Debug and ReleaseFast each pass 84 logical-query, 144 query-contract,
and 44 introducer cases plus the focused preparation regression, with no leaks;
667 SDK tests and generation consistency pass. Linux additionally passes the
actual ingestion fixture with two CPUs, 1 GiB RAM and swap disabled. That resolves
the small-node admission failure recorded in the preceding qualification.

Two alternating warm-filesystem rounds on this shared Apple Silicon host produce
these HTTP medians in milliseconds. Linux uses native ARM64 instructions in the
VM, two CPUs and 1 GiB for retrieval/concurrency; paired ingestion uses equal
8 GiB envelopes. The separate 1 GiB check establishes candidate admission, not
performance equivalence under the smaller envelope.

| Workload | macOS reference → candidate | Linux reference → candidate |
|---|---:|---:|
| Tiny values | 7.65 → 6.93 | 9.22 → 9.76 |
| Selected 4 KiB values | 9.50 → 9.76 | 14.97 → 15.45 |
| Dominant selected 16 KiB values | 5.95 → 5.87 | 7.72 → 7.79 |
| Unselected escaped arrays | 6.71 → 7.98 | 9.10 → 8.41 |
| Nested source contract | 7.49 → 7.92 | 10.79 → 9.84 |
| One concurrent client | 5.11 → 5.27 | 11.38 → 12.21 |
| Four concurrent clients | 9.56 → 9.88 | 47.16 → 49.09 |
| Repetitive ingestion | 148.20 → 146.21 | 363.70 → 358.85 |
| Source-like ingestion | 133.68 → 140.83 | 388.94 → 384.64 |
| Distinct-word ingestion | 1390.44 → 1572.54 | 1935.62 → 2113.11 |

Results are mixed. Bounded spill fixes admission and reduces scratch retention,
but distinct-word ingestion takes about 13% longer on macOS and 9% longer on
Linux in this comparison. These trials establish neither a general latency win
nor an RSS reduction. Process restarts and fixture-file cache advice do not
establish cold-device performance.

The broader cost target still requires coordinator-owned request-wide verification
grants and maintained per-segment source-binding certificates. Current budgets
remain per shard request, and writes invalidate whole-generation proofs. The
[execution design](../../../docs/design/logical-field-queries.md) records ownership,
fallback behavior, and the required fences for those remaining changes.

### Postings runs and retained result pages

The [postings/page qualification](bench/results/grep-postings-pages-summary.json)
compares matched ReleaseFast, stripped server builds with the same compiler.
It supersedes the preceding server qualification for these changes; the
standalone CLI and its dependency sources are unchanged. Raw trials and logs
remain local ignored artifacts. These measurements use a warm filesystem on
one shared Apple Silicon host and its ARM64 Linux VM.

Default postings runs retain the 8 MiB trigger on small managed build slices.
Larger slices can use up to 64 MiB, based on advisory live headroom; ordinary
resource admission still governs allocations. Pressure spills the largest fields
first and retains tiny companions. The private run's lazy read cache increases
from 64 to 512 KiB, with four-way lookup. The sixteen-cursor component regression
performs 2,048 logical reads with 32 physical reads. This work bound is separate
from end-to-end ingestion performance. Runs remain temporary disk scratch,
unlinked while open on POSIX; query membership facts remain memory-only.

Complete immutable memberships remain borrowed through constant-score pages,
ranked include/exclude gates, and native sorted pagination. Captured deletion
masks are subtracted word by word; sparse iteration seeks over unrelated mask
ranges, and dense deletion skips check cancellation. Misses and unsupported
Boolean shapes retain owned execution. Consumers still allocate requested
output and capture deletion state. This removes avoidable bitmap copies, not
the cost of parsing, authorization, read-cut validation, and presentation.

Source windows use their already-leased segment reader for identity lookup.
Separate opt-in diagnostic runs attribute key lookup, provider work, revision
lookup, directory reads/planning, selected ranges, reconstruction, and fallback.
For reproduction, pass frozen server binaries to the benchmark driver:

```sh
python3 zig/pkg/antfly-grep/bench/logical_sources.py \
  --baseline /path/to/reference/antfly --candidate /path/to/candidate/antfly \
  --grep /path/to/antfly-grep --suite all --rounds 2 \
  --output output/grep-postings-paired
python3 zig/pkg/antfly-grep/bench/logical_sources.py \
  --baseline /path/to/reference/antfly --candidate /path/to/candidate/antfly \
  --grep /path/to/antfly-grep --suite hints --source-phases --rounds 1 \
  --output output/grep-postings-phase-diagnostic
```

Use a separate directory for each run; the driver refuses to overwrite evidence.
Run the `pages` diagnostic suite separately to inspect membership ownership.
Ordinary benchmark requests enable the public query profile, including the
top-level lookup/ownership counters. Extra physical-source phase clocks and log
capture are opt-in; those diagnostic timings are not pooled with ordinary
latency trials. The
retained-page fixture covers early, middle, and final dense/sparse pages over
16,384 documents and requires zero source re-verification on measured pages.

Maintained source-binding certificates across writes and coordinator-owned
verification grants remain unimplemented. Existing whole-generation proofs and
per-shard request budgets remain authoritative. The
[execution design](../../../docs/design/logical-field-queries.md) records the
publication, mutation, repair, restart, capacity, and lifetime fences needed for
those changes.

Qualification passes 21 integration/upgrade cases on each platform (seven opt-in
skips), 215 selected search/storage tests in both Debug and ReleaseFast (one
skip), 89 logical-query and 144 query-contract cases in Debug, and 46 introducer
cases in both modes. Allocation-failure and ownership checks report no leaks.
The separate Linux ingestion check passes with two CPUs, 1 GiB RAM and swap
disabled. Paired Linux retrieval uses two CPUs/1 GiB; paired ingestion uses equal
8 GiB envelopes with swap disabled.

Two alternating primary rounds produce these HTTP medians in milliseconds:

| Workload | macOS reference → candidate | Linux reference → candidate |
|---|---:|---:|
| Tiny values | 11.74 → 12.49 | 12.73 → 13.63 |
| Selected 4 KiB values | 14.71 → 19.68 | 24.13 → 23.86 |
| Dominant selected values | 8.94 → 10.36 | 10.41 → 14.59 |
| Unselected escaped arrays | 11.47 → 11.24 | 14.70 → 12.05 |
| Nested source | 11.78 → 12.64 | 15.90 → 15.24 |
| One concurrent client | 8.35 → 8.23 | 15.34 → 15.25 |
| Four concurrent clients | 18.13 → 16.30 | 69.91 → 67.85 |
| Fresh distinct predicates | 82.12 → 69.00 | 85.45 → 78.85 |
| Repetitive ingestion | 259.29 → 184.80 | 1093.69 → 728.92 |
| Source-like ingestion | 213.83 → 197.79 | 1561.50 → 846.28 |
| Distinct-word ingestion | 2522.38 → 2118.94 | 7344.83 → 3470.95 |

Linux distinct-word ingestion reduces process read characters from 205.9 to
128.4 MB and process write bytes from 96.6 to 75.3 MB (about 38% and 22%). Its
process CPU median falls from 3.71 to 2.95 seconds. These are process I/O
counters on a warm filesystem, not cold-device bandwidth measurements.
Ingestion improves in these trials; retrieval remains mixed, including slower
selected-value cases. No general latency or RSS improvement is established.

The primary cached-page trials were also mixed and showed some larger macOS
slowdowns. A post-hoc four-round page repeat, kept separate from those primary
trials, produced the following HTTP medians. It did not reproduce the large
late-page slowdown, and it does not establish a consistent page-latency win.

| Cached page | macOS reference → candidate | Linux reference → candidate |
|---|---:|---:|
| Dense, first | 153.86 → 169.17 | 216.30 → 218.55 |
| Dense, middle | 162.54 → 162.55 | 225.58 → 219.74 |
| Dense, final | 154.98 → 152.31 | 218.20 → 222.35 |
| Sparse, first | 23.51 → 27.37 | 20.20 → 19.31 |
| Sparse, middle | 24.15 → 22.86 | 20.33 → 20.41 |
| Sparse, final | 24.63 → 25.82 | 19.14 → 20.24 |

On each platform, the candidate page diagnostics record 90 successful segment
borrows across 18 page requests, zero membership copies, and zero source
re-verification. This establishes the ownership/work bound independently of
latency. Dense-deletion regressions check a million-document domain, wordwise
skipping, cancellation, sparse seeks and upper-bound clipping.

Fresh-predicate diagnostics identify revision lookup (median component times
27.3–27.5 ms) and selected-range reads (15.8–18.1 ms) as the main source costs in
that fixture. Identity lookup is only 30–51 μs; directory reads take 6.3–6.7 ms.
These component medians are diagnostic observations, not additive attribution
of every workload. Batched revision/source validation and maintained binding
certificates are better next targets than enlarging an identity cache.


### Decisions transport qualification

`bench/decision_transport.py` now compares the public single-input Decisions
contract, native Antfly batches, and versioned shared contexts. It uses local
constant-score fixtures, alternating trial order, binary/source hashes, and
output equivalence; it never contacts a model. Use `--transport single` or
`--transport shared-v1`, with `--baseline-transport antfly-batch`. To isolate
transport from code changes, pass the same binary as candidate and baseline.
`--cache` measures warmed decision caches; `--keep-alive` enables persistent
fixture connections. Write raw JSON under ignored `output/`, not this directory.
Protocol compatibility, fewer bytes or requests, and cache hits do not establish
model quality, provider token charges, or a universal latency advantage.


The [Decisions client qualification](bench/results/grep-decisions-client-summary.json)
records artifact hashes, conformance checks, and three alternating trials per
transport on macOS and the ARM64 Linux VM. In the 512-line constant-score fixture,
all modes preserve 513 evaluations and identical output. Shared contexts reduce
request bodies from 10,548,999 to 1,412,265 bytes (86.6%) with 65 requests;
common single-input Decisions needs 513 requests. Warmed caches require zero
provider requests in both protocols. These are protocol byte/request measurements,
not provider token costs or a claim that complete searches are free.


## Visual decision search

Use the same Decisions endpoint and credentials with explicit image mode:

```sh
antfly-grep --provider antfly --model VISION_DECIDER \
  --media images --ask 'Does this show a failed checkout?' --json ./screenshots
antfly-grep --provider antfly --model VISION_DECIDER \
  --media images --ask 'Which slide shows declining revenue?' --table-config tables.json
```

Local PNG/JPEG/WebP/GIF files are whole-image units. Indexed
[document v2](schema/document-v2.json) adds page/slide/image manifests and
revision-bound inline image fields on the same parent row. The configured
`document_field` selects v1 or v2 sources; text mode remains the default. DocsAF
`ToEvidenceRecord` can construct v2 source records with explicit ACL metadata.
PDF/slide rendering and Cloud Drive synchronization belong to ingestion.

Visual mode sends one ordered text/image evidence message per unit, with multiple
questions sharing that image. JSON evidence records identify the unit, page/slide,
source revision, image digest and recipe; they contain no invented line numbers
or bounding boxes. Counts refer to matching visual units. The provider must
support image Decisions; native text-only models, lexical prefilters, triage,
stdin and workspace media are rejected. Antfly multimodal batches and shared text
contexts preserve images; explicit single-input transport remains available for
custom services.

Default image limits are 512 KiB and 16,777,216 pixels. Adjust
`--max-image-bytes SIZE` (maximum 768 KiB) / `--max-image-pixels N`; the 1 MiB
serialized Decisions evidence cap still applies. Images are inspected without
raster decoding. Budget errors retain completed partial results and exit 2.
Cached scores avoid repeat inference; indexed media is freshly authorized and
validated before every cache lookup. Grep never fetches arbitrary provider URLs.
See the [design](../../../docs/design/antfly-grep.md#multimodal-source-evidence-and-cloud-boundary-2026-10-21)
and [Cloud delivery issue](https://github.com/antflydb/colony/issues/790) for the
private asset, ACL and connector lifecycle requirements. Cloud connector work is
not included in this CLI implementation; fixture tests do not establish visual
model quality.


Visual evaluations use waves capped at 16 units and 8 MiB conservative evidence.
Indexed waves fetch distinct same-parent image fields together with one fresh
authorized ID query; these point reads create no pagination snapshots. The
manifest scan keeps its retained snapshot and ascending-ID order. Identical asset
references share validated bytes within a wave, and successful siblings survive
refusal, malformed item answers and oversized-batch partition failures. Warm
scores still require current asset authorization, revision and digest validation.
Local `--max-filesize` filtering and hard `--max-file-size` limits apply before
inference. Quiet mode respects file selection, including `--files-without-match`.

The [visual batching qualification](bench/results/grep-visual-batching-summary.json)
compares the prior PR binary with this implementation using eight distinct
same-parent images, five alternating trials and 20 ms artificial delay per HTTP
request. Cold requests drop from 17 to 3 (8 to 1 Decisions; 9 to 2 index queries),
and warm requests from 9 to 2 with no inference. Median elapsed time was
468 to 108 ms cold / 232 to 66 ms warm on macOS, and 389 to 78 ms cold /
224 to 49 ms warm in the two-CPU, 1-GiB ARM64 Linux VM. These synthetic delays
measure transport shape, not live-provider speed or model-token savings.
That historical qualification uses one parent. The follow-up below adds
cross-parent scheduling. Reproduce with `bench/visual_batches.py --help`; raw trial reports remain in ignored `output/`.


Cross-parent visual search now groups up to eight equal-shape parent projections,
coalesces Decisions across bounded worker lanes, and retains ordered output and
per-parent authorization/revision/digest checks. Asset-size rejection falls back
to individual parents; oversized per-parent field groups split adaptively. A
borrowed asset page covers one evaluation wave, then later waves authorize again.
Explicit finite budgets and early-selection modes retain serial admission; the
default request safety cap remains shared across every HTTP attempt. Active
parents can increase RSS: concurrency is a latency/throughput tradeoff, not a
claim that every workload uses less memory.

`bench/visual_batches.py --parents 8 --units 1` measures separate documents;
`--parents 20 --units 8` crosses pages and windows, and `--image-bytes 262144`
adds valid PNG payloads to exercise larger transfers. Five alternating paired
trials record exact output equivalence, HTTP counts, child CPU/wall time, first
stdout and peak RSS. Use pinned baseline/current binaries and write raw reports
to ignored `output/`. Live model tokens, prices and relevance remain unmeasured.

The [cross-parent qualification](bench/results/grep-cross-parent-summary.json)
records the final binaries and all four workload shapes. For eight separate
documents, total requests fall from 17 to 3 cold and 9 to 2 warm:

| Host | Cold median, baseline → current | Warm median, baseline → current |
|---|---:|---:|
| macOS ARM64 | 454 → 110 ms | 234 → 68 ms |
| Linux ARM64 VM, 2 CPUs / 1 GiB | 418 → 78 ms | 224 → 53 ms |

These are loopback fixtures with 20 ms artificial delay per HTTP request. With
256 KiB images, peak RSS rises from 7.8 to 16.9 MiB on macOS and 8.5 to 18.8 MiB
in the Linux VM. Cold tiny-input CPU time also rises from 11 to 29 ms in that VM.
Unranked first-output latency is similar or slightly worse despite lower total
time; results still arrive before later provider work finishes. The report
includes CPU, RSS and first-output figures rather than implying universal cost
or latency improvements. Live provider cost and relevance remain unmeasured.

The scalar source-proof cache now admits one cold entry with CLOCK eviction
instead of clearing all 4,096 entries, retaining 4,095 existing facts. Proof
lookup and registration take one lock per bounded window of up to 64 rows.
Epoch, owner and publication checks still fence reuse; a cached fact never
authorizes an asset or substitutes for a complete predicate certificate.


Cross-parent workers now refill as each lane completes, with a fixed 16-slot
ordered completion buffer and the existing global pending-output quota. Shared
asset responses retire after their last wave borrower. Ordinary parsed strings
borrow their retained response, removing a second base64-string allocation;
response bytes count toward the page memory limit. Image-wave scratch has a
shared 64 MiB conservative admission budget, separate from cumulative source
bytes and provider concurrency. This is an admission estimate, not an RSS cap.

Fallback asset reads use the invocation's request counter and cancellation token.
The default 1,000-request cap applies to all index and provider attempts across
lanes. Default-budget image searches can overlap a manifest page only when
conservative current-page retry/split headroom remains; explicit finite budgets
and text-stage searches keep serial metadata admission. Server-owned conditional
source identities use the generic [conditional projection contract](../../../docs/design/antfly-grep.md#conditional-source-projection-and-future-storage-authorities).
Warm descriptor and decision hits can omit image transfers after a current
permission/revision check. Changed values or unavailable scores materialize and
validate full bytes. Older servers retain full-value fallback. That qualification used the server reference path, which reads and hashes
selected values. The follow-up below adds bounded storage-owned scalar proofs.

The [rolling-evidence qualification](bench/results/grep-rolling-evidence-summary.json)
compares this implementation with the preceding cross-parent binary. In a
128-image workload (eight parents, sixteen 256 KiB images each), both use 17
index requests. Cold peak RSS falls from 154.4 to 58.6 MiB on macOS and 150.9 to
92.8 MiB in the two-CPU/1-GiB Linux VM. Warm peak RSS falls from 98.7 to 24.3 MiB
and 122.9 to 41.1 MiB respectively. Cold elapsed medians are 420→436 ms on macOS
and 489→432 ms in the VM; warm medians rise from 181→210 ms and 202→235 ms.
Memory admission therefore trades some warm latency for a smaller working set.
The slow-parent fixture improves cold elapsed time by about 6% on both hosts;
ordinary workloads and CPU/first-output metrics do not improve uniformly.
These fixtures impose 20 ms HTTP delays and synthetic constant scores; they
establish no live-provider token savings or universal performance guarantee.

Final validation passed 282/284 CLI cases on macOS and 281/284 in the Linux VM
(two/three platform skips), 24 media regressions, 25 real-server cases (seven
opt-in benchmark skips), package units, 31 packaging tests, 35 boundary-checker
regressions and Windows cross-compilation. The server integration uses the
previously frozen native server. Raw trials remain in ignored local output;
the compact report records binary hashes, CPU, RSS, first output and limitations.

Main was subsequently merged through `eb5145c3b5` (#1259). After that merge and
the additional failed-drain lifetime regression, the rebuilt final CLI passed
283/285 cases on macOS and 282/285 in Linux (two/three platform skips), including
25 media cases and Linux's wide-repository watcher under a 256-descriptor limit.
The merged native DB suite passed four tests with zero leaks; all five registry
cases and Windows cross-compilation passed. The report records these final
validation hashes separately; its performance measurements remain pinned to the
earlier measured binaries. Failed deferred drains join and discard remaining
borrowers before retiring any manifest page, including failures inside cleanup.

The [conditional-projection qualification](bench/results/grep-conditional-projection-summary.json)
compares the merged rolling-lane binary with bounded verified-image descriptors
and cross-manifest worker refill. Five alternating fixture rounds produced these
medians (baseline → candidate):

| Fixture | macOS elapsed | Constrained Linux elapsed |
|---|---:|---:|
| 128 large images, cold | 454 → 525 ms | 415 → 475 ms |
| 128 large images, warm | 229 → 205 ms | 210 → 239 ms |
| 24 single-row pages, slow first parent, cold | 1,872 → 998 ms | 1,767 → 922 ms |
| 24 single-row pages, warm | 720 → 674 ms | 690 → 658 ms |

For the large-image warm fixture, median CLI child CPU falls from 117.6 to
17.1 ms on macOS and 187.8 to 33.0 ms in Linux; peak RSS falls from 24.7 to
7.1 MiB and 40.2 to 16.8 MiB. Projected image bytes fall from 44,742,400 to
zero on macOS and 5,243,250 in Linux. The bounded best-effort descriptor cache
can lose facts to collisions or racing writes; those cases safely materialize
source again. Warm zero-transfer and lower elapsed time are therefore not
guaranteed. Cold large-image latency regresses, and tiny-image warm child CPU
increases. Both variants already make zero warm provider calls.

These are generated-image/constant-score loopback fixtures with 20 ms artificial
HTTP delays. Linux runs in a two-CPU, 1-GiB container in the existing VM on the
same physical Mac. The server fixture's hashing contributes to wall time but
not CLI child CPU. The native server used for those measurements reads and hashes
selected values; those measurements do not establish server CPU/storage savings,
live-provider token savings, or independent cross-host performance. The compact
report pins binary and driver hashes; raw trials and logs remain ignored.

Final qualification passed 285 Linux CLI cases (three platform skips), all
28 macOS media cases, 151 query-contract tests with zero leaks, and 27 native
server cases (seven opt-in benchmark skips). The native cases include single-
and two-shard delivery, warm omission, current-value changes, authorization,
revision and snapshot fences. SDK, MCP/OpenAPI, packaging and boundary checks
also pass. The broad macOS suite has an intermittent watcher assertion; affected
cases pass in isolation, and the inventory-visit failure reproduces on the merged
baseline. The report preserves this qualification and exact artifact hashes.


The [conditional planning qualification](bench/results/grep-conditional-planning-summary.json)
compares the reviewed PR binary with score-aware retrieval and bounded cache
writer serialization. Changed questions now request full grouped values directly;
warm hits retain the exact descriptors used to make their conditional requests.
Conditional response bytes and full-image processing scratch have separate
admission estimates, with the existing shared processing limit unchanged.

Five alternating rounds of the same 128-image fixture show these medians:

| Host | Changed-question elapsed | Warm elapsed | Warm CLI child CPU |
|---|---:|---:|---:|
| macOS ARM64 | 693 → 534 ms | 220 → 214 ms | 17.3 → 12.1 ms |
| Linux ARM64 VM, 2 CPUs / 1 GiB | 654 → 473 ms | 222 → 192 ms | 35.6 → 11.8 ms |

Changed-question index requests fall from 33 to 17. Warm requests fall from
17/18 to 10, with zero image bytes on both candidate hosts. Both baselines
already avoid warm inference. Cold CPU and peak RSS do not improve uniformly.
These constant-score fixtures impose 20 ms HTTP delays; child CPU excludes the
Python server. Linux shares the physical Mac. These results establish neither
live model cost savings nor a universal speed or memory guarantee.

The native source-only retrieval path now publishes bounded canonical-value
certificates atomically with storage writes. Current scalar leaves can be
omitted without loading or hashing their payloads, after fresh authorization
and publication checks. Certificates bind content, directory and canonicalizer
ABI; unsupported/stale cases use reference delivery and bounded repair. Other
selected fields and storage layouts can still require source reads. The metadata
and write hashing cost are explicit in the design, rather than implying free
reads or writes. `e2e/antfly/bench_source_projection.py` separately qualifies
native server ingestion, full delivery and conditional delivery without a model.


The same qualification measures the final native server after merging main
through `871c143788` (#1262), using five alternating rounds, eight fresh documents
with 256 KiB ASCII values, one shard, exact response hashes and mandatory
certificate-hit checks. Each candidate round records 16 certificate hits and no
full-value fallbacks. Median times:

| Native phase | Elapsed, baseline → current | Server process CPU, baseline → current |
|---|---:|---:|
| Ingestion | 56.36 → 66.24 ms | 64.69 → 80.22 ms |
| Full delivery | 15.21 → 17.36 ms | 12.42 → 14.83 ms |
| Conditional delivery | 10.29 → 2.16 ms | 8.89 → 1.09 ms |

Conditional delivery is about 79% faster and uses 88% less server CPU in this
synthetic corpus. Ingestion/full delivery regress about 14–24% in elapsed/CPU
terms. Conditional peak RSS is 152.5 → 149.3 MiB. The candidate also includes
main's CLI unification; this pair does not isolate that merge. The report retains
the separate pre-main feature qualification, where ingestion/full-delivery costs
were about 5–7%. Shared-host measurements and background work vary between runs.

Whole-process CPU includes background work, and sampled RSS is not an allocation
bound. These are fresh native JSON certificates on the shared Mac, with no
physical-I/O or live-model claim. Unsupported/stale layouts retain reference
execution. Proof reuse shifts work to bounded metadata and writes; authorization
and remaining selected source reads still have a cost.

Final validation: 288 CLI cases on each host (286 macOS / 285 Linux passed;
two/three platform skips), 149 query contracts, 15 identity/publication/snapshot
cases, 91 logical-field cases plus one skip, and 27 native integration cases plus
seven opt-in benchmark skips. Contract tests report zero leaks. The real storage
regression verifies published certificates, retained read cuts and replacement
visibility; parsed ID-query worker round trips exercise the actual eligible
request shape. Storage discovery, compatibility, license boundaries and Windows
CLI cross-compilation pass. Raw trials and logs remain ignored.

After the main merge, 149 query contracts, 15 identity/storage cases, 91 logical
field cases (one skip), 27 native cases (seven opt-in benchmark skips), four
release-packaging checks and the Apache boundary pass. Rebuilt macOS, Linux and
Windows CLI binaries are byte-identical to the fully tested binaries above.
Main's unified CLI and Apache licensing are preserved; the compact report records
the final native binary and both pre-/post-main measurement sets separately.


The final main refresh through `2aed23bb0e` (#1264/#1265) is merged in
`5447f58f3a`. Both 288-case CLI suites pass again, along with shared PDF/mock OCR
integration, 149 query contracts, 15 identity/storage cases, 91 logical-field
cases (one skip), generated error-name checks and both error-transport tests.
Windows CLI cross-compilation and the Apache boundary pass. Relevant query/storage
source hashes and macOS/Linux CLI bytes match the qualified build. Native server
integration and timings remain pinned to the recorded `9ca2e6e28f` build; the
compact report distinguishes this final targeted validation.


The generic conditional-source follow-up (`611a4ce4ca`, main merged through
`c1bc94b174`) uses an explicit canonicalization ABI with fixed compatibility
vectors, collects identities during directory publication, and batches owned
metadata within one pinned transaction. Equal omission masks share byte-bounded
source reads. Admission accounts for coalesced range retention or full primary
sizes according to backend capabilities; each subwindow closes its scope before
the next opens. One large row can progress, and point reads reuse their metadata
scope. CLI shape buckets fetch concurrently through the existing worker pool,
with proportional reservations under one page-memory budget.

The [generic qualification](bench/results/grep-generic-conditional-summary.json)
records six client controls and thirteen native cases: ASCII, escaped/Unicode,
numeric, wide, opaque, small and mixed values; 1/8/64 rows; and exact, parent,
wildcard and exclusion masks. Native cases use five alternating trials and ten
individually timed reads per phase, with parsing outside timings and every
response checked. Ordinary reads and fallback layouts are controls, not omitted
from the results. Raw reports/logs are local and unpublished under `output/`.

Mixed-shape client requests fall from 10 to 4; dense warm-image requests fall
from 18 to 12. Warm client CPU/latency improve in these fixtures, while tiny cold
client CPU can increase. Native gains are modest and uneven: eight-row ASCII
conditional delivery is 1.086 → 0.983 ms; the single-row control is 0.662 →
0.738 ms and mixed exact delivery is 1.350 → 1.490 ms. These are workload and
host-dependent tradeoffs, not a universal throughput, memory or zero-cost claim.
Native caches are warm; the Linux VM shares the Mac host and has two CPUs/1 GiB.
The report preserves all medians, binary/corpus hashes and reproduction commands.

Final checks pass: 287 macOS CLI cases plus two skips; 286 Linux VM CLI cases
plus three skips; 149 query, 18 identity/storage and 94 logical-field contracts
(one skip, zero leaks). The eighteen focused storage tests also pass in the
Linux VM, including full-value range adapters, scope retirement, mixed masks,
large-row progress and cancellation/allocation failures. Build them portably
with `zig build source-value-identity-test-build -Dtarget=aarch64-linux-gnu`
from `zig/`. All 28 real-server indexed tests pass (seven opt-in benchmark skips),
including persisted certificate upgrade from the previous server. Windows CLI
cross-compilation, compatibility/storage discovery and the Apache boundary pass.
The final source rebuild produces the same qualified native binary hash.

### Bounded materialization and decision request bodies

Conditional materialization keeps reconstructed rows in reclaimable subwindow
scratch. Complete primary values borrow their pinned read scope until projection
finishes; the existing owned logical-read API retains its ownership contract.
One selector serves each window, and small overlays restore omitted ancestors
without decoding retained payloads into a second JSON tree. The public encoder
streams finalized sources while preserving source-dependent hierarchy metadata,
private-field removal, duplicate-key rejection and number normalization.

Strict preparation produces a receipt bounded to immutable source bytes. The
span writer copies strings whose spellings already match the default JSON
encoder, including canonical control escapes. Noncanonical escapes, numbers and
unqualified input retain normalization. This receipt is temporary syntax evidence;
it grants no document access or cross-source reuse.

Remote decision requests borrow qualified image URLs of at least 4 KiB through
HTTPx body segments. Smaller image, text-only and local-runtime requests use the
ordinary contiguous encoder, allocating no segment plan. Exact admission counts
known large URL lengths without serializing those payloads into a counting sink.
Inputs stay pinned through transport completion, cancellation and retries. The
wire schema, request/response limits and inference budgets are unchanged.

Metadata rewrites can reuse canonical leaf certificates from the writer's current
snapshot when decoded ancestry, leaf kind, raw length/digest and canonical ABI
match. Unsupported layouts retain the fused reference writer. The incoming body
still needs validation and its content digest, and the primary JSON row is still
written. The [design](../../../docs/design/antfly-grep.md#conditional-source-projection-and-future-storage-authorities)
specifies the separate immutable-value/manifest migration needed to remove that
write amplification, including retained ownership and rollback requirements.

The [materialization qualification](bench/results/grep-materialization-summary.json)
compares these changes with PR head `0f28b974b9` using five alternating client
trials on tiny and 256 KiB images. For 128 large-image decisions, median client
CPU falls 195.0 → 133.2 ms on macOS and 276.5 → 193.9 ms in the constrained
Linux VM. Median peak RSS falls 58.5 → 33.6 MiB and 88.5 → 45.0 MiB,
respectively. Tiny cold-image CPU is 47.5 → 48.2 ms on macOS, retaining the
ordinary encoder. Warm score hits make no provider calls. These are fixture
measurements; request counts, transfer bytes and model billing are not reduced
by borrowing the body. The VM shares the same physical host; elapsed-time
measurements are sensitive to concurrent host work.

Raw trials and logs remain local under ignored `output/`; the compact report
preserves binary/source hashes, controls, medians and reproduction commands.

Focused validation passes 24 identity/storage, 151 API contract and 96 logical-field
tests (one skip), with zero leaks. The CLI suites pass 287 macOS cases (two
skips) and 286 cases in the constrained Linux VM (three skips). The storage suite
covers arena retention, borrowed/owned read lifetimes, current-snapshot identity
reuse, changed ancestry, incompatible certificates and allocation failures.
Request-body tests cover all decision transports, exact wire bytes, URL-proof
invalidation, sizing and cancellation-compatible borrowed lifetimes.

The thirteen native cases use three alternating trials, five individually timed
reads per phase and two verified metadata rewrites per trial. Eight-row full-source
latency is 10.74 → 9.54 ms for ASCII, 25.41 → 19.76 ms for escaped strings,
and 24.24 → 19.13 ms for Unicode. Escaped-value metadata-update CPU falls
152.44 → 137.20 ms; primary JSON is still physically rewritten. Controls remain
mixed: numeric full-source delivery is 0.92 → 1.07 ms, while the single-row
metadata-update CPU control is 17.49 → 18.03 ms. Ordinary reads, conditional
hits, opaque/small values, 1/8/64 rows and parent/wildcard/exclusion masks all
remain in the report. These warm-cache, shared-host results establish measured
tradeoffs, not universal latency or zero-cost guarantees.

All 28 real-server indexed integration cases pass against the final native binary
(seven opt-in benchmark skips). The 24 identity/storage regressions also pass in
the constrained Linux VM, including canonical-string receipt activation and scope
boundaries.


### Experimental storage format is deferred

PR #1263 keeps the existing document storage format. Immutable payloads, field
publication packs and automatic migration are preserved separately on the
`experiment/immutable-document-format` branch pending mixed-workload
qualification. Format-independent analysis caching and FST cleanup require no
storage upgrade. See the [split decision](../../../docs/design/antfly-grep.md#format-migration-split-2026-10-26)
for the measured tradeoffs and upgrade criteria.

For indexed images, `--indexed-media-transport=source` targets the Antfly inference
gateway. The CLI obtains fresh authorized value identities and sends table,
document, field, revision and content-hash references instead of downloading and
resending image bytes. The gateway rechecks authorization, revision, identity
and content before provider delivery. Expanded-image admission limits still
apply; a reference is not a read grant. The default inline transport remains
available for ordinary decision endpoints. This uses the existing document
format.

### General conditional projection

Indexed image reuse uses the ordinary query API's `select` expression map and
`params`, with pure expressions returned in `_computed`. `fields` stays a list
of strings and controls `_source` independently. `value_identity`, `is_missing`
and lazy CASE lower to the existing SQL scalar binder and VM. The unreleased
`source_projection` / `_source_projection` protocol is removed. Servers that
reject or ignore `select` fall back to ordinary image reads; missing computed
values never count as verified cache hits. Source-reference transport requires
an identity-capable server.

The inference gateway batches images within a parent, then batches parents with
identical masks and filters. It preserves authorization, revision and digest
checks per reference, and temporary lookup failures retain retryable statuses.
See [the design](../../../docs/design/antfly-grep.md#conditional-source-projection-and-future-storage-authorities)
for the API, limits, metadata fast path and fallback rules. Historical source
projection reports above predate this interface. The updated
`e2e/antfly/bench_source_projection.py` compares ordinary, cold, metadata-backed
warm, and equivalent value-reading reference expressions; optional comparison
binaries must support `select`.

Cloud catalog queries return an authenticated policy proof for snapshot traversal.
A selected source's first successful ordinary content query returns its own
`X-Antfly-Source-Access` proof and `X-Antfly-Source-Publication` fence. The server
rechecks the job attempt and catalog publication after snapshot acquisition and
before releasing any rows. Grep requires and pins both headers across
continuations, asset reads and session renewal. Unrelated source sync/rename does not invalidate an ongoing scan.
A selected-source sharing, principal/group or effective row-policy change ends the
search explicitly as incomplete.

Portable source profiles use [sources-v2.json](schema/sources-v2.json), independently
of Cloud login. `antfly-grep --source-catalog catalog.json --source manuals PATTERN`
loads an explicit endpoint, physical table bindings, source filters and readiness;
`ANTFLY_GREP_TABLE_TOKEN` supplies its separate data-plane credential. This path
never invokes `antfly-cloud`. Pending, syncing, stale and expired bindings fail
explicitly. A catalog configures search and does not grant server access.

The public [ingestion module and standalone runner](../../../go/pkg/sources/README.md)
can produce this catalog from GitHub, Drive Docs/Slides, S3 or GCS without Colony.
It uses the same provider/normalization components as hosted imports and can run
from cron or another scheduler; synchronization is not embedded in grep.

Cloud catalog discovery uses the same ordinary `antfly_sources` table queries.
Both Cloud and self-hosted search use ordinary `/tables/{table}/query` requests.
Cloud applies `X-Antfly-Source-ID`, `X-Antfly-Source-Access` and
`X-Antfly-Source-Publication` preconditions on that route to enforce the current binding, access and readiness. These are Cloud
preconditions, not new Antfly database APIs or authorization capabilities.

Discover the same source catalog directly from a self-hosted Antfly table:

```sh
antfly-grep --source-endpoint https://antfly.example/db/v1 \
  --source manuals 'retry budget'
```

The default ordinary catalog table is `antfly_sources`; override it with
`--source-table NAME`. Authenticate with the normal table token and use
`--source-principal ID` for additional local reader filtering when needed.
The ingestion identity publishes versioned source records after content and
reconciliation become query-visible. Grep traverses one bounded catalog snapshot
before searching. Catalog and content visibility require server authorization;
catalog records grant no authority. `--source-catalog FILE` remains available for
file exports. See the [source record schema](schema/source-record-v1.json).

Cloud source configuration and sync actions use existing Colony import-job
endpoints with `mode: source`. There is no separate Cloud `/sources` API. The
Cloud session adapter pins authenticated query identity and catalog/source policy
headers across pagination, asset reads and token renewal; the gateway enforces
current row permissions.

Projection delivery uses the same numeric semantics for direct fields, aliases,
CASE arms and typed JSON operations, including nested numbers and underflow.
Request-owned programs and unconditional read sets avoid repeated binding; CASE
branches remain lazy. Immutable completed-output receipts avoid repeated
validation/copying, and opaque identity hashing streams without a payload DOM.
Media lookup uses at most four concurrent groups under the existing request
budget; identical payload inspection is shared only within the request, while
claims remain checked per reference.

The [bounded-reuse qualification](../../../docs/design/antfly-grep.md#release-qualification-for-the-bounded-reuse-follow-up-2026-10-27)
records source/media controls, hashes, costs and reproduction commands. Large
computed reads use 27–32% less CPU in those paired runs. Distinct-source media
latency improves with higher sampled RSS; small writes do not uniformly improve.
The server comparison includes newer main changes and is not an isolated or
universal performance guarantee. Document format and canonical ABI are unchanged.


The [alias and media reuse qualification](../../../docs/design/antfly-grep.md#release-qualification-for-alias-and-media-reuse-2026-10-28)
records the current production binary, paired controls and repeated observations.
Escaped aliases use 42% less server CPU and alias chains 34% less; larger full
source projections use 16–36% less. Small projections are near parity, and media
CPU/latency gains vary by case, with increases retained in the report. These are
warm synthetic whole-server comparisons against an older main revision on one
shared host. They do not establish universal or zero-cost behavior. Document
format and canonical ABI remain unchanged.


The [query-read follow-up](../../../docs/design/antfly-grep.md#measured-query-read-optimizations-after-1263)
qualifies required-ID owner routing and projection preparation against the merge
of #1263. Five alternating 50-read trials on a shared M4 Max reduce eight-shard
clustered-ID read CPU about 79–85% and spread-ID read CPU about 41–49%, with equal
values, scores and counts. Initial short-run latency and other controls vary;
these are warm synthetic measurements, not universal guarantees. Canonical
fingerprinting avoids a payload-sized allocation using a fixed 4 KiB buffer.
Earlier range-batch admission and direct parent media allocation experiments were left
out after their CPU/memory gains failed to hold consistently. The design includes
both experimental sets, final controls, hashes and reproduction commands. Raw
JSON reports/logs stay ignored. Public query syntax and ASCH25 remain unchanged.


The [selected-range ownership design](../../../docs/design/antfly-grep.md#selected-range-ownership-and-shared-window-plans)
separates retained output from physical I/O cost. Native LSM range results retain
selected copies while temporary whole-value results retire after each document.
Projection windows reuse both selected-read and full-read plans. Gateway media
jobs transfer request-local payload leases while releasing query scratch at
completion; repeated references to one field can share the retained bytes.


The [current ownership and transport qualification](../../../docs/design/antfly-grep.md#batch-cancellation-and-structured-native-media-2026-10-30)
records the contracts, complete paired controls, independent repeats and artifact
identities. Selected LSM reads borrow one admitted point until selected spans
are copied. Controlled batches retain one bounded scratch owner while polling
cancellation/deadlines between whole-value reads and retiring payload pins per
document. The persisted-row fixture uses 32.3% fewer scratch allocations.

Gateway media jobs transfer request-local payload leases and release completed
parsing scratch. Compatible native generators/deciders reuse verified codec
bytes. Legacy hints supplementing a retained URL keep a 1 MiB soft cap;
compact native payloads replace that URL and stay within the existing expanded
request ceiling (at most 12 MiB of codecs). Structured generation adapts message
headers before serialization and passes borrowed attachments through the shared
native handler, preserving options, tool history, token budgets, usage,
completion and request controls. Older linked runtimes keep the JSON fallback;
worker protocol 8 requires a matching explicitly configured worker CLI.

The isolated structured-transport tool allocates 9,913 bytes per invocation for a
1 MiB synthetic payload versus 8,402,713 through JSON/framing (99.88% less). It excludes
source verification, caller-retained inputs, fixed dispatch, process transfer
and model work. The 44-control whole-query matrix remains mixed/mostly flat;
the design records the repeated wide conditional-read regression alongside the
gains. These results do not establish universal latency, RSS or provider-cost
improvements.

Physical-read probes still fetch complete checksummed/compressed blocks.
Selected retention is not sparse storage I/O. Public HTTP/query syntax, the
document schema and ASCH25 remain unchanged; this follow-up introduces no
persistent-format migration. Earlier qualification runs and rejected experiments
remain in the linked design history.


The [source-codec and payload-sharing contract](../../../docs/design/antfly-grep.md#bounded-source-codecs-and-shared-worker-payloads-2026-10-30)
keeps compatible native source images as verified codecs and small internal
handles, rendering expanded URLs only for legacy/hosted consumers. Logical
request, image and token limits remain unchanged. Workers transfer each shared
immutable payload once and retain separate logical references. Copy-backed
projection batches reject ineligible pairs before opening scopes or planning
spans. These are request-local changes with no document-format migration.


The same-main follow-up qualifies 35 whole-server controls and independent
seven-round repeats, with response-equivalence checks. Attribution confirms 56
ineligible pairs rejected before any Selection planning, while eligible batches
still run. The isolated source-preparation fixture allocates 67–97% fewer bytes;
eight shared 1 MiB worker payloads use about 87% fewer wire bytes. These scoped
results exclude model work and do not establish universal query speed, RSS or
billing improvements. Initial and repeated slower cases remain in the design.

The final merge of main through `0ff73ed56f` rebuilds byte-identical macOS server
and grep artifacts. Indexed HTTP integration passes 57 tests (7 optional
profiling skips); native fixture, allocation/wire and Linux VM scopes are
recorded in the linked design. Full PR CI and required review remain separate.
