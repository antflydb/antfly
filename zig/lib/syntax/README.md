# Native source grammars

`src/grammar.zig` executes the complete pinned Go, Python, and Rust LR grammars
with native Zig GLR parsing and native lexical automata. Python indentation,
formatted strings, Rust raw strings, and nested comments use native scanners.
Normal builds require no C/Rust compiler, parser generator, or foreign runtime.
Grammar tables are immutable, compact, and shared across workers.

Parsing uses persistent stacks, a frontier ordered by source position, exact
stack verification on hash collisions, memoized configurations, deterministic
ambiguity selection, and a hard allocator budget (64 MiB by default). The API
reports incomplete syntax or budget exhaustion; grep uses bounded text chunks
when complete syntax analysis is unavailable. A grammar parser does not expand
Rust macros or perform Go type resolution. Go generic invocations remain calls
until semantic information can distinguish conversions.

Run `zig test lib/syntax/src/grammar.zig` from `zig/`, or `zig build
-Dgrep-only=true grep-unit`. Corpus tests compare normalized named trees from
65 Go, 115 Python, and 148 Rust valid upstream cases. Nonsemantic comments and
line continuations are omitted from comparison. Two ambiguous upstream snapshots
use compiler-backed choices: CPython's starred attribute/call binding and Go's
CallExpr for generic invocations. Go builtin tokens also admit ordinary call
syntax, covering shadowed builtins and Go 1.26 `new(expression)`. These normalization rules are explicit in the
generator. Invalid input and memory admission have separate tests.

Pinned upstream sources (MIT):

- Go `1547678a9da59885853f5f5cc8a99cc203fa2e2c` (0.25.0).
- Python `293fdc02038ee2bf0e2e206711b69c90ac0d413f` (0.25.0).
- Rust `77a3747266f4d621d0757825e6b11edcbf991ca5` (0.24.2).

For maintenance, extract those releases into a directory containing
`tree-sitter-LANGUAGE-VERSION`, then run:

```sh
python3 lib/syntax/tools/generate_grammars.py /path/to/sources
zig fmt lib/syntax/src/grammars
zig test lib/syntax/src/grammar.zig
```

The generator verifies the upstream parser hashes before translating lexical
automata and packing LR tables. Update reviewed embedded asset identities after
regeneration. Full upstream MIT notices are retained in generated source,
scanner adaptations, the third-party notice bundle, and canonical license files.

`grammar_check.zig` is a maintenance utility for real-file acceptance, elapsed
time, and allocator peak measurements:

```sh
zig build-exe lib/syntax/src/grammar_check.zig -OReleaseFast -femit-bin=/tmp/grammar-check
/tmp/grammar-check path/to/file.go path/to/file.py path/to/file.rs
```

JavaScript/TypeScript use the separately pinned native Yuku parser exposed by
`src/root.zig`; Zig uses `std.zig.Ast` in grep's source index.
