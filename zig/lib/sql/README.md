# Antfly SQL Parser Foundation

`lib/sql` owns the storage-independent SQL scanner and parser foundation. It
has no catalog, storage, API, or runtime dependencies, so command-line tools,
servers, and tests can share one lexical and syntax contract.

The stable low-level pieces are exported from `root.zig`:

- `lexer` produces source-spanned tokens and structured lexical diagnostics.
- `token` owns keyword recognition and PostgreSQL keyword categories.
- `parser` provides the minimal token cursor used while generated parsing is
  integrated incrementally.

The generated parser facade remains private until its imported grammar
conflicts are resolved deliberately. Its user-facing boundary is
`parseSqlResultAlloc`: one lex/parse attempt returns either success or a
source-aware diagnostic. Callers should use that result API instead of parsing
once and reparsing failures for diagnostics.

The common pre-tokenized path uses fixed stack buffers and performs no parser
allocations. Long statements fall back to allocator-backed buffers. Syntax
diagnostics allocate only their owned expected-token list and actual token;
lexical diagnostics are allocation-free.

Useful commands:

```sh
zig build sql-parser-test
zig build sql-parser-bench -- --mode all
zig build regen-sql-grammar
zig build sql-grammar-generated-check
```

See `grammar/GRAMMAR.md` for grammar ownership and compatibility policy.
