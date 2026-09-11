# GLiNER2.5 bounded regex validators

The native version 2 schema supports regex filters on entity definitions and
record fields. The request executor must install the same request-local regex
context in schema compilation and result decoding. Unsupported syntax fails
before inference. This document defines the validator contract; model/backend
qualification and availability remain separate release gates.

```json
{
  "entities": ["identifier"],
  "entity_definitions": {
    "identifier": {
      "validators": [
        {"pattern": "[A-Z]{2}-\\d{4}", "mode": "full", "flags": 2}
      ]
    }
  }
}
```

`mode: "full"` requires the entire extracted value to match. `mode: "partial"`
searches anywhere within the value. The default is `full`. `exclude: true`
inverts the result. All validators on a value must pass. Matching uses original
UTF-8 text, without tokenizer normalization. No captures or match offsets are
returned by a validator.

Enum fields validate the canonical declared choice. A literal mention can supply
document offsets, but its casing does not change the value being validated.
Disallowed choices are removed before record assignment and from every fallback
path. An explicitly required field that cannot retain a valid choice raises a
typed required-field error. This corrects the pinned upstream decoder, which
can bypass validators for schema-prefix enum choices and literal overrides.

## Supported syntax

The boolean matching semantics are pinned to Python 3.12 and Unicode 15.0.0:

- Literal Unicode characters, escaped punctuation, dot, character classes,
  negated classes, and character ranges.
- Concatenation, alternatives, ordinary capturing groups, and `(?:...)` groups.
- `*`, `+`, `?`, `{n}`, `{n,m}`, `{n,}`, `{,m}`, and `{,}` repetitions. Lazy
  repetitions are accepted because they preserve boolean match results when
  capture-dependent constructs are absent.
- `\d`, `\s`, `\w`, and their uppercase complements. Unicode `\d` means
  decimal digits, while `\w` follows Python's Unicode word-character rules.
- `^`, `$`, `\A`, `\Z`, `\b`, and `\B`. `$` also matches before a final
  newline; `\Z` requires the absolute end. Python 3.12's `\B` does not match an
  empty input.
- Standard control escapes, octal character escapes, `\xHH`, `\uHHHH`, and
  `\UHHHHHHHH`.

The numeric `flags` field is a bitmask. Its default is `2` (`IGNORECASE`).

| Flag | Value | Behavior |
| --- | ---: | --- |
| `IGNORECASE` | 2 | Unicode case equivalence; ASCII-only when combined with `ASCII` |
| `MULTILINE` | 8 | `^` and `$` also recognize newline boundaries |
| `DOTALL` | 16 | Dot also matches a newline |
| `UNICODE` | 32 | Explicit Unicode semantics, which are already the default |
| `VERBOSE` | 64 | Ignore unescaped ASCII whitespace and `#` comments outside classes |
| `ASCII` | 256 | ASCII character properties, boundaries, and case matching |

Combining `ASCII` and `UNICODE` is invalid. Unicode case matching follows regex
equivalence, not string case folding: `ss` does not match `ß`.

Lookaround, backreferences, conditional groups, named groups, inline flags,
comment groups, atomic groups, possessive repetitions, named Unicode character
escapes, and locale/debug/template flags are explicitly unsupported. They are
never approximated or delegated to an unbounded regex engine. Invalid Python
syntax is rejected separately from unsupported constructs.

## Resource and failure contract

Compilation produces an immutable Thompson NFA. Matching streams Unicode
codepoints through bounded scratch storage; it does not backtrack or grow a
lazy DFA cache. Generation stamps avoid clearing every compiled state when few
states are active. Nullable repeated groups terminate without recursion.

Default engine limits are 4,096 UTF-8 pattern bytes, 4,096 AST nodes, 4,096 NFA
states, depth 64, repetition bounds of 1,024, and 32,768 charged class ranges.
Compilation allows two million work steps per pattern. A request-local context
additionally limits unique patterns to 128, total pattern bytes to 64 KiB, total
NFA states to 32,768, and total compilation work to eight million steps.

Each match allows one MiB of UTF-8 text and ten million work steps. The request
context limits aggregate match work to forty million steps. Service settings
can impose smaller limits. Work steps measure bounded parser/NFA operations;
the request's cancellation and deadline controls remain authoritative.

Pattern and state limits, matching exhaustion, cancellation, malformed UTF-8,
and allocation failure propagate as errors. An exhausted search is never
reported as a non-match. The version 2 HTTP adapter maps invalid syntax to
`INVALID_EXTRACTION_REQUEST`, unsupported syntax/flags to
`UNSUPPORTED_EXTRACTION_FEATURE`, and validator resource exhaustion to
`EXTRACTION_LIMIT_EXCEEDED`. Requests remain atomic.

## Verification and integration

`scripts/gliner25/capture_regex.py` captures expected results with the pinned
Python 3.12 oracle, without loading a model. `testdata/gliner25/regex.json` records
full-match, search, and prefix-match results, together with the complete Unicode
15 decimal-digit ranges. Native tests compare all cases and every Unicode scalar
against that digit inventory. Additional tests cover unsupported syntax,
compiler expansion limits, nonlinear-looking repetition, cancellation,
request-wide budgets, and allocation-failure cleanup.

The request owner initializes `extraction_regex.Context`, applies
`context.compilerOptions(...)` while compiling all item schemas, and sets
`pipeline.Options.regex_context = &context` with
`validate_value_fn = extraction_regex.Context.validateValue`. The context must
remain at a stable address and outlive request execution. Compiled patterns are
cached by exact pattern and flags; `mode` and `exclude` remain per-validator
choices. A context is request-local and must not be shared between concurrent
executions.
