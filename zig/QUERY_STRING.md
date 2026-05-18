# Query String Language

This document describes the Lucene-style query string syntax supported by
[query_string.zig](pkg/antfly/src/search/query_string.zig).

The parser produces the internal `Filter` AST, which is then converted into the
typed DB full-text query model by
[public_query_string.zig](pkg/antfly/src/api/public_query_string.zig).

The design goal is a focused, typed subset:

- common full-text query-string syntax is supported
- structured query execution still happens through typed query/filter nodes
- this is not intended to be Elasticsearch/Lucene query-string parity

### Default Behavior

- default field: `_all`
- default operator: `AND`
- configurable parser default operator: `AND` or `OR`
- empty input parses as `match_all`

Examples:

- `foo bar`
  - default `AND` => both terms required
- with default operator `OR`
  - either term may match

### Supported Syntax

#### Terms

- `hello`
- `title:hello`

These produce term filters.

#### Phrases

- `"hello world"`
- `title:"hello world"`

These produce phrase filters.

#### Phrase Slop

- `"hello world"~3`
- `title:"hello world"~2`

This sets phrase `slop`.

#### Boolean Operators

- `foo AND bar`
- `foo OR bar`
- `NOT foo`
- `+required`
- `-excluded`
- `(foo OR bar) AND baz`

Notes:

- explicit `AND` / `OR` behave as expected
- `NOT foo` is implemented as `must_not(foo)` with an implicit `match_all`
- `+` marks a required clause
- `-` marks an excluded clause

#### Field Groups

- `title:(foo bar)`
- `title:(foo OR bar)`

The field is applied to the grouped subquery.

This is equivalent to:

- `title:foo AND title:bar`
- `title:foo OR title:bar`

depending on the group contents and default operator.

#### Prefix

- `pre*`
- `title:pre*`

This is prefix syntax, not generic wildcard syntax.

#### Regex

- `title:/foo.*/`

Regex is field-scoped.

#### Fuzzy

- `title:~term`

This is field-scoped fuzzy term syntax.

#### Boosts

- `hello^2`
- `"hello world"^4`
- `"hello world"~3^2`
- `(foo OR bar)^5`
- `age:[10 TO 20]^3`

Boosts are carried through into the typed full-text query model where supported.

#### Inline Ranges

##### Numeric

- `age:[10 TO 20]`
- `age:[10 TO 20}`
- `age:[10 TO *}`
- `age:{* TO 20]`

##### Date

- `created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]`

Dates currently use UTC `Z` timestamp literals.

##### Term

- `title:[alpha TO omega]`

Range delimiters:

- `[` inclusive lower bound
- `{` exclusive lower bound
- `]` inclusive upper bound
- `}` exclusive upper bound

`*` means unbounded.

### Mapping to Typed Query Forms

The query string is parsed to `search.Filter`, then converted to typed DB query
shapes roughly as follows:

- term => `match`
- phrase => `match_phrase`
- prefix => `prefix`
- regexp => `regexp`
- wildcard => `wildcard` when present in the filter AST
- fuzzy => `fuzzy`
- numeric range => `numeric_range`
- date range => `date_range`
- term range => `term_range`
- bool filter => `bool_query`

### API Surfaces

Current API usage is primarily through query-string input fields that eventually
flow into the parser helper in
[query_contract.zig](pkg/antfly/src/api/query_contract.zig).

Supported behavior at that layer:

- normal query-string parsing
- boosts
- slop
- inline ranges
- field groups
- default operator override if the incoming query-string request shape includes
  `default_operator`

Accepted `default_operator` values:

- `and`
- `or`

case-insensitive

### Current Limits

This parser intentionally does not try to cover the full Elasticsearch/Lucene
surface.

Notable gaps:

- no Lucene-complete escaping parity
- no multi-field expansion like `title,body:(foo bar)`
- no `simple_query_string` forgiving mode
- no query-string-native `minimum_should_match` syntax
- no inline query-string support for the wider Elasticsearch query families
  like `dis_max`, `function_score`, span queries, nested queries, or scripts
- wildcard support is not a general Lucene wildcard grammar; the parser’s
  explicit shorthand is prefix `*`

### Examples

Basic:

```text
body:alpha AND title:"beta gamma"
```

Boosted:

```text
body:alpha^2 AND title:"beta gamma"~3^4
```

Grouped:

```text
title:(alpha OR beta) AND status:published
```

Ranges:

```text
score:[10 TO 20}
created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]
title:[alpha TO omega]
```

### Guidance

Use query strings for compact user-entered search syntax.

Use structured typed queries when:

- you need exact control over query shape
- you want stable machine-generated requests
- you need features beyond the query-string subset described here
