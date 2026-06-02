# Algebraic Sparse-Token Database Theory

This note sketches a database design where records are represented as sparse
symbolic tokens, query plans compile into algebraic transforms, and aggregations
are evaluated as folds over algebraic structures chosen for each operator.

The core idea is not to force database operations into group theory alone.
Groups are useful for reversible updates, but ordinary database work also needs
monoids, semirings, lattices, vector spaces, and sparse tensor operations.

## Thesis

A database can model its state as a sparse formal vector:

```text
database state: sparse vector / formal sum of tuples
query plan:      composition of algebraic transforms
aggregation:     fold into a monoid, group, semiring, or lattice
updates:         delta vectors
```

The useful research direction is:

```text
Build a query engine whose intermediate representation is an algebraic program
over sparse formal vectors, where each operator advertises algebraic laws that
the optimizer uses for incremental execution, factorization, and physical layout
selection.
```

## Sparse Tuple Space

Each logical tuple is represented as a basis token:

```text
t1 = basis("users", id=1, country="US", age=34)
t2 = basis("users", id=2, country="DE", age=28)
```

A table is a sparse formal sum of those tokens:

```text
Users = 1*t1 + 1*t2 + ...
```

For multiset semantics, coefficients represent multiplicity:

```text
Users = 2*t1 + 1*t2
```

For update semantics, inserts and deletes are deltas:

```text
delta  = +1*t3 - 1*t1
Users' = Users + delta
```

If coefficients live in the integers, this part is genuinely group-like:
inserts and deletes are additive inverses.

## Creating Sparse Tokens and Vectors

Sparse tokens are created by defining a basis space and mapping each database
fact into one or more basis coordinates.

At the simplest level, a token is a stable symbolic key:

```text
basis("users", id=42, country="US", age=34)
```

Internally, the engine can encode that key as a sparse vector coordinate:

```text
hash("users|id=42|country=US|age=34") -> coefficient 1
```

The physical vector is then a sparse map:

```text
coordinate_id -> coefficient
```

For example:

```text
{
  token_abc: 1,
  token_def: 1,
  token_xyz: 1
}
```

Each token is an implicit basis vector, and the coefficient describes its
presence, multiplicity, weight, or delta.

### Row Tokens

A row token represents a full tuple:

```text
basis("users", id=1, country="US", age=34)
basis("users", id=2, country="DE", age=28)
```

This is closest to exact relational semantics:

```text
Users =
  1*basis("users", id=1, country="US", age=34)
+ 1*basis("users", id=2, country="DE", age=28)
```

Row tokens are useful for identity and exact reconstruction, but they are too
opaque for many query operations if used alone.

### Attribute Tokens

An attribute token represents one atomic column fact:

```text
basis("users.id", row=1, value=1)
basis("users.country", row=1, value="US")
basis("users.age", row=1, value=34)
```

A row becomes a small sparse vector of facts:

```text
row_1 =
  1*basis("users.id", row=1, value=1)
+ 1*basis("users.country", row=1, value="US")
+ 1*basis("users.age", row=1, value=34)
```

Attribute tokens are the most important representation for relational
execution. They make filters, projections, joins, indexes, partial updates, and
grouping keys visible to the engine.

### Feature Tokens

A feature token represents a derived or approximate property:

```text
basis("country=US")
basis("age_bucket=30_39")
basis("active=true")
```

For example:

```text
user_1 =
  1*basis("country=US")
+ 1*basis("age_bucket=30_39")
+ 1*basis("plan=pro")
```

Feature tokens are useful for approximate search, vector indexes, machine
learning features, and hybrid symbolic/vector execution. They should usually be
derived from exact row or attribute tokens rather than replacing them.

### Layered Token Model

A practical engine should use multiple token layers:

```text
row token:       exact identity
attribute token: queryable facts
feature token:   derived/index/search representation
```

For example:

```text
row_id = basis("users.row", id=1)

facts =
  1*basis("users.country", row=1, value="US")
+ 1*basis("users.age", row=1, value=34)
+ 1*basis("users.plan", row=1, value="pro")

features =
  1*basis("country=US")
+ 1*basis("age_bucket=30_39")
+ 1*basis("plan=pro")
```

The database state can store all of these as sparse maps:

```text
{
  basis("users.row", id=1): 1,
  basis("users.country", row=1, value="US"): 1,
  basis("users.age", row=1, value=34): 1,
  basis("users.plan", row=1, value="pro"): 1
}
```

### Canonical Token Encoding

A token should usually include:

```text
namespace or table
attribute name
row identity or entity identity
value
type tag
optional version or time
```

For example:

```text
Token {
  space: "users.country",
  row: 1,
  value: "US",
  type: "string"
}
```

The engine should encode this into a deterministic canonical form:

```text
users.country|row=1|type=string|value=US
```

Then it can assign a compact coordinate:

```text
coordinate_id = hash128(canonical_token)
```

The sparse vector stores only:

```text
coordinate_id -> coefficient
```

For exactness, the engine should retain a symbol table:

```text
coordinate_id -> canonical token
```

This allows compact execution while preserving debuggability, reversibility,
and collision handling.

### Coefficients

The coefficient attached to a token depends on the chosen semantics:

```text
+1 = fact exists
-1 = deletion delta
n  = multiplicity or count
w  = weight, probability, or score
```

Integer coefficients support group-like insert/delete maintenance. Natural
number coefficients support multiset semantics. Floating coefficients can
represent weights or probabilities, but may weaken exact relational guarantees.

### Example Tokenization

Input row:

```json
{
  "id": 1,
  "country": "US",
  "age": 34
}
```

Exact row token:

```text
+basis("users.row", id=1)
```

Attribute tokens:

```text
+basis("users.country", row=1, value="US")
+basis("users.age", row=1, value=34)
```

Index tokens:

```text
+basis("idx.users.country", value="US", row=1)
+basis("idx.users.age", value=34, row=1)
```

Derived feature tokens:

```text
+basis("feature.country=US", row=1)
+basis("feature.age_bucket=30_39", row=1)
```

Then `GROUP BY country COUNT(*)` can scan or index over:

```text
basis("idx.users.country", value=country, row=row_id)
```

and emit:

```text
basis("group.country", value=country) += 1
```

For example:

```text
basis("idx.users.country", value="US", row=1)
  -> basis("group.country", value="US") += 1
```

The key rule is that a sparse token should represent an atomic fact that the
query engine can move, combine, delete, or index independently. Opaque row
tokens are useful, but database execution needs attribute-level tokens because
`GROUP BY`, `WHERE`, `JOIN`, `MIN`, and `MAX` operate on attributes.

## Temporal Analytics and Cylinders

Temporal analytical data can be represented with cylinders.

In this design, a cylinder is a key-space selector crossed with a time interval:

```text
Cylinder = KeyBasis x TimeRegion x MeasureAlgebra
```

For example:

```text
basis("sales.amount", store=12, sku=99)
  x interval[2026-01-01, 2026-02-01)
```

This represents the analytical subject `sales.amount` for `(store=12, sku=99)`
over a specific span of time.

The engine should treat cylinders as a logical and physical planning concept,
not as a replacement for raw facts. A practical layout is:

```text
raw facts:       point tokens / event tokens
temporal index:  interval or bucket tokens
rollups:         cylinder tokens
```

### Point Events

Raw temporal data starts as point tokens:

```text
+basis("orders.amount", order=100, customer=7, time=t1, value=50)
```

or, separating identity and measure:

```text
+basis("orders.row", order=100)
+basis("orders.customer", order=100, value=7)
+basis("orders.time", order=100, value=t1)
+basis("orders.amount", order=100, value=50)
```

Point tokens preserve exact event-level detail and support reconstruction,
auditing, late-arriving data, and recomputation.

### Cylinder Rollups

For analytical access, point tokens can be folded into materialized cylinders:

```text
daily cylinder:
  basis("orders.amount.sum", customer=7) x day(2026-05-10) += 50

monthly cylinder:
  basis("orders.amount.sum", customer=7) x month(2026-05) += 50
```

A cylinder can also be described as a structured object:

```text
Cylinder {
  key: basis("orders.by_customer", customer=7),
  time: [2026-05-01, 2026-06-01),
  measure: "amount",
  algebra: SumMonoid,
  value: 9132.50
}
```

The cylinder key defines the analytical grouping dimensions. The time region
defines the temporal extent. The measure algebra defines how values are
combined and incrementally maintained.

### Time Buckets and Regions

Time regions may be regular buckets:

```text
minute(2026-05-10T12:34Z)
hour(2026-05-10T12Z)
day(2026-05-10)
month(2026-05)
```

or arbitrary intervals:

```text
interval[2026-05-01T00:00Z, 2026-05-11T00:00Z)
```

Regular buckets are useful for rollups and partition pruning. Arbitrary
intervals are useful for window queries and temporal predicates. The engine can
answer arbitrary intervals by combining complete buckets and scanning boundary
fragments.

### Algebra per Cylinder

Additive aggregates work especially well with cylinders:

```text
COUNT: additive monoid/group
SUM:   additive monoid/group
AVG:   product algebra of (sum, count)
```

For example:

```text
avg(amount) = sum(amount) / count(amount)
```

can be maintained as:

```text
CylinderValue = {
  sum:   SumMonoid,
  count: CountMonoid
}
```

Other analytical aggregates need support state:

```text
MAX:        ordered values, count per value, or top-k support
MIN:        ordered values, count per value, or bottom-k support
DISTINCT:   set or sketch per cylinder
PERCENTILE: histogram, t-digest, quantile sketch, or raw support
TOP-K:      heap plus count map
```

The cylinder should declare this algebra and support state explicitly, so the
optimizer knows whether updates are reversible, monotone, approximate, or
require recomputation.

### Incremental Temporal Maintenance

When a new point event arrives:

```text
Delta =
  +basis("orders.amount", order=100, customer=7, time=2026-05-10T14:03Z, value=50)
```

the engine maps it to every materialized cylinder that covers its key and time:

```text
day(2026-05-10), customer=7:
  sum += 50
  count += 1

month(2026-05), customer=7:
  sum += 50
  count += 1
```

For deletes or corrections, additive cylinders can apply inverse deltas:

```text
sum += -50
count += -1
```

Non-invertible cylinders, such as `MAX` or `PERCENTILE`, need retained support
state or affected-bucket recomputation.

### Temporal Query Planning

A query such as:

```sql
SELECT customer, SUM(amount)
FROM orders
WHERE time >= '2026-05-01' AND time < '2026-06-01'
GROUP BY customer;
```

can be planned as a cylinder lookup:

```text
key:     basis("orders.by_customer", customer)
time:    month(2026-05)
measure: amount
algebra: SumMonoid
```

If the query interval does not align with materialized buckets, the planner can
combine:

```text
complete cylinders
+ boundary scans
+ optional smaller-grain cylinders
```

This makes temporal analytics a sparse algebraic planning problem:

```text
event tokens       -> exact history
bucket tokens      -> temporal index
cylinder tokens    -> materialized analytical folds
query over time    -> composition of cylinders and boundary facts
```

The design rule is to store temporal analytics as exact sparse event tokens
plus materialized cylinder tokens over time intervals. Each cylinder declares
the algebra needed to maintain its aggregate.

## Queries as Algebraic Operators

Queries compile into maps over sparse formal sums.

A filter maps a basis token either to itself or to zero:

```text
filter_p(t) =
  t if p(t)
  0 otherwise
```

A projection maps one basis space into another:

```text
project_country_age(
  basis("users", id=1, country="US", age=34)
)
= basis("country_age", country="US", age=34)
```

A `GROUP BY country COUNT(*)` maps each tuple to a group basis and folds
coefficients with addition:

```text
basis("users", id, country, age)
  -> basis("group", country) with value 1
```

The SQL query:

```sql
SELECT country, COUNT(*)
FROM users
GROUP BY country;
```

can be represented as:

```text
fold_by(
  key = country,
  value = 1,
  combine = +
)
```

The result is another sparse vector:

```text
3*basis("US") + 2*basis("DE") + ...
```

Here, the basis token identifies the group and the coefficient stores the
aggregate.

## Aggregates and Algebraic Requirements

Different database operators require different algebraic structures.

```text
COUNT: additive monoid, or additive group when deletes are required
SUM:   additive monoid/group
MIN:   meet-semilattice
MAX:   join-semilattice
JOIN:  semiring-like multiplication and addition
```

`SUM` and `COUNT` are friendly to incremental maintenance because they can be
represented with additive deltas:

```text
insert x => +x
delete x => -x
```

`MIN` and `MAX` are different. They are associative, commutative, and
idempotent, but they generally have no inverse. This makes insertions easy and
deletions harder.

For example:

```text
MAX(age) GROUP BY country
```

uses:

```text
combine  = max
identity = -infinity
```

An insert can be maintained from the previous value:

```text
new_max = max(old_max, inserted_age)
```

A delete cannot always be repaired from the old max alone. If the deleted row
provided the maximum, the engine needs support state such as:

```text
country -> ordered multiset of ages
country -> top-k ages
country -> heap plus tombstones
country -> count per age
```

This distinction should be visible in the query IR. Groups support cheap
inverse updates. Semilattices support monotone accumulation. Non-invertible
aggregates require retained evidence or recomputation.

## Joins as Contractions

Joins can be modeled as sparse tensor contractions or basis-token matching.

Given:

```text
Orders(user_id, amount)
Users(id, country)
```

A join on `Orders.user_id = Users.id` matches basis coordinates:

```text
basis("orders", order_id, user_id, amount)
  tensor basis("users", user_id, country)
    -> basis("joined", country, amount)
```

This is analogous to contraction over `user_id`:

```text
Orders[user_id, order_id, amount]
Users[user_id, country]

Join = contraction over user_id
```

For incremental updates, joins admit delta rules:

```text
Delta(A join B) =
  Delta(A) join B
+ A join Delta(B)
+ Delta(A) join Delta(B)
```

This makes joins natural candidates for differential or incremental execution.

## Engine Architecture

An experimental engine could be organized in layers.

### Logical Tuple Space

Tables are sparse formal sums of basis tokens. Coefficients may represent:

```text
multiplicity
weights
probabilities
provenance
authorization labels
delta counts
```

### Algebraic IR

The query compiler lowers SQL, Datalog, or another query language into an
algebraic intermediate representation with operators such as:

```text
map
filter
project
join / contract
group / fold
union
difference
```

Each operator declares the laws it relies on:

```text
associative
commutative
idempotent
has identity
has inverse
distributes over another operator
monotone
```

The optimizer can use those laws to reorder, factor, cache, or incrementally
maintain computations.

### Physical Representation

The sparse formal model can compile to several physical layouts:

```text
hash maps from basis keys to coefficients
compressed bitmaps
CSR / COO sparse matrices
tries over tuple keys
factorized join indexes
columnar segments
ordered maps for min/max support
```

The engine should choose layouts based on sparsity, cardinality, update rate,
query shape, and the algebraic requirements of the operators.

### Incremental Maintenance

For linear query fragments:

```text
Q(D + DeltaD) = Q(D) + Q(DeltaD)
```

For joins, the engine expands deltas through the product rule shown above.

For aggregates, the algebra determines the maintenance strategy:

```text
group aggregate with inverse: apply delta directly
monotone semilattice: merge new evidence
non-invertible delete: use support state or recompute affected group
```

## Example

Input:

```text
Users =
  1*basis(id=1, country="US", age=34)
+ 1*basis(id=2, country="US", age=20)
+ 1*basis(id=3, country="DE", age=28)
```

Query:

```sql
SELECT country, COUNT(*), MAX(age)
FROM users
GROUP BY country;
```

Compiled folds:

```text
count_fold:
  basis(id, country, age) -> basis(country) with value 1
  combine = +

max_fold:
  basis(id, country, age) -> basis(country) with value age
  combine = max
```

Result:

```text
count:
  US -> 2
  DE -> 1

max:
  US -> 34
  DE -> 28
```

Insert:

```text
Delta = +1*basis(id=4, country="US", age=41)
```

Maintenance:

```text
Delta count(US) = +1
Delta max(US)   = max(34, 41) = 41
```

Delete:

```text
Delta = -1*basis(id=4, country="US", age=41)
```

Maintenance:

```text
Delta count(US) = -1
```

The `MAX` result cannot be repaired from the previous maximum alone. The engine
must consult retained support state or recompute the affected group.

## Why This Is Useful

The design unifies several database concepts under one execution model:

```text
relational query       = algebraic expression
materialized view      = cached expression result
incremental update     = expression derivative over deltas
index                  = chosen sparse basis layout
aggregation            = fold over declared algebra
query optimization     = law-preserving rewrite
provenance / weighting = coefficient interpretation
```

This connects ideas from relational algebra, sparse linear algebra, Datalog,
semiring provenance, differential dataflow, incremental view maintenance,
factorized databases, CRDT-style merge semantics, and vector databases.

The key design principle is to let each query fragment declare the algebraic
structure it actually needs. Group theory is one useful tool, especially for
reversible updates, but the full engine should be algebra-polymorphic.

## Antfly Prototype

`antfly-zig` is a good fit for a first prototype, but the experiment should
start as a sidecar analytical index rather than a replacement for the core
storage or query path.

The existing architecture already has several useful entry points:

```text
document-first writes
derived work after base document commit
sequence-based derived log
per-index replay and watermarks
full-text, dense, sparse, and graph index targets
query-time aggregations over result sets
backend-neutral primary storage seams
```

That means the prototype can reuse the normal write and replay lifecycle while
adding one new derived target that materializes symbolic sparse tokens and
temporal cylinders.

### Prototype Shape

Add a small algebraic module under the DB layer:

```text
pkg/antfly/src/storage/db/algebraic/
  token.zig        canonical token -> coordinate_id
  vector.zig       sparse formal vector / deltas
  algebra.zig      count, sum, min, max, avg, and support-state traits
  cylinder.zig     temporal rollup keys and bucket math
  index.zig        materialized token/cylinder store
  planner.zig      route supported aggregation requests to the algebraic index
```

The first implementation should be intentionally narrow:

```text
exact JSON document tokenization
field terms for group keys
numeric fields for measures
datetime fields for cylinders
COUNT, SUM, AVG, MIN, MAX
date_histogram plus COUNT/SUM/AVG
single-table aggregation queries
fallback to the existing aggregation path for unsupported shapes
```

### Derived Target

Introduce a new derived target:

```text
full_text
dense_vector
sparse_vector
graph
algebraic_tokens
```

The derived batch should carry enough information for the algebraic index to
apply insert, overwrite, and delete deltas. At first, this can be derived from
the cleaned base document and existing changed/deleted document keys rather
than adding a large new payload shape.

Conceptually:

```text
document write
  -> parse base JSON once
  -> commit base document
  -> append derived record
  -> algebraic worker tokenizes JSON into deltas
  -> algebraic index updates token and cylinder state
```

This keeps the prototype aligned with the current derived-log model and avoids
making query correctness depend on background execution. Rebuild-from-docstore
should remain the fallback for missed or incompatible algebraic index state.

### Tokenization

For each document, generate row and attribute tokens:

```text
+basis("row", table=<table>, doc=<doc_key>)
+basis("field", table=<table>, field="country", doc=<doc_key>, type=string, value="US")
+basis("field", table=<table>, field="age", doc=<doc_key>, type=number, value=34)
+basis("field", table=<table>, field="created_at", doc=<doc_key>, type=datetime, value=t1)
```

Canonical tokens should be stable byte strings:

```text
field|table=orders|field=customer|doc=100|type=string|value=alice
```

The physical coordinate can be a compact hash:

```text
coordinate_id = hash128(canonical_token)
```

For exactness and debugging, keep a symbol table:

```text
coordinate_id -> canonical token
```

The first version can use a backend key-value layout rather than trying to
reuse the embedding sparse index directly. The existing sparse index is useful
prior art for sorted dimensions, posting chunks, and split planning, but this
prototype needs symbolic token semantics and aggregate support state.

### Schemaless and Late-Typed Facts

Antfly should remain schemaless by default. Algebraic indexing must therefore
not depend on a user-declared schema being present before it can build useful
symbolic state.

The long-term model is two-tiered:

```text
universal schemaless facts:
  fact(doc{}, path{}, kind{}, value{}) -> 1

typed/materialized projections:
  docfact(role{}, field{}, scalar{}, doc{}) -> 1
  postings(field{}, analyzer{}, term{}, doc{}) -> freq/norm/positions
  embedding(doc{}, dim[]) -> float
  sparse_embedding(doc{}, token{}) -> weight
  cylinder(bucket{}, group{}, metric{}) -> value
```

Every JSON document can always be projected into universal path/kind/value
facts:

```json
{"customer":"alice","amount":20,"body":"hello world","meta":{"tier":"gold"},"tags":["new"]}
```

```text
fact(doc=o1, path="customer", kind=string, value="alice") -> 1
fact(doc=o1, path="amount", kind=number, value=20)        -> 1
fact(doc=o1, path="body", kind=string, value="hello world") -> 1
fact(doc=o1, path="meta", kind=object, value="")          -> 1
fact(doc=o1, path="meta/tier", kind=string, value="gold") -> 1
fact(doc=o1, path="tags", kind=array, value="")           -> 1
fact(doc=o1, path="tags/0", kind=string, value="new")     -> 1
```

The empty object/array value is a structural presence marker. It is useful for
bounded `exists` and delete compensation, but planners should not reinterpret it
as a scalar term value.

Schemas, inferred path profiles, query observations, or explicit index configs
can then promote selected paths into typed projections:

```text
docfact(role=group, field="customer", scalar="alice", doc=o1) -> 1
docfact(role=measure, field="amount", scalar=20, doc=o1)      -> 1
postings(field="body", analyzer="default", term="hello", doc=o1) -> freq
```

This keeps the write contract schemaless while letting the optimizer converge
toward typed tensor execution for hot, unambiguous paths. If a path has mixed
kinds, the planner must either use kind-qualified semantics or fall back:

```text
amount=20       kind=number
amount="20"     kind=string
amount="twenty" kind=string
```

A numeric range over `amount` can safely use only `kind=number` facts if that
matches the public query contract. If the query requires coercion across string
and numeric representations, the plan needs an explicit coercion policy or a
slower verification path. For schema-declared `docfact` fields, parseable JSON
strings are canonicalized through the declared integer/number type before being
stored as typed facts; malformed strings are omitted from that typed projection
and remain available through schemaless path facts/fallback.

The practical rule is:

```text
schemaless ingest creates generic facts
observed path profiles describe possible typed interpretations
declared schemas pin ambiguous paths
adaptive materializations only promote proven-safe typed shapes
fallback remains available for unknown or ambiguous semantics
```

### Lexical Tensor Access Paths

Full-text, algebraic scalar lookup rows, sparse vectors, dense vectors, and
graph layouts should eventually be physical access paths under one typed
sparse-tensor query IR, not isolated query engines with separate semantics.

The full-text term dictionary is a particularly useful physical layout for
mapped string dimensions. Vellum/FST lookup, range iteration, and automaton
search can implement label selection for dimensions such as `term{}` or
`scalar{}`:

```text
term:
  slice(postings, field=body, term="cat")
  -> tensor(doc{})

prefix / term_range:
  range-select lexicon(field{}, term{})
  join selected_terms with postings on term{}
  reduce(or, term{})
  -> tensor(doc{})

wildcard / regexp:
  automaton-select lexicon(field{}, term{})
  join selected_terms with postings on term{}
  reduce(or, term{})
  -> tensor(doc{})
```

The algebraic index should not embed a second full search engine. BM25 scoring,
positions, phrase/proximity, highlighting, and analyzer-specific ranking state
belong to the full-text physical layout. Algebraic planning should consume the
candidate tensors those layouts can produce and then continue with exact
filters, vector pruning, joins, or aggregate folds:

```text
join(candidate_docs(doc{}), docfact(role{}, field{}, scalar{}, doc{}))
reduce(count/sum/min/max, doc{} or group{})
```

Analyzed text terms and canonical scalar facts must remain distinct dimensions:

```text
postings(field{}, analyzer{}, term{}, doc{})      // analyzed text semantics
docfact(role{}, field{}, kind{}, scalar{}, doc{}) // canonical scalar semantics
```

They may share FST/dictionary machinery, but they should not share meaning by
accident. The planner should select a lexical access path only when the query's
field, analyzer, kind, and coercion rules prove that the candidate tensor has
the requested semantics.

The storage rule is to share a dictionary/postings layout whenever the semantic
label space is identical, and to create a separate layout only when the label
space is different:

```text
dictionary_identity =
  table or index scope
  field or JSON path
  label kind: analyzed_term | canonical_scalar | sparse_token | graph_label
  analyzer or canonicalization version
  value kind and coercion policy
```

Dictionary identity is a storage-level ownership contract, not an index-owner
preference. Algebraic planning must not build a duplicate FST for the same
`dictionary_identity` already owned by the lexical/full-text layout. It should
ask that layout for a candidate tensor. Separate FSTs are justified for distinct
semantics, such as analyzed `body` terms versus canonical scalar
`customer="alice"` facts, because those labels have different tokenization,
canonicalization, value-kind, and coercion rules.

The explicit anti-pattern is two physical owners for the same label space:

```text
full_text FST for body/default analyzed terms
algebraic FST for body/default analyzed terms
```

That is duplicate storage and a correctness risk. The right model is one
dictionary/postings owner per `dictionary_identity`, with multiple planners
consuming the same advertised tensor access path. The owner may be full-text,
algebraic path promotion, sparse-vector, or graph, but the registry row is the
shared source of truth. Consumers that cannot prove an exact identity match must
fall back or create a separate dictionary for their distinct semantic dimension.

### Materialized Expressions

The sidecar materializes declared tensor expressions, not ad hoc result
families. A materialized expression is identified by a stable semantic
fingerprint derived from the expression fragment, declared law, dimensions,
metadata, dictionary identity where relevant, and the owning algebraic index.
The durable row shape is:

```text
materialized_expr:<expr_id>:<axis_key> -> law state
```

Examples:

```text
materialized_expr:expr(count by country):country=US -> 42
materialized_expr:expr(sum amount by customer):customer=alice -> 9132.50
materialized_expr:expr(avg amount by customer):customer=alice -> { sum: 9132.50, count: 27 }
```

For `COUNT`, `SUM`, and `AVG`, expression rows receive direct law deltas:

```text
insert amount=50:
  count += 1
  sum += 50

delete amount=50:
  count += -1
  sum += -50
```

For `MIN` and `MAX`, store support tensors keyed by the same expression id:

```text
minmax:<expr_id>:<axis_key>:<measure_value> -> count-law support
materialized_expr:<expr_id>:<axis_key> -> current boundary value
```

Deletes decrement the support count. If the deleted value was the current max
and the count reaches zero, the index finds the next supported value or marks
the group for bounded recomputation.

### Temporal Expressions

For temporal analytics, the time bucket is just another expression axis:

```text
materialized_expr:<expr_id>:time=<bucket_start>:group=<axis_key> -> law state
```

Examples:

```text
materialized_expr:expr(day sum amount by customer):day=2026-05-10:customer=alice -> 250
materialized_expr:expr(month avg amount by customer):month=2026-05:customer=alice -> { sum: 9132.50, count: 27 }
```

The first supported bucket kinds can be:

```text
hour
day
month
```

Later versions can add arbitrary intervals by combining complete materialized
bucket expressions with boundary scans:

```text
query interval
  -> complete materialized buckets
  -> boundary document scans
  -> optional smaller-grain expression buckets
```

### Query Planning

The planner should only route to the algebraic index when the request matches a
supported shape:

```text
single table
no joins
no graph expansion
supported filter subset
supported group field
supported measure field
supported aggregate algebra
optional aligned date_histogram bucket
```

Example supported requests:

```sql
GROUP BY country COUNT(*)
GROUP BY customer SUM(amount)
GROUP BY customer AVG(amount)
GROUP BY customer, day(created_at) SUM(amount)
GROUP BY customer, month(created_at) COUNT(*)
```

Unsupported requests should fall back to the current search/aggregation path.
This makes the feature safe to introduce incrementally and gives the planner a
clear correctness boundary.

### Storage Layout

A simple backend layout can use prefixed keys:

```text
sym:<coordinate_id>                 -> canonical token
docfact:<doc_key>                   -> schema-derived typed symbolic fact list
pathfact:<doc_key>                  -> schemaless path/kind/value fact list
path_lookup:<path>:<kind>:<value>:<doc_key> -> exact schemaless candidate lookup
joinfact:<join_name>:<side>:<fact_key> -> projected join contribution fact
docjf:<doc_key>:<join_name>:<side>:<fact_key> -> document-local join fact reference
materialized_expr:<expr_id>:<axis_key> -> cached law-declared tensor expression result
minmax:<expr_id>:<axis_key>:<value> -> support tensor for delete-safe extrema
lexicon:<dictionary_identity>:<label> -> shared dictionary metadata
postings:<dictionary_identity>:<label>:<doc_key> -> shared candidate/posting payload
lexicon_fst:<dictionary_identity>   -> rebuildable FST artifact for the dictionary owner
watermark:<index_name>              -> applied derived-log sequence
```

`docfact`, `pathfact`, and `docjf` give overwrite/delete a precise way to
reverse prior facts. `materialized_expr` is the only durable aggregate result
layout; support rows, lexical dictionaries, and postings are auxiliary tensors
or shared access paths.

### Prototype Bootstrap

The bootstrap prototype used the following narrow milestone to prove the idea
without committing the full database to the new execution model:

```text
1. Create an algebraic index catalog entry.
2. Tokenize string, number, and datetime JSON fields.
3. Maintain COUNT by one string field.
4. Maintain SUM and AVG by one string group field and one numeric measure.
5. Maintain daily SUM temporal expression buckets over one datetime field.
6. Route matching aggregation requests to the algebraic index.
7. Fall back to existing aggregation code for everything else.
8. Add reopen/replay tests through the derived log.
```

The demonstration query set should be:

```text
GROUP BY country COUNT(*)
GROUP BY customer SUM(amount)
GROUP BY customer AVG(amount)
GROUP BY customer, day(created_at) SUM(amount)
```

The current implementation has moved past this bootstrap target. Completed
capabilities are tracked in Implementation State, durable implementation rules
live in the Implementation Guide, and the Roadmap now records boundary
conditions rather than an active milestone list.

### Risks

The main design risks are:

```text
token cardinality explosion
schema drift and mixed field types
overwrite/delete correctness
late-arriving temporal events
non-invertible aggregate maintenance
planner accidentally choosing the sidecar for unsupported semantics
symbol-table and hash-collision handling
distributed/sharded aggregate merge semantics
```

The mitigation is to keep the first index opt-in, schema-constrained, and
planner-gated. The prototype should prove that derived-log replay can maintain
algebraic materializations correctly before trying to generalize the query IR.

### Success Criteria

The prototype is successful if it can show:

```text
correct results across insert, overwrite, delete, reopen, and replay
faster common group/time aggregations than scanning hit stored_data
clear fallback behavior for unsupported query shapes
bounded support-state behavior for MIN/MAX
simple extension path toward more algebraic operators
```

The prototype has moved beyond that first milestone: it now has an explicit
typed algebraic IR, materialized-expression IDs, tensor-program envelopes, and
proof-gated access paths. Additional bounded query shapes should extend those
artifacts only when they preserve exact semantics and have benchmark evidence;
unsupported text, join, vector, graph, and schemaless shapes stay on explicit
fallback paths.

## Current Implementation

The current Zig prototype implements the first local algebraic sidecar under:

```text
pkg/antfly/src/storage/db/algebraic/
```

It is opt-in through `.algebraic` index configs, maintained through derived
replay, and planner-gated so unsupported or unhealthy shapes fall back to the
existing aggregation path. Base documents remain canonical. Algebraic state is
derived, rebuildable, and scoped to prefixed sidecar keys.

Implemented capability includes:

```text
internal algebraic index kind and derived replay target
canonical tuple/scalar token encoding with symbol-id backed keys
typed value canonicalization module for string/integer/number/bool/time/bytes
algebraic law registry for group, monoid, lattice, and semiring descriptors
explicit document fact-list projection and persisted docfact sidecar rows
sparse tensor row/key module for law-backed coordinate slots
tensor-only materialized-expression row decoding for new algebraic state
direct COUNT/SUM/AVG/MIN/MAX materializations
exact MIN/MAX support rows for duplicate values and delete repair
hour/day/month temporal cylinders
configured composite equi-join aggregate maintenance
explicit join document role and side gating
join-side fact projection to only fields required by configured materializations
LSM-backed bulk join ingest with cursor-capable write-batch matching
bulk maintenance-plan caching for ready adaptive materializations during overwrite/delete bulk ingest
resource-manager accounting for algebraic tensor accumulator working memory through `algebraic.tensor_accumulators`
temporal join bucket/window matching with range-pruned fact scans
overwrite/delete compensation from persisted document and join fact rows
root metric, terms, date_histogram, constrained rollup, and nested metric routing
internal algebraic query IR for metric, bucket, constraint, and configured join shapes
bucket child metric planning through the algebraic query IR
strict configured join materialization matching unless the query names the materialization directly
opt-in implicit join materialization matching when a configured join fold is declared safe for normal query planning
bucket-limit, row-scan budget, and health checks before planner selection
planner-visible schema lifecycle gating for stale/rebuild-required algebraic configs
planner estimates for recent scan rows and result buckets in algebraic status
planner last-decision, fallback, lifecycle readiness, lifecycle blocking, dictionary registry ownership, and vector symbolic-filter routing diagnostics in index status and benchmark JSONL events
semantic dictionary registry ownership that prevents duplicate FST/postings publication for equivalent analyzed-term, canonical-scalar, sparse-token, and graph-label identities
persisted query-shape observation and materialization recommendation scaffolding
scannable persisted materialization-state rows and DB status counters for adaptive recommendations
DB-open and add-index hydration of persisted adaptive observation status
internal DB-level listing of persisted algebraic materialization recommendation states for status assembly
internal DB-level listing of persisted adaptive query observations for status assembly
engine-owned adaptive lifecycle transitions for persisted algebraic materialization recommendation states
validated adaptive lifecycle transitions for materialization recommendation states, with no manual user-facing lifecycle API
deterministic adaptive materialization ids from canonical recommendation hashes
policy-gated adaptive candidate evaluation with persisted ranking and progress rows
persisted adaptive candidate decision history and policy-drift status/benchmark counters
bounded adaptive tensor backfill from persisted docfact rows with ready promotion
planner routing for ready adaptive terms/date histogram tensors with nested metric tensors
cost/savings-aware adaptive dematerialization recommendations with safe tensor cleanup
adaptive lifecycle primitives for observing/recommended/backfilling/ready/stale materializations
adaptive lifecycle recovery tests for interrupted backfill, schema drift, and dictionary-owner conflicts
schemaless path profiles with adaptive typed path promotion, dictionary-backed lookup rows, readiness gating, and mixed-kind/coercion fallback policy
explicit planner fallback and rejection paths for unsupported ranking, non-proven traversal, arbitrary joins, unsupported constraints, and mixed-kind/coercion cases
explicit semiring product hooks for provenance-backed sparse vector contractions
law-compatible tensor row merge primitive for broader tensor execution
set-union lattice law coverage through tensor row mutation/merge and distributed partial merge
planner metric folds routed through the algebraic law registry
proof-gated group, lattice, and provenance-semiring laws for aggregate, MIN/MAX support-row, distributed partial, and graph/path traversal capabilities
fact-only append-only bulk fast path for bounded sidecar fact ingestion
benchmark counters and quick/standard/large profiles for disk usage, query time, write cost, join scans, and flushes
benchmark reporting for planner estimates, observed query shapes, docfact rows, and recommendation candidates
benchmark correctness classification for every algebraic-vs-baseline query comparison, with guardrails for missing classifications
benchmark summary comparisons for adaptive materialized vs static vs fallback algebraic disk, query, and update paths
adaptive coverage benchmark mode for root metrics, terms metrics, date histograms, constrained folds, configured joins, warmup cost, and churn
storage-focused algebraic benchmark root so adaptive coverage smoke builds without full API/server dependencies
bounded LSM analytics smoke mode for durable constrained/adaptive coverage checks
LSM analytics benchmark mode for durable sidecar scale, cardinality, write amplification, churn, fanout, constrained, and adaptive coverage cases
churn row-family benchmark counters for materialized_expr, docfact, pathfact, path_lookup, path_profile, joinfact, docjf, minmax, and sym sidecar rows
public query guardrail query shapes for dense, hybrid, hybrid-filter, hybrid-filter-exclude, and projected hybrid filter vector searches
public query guardrail schema and algebraic toggles so default, schema-only, and schema-plus-algebraic runs are comparable
public query guardrail JSONL summary events for no-schema, schema-only, and schema-plus-algebraic vector-pruning comparisons, including a no-listener handler mode for reproducible archived runs and local/swarm modes for transport/server overhead evidence
benchmark-wide performance evidence summary for dataset, query, correctness, cold/warm, fanout, constrained, wide-key, stats, cardinality, range, histogram, churn, churn row-family, public-query, LSM, symbol/support byte growth, accumulator flush, adaptive bulk-maintenance counters, and public-query RSS coverage
mixed-role benchmark root-cardinality comparisons constrain algebraic sidecar reads by primary document role so derived customer/profile facts do not broaden doc-scan/full-text order-only baselines
algebraic-summary performance guardrail thresholds for coverage counts, cold/warm reads, fanout, constrained queries, wide-key composite queries, stats/cardinality/range/histogram queries, correctness failures, query latency, byte cost, symbol/support bytes, accumulator flushes, LSM flush/write-pressure compaction counts, public-query RSS, and churn cost
algebraic-summary baseline-file comparison ratios for stable local performance guardrails
`algebraic-performance-guardrail` build step with a checked-in JSONL fixture for coverage and baseline-ratio verifier regressions
`algebraic-planner-ownership-guardrail` build step under `tools/guardrails/` that rejects production raw tensor-program construction outside the algebraic planner/IR layer
`algebraic-archive-guardrail` build step that verifies archived production-hardening run directories include environment notes, raw/summary JSONL, threshold flags, optional baseline comparison, and non-smoke provenance
`algebraic-roadmap-guardrail` build step that combines CI-safe algebraic performance, planner-ownership, and archive-evidence checks
`scripts/run_algebraic_production_hardening.sh` runner for archived LSM analytics, adaptive coverage, cold/warm read coverage, graph traversal, public query no-schema/schema/algebraic comparisons, summary generation, threshold enforcement, optional baseline-ratio checks, bounded cardinality and per-stage sizing/churn knobs, query-shape coverage thresholds for cold/warm/constrained/wide/stats/cardinality/range/histogram records, path-promotion FST rebuild thresholds, public-query mode selection through `ALGEBRAIC_HARDENING_PUBLIC_MODE`, optional public-query symbolic-profile enforcement through `ALGEBRAIC_HARDENING_PUBLIC_REQUIRE_SYMBOLIC_PROFILE=1`, optional LSM bulk-ingest stress through `ALGEBRAIC_HARDENING_LSM_BULK_INGEST=1` across LSM analytics and adaptive coverage stages, LSM bulk finish knobs for flush, compact, deferred-L0 targets, and bounded foreground compaction budgets, and optional broad unit-test evidence
LSM bulk-session finish direct-ingests the final mutable state as a sorted run when direct bulk ingest is enabled and no immutable flush is pending, so algebraic bulk sidecars avoid a final normal flush and archived runs can guard `total_lsm_sorted_ingest_runs`
algebraic bulk-ingest sessions defer promoted path dictionary FST rebuilds across all flushed coalescer batches and rebuild each dirty promoted dictionary once at DB bulk-session finish, before the primary store publishes the final sorted run
`scripts/run_algebraic_integration_matrix.sh` runner for archived enabled/disabled integration evidence across roadmap guardrails, public-query default no-schema, schema-only, schema-plus-algebraic, focused algebraic DB tests, provisioned distributed non-algebraic fallback coverage, optional broad unit tests, and optional selected e2e tests
durable planner default policy remains opt-in and conservative until LSM guardrail evidence covers latency, bytes, write cost, churn, cold reads, fanout, and constrained queries
schema capability fingerprints, skipped-unbounded-field metadata, and debug lifecycle classification
schema-derived v2 configs with declared laws and adaptive defaults
runtime-adaptive dynamic templates: bounded table-level dynamic templates (keyword/numeric/boolean/datetime) compile into capability `dynamic_field_rules` and project template-matched fields into typed docfacts at ingest, so template changes take effect for new writes without a schema version bump or reindex; unbounded text templates stay on the schemaless path-fact path (cardinality guard), and template-only updates refresh both the durable config and live indexes in place (`Index.reloadConfigJson`)
relational `json` columns compile into scoped JSON-subdocument domains: embedded schemas and column-local dynamic templates emit prefixed capability fields (`attrs.plan`, `attrs.score`) plus a per-column capability fingerprint, algebraic docfact projection serves aggregations over those paths from the relational row-derived document body, and JSON-domain fingerprint changes mark that domain `rebuild_required` so field resolution withholds stale subdocument facts until rebuild
relational column `indexed: false` suppresses only relational column-major predicate scan entries; embedded JSON algebraic/full-text projection still follows the JSON column's declared embedded schema and dynamic templates
public algebraic index requests constrained to schema-derived capability sidecars with internal materialization fields stripped/rejected
canonical-token shard merge keys for distributed symbol semantics
distributed partial merge helpers that combine shard results by canonical axis and law
distributed partial validation tests for malformed canonical law values
distributed partial merge tests for count, sum, sumsquares, avg-state, min, and max law families on shared canonical bucket axes
distributed partial merge tests for set-union lattice payloads on shared canonical axes
validated distributed partial-envelope merge tests for different shard-local symbol ids resolving to the same canonical axis
distributed partial aggregation tests for independent shard dictionaries merging the same canonical terms/stat/join axes
unit coverage for planner-routed stats, nested metric, range/histogram, cardinality, pathfact predicate, multi-field composite terms, partial distributed tensor programs, derived joins, and canonical distributed law merges
distributed partial tensor-program proof/export counters in algebraic status and benchmark summaries
distributed partial tensor-program proof/export counters are summarized through the compact public algebraic index-status contract
adaptive coverage summaries expose rebuild-required, stale, cleanup-recommended, decision-history, and policy-drift counters
LSM reopen coverage for adaptive dematerialization cleanup proving tensor rows, progress rows, and candidate rows stay removed while materialization state remains stale
compact public algebraic index status exposed as a single `AlgebraicIndexStats` envelope without public `AlgebraicRuntimeHealth` or `AlgebraicAdaptiveProgressStatus` schemas in the normal index-status response
public OpenAPI keeps detailed algebraic runtime health, adaptive progress, adaptive candidate, and candidate-decision records out of the stable status schema; those records remain internal state or diagnostics/benchmark evidence
internal algebraic partial request protocol accepts only tensor expression/program envelopes, rejecting named materialization requests and cardinality-only bodies
public table/index parser support for `type: "algebraic"` only through `derive_from_schema: true`
native dense-vector pruning for safe symbolic doc-id constraints by translating doc ids to HBC vector ids
native sparse-vector pruning for safe symbolic doc-id constraints by filtering doc nums during sparse score accumulation
native vector pruning for structured stored filters by resolving filter doc ids through the text index before dense/sparse ranking
required symbolic vector filters fail closed when algebraic lifecycle state is stale
internal shard-query wire fields for native doc-id include/exclude candidate sets so resolved algebraic constraints survive distributed fanout
proof-gated dense/sparse vector and graph constraint input tensors, including `native_doc_id_constraints` and `graph_target_constraints`, with fail-closed validation of required envelopes
first-class tensor access-path helpers for docfact, pathfact, lexical, sparse-token postings, dense/sparse vector search, and graph layouts
planner-owned dense/sparse vector and graph tensor-program construction, with catalog code limited to access-path discovery
planner-owned docfact, pathfact, and derived-join range/histogram tensor-program construction for distributed partial fanout
planner-owned configured-materialization tensor-program construction for distributed partial fanout, replacing API-local tensor expression derivation
distributed configured-aggregate partial fanout encoded as planner-owned tensor program/access-path envelopes instead of named materialization requests
root cardinality distributed partials can be requested through planner-owned tensor program envelopes while preserving exact distinct-value merge keys
terms-with-nested-cardinality distributed partials can be requested through planner-owned tensor program envelopes with canonical bucket and distinct-value merge keys
range-with-nested-cardinality distributed partials can be requested through planner-owned tensor program envelopes with canonical range and distinct-value merge keys
histogram-with-nested-cardinality distributed partials can be requested through planner-owned tensor program envelopes with canonical bucket and distinct-value merge keys
production distributed cardinality, terms-cardinality, range-cardinality, and histogram-cardinality aggregation paths fall back to the normal aggregation planner when a tensor program cannot be proven instead of using bespoke partial request bodies
production distributed aggregation request planning is centralized through planner-owned tensor-program selection before provisioned or hosted shard fanout
distributed graph expand/get-edges requests carry tensor program envelopes plus access-path proofs instead of id-only program assertions
distributed graph validation derives expected graph tensor programs from planner-owned envelopes without exposing id-only helper APIs
graph index status exposes bounded algebraic semiring traversal attempts, proofs, rejects, fallbacks, and result-node counts
graph traversal benchmark smoke and summary events expose proof/reject/fallback counters, result-node counts, path bytes, and query latency
```

Planner cost guards are deliberately conservative. `max_result_buckets` bounds
returned algebraic buckets, while `max_planner_scan_rows` bounds sidecar row
scans for rollups, terms, and date histograms. If the sidecar would need to scan
more rows than the configured budget, the planner records an explicit fallback
reason and lets the existing aggregation path answer the request.

The current storage model is intentionally direct and debuggable. It is a
schema-derived algebraic sidecar with a first schemaless fact substrate and
shared dictionary registry, while base documents remain the canonical source:

```text
docfact:<doc_key>                   -> explicit typed symbolic fact list
docjf:<doc_key>:<join>:<side>:<fact_key> -> document-local join fact reference
pathfact:<doc_key>                  -> universal schemaless path/kind/value fact list
path_profile:<path>                 -> observed kinds, cardinality, parse/coercion stats, token stats
path_lookup:<path>:<kind>:<value>:<doc_key> -> exact schemaless candidate lookup
observe:<shape>                     -> persisted query-shape observation count/reason/recommendation
materialization-state:<recommendation> -> scannable adaptive lifecycle state for a recommended materialization
adaptive-candidate:<recommendation> -> persisted rank/decision/materialization id for adaptive work
adaptive-decision-history:<recommendation>:<generation> -> persisted adaptive policy decision audit row
adaptive-progress:<recommendation>  -> persisted adaptive backfill/dematerialization progress
materialized_expr:<expr_id>:<axis_key> -> cached law-declared tensor expression result
minmax:<expr_id>:<group>:<value> -> count-law support tensor for delete-safe extrema
joinfact:<join_name>:<side>:<fact_key> -> projected left/right contribution facts
docjf:<doc_key>:<join_name>:<side>:<fact_key> -> per-document join fact references
lexicon:<dictionary_identity>:<label> -> owned/shared dictionary metadata for that identity
lexicon_fst:<dictionary_identity>   -> rebuildable Vellum/FST artifact only for the registry owner of row lexicons
postings:<dictionary_identity>:<label>:<doc_key> -> owned/shared candidate/posting payload for that identity
symbol rows                        -> canonical token to compact id mapping
status rows                        -> health, replay, and instrumentation data
```

Configured direct, temporal, adaptive, schemaless, and join materializations
all read and write through `materialized_expr` rows plus declared support
tensors where a law needs delete-safe support state.
The long-term storage model should make the typed sparse-tensor IR broad enough
that all reusable layouts below are exposed as first-class access paths rather
than aggregate-specific helpers:

```text
docfact/pathfact rows       -> symbolic sparse tensor leaves
lexicon/postings/FST rows   -> reusable lexical tensor access paths
materialized_expr rows      -> cached law-declared tensor expression outputs
vector/sparse/graph layouts -> advertised physical tensor access paths
```

These rows should remain derived and rebuildable. Declared schemas can pin
`path` to typed `field` roles. Without a declared schema, path profiles and query
observations decide which paths are safe to promote into typed projections or
adaptive materializations.

The join implementation is deliberately configured, not a general SQL join
planner. V1 joins are composite equi-joins with optional temporal bucket or
window constraints. Join materializations declare which side supplies measures
and which side supplies group fields, so the engine does not assume one fixed
orders-to-dimensions shape.

## Performance Coverage

Performance work is coverage-first before CI thresholds. The benchmark outputs
are line-oriented so local runs can be summarized and compared without committing
large JSONL result files.

Adaptive coverage smoke:

```sh
zig build algebraic-bench -- --mode adaptive-coverage --algebraic-backend mem --docs 1000 --repeats 1 --batch-size 250 --churn-ops 100 2> /tmp/algebraic-adaptive-coverage.jsonl
zig build algebraic-summary -- --input /tmp/algebraic-adaptive-coverage.jsonl
```

Durable LSM analytics smoke:

```sh
zig build algebraic-bench -- --mode lsm-analytics-smoke --docs 100 --repeats 1 --batch-size 50 --churn-ops 1 2> /tmp/algebraic-lsm-analytics-smoke.jsonl
zig build algebraic-summary -- --input /tmp/algebraic-lsm-analytics-smoke.jsonl
```

Full durable LSM analytics run:

```sh
zig build algebraic-bench -- --mode lsm-analytics --docs 5000 --repeats 3 --batch-size 500 --churn-ops 500 2> /tmp/algebraic-lsm-analytics.jsonl
zig build algebraic-summary -- --input /tmp/algebraic-lsm-analytics.jsonl
```

Hybrid vector symbolic-pruning smoke:

```sh
zig build public-query-guardrail -- --query-shape hybrid-filter --docs 5000 --queries 100 --repeats 3 2> /tmp/public-query-noschema.jsonl
zig build public-query-guardrail -- --query-shape hybrid-filter --with-schema --docs 5000 --queries 100 --repeats 3 2> /tmp/public-query-schema.jsonl
zig build public-query-guardrail -- --query-shape hybrid-filter --with-algebraic --docs 5000 --queries 100 --repeats 3 2> /tmp/public-query-algebraic.jsonl
cat /tmp/public-query-noschema.jsonl /tmp/public-query-schema.jsonl /tmp/public-query-algebraic.jsonl > /tmp/public-query-compare.jsonl
zig build algebraic-summary -- --input /tmp/public-query-compare.jsonl
```

`--with-algebraic` implies `--with-schema`. Without either flag, the public API
benchmark remains a no-schema/no-algebraic baseline. Add
`--require-symbolic-profile` when the run should fail unless the public response
contains dense/HBC profile counters for the algebraic symbolic-pruning path;
hybrid paths currently report profile completeness explicitly because not every
public query shape exposes dense profile counters yet.

Performance evidence guardrail:

```sh
zig build algebraic-performance-guardrail
zig build algebraic-planner-ownership-guardrail
zig build algebraic-archive-guardrail
zig build algebraic-roadmap-guardrail

zig build algebraic-summary -- --input /tmp/algebraic-combined.jsonl \
  --baseline /tmp/algebraic-baseline-summary.jsonl \
  --require-performance-evidence \
  --min-lsm-dataset-cases 1 \
  --min-lsm-query-records 1 \
  --min-cold-query-records 1 \
  --min-warm-query-records 1 \
  --min-constrained-query-records 1 \
  --min-fanout-dataset-cases 1 \
  --min-public-query-comparison-pairs 2 \
  --max-correctness-failures 0 \
  --max-algebraic-query-ms 25 \
  --max-public-query-http-us 5000 \
  --max-algebraic-bytes-per-doc 4096 \
  --max-symbol-bytes-per-doc 1024 \
  --max-support-bytes-per-doc 1024 \
  --max-accumulator-flush-count 10000 \
  --max-public-query-load-rss-peak-bytes 1073741824 \
  --max-public-query-search-rss-peak-bytes 1073741824 \
  --max-churn-algebraic-update-ms 1000 \
  --max-algebraic-query-ms-ratio-vs-baseline 1.25 \
  --max-public-query-http-us-ratio-vs-baseline 1.25 \
  --max-algebraic-bytes-per-doc-ratio-vs-baseline 1.10 \
  --max-churn-algebraic-update-ms-ratio-vs-baseline 1.25
```

The guardrail thresholds are command-line values rather than hard-coded CI
constants. Local baseline runs should set them from stable measured numbers with
enough tolerance to catch regressions without pinning hardware noise. The
optional `--baseline` file should contain a prior `performance_evidence_summary`
event from `algebraic-summary`.

Production-hardening archives can be checked independently:

```sh
zig build algebraic-archive-guardrail -- \
  --archive bench/results/algebraic-production-hardening/20260517T000000Z \
  --require-thresholds \
  --require-baseline \
  --require-non-smoke
```

Use `--require-thresholds` once a run is meant to count as production evidence,
`--require-baseline` once variance has been established from a prior summary,
and `--require-non-smoke` for representative archived runs.

Bounded graph traversal smoke:

```sh
zig build algebraic-bench -- --mode graph-traversal-smoke --docs 100 --repeats 3 --fanout 2 2> /tmp/algebraic-graph-traversal.jsonl
zig build algebraic-summary -- --input /tmp/algebraic-graph-traversal.jsonl
```

The important summary events are:

```text
adaptive_query_compare      -> static/fallback/materialized latency by query shape
adaptive_warmup_compare     -> backfill/warmup cost plus persisted candidate/progress lifecycle and policy-drift counts
adaptive_coverage_summary   -> coverage matrix for adaptive benchmark cases plus rebuild/stale/cleanup/policy-drift counters
lsm_analytics_summary       -> durable sidecar case/query/churn coverage
public_query_symbolic_filter -> expected symbolic match set, profile completeness, and HBC candidate/rerank counts
public_query_guardrail_summary -> public query latency, QPS, HBC, LSM, replay, and memory counters for one run
public_query_comparison_summary -> no-schema/schema-only/schema-plus-algebraic public query latency, candidate, and byte comparisons
churn_row_family -> per-workload row-family entry and byte counters for materialized_expr, docfact, pathfact, path_lookup, path_profile, joinfact, docjf, minmax, and sym rows; each event carries total workload update time, not isolated per-family timing
churn_row_family_summary -> aggregate churn row-family entry and byte totals across the input JSONL
performance_evidence_summary -> top-level coverage counts for scan, full-text, LSM, cold/warm, fanout, constrained, churn, churn row-family, correctness, public-query evidence, symbol/support bytes, accumulator flushes, path-promotion FST rebuilds, LSM flush/write-pressure compactions, and public-query RSS peaks
performance_baseline_comparison -> current-vs-baseline ratios for latency, bytes, churn, and public-query evidence
graph_algebraic_traversal_summary -> bounded traversal proof/reject/fallback counters, result nodes, path bytes, and query latency
```

## Implementation State

This section records what the current prototype already implements. The roadmap
below is intentionally forward-looking and should not repeat these completed
notes or carry design rules that future implementation must preserve.

### Tensor Expression Storage

Configured aggregate state is stored as law-declared tensor expressions:
`materialized_expr:<expr_id>:<axis_key> -> law state`. Direct count, sum,
avg, sumsquares, temporal buckets, and support-backed min/max all mutate through
tensor/law helpers. `minmax:<expr_id>:<axis_key>:<value>` rows are count-law
support tensors for delete-safe extrema, keyed by the same stable expression id
as the current result row.

`materialized_expr` is also the cache contract for configured materializations,
ready adaptive materializations, promoted schemaless path folds, and configured
join folds. Expression fingerprints carry semantic identity separate from the
physical access-path owner, so same-shaped folds over different source fields,
join semantics, or laws cannot collide.

### Fact Substrates

`docfact` is the schema-derived typed fact substrate. `pathfact`, `path_lookup`,
and `path_profile` provide the schemaless JSON-pointer substrate with typed
kind/value facts, mixed-kind/profile signals, and exact candidate lookups.
Deletes and overwrites reverse prior state through stored facts instead of
stored document snapshots.

`joinfact` and `docjf` provide the join substrate. Join facts retain side-local
group, measure, and time facts; `docjf` records per-document references so
updates and deletes can reverse only the affected join contributions.

### Planning And Execution

The current planner can route exact metric, terms, date-histogram, histogram,
range, and date-range folds through algebraic execution when declared laws,
access paths, lifecycle state, and constraints prove the request exact. Exact
`stats` is represented as the component bundle of `avg`, `min`, `max`, and
`sumsquares`; exact cardinality uses canonical distinct-value partials rather
than a sketch.

The typed IR includes tensor dimensions, fragments, physical access-path proofs,
`TensorExpr` fingerprints, and `TensorProgram` envelopes. Materialized terms and
date-histogram executors consume proven multi-output tensor programs for bucket
counts plus child metrics, including multi-field composite terms over configured
group fields. `docfact_rows`, `pathfact_rows`, and `join_fact_rows` can execute
bounded non-materialized folds through the same proof model.
Schemaless `pathfact` bucket folds can also apply proven pathfact predicate
constraints, including typed equality and string-prefix predicates, before
emitting term buckets and child metrics.

Algebraic bucket ordering is part of the public aggregation contract, not an
internal storage detail. Composite terms may use compact canonical tuple or
symbol-id keys on disk, but ties are ordered by the rendered public JSON bucket
key so algebraic, full-text-backed, and document-scan aggregation responses stay
checksum-equivalent across typed scalar encodings and shard-local dictionaries.

Configured and derived joins are deliberately exact-law only. Public
`algebraic_join` requests must carry configured join name, side roles, optional
temporal mode, and law-compatible aggregation semantics. Broader joins remain
out of scope unless side roles, fanout, temporal semantics, and merge laws can be
proved exact.

### Public Query Contract

Public search requests now have a canonical `query` field for structured query
trees. Compatibility request fields are normalized into that tree before
algebraic, full-text, vector, graph, and join planning: `full_text_search`
becomes scoring `bool.must`, `filter_query` becomes non-scoring `bool.filter`,
and `exclusion_query` becomes `bool.must_not`.

Structured filters support JSON-pointer-style `path` aliases beside `field`,
typed scalar `term` values for strings, numbers, booleans, and null, multi-value
`terms`, and `exists`. Query-string syntax remains accepted as a full-text escape
hatch, but it is not the canonical representation for algebraic filters or
typed fact planning.

### Adaptive Materialization

Query observations, path-profile observations, candidate scoring, lazy backfill,
readiness state, and dematerialization are engine-owned. Public status exposes
recommendation and progress health under `/tables/{table}/indexes/{index}` in
`status.algebraic`; there is no manual user-facing materialization lifecycle API.
Generated OpenAPI Zig types model index-status discriminators as typed
single-value enums, so `AlgebraicIndexStats` remains the one compact public
status envelope while generated code rejects mismatched stats variants.

Candidates are ranked with observed demand, estimated scan savings, write cost,
and measured owned sidecar bytes after backfill. Dematerialization removes owned
adaptive tensor/progress/candidate state and leaves the shape stale until future
observations requalify it. Candidate generations and decision-history rows are
persisted so status and benchmark output can explain policy drift, skipped
backfills, ready promotion, and automatic dematerialization decisions.

### Distributed Protocol

Distributed algebraic partials use canonical token semantics rather than
shard-local ids. Partial merge keys include canonical axis, metric or expression
identity, and law identity so distinct laws over the same coordinate do not
collide. Coordinator-side scalar and bucket merges route through tensor row merge
helpers.

Internal shard protocols carry access-path, tensor-expression, and tensor-program
envelopes. Shards re-prove owner, layout, fragments, dimensions, law ids,
dictionary identity, expression ids, and program ids before exporting partials.
`docfact`, `pathfact`, derived-join, and configured-materialization tensor
programs can return canonical partial rows without requiring a named
materialization request body. Configured aggregate fallback partials are sent as
planner-owned tensor program/access-path envelopes; named materialization lookup
is retained only as an internal storage utility for local sidecar scans and
tests.

Root cardinality, terms-with-nested-cardinality,
range-with-nested-cardinality, and histogram-with-nested-cardinality
distributed partials use planner-owned tensor program envelopes with canonical
distinct-value and bucket/range merge keys.

### Lexical Access Paths

Lexical access is shared by semantic `dictionary_identity`, not by planner. A
single owner publishes dictionary/postings/FST rows for a given identity; algebraic
planning consumes that advertised tensor access path instead of building a
duplicate FST. Analyzed full-text terms, canonical scalar facts, sparse tokens,
and graph labels remain separate identities when their semantics differ.
Adaptive path promotion writes lexicon/posting rows during partial backfill while
the dictionary registry is `building`, then rebuilds the promoted FST once and
marks the registry `ready` only when backfill reaches the end. Ready bulk ingest
continues to coalesce promoted dictionary dirtiness and rebuild each changed
dictionary once per batch.

Full-text remains responsible for analyzer-specific ranking behavior such as
BM25, phrase/proximity, highlighting, and analyzer state. Algebraic consumes exact
candidate tensors for filters, joins, vector pruning, and aggregate folds.

For relational tables, algebraic remains a derived index over the relational base
store. Fact projection, materialization maintenance, replay, and backfill hydrate
documents from committed relational base rows when they need a document body;
they do not read stale generic document KV values or derived text segment
columns as authoritative relational data. See [RELATIONAL.md](RELATIONAL.md) for
the one-store relational storage contract.

### Vector And Graph Integration

Supported algebraic `docfact` and `pathfact` filters can be resolved into native
include/exclude doc-id sets before dense or sparse vector traversal. Hosted
vector-worker requests carry raw symbolic filters plus a requirement that shard
execution resolve them algebraically or fail closed.

Graph indexes can opt into bounded provenance-semiring traversal. Eligible
traversal, shortest-path, and simple linear-pattern shapes prove a typed graph
access path and tensor program before using semiring execution. Ambiguous
provenance, weighted/non-deduped traversals, and richer graph patterns remain on
the normal graph executor until exact semantics are proven.

## Implementation Guide

This section is the working implementation contract. It folds the detailed notes
that used to live under short-, medium-, and long-term roadmap bullets into one
place, so the roadmap can stay focused on outcomes, ordering, and exit criteria.
When roadmap work introduces a new architectural invariant, move that invariant
here after the implementation proves it.

### Operating Rules

Algebraic execution remains opt-in, engine-owned, and evidence-driven. Schema
and query observation can derive capability plans, but public APIs must not
expose manual materialization lifecycle controls. Planner selection requires
ready lifecycle state, exact law proofs, matching dictionary identity,
dimensional compatibility, valid merge semantics, and benchmark-backed cost
gates.

The public contract is query capability plus status. External API, status, and
future protocol surfaces should use engine state, tensor expressions, access
paths, cleanup recommendations, and fallback reasons. Internal storage can keep
using `materialized_expr` as the durable tensor-cache row name because that is a
physical implementation detail, not a user-facing lifecycle API.

The stable public query surface is the normalized structured `query` tree.
Compatibility shorthands normalize into that tree at the API boundary. Typed
terms, path aliases, `terms`, and `exists` are the algebraic filter surface;
query strings remain a full-text escape hatch rather than the algebraic planning
contract.

### Public Status Surface

The default public index status should be compact and operator-focused. It should
answer whether the algebraic sidecar is enabled, ready, selected recently,
falling back, rebuilding, stale, or blocked, and it should expose enough counters
to see broad cost and health trends. It should not expose every adaptive
candidate, tensor expression, dictionary owner, decision-history row, or internal
materialized-expression detail by default.

`AlgebraicIndexStats` is the natural public envelope because it matches the
normal index-status shape used by the other index families. The normal public
OpenAPI response should not require users to understand separate
`AlgebraicRuntimeHealth` or `AlgebraicAdaptiveProgressStatus` schemas. Those are
implementation/debug concepts; collapse the few durable facts users need into
`AlgebraicIndexStats`, and keep the detailed runtime/adaptive records internal.
Fine-grained records such as `AlgebraicCapabilityStats`,
`AlgebraicAdaptiveCandidateStatus`, and
`AlgebraicAdaptiveCandidateDecisionStatus` are useful internal/debug data, but
they should not be stable public OpenAPI schemas. If operators need them, add an
explicit diagnostics/admin surface with separate compatibility expectations, or
emit them through benchmark/debug JSONL where the shape can evolve.

So the answer to whether the public API needs every algebraic status type is no.
The stable surface should expose one public index-status envelope and a small set
of durable fields. Capability records, adaptive candidate rows, candidate
decision history, runtime health internals, and progress rows are implementation
or diagnostics data. They are still important for benchmarks, debug endpoints,
and internal recovery decisions, but making each one a public schema would lock
the storage and adaptive policy model too early.

This is the compatibility rule for the public API:

```text
public:   AlgebraicIndexStats
internal: AlgebraicCapabilityStats
internal: AlgebraicAdaptiveCandidateStatus
internal: AlgebraicAdaptiveCandidateDecisionStatus
internal: AlgebraicRuntimeHealth
internal: AlgebraicAdaptiveProgressStatus
```

The public record should summarize internal state with counts, current
lifecycle/readiness fields, last planner decision, last fallback reason, simple
progress counters, and coarse health/error counters. It should not expose
candidate rows, decision-history entries, tensor proof objects, dictionary
registries, materialized-expression identifiers, exact error document keys, or
sequence-level adaptive worker cursors as normal user-facing status.

The stable public status fields should be durable concepts, not implementation
tables:

```text
enabled/healthy/ready/rebuilding/stale
planner selected/fallback counts and latest fallback reason
schema capability lifecycle status and rebuild reason
adaptive materialization lifecycle counts
active backfill or cleanup progress summary
owned derived bytes and indexed document count
```

The preferred stable schema shape is a small `AlgebraicIndexStats` record with
fields like status, readiness, rebuild/stale/blocking reason, lifecycle counts,
planner last decision/fallback reason, owned derived bytes, and indexed document
count. If active progress is exposed, keep it embedded and simple, such as an
operation name plus percent or item counters, rather than a standalone adaptive
progress object.

Detailed decision history, candidate scoring inputs, policy-drift records,
canonical recommendation hashes, dictionary ownership rows, error document keys,
worker sequence cursors, and tensor-program proof internals belong in
debug/admin diagnostics or benchmark JSONL, where they can evolve without
becoming a broad public compatibility burden.

### Coverage And Selection

Every optimized path needs benchmark coverage against scan and full-text
aggregation baselines with query latency, write amplification, owned derived
bytes, update/delete churn, cold and warm reads, join fanout, constrained
predicates, and LSM behavior. Planner defaults stay conservative until the
LSM-backed numbers show a durable win.

Benchmark output should expose planner estimates, selected/fallback decisions,
adaptive candidate status, decision history, policy drift, dictionary registry
state, vector symbolic-pruning counters, graph traversal counters, and partial
merge validation status. A fast query path is not enough; the summary must make
storage and write cost visible.

CI-safe benchmark fixtures prove coverage shape, parser behavior, and regression
plumbing. They are not production performance evidence by themselves. Production
claims require larger repeatable runs with checked-in or archived baseline
summaries, hardware/environment notes, and threshold tolerances chosen from
measured variance rather than the tiny guardrail fixture.
The archive verifier supports explicit minimum environment floors for documents,
repeats, churn operations, public-query documents, graph documents, adaptive
documents, and cold-read documents so non-smoke archives can prove the scale
they claim instead of relying on the `smoke=0` label alone.

Current local non-smoke archive evidence:

```sh
zig build algebraic-archive-guardrail -- \
  --archive bench/results/algebraic-production-hardening/interactive-current-10k-post-resource-dist-envelope \
  --require-thresholds \
  --require-non-smoke \
  --min-docs 10000 \
  --min-repeats 2 \
  --min-churn-ops 100 \
  --min-public-docs 1000 \
  --min-graph-docs 1000 \
  --min-adaptive-docs 1000 \
  --min-cold-docs 500
```

This thresholded archive passes the verifier and covers 23 LSM dataset cases,
185 LSM query records, 63 algebraic query records, 61 doc-scan query records,
61 full-text query records, cold/warm reads, constrained rollups, wide keys,
stats/cardinality/range/histogram shapes, join fanout, update/delete churn,
adaptive materialization coverage, graph traversal, and public
no-schema/schema/schema-plus-algebraic comparisons. The summary reported zero
correctness failures, 391 sorted-ingest LSM runs, zero normal LSM flushes, zero
write-pressure compactions, zero path-dictionary FST rebuilds, max algebraic
query latency of about 232 ms, max algebraic bytes per document of about 90.5
KiB, max churn update latency of about 449 ms, and public handler-mode algebraic
vector filtering slower than the no-schema/schema baselines. These numbers are
evidence for the current implementation state, not release thresholds; adaptive
warmup/churn cost and public-query overhead remain explicit optimization
targets.

`zig build algebraic-planner-ownership-guardrail` is a repo policy check under
`tools/guardrails/`, not a benchmark. It enforces that production API, graph,
and storage DB code do not construct raw tensor programs outside the algebraic
planner/IR layer. Test blocks may still build explicit programs to exercise
protocol validation and executor rejection behavior.

Failure-injection coverage should target stale lifecycle state, missing or
conflicting dictionary ownership, adaptive backfill interruption, distributed
partial validation failure, symbolic filter fail-closed behavior, and
dematerialization cleanup.

### Fact And Schema Lifecycle

Schema-derived `docfact` rows are the correctness boundary for declared typed
fields. Schemaless `pathfact`, `path_lookup`, and `path_profile` rows are the
discovery substrate for late-typed promotion. Mixed-kind paths require
kind-qualified plans, explicit coercion policy, or fallback.

Schema lifecycle drift marks the algebraic capability stale or rebuild-required
while migration is in progress. Durable regeneration records
`capability_lifecycle_status: "rebuild_required"` for crash safety, and local
live reload persists that pending state before clearing algebraic rows. The
index manager then replays committed base rows through the refreshed config and
persists `capability_lifecycle_status: "current"` after success. While pending,
the planner declines schema-derived algebraic execution, favoring correct scan
fallback over reading facts that only cover the post-change subset.

Relational embedded JSON domains apply that lifecycle per column path. Durable
schema regeneration and local schema reload preserve user-owned knobs, compare
each `json_subdocument_domains` fingerprint, and mark changed domains
`lifecycle_status: "rebuild_required"` until local replay has reprojected the
committed relational rows. `Index.fieldConfig` declines both static and dynamic
fields under that JSON path while the domain is pending, so queries either fall
back or report pending capability instead of reading stale facts. Fields outside
the pending JSON column remain eligible for algebraic execution.

### Adaptive Lifecycle

Adaptive materialization is driven by persisted observations, path profiles,
policy scores, readiness state, measured storage/write cost, and decision
history. Policy drift is durable state, so future scoring changes must preserve
enough history to explain why a candidate was skipped, backfilled, promoted,
dematerialized, rebuilt, or left stale.

Automatic de-materialization must be safe and reversible. Owned adaptive tensor,
candidate, progress, and cleanup rows can be removed when workload demand no
longer justifies them. Base documents, fact rows, path profiles, dictionary
ownership, and schema-derived capability metadata remain the source for future
re-materialization.

### Shared Access Paths

Lexical access paths are shared by semantic `dictionary_identity`, not by planner
or index owner. Algebraic planning consumes an existing full-text,
path-promotion, sparse-vector, or graph dictionary/postings/FST layout when its
semantic identity exactly matches the query. It must not build a duplicate FST
for the same field, analyzer, label kind, canonicalization version, value kind,
and coercion policy.

Distinct semantics justify distinct dictionaries: analyzed full-text terms,
canonical scalar facts, sparse vector tokens, and graph labels can share physical
machinery without sharing identity. Any consumer that cannot prove identity
equality must either fall back or create a separate dictionary for its distinct
semantic dimension.

### Planner Ownership

New query shapes should compile into typed tensor programs from the normalized
query tree, not into one-off execution paths. Current planner-owned tensor
builders cover configured materialized metrics, derived join folds,
schema-derived `docfact` bucket folds including distributed multi-output
histogram/range/date-range envelopes, schemaless `pathfact` bucket folds
including distributed terms/histogram/range/date-range envelopes, dense/sparse
vector-search programs with optional native doc-id constraint inputs, and
provenance-semiring graph traversal/edge programs with optional target
constraints where applicable. Catalog and API code should discover and validate
access paths, but planner-owned code should construct the tensor programs for
eligible vector, graph, aggregate, and join shapes.

Additional query shapes should extend `storage/db/algebraic/planner.zig` rather
than constructing executor-local tensor programs. Fallback cases should move
onto shared tensor execution only when benchmark data shows a durable win over
the existing scan, full-text, vector, or graph path.

Join expansion stays exact-law only: side roles, fanout, temporal semantics, and
merge laws must be configured and provable. Broader SQL-style join planning is
not a goal unless the configured algebraic laws preserve exact public semantics.

### Distributed Execution

Distributed partials use canonical token semantics and law-aware merge keys.
Shard-local symbol ids are physical only. Exported partials carry canonical axis
identity, expression or metric identity, law identity, dictionary identity where
relevant, and enough tensor-program metadata for the coordinator to re-prove
exact merge behavior.

Protocol work should continue replacing named aggregate sidecar requests with
tensor/access-path envelopes. The distributed merge layer now has a
`PartialProtocol` envelope validator that rejects owner, layout, fragment,
dimension, law, metric, dictionary identity, expression id, and program id
mismatches before merging canonical partial rows. Tensor-program partial export
can now return `PartialEnvelope` rows, so shard executors have a concrete
boundary type for attaching that protocol metadata as rows leave a shard.

### Vector And Graph Execution

Symbolic algebraic constraints are pushed into dense, sparse, hosted vector, and
graph execution only when they can be resolved exactly and fail closed when
required. Vector and graph paths should advertise physical tensor access paths
that the shared planner can prove, rather than special-casing algebraic
internals.

Dense vectors keep ranking/search semantics in the vector executor. Full-text
keeps BM25, phrase/proximity, highlighting, and analyzer-specific ranking in the
lexical executor. Graph traversal keeps non-proven traversal semantics in the
graph executor. Algebraic participates when it can provide exact candidate
tensors, constraints, folds, or semiring programs.

### Runtime Optimization

Physical optimization follows measurement. The key levers are symbol-id row
layout, row locality, batching, bulk join fanout, dictionary/FST sharing, cold
reads, support-row compaction, update/delete churn, and write amplification.
Bulk ingest should follow the dense-index pattern where possible: compile shared
batch state once, then stream document mutations through that state. Algebraic
now caches decoded ready adaptive materialization specs across consecutive bulk
maintenance batches and invalidates that cache whenever adaptive observation,
progress, or materialization lifecycle state changes. The cached specs are
reused for schemaless path promotion, direct adaptive tensors, and ready join
tensors. Benchmark JSONL emits
`algebraic_adaptive_maintenance_plan_build_count`,
`algebraic_adaptive_maintenance_cached_spec_count`, and
`algebraic_adaptive_maintenance_disabled_count` on status/churn events and
rolls them into `performance_evidence_summary`, so archived runs can prove the
path was exercised. This avoids repeated LSM cursor scans of adaptive
progress/state rows during churn-heavy overwrite/delete batches while preserving
exact stale marking for non-invertible laws inside the batch. Invertible ready
adaptive tensor deltas, including promoted path-fact count tensors, are also
coalesced inside the bulk maintenance context and flushed once per affected
tensor/expression row before the batch commits. Non-append bulk ingest now also
opens a cursor-capable write batch, so overwrite/delete maintenance, configured
joins, adaptive joins, and schemaless path promotion can share the same LSM
bulk-ingest transaction window instead of falling back to a plain write
transaction whenever cursor scans are required.
The catalog attaches the shared `ResourceManager` to algebraic indexes at open
time. Algebraic now reports coalesced tensor accumulator working memory under
the `algebraic.tensor_accumulators` slice and releases that usage when the batch
or index closes. Backend runtime wiring remains inherited from DB/table open:
algebraic writes execute through the same DocStore/LSM runtime store as the
primary and other managed index paths.
Internal algebraic status also exposes `algebraic_path_dictionary_fst_rebuild_count`
to benchmark JSONL. `algebraic-summary` rolls it into
`total_path_dictionary_fst_rebuild_count` and
`max_path_dictionary_fst_rebuild_count`, with
`--max-path-dictionary-fst-rebuild-count` available for archived evidence. This
keeps the path-promotion dictionary/FST rebuild optimization measurable without
expanding the compact public index stats shape.

Churn benchmarks also emit row-family counters for `materialized_expr`,
`docfact`, `pathfact`, `path_lookup`, `path_profile`, `joinfact`, `docjf`,
`minmax`, and `sym`. These counters report before/after entries and bytes for
each family plus the total workload update time. They are intended to identify
which physical families dominate churn, not to claim per-family isolated timing.
The first May 17, 2026 small bulk-ingest smoke showed `docfact`, `pathfact`, and
`path_lookup` dominate sidecar bytes while `minmax` and `sym` remained small.
The overwrite path now coalesces fact/path updates by skipping the pre-delete
for overwritten upserts, diffing old and new `docfact` lookup rows, diffing old
and new `path_lookup` rows, and only rewriting `docfact`/`pathfact` payload rows
when their encoded payloads changed. Aggregate, adaptive, path-profile, and join
deltas still run through their exact old-minus/new-plus maintenance paths when
the projected dependency changes. A later pass made that skip explicit for
unchanged profile rows, unchanged path-promotion facts, and unchanged docfact
adaptive/direct tensors. On the same small LSM bulk-ingest smoke, adaptive churn
dropped from about 1.8s to about 0.1s after row coalescing, then to about 0.03s
after adaptive/profile skip gating for the isolated LSM adaptive case. The
combined smoke archive's max algebraic churn dropped from about 1.8s to about
0.08s. After routing non-append bulk ingest through the cursor-capable write
batch, a May 17, 2026 focused LSM smoke with 100 docs, 8 churn ops, batch size
50, adaptive profile, and `--algebraic-bulk-ingest` measured 24.981ms algebraic
update time versus 6.237ms full-text update time, with correctness checks
passing. A follow-up 120-doc focused comparison with 64 customers and 16
products measured direct LSM build/churn at about 278ms/157ms and bulk LSM
build/churn at about 20ms/33ms. Adaptive constrained terms now choose between
lazy point lookups for tiny bucket sets and preloaded child metric row maps for
larger bucket sets; the same bulk smoke moved constrained terms from about
0.70ms to about 0.50ms while preserving checksums. Configured and adaptive
materialized tensors now stay on the normal tensor-law mutation path inside
cursor-capable bulk write batches. The fact-only append-only fast path remains
available for schema/fact ingestion, but preaggregated materialization
accumulation is guarded off until it has the same production correctness
evidence as the tensor-law path. This keeps LSM direct sorted ingest for bulk
batches without treating repeated aggregate-row mutations as a separate
append-only fold.
Path-promotion dictionary maintenance now batches promoted lexicon/posting
changes during bulk maintenance. Standalone bulk batches rebuild each dirty FST
once before commit; DB-level bulk-ingest sessions carry dirty promoted
dictionaries across every flushed coalescer batch and rebuild each one once at
session finish before the primary store publishes the final sorted run. Non-bulk
writes keep immediate rebuild semantics. Primary document writes inside an
external DB bulk-ingest session now also open their LSM write batch with
`BatchOptions{ .mode = .bulk_ingest }`, which lets the primary store use the
same direct sorted-ingest fast path as algebraic/dense sidecars instead of only
benefiting from the elevated active-session flush threshold. Algebraic benchmark
dataset rows expose `algebraic_lsm_flushes`,
`algebraic_lsm_flush_output_runs`, `algebraic_lsm_sorted_ingest_runs`,
`algebraic_lsm_sorted_ingest_bytes`, and
`algebraic_lsm_write_pressure_compactions`; `algebraic-summary` rolls those into
`performance_evidence_summary` and supports
`--min-lsm-sorted-ingest-runs` so archived LSM bulk runs can prove the direct
ingest path stayed active. Path-promotion FST rebuild counts are also rolled up
and bounded with `--max-path-dictionary-fst-rebuild-count` so archives can catch
accidental per-row rebuild regressions. LSM flushes and write-pressure
compactions can be bounded with `--max-lsm-flushes` and
`--max-lsm-write-pressure-compactions` for archives that should stay on direct
sorted ingest. These LSM thresholds are part of the algebraic sidecar contract:
they prove that algebraic LSM-backed sidecars are not regressing into normal
flushes or pressure compactions under algebraic workloads. They are not intended
to certify generic LSM performance; standalone compaction policy, WAL, block
cache, and read-amplification questions belong in storage-specific LSM
benchmarks. The summary also reports
`unclassified_algebraic_comparisons`, adds a `correctness_record` flag to query
summaries, and `--require-performance-evidence` requires unclassified
comparisons to remain zero so new algebraic benchmark comparisons cannot bypass
correctness classification. The wide-key benchmark issues the public
multi-field `terms.fields` shape over the same canonical group axes as its
configured materializations, so it is now an exact comparison instead of a
deliberately classified shape mismatch. The production-hardening runner forwards LSM tuning
environment knobs such as `ALGEBRAIC_HARDENING_LSM_FLUSH_THRESHOLD`,
`ALGEBRAIC_HARDENING_LSM_FLUSH_THRESHOLD_BYTES`,
`ALGEBRAIC_HARDENING_LSM_BULK_INGEST_FLUSH_THRESHOLD_MULTIPLIER`,
`ALGEBRAIC_HARDENING_LSM_BULK_INGEST_FLUSH_THRESHOLD_BYTES_MULTIPLIER`,
`ALGEBRAIC_HARDENING_LSM_DIRECT_BULK_INGEST`,
`ALGEBRAIC_HARDENING_LSM_COMPACT_THRESHOLD_RUNS`,
`ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_RUNS_BASE`,
`ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_RUNS_MULTIPLIER`,
`ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_BYTES_BASE`, and
`ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_BYTES_MULTIPLIER`, so archived bulk
runs can intentionally force or relax direct-ingest eligibility, row-count and
byte-count flush thresholds, active bulk-session threshold multipliers, and L0
debt behavior. The `dataset_lsm_config` benchmark event records these values so
archive summaries can be tied back to the exact LSM finish-session policy under
test. The May 18, 2026 adaptive churn microbench showed the ready-spec cache
reduced repeated maintenance-plan builds substantially, but the materialized
adaptive LSM churn case remained dominated by lower-level sidecar mutation cost;
larger archive work should keep that as an open optimization target rather than
treating the cache as a complete churn fix. The algebraic benchmark and
production-hardening runner now expose LSM bulk finish controls for publish-only,
flush-on-finish, compact-on-finish, deferred-L0 targets, and bounded foreground
compaction steps/bytes/time so archives can compare publish latency against
maintenance debt. External DB bulk finish now publishes the primary LSM session
before forcing managed-index catch-up to `full_index`, so algebraic sidecar rows
can fold the final coalesced documents and survive a durable LSM reopen at the
user-visible finish boundary. The production-hardening runner also forwards the
selected LSM bulk-ingest flags to the cold/warm read stage. Cold-read archives
should measure reopen behavior over the same direct sorted-ingest sidecar layout
as the scale, adaptive, and churn stages when
`ALGEBRAIC_HARDENING_LSM_BULK_INGEST=1`, rather than forcing a normal non-bulk
build that creates unrelated flushes. MIN/MAX support compaction and per-row
path-promotion FST rebuilds are no longer the primary bottlenecks in this
workload.

Optimizations should preserve canonical merge semantics across shards even when
local storage uses compact ids.

### Law Expansion

New algebraic structures should be added only when they unlock exact planner
capabilities. Semirings, lattices, provenance/path algebras, and domain laws need
declared identity/merge behavior, tensor proof hooks, distributed merge tests,
and targeted benchmarks. They should not be added as unused abstractions.

### Applying This Guide

Roadmap work should update this guide only when it changes an implementation
rule that future work must preserve. Completed behavior belongs in
Implementation State. Temporary sequencing, estimates, and exit criteria belong
in the Roadmap. Code-specific details belong beside the relevant tests or module
docs once they are no longer architectural constraints.

## Roadmap

The previous short-, medium-, and long-term design rules have been folded into
the Implementation Guide, and completed behavior has been moved into
Implementation State. The active roadmap now tracks production-hardening work
only. When one of these items lands, move the durable facts into the sections
above and remove the temporary roadmap item.

### Production Hardening

1. Establish real performance baselines.
   Run larger LSM analytics, adaptive coverage, public query, vector pruning, and
   graph traversal benchmark suites on representative data. Archive the JSONL
   summaries, record hardware/environment notes, and set regression thresholds
   for latency, bytes, symbol/support-row growth, accumulator flushes, public
   query RSS, write cost, churn, cold/warm reads, fanout, constrained queries,
   and public query behavior. Use
   `scripts/run_algebraic_production_hardening.sh` as the canonical local runner
   so every archived run includes the same raw JSONL, combined summary, and
   environment metadata. Public-query archive comparisons default to
   `ALGEBRAIC_HARDENING_PUBLIC_MODE=handler`, which exercises the public query
   handler and planner without a TCP listener; use
   `ALGEBRAIC_HARDENING_PUBLIC_MODE=local` or `swarm` for transport/server
   overhead runs. Symbolic profile enforcement is a separate opt-in
   (`ALGEBRAIC_HARDENING_PUBLIC_REQUIRE_SYMBOLIC_PROFILE=1`) for archives that
   specifically validate vector-pruning profile counters. Keep the default
   combined run direct unless the run is specifically validating LSM bulk ingest; set
   `ALGEBRAIC_HARDENING_LSM_BULK_INGEST=1` for that stress path and archive it
   separately. Set `ALGEBRAIC_HARDENING_BASELINE` plus threshold env vars once
   representative runs establish acceptable variance, so production archives
   fail closed on correctness, coverage, latency, byte, and churn regressions.
   Validate any archive used as evidence with
   `zig build algebraic-archive-guardrail -- --archive <dir>
   --require-thresholds --require-baseline --require-non-smoke`.

2. Prove full-suite test health.
   Run the broad repo test/build matrix with algebraic enabled and disabled.
   Separate unrelated existing failures from algebraic regressions, then keep a
   focused algebraic CI path plus a periodic broader integration path. Use
   `scripts/run_algebraic_integration_matrix.sh` as the local evidence runner:
   its default lanes cover the disabled/default public-query path, schema-only
   path, schema-plus-algebraic path, focused algebraic DB tests, and a
   provisioned distributed fallback test whose name intentionally does not match
   the broad `--test-filter algebraic` lane. Set
   `ALGEBRAIC_MATRIX_WARM_BUILDS=0` only when the build cache is already known
   warm; the default warm-build pass keeps archived lanes from failing during
   first-compile setup rather than actual algebraic behavior. Set
   `ALGEBRAIC_MATRIX_RUN_UNIT_TEST=1` and `ALGEBRAIC_MATRIX_RUN_E2E=1` for the
   broader periodic matrix, and archive the resulting `environment.txt`,
   `warm-builds.txt`, `commands.txt`, `status.tsv`, and per-lane stdout/stderr
   artifacts.

3. Harden crash, recovery, and rebuild behavior.
   Add failure-injection coverage for interrupted adaptive backfill,
   dematerialization cleanup, corrupt or stale sidecar rows, schema drift during
   ingest, dictionary owner conflicts, and reopen/replay across the LSM backend.

4. Exercise sustained write-path pressure.
   Run long overwrite/delete/churn workloads for aggregate tensors, MIN/MAX
   support rows, temporal buckets, joins, schemaless facts, adaptive cleanup, and
   bulk LSM ingest. Track sidecar bytes, support-row growth, flush counts,
   cached bulk-maintenance plan hits, row-family churn counters, and update
   latency. Keep the cursor-capable bulk write-batch path in the archive so
   regressions in configured/adaptive tensor maintenance show up next to
   fact-only append and direct-write runs.

5. Validate distributed and sharded failure modes.
   Test mixed shard-local symbol ids, canonical merge keys, missing/stale shards,
   aggregate law-family partial merges, rejected tensor envelopes, partial
   validation failures, vector fail-closed routing, and graph traversal
   proof/reject behavior across shard fanout. Positive coverage currently exists
   for canonical same-token/different-local-dictionary merges, aggregate law
   families (count/sum/sumsquares/avg/min/max), distributed stats/date/terms/
   range/histogram/cardinality folds, and derived join partials; archived
   integration runs should still add negative shard-health and stale/
   missing-shard evidence.

6. Bound memory and storage growth.
   Measure and cap symbol registry growth, docfact/pathfact volume, path-profile
   cardinality, dictionary/FST ownership, support-row compaction, accumulator
   memory, and derived sidecar bytes under high-cardinality workloads.

7. Keep the external API capability-oriented.
   Verify that users request query capability and observe readiness/status only;
   manual materialization lifecycle controls, internal expression ids, and
   engine-owned adaptive transitions must stay out of the stable user-facing API.
   Keep normal index status shaped as a single compact `AlgebraicIndexStats`
   record, matching the other index types. Do not expose
   `AlgebraicCapabilityStats`, `AlgebraicAdaptiveCandidateStatus`,
   `AlgebraicAdaptiveCandidateDecisionStatus`, `AlgebraicRuntimeHealth`, or
   `AlgebraicAdaptiveProgressStatus` as public schemas in the default status
   response; collapse durable readiness, lifecycle, planner fallback, and simple
   active-progress facts into `AlgebraicIndexStats`. If detailed
   adaptive/capability diagnostics are needed, expose them separately from normal
   index status and keep the default OpenAPI schema compact. Treat additions to
   `AlgebraicIndexStats` as a compatibility-budget decision: prefer coarse
   counters and latest operator-relevant reasons, and keep candidate lists,
   proof objects, expression ids, dictionary registries, policy-scoring inputs,
   exact error document keys, decision-history counts, policy-drift counts,
   planner estimates, distributed proof counters, vector-filter internals, and
   adaptive worker sequence cursors out of the stable response. Keep one positive
   contract test for the generated public shape instead of migration-style tests
   for unshipped alternatives.

### Non-goals

```text
arbitrary query-time joins
automatic typed materialization of every schema field or schemaless path
automatic materialization of every aggregate combination
manual user-facing materialization lifecycle or explicit materialization definition APIs
best-effort approximate MIN/MAX
using the algebraic sidecar when status indicates incomplete derived state
```

## Approximate cardinality (HyperLogLog)

An index can materialize approximate distinct-counts so a `cardinality`
aggregation is answered from a per-group sketch instead of scanning and
deduplicating every value. Configure it with `hll_cardinalities`:

```json
{
  "hll_cardinalities": [
    {"name": "customers_by_region", "group_by": ["region"],
     "value_field": "customer", "precision": 14}
  ]
}
```

`group_by` are the bucket axes, `value_field` is the field whose distinct values
are counted, and `precision` (4–18, default 14) sizes each sketch at
`2^precision` bytes and sets its accuracy.

### Result contract

Every `cardinality` result is self-describing, so a client can always tell an
estimate from an exact count and reason about the error budget:

```json
{"value": 4044, "approximate": true, "relative_error": 0.0081}   // from a sketch
{"value": 4096, "approximate": false}                            // exact distinct scan
```

`relative_error` is the HyperLogLog standard error of the sketch that produced
the value, `1.04 / sqrt(2^precision)` — about 1.6% at p=12 and 0.8% at p=14. It
is present only when `approximate` is true.

Queries choose the exact/approximate contract with `mode` on a `cardinality`
aggregation:

- `auto` (default): use a matching current sketch when one applies; otherwise
  fall back to an exact distinct scan.
- `exact`: always run the exact distinct scan and return `approximate: false`.
- `approximate`: require a matching current sketch. If no sketch applies, the
  query fails instead of silently scanning.

### Selection and maintenance

The planner answers a `cardinality` from a matching sketch automatically when
the query has no constraints and no MVCC read generation (sketches are
maintained unconstrained and without per-generation visibility), a sketch's
`group_by`/`value_field` match the query, and that materialization is not
mid-rebuild; otherwise it falls back to the exact distinct scan (which reports
`approximate: false`). Deletes and overwrites mark the affected groups dirty and
rebuild only those groups' sketches in the background.

`hll_cardinalities` is user-owned algebraic index configuration. Schema-derived
regeneration and live schema reload preserve it alongside the other runtime
knobs, so dynamic-template or schema updates do not drop configured sketches.

Cardinality sketches can also be promoted adaptively. Repeated cardinality
queries are observed in the same adaptive-materialization stream; once a
leader-gated candidate crosses the promotion threshold, the index records an HLL
configuration for that shape, backfills the sketch, and then maintains it with
the same dirty-group rebuild path used by static `hll_cardinalities`.
