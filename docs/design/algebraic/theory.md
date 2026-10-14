# Algebraic Sparse-Token Theory: Extended Notes

> Relocated verbatim from `zig/ALGEBRAIC.md` (lines 16–923 at commit c6c0ba51cd) on 2026-10-12. Durable decisions were folded into [ALGEBRAIC.md](../../../zig/ALGEBRAIC.md) before the move; it keeps the definitions, laws, and operator/join semantics the implementation relies on. This note keeps the original derivations, worked tokenization and temporal-cylinder examples, the end-to-end COUNT/MAX example, and the original prototype sketch.

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
zig/pkg/antfly-embedded/src/storage/db/algebraic/
  token.zig        canonical token -> coordinate_id
  vector.zig       sparse formal vector / deltas
  algebra.zig      count, sum, min, max, avg, and support-state traits
  cylinder.zig     temporal rollup keys and bucket math
  index.zig        materialized token/cylinder store
  planner.zig      route supported aggregation requests to the algebraic index
```

The module has since grown well beyond this first cut (fact, path, join, law,
lexical, distributed, adaptive, HLL, and planner-control files, among others).

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
