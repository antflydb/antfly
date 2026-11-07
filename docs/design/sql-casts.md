# Database-scoped SQL casts

SQL casts are catalog objects identified by an ordered source/target type pair
inside a database. They share the existing catalog transaction, decoder barrier,
callable dependency directory, and bounded mutation-outcome journal. The support
function has a namespace; the cast itself does not. PostgreSQL 18 is the SQL
contract and independent oracle.

## Publication and recovery

The public SQL producer captures a catalog revision and immutable database ID,
resolves the exact support-function signature at that revision, and authorizes
publication before submitting one native command. The native transaction
revalidates database and function identity, signature, return type, and duplicate
admission. The row, reverse dependency, catalog revision, and retained outcome
commit together. Generic catalog writes cannot bypass this path.

One, two, or three support arguments are supported. They receive the source
value, optional integer type modifier, and optional explicitness boolean.
Validation precedence follows PostgreSQL: an invalid support function is
reported before an existing cast. Immutable PostgreSQL builtin cast identities
cannot be replaced. Their generated identity table is independently reproducible
from `pg_cast`; automatic string I/O conversions are not mistaken for catalog
objects.

The shared callable-dependency directory tags routine and cast owners. Rebuild
and restore validation check the exact support signature and common database.
Function retirement uses that directory for RESTRICT/CASCADE; database deletion
cannot orphan a cast. Decoder capability 110 covers cast publication and the
tagged directory representation.

An ambiguous response returns a compact token containing an outcome identity,
not SQL text or authority. Recovery only observes the journal; it never
resubmits a mutation. It resolves the immutable database ID, authorizes the
current database name, and checks catalog incarnation. Database rename and old
name reuse cannot redirect the token. Cold replay returns the retained result
without resurrecting a subsequently retired cast.

## Binding and execution

The statement owner caches positive and negative pair lookups at its catalog
cut. Builtin cast identities skip metadata lookup. Support functions bind by
immutable identity through the existing typed routine VM, with ordinary work,
NULL, cancellation, and output-ownership rules. Row execution performs no
catalog lookup or function-body parsing. Reusable row scratch bounds allocation
retention; the cast function can still allocate its output.

Prepared resources retain cast identity, generation, database ID, and catalog
incarnation. An absent cast is also a dependency: publishing one invalidates an
already prepared builtin conversion. Function replacement independently
invalidates its supporting-function witness. Context validation distinguishes
explicit conversions, assignments, and implicit function argument conversions.
Target numeric modifiers survive both inference and assignment emission.

Stored schema and view programs currently bind builtin conversions. New stored
definitions that require a custom cast are rejected before publication, until
their durable dependency protocol is implemented. Existing views retain builtin
binding when a custom cast is later created; nested view binding temporarily
uses the view's resolver and restores the caller's resolver afterward.

## Qualification and remaining scope

`scripts/generate_sql_cast_reference.py` records PostgreSQL results for original
cases `sql-0139` through `sql-0141`, duplicate and invalid declarations, conversion
contexts, SQL NULL versus JSON null, numeric modifiers, namespace rename,
function retirement, name reuse, and prepared-resource dependencies. The strict
`antfly-api-sql-cast-test` Debug campaign executes those original statements
through native authenticated public owners, checks cold postimages, and injects
a committed mutation with a lost acknowledgement. The metadata callable gate
also checks dependency rebuilding, exact rejected replay, cold retirement, and
database identity after rename and name reuse. The SQL kernel gate covers typed
execution, repeated-row binding reuse, numeric modifiers, and view boundaries.

Publication currently supports function-based explicit and assignment casts
between native builtin type identities. AS IMPLICIT, WITH INOUT, binary casts,
named enum/domain identities, and durable custom-cast dependencies in stored
schema/view definitions remain unsupported. This qualification does not certify
the full PostgreSQL coercion search graph or close unrelated type DDL cases.

Normative reference: [PostgreSQL 18 CREATE CAST](https://www.postgresql.org/docs/18/sql-createcast.html).
