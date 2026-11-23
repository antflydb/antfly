# Native SQL routines

Scalar SQL-language functions are catalog objects, separate from relation names.
A function is identified by namespace, name and canonical input-type signature;
the result type is not part of overload identity. CREATE allocates an immutable
catalog ID. CREATE OR REPLACE retains that ID and owner and advances the
function generation. Changing a result type or an existing argument name is
rejected before publication. DROP resolves a particular signature and fences
against its admitted ID and generation.

The compiled plan owns routine body bytes independently of request storage and
lexer scratch. Plan-cache hits cannot borrow a prior HTTP request or decoded
string token; cache poisoning and allocation-failure regressions verify this
boundary for quoted SQL functions and procedural bodies.

The declaration parser retains typed positional/named inputs, result domain,
SQL body, strictness and volatility. Unnamed inputs expose `$n`, never invented
`arg1` names. PostgreSQL is the behavioral oracle, including declarations that
must fail body analysis. Unknown literals participate in category/preferred-type
overload resolution; typed values retain their input domains. Exact NUMERIC
arguments and results do not pass through floating point.

## Execution ownership

A statement owner captures authorized namespace lookup and a catalog revision,
uses covering signature entries to select an overload, and loads only the
selected definition. The owner prepares immutable typed body programs and
caches them by ID/generation. Repeated calls share those programs. Metadata
reads, parsing and overload selection never occur in the row loop.

The scalar VM evaluates actual arguments once and enters the body with an owned
argument frame. The caller and callee share work, cancellation, depth and byte
budgets. Small argument frames use stack storage. Strict calls preserve NULL
semantics. A 10,000-row Debug regression executes an integer successor function
with a zero-byte execution allocator and asserts no catalog callbacks after
binding. This is an allocation/callback invariant, not a throughput claim.

Prepared manifests retain function signature, ID, generation and cluster
incarnation. Execution reauthorizes the captured namespace and verifies the
manifest at a fresh statement cut. A search-path edit cannot retarget the call;
a replaced definition or reused name requires a new binding.

## Durable publication and recovery

Distributed and standalone publication share the native catalog transaction and
bounded catalog outcome journal already used by sequence DDL. A nonce binds the
full declaration digest and immutable namespace/cluster identities. Native apply
validates and prepares the body before writing catalog/index changes. Its
preparation allocator is bounded. Deterministic definition failures are retained
rejections; allocator/storage failures abort the transaction.

Decoder floor 109 activates callable publication, retained annotations and
operator/aggregate dependencies with version 2 routine records. Linked control reaches the
same concrete native owner through catalog projection interfaces, preserving
request deadlines and cancellation. Routine records have no physical table,
placement or storage binding. Parent schema/database emptiness checks include
routines, so parent deletion cannot orphan them. Cold reads validate canonical
signatures through the ordinary catalog indexes.

The producer reserves a recovery receipt before submitting a mutation. An
ambiguous reply triggers read-only observation of that exact operation, never a
second submission or inference from an object's current name. Versioned recovery
tokens contain a compact scoped nonce/digest, not function bodies or authority.
Native journal digests commit the observation scope as well as the full request
hash, preventing a token's namespace/name from being edited independently. Every
observation rechecks live namespace administration.

## Callable annotations

`COMMENT ON FUNCTION` and `COMMENT ON PROCEDURE` resolve the exact input
signature using the same authorized search-path cut as callable DDL. The native
transaction fences the namespace, cluster incarnation, object ID and executable
generation. It updates the ordinary catalog resource description and appends an
exact outcome in the existing bounded journal. Comments do not recompile bodies,
allocate a new callable ID, advance its executable generation, rewrite rows or
create index work. A subsequent CREATE OR REPLACE preserves the description;
DROP removes it with the object, so name reuse cannot inherit an old comment.
NULL and empty comments remove the description, as PostgreSQL does.

Descriptions are opt-in snapshot data. Execution capture does not transfer
annotation text. Text is bounded to 64 KiB, validated before admission, and is
never interpreted as SQL. Lost acknowledgments use the same read-only,
kind/scope-bound recovery path as publication. Stale executable identities are
retained rejections; a name lookup cannot retarget a recovered annotation.

## Native binary operators

Binary SQL operators use the same catalog resources, owner capture, scalar VM,
prepared manifests and bounded outcome journal as functions. The catalog keeps
operator signatures in a separate family, so a quoted function with the same
symbol and argument types cannot collide. Each operator references an immutable
function ID and namespace ID. Replacement follows the function generation;
namespace rename follows its identity; dropping and recreating a function name
never retargets an existing operator.

Binding prepares the referenced function once. A 10,000-row regression uses a
zero-byte execution allocator and checks that catalog callbacks stop after
binding. A covering dependency directory bounds function retirement to its
actual dependents, rather than scanning unrelated catalog objects. Native
publication and restore rebuild/validate this directory. RESTRICT returns 2BP01;
CASCADE removes the function and operators in one catalog transaction. Retirement
is bounded to 1,024 dependent objects and fails before publication above that
limit. Transport notice buffers reserve capacity before admission, and notice
bytes live through durable outcome serialization.

Expected semantic rejections travel as typed outcome payloads across compiled
storage boundaries, preserving PostgreSQL errors without depending on a
status-only error registry. Apply persists the same rejection under the original
nonce; ambiguous transport failures still require read-only observation.

The independent PostgreSQL operator fixture and mounted cold-owner campaign
qualify original cases sql-0133 through sql-0135, including overloads, UPDATE
RETURNING row multisets, NULLs, namespace rename, dependency rejection, cascade
notices and authenticated reader restrictions. Unary operators, commutators,
negators, selectivity callbacks and hash/merge planner declarations remain
explicitly unsupported.

## Qualification and remaining activation

The scalar-function campaign executes original cases sql-0397 through sql-0404 unchanged
against real HTTP, native metadata Raft and native data owners. It compares
PostgreSQL command tags, result domains, complete row images, SQL NULL flags and
negative SQLSTATEs. It covers replacement, overloads, UPDATE, row filters,
unauthorized DROP, exact lost-ACK recovery, and cold owner restart. The independent
PostgreSQL fixture is reproducible with `generate_sql_routine_reference.py`.

The procedure path shares this catalog, typed scalar VM and outcome journal.
Its bounded PL/pgSQL instruction stream supports BEGIN/END, NULL, literal
RAISE NOTICE, scalar PERFORM and void RETURN. PERFORM binds on first demand;
RETURN prevents binding unreachable instructions. HTTP and pgwire preserve
notices emitted before a later error. Procedure recovery tokens bind the routine
kind, and the mounted campaign checks session/savepoint behavior, cold ownership
and exact lost-ACK recovery against independent PostgreSQL fixtures generated by
`generate_sql_procedure_reference.py`. That campaign also executes the unchanged
original callable COMMENT and DROP FUNCTION pair with real overloads and complete
native annotation postimages. Result execution leases retain routine
constants until consumers release the result, including borrowed text values.

This does not activate general procedural control flow, table-reading PERFORM,
procedure DML, nested CALL, OUT parameters, transaction control inside bodies,
SQL-language procedures, triggers, set-returning functions, multi-query
or table-reading bodies, recursive/composed user routines, external effects in
bodies, function-local settings, SECURITY DEFINER, or user-defined argument types.
Those need catalog dependency/authority and retained execution contracts. Stored
views and schema expressions still reject user-function calls before publication;
DROP must not claim to manage dependencies that were never durably recorded.
PARALLEL, COST and ROWS options remain explicitly unsupported until their catalog
and planner semantics are retained. The original invalid `shift_amount(numeric)`
body references an undeclared `arg1` and correctly fails with PostgreSQL's 42703;
its rejection does not imply that a callable shift function exists.

Further activation should extend this same catalog, VM and outcome path, with
PostgreSQL differential witnesses and native fault tests. It must not introduce
another routine interpreter, coordinator or receipt journal.


## User-defined aggregates

Ordinary typed aggregates share the function signature namespace. Their records
reference transition and optional final functions by immutable catalog and
namespace IDs. Native publication validates every support signature at one
catalog cut. The covering dependency directory tracks both edges, is rebuilt on
restore, and supports bounded RESTRICT/CASCADE retirement after cold restart.
Names reused after retirement cannot retarget an aggregate.

Binding prepares typed transition/final programs once and casts INITCOND to the
state domain before publication. Execution preserves strict NULL handling and
first-nonnull initialization. Transition state, final results, cancellation and
work use the existing scalar VM budgets. Prepared manifests retain the aggregate
and support generations and namespace identities; replacements require a fresh
HTTP prepared resource. Overload lookup distinguishes an ordinary function from
an aggregate using its resolved argument types.

An unordered, non-DISTINCT global aggregate keeps only its running state instead
of sorting all inputs. Ordered, grouped and DISTINCT variants use the bounded
spill machinery. Two alternating state arenas preserve owned values without
retaining prior states. Partial aggregation is not inferred from an arbitrary
transition function: a valid combine contract is required before that optimization
can be enabled. Relational lowering classifies catalog calls once per boundary;
derived and CTE consumers retain the same physical input identities.
