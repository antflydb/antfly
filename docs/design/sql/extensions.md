# SQL extension packages

SQL extension installation adapts Antfly's owned immutable WASM package registry. It does not load PostgreSQL C libraries and does not translate a familiar package name into a compatibility no-op. `CREATE EXTENSION` requires a registered package that declares and supplies executable typed SQL exports.

The supported package domain consists of pure scalar functions. SQL extension controls reject table shapes, other object declarations and requested host capabilities until those kinds have native transactional installation and dependency machinery. The existing persistent-expression resolver also rejects named routines in stored views and schema expressions; installing a SQL extension does not bypass that boundary. Implementing an upstream PostGIS, pgcrypto, UUID or vector package requires its actual functionality, independently of lifecycle support.

## Package control

`PackageManifest.sql` supplies:

- `default_version`: the administrator-configured default, rather than the greatest semantic version.
- `schema`: an optional fixed installation namespace. Otherwise SQL's `SCHEMA` option or current creation namespace selects placement.
- `exports`: ordinary function names, argument/result types, strictness and volatility, plus the WASM runtime and tool selector that implement each function.
- `updates_from`: explicit predecessor versions supported by this version's declarative export transition.

Version names are opaque. Update planning follows the shortest declared directed path, including a downgrade only when a matching edge exists. A transition publishes exactly the target export set. It retains the immutable identity of compatible signatures, advances their executable generation, and checks dependencies before removing an export. Arbitrary PostgreSQL SQL migration scripts and existing REST update artifacts are not silently executed or ignored as substitutes for this contract.

Every SQL export must reference a declared core WASM artifact with a SHA256 digest. Core modules use the existing `antfly_extension_alloc`, `antfly_extension_call_tool` and `antfly_extension_free_buffer` ABI. SQL invocation has no host imports, provider calls, WASI or sidecar fallback. Input and output preserve typed canonical SQL text and distinguish SQL NULL from JSON null.

## Authorization

Extension management uses Antfly's existing live database `admin` permission, including API-key attenuation and revocation. This is an explicit administrative capability for schema management, rather than PostgreSQL's database `CREATE` privilege or mere database ownership. As with Antfly's existing namespace-admin routine management and native-administrator SQL grant path, an authorized administrator can manage an object created by another user. That operation preserves its immutable creator/owner identity.

The mounted lifecycle fixture uses a read-only key for the negative PostgreSQL-role witness, a separate nonowner administrator key for the deliberate administrative override, and live revocation of that elevated key. PostgreSQL's unprivileged `fixture_other` role is compared with a credential lacking administration, rather than with Antfly's elevated database administrator. No extension-only ownership exception or new grant system bypasses the existing permission machinery.

## Publication and recovery

The extension is a database-scoped shared catalog resource with a stable identity. Its record includes its selected package version/digest, installation namespace, immutable owner, ordinary routine membership and extension dependency identities. Functions live in the selected namespace and use the same durable callable directory as ordinary SQL functions.

Ingress verifies and compiles each selected artifact before publication. The mutation binds a digest of the package registry captured alongside its catalog snapshot, so a concurrent registry replacement cannot change the package after artifact admission. Already installed requirements are reused without loading their packages again. Ingress pins their immutable installation IDs in the mutation, so a concurrent DROP/recreate cannot turn a skipped preflight into an unverified new installation. One native catalog transaction publishes or removes the extension, routines, membership index and dependency edges, and appends its exact retained outcome. A semantic rejection occurs before effects. Any error after the first effect aborts the entire transaction.

The existing SQL catalog receipt and public recovery route support extension operations. Receipt tokens carry the bound nonce, scope and digest; they grant no authority and cannot replay SQL. Recovery rechecks live database authorization and immutable database identity. A lost response is observed through the same retained outcome after a restart.

Direct DROP or replacement of an owned member is rejected. DROP EXTENSION removes members automatically; RESTRICT blocks external dependents and CASCADE traverses the shared callable and extension dependency graph. Generic catalog mutations cannot synthesize a WASM binding or delete a member behind its extension record. Whole-state validation checks owner, namespace, generation, package digest, unique membership, dependency identities and acyclicity; derived membership indexes are rebuilt through the existing shared catalog machinery.

Decoder capability 178 protects the new catalog command and routine representations. Package/version counts, membership, graph work, decoded registry bytes and cascade footprints are explicitly bounded before native effects. Runtime preparation occurs once per statement and a shared artifact/runtime uses one compiled instance with bounded fuel and memory, rather than reading or compiling a module for every output row.

## Validation evidence

Lifecycle conformance must execute the original SQL statements through public ingress, inspect complete extension/member/dependency postimages, call genuine WASM functions, and repeat observations after cold reopen. Controlled local packages can demonstrate lifecycle semantics under original package names, but those fixtures establish no upstream package-function compatibility. PostgreSQL 18 oracle results, immutable original-statement hashes and actual Debug native gates are required before the implementation ledger can credit a case.

## Global package defaults and version rollout

A SQL package version is immutable after registration. Re-registering its exact
manifest is idempotent; changing its bytes, replacing it with a non-SQL package,
or deleting it is refused. Other non-SQL package registration semantics are
unchanged. Registering a later version makes that exact version available but
never changes a published default.

The global package catalog owns a separate durable control: package identity,
monotonic generation, explicit default version and SHA256 digest. The first SQL
registration initializes that control from its `sql.default_version` declaration.
That declaration is immutable bootstrap intent, not a mutable current-default
field on every historical manifest. If the declared initial version is not yet
registered, the control is pending. Only that package's omitted-version lookup
fails; explicit versions and unrelated packages remain usable. Registering the
exact pending target resolves its digest atomically with the version row.

To promote a registered version, an operator writes one `sql-control.json` in the
configured package store. Existing package-store synchronization reads these
files after registering all manifests. The JSON request contains `name`,
`identity` (the current control's 32-byte identity), `expected_generation`,
`version`, `package_digest` (the exact 32-byte immutable manifest digest), and a
nonzero 16-byte `nonce`. Byte arrays use JSON integer arrays. There must be at
most one such file per package. This local operator surface is not exposed to
database-admin SQL credentials.

Promotion compares the exact prior identity/generation and target digest. Its
control update and bounded 64-entry retained receipt ring commit atomically.
Replaying an accepted request returns its original receipt and never re-applies
its target after a newer promotion. Once a receipt expires, the stale expected
generation still prevents rewinding the default. An operator must not substitute
a fresh nonce and current generation when recovering an uncertain old request.
The returned receipt describes the original applied generation, not necessarily
the current default.

Semantic preflight errors are returned before a metadata proposal. A competing
registration or stale promotion that reaches committed Raft apply is a
side-effect-free loser, preserving apply progress. The operator waits for its
proposal and verifies the exact retained receipt before reporting success.
Controls and complete retained receipts travel with metadata snapshots, native
checkpoints and portable catalog exports. Restore validates control identity,
version/digest targets and receipt custody before publishing. Read-only control
discovery and SQL request snapshots omit the receipt ring; immutable version
manifests continue to expose their own bootstrap declarations.

A checkpoint restores exactly its captured control and receipt cut. Commands
accepted only after that cut are outside the backup's history; replaying such
a command against its captured predecessor is ordinary state/log recovery.
Retention does not claim to reconstruct decisions absent from the backup.

## Extension comments

`COMMENT ON EXTENSION` uses the same shared catalog resource and atomic
mutation/outcome path as extension creation, update and drop. It checks live
Antfly database-admin authorization; this existing administrative capability
can manage a different creator's extension while preserving its immutable owner.
Setting the comment to `NULL` removes its metadata. The mounted lifecycle tests
exercise the original comment statement, administrator override, attenuated-key
denial, NULL removal and cold observations. Native outcome and public receipt
recovery checks verify the same durable resource state; comment handling does
not create a separate in-memory or package-manifest metadata authority.
