# Merge Audit Tooling

Use this directory for conflict-heavy `origin/main` merges where incoming changes
must be accounted for surgically.

## Before Merging Main

Default workflow:

```sh
make merge-audit-snapshot
```

Equivalent explicit command:

```sh
python3 scripts/merge_audit/audit_main_capture.py \
  --incoming-ref origin/main \
  --previous-main-merge <last-main-merge> \
  --write-snapshot /tmp/antfly-main-merge-snapshot.json
```

The snapshot freezes the incoming SHA and path list, so the later audit is not
affected if `origin/main` moves.

Use this to fail fast if `origin/main` has moved since the snapshot:

```sh
make merge-audit-check-snapshot
```

Before starting conflict resolution, generate both split inventories against
the clean branch commit:

```sh
make merge-audit-split-baselines \
  MERGE_AUDIT_BASE=<previous-main-parent-sha> \
  MERGE_AUDIT_INCOMING=<pinned-incoming-sha> \
  MERGE_AUDIT_DESTINATION_REF=<pinned-premerge-branch-sha>
```

During an active merge, `HEAD` remains the pre-merge branch commit. The target
therefore reads all split destinations from a reproducible clean tree without
interpreting conflict-marker text. `merge-audit-split-branch-baseline` compares
the previous-main source to itself and inventories pre-existing split-refactor
coverage. `merge-audit-split-baseline` separately compares
`previous-main..incoming` and inventories the new incoming review queue. Keep
both report directories outside the repository; together they distinguish
inherited split debt from changes introduced by the current main merge.

## After Resolving Conflicts

Default workflow:

```sh
make merge-audit
```

Equivalent explicit commands:

```sh
python3 scripts/merge_audit/audit_main_capture.py \
  --snapshot /tmp/antfly-main-merge-snapshot.json \
  --strict-snapshot-ref \
  --json-out /tmp/antfly-main-merge-audit.json \
  --write-report /tmp/antfly-main-merge-audit.md

python3 scripts/merge_audit/audit_zig_split_merge.py \
  --write-report /tmp/antfly-zig-split-merge-audit.md

make merge-audit-split-declarations \
  MERGE_AUDIT_BASE=<previous-main-parent-sha> \
  MERGE_AUDIT_INCOMING=<pinned-incoming-sha>
```

Then run the focused build/tests appropriate for the conflicts and finish with:

```sh
git diff --cached --check
git diff --check
```

## Split Declaration Audit

`audit_split_declarations.py` compares changed functions, named tests, module
and direct-container bindings, container types, direct container fields, and
enum/union variants in
a monolithic Zig source with its current split destinations. Binding coverage
includes imports, constants, and hook state while excluding function/test-local
bindings. Container-field coverage includes named containers and
anonymous structs returned by generic context factories such as
`AsyncContext(comptime DB: type) type`. Variant coverage prevents enum or bare
union cases added or changed in main from disappearing behind a split-container
review status. It distinguishes exact declarations,
already-integrated incoming deltas, clean declaration-level three-way merge
candidates, conflicts, ambiguous destinations, missing declarations, and
missing container fields. Function- and test-local declarations are covered by
their enclosing declaration and are not double-counted as independent
obligations.

For a source file renamed between the pinned revisions, pass
`--base-source <old-path>` and `--incoming-source <new-path>` while keeping
`--source` as the logical audit label. This preserves one declaration lineage
across the rename instead of requiring an unaudited manual diff.

Upstream deletions are obligations too. A deleted declaration passes only when
it is absent from every mapped split destination. A retained baseline copy, a
branch-modified copy, or an ambiguous mapped copy remains in the strict review
queue; this prevents a refactor from silently preserving behavior that main
intentionally removed.

When the branch deliberately retains a deleted declaration, record its exact
obligation key and review reason in that migration's `retained_deletions` map.
Each entry also pins the base declaration SHA-256, retained declaration
SHA-256, and exact destination path. The acknowledgement is accepted only when
all hashes and the unique conflict-free destination still match. Unknown,
stale, absent, or ambiguous entries fail closed, so this cannot become a broad
or cross-merge waiver.

When the split refactor deliberately removes a declaration that still exists
on incoming main, record it in `intentional_declaration_deletions`. Each entry
pins the incoming declaration SHA-256 and a concrete reason. It passes only
when that exact declaration is absent from every split destination; a changed
incoming body, a reintroduced destination candidate, or an unused manifest
entry fails closed. Use this for reviewed obsolete wrappers, not for behavior
that merely moved to another owner.

For a composition-root declaration that necessarily conflicts after its body
was split into independently audited owners, use the migration's
`reviewed_compositions` map only after every child obligation is resolved.
Each entry pins the exact base, incoming, and current declaration SHA-256,
destination path, and review reason. It is accepted only for a live three-way
conflict; a changed body, stale path, resolved conflict, or unused entry fails
closed. This is suitable for a top-level `build()` orchestrator after both the
declaration audit and all-incoming build-surface audit pass, not for ordinary
function conflicts.

For an ordinary declaration whose incoming and branch changes were manually
composed, record a `reviewed_resolutions` entry. It pins the live audit status,
nullable base SHA-256 (null for same-name additions), incoming and current
SHA-256 values, destination path, and a concrete semantic review reason. Only
`three_way_conflict`, `added_name_collision`, `clean_candidate`, and diverged
container fields or variants are reviewable. A clean candidate should be
reviewed only when applying it would violate a branch-side API migration; the
hash-locked reason must identify that migration. Container divergence is reached only after every
incoming field or variant is present; structural omissions, stale fields,
ambiguous placement, and conflicted files cannot be acknowledged. Changed,
resolved, unused, or status-mismatched entries fail closed on the next run.

`audit_split_migrations.py` is the aggregate gate. By default it runs every
entry in `policy.json`'s `split_migrations` object, writes one JSON report per
migration plus `index.json` and a source-ordered `review-queue.json`, and can
fail closed with `--strict` when any obligation still needs review. The queue
keeps incoming or deletion-base source lines and neighboring placement anchors
so contiguous upstream feature clusters can be reviewed together. This keeps `db`,
`table_reads`, and
`table_writes` in one merge gate without duplicating their names in build
recipes. Repeated `--migration` arguments provide a tighter exploratory loop;
the default deliberately audits the complete manifest.

The manifest's `required_split_migrations` list prevents a high-risk refactor
from silently falling out of that gate. Each required migration must exist and
must have a `moved_paths` entry whose source facade and owner paths are covered
by its declared destinations. This currently pins `db`, `table_reads`,
`table_writes`, the HTTP server, both build facades, and the composed Antfly
storage/test build helpers; policy drift fails before any child audit runs.

A migration's `declaration_companion_globs` extends the declaration gate to
same-path Zig files in the refactor's ownership scope. The aggregate runner
discovers every matching file changed between the pinned baseline and incoming
commit. Byte-exact additions or updates and absent deletions pass directly;
composed files receive their own declaration-level three-way audit. Divergent
additions, retained deletions, missing current files, overlapping owners, and
malformed patterns fail closed. Sources owned by another named migration are
delegated to that migration, so this stays extensible without a per-merge file
list. DB, table reads, and table writes use this for their split owner trees.

Path-specific companion resolutions belong under the owning migration's
`declaration_companion_policies` map. Each path accepts the same declaration
owner, alias, deletion, and hash-locked review fields as a normal split
migration, but the aggregate runner applies them only while auditing that exact
source and destination file. Unknown paths and stale review entries therefore
fail without granting a waiver to another DB or API owner.

The aggregate runner also accepts `--destination-ref`, forwarding the same
clean-tree mode to every migration. Candidate generation is rejected in this
mode because baseline inventories must be read-only. Destination entries added
after that pinned ref are skipped while the rest of the migration is audited;
an entirely missing destination tree still fails as a manifest error.

The Make targets deliberately keep the two baselines separate. The historical
branch baseline invokes the declaration audit with `--base` and `--incoming`
both pinned to `MERGE_AUDIT_BASE`; the incoming baseline uses the normal
`MERGE_AUDIT_BASE..MERGE_AUDIT_INCOMING` range. Do not use an unresolved
historical obligation as a waiver for the current merge: the strict post-merge
declaration gate still audits all carried declarations, while the historical
report identifies whether a finding was inherited or newly regressed.

`audit_zig_build_surface.py` complements declaration comparison for large
`build()` bodies. The merge gate runs it for both `zig_build` and
`inference_build`, tracking incoming-added build options, named steps, module
root sources, and executable artifact names across facades and split build
helpers. Missing registrations fail the normal and post-commit merge gates.
Both build migrations are required, so declaration ownership and registration
wiring must remain configured for every merge.

The same report inventories newly incoming semantic build facts: module and
option imports, named-step dependency edges, native source files, include and
library paths, system libraries, and frameworks. Facts normalize context-local
module parameters and step/run variable names while retaining the imported or
depended-on endpoint. `build_surface_dependency_helpers` describes split
helpers that register an edge from a literal argument or a named descriptor;
optional `target_argument` and `step_field` entries keep those contracts
machine-checkable after the build root is decomposed.

Use `--include-carried` for conflict-heavy merges. It verifies every incoming
registration, not only names added since the previous-main base. Pass
`--zig-build-dir zig` to make the compiled `zig build -l -fincremental` step
inventory authoritative for named steps, preventing an artifact or descriptor
with the same static `.name` from satisfying a step obligation. Repeat the
option when a refactor moved registrations across build roots. Steps omitted
from the default graph by a build option belong in
`build_surface_conditional_steps`; each entry must still have a direct static
`b.step(...)` registration in the migrated destination.
A migration's `build_surface_helpers` can
describe literal name/path arguments accepted by split registration helpers,
so wrappers remain auditable without hard-coding each repository helper in the
scanner.

Pass `--retention-ref <premerge-sha>` to run the complementary branch-retention
check in the same report. This requires every registration carried or added by
the refactored branch to remain represented in the resolved worktree, so the
audit protects both sides of the merge. Rename, omission, and conditional-step
rules that do not exist in that historical ref are listed as inapplicable;
rules whose source does exist still require every configured target and remain
fail-closed. Branch-only renames that occurred after the previous-main base
belong in `build_surface_retention_aliases`; they are applied only to this
reverse check, so normal incoming aliases remain strict and cannot become
stale silently. `make merge-audit-build-surface` uses
`MERGE_AUDIT_RETENTION_REF`, which defaults to the clean pre-merge destination
ref, and the post-commit gate uses `HEAD^1`.

Semantic categories listed in `build_surface_delta_only_categories` audit the
complete `previous-main..incoming` delta but do not reinterpret older incoming
edges through a later branch refactor. The pinned premerge snapshot and split
declaration baseline protect that branch-owned graph, while `--include-carried`
continues to require every carried public registration. This division avoids
hundreds of variable-name waivers without weakening the next merge: every new
main edge is a strict obligation, and every existing split helper remains a
hash-checked declaration obligation.

Intentional build-surface renames belong in `build_surface_aliases`. Every
alias source must still exist in the pinned incoming build and every target
must exist in the current static or compiled inventory; stale sources and
missing targets fail closed. Use `build_surface_omissions` only for an incoming
surface that genuinely has no current registration, with a non-empty reason.
Generated replacements should name their generation and check workflow in that
reason. These policies classify evidence; they do not create build steps or
prove that a broader target contains a missing leaf test, so dependency wiring
must be reviewed before adding an alias.

Build registration inventories do not prove that the helpers implementing a
registration retained their behavior. `build_companion_globs` therefore
discovers every incoming-changed build file in the configured ownership scope
and compares its bytes with the worktree. Exact files pass automatically. A
composed file must be the source of another required split migration, which
subjects each changed declaration to the same three-way, hash-locked review as
`db.zig` and the table facades. Missing, retained-deleted, or divergent files
without that owner fail the build-surface gate. This also catches a newly added
helper on the next main merge without first hard-coding its filename.

`audit_zig_relative_imports.py` validates every file-relative `@import` ending
in `.zig`. By default it scans tracked and untracked Zig files under the
manifest's `zig_relative_import_roots`; repeated `--migration` arguments narrow
it to the selected split destinations for a faster feedback loop. This catches
imports that still use a monolith's old directory depth or point at an
intentionally removed facade after code moves. Commented imports and package
module imports are ignored. Repository escapes and missing targets fail the
gate. The repository or a migration may declare a relative import exception
containing `path`, `import`, and `reason`; unused exceptions also fail so the
manifest cannot accumulate stale suppressions.

Before editing a conflicted split facade, `audit_split_declarations.py` can read
its destinations from the clean pinned branch with `--destination-ref
<premerge-sha>`. This mode is read-only: candidate generation and replacements
are rejected. It provides a reproducible pre-resolution inventory even when
the worktree facade contains conflict markers.

With `--policy`, `test_name_aliases` maps intentional branch test renames back
to incoming names. The aliased declaration is still compared normally, so a
rename cannot suppress body-level semantic review.

`declaration_name_aliases` provides the same manifest-driven treatment for
renamed containers and other declarations. Owner aliases apply to nested
methods, and the old/new symbol names are canonicalized in declaration bodies,
so a type rename does not turn every preserved method into a false gap.

For systematic names introduced by a split, a migration's
`test_name_rewrites` maps an exact destination path plus `source_prefix` and
`destination_prefix`. The path-scoped rewrite only selects the candidate; the
audit canonicalizes the test header and still compares or three-way merges the
complete body. Clean candidates restore the split destination's test name.

An incoming-private declaration exported only because it crossed a split-module
boundary is classified as `split_visibility_adapted` when removing that one
`pub` modifier makes the declarations otherwise identical.
Conversely, an otherwise exact private function with a unique qualified call
from another split destination is classified as `split_visibility_missing`.
The check covers both owner methods and top-level helpers referenced through an
exactly resolved module import, including facade aliases such as
`const helper = split_owner.helper`. This catches declarations that became
inaccessible when a monolith was split; the status remains a strict review
failure until the destination exports it.

Relative `@import` bindings are resolved against their owning source path.
Bindings that resolve to the same repository module are classified as
`split_import_path_adapted`, even when several split files import that module.
Qualified references through a split owner's top-level import alias are also
canonicalized against inline or monolith aliases and classified as
`split_module_reference_adapted` only when the full declaration otherwise
matches.
For helpers moved behind a split owner, `symbol_call_migrations` may map an
exact old call symbol to one or more exact replacement call symbols. Only call
sites are canonicalized, and the complete declaration must otherwise match;
plain values or unrelated identifiers are not rewritten.
Exact renamed constants or type references can use
`symbol_reference_migrations`. These rules match complete tokens only and are
also applied when comparing aliased binding definitions and direct container
fields; structural field presence remains strict.

When a contiguous source lineage moved into one split owner, use
`declaration_placement_ranges` with exact `start` and `end` obligation keys,
the destination `path`, and a review `reason`. The boundaries must each resolve
uniquely in the pinned incoming source and may not conflict with exact
`declaration_placements`. Candidate generation still emits every declaration
inside the range as a separate hash-locked obligation; owner declarations are
inserted at the start of the manifest-declared mixin rather than appended at
file scope.
Monolith constants moved into a split owner and re-exported through a qualified
binding are classified as `split_binding_alias_adapted` only when the alias
resolves through that file's import binding to an equivalent owner declaration.

Qualified split aliases that stand in for monolith containers are reported as
`split_alias_review`. They satisfy the structural presence check but remain
explicit review obligations because field/variant equivalence belongs to the
aliased owner.

Named owner containers are recorded for methods and nested types. Matching
requires the same owner for owned declarations, so common method names and
file-level placement suggestions cannot erase owning-struct boundaries. A
method moved to a different owner must be represented by an explicit split
adapter or reviewed placement; a same-named wrapper is not accepted as proof.
`declaration_owner_migrations` may pin such a move to an exact destination
path, owner, and optional target name/kind. The pinned target is then compared
normally. For an optional target name, only the parsed declaration header is
canonicalized during comparison and three-way merge, and emitted candidates
restore the split name. The manifest changes ownership and naming, not the
semantic result.

When a container refactor replaces direct fields without retaining shims, use
`container_field_migrations`. Each container obligation maps an incoming field
to a direct current replacement field and a concrete reason. The audit verifies
that the source field exists in pinned incoming main, the replacement exists in
the matched current container, and every configured container mapping is used.
Multiple source fields may map to one options/state field when the reason
documents the aggregation. Stale sources, missing targets, and unused mappings
fail closed.

For Zig mixin refactors, `declaration_mixins` identifies an exact destination
path, factory function, and logical owner. The factory must return a struct.
Its direct declarations are audited as members of that owner only when the
split tree has no ordinary declaration with the same kind, name, and owner.
This covers private methods moved into `Impl(comptime DB: type)` modules while
keeping facade methods authoritative and preserving duplicate candidates as
ambiguities.

For a declaration added only on incoming main, the JSON report includes a
`suggested_path` plus preceding/following anchors when the nearest uniquely
mapped declarations on both sides agree on one split file. Boundary
disagreements remain unsuggested; placement evidence never creates a candidate
or edits the worktree.

The audit does not edit the worktree. To generate only high-confidence candidates
for review in a separate directory:

```sh
python3 scripts/merge_audit/audit_split_declarations.py \
  --base <previous-main-parent-sha> \
  --incoming <pinned-incoming-sha> \
  --migration db \
  --policy scripts/merge_audit/policy.json \
  --include-unchanged \
  --json-out /tmp/antfly-db-split-audit.json \
  --candidate-dir /tmp/antfly-db-split-candidates
```

Run the complete refactor audit with:

```sh
python3 scripts/merge_audit/audit_split_migrations.py \
  --base <previous-main-parent-sha> \
  --incoming <pinned-incoming-sha> \
  --policy scripts/merge_audit/policy.json \
  --include-unchanged \
  --output-dir /tmp/antfly-split-declaration-audit \
  --strict
```

Candidate generation requires an outside-repository empty directory, rejects
ambiguous declarations and destination files with conflict markers, normalizes
move-only indentation, and records source/current hashes in
`split-declaration-candidates.json`. Candidates still require diff review,
compilation, focused tests, and semantic classification; a clean textual merge
is evidence, not proof of behavioral completeness.

For a conflict whose reviewed resolution is the complete pinned incoming
declaration, pass both `--incoming-replacement-key '<exact key>'` and the same
`--candidate-key`. This mode deliberately bypasses declaration-level three-way
merge for only that key, reindents the incoming declaration at its current
owner span, and emits the same hash-locked outside-repository candidate. It is
appropriate only after reviewing the base-to-current delta; it must not be used
as a blanket conflict strategy.

After reviewing a candidate diff, apply one or more complete candidate files
with the hash-locked applicator:

```sh
python3 scripts/merge_audit/apply_split_candidates.py \
  /tmp/antfly-db-split-candidates/split-declaration-candidates.json \
  --path zig/pkg/antfly/src/storage/db/lifecycle.zig \
  --apply
```

The applicator defaults to a dry run. Before changing any file it verifies all
selected current and candidate hashes, repository containment, the
outside-repository candidate root, regular-file ownership, and absence of
conflict markers. Selected replacements are staged before atomic rename.

Pass `--include-missing-candidates` to additionally emit review-only insertions
for missing declarations when same-owner declarations immediately before and
after the incoming declaration both map to one destination and the following
anchor is unique. This is useful for moved tests and helpers, but remains
opt-in because an exact incoming declaration can still require split-module
imports, visibility adaptation, or intentional supersession review.

Use repeated `--candidate-key '<exact obligation key>'` arguments to emit a
reviewed subset of available merge and insertion candidates. Unknown keys fail
closed. This avoids applying an unrelated candidate merely because it shares a
destination file.

Use repeated `--exclude-candidate-key '<exact obligation key>'` arguments when
most candidates in a reviewed batch are valid but a small number need manual
split-boundary adaptation. Excluded keys also fail closed if they are unknown,
and a key cannot be both selected and excluded.

When refactoring has reordered a test/helper so heavily that neighboring
anchors cannot establish placement, record its exact obligation key in the
migration's `declaration_placements`. For top-level declarations this enables
a review-only EOF candidate in that destination. Container-owned methods still
require manual placement inside their owner.

Rerun the audit after applying a reviewed candidate set. Earlier ports can make
matching more precise, while declarations whose merge result already equals the
current branch are classified as `integrated` and never emitted as no-op
candidates.

For a reproducible manual review queue, pass `--include-review-bodies`. The JSON
then records the exact base, incoming, and selected current declaration bodies
alongside their hashes. This is intentionally opt-in because full monolith
audits can produce large reports.

`--include-unchanged` is required for refactor safety: it verifies declarations
that were already present at the pinned base as well as new incoming deltas.
Without it, a feature accidentally dropped while splitting a monolith would be
invisible when main did not modify that declaration again. The repository make
target enables this mode for DB, reads, and writes.

Run the focused tooling regression tests with:

```sh
python3 -m unittest scripts.merge_audit.test_audit_split_declarations -v
python3 -m unittest scripts.merge_audit.test_apply_split_candidates -v
```

## After Committing A Merge

Use the merge commit parents explicitly so the audit still sees the original
ours and incoming sides:

```sh
make merge-audit-post-commit
```

Equivalent explicit commands:

```sh
python3 scripts/merge_audit/audit_main_capture.py \
  --incoming-ref HEAD^2 \
  --ours-ref HEAD^1 \
  --previous-main-merge <last-main-merge> \
  --json-out /tmp/antfly-main-merge-audit.json \
  --write-report /tmp/antfly-main-merge-audit.md

python3 scripts/merge_audit/audit_zig_split_merge.py \
  --origin HEAD^2 \
  --base <previous-main-parent-sha> \
  --write-report /tmp/antfly-zig-split-merge-audit.md
```

## Policy

`policy.json` holds repo-specific merge knowledge: named `split_migrations`,
moved paths, intentional side choices, renamed helpers/tests, high-risk paths
requiring explicit review, and generated-file provenance. A split migration is
the single source of truth for the incoming monolith and all current facade or
directory destinations, so the normal and post-commit Make gates cannot drift.
Keep policy changes specific and documented.

Reusable structural policy (`moved_paths`, aliases, review requirements, and
generated-file provenance) applies across merges. Resolution decisions and
review acknowledgements are merge-specific: when any are present, the manifest
must include `decision_scope.ours_sha` and `decision_scope.incoming_sha` as full
commit SHAs. The audit ignores those decisions unless both SHAs match the merge
being audited. Update the scope only after re-reviewing every carried decision;
never advance it mechanically.

## Make Variables

Override these when the default refs or output paths are not what you want:

- `MERGE_AUDIT_INCOMING`, default `origin/main`
- `MERGE_AUDIT_PREVIOUS_MAIN_MERGE`, default last prior `origin/main` merge
- `MERGE_AUDIT_BASE`, default second parent of `MERGE_AUDIT_PREVIOUS_MAIN_MERGE`
- `MERGE_AUDIT_SNAPSHOT`, default `/tmp/antfly-main-merge-snapshot.json`
- `MERGE_AUDIT_JSON`, default `/tmp/antfly-main-merge-audit.json`
- `MERGE_AUDIT_REPORT`, default `/tmp/antfly-main-merge-audit.md`
- `MERGE_AUDIT_ZIG_REPORT`, default `/tmp/antfly-zig-split-merge-audit.md`
- `MERGE_AUDIT_SPLIT_DIR`, default `/tmp/antfly-split-declaration-audit`
- `MERGE_AUDIT_SPLIT_BASELINE_DIR`, default `/tmp/antfly-split-declaration-baseline`
- `MERGE_AUDIT_SPLIT_BRANCH_BASELINE_DIR`, default `/tmp/antfly-split-branch-baseline`
- `MERGE_AUDIT_BUILD_SURFACE_JSON`, default `/tmp/antfly-zig-build-surface-audit.json`
- `MERGE_AUDIT_ZIG_BUILD_DIR`, default `zig`
- `MERGE_AUDIT_DESTINATION_REF`, default `HEAD`
