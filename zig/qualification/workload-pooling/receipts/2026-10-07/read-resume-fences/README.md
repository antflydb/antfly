# Shared read-resume policy and catalog fences

Scheduled HTTP SQL SELECT/EXPLAIN and retained PG SELECT streams own a bounded
read guard. It copies at most 64 logical/physical table bindings, schema versions,
catalog revisions, effective user filters, and native RLS serving generations
before the first source read, including reads during Stream.open. Each successful
runnable grant changes the capsule generation; cooperative yields and private
spill resumes invalidate the previous authority check. Output delivery forces a
fresh check. Failed checks are never cached as success.

Validation checks current catalog identity and serving policy, then captures
managed user/API-key policy atomically after catalog I/O. Current permissions
remain intersected with admitted scope. Protected native policies also pin roles
and metadata used by existing proofs. Unprotected reads with no effective user
filter continue after unrelated roles/metadata changes. Pending publication
installation retains prior serving authority; serving transitions and changed
policy generations stop the read. API-key ingress now preserves the owner's
persistent user identity. Logical-to-physical projection preserves null overrides.
Mutation execution does not install the read guard; accepted write completion is
not changed by these read-only fences.

Final macOS Zig 0.17.0 ReleaseSafe qualification:

```sh
zig build antfly-read-authority-test antfly-usermgr-test -Doptimize=ReleaseSafe -j1 --summary all
```

All 69 build steps pass. The target executes three embedded guard fixtures,
two coordinator fixtures, 16 embedded UserManager/authority fixtures, four
server storage-adapter fixtures, and one independent archive ABI fixture: 26
named tests, no skips or leaks. The same command with
`-Dtarget=aarch64-linux-gnu` passes all 66 Linux build steps and the same 26 tests
using pinned Nix/glibc Zig 0.17.0 in a two-CPU container with 4 GiB memory and no
extra swap. Compiler allowances are distinct from runtime qualification limits.

One coordinator fixture executes 22 schedules: unchanged success, filter and
permission revocation, role/metadata changes with native protection, unrelated
unprotected changes, replaced account/table/schema/revision/physical binding,
API-key expiry/deletion through real ingress, policy mutation during catalog
lookup, original deadline expiry, policy enable/generation/disable, prior serving
authority during pending installation, and rejected serving transition. Every
case verifies zero runnable and operation metadata charges after completion.
Ordinary and protected control operations run while the read is resource-blocked.
The guard fixtures check every allocation failure, immutable owned pins, the
64-table bound, per-grant and forced validation, added-table invalidation,
failed-check retry, reentrancy rejection, and independently supplied scratch.

Both new coordinator and embedded tests are explicitly imported by the test root
and selected by exact prefixes. The audit caught a missing import before it was
corrected. A broader initial prefix also selected an unrelated storage fixture;
the final selection excludes it. Adjacent compressed logs contain every named
fixture and all 22 case markers on both platforms; exact source hashes are listed.
The production boundary audit includes embedded_root.zig, storage/db/db.zig, and
sql/read_fence.zig explicitly: 737 authored sources, no server coordination imports.

The expanded native API pooling target also passes all 50 build steps and all
75 selected tests (36 embedded and 39 coordinator), with no skips or leaks.
`native-full-api.log.gz` preserves the complete run. The full Nix/glibc GNU AArch64 API target also passes all 47 build steps
and the same 75 selected tests without skips or leaks. Both exact artifacts
subsequently pass again in a separate two-CPU, 2 GiB container with no extra
swap and explicit zero exit statuses. Artifact hashes and complete build/runtime
logs are adjacent. The build snapshot has the nine source hashes recorded here
and precedes the separate private spill creation wait change.
These focused results establish managed Basic/API-key and publication fences,
not fresh validation of externally issued trusted credentials, general storage/RPC
or directory wait suspension, serialization/preparation scheduling, parallel-child
ownership, all-workload/distributed-attempt integration, performance/default
qualification, or completion of the full design. Scheduling remains opt-in.
