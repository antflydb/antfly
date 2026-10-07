# Node owner control ABI candidate

An optional function-table capability appends node runnable owner create, close
and destroy operations. Create validates the context, memory owner and native
policy type before casting. Caller-side ownership retains a stable boundary
allocator through successful destruction. Destruction refuses live borrowers
or executions without closing the owner as a side effect. This prepares a
node lifetime outside any particular API instance while keeping scheduler state
and Zig errors inside the API archive.

All 14 native core tests pass with destruction-refusal checks. Both native and
pinned Nix/glibc GNU AArch64 layout gates pass their two API-kernel checks,
including rejection of a truncated optional capability prefix. Source hashes
and direct commands are in `contracts.json`, with complete logs retained.
These small gates do not compile or execute the owner factory/opaque wrapper.
Their added API fixtures still need the full API gate after the preceding
shared-owner/provider source finishes its current qualification. Separate
native caller/owner ABI execution, standalone wiring, inference/background
consumers and default qualification remain pending.

The combined candidate now creates the opaque node owner for standalone opt-in
scheduling and passes its handle to the API. Normal shutdown and exceptional
future teardown close it before transport joins; final destruction follows
DataServer/provider retirement. The API destructor captures its borrower pointer
before poisoning its fields. `pending-candidate.json` pins the source currently
under the combined native API gate, including this standalone candidate. The
latest destruction-refusal core also passes all 14 tests on Nix/glibc GNU; full
API/production execution of the new wiring remains unqualified.

The corrected combined native API gate now passes all 50 build steps and
78 tests (37 embedded, 41 coordinator), with zero skips or leaks. Its complete
output and source hashes are recorded in the shared-node-owner-abi native API
receipt. This includes the checked factory/wrapper fixtures, two API borrowers,
closed/conflicting configuration checks, provider checkpoints, and existing
HTTP/pgwire contracts. GNU API, separate archive execution and standalone
production startup/shutdown remain pending.
