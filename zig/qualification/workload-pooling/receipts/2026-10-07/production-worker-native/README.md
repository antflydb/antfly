# Native production archive and real worker transport

Candidate: `18662ddc0e` plus the production probe changes pinned by each result
file. Both commands exited 0 with 23/23 build steps on native macOS:

```sh
zig build linked-inference-abi-integration-test -Doptimize=ReleaseSafe -j1 \
  --maxrss 23622320128 --cache-dir /private/tmp/antfly-default-pooling-cache --summary all
zig build linked-inference-worker-integration-test -Doptimize=ReleaseSafe -j1 \
  --maxrss 23622320128 --cache-dir /private/tmp/antfly-default-pooling-cache --summary all
```

Compiler `/Users/ajroetker/bin/zig-0.17.0/zig`; global cache
`/private/tmp/antfly-default-pooling-global`; cwd the design worktree's `zig/`.
Source hashes were rechecked against the completed result metadata. These are
compilation allowances, not runtime resource qualification limits.

The archive probe calls only the production-exported C function table. It
rejects an old provider tail and oversized CPU demand before touching an invalid
node pointer; it exercises valid delegated scope, borrowed class restoration,
authority rejection and existing attachment/body validation. The full inference
archive, including worker dispatcher, is independently compiled.

The subprocess probe uses the production worker client and re-execs through the
production process ABI into its actual private worker loop. Configure performs
real bidirectional resource-owner RPC. A borrowed budget travels to the child,
a malformed provider call returns the expected error, and the parent phase is
restored. A request bypassing client-side validation proves that oversized demand
is rejected in the child before parsing an empty attachment body. Reaping leaves
no retained resource-context references or reservation leases. The test resource
owner is a minimal accounting fixture, not an overload or memory-policy proof.

These malformed-provider calls verify transport, archive and lifecycle plumbing.
They do not establish model numerical correctness, foreign backend CPU-team
bounds, physical cohort fairness or default overload behavior. Native focused
scope/matrix gates are recorded separately. The same two production probes are
currently compiling/running under pinned Nix/glibc Linux in a 2 CPU/20 GiB
compile container; that outcome remains pending. Whole API qualification of
ABI v30, combined production startup, background/durability work and automatic
/default overload qualification are still required. Defaults remain disabled.

The matching pinned Nix/glibc aarch64-linux-gnu gate also passed both production archive and real worker subprocess probes (exit 0, 23/23 build steps). Source hashes were checked against the worktree before retaining linux-result.json and the complete compressed log. This qualifies the ABI and worker-wire validation paths described above, not numerical model execution, foreign backend teams, overload behavior, or automatic defaults.
