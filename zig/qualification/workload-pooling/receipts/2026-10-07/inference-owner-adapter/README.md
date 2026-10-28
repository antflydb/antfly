# Inference owner adapter and real owner handoff gate

Base: `cb19bd567b`. Source hashes, exact Zig argument arrays and full logs are
stored alongside this receipt. The adapter acquires an inference-class capsule
through the checked owner port, charges invocation/cohort metadata plus payload
estimates, and installs a synchronous one-CPU native worker scope. Completion
restores the previous scope and retires the owner capsule. Member deadlines and
cancellation remain collective; no earliest-member deadline is invented.

Native macOS and pinned Nix/glibc GNU AArch64 both pass 22 adapter/broker tests
and 8 real API capsule/integration tests, with exit code 0. The integration gate
uses the actual API owner and runnable scheduler, not a replacement scheduler:
a query parks, model work takes its ordinary slot, control work proceeds beside
it, model work finishes, and the query reacquires its grant. One canceled member
does not stop its live peer; all-canceled admission leaves no new operation.
The adapter gate also verifies rejection rollback and the scope on the actual
broker callback worker. Existing native kernel fan-out qualification is recorded
separately in `admitted-cpu-workers`.

The focused integration source manifest intentionally owns only request
admission; this is a focused gate, not the full API suite. Initial compilation
identified the missing focused manifest binding; supplying that explicit binding
required no production source change.

Linux used image
`sha256:fb3236b6cc7411028d62f76eb418110a6c34e246b9437a9d093d58a53a3e2954`,
entrypoint `/opt/zig-nix/bin/zig`, 2 CPUs and 1 GiB without extra swap. Source was
read-only at `/work`, cwd/cache writable at `/cache`, global cache at `/global`.
Native cwd was the design worktree. See `commands.json` for module bindings.

## Required next work

Production Node/host installation and parent handoff are not wired yet. A
scheduled SQL parent currently retains its grant when calling embedded inference;
installing a nested broker grant now could deadlock when ordinary slots are full.
The real-owner test proves the required park/admit/finish/reacquire sequence,
not production propagation. Owner pointers remain process-local; worker-process
handoff needs its own protocol. Generic backend teams, checkpoint quanta,
complete retained-memory accounting, full production gates and default overload
qualification remain pending. Defaults stay disabled.
