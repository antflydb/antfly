# Runnable scheduler core verification

The request-admission module exports a new caller-owned, allocation-free runnable
scheduler. Outstanding count and declared metadata bytes remain charged while
resource/output-blocked; slot permits do not. Deficit round robin charges slot
demand, bounded backfill protects large operations against starvation, and control
slots remain reserved within total capacity.

The exported module runs ten named scheduler fixtures plus its anonymous import
fixture. They cover weighted sharing, large-job progress, protected slots, blocked
ownership at full outstanding bounds, cooperative yield transitions, cancellation
and expiry after a grant, a real queue timeout, invalid policy/demand rejection,
and shutdown racing a block. The shutdown regression initially failed; the final
implementation stops a post-shutdown block and releases runnable ownership.
Queue expiry is measured from enqueue and checked even when grant wins the race.

All eleven tests pass on macOS ReleaseSafe and pinned Nix glibc Zig 0.17.0
GNU Linux ReleaseSafe.

The source boundary audit passes for 734 sources with no server coordination
imports. Formatting and diff checks pass. Source hashes identify the tested core.

These are state-machine results. The production HTTP/pgwire adapters have not yet
been connected to this scheduler, and the enlarged repository pooling target has
not been run. Real operator quanta, backend wait release, slow-consumer protocol
fixtures, write/inference/fan-out integration, remote uncertainty accounting,
release qualification and automatic defaults remain required. The complete goal
is active; this core does not qualify the full design.
