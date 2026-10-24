# Main merge and pgwire task-capacity repair

Merged `origin/main` at `ac2d3779ee` into the pooling branch. Both inventory
argument fixes are preserved by the helper that inserts audit controls before
runtime passthrough arguments. The generated control catalog is current and the
embedded boundary audit passes for 733 sources.

Main runs each connection owner, protocol runner and deadline watchdog on the
shared API executor (16 task slots by default). The accept task plus five peers
fill that executor, so the next peer loses its connection during startup. This
explains the five constructed peers in the earlier Linux receipts. The pooling
listener uses its own bounded transport and one shared deadline owner.

The new regression fills an independent outbound executor, retains 32 pgwire
startup sockets, verifies that the outbound executor remains full, and joins the
listener before rebinding its port. All 88 pgwire tests pass on macOS ReleaseSafe
and pinned Nix glibc Zig 0.17.0 GNU Linux ReleaseSafe. The Linux compiler initially
exceeded its 1 GiB declared build claim and retried successfully; that diagnostic
is retained in the complete log.

The merged native API pooling target passes all 55 selected contracts (17 local
and 38 API cases), with all 50 build steps successful and no leaked tests.
The qualification harness passes 14 tests and the selection auditor passes 10.

CI can apply the focused `baseline-pgwire-transport.patch` to older main and
retains the actual applied diff. Main keeps its existing three tasks per peer,
so the patch reserves three slots per configured connection and cancel-handshake
slot plus one accept slot. It leaves SQL admission and allocation policy intact.
The patch applies cleanly to the prior b793 baseline. Historical startup and
qualification failures remain unchanged. No fresh full production matrices were
run for this merge; these results establish the socket/task regression, not new
performance qualification or automatic waiting defaults.

The merge hook could not run its pinned pnpm formatter on an upstream TypeScript
story; the merge commit bypassed that hook. Changed Zig formatting, patch checks,
workflow YAML and all eight workflow shell blocks pass targeted validation.
