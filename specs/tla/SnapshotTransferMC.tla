------------------------- MODULE SnapshotTransferMC -------------------------
(*
  Model checking overrides for AntflySnapshotTransfer.

  Defines concrete constant values for a small model that exercises:
    - 3 nodes: enough for leader + 2 peers, peer-crash dynamics
    - MaxRetries = 3: small enough for tractable state space, large enough
      to test retry exhaustion vs permanent detection
    - MaxSnapshots = 3: exercises GC (old snapshot deleted when new created)
      and the scenario where a fetching node's target was GC'd

  Exercises:
    - Successful snapshot transfer (peer has it)
    - Permanent failure (all peers GC'd the snapshot)
    - Retry exhaustion (transient errors, no peers available)
    - Node crash during transfer
    - Leadership change during snapshot creation
    - GC racing with transfer (leader creates new snap, GCs old,
      while another node is trying to fetch the old one)

  Checking guidance:
    Safety invariants (27M distinct states, ~90s):
      Use AntflySnapshotTransfer-safety.cfg (invariants only, no temporal).

    Liveness properties (temporal checking with SF is expensive):
      Use AntflySnapshotTransfer.cfg with reduced constants below.
      MaxRetries=1, MaxSnapshots=1 completes in ~25s (19K states).
      MaxRetries=2, MaxSnapshots=2 takes ~30+ min (2.5M states).
*)

EXTENDS AntflySnapshotTransfer

\* Model value constants (assigned in .cfg file)
CONSTANTS n1, n2, n3

\* --- Concrete constant definitions ---
\* Default: full model for safety checking (fast).
\* For liveness checking, override with smaller values (see guidance above).

MCNodes      == {n1, n2, n3}
MCMaxRetries == 3
MCMaxSnapshots == 3

=============================================================================
