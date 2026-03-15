------------------------------- MODULE MC -------------------------------
(*
  Model checking overrides for AntflyTransaction.

  Defines concrete constant values for a small model that exercises:
    - Multi-shard transactions (t1 spans s1 + s2)
    - Single-shard transactions (t2 on s1 only)
    - OCC conflict on shared key k1
    - LWW timestamp ordering

  Expected state space: ~10^5-10^6 distinct states.
*)

EXTENDS AntflyTransaction

\* Model value constants (assigned in .cfg file)
CONSTANTS t1, t2, s1, s2, k1, k2

\* --- Concrete constant definitions ---

MCTxns   == {t1, t2}
MCShards == {s1, s2}
MCKeys   == {k1, k2}

MCTxnShards == (t1 :> {s1, s2}) @@ (t2 :> {s1})

MCTxnKeys == (<<t1, s1>> :> {k1}) @@ (<<t1, s2>> :> {k2}) @@ (<<t2, s1>> :> {k1})

MCTxnReadSet == (t1 :> {k1}) @@ (t2 :> {k1})

MCTxnCoord == (t1 :> s1) @@ (t2 :> s1)

MCMaxTimestamp == 4

MCStalePendingThreshold == 1

\* --- State constraint ---
\* Bound the clock to keep the state space finite and tractable.
StateConstraint == clock <= MCMaxTimestamp

=============================================================================
