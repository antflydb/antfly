\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.

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
