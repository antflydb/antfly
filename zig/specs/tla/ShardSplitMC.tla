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

----------------------------- MODULE ShardSplitMC -----------------------------
(*
  Model checking overrides for AntflyShardSplit.

  Defines concrete constant values for a small model that exercises:
    - 3 keys: k1 stays with parent, k2 and k3 go to child
    - Concurrent writes to both sides of the split during every phase
    - Delta writes to parent during split (k2, k3 routed to parent)
    - Child delta replay and catch-up
    - Leadership change at every phase
    - Timeout rollback from both prepare and splitting phases
    - The critical byteRange-before-archive ordering

  The state space is naturally finite because all variables have bounded
  domains (enumerations, BOOLEAN, SUBSET of finite Keys).
*)

EXTENDS AntflyShardSplit

\* Model value constants (assigned in .cfg file)
CONSTANTS k1, k2, k3

\* --- Concrete constant definitions ---

MCKeys       == {k1, k2, k3}
MCParentKeys == {k1}         \* k1 stays with parent after split
MCChildKeys  == {k2, k3}     \* k2 and k3 go to child shard

=============================================================================
