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

------------------- MODULE AntflyEnrichmentLifecycleLockOrder -------------------
(*
  Focused model of the adverse state observed in the July 28 smokeout:

    delete: lifecycle mutex -> waits for enrichment worker join
    worker: waits for the DB apply lock to publish generated artifacts
    status: apply lock -> reads enrichment lifecycle diagnostics

  Runtime status is optional diagnostics and must not wait for the lifecycle
  mutex while it owns the apply lock. The implementation uses tryLock and
  retains its prior/persisted snapshot when a lifecycle transition is active.

  BlockingStatusSnapshot is the #371 behavior before the local prototype.
*)

EXTENDS TLC

CONSTANT BlockingStatusSnapshot

VARIABLES applyOwner, lifecycleOwner, workerPhase, deletePhase, statusPhase

vars == <<applyOwner, lifecycleOwner, workerPhase, deletePhase, statusPhase>>

\* Begin directly at the captured adverse interleaving. This keeps the model
\* about the lock-order obligation rather than the unrelated request schedule.
Init ==
    /\ applyOwner = "status"
    /\ lifecycleOwner = "delete"
    /\ workerPhase = "waiting_apply"
    /\ deletePhase = "waiting_worker"
    /\ statusPhase = "waiting_lifecycle"

StatusSnapshot ==
    /\ statusPhase = "waiting_lifecycle"
    /\ (~BlockingStatusSnapshot \/ lifecycleOwner = "none")
    /\ statusPhase' = "done"
    /\ applyOwner' = "none"
    /\ UNCHANGED <<lifecycleOwner, workerPhase, deletePhase>>

WorkerPublishes ==
    /\ workerPhase = "waiting_apply"
    /\ applyOwner = "none"
    /\ workerPhase' = "done"
    /\ UNCHANGED <<applyOwner, lifecycleOwner, deletePhase, statusPhase>>

DeleteFinishes ==
    /\ deletePhase = "waiting_worker"
    /\ workerPhase = "done"
    /\ deletePhase' = "done"
    /\ lifecycleOwner' = "none"
    /\ UNCHANGED <<applyOwner, workerPhase, statusPhase>>

Next ==
    \/ StatusSnapshot
    \/ WorkerPublishes
    \/ DeleteFinishes

Spec ==
    Init
    /\ [][Next]_vars
    /\ WF_vars(StatusSnapshot)
    /\ WF_vars(WorkerPublishes)
    /\ WF_vars(DeleteFinishes)

TypeOK ==
    /\ applyOwner \in {"none", "status"}
    /\ lifecycleOwner \in {"none", "delete"}
    /\ workerPhase \in {"waiting_apply", "done"}
    /\ deletePhase \in {"waiting_worker", "done"}
    /\ statusPhase \in {"waiting_lifecycle", "done"}
    /\ BlockingStatusSnapshot \in BOOLEAN

DeleteEventuallyCompletes ==
    deletePhase = "waiting_worker" ~> deletePhase = "done"

=============================================================================
