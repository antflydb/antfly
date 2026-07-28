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

--------------------- MODULE AntflyReplayDeleteSupersession ---------------------
(*
  Bounded model of derived replay when a later primary-store delete supersedes
  an earlier upsert before a derived index consumes it.

  Implementation correspondence:
  - a primary write publishes a thin replay upsert;
  - TTL or an ordinary delete removes the source document, records a durable
    document-identity tombstone, and publishes a later replay delete;
  - full-text, dense-field, and sparse-field replay read the current primary
    document when the thin upsert has no inline value;
  - a missing source with a durable tombstone is superseded work, while a
    missing source without identity proof must remain retryable.

  BuggyRetryDeleted models treating both kinds of missing source as transient.
  The worker keeps retrying the stale upsert and can never reach the later
  delete record.
*)

EXTENDS Naturals, TLC

CONSTANT BuggyRetryDeleted

VARIABLES
    documentState,
    pendingUpsert,
    pendingDelete,
    indexVisible,
    appliedUpsert,
    appliedDelete,
    retryParity

vars == <<documentState, pendingUpsert, pendingDelete, indexVisible,
          appliedUpsert, appliedDelete, retryParity>>

Init ==
    /\ documentState = "absent"
    /\ pendingUpsert = FALSE
    /\ pendingDelete = FALSE
    /\ indexVisible = FALSE
    /\ appliedUpsert = FALSE
    /\ appliedDelete = FALSE
    /\ retryParity = 0

WriteDocument ==
    /\ documentState = "absent"
    /\ ~pendingUpsert
    /\ documentState' = "live"
    /\ pendingUpsert' = TRUE
    /\ UNCHANGED <<pendingDelete, indexVisible, appliedUpsert, appliedDelete,
                   retryParity>>

DeleteDocument ==
    /\ documentState = "live"
    /\ ~pendingDelete
    /\ documentState' = "deleted"
    /\ pendingDelete' = TRUE
    /\ UNCHANGED <<pendingUpsert, indexVisible, appliedUpsert, appliedDelete,
                   retryParity>>

\* A replay upsert with no primary row and no durable tombstone is not safe to
\* skip. This action represents a publication/visibility gap that must retry.
PublishUnknownMissingUpsert ==
    /\ documentState = "absent"
    /\ ~pendingUpsert
    /\ pendingUpsert' = TRUE
    /\ UNCHANGED <<documentState, pendingDelete, indexVisible, appliedUpsert,
                   appliedDelete, retryParity>>

ApplyLiveUpsert ==
    /\ pendingUpsert
    /\ documentState = "live"
    /\ pendingUpsert' = FALSE
    /\ indexVisible' = TRUE
    /\ appliedUpsert' = TRUE
    /\ UNCHANGED <<documentState, pendingDelete, appliedDelete, retryParity>>

ApplySupersededUpsert ==
    /\ pendingUpsert
    /\ documentState = "deleted"
    /\ ~BuggyRetryDeleted
    /\ pendingUpsert' = FALSE
    /\ indexVisible' = FALSE
    /\ appliedUpsert' = TRUE
    /\ UNCHANGED <<documentState, pendingDelete, appliedDelete, retryParity>>

RetryMissingUpsert ==
    /\ pendingUpsert
    /\ documentState \in {"absent", "deleted"}
    /\ (documentState = "absent" \/ BuggyRetryDeleted)
    /\ retryParity' = 1 - retryParity
    /\ UNCHANGED <<documentState, pendingUpsert, pendingDelete, indexVisible,
                   appliedUpsert, appliedDelete>>

ApplyDelete ==
    /\ pendingDelete
    /\ ~pendingUpsert
    /\ pendingDelete' = FALSE
    /\ indexVisible' = FALSE
    /\ appliedDelete' = TRUE
    /\ UNCHANGED <<documentState, pendingUpsert, appliedUpsert, retryParity>>

Next ==
    \/ WriteDocument
    \/ DeleteDocument
    \/ PublishUnknownMissingUpsert
    \/ ApplyLiveUpsert
    \/ ApplySupersededUpsert
    \/ RetryMissingUpsert
    \/ ApplyDelete

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(ApplyLiveUpsert)
    /\ WF_vars(ApplySupersededUpsert)
    /\ WF_vars(ApplyDelete)
    /\ WF_vars(RetryMissingUpsert)

TypeOK ==
    /\ documentState \in {"absent", "live", "deleted"}
    /\ pendingUpsert \in BOOLEAN
    /\ pendingDelete \in BOOLEAN
    /\ indexVisible \in BOOLEAN
    /\ appliedUpsert \in BOOLEAN
    /\ appliedDelete \in BOOLEAN
    /\ retryParity \in 0..1
    /\ BuggyRetryDeleted \in BOOLEAN

UnknownMissingNeverAdvances ==
    documentState = "absent" /\ pendingUpsert => ~appliedUpsert

DurableDeleteEventuallyConverges ==
    documentState = "deleted"
        ~> (~pendingUpsert /\ ~pendingDelete /\ ~indexVisible /\ appliedDelete)

Safety == TypeOK /\ UnknownMissingNeverAdvances

=============================================================================
