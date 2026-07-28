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

-------------------------- MODULE AntflyTableAdmission --------------------------
(*
  Public table admission boundary. Unsupported index configurations must be
  rejected before desired or committed catalog state is written, leaving the
  name immediately reusable by a valid request.
*)

EXTENDS TLC

CONSTANT BuggyPersistInvalid

RequestKinds == {"none", "valid", "invalid"}

VARIABLES requestKind, desired, committed, rejected, accepted
vars == <<requestKind, desired, committed, rejected, accepted>>

Init ==
    /\ requestKind = "none"
    /\ desired = FALSE
    /\ committed = FALSE
    /\ rejected = FALSE
    /\ accepted = FALSE

BeginRequest(kind) ==
    /\ requestKind = "none"
    /\ ~desired
    /\ ~committed
    /\ kind \in {"valid", "invalid"}
    /\ requestKind' = kind
    /\ rejected' = FALSE
    /\ accepted' = FALSE
    /\ UNCHANGED <<desired, committed>>

RejectInvalid ==
    /\ requestKind = "invalid"
    /\ ~desired
    /\ ~committed
    /\ requestKind' = "none"
    /\ rejected' = TRUE
    /\ UNCHANGED <<desired, committed, accepted>>

PersistValid ==
    /\ requestKind = "valid"
    /\ ~desired
    /\ desired' = TRUE
    /\ UNCHANGED <<requestKind, committed, rejected, accepted>>

CommitValid ==
    /\ requestKind = "valid"
    /\ desired
    /\ ~committed
    /\ committed' = TRUE
    /\ accepted' = TRUE
    /\ requestKind' = "none"
    /\ UNCHANGED <<desired, rejected>>

BuggyPersistThenReject ==
    /\ BuggyPersistInvalid
    /\ requestKind = "invalid"
    /\ ~desired
    /\ desired' = TRUE
    /\ committed' = TRUE
    /\ rejected' = TRUE
    /\ requestKind' = "none"
    /\ UNCHANGED accepted

DropTable ==
    /\ desired \/ committed
    /\ desired' = FALSE
    /\ committed' = FALSE
    /\ UNCHANGED <<requestKind, rejected, accepted>>

Next ==
    \/ \E kind \in {"valid", "invalid"}: BeginRequest(kind)
    \/ RejectInvalid
    \/ PersistValid
    \/ CommitValid
    \/ BuggyPersistThenReject
    \/ DropTable

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyPersistInvalid \in BOOLEAN
    /\ requestKind \in RequestKinds
    /\ desired \in BOOLEAN
    /\ committed \in BOOLEAN
    /\ rejected \in BOOLEAN
    /\ accepted \in BOOLEAN

RejectedLeavesNoResidue == rejected => ~desired /\ ~committed
CommittedWasAccepted == committed => accepted
NameReusableAfterReject == rejected => requestKind = "none"
Safety == TypeOK /\ RejectedLeavesNoResidue /\ CommittedWasAccepted /\ NameReusableAfterReject

=============================================================================
