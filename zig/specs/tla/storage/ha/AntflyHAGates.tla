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

----------------------------- MODULE AntflyHAGates -----------------------------
(*
  Exhaustive decision model for storage/ha gate modules:
    - commit_gate.zig
    - read_gate.zig
    - write_gate.zig
    - owner_job_gate.zig
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyStandbyBackgroundRuntime

MaxLsn == 3
Roles == {"primary", "standby", "former_primary"}
CommitModes == {"async", "remote_write", "remote_apply"}
FailurePolicies == {"block", "fail_closed", "degrade_to_async"}
ReadConsistency == {"stale_ok", "at_least_lsn", "primary"}
CommitActions == {"acknowledge", "wait_for_standby", "reject", "acknowledge_degraded"}
ReadActions == {"serve_standby", "wait_for_apply", "wait_for_metadata", "route_to_primary"}
WriteActions == {"allow_write", "reject_write", "wait_for_promotion"}
OwnerActions == {"allow_owner_job", "reject_owner_job"}
BackgroundActions == {"run_mutating_runtime", "suppress_mutating_runtime"}

VARIABLES
    role,
    fenced,
    promotionHandoffOpen,
    targetLsn,
    receivedLsn,
    appliedLsn,
    safeReadLsn,
    metadataAppliedLsn,
    commitMode,
    failurePolicy,
    readConsistency,
    commitAction,
    readAction,
    writeAction,
    ownerAction,
    backgroundAction

vars == <<role, fenced, promotionHandoffOpen, targetLsn, receivedLsn,
          appliedLsn, safeReadLsn, metadataAppliedLsn, commitMode,
          failurePolicy, readConsistency, commitAction, readAction,
          writeAction, ownerAction, backgroundAction>>

CommitDecision(cm, fp, target, applied) ==
    IF cm = "async" THEN "acknowledge"
    ELSE IF applied >= target THEN "acknowledge"
    ELSE IF fp = "fail_closed" THEN "reject"
    ELSE IF fp = "degrade_to_async" THEN "acknowledge_degraded"
    ELSE "wait_for_standby"

ReadDecision(rc, target, safe, metadata) ==
    IF rc = "primary" THEN "route_to_primary"
    ELSE IF rc = "stale_ok" THEN "serve_standby"
    ELSE IF safe < target THEN "wait_for_apply"
    ELSE IF metadata < target THEN "wait_for_metadata"
    ELSE "serve_standby"

WriteDecision(r, f, handoff) ==
    IF r = "primary" /\ ~f THEN "allow_write"
    ELSE IF r = "standby" /\ handoff /\ f THEN "wait_for_promotion"
    ELSE "reject_write"

OwnerDecision(r, f) ==
    IF r = "primary" /\ ~f THEN "allow_owner_job"
    ELSE "reject_owner_job"

BackgroundDecision(r, f) ==
    IF BuggyStandbyBackgroundRuntime /\ r = "standby" THEN "run_mutating_runtime"
    ELSE IF r = "primary" /\ ~f THEN "run_mutating_runtime"
    ELSE "suppress_mutating_runtime"

Init ==
    /\ role \in Roles
    /\ fenced \in BOOLEAN
    /\ promotionHandoffOpen \in BOOLEAN
    /\ targetLsn \in 0..MaxLsn
    /\ receivedLsn \in 0..MaxLsn
    /\ appliedLsn \in 0..receivedLsn
    /\ safeReadLsn \in 0..appliedLsn
    /\ metadataAppliedLsn \in 0..appliedLsn
    /\ commitMode \in CommitModes
    /\ failurePolicy \in FailurePolicies
    /\ readConsistency \in ReadConsistency
    /\ commitAction = CommitDecision(commitMode, failurePolicy, targetLsn, appliedLsn)
    /\ readAction = ReadDecision(readConsistency, targetLsn, safeReadLsn, metadataAppliedLsn)
    /\ writeAction = WriteDecision(role, fenced, promotionHandoffOpen)
    /\ ownerAction = OwnerDecision(role, fenced)
    /\ backgroundAction = BackgroundDecision(role, fenced)

Next == FALSE

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ role \in Roles
    /\ fenced \in BOOLEAN
    /\ promotionHandoffOpen \in BOOLEAN
    /\ targetLsn \in 0..MaxLsn
    /\ receivedLsn \in 0..MaxLsn
    /\ appliedLsn \in 0..MaxLsn
    /\ safeReadLsn \in 0..MaxLsn
    /\ metadataAppliedLsn \in 0..MaxLsn
    /\ commitMode \in CommitModes
    /\ failurePolicy \in FailurePolicies
    /\ readConsistency \in ReadConsistency
    /\ commitAction \in CommitActions
    /\ readAction \in ReadActions
    /\ writeAction \in WriteActions
    /\ ownerAction \in OwnerActions
    /\ backgroundAction \in BackgroundActions

ProgressOrdered ==
    /\ safeReadLsn <= appliedLsn
    /\ metadataAppliedLsn <= appliedLsn
    /\ appliedLsn <= receivedLsn

FailClosedDoesNotAppendAck ==
    /\ commitMode # "async"
    /\ failurePolicy = "fail_closed"
    /\ appliedLsn < targetLsn
    => commitAction = "reject"

StandbyReadNeverBeyondSafeOrMetadata ==
    readAction = "serve_standby" /\ readConsistency # "primary" =>
        IF readConsistency = "at_least_lsn" THEN
            /\ safeReadLsn >= targetLsn
            /\ metadataAppliedLsn >= targetLsn
        ELSE TRUE

FencedPrimaryCannotWrite ==
    role = "primary" /\ fenced => writeAction = "reject_write"

OwnerJobsRequireWritablePrimary ==
    ownerAction = "allow_owner_job" => /\ role = "primary" /\ ~fenced

MutatingBackgroundRequiresWritablePrimary ==
    backgroundAction = "run_mutating_runtime" => /\ role = "primary" /\ ~fenced

StandbyDoesNotRunMutatingBackground ==
    role = "standby" => backgroundAction = "suppress_mutating_runtime"

Safety ==
    /\ TypeOK
    /\ ProgressOrdered
    /\ FailClosedDoesNotAppendAck
    /\ StandbyReadNeverBeyondSafeOrMetadata
    /\ FencedPrimaryCannotWrite
    /\ OwnerJobsRequireWritablePrimary
    /\ MutatingBackgroundRequiresWritablePrimary
    /\ StandbyDoesNotRunMutatingBackground

=============================================================================
