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

----------------------- MODULE AntflyLiveSourceCardinality ----------------------
(*
  Bounded model of primary-source cardinality publication after TTL cleanup.

  Implementation correspondence:
  - TTL cleanup durably updates the document-identity visibility summary;
  - DB.open constructs the TTL runtime before its by-value DB wrapper is moved
    into the managed write cache, so the runtime owner is rebound at the stable
    cache address and refreshes any mutation from that move window;
  - live-writer runtime status publishes the maintained source cardinality;
  - artifact anti-regression may preserve derived-index facts, but must not
    replace an authoritative live source-count decrease.

  BuggyPreMoveOwner models a TTL callback retaining the DB's pre-move address.
  BuggyPreserveLiveCardinality models the runtime snapshot cache replacing a
  live zero with the previously published source count.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyPreMoveOwner, BuggyPreserveLiveCardinality

VARIABLES
    durableSourceCount,
    dbCachedSourceCount,
    visibleSourceCount,
    ownerBound,
    ownerTargetsLiveDb,
    deleteCommitted,
    statusPublished,
    publishParity

vars ==
    <<durableSourceCount, dbCachedSourceCount, visibleSourceCount,
      ownerBound, ownerTargetsLiveDb, deleteCommitted, statusPublished,
      publishParity>>

Init ==
    /\ durableSourceCount = 1
    /\ dbCachedSourceCount = 1
    /\ visibleSourceCount = 1
    /\ ownerBound = FALSE
    /\ ownerTargetsLiveDb = FALSE
    /\ deleteCommitted = FALSE
    /\ statusPublished = FALSE
    /\ publishParity = 0

BindStableOwner ==
    /\ ~ownerBound
    /\ ownerBound' = TRUE
    /\ ownerTargetsLiveDb' = ~BuggyPreMoveOwner
    /\ dbCachedSourceCount' =
        IF BuggyPreMoveOwner THEN dbCachedSourceCount
        ELSE durableSourceCount
    /\ UNCHANGED <<durableSourceCount, visibleSourceCount, deleteCommitted,
                   statusPublished, publishParity>>

CommitTtlDelete ==
    /\ ~deleteCommitted
    /\ durableSourceCount' = 0
    /\ dbCachedSourceCount' =
        IF ownerBound /\ ownerTargetsLiveDb THEN 0
        ELSE dbCachedSourceCount
    /\ deleteCommitted' = TRUE
    /\ UNCHANGED <<visibleSourceCount, ownerBound, ownerTargetsLiveDb,
                   statusPublished, publishParity>>

PublishLiveStatus ==
    /\ deleteCommitted
    /\ visibleSourceCount # dbCachedSourceCount \/ ~statusPublished
    /\ visibleSourceCount' =
        IF BuggyPreserveLiveCardinality /\
           dbCachedSourceCount < visibleSourceCount
        THEN visibleSourceCount
        ELSE dbCachedSourceCount
    /\ statusPublished' = TRUE
    /\ publishParity' = 1 - publishParity
    /\ UNCHANGED <<durableSourceCount, dbCachedSourceCount, ownerBound,
                   ownerTargetsLiveDb, deleteCommitted>>

Next ==
    \/ BindStableOwner
    \/ CommitTtlDelete
    \/ PublishLiveStatus

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(BindStableOwner)
    /\ WF_vars(CommitTtlDelete)
    /\ WF_vars(PublishLiveStatus)

TypeOK ==
    /\ durableSourceCount \in 0..1
    /\ dbCachedSourceCount \in 0..1
    /\ visibleSourceCount \in 0..1
    /\ ownerBound \in BOOLEAN
    /\ ownerTargetsLiveDb \in BOOLEAN
    /\ deleteCommitted \in BOOLEAN
    /\ statusPublished \in BOOLEAN
    /\ publishParity \in 0..1
    /\ BuggyPreMoveOwner \in BOOLEAN
    /\ BuggyPreserveLiveCardinality \in BOOLEAN

DeleteEventuallyPublishesZero ==
    deleteCommitted ~> visibleSourceCount = 0

=============================================================================
