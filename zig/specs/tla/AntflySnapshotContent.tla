\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

----------------------------- MODULE AntflySnapshotContent -----------------------------
(*
  Snapshot content/index provenance model.

  AntflySnapshotTransfer.tla checks local archive presence. This sibling checks
  the lower-level content/index claim: applying snapshot ID/index i must apply
  the content created for i, not merely any local file with that ID shape.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyWrongContent, BuggyGcNeededContent

Nodes == {"leader", "follower"}
Snaps == 1..2

VARIABLES
    created,
    stored,
    content,
    target,
    followerNeedsContent,
    applied,
    appliedContent

vars == <<created, stored, content, target, followerNeedsContent, applied, appliedContent>>

Init ==
    /\ created = {}
    /\ stored = [n \in Nodes |-> {}]
    /\ content = [n \in Nodes |-> [s \in Snaps |-> 0]]
    /\ target = [n \in Nodes |-> 0]
    /\ followerNeedsContent = FALSE
    /\ applied = [n \in Nodes |-> 0]
    /\ appliedContent = [n \in Nodes |-> 0]

CreateSnapshot(s) ==
    /\ s \in Snaps
    /\ s \notin created
    /\ created' = created \cup {s}
    /\ stored' = [stored EXCEPT !["leader"] = @ \cup {s}]
    /\ content' = [content EXCEPT !["leader"][s] = s]
    /\ UNCHANGED <<target, followerNeedsContent, applied, appliedContent>>

SendSnapshot(s) ==
    /\ s \in stored["leader"]
    /\ target["follower"] = 0
    /\ target' = [target EXCEPT !["follower"] = s]
    /\ UNCHANGED <<created, stored, content, followerNeedsContent, applied, appliedContent>>

FetchSnapshot ==
    /\ target["follower"] \in Snaps
    /\ LET s == target["follower"] IN
       /\ s \in stored["leader"]
       /\ stored' = [stored EXCEPT !["follower"] = @ \cup {s}]
       /\ content' = [content EXCEPT !["follower"][s] =
            IF BuggyWrongContent THEN IF s = 1 THEN 2 ELSE 1 ELSE content["leader"][s]]
       /\ followerNeedsContent' = TRUE
    /\ UNCHANGED <<created, target, applied, appliedContent>>

ApplySnapshot ==
    /\ target["follower"] \in stored["follower"]
    /\ LET s == target["follower"] IN
       /\ applied' = [applied EXCEPT !["follower"] = s]
       /\ appliedContent' = [appliedContent EXCEPT !["follower"] = content["follower"][s]]
       /\ target' = [target EXCEPT !["follower"] = 0]
       /\ followerNeedsContent' = FALSE
    /\ UNCHANGED <<created, stored, content>>

BuggyGcFollowerNeededContent ==
    /\ BuggyGcNeededContent
    /\ target["follower"] \in stored["follower"]
    /\ LET s == target["follower"] IN
       /\ stored' = [stored EXCEPT !["follower"] = @ \ {s}]
       /\ content' = [content EXCEPT !["follower"][s] = 0]
    /\ UNCHANGED <<created, target, followerNeedsContent, applied, appliedContent>>

Next ==
    \/ \E s \in Snaps: CreateSnapshot(s)
    \/ \E s \in Snaps: SendSnapshot(s)
    \/ FetchSnapshot
    \/ ApplySnapshot
    \/ BuggyGcFollowerNeededContent

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyWrongContent \in BOOLEAN
    /\ BuggyGcNeededContent \in BOOLEAN
    /\ created \subseteq Snaps
    /\ stored \in [Nodes -> SUBSET Snaps]
    /\ content \in [Nodes -> [Snaps -> 0..2]]
    /\ target \in [Nodes -> 0..2]
    /\ followerNeedsContent \in BOOLEAN
    /\ applied \in [Nodes -> 0..2]
    /\ appliedContent \in [Nodes -> 0..2]

AppliedContentMatchesIndex ==
    applied["follower"] # 0 => appliedContent["follower"] = applied["follower"]

TargetContentNotGcBeforeApply ==
    target["follower"] # 0 /\ followerNeedsContent =>
        /\ target["follower"] \in stored["follower"]
        /\ content["follower"][target["follower"]] = target["follower"]

Safety ==
    /\ TypeOK
    /\ AppliedContentMatchesIndex
    /\ TargetContentNotGcBeforeApply

=============================================================================
