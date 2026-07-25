-------------------------- MODULE AntflyRaftReadyPipeline -----------------------
(*
Code anchors:
  - lib/raft/src/runtime/scheduler.zig ReadyPass/fair_ready/
    continuation_ready and deferReady/completeReady.
  - lib/raft/src/runtime/multi_raft.zig Ready admission, message cloning,
    applyCommittedConfChanges, outbox processing, PendingApplyTask.conf_state,
    and snapshot-candidate scheduling.
What this proves:
  - A fair pass visits each group once and grants continuations only after fair
    coverage to productive work within the step budget.
  - Backpressure-denied Ready work never takes ownership of message buffers.
  - Accepted messages are cloned before configuration apply and survive it.
  - Snapshot candidates carry membership from their exact applied index rather
    than newer live Raft progress.
Deliberate omissions:
  - Payload bytes, persistence/transport failures after ownership transfer,
    quiesce/register churn, log compaction thresholds, and wall-clock metrics.
State bounds:
  - Three groups, one denied group, one hot continuation, one membership
    change, one snapshot candidate, and two outbound messages.
Make targets:
  - AntflyRaftReadyPipeline, heavy-liveness, and its Bad* checks.
Correspondence tier:
  - Mature: scheduler, MultiRaft, and runtime ownership tests cover the actions.
*)

EXTENDS Naturals, FiniteSets

CONSTANTS GroupCount, HotGroup, MaxReadySteps,
          BuggyRepeatHot, BuggyDeniedContinuation, BuggyEarlyContinuation,
          BuggyOverBudget, BuggyCloneBeforeAdmission,
          BuggyApplyBeforeOwnership, BuggyAliasMessages,
          BuggyUseLiveMembership

ASSUME /\ GroupCount >= 2
       /\ HotGroup \in 1..GroupCount
       /\ HotGroup # GroupCount
       /\ MaxReadySteps > GroupCount
       /\ BuggyRepeatHot \in BOOLEAN
       /\ BuggyDeniedContinuation \in BOOLEAN
       /\ BuggyEarlyContinuation \in BOOLEAN
       /\ BuggyOverBudget \in BOOLEAN
       /\ BuggyCloneBeforeAdmission \in BOOLEAN
       /\ BuggyApplyBeforeOwnership \in BOOLEAN
       /\ BuggyAliasMessages \in BOOLEAN
       /\ BuggyUseLiveMembership \in BOOLEAN

Groups == 1..GroupCount
DeniedGroup == GroupCount
ReadyMessages == {"vote", "append"}
NoMessages == [g \in Groups |-> {}]

VARIABLES phase, fairVisited, duplicateFairVisit, admitted, deferredGroups,
          cloned, completed, productiveGroups, continuationQueued,
          processedSteps, ownedMessages, sentMessages,
          raftConf, appliedIndex, appliedConf, candidateIndex, candidateConf,
          configApplied, appliedWithoutOwnership

vars == <<phase, fairVisited, duplicateFairVisit, admitted, deferredGroups,
          cloned, completed, productiveGroups, continuationQueued,
          processedSteps, ownedMessages, sentMessages,
          raftConf, appliedIndex, appliedConf, candidateIndex, candidateConf,
          configApplied, appliedWithoutOwnership>>

Init ==
    /\ phase = "idle"
    /\ fairVisited = {}
    /\ duplicateFairVisit = FALSE
    /\ admitted = {}
    /\ deferredGroups = {}
    /\ cloned = {}
    /\ completed = {}
    /\ productiveGroups = {}
    /\ continuationQueued = {}
    /\ processedSteps = 0
    /\ ownedMessages = NoMessages
    /\ sentMessages = NoMessages
    /\ raftConf = "old"
    /\ appliedIndex = 1
    /\ appliedConf = "old"
    /\ candidateIndex = 0
    /\ candidateConf = "none"
    /\ configApplied = FALSE
    /\ appliedWithoutOwnership = FALSE

BeginFair ==
    /\ phase = "idle"
    /\ phase' = "fair"
    /\ UNCHANGED <<fairVisited, duplicateFairVisit, admitted, deferredGroups,
                  cloned, completed, productiveGroups, continuationQueued,
                  processedSteps, ownedMessages, sentMessages, raftConf,
                  appliedIndex, appliedConf, candidateIndex, candidateConf,
                  configApplied, appliedWithoutOwnership>>

FairAttempt(g) ==
    /\ phase = "fair"
    /\ g \in Groups
    /\ IF BuggyRepeatHot THEN g = HotGroup ELSE g \notin fairVisited
    /\ fairVisited' = fairVisited \cup {g}
    /\ duplicateFairVisit' =
        (duplicateFairVisit \/ (g \in fairVisited))
    /\ admitted' = IF g = DeniedGroup THEN admitted ELSE admitted \cup {g}
    /\ deferredGroups' =
        IF g = DeniedGroup THEN deferredGroups \cup {g} ELSE deferredGroups
    /\ continuationQueued' =
        IF g = DeniedGroup /\ BuggyDeniedContinuation
        THEN continuationQueued \cup {g}
        ELSE continuationQueued
    /\ UNCHANGED <<phase, cloned, completed, productiveGroups, processedSteps,
                  ownedMessages, sentMessages, raftConf, appliedIndex,
                  appliedConf, candidateIndex, candidateConf, configApplied,
                  appliedWithoutOwnership>>

BuggyCloneDeniedReady ==
    /\ BuggyCloneBeforeAdmission
    /\ phase = "fair"
    /\ DeniedGroup \in deferredGroups
    /\ DeniedGroup \notin cloned
    /\ cloned' = cloned \cup {DeniedGroup}
    /\ ownedMessages' =
        [ownedMessages EXCEPT ![DeniedGroup] = ReadyMessages]
    /\ UNCHANGED <<phase, fairVisited, duplicateFairVisit, admitted,
                  deferredGroups, completed, productiveGroups,
                  continuationQueued, processedSteps, sentMessages, raftConf,
                  appliedIndex, appliedConf, candidateIndex, candidateConf,
                  configApplied, appliedWithoutOwnership>>

CloneReady(g) ==
    /\ phase = "fair"
    /\ g \in admitted \ cloned
    /\ cloned' = cloned \cup {g}
    /\ ownedMessages' = [ownedMessages EXCEPT ![g] = ReadyMessages]
    /\ UNCHANGED <<phase, fairVisited, duplicateFairVisit, admitted,
                  deferredGroups, completed, productiveGroups,
                  continuationQueued, processedSteps, sentMessages, raftConf,
                  appliedIndex, appliedConf, candidateIndex, candidateConf,
                  configApplied, appliedWithoutOwnership>>

CommitMembership ==
    /\ phase = "fair"
    /\ HotGroup \in admitted
    /\ raftConf = "old"
    /\ raftConf' = "new"
    /\ UNCHANGED <<phase, fairVisited, duplicateFairVisit, admitted,
                  deferredGroups, cloned, completed, productiveGroups,
                  continuationQueued, processedSteps, ownedMessages,
                  sentMessages, appliedIndex, appliedConf, candidateIndex,
                  candidateConf, configApplied, appliedWithoutOwnership>>

CaptureSnapshotCandidate ==
    /\ raftConf = "new"
    /\ appliedConf = "old"
    /\ candidateIndex = 0
    /\ candidateIndex' = 2
    /\ candidateConf' =
        IF BuggyUseLiveMembership THEN raftConf ELSE appliedConf
    /\ UNCHANGED <<phase, fairVisited, duplicateFairVisit, admitted,
                  deferredGroups, cloned, completed, productiveGroups,
                  continuationQueued, processedSteps, ownedMessages,
                  sentMessages, raftConf, appliedIndex, appliedConf,
                  configApplied, appliedWithoutOwnership>>

CompleteReady(g) ==
    /\ phase = "fair"
    /\ g \in admitted \ completed
    /\ processedSteps < MaxReadySteps
    /\ IF g = HotGroup THEN candidateIndex = 2 ELSE TRUE
    /\ IF BuggyApplyBeforeOwnership THEN TRUE ELSE g \in cloned
    /\ completed' = completed \cup {g}
    /\ productiveGroups' = productiveGroups \cup {g}
    /\ processedSteps' =
        IF BuggyOverBudget THEN MaxReadySteps + 1 ELSE processedSteps + 1
    /\ continuationQueued' =
        IF g = HotGroup THEN continuationQueued \cup {g}
        ELSE continuationQueued
    /\ appliedWithoutOwnership' =
        (appliedWithoutOwnership \/ (g \notin cloned))
    /\ configApplied' = (configApplied \/ (g = HotGroup))
    /\ appliedIndex' = IF g = HotGroup THEN 3 ELSE appliedIndex
    /\ appliedConf' = IF g = HotGroup THEN "new" ELSE appliedConf
    /\ ownedMessages' =
        IF g = HotGroup /\ BuggyAliasMessages
        THEN [ownedMessages EXCEPT ![g] = {"append"}]
        ELSE ownedMessages
    /\ UNCHANGED <<phase, fairVisited, duplicateFairVisit, admitted,
                  deferredGroups, cloned, sentMessages, raftConf,
                  candidateIndex, candidateConf>>

SendMessages(g) ==
    /\ g \in completed
    /\ g \in cloned
    /\ sentMessages[g] = {}
    /\ sentMessages' = [sentMessages EXCEPT ![g] = ownedMessages[g]]
    /\ UNCHANGED <<phase, fairVisited, duplicateFairVisit, admitted,
                  deferredGroups, cloned, completed, productiveGroups,
                  continuationQueued, processedSteps, ownedMessages, raftConf,
                  appliedIndex, appliedConf, candidateIndex, candidateConf,
                  configApplied, appliedWithoutOwnership>>

BeginContinuation ==
    /\ phase = "fair"
    /\ continuationQueued # {}
    /\ IF BuggyEarlyContinuation THEN TRUE ELSE fairVisited = Groups
    /\ phase' = "continuation"
    /\ UNCHANGED <<fairVisited, duplicateFairVisit, admitted, deferredGroups,
                  cloned, completed, productiveGroups, continuationQueued,
                  processedSteps, ownedMessages, sentMessages, raftConf,
                  appliedIndex, appliedConf, candidateIndex, candidateConf,
                  configApplied, appliedWithoutOwnership>>

FinishWithoutContinuation ==
    /\ phase = "fair"
    /\ fairVisited = Groups
    /\ admitted = completed
    /\ continuationQueued = {}
    /\ phase' = "done"
    /\ UNCHANGED <<fairVisited, duplicateFairVisit, admitted, deferredGroups,
                  cloned, completed, productiveGroups, continuationQueued,
                  processedSteps, ownedMessages, sentMessages, raftConf,
                  appliedIndex, appliedConf, candidateIndex, candidateConf,
                  configApplied, appliedWithoutOwnership>>

RunContinuation(g) ==
    /\ phase = "continuation"
    /\ g \in continuationQueued
    /\ IF BuggyOverBudget THEN TRUE ELSE processedSteps < MaxReadySteps
    /\ processedSteps' = processedSteps + 1
    /\ continuationQueued' = continuationQueued \ {g}
    /\ UNCHANGED <<phase, fairVisited, duplicateFairVisit, admitted,
                  deferredGroups, cloned, completed, productiveGroups,
                  ownedMessages, sentMessages, raftConf, appliedIndex,
                  appliedConf, candidateIndex, candidateConf, configApplied,
                  appliedWithoutOwnership>>

FinishContinuation ==
    /\ phase = "continuation"
    /\ continuationQueued = {}
    /\ phase' = "done"
    /\ UNCHANGED <<fairVisited, duplicateFairVisit, admitted, deferredGroups,
                  cloned, completed, productiveGroups, continuationQueued,
                  processedSteps, ownedMessages, sentMessages, raftConf,
                  appliedIndex, appliedConf, candidateIndex, candidateConf,
                  configApplied, appliedWithoutOwnership>>

Next ==
    \/ BeginFair
    \/ BuggyCloneDeniedReady
    \/ CommitMembership
    \/ CaptureSnapshotCandidate
    \/ BeginContinuation
    \/ FinishWithoutContinuation
    \/ FinishContinuation
    \/ \E g \in Groups:
        \/ FairAttempt(g)
        \/ CloneReady(g)
        \/ CompleteReady(g)
        \/ SendMessages(g)
        \/ RunContinuation(g)

Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(BeginFair)
    /\ \A g \in Groups:
        /\ WF_vars(FairAttempt(g))
        /\ WF_vars(CloneReady(g))
        /\ WF_vars(CompleteReady(g))
        /\ WF_vars(SendMessages(g))
    /\ WF_vars(CommitMembership)
    /\ WF_vars(CaptureSnapshotCandidate)

NoDuplicateFairVisit == ~duplicateFairVisit

FairCoverageBeforeContinuation ==
    phase \in {"continuation", "done"} /\ productiveGroups # {} =>
        fairVisited = Groups

ContinuationRequiresProductiveAdvance ==
    continuationQueued \subseteq productiveGroups

ReadyStepBudgetBounded ==
    processedSteps <= MaxReadySteps

DeniedReadyDoesNotOwnMessages ==
    \A g \in deferredGroups:
        /\ g \notin cloned
        /\ ownedMessages[g] = {}
        /\ sentMessages[g] = {}

ConfigurationApplyRequiresOwnedMessages ==
    ~appliedWithoutOwnership

ReadyMessagesPreserved ==
    configApplied /\ HotGroup \in cloned =>
        ownedMessages[HotGroup] = ReadyMessages

SnapshotMembershipMatchesAppliedIndex ==
    candidateIndex = 0 \/
    /\ candidateIndex = 2
    /\ candidateConf = "old"

MembershipEventuallyApplied == <>(appliedConf = "new")
MessagesEventuallySent == <>(sentMessages[HotGroup] = ReadyMessages)

=============================================================================
