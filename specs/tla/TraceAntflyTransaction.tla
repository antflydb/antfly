--------------------------- MODULE TraceAntflyTransaction ---------------------------
(*
  Trace validation spec for Antfly's distributed transaction protocol.

  Validates that ndjson trace files produced by instrumented Go code
  (built with -tags with_tla) constitute valid behaviors of the
  AntflyTransaction specification.

  All TLA+ constants (Txns, Shards, Keys, TxnShards, TxnKeys, TxnReadSet,
  TxnCoord, MaxTimestamp, StalePendingThreshold) are derived from the trace
  itself -- no MC module or model values needed.

  Modeled after etcd/raft's Traceetcdraft.tla.

  Usage:
    env JSON=trace.ndjson java -cp tla2tools.jar:CommunityModules-deps.jar \
      tlc2.TLC TraceAntflyTransaction -config TraceAntflyTransaction.cfg \
      -workers 1 -lncheck final

  The trace file must be ndjson where each line has:
    {"tag":"antfly-trace","event":{"name":"...","txnId":"...","shardId":"...","state":{...}}}
*)

EXTENDS AntflyTransaction, Json, IOUtils, Sequences, SequencesExt, TLC

-------------------------------------------------------------------------------------

\* Trace validation requires BFS and single worker.
ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

-------------------------------------------------------------------------------------
\* Helpers

\* Convert a TLA+ sequence (from JSON array) to a set.
LOCAL SeqToSet(seq) == {seq[i] : i \in 1..Len(seq)}

-------------------------------------------------------------------------------------
\* Read and filter the trace log

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./trace.ndjson"

\* Parse ndjson, keep only antfly-trace tagged lines
OriginTraceLog ==
    SelectSeq(
        ndJsonDeserialize(JsonFile),
        LAMBDA l: "tag" \in DOMAIN l /\ l.tag = "antfly-trace")

TraceLog ==
    TLCEval(
        IF "MAX_TRACE" \in DOMAIN IOEnv
        THEN SubSeq(OriginTraceLog, 1, atoi(IOEnv.MAX_TRACE))
        ELSE OriginTraceLog)

-------------------------------------------------------------------------------------
\* Derive all constants from the trace

\* Set of transaction IDs mentioned in the trace
TraceTxns == TLCEval(FoldSeq(
    LAMBDA x, acc: acc \cup {x.event.txnId},
    {}, TraceLog))

\* Set of shard IDs mentioned in the trace (non-empty shardId only)
TraceShards == TLCEval(FoldSeq(
    LAMBDA x, acc: acc \cup IF x.event.shardId /= "" THEN {x.event.shardId} ELSE {},
    {}, TraceLog))

\* Set of all keys from WriteIntentOnShard and WriteIntentFails events
TraceKeys == TLCEval(FoldSeq(
    LAMBDA x, acc:
        IF x.event.name \in {"WriteIntentOnShard", "WriteIntentFails"}
           /\ "state" \in DOMAIN x.event
           /\ "writeKeys" \in DOMAIN x.event.state
        THEN acc \cup SeqToSet(x.event.state.writeKeys)
                  \cup SeqToSet(x.event.state.deleteKeys)
                  \cup IF "predicateKeys" \in DOMAIN x.event.state
                      THEN SeqToSet(x.event.state.predicateKeys) ELSE {}
        ELSE acc,
    {}, TraceLog))

\* TxnShards[t]: set of shards touched by txn t (from any event with non-empty shardId)
TraceTxnShards == TLCEval(FoldSeq(
    LAMBDA x, acc:
        IF x.event.shardId /= "" THEN
            [acc EXCEPT ![x.event.txnId] = @ \cup {x.event.shardId}]
        ELSE acc,
    [t \in TraceTxns |-> {}], TraceLog))

\* TxnKeys[<<t, s>>]: keys written by txn t on shard s
\* (from WriteIntentOnShard and WriteIntentFails events)
TraceTxnKeys == TLCEval(FoldSeq(
    LAMBDA x, acc:
        IF x.event.name \in {"WriteIntentOnShard", "WriteIntentFails"}
           /\ "state" \in DOMAIN x.event
           /\ "writeKeys" \in DOMAIN x.event.state
        THEN LET pair == <<x.event.txnId, x.event.shardId>>
                 keys == SeqToSet(x.event.state.writeKeys)
                         \cup SeqToSet(x.event.state.deleteKeys)
             IN [acc EXCEPT ![pair] = @ \cup keys]
        ELSE acc,
    [p \in TraceTxns \X TraceShards |-> {}], TraceLog))

\* TxnReadSet[t]: OCC predicate keys for txn t
\* (union of predicateKeys from WriteIntentOnShard/WriteIntentFails events)
TraceTxnReadSet == TLCEval(FoldSeq(
    LAMBDA x, acc:
        IF x.event.name \in {"WriteIntentOnShard", "WriteIntentFails"}
           /\ "state" \in DOMAIN x.event
           /\ "predicateKeys" \in DOMAIN x.event.state
        THEN [acc EXCEPT ![x.event.txnId] = @ \cup SeqToSet(x.event.state.predicateKeys)]
        ELSE acc,
    [t \in TraceTxns |-> {}], TraceLog))

\* TxnCoord[t]: coordinator shard for txn t (shard where InitTransaction fired)
TraceTxnCoord == TLCEval(FoldSeq(
    LAMBDA x, acc:
        IF x.event.name = "InitTransaction" THEN
            [acc EXCEPT ![x.event.txnId] = x.event.shardId]
        ELSE acc,
    [t \in TraceTxns |-> ""], TraceLog))

\* MaxTimestamp: largest timestamp observed in the trace + 2 (buffer for clock ticks)
TraceMaxTimestamp == TLCEval(
    FoldSeq(
        LAMBDA x, acc:
            IF x.event.name = "InitTransaction"
               /\ "state" \in DOMAIN x.event
               /\ "timestamp" \in DOMAIN x.event.state
               /\ x.event.state.timestamp > acc
            THEN x.event.state.timestamp
            ELSE acc,
        0, TraceLog) + 2)

\* StalePendingThreshold: minimum clock ticks before auto-abort.
\* Set to 1 (the minimum) since trace validation uses a bounded clock.
TraceStalePendingThreshold == 1

-------------------------------------------------------------------------------------
\* Trace cursor variables

VARIABLE l   \* Current position in TraceLog (1-indexed)
VARIABLE pl  \* Previous position (for first-visit checks)

logline == TraceLog[l]

-------------------------------------------------------------------------------------
\* Helpers

LoglineIsEvent(e) ==
    /\ l <= Len(TraceLog)
    /\ logline.event.name = e

LoglineIsTxnEvent(e, t) ==
    /\ LoglineIsEvent(e)
    /\ logline.event.txnId = t

LoglineIsTxnShardEvent(e, t, s) ==
    /\ LoglineIsTxnEvent(e, t)
    /\ logline.event.shardId = s

StepToNextTrace ==
    /\ l' = l + 1
    /\ pl' = l

-------------------------------------------------------------------------------------
\* Trace-guided actions
\*
\* Each *IfLogged action checks the current logline, fires the corresponding
\* AntflyTransaction action, and advances the trace cursor.

InitTransactionIfLogged(t) ==
    /\ LoglineIsTxnEvent("InitTransaction", t)
    /\ InitTransaction(t)
    /\ StepToNextTrace

CheckPredicatesIfLogged(t) ==
    /\ LoglineIsTxnEvent("CheckPredicates", t)
    /\ CheckPredicates(t)
    /\ StepToNextTrace

WriteIntentOnShardIfLogged(t, s) ==
    /\ LoglineIsTxnShardEvent("WriteIntentOnShard", t, s)
    /\ WriteIntentOnShard(t, s)
    /\ StepToNextTrace

WriteIntentFailsIfLogged(t, s) ==
    /\ LoglineIsTxnShardEvent("WriteIntentFails", t, s)
    /\ WriteIntentFails(t, s)
    /\ StepToNextTrace

CommitTransactionIfLogged(t) ==
    /\ LoglineIsTxnEvent("CommitTransaction", t)
    /\ CommitTransaction(t)
    /\ StepToNextTrace

AbortTransactionIfLogged(t) ==
    /\ LoglineIsTxnEvent("AbortTransaction", t)
    /\ AbortTransaction(t)
    /\ StepToNextTrace

ResolveIntentsOnShardIfLogged(t, s) ==
    /\ LoglineIsTxnShardEvent("ResolveIntentsOnShard", t, s)
    /\ ResolveIntentsOnShard(t, s)
    /\ StepToNextTrace

RecoveryResolveIfLogged(t, s) ==
    /\ LoglineIsTxnShardEvent("RecoveryResolve", t, s)
    /\ \E s2 \in Shards : RecoveryResolve(t, s2)
    /\ StepToNextTrace

CleanupTxnRecordIfLogged(t) ==
    /\ LoglineIsTxnEvent("CleanupTxnRecord", t)
    /\ CleanupTxnRecord(t)
    /\ StepToNextTrace

\* RecoveryAutoAbort: coordinator-level auto-abort, shardId is empty
RecoveryAutoAbortIfLogged(t) ==
    /\ LoglineIsTxnEvent("RecoveryResolve", t)
    /\ logline.event.shardId = ""
    /\ RecoveryAutoAbort(t)
    /\ StepToNextTrace

\* TickClock can fire between any trace events to advance the clock.
\* The trace does not explicitly log clock ticks, but the spec needs
\* them for StalePendingThreshold-based recovery.
TickClockIfNeeded ==
    /\ l <= Len(TraceLog)
    /\ TickClock
    /\ UNCHANGED <<l, pl>>

-------------------------------------------------------------------------------------
\* Trace-guided next-state relation

TraceInit ==
    /\ l = 1
    /\ pl = 0
    /\ Init

LoggedStep ==
    /\ l <= Len(TraceLog)
    /\ \/ \E t \in Txns :
            \/ InitTransactionIfLogged(t)
            \/ CheckPredicatesIfLogged(t)
            \/ CommitTransactionIfLogged(t)
            \/ AbortTransactionIfLogged(t)
            \/ CleanupTxnRecordIfLogged(t)
            \/ RecoveryAutoAbortIfLogged(t)
            \/ \E s \in Shards :
                \/ WriteIntentOnShardIfLogged(t, s)
                \/ WriteIntentFailsIfLogged(t, s)
                \/ ResolveIntentsOnShardIfLogged(t, s)
                \/ RecoveryResolveIfLogged(t, s)

TraceNext ==
    \/ LoggedStep
    \/ /\ ~ENABLED LoggedStep
       /\ TickClockIfNeeded

TraceSpec == TraceInit /\ [][TraceNext]_<<l, pl, vars>>

-------------------------------------------------------------------------------------

TraceView ==
    <<vars, l>>

-------------------------------------------------------------------------------------
\* TraceMatched: violated if TLC finishes with trace not fully consumed.

TraceMatched ==
    [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
