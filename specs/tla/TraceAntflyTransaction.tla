--------------------------- MODULE TraceAntflyTransaction ---------------------------
(*
  Trace validation spec for Antfly's distributed transaction protocol.

  Validates that ndjson trace files produced by instrumented Go code
  (built with -tags with_tla) constitute valid behaviors of the
  AntflyTransaction specification.

  Modeled after etcd/raft's Traceetcdraft.tla.

  Usage:
    env JSON=trace.ndjson java -cp tla2tools.jar:CommunityModules-deps.jar \
      tlc2.TLC TraceAntflyTransaction -config TraceAntflyTransaction.cfg \
      -workers 1 -lncheck final

  The trace file must be ndjson where each line has:
    {"tag":"antfly-trace","event":{"name":"...","txnId":"...","shardId":"...","state":{...}}}
*)

EXTENDS AntflyTransaction, Json, IOUtils, Sequences, TLC

-------------------------------------------------------------------------------------

\* Trace validation requires BFS and single worker.
ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

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
\* Derive constants from the trace

\* Extract set of txn IDs mentioned in the trace
TraceTxns == TLCEval(FoldSeq(
    LAMBDA x, acc: acc \cup {x.event.txnId},
    {}, TraceLog))

\* Extract set of shard IDs mentioned in the trace
TraceShards == TLCEval(FoldSeq(
    LAMBDA x, acc: acc \cup IF x.event.shardId /= "" THEN {x.event.shardId} ELSE {},
    {}, TraceLog))

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

RecoveryAutoAbortIfLogged(t) ==
    /\ LoglineIsTxnEvent("RecoveryResolve", t)
    /\ logline.event.shardId = ""  \* Recovery abort has no shard
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

TraceNext ==
    \/ /\ l <= Len(TraceLog)
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
    \/ TickClockIfNeeded

TraceSpec == TraceInit /\ [][TraceNext]_<<l, pl, vars>>

-------------------------------------------------------------------------------------

TraceView ==
    <<vars, l>>

-------------------------------------------------------------------------------------
\* TraceMatched: violated if TLC finishes with trace not fully consumed.

TraceMatched ==
    [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
