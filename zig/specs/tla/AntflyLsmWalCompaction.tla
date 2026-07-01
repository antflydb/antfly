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

------------------------- MODULE AntflyLsmWalCompaction -------------------------
(*
  Bounded WAL/checkpoint/compaction lifecycle model for storage/lsm_backend.

  Implementation correspondence:
  - currentSegment, segmentOfSeq, writtenSeqs, and durableSeqs abstract
    storage/lsm_backend/wal.zig's segmented state WAL.
  - checkpointSeq/deletedSegments/oldestRetainedSegment abstract
    retireCoveredSegments and checkpoint.index.
  - corruptTailSegment abstracts replayIntoMutable allowing a corrupt tail only
    on the current segment.
  - tableSeq/compactionActive/compactionInput abstract durable table publication
    after WAL-backed mutable state has been flushed/compacted.
  - readerPinnedSegment abstracts retained LSM/index readers that must keep their
    visible segment generation alive while compaction/retirement proceeds.

  Deliberate omissions:
  - Bytes, CRCs, and record payload decoding are represented as valid/corrupt
    segment state.
  - Atomic temp-file replacement is represented by all-or-nothing checkpoint and
    compaction publication actions.
  - Dedicated replay-row WAL segments are handled by AntflyDerivedReplay; this
    model focuses on state WAL durability and segment retirement.

  Bug classes:
  - checkpoint covers unsynced WAL entries;
  - a corrupt tail is rotated into a non-current segment, where replay must fail;
  - compaction/retirement deletes a reader-pinned segment.
*)

EXTENDS Naturals, TLC

CONSTANTS
    MaxSeq,
    MaxSegment,
    BuggyCheckpointUnsynced,
    BuggyRotateCorruptTail,
    BuggyRetirePinned

Seqs == 1..MaxSeq
Segments == 1..MaxSegment

VARIABLES
    walEnd,
    durableWalEnd,
    writtenSeqs,
    durableSeqs,
    segmentOfSeq,
    currentSegment,
    oldestRetainedSegment,
    checkpointSeq,
    mutableSeq,
    tableSeq,
    compactionActive,
    compactionInput,
    readerPinnedSegment,
    deletedSegments,
    corruptTailSegment

vars == <<walEnd, durableWalEnd, writtenSeqs, durableSeqs, segmentOfSeq,
          currentSegment, oldestRetainedSegment, checkpointSeq, mutableSeq,
          tableSeq, compactionActive, compactionInput, readerPinnedSegment,
          deletedSegments, corruptTailSegment>>

PrefixDurable(n, durable) ==
    \A s \in 1..n: s \in durable

DurableEnd(durable) ==
    CHOOSE n \in 0..MaxSeq:
        /\ PrefixDurable(n, durable)
        /\ \A m \in 0..MaxSeq: PrefixDurable(m, durable) => m <= n

SeqInSegment(seg) ==
    {s \in writtenSeqs : segmentOfSeq[s] = seg}

CanRetireSegment(seg) ==
    /\ seg = oldestRetainedSegment
    /\ seg < currentSegment
    /\ seg \notin deletedSegments
    /\ readerPinnedSegment # seg
    /\ \A s \in SeqInSegment(seg): s <= checkpointSeq

Init ==
    /\ walEnd = 0
    /\ durableWalEnd = 0
    /\ writtenSeqs = {}
    /\ durableSeqs = {}
    /\ segmentOfSeq = [s \in Seqs |-> 0]
    /\ currentSegment = 1
    /\ oldestRetainedSegment = 1
    /\ checkpointSeq = 0
    /\ mutableSeq = 0
    /\ tableSeq = 0
    /\ compactionActive = FALSE
    /\ compactionInput = 0
    /\ readerPinnedSegment = 0
    /\ deletedSegments = {}
    /\ corruptTailSegment = 0

\* appendStateWithOptions appends one valid state record to the current segment.
AppendWal(sync) ==
    /\ sync \in BOOLEAN
    /\ walEnd < MaxSeq
    /\ corruptTailSegment # currentSegment
    /\ LET seq == walEnd + 1
           newDurable == IF sync THEN durableSeqs \cup {seq} ELSE durableSeqs
       IN
       /\ walEnd' = seq
       /\ writtenSeqs' = writtenSeqs \cup {seq}
       /\ durableSeqs' = newDurable
       /\ durableWalEnd' = DurableEnd(newDurable)
       /\ segmentOfSeq' = [segmentOfSeq EXCEPT ![seq] = currentSegment]
       /\ mutableSeq' = seq
       /\ UNCHANGED <<currentSegment, oldestRetainedSegment, checkpointSeq,
                     tableSeq, compactionActive, compactionInput,
                     readerPinnedSegment, deletedSegments, corruptTailSegment>>

\* syncCurrentState makes every record in the current segment durable.
SyncCurrentSegment ==
    /\ SeqInSegment(currentSegment) # {}
    /\ LET newDurable == durableSeqs \cup SeqInSegment(currentSegment)
       IN
       /\ durableSeqs' = newDurable
       /\ durableWalEnd' = DurableEnd(newDurable)
       /\ UNCHANGED <<walEnd, writtenSeqs, segmentOfSeq, currentSegment,
                     oldestRetainedSegment, checkpointSeq, mutableSeq, tableSeq,
                     compactionActive, compactionInput, readerPinnedSegment,
                     deletedSegments, corruptTailSegment>>

\* Segment rotation is safe only after the old current segment is durable and
\* has no corrupt tail. replayIntoMutable only tolerates corruption on the
\* current segment.
RotateSegment ==
    /\ currentSegment < MaxSegment
    /\ SeqInSegment(currentSegment) # {}
    /\ SeqInSegment(currentSegment) \subseteq durableSeqs
    /\ corruptTailSegment # currentSegment
    /\ currentSegment' = currentSegment + 1
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, oldestRetainedSegment, checkpointSeq,
                  mutableSeq, tableSeq, compactionActive, compactionInput,
                  readerPinnedSegment, deletedSegments, corruptTailSegment>>

BuggyRotateSegmentWithCorruptTail ==
    /\ BuggyRotateCorruptTail
    /\ currentSegment < MaxSegment
    /\ SeqInSegment(currentSegment) # {}
    /\ corruptTailSegment = currentSegment
    /\ currentSegment' = currentSegment + 1
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, oldestRetainedSegment, checkpointSeq,
                  mutableSeq, tableSeq, compactionActive, compactionInput,
                  readerPinnedSegment, deletedSegments, corruptTailSegment>>

\* Environment/action representing junk after a valid prefix on the current WAL
\* segment, as exercised by the corrupt-tail replay tests.
InjectCurrentCorruptTail ==
    /\ SeqInSegment(currentSegment) # {}
    /\ corruptTailSegment = 0
    /\ corruptTailSegment' = currentSegment
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  checkpointSeq, mutableSeq, tableSeq, compactionActive,
                  compactionInput, readerPinnedSegment, deletedSegments>>

ReplayDropsCurrentCorruptTail ==
    /\ corruptTailSegment = currentSegment
    /\ corruptTailSegment' = 0
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  checkpointSeq, mutableSeq, tableSeq, compactionActive,
                  compactionInput, readerPinnedSegment, deletedSegments>>

\* Crash/reopen drops the non-durable suffix and keeps the durable prefix.
CrashDropsUnsyncedTail ==
    /\ walEnd > durableWalEnd
    /\ walEnd' = durableWalEnd
    /\ writtenSeqs' = 1..durableWalEnd
    /\ durableSeqs' = durableSeqs \cap (1..durableWalEnd)
    /\ mutableSeq' = durableWalEnd
    /\ compactionActive' = FALSE
    /\ compactionInput' = 0
    /\ corruptTailSegment' = 0
    /\ UNCHANGED <<durableWalEnd, segmentOfSeq, currentSegment,
                  oldestRetainedSegment, checkpointSeq, tableSeq,
                  readerPinnedSegment, deletedSegments>>

StartCompaction ==
    /\ ~compactionActive
    /\ tableSeq < mutableSeq
    /\ mutableSeq <= durableWalEnd
    /\ compactionActive' = TRUE
    /\ compactionInput' = mutableSeq
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  checkpointSeq, mutableSeq, tableSeq, readerPinnedSegment,
                  deletedSegments, corruptTailSegment>>

PublishCompaction ==
    /\ compactionActive
    /\ compactionInput <= durableWalEnd
    /\ tableSeq' = compactionInput
    /\ compactionActive' = FALSE
    /\ compactionInput' = 0
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  checkpointSeq, mutableSeq, readerPinnedSegment,
                  deletedSegments, corruptTailSegment>>

Checkpoint ==
    /\ tableSeq > checkpointSeq
    /\ tableSeq <= durableWalEnd
    /\ checkpointSeq' = tableSeq
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  mutableSeq, tableSeq, compactionActive, compactionInput,
                  readerPinnedSegment, deletedSegments, corruptTailSegment>>

BuggyCheckpointPastDurableWal ==
    /\ BuggyCheckpointUnsynced
    /\ walEnd > durableWalEnd
    /\ checkpointSeq' = walEnd
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  mutableSeq, tableSeq, compactionActive, compactionInput,
                  readerPinnedSegment, deletedSegments, corruptTailSegment>>

PinReader(seg) ==
    /\ seg \in Segments
    /\ seg \notin deletedSegments
    /\ SeqInSegment(seg) # {}
    /\ readerPinnedSegment = 0
    /\ readerPinnedSegment' = seg
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  checkpointSeq, mutableSeq, tableSeq, compactionActive,
                  compactionInput, deletedSegments, corruptTailSegment>>

UnpinReader ==
    /\ readerPinnedSegment # 0
    /\ readerPinnedSegment' = 0
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, oldestRetainedSegment,
                  checkpointSeq, mutableSeq, tableSeq, compactionActive,
                  compactionInput, deletedSegments, corruptTailSegment>>

RetireCoveredSegment ==
    /\ CanRetireSegment(oldestRetainedSegment)
    /\ deletedSegments' = deletedSegments \cup {oldestRetainedSegment}
    /\ oldestRetainedSegment' = oldestRetainedSegment + 1
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, checkpointSeq, mutableSeq,
                  tableSeq, compactionActive, compactionInput,
                  readerPinnedSegment, corruptTailSegment>>

BuggyRetirePinnedSegment ==
    /\ BuggyRetirePinned
    /\ oldestRetainedSegment < currentSegment
    /\ oldestRetainedSegment \notin deletedSegments
    /\ readerPinnedSegment = oldestRetainedSegment
    /\ \A s \in SeqInSegment(oldestRetainedSegment): s <= checkpointSeq
    /\ deletedSegments' = deletedSegments \cup {oldestRetainedSegment}
    /\ oldestRetainedSegment' = oldestRetainedSegment + 1
    /\ UNCHANGED <<walEnd, durableWalEnd, writtenSeqs, durableSeqs,
                  segmentOfSeq, currentSegment, checkpointSeq, mutableSeq,
                  tableSeq, compactionActive, compactionInput,
                  readerPinnedSegment, corruptTailSegment>>

Next ==
    \/ \E sync \in BOOLEAN: AppendWal(sync)
    \/ SyncCurrentSegment
    \/ RotateSegment
    \/ BuggyRotateSegmentWithCorruptTail
    \/ InjectCurrentCorruptTail
    \/ ReplayDropsCurrentCorruptTail
    \/ CrashDropsUnsyncedTail
    \/ StartCompaction
    \/ PublishCompaction
    \/ Checkpoint
    \/ BuggyCheckpointPastDurableWal
    \/ \E seg \in Segments: PinReader(seg)
    \/ UnpinReader
    \/ RetireCoveredSegment
    \/ BuggyRetirePinnedSegment

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ MaxSeq \in 1..5
    /\ MaxSegment \in 1..4
    /\ BuggyCheckpointUnsynced \in BOOLEAN
    /\ BuggyRotateCorruptTail \in BOOLEAN
    /\ BuggyRetirePinned \in BOOLEAN
    /\ walEnd \in 0..MaxSeq
    /\ durableWalEnd \in 0..MaxSeq
    /\ writtenSeqs \subseteq Seqs
    /\ durableSeqs \subseteq writtenSeqs
    /\ segmentOfSeq \in [Seqs -> 0..MaxSegment]
    /\ currentSegment \in Segments
    /\ oldestRetainedSegment \in Segments \cup {MaxSegment + 1}
    /\ checkpointSeq \in 0..MaxSeq
    /\ mutableSeq \in 0..MaxSeq
    /\ tableSeq \in 0..MaxSeq
    /\ compactionActive \in BOOLEAN
    /\ compactionInput \in 0..MaxSeq
    /\ readerPinnedSegment \in 0..MaxSegment
    /\ deletedSegments \subseteq Segments
    /\ corruptTailSegment \in 0..MaxSegment

SequenceOrder ==
    /\ writtenSeqs = 1..walEnd
    /\ durableWalEnd = DurableEnd(durableSeqs)
    /\ durableWalEnd <= walEnd
    /\ checkpointSeq <= tableSeq
    /\ tableSeq <= mutableSeq
    /\ mutableSeq <= walEnd
    /\ checkpointSeq <= durableWalEnd
    /\ currentSegment \notin deletedSegments
    /\ oldestRetainedSegment <= currentSegment
    /\ \A s \in writtenSeqs: segmentOfSeq[s] \in Segments
    /\ \A i, j \in writtenSeqs:
        i < j => segmentOfSeq[i] <= segmentOfSeq[j]

CompactionInputBounded ==
    compactionActive =>
        /\ compactionInput > 0
        /\ compactionInput <= durableWalEnd
        /\ tableSeq < compactionInput

RetiredSegmentsSafe ==
    \A seg \in deletedSegments:
        /\ seg < currentSegment
        /\ \A s \in SeqInSegment(seg): s <= checkpointSeq

ReaderPinsRetained ==
    readerPinnedSegment # 0 =>
        /\ readerPinnedSegment \in Segments
        /\ readerPinnedSegment \notin deletedSegments

CorruptTailOnlyOnCurrentSegment ==
    corruptTailSegment = 0 \/ corruptTailSegment = currentSegment

CheckpointOnlyCoversDurableWal ==
    checkpointSeq <= durableWalEnd

Safety ==
    /\ TypeOK
    /\ SequenceOrder
    /\ CompactionInputBounded
    /\ RetiredSegmentsSafe
    /\ ReaderPinsRetained
    /\ CorruptTailOnlyOnCurrentSegment
    /\ CheckpointOnlyCoversDurableWal

=============================================================================
