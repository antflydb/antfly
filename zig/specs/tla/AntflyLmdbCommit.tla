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

----------------------------- MODULE AntflyLmdbCommit -----------------------------
(*
  Crash-point and reader-reclamation model of the Zig LMDB commit path.

  Code anchors:
  - pkg/antfly/src/lmdb/txn.zig
  - pkg/antfly/src/lmdb/prepare_commit_support.zig
  - pkg/antfly/src/lmdb/commit_support.zig
  - pkg/antfly/src/lmdb/free_db.zig
  - pkg/antfly/src/lmdb/readers.zig
*)

EXTENDS Naturals, TLC

CONSTANTS MaxTxn, MaxPage, BuggyMetaBeforeDataSync, BuggyReuseReaderVisible

Metas == {0, 1}
Readers == {"r1", "r2"}
TxnIds == 0..MaxTxn
Pages == 1..MaxPage
NoTxn == MaxTxn + 1
PageTxn == 0..NoTxn

VARIABLES
    activeMeta,
    metaTxn,
    durableDataTxn,
    pageTxn,
    snapshotPages,
    freeRecordTxn,
    writerActive,
    writerTxn,
    parentDirty,
    childActive,
    childDirty,
    dataWritten,
    dataSynced,
    metaWritten,
    metaSynced,
    preparedPages,
    preparedRetired,
    readerTxn

vars == <<activeMeta, metaTxn, durableDataTxn, pageTxn, snapshotPages,
          freeRecordTxn, writerActive, writerTxn, parentDirty, childActive,
          childDirty, dataWritten, dataSynced, metaWritten, metaSynced,
          preparedPages, preparedRetired, readerTxn>>

ActiveTxn == metaTxn[activeMeta]
InactiveMeta == 1 - activeMeta
ActiveReaderTxns == {readerTxn[r] : r \in Readers} \ {NoTxn}
MinTxn(S) == CHOOSE t \in S: \A u \in S: t <= u
AllSnapshotPages == UNION {snapshotPages[t] : t \in TxnIds}

ReaderAllowsReuse(retireTxn) ==
    ActiveReaderTxns = {} \/ retireTxn <= MinTxn(ActiveReaderTxns)

FreshPages ==
    {p \in Pages : pageTxn[p] = NoTxn /\ freeRecordTxn[p] = NoTxn}

ReusablePages ==
    {p \in Pages : freeRecordTxn[p] # NoTxn /\ ReaderAllowsReuse(freeRecordTxn[p])}

ReaderBlockedReusablePages ==
    {p \in Pages : freeRecordTxn[p] # NoTxn /\ ~ReaderAllowsReuse(freeRecordTxn[p])}

CommitFreeRecords(nextPages, retiredPages, txnid) ==
    [p \in Pages |->
        IF p \in nextPages THEN NoTxn
        ELSE IF p \in retiredPages THEN txnid
        ELSE freeRecordTxn[p]]

Init ==
    /\ activeMeta = 0
    /\ metaTxn = [m \in Metas |-> 0]
    /\ durableDataTxn = 0
    /\ pageTxn = [p \in Pages |-> IF p = 1 THEN 0 ELSE NoTxn]
    /\ snapshotPages = [t \in TxnIds |-> IF t = 0 THEN {1} ELSE {}]
    /\ freeRecordTxn = [p \in Pages |-> NoTxn]
    /\ writerActive = FALSE
    /\ writerTxn = 0
    /\ parentDirty = FALSE
    /\ childActive = FALSE
    /\ childDirty = FALSE
    /\ dataWritten = FALSE
    /\ dataSynced = FALSE
    /\ metaWritten = FALSE
    /\ metaSynced = FALSE
    /\ preparedPages = {}
    /\ preparedRetired = {}
    /\ readerTxn = [r \in Readers |-> NoTxn]

BeginWrite ==
    /\ ~writerActive
    /\ ActiveTxn < MaxTxn
    /\ writerActive' = TRUE
    /\ writerTxn' = ActiveTxn + 1
    /\ parentDirty' = FALSE
    /\ childActive' = FALSE
    /\ childDirty' = FALSE
    /\ dataWritten' = FALSE
    /\ dataSynced' = FALSE
    /\ metaWritten' = FALSE
    /\ metaSynced' = FALSE
    /\ preparedPages' = {}
    /\ preparedRetired' = {}
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, readerTxn>>

ParentPut ==
    /\ writerActive
    /\ ~childActive
    /\ ~dataWritten
    /\ parentDirty' = TRUE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  childActive, childDirty, dataWritten, dataSynced,
                  metaWritten, metaSynced, preparedPages, preparedRetired,
                  readerTxn>>

BeginChild ==
    /\ writerActive
    /\ ~childActive
    /\ ~dataWritten
    /\ childActive' = TRUE
    /\ childDirty' = FALSE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  parentDirty, dataWritten, dataSynced, metaWritten,
                  metaSynced, preparedPages, preparedRetired, readerTxn>>

ChildPut ==
    /\ childActive
    /\ childDirty' = TRUE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  parentDirty, childActive, dataWritten, dataSynced,
                  metaWritten, metaSynced, preparedPages, preparedRetired,
                  readerTxn>>

CommitChild ==
    /\ childActive
    /\ childDirty
    /\ parentDirty' = TRUE
    /\ childActive' = FALSE
    /\ childDirty' = FALSE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  dataWritten, dataSynced, metaWritten, metaSynced,
                  preparedPages, preparedRetired, readerTxn>>

AbortChild ==
    /\ childActive
    /\ childActive' = FALSE
    /\ childDirty' = FALSE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  parentDirty, dataWritten, dataSynced, metaWritten,
                  metaSynced, preparedPages, preparedRetired, readerTxn>>

WriteDataPage(p) ==
    /\ writerActive
    /\ parentDirty
    /\ ~childActive
    /\ ~dataWritten
    /\ p \in FreshPages \cup ReusablePages
    /\ pageTxn' = [pageTxn EXCEPT ![p] = writerTxn]
    /\ preparedPages' = {p}
    /\ preparedRetired' = snapshotPages[ActiveTxn]
    /\ dataWritten' = TRUE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, snapshotPages,
                  freeRecordTxn, writerActive, writerTxn, parentDirty,
                  childActive, childDirty, dataSynced, metaWritten,
                  metaSynced, readerTxn>>

BuggyWriteDataReusesReaderPage(p) ==
    /\ BuggyReuseReaderVisible
    /\ writerActive
    /\ parentDirty
    /\ ~childActive
    /\ ~dataWritten
    /\ p \in ReaderBlockedReusablePages
    /\ pageTxn' = [pageTxn EXCEPT ![p] = writerTxn]
    /\ preparedPages' = {p}
    /\ preparedRetired' = snapshotPages[ActiveTxn]
    /\ dataWritten' = TRUE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, snapshotPages,
                  freeRecordTxn, writerActive, writerTxn, parentDirty,
                  childActive, childDirty, dataSynced, metaWritten,
                  metaSynced, readerTxn>>

SyncData ==
    /\ writerActive
    /\ dataWritten
    /\ ~dataSynced
    /\ durableDataTxn' = writerTxn
    /\ dataSynced' = TRUE
    /\ UNCHANGED <<activeMeta, metaTxn, pageTxn, snapshotPages,
                  freeRecordTxn, writerActive, writerTxn, parentDirty,
                  childActive, childDirty, dataWritten, metaWritten,
                  metaSynced, preparedPages, preparedRetired, readerTxn>>

WriteMeta ==
    /\ writerActive
    /\ dataSynced
    /\ ~childActive
    /\ ~metaWritten
    /\ metaTxn' = [metaTxn EXCEPT ![InactiveMeta] = writerTxn]
    /\ metaWritten' = TRUE
    /\ UNCHANGED <<activeMeta, durableDataTxn, pageTxn, snapshotPages,
                  freeRecordTxn, writerActive, writerTxn, parentDirty,
                  childActive, childDirty, dataWritten, dataSynced,
                  metaSynced, preparedPages, preparedRetired, readerTxn>>

BuggyWriteMetaBeforeDataSync ==
    /\ BuggyMetaBeforeDataSync
    /\ writerActive
    /\ dataWritten
    /\ ~childActive
    /\ ~metaWritten
    /\ metaTxn' = [metaTxn EXCEPT ![InactiveMeta] = writerTxn]
    /\ metaWritten' = TRUE
    /\ UNCHANGED <<activeMeta, durableDataTxn, pageTxn, snapshotPages,
                  freeRecordTxn, writerActive, writerTxn, parentDirty,
                  childActive, childDirty, dataWritten, dataSynced,
                  metaSynced, preparedPages, preparedRetired, readerTxn>>

SyncMeta ==
    /\ writerActive
    /\ metaWritten
    /\ ~metaSynced
    /\ metaSynced' = TRUE
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  parentDirty, childActive, childDirty, dataWritten,
                  dataSynced, metaWritten, preparedPages, preparedRetired,
                  readerTxn>>

PublishMeta ==
    /\ writerActive
    /\ metaSynced
    /\ activeMeta' = InactiveMeta
    /\ snapshotPages' = [snapshotPages EXCEPT ![writerTxn] = preparedPages]
    /\ freeRecordTxn' = CommitFreeRecords(preparedPages, preparedRetired, writerTxn)
    /\ writerActive' = FALSE
    /\ writerTxn' = 0
    /\ parentDirty' = FALSE
    /\ childActive' = FALSE
    /\ childDirty' = FALSE
    /\ dataWritten' = FALSE
    /\ dataSynced' = FALSE
    /\ metaWritten' = FALSE
    /\ metaSynced' = FALSE
    /\ preparedPages' = {}
    /\ preparedRetired' = {}
    /\ UNCHANGED <<metaTxn, durableDataTxn, pageTxn, readerTxn>>

AbortWrite ==
    /\ writerActive
    /\ ~dataWritten
    /\ writerActive' = FALSE
    /\ writerTxn' = 0
    /\ parentDirty' = FALSE
    /\ childActive' = FALSE
    /\ childDirty' = FALSE
    /\ dataWritten' = FALSE
    /\ dataSynced' = FALSE
    /\ metaWritten' = FALSE
    /\ metaSynced' = FALSE
    /\ preparedPages' = {}
    /\ preparedRetired' = {}
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, readerTxn>>

CrashAndReopen ==
    /\ writerActive
    /\ LET useNewMeta == metaSynced /\ metaTxn[InactiveMeta] <= durableDataTxn IN
        /\ activeMeta' = IF useNewMeta THEN InactiveMeta ELSE activeMeta
        /\ snapshotPages' =
            IF useNewMeta THEN [snapshotPages EXCEPT ![writerTxn] = preparedPages]
            ELSE snapshotPages
        /\ freeRecordTxn' =
            IF useNewMeta THEN CommitFreeRecords(preparedPages, preparedRetired, writerTxn)
            ELSE freeRecordTxn
    /\ writerActive' = FALSE
    /\ writerTxn' = 0
    /\ parentDirty' = FALSE
    /\ childActive' = FALSE
    /\ childDirty' = FALSE
    /\ dataWritten' = FALSE
    /\ dataSynced' = FALSE
    /\ metaWritten' = FALSE
    /\ metaSynced' = FALSE
    /\ preparedPages' = {}
    /\ preparedRetired' = {}
    /\ readerTxn' = [r \in Readers |-> NoTxn]
    /\ UNCHANGED <<metaTxn, durableDataTxn, pageTxn>>

BeginReader(r) ==
    /\ r \in Readers
    /\ readerTxn[r] = NoTxn
    /\ readerTxn' = [readerTxn EXCEPT ![r] = ActiveTxn]
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  parentDirty, childActive, childDirty, dataWritten,
                  dataSynced, metaWritten, metaSynced, preparedPages,
                  preparedRetired>>

EndReader(r) ==
    /\ r \in Readers
    /\ readerTxn[r] # NoTxn
    /\ readerTxn' = [readerTxn EXCEPT ![r] = NoTxn]
    /\ UNCHANGED <<activeMeta, metaTxn, durableDataTxn, pageTxn,
                  snapshotPages, freeRecordTxn, writerActive, writerTxn,
                  parentDirty, childActive, childDirty, dataWritten,
                  dataSynced, metaWritten, metaSynced, preparedPages,
                  preparedRetired>>

Next ==
    \/ BeginWrite
    \/ ParentPut
    \/ BeginChild
    \/ ChildPut
    \/ CommitChild
    \/ AbortChild
    \/ \E p \in Pages:
        \/ WriteDataPage(p)
        \/ BuggyWriteDataReusesReaderPage(p)
    \/ SyncData
    \/ WriteMeta
    \/ BuggyWriteMetaBeforeDataSync
    \/ SyncMeta
    \/ PublishMeta
    \/ AbortWrite
    \/ CrashAndReopen
    \/ \E r \in Readers:
        \/ BeginReader(r)
        \/ EndReader(r)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ activeMeta \in Metas
    /\ metaTxn \in [Metas -> TxnIds]
    /\ durableDataTxn \in TxnIds
    /\ pageTxn \in [Pages -> PageTxn]
    /\ snapshotPages \in [TxnIds -> SUBSET Pages]
    /\ freeRecordTxn \in [Pages -> PageTxn]
    /\ writerActive \in BOOLEAN
    /\ writerTxn \in TxnIds
    /\ parentDirty \in BOOLEAN
    /\ childActive \in BOOLEAN
    /\ childDirty \in BOOLEAN
    /\ dataWritten \in BOOLEAN
    /\ dataSynced \in BOOLEAN
    /\ metaWritten \in BOOLEAN
    /\ metaSynced \in BOOLEAN
    /\ preparedPages \subseteq Pages
    /\ preparedRetired \subseteq Pages
    /\ readerTxn \in [Readers -> PageTxn]

PublishedMetaHasData ==
    metaTxn[activeMeta] <= durableDataTxn

MetaWriteAfterDataSync ==
    metaWritten => /\ dataSynced
                   /\ writerTxn <= durableDataTxn
                   /\ metaTxn[InactiveMeta] = writerTxn

ReaderSnapshotVisible ==
    \A r \in Readers:
        readerTxn[r] # NoTxn =>
            /\ readerTxn[r] <= ActiveTxn
            /\ \A p \in snapshotPages[readerTxn[r]]: pageTxn[p] = readerTxn[r]

CommittedPagesMatchActiveMeta ==
    \A p \in snapshotPages[ActiveTxn]: pageTxn[p] = ActiveTxn

RetiredPagesNotInActiveSnapshot ==
    \A p \in Pages:
        freeRecordTxn[p] # NoTxn => p \notin snapshotPages[ActiveTxn]

CommitPhasesDoNotRaceChild ==
    (dataWritten \/ dataSynced \/ metaWritten \/ metaSynced) =>
        /\ writerActive
        /\ parentDirty
        /\ ~childActive

PreparedCommitShape ==
    dataWritten => /\ preparedPages # {}
                   /\ preparedRetired = snapshotPages[ActiveTxn]

SingleWriter ==
    writerActive => writerTxn > ActiveTxn

Safety ==
    /\ TypeOK
    /\ PublishedMetaHasData
    /\ MetaWriteAfterDataSync
    /\ ReaderSnapshotVisible
    /\ CommittedPagesMatchActiveMeta
    /\ RetiredPagesNotInActiveSnapshot
    /\ CommitPhasesDoNotRaceChild
    /\ PreparedCommitShape
    /\ SingleWriter

=============================================================================
