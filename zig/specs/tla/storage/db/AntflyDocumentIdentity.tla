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

------------------------- MODULE AntflyDocumentIdentity -------------------------
(*
  Bounded model of document identity namespace, stable ordinals, generation
  visibility, resolved-doc-filter context, and namespace repair/open behavior.

  Concrete Zig contracts modeled:
    - doc_identity.ensureOrdinal* allocates an ordinal once per logical document.
    - deletes mark deleted_generation; live/current visibility excludes
      tombstoned ordinals.
    - public search accepts only the current identity_read_generation.
    - resolved-doc-filter wire context must match both namespace and generation.
    - namespace reassignment rewrites canonical rows for all allocated ordinals.
    - strict reopen rejects namespace mismatches unless repaired.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyReuseOrdinal,
    BuggyAcceptStaleFilter,
    BuggyAcceptNamespaceMismatch

Docs == {"docA", "docB"}
NoDoc == "none"
Owners == Docs \cup {NoDoc}
Namespaces == 1..2
MaxOrdinal == 2
MaxGeneration == 4
Ordinals == 1..MaxOrdinal

VARIABLES
    currentGeneration,
    nextOrdinal,
    primaryLive,
    docOrdinal,
    ordinalOwner,
    everOwner,
    createdGeneration,
    deletedGeneration,
    storedNamespace,
    canonicalNamespace,
    filterOrdinal,
    filterGeneration,
    filterNamespace,
    filterAccepted,
    openConfiguredNamespace,
    openAccepted

vars == <<currentGeneration, nextOrdinal, primaryLive, docOrdinal,
          ordinalOwner, everOwner, createdGeneration, deletedGeneration,
          storedNamespace, canonicalNamespace, filterOrdinal, filterGeneration,
          filterNamespace, filterAccepted, openConfiguredNamespace, openAccepted>>

Init ==
    /\ currentGeneration = 1
    /\ nextOrdinal = 1
    /\ primaryLive = [d \in Docs |-> FALSE]
    /\ docOrdinal = [d \in Docs |-> 0]
    /\ ordinalOwner = [o \in Ordinals |-> NoDoc]
    /\ everOwner = [o \in Ordinals |-> NoDoc]
    /\ createdGeneration = [o \in Ordinals |-> 0]
    /\ deletedGeneration = [o \in Ordinals |-> 0]
    /\ storedNamespace = 1
    /\ canonicalNamespace = [o \in Ordinals |-> 0]
    /\ filterOrdinal = 0
    /\ filterGeneration = 1
    /\ filterNamespace = 1
    /\ filterAccepted = FALSE
    /\ openConfiguredNamespace = 1
    /\ openAccepted = TRUE

Allocated(o) ==
    createdGeneration[o] # 0

VisibleAt(o, gen) ==
    /\ o \in Ordinals
    /\ Allocated(o)
    /\ createdGeneration[o] <= gen
    /\ deletedGeneration[o] = 0 \/ deletedGeneration[o] > gen

AdvanceGeneration ==
    currentGeneration < MaxGeneration

InsertNew(d) ==
    /\ d \in Docs
    /\ ~primaryLive[d]
    /\ docOrdinal[d] = 0
    /\ nextOrdinal <= MaxOrdinal
    /\ AdvanceGeneration
    /\ currentGeneration' = currentGeneration + 1
    /\ primaryLive' = [primaryLive EXCEPT ![d] = TRUE]
    /\ docOrdinal' = [docOrdinal EXCEPT ![d] = nextOrdinal]
    /\ ordinalOwner' = [ordinalOwner EXCEPT ![nextOrdinal] = d]
    /\ everOwner' = [everOwner EXCEPT ![nextOrdinal] = d]
    /\ createdGeneration' = [createdGeneration EXCEPT ![nextOrdinal] = currentGeneration']
    /\ deletedGeneration' = [deletedGeneration EXCEPT ![nextOrdinal] = 0]
    /\ canonicalNamespace' = [canonicalNamespace EXCEPT ![nextOrdinal] = storedNamespace]
    /\ nextOrdinal' = nextOrdinal + 1
    /\ filterAccepted' = FALSE
    /\ openAccepted' = FALSE
    /\ UNCHANGED <<storedNamespace, filterOrdinal, filterGeneration,
                  filterNamespace, openConfiguredNamespace>>

UpdateExisting(d) ==
    /\ d \in Docs
    /\ primaryLive[d]
    /\ AdvanceGeneration
    /\ currentGeneration' = currentGeneration + 1
    /\ filterAccepted' = FALSE
    /\ openAccepted' = FALSE
    /\ UNCHANGED <<nextOrdinal, primaryLive, docOrdinal, ordinalOwner,
                  everOwner, createdGeneration, deletedGeneration,
                  storedNamespace, canonicalNamespace, filterOrdinal,
                  filterGeneration, filterNamespace, openConfiguredNamespace>>

DeleteDoc(d) ==
    /\ d \in Docs
    /\ primaryLive[d]
    /\ AdvanceGeneration
    /\ currentGeneration' = currentGeneration + 1
    /\ primaryLive' = [primaryLive EXCEPT ![d] = FALSE]
    /\ deletedGeneration' = [deletedGeneration EXCEPT ![docOrdinal[d]] = currentGeneration']
    /\ filterAccepted' = FALSE
    /\ openAccepted' = FALSE
    /\ UNCHANGED <<nextOrdinal, docOrdinal, ordinalOwner, everOwner,
                  createdGeneration, storedNamespace, canonicalNamespace,
                  filterOrdinal, filterGeneration, filterNamespace,
                  openConfiguredNamespace>>

ResurrectDoc(d) ==
    /\ d \in Docs
    /\ ~primaryLive[d]
    /\ docOrdinal[d] # 0
    /\ AdvanceGeneration
    /\ currentGeneration' = currentGeneration + 1
    /\ primaryLive' = [primaryLive EXCEPT ![d] = TRUE]
    /\ createdGeneration' = [createdGeneration EXCEPT ![docOrdinal[d]] = currentGeneration']
    /\ deletedGeneration' = [deletedGeneration EXCEPT ![docOrdinal[d]] = 0]
    /\ filterAccepted' = FALSE
    /\ openAccepted' = FALSE
    /\ UNCHANGED <<nextOrdinal, docOrdinal, ordinalOwner, everOwner,
                  storedNamespace, canonicalNamespace, filterOrdinal,
                  filterGeneration, filterNamespace, openConfiguredNamespace>>

BuggyReuseTombstonedOrdinal(d, o) ==
    /\ BuggyReuseOrdinal
    /\ d \in Docs
    /\ o \in Ordinals
    /\ ~primaryLive[d]
    /\ docOrdinal[d] = 0
    /\ Allocated(o)
    /\ deletedGeneration[o] # 0
    /\ ordinalOwner[o] # d
    /\ AdvanceGeneration
    /\ currentGeneration' = currentGeneration + 1
    /\ primaryLive' = [primaryLive EXCEPT ![d] = TRUE]
    /\ docOrdinal' = [docOrdinal EXCEPT ![d] = o]
    /\ ordinalOwner' = [ordinalOwner EXCEPT ![o] = d]
    /\ createdGeneration' = [createdGeneration EXCEPT ![o] = currentGeneration']
    /\ deletedGeneration' = [deletedGeneration EXCEPT ![o] = 0]
    /\ canonicalNamespace' = [canonicalNamespace EXCEPT ![o] = storedNamespace]
    /\ filterAccepted' = FALSE
    /\ openAccepted' = FALSE
    /\ UNCHANGED <<nextOrdinal, everOwner, storedNamespace, filterOrdinal,
                  filterGeneration, filterNamespace, openConfiguredNamespace>>

ReassignNamespace ==
    /\ storedNamespace = 1
    /\ storedNamespace' = 2
    /\ canonicalNamespace' = [o \in Ordinals |->
        IF Allocated(o) THEN 2 ELSE canonicalNamespace[o]]
    /\ filterAccepted' = FALSE
    /\ openAccepted' = FALSE
    /\ UNCHANGED <<currentGeneration, nextOrdinal, primaryLive, docOrdinal,
                  ordinalOwner, everOwner, createdGeneration, deletedGeneration,
                  filterOrdinal, filterGeneration, filterNamespace,
                  openConfiguredNamespace>>

BuildWireFilter(d) ==
    /\ d \in Docs
    /\ primaryLive[d]
    /\ filterOrdinal' = docOrdinal[d]
    /\ filterGeneration' = currentGeneration
    /\ filterNamespace' = storedNamespace
    /\ filterAccepted' = FALSE
    /\ UNCHANGED <<currentGeneration, nextOrdinal, primaryLive, docOrdinal,
                  ordinalOwner, everOwner, createdGeneration, deletedGeneration,
                  storedNamespace, canonicalNamespace, openConfiguredNamespace,
                  openAccepted>>

UseWireFilter ==
    /\ filterOrdinal \in Ordinals
    /\ filterGeneration = currentGeneration
    /\ filterNamespace = storedNamespace
    /\ VisibleAt(filterOrdinal, currentGeneration)
    /\ filterAccepted' = TRUE
    /\ UNCHANGED <<currentGeneration, nextOrdinal, primaryLive, docOrdinal,
                  ordinalOwner, everOwner, createdGeneration, deletedGeneration,
                  storedNamespace, canonicalNamespace, filterOrdinal,
                  filterGeneration, filterNamespace, openConfiguredNamespace,
                  openAccepted>>

BuggyUseStaleFilter ==
    /\ BuggyAcceptStaleFilter
    /\ filterOrdinal \in Ordinals
    /\ filterGeneration # currentGeneration
    /\ filterNamespace = storedNamespace
    /\ VisibleAt(filterOrdinal, currentGeneration)
    /\ filterAccepted' = TRUE
    /\ UNCHANGED <<currentGeneration, nextOrdinal, primaryLive, docOrdinal,
                  ordinalOwner, everOwner, createdGeneration, deletedGeneration,
                  storedNamespace, canonicalNamespace, filterOrdinal,
                  filterGeneration, filterNamespace, openConfiguredNamespace,
                  openAccepted>>

OpenWithConfiguredNamespace(ns) ==
    /\ ns \in Namespaces
    /\ openConfiguredNamespace' = ns
    /\ openAccepted' = (ns = storedNamespace)
    /\ UNCHANGED <<currentGeneration, nextOrdinal, primaryLive, docOrdinal,
                  ordinalOwner, everOwner, createdGeneration, deletedGeneration,
                  storedNamespace, canonicalNamespace, filterOrdinal,
                  filterGeneration, filterNamespace, filterAccepted>>

BuggyOpenNamespaceMismatch(ns) ==
    /\ BuggyAcceptNamespaceMismatch
    /\ ns \in Namespaces
    /\ ns # storedNamespace
    /\ openConfiguredNamespace' = ns
    /\ openAccepted' = TRUE
    /\ UNCHANGED <<currentGeneration, nextOrdinal, primaryLive, docOrdinal,
                  ordinalOwner, everOwner, createdGeneration, deletedGeneration,
                  storedNamespace, canonicalNamespace, filterOrdinal,
                  filterGeneration, filterNamespace, filterAccepted>>

Next ==
    \/ ReassignNamespace
    \/ BuildWireFilter("docA")
    \/ BuildWireFilter("docB")
    \/ UseWireFilter
    \/ BuggyUseStaleFilter
    \/ \E ns \in Namespaces:
        \/ OpenWithConfiguredNamespace(ns)
        \/ BuggyOpenNamespaceMismatch(ns)
    \/ \E d \in Docs:
        \/ InsertNew(d)
        \/ UpdateExisting(d)
        \/ DeleteDoc(d)
        \/ ResurrectDoc(d)
        \/ \E o \in Ordinals: BuggyReuseTombstonedOrdinal(d, o)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ currentGeneration \in 1..MaxGeneration
    /\ nextOrdinal \in 1..(MaxOrdinal + 1)
    /\ primaryLive \in [Docs -> BOOLEAN]
    /\ docOrdinal \in [Docs -> 0..MaxOrdinal]
    /\ ordinalOwner \in [Ordinals -> Owners]
    /\ everOwner \in [Ordinals -> Owners]
    /\ createdGeneration \in [Ordinals -> 0..MaxGeneration]
    /\ deletedGeneration \in [Ordinals -> 0..MaxGeneration]
    /\ storedNamespace \in Namespaces
    /\ canonicalNamespace \in [Ordinals -> 0..2]
    /\ filterOrdinal \in 0..MaxOrdinal
    /\ filterGeneration \in 1..MaxGeneration
    /\ filterNamespace \in Namespaces
    /\ filterAccepted \in BOOLEAN
    /\ openConfiguredNamespace \in Namespaces
    /\ openAccepted \in BOOLEAN

AllocatedOrdinalsHaveStableOwner ==
    \A o \in Ordinals:
        /\ (everOwner[o] = NoDoc) = (ordinalOwner[o] = NoDoc)
        /\ everOwner[o] # NoDoc => ordinalOwner[o] = everOwner[o]

LiveDocsHaveVisibleOrdinals ==
    \A d \in Docs:
        primaryLive[d] =>
            /\ docOrdinal[d] \in Ordinals
            /\ ordinalOwner[docOrdinal[d]] = d
            /\ VisibleAt(docOrdinal[d], currentGeneration)

NoTwoLiveDocsShareOrdinal ==
    \A d1 \in Docs:
        \A d2 \in Docs:
            d1 # d2 /\ primaryLive[d1] /\ primaryLive[d2] =>
                docOrdinal[d1] # docOrdinal[d2]

TombstoneHidesCurrentGeneration ==
    \A o \in Ordinals:
        deletedGeneration[o] # 0 /\ deletedGeneration[o] <= currentGeneration =>
            ~VisibleAt(o, currentGeneration)

ResolvedFilterMatchesCurrentContext ==
    filterAccepted =>
        /\ filterOrdinal \in Ordinals
        /\ filterGeneration = currentGeneration
        /\ filterNamespace = storedNamespace
        /\ VisibleAt(filterOrdinal, currentGeneration)

CanonicalRowsMatchStoredNamespace ==
    \A o \in Ordinals:
        Allocated(o) => canonicalNamespace[o] = storedNamespace

StrictOpenRejectsNamespaceMismatch ==
    openAccepted => openConfiguredNamespace = storedNamespace

Safety ==
    /\ TypeOK
    /\ AllocatedOrdinalsHaveStableOwner
    /\ LiveDocsHaveVisibleOrdinals
    /\ NoTwoLiveDocsShareOrdinal
    /\ TombstoneHidesCurrentGeneration
    /\ ResolvedFilterMatchesCurrentContext
    /\ CanonicalRowsMatchStoredNamespace
    /\ StrictOpenRejectsNamespaceMismatch

=============================================================================
