----------------------------- MODULE AntflyQueryCompleteness -----------------------------
EXTENDS Naturals, TLC

(*
  Focused split-routing query completeness model.

  This is intentionally a control-plane/visibility model, not a ranking,
  vector, analyzer, or byte-content model. It checks that a query crossing a
  split boundary cannot miss or double-count a document when route publication,
  child serving, and parent ownership change in different orders.
*)

CONSTANTS
  BuggyRouteBeforeChildReady,
  BuggyDoubleServe,
  BuggyDropMovedDoc

Docs == {"left", "right"}
LeftDoc == "left"
RightDoc == "right"

VARIABLES
  parentRightOwned,
  childHasRight,
  childServing,
  routeToChild,
  parentStillScansRight,
  queryRan,
  resultCount

vars == << parentRightOwned,
          childHasRight,
          childServing,
          routeToChild,
          parentStillScansRight,
          queryRan,
          resultCount >>

ZeroCounts == [d \in Docs |-> 0]

Init ==
  /\ parentRightOwned = TRUE
  /\ childHasRight = FALSE
  /\ childServing = FALSE
  /\ routeToChild = FALSE
  /\ parentStillScansRight = TRUE
  /\ queryRan = FALSE
  /\ resultCount = ZeroCounts

CopyRightDocToChild ==
  /\ ~childHasRight
  /\ childHasRight' = TRUE
  /\ UNCHANGED << parentRightOwned, childServing, routeToChild,
                  parentStillScansRight, queryRan, resultCount >>

PublishChildServing ==
  /\ childHasRight \/ BuggyDropMovedDoc
  /\ ~childServing
  /\ childServing' = TRUE
  /\ UNCHANGED << parentRightOwned, childHasRight, routeToChild,
                  parentStillScansRight, queryRan, resultCount >>

PublishRouteToChild ==
  /\ ~routeToChild
  /\ IF BuggyRouteBeforeChildReady
        THEN TRUE
        ELSE childServing
  /\ routeToChild' = TRUE
  /\ parentRightOwned' = FALSE
  /\ parentStillScansRight' = BuggyDoubleServe \/ FALSE
  /\ UNCHANGED << childHasRight, childServing, queryRan, resultCount >>

FinishParentTrim ==
  /\ routeToChild
  /\ parentStillScansRight
  /\ parentStillScansRight' = FALSE
  /\ UNCHANGED << parentRightOwned, childHasRight, childServing, routeToChild,
                  queryRan, resultCount >>

ParentContributesRight ==
  parentRightOwned \/ parentStillScansRight \/ ~routeToChild

ChildContributesRight ==
  routeToChild /\ childServing /\ childHasRight

RunQuery ==
  /\ ~queryRan
  /\ queryRan' = TRUE
  /\ resultCount' =
      [d \in Docs |->
        IF d = LeftDoc THEN 1
        ELSE (IF ParentContributesRight THEN 1 ELSE 0)
           + (IF ChildContributesRight THEN 1 ELSE 0)]
  /\ UNCHANGED << parentRightOwned, childHasRight, childServing, routeToChild,
                  parentStillScansRight >>

Next ==
  \/ CopyRightDocToChild
  \/ PublishChildServing
  \/ PublishRouteToChild
  \/ FinishParentTrim
  \/ RunQuery

TypeOK ==
  /\ BuggyRouteBeforeChildReady \in BOOLEAN
  /\ BuggyDoubleServe \in BOOLEAN
  /\ BuggyDropMovedDoc \in BOOLEAN
  /\ parentRightOwned \in BOOLEAN
  /\ childHasRight \in BOOLEAN
  /\ childServing \in BOOLEAN
  /\ routeToChild \in BOOLEAN
  /\ parentStillScansRight \in BOOLEAN
  /\ queryRan \in BOOLEAN
  /\ resultCount \in [Docs -> 0..2]

NoMissingDocs ==
  queryRan => \A d \in Docs : resultCount[d] >= 1

NoDuplicateDocs ==
  queryRan => \A d \in Docs : resultCount[d] <= 1

RouteRequiresChildServing ==
  routeToChild => childServing

Safety ==
  /\ TypeOK
  /\ NoMissingDocs
  /\ NoDuplicateDocs
  /\ RouteRequiresChildServing

Spec == Init /\ [][Next]_vars

=============================================================================
