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

--------------------- MODULE AntflyLsmLifecycle_anim ---------------------
(*
  Spectacle animation view for AntflyLsmLifecycle.tla.

  This module is auto-loaded by Spectacle (https://github.com/will62794/spectacle)
  when the base spec is opened: the `_anim.tla` suffix next to the spec is the
  tool's animation convention. It is not used by TLC model checking.

  Suggested constants for interactive exploration:
    ReadCache = "rc"   WriteCache = "wc"   BuggyIndexFailLeaksTemp = FALSE
  (set BuggyIndexFailLeaksTemp = TRUE to walk the expected-failure mutant
  and watch the temps card turn red on "Leaked")

  Layout: one card per tracked resource (two cache entries, the mutable read
  snapshot, the removeSegments temporaries), colored by lifecycle location,
  with lease / reader / cleanup-capacity badges.
*)

EXTENDS AntflyLsmLifecycle, Sequences, FiniteSets, TLC

\* --- Minimal SVG helpers (same shape as the Spectacle example specs) ---

Merge(r1, r2) ==
    LET D1 == DOMAIN r1 D2 == DOMAIN r2 IN
    [k \in (D1 \cup D2) |-> IF k \in D1 THEN r1[k] ELSE r2[k]]

SVGElem(_name, _attrs, _children, _innerText) ==
    [name |-> _name, attrs |-> _attrs, children |-> _children, innerText |-> _innerText]

SText(x, y, text, attrs) ==
    SVGElem("text", Merge([x |-> x, y |-> y], attrs), <<>>, text)

SRect(x, y, w, h, attrs) ==
    SVGElem("rect", Merge([x |-> x, y |-> y, width |-> w, height |-> h], attrs), <<>>, "")

SCircle(cx, cy, r, attrs) ==
    SVGElem("circle", Merge([cx |-> cx, cy |-> cy, r |-> r], attrs), <<>>, "")

SGroup(children, attrs) == SVGElem("g", attrs, children, "")

NoAttrs == [k \in {} |-> ""]
LabelStyle == ("font-size" :> "13px" @@ "font-family" :> "sans-serif")
SmallStyle == ("font-size" :> "11px" @@ "font-family" :> "sans-serif" @@ "fill" :> "#52514e")

Injective(f) == \A x, y \in DOMAIN f : f[x] = f[y] => x = y
SetToSeq(S) == CHOOSE f \in [1..Cardinality(S) -> S] : Injective(f)

\* --- Lifecycle-location colors ---
\* empty/idle: neutral; owned-by-live-path: blue; owned-by-cleanup-path:
\* warning yellow; destroyed/freed: gray; leaked: critical red.

LocFill(loc) ==
    IF loc \in {"Absent", "NoSnapshot", "None"} THEN "#f0efec"
    ELSE IF loc \in {"Live", "MutableOwner", "NewOnly", "BothAllocated"} THEN "#9ec5f4"
    ELSE IF loc \in {"Retired", "RetiredOwner"} THEN "#fab219"
    ELSE IF loc \in {"Destroyed", "Freed"} THEN "#c3c2b7"
    ELSE IF loc = "Published" THEN "#86e8c4"
    ELSE "#d03b3b"  \* Leaked

\* A badge: filled dot when `on`, with a small label.
Badge(x, y, label, on) ==
    SGroup(<<
        SCircle(x, y, 5, [fill |-> IF on THEN "#0ca30c" ELSE "#f0efec",
                          stroke |-> "#898781"]),
        SText(x + 10, y + 4, label, SmallStyle)
    >>, NoAttrs)

\* A resource card: name, current location, and badges underneath.
Card(x, y, title, loc, badges) ==
    SGroup(<<
        SRect(x, y, 190, 58, [fill |-> LocFill(loc), stroke |-> "#898781",
                              rx |-> 6]),
        SText(x + 10, y + 20, title, LabelStyle),
        SText(x + 10, y + 38, loc, LabelStyle),
        SGroup(badges, NoAttrs)
    >>, NoAttrs)

\* --- Cache entry cards ---

CacheSeq == SetToSeq(Caches)

CacheCard(i) ==
    LET c == CacheSeq[i] IN
    Card(20, 20 + (i - 1) * 80, ToString(c), cacheLoc[c], <<
        Badge(40, 88 + (i - 1) * 80 - 20, "lease", cacheLeases[c] = 1),
        Badge(120, 88 + (i - 1) * 80 - 20, "cleanup cap", cacheRetiredCap[c] = 1)
    >>)

cacheCards == [i \in DOMAIN CacheSeq |-> CacheCard(i)]

\* --- Mutable read snapshot card ---

snapshotCard ==
    Card(260, 20, "mutable_read_snapshot", snapshotLoc, <<
        Badge(280, 88, "reader", activeReaders = 1),
        Badge(360, 88, "cleanup cap", snapshotRetiredCap = 1)
    >>)

\* --- removeSegments temporaries card ---

indexCard ==
    Card(260, 100, "removeSegments temps", indexTemp, <<
        Badge(280, 168, "op failed", indexOpFailed)
    >>)

AnimView ==
    SGroup(<<
        SGroup(cacheCards, NoAttrs),
        snapshotCard,
        indexCard
    >>, ("transform" :> "scale(1.4) translate(10 10)"))

=============================================================================
