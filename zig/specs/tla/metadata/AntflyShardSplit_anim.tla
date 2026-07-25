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

---------------------- MODULE AntflyShardSplit_anim ----------------------
(*
  Spectacle animation view for AntflyShardSplit.tla.

  Auto-loaded by Spectacle (https://github.com/will62794/spectacle) when the
  base spec is opened; the `_anim.tla` suffix next to the spec is the tool's
  animation convention. Not used by TLC model checking.

  Suggested constants for interactive exploration (mirrors ShardSplitMC):
    Keys = {"k1","k2","k3"}   ParentKeys = {"k1"}   ChildKeys = {"k2","k3"}
    BuggyChildDefaultOnReplayCaughtUp = FALSE
  (set the Buggy constant to TRUE to walk the expected-failure mutant where
  the tablemgr promotes the child on SplitReplayCaughtUp instead of
  SplitCutoverReady)

  Layout:
    - top strip: split phase, routing flag, finalize fence flag
    - left card: parent shard (leader dot, owned-range keys, pending deltas)
    - right card: child shard (state, snapshot/init/leader/cutover badges,
      replayed keys)
    - bottom strip: authoritative data placement per key (parent/child/both)
*)

EXTENDS AntflyShardSplit, Sequences, FiniteSets, TLC

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
TitleStyle == ("font-size" :> "13px" @@ "font-family" :> "sans-serif" @@ "font-weight" :> "bold")
LabelStyle == ("font-size" :> "12px" @@ "font-family" :> "sans-serif")
SmallStyle == ("font-size" :> "10px" @@ "font-family" :> "sans-serif" @@ "fill" :> "#52514e")
ChipText == ("font-size" :> "11px" @@ "font-family" :> "sans-serif" @@ "fill" :> "#ffffff")

Injective(f) == \A x, y \in DOMAIN f : f[x] = f[y] => x = y
SetToSeq(S) == CHOOSE f \in [1..Cardinality(S) -> S] : Injective(f)

KeySeq == SetToSeq(Keys)
KeyIdx == DOMAIN KeySeq

\* --- Shared pieces ---

Badge(x, y, label, on) ==
    SGroup(<<
        SCircle(x, y, 5, [fill |-> IF on THEN "#0ca30c" ELSE "#f0efec",
                          stroke |-> "#898781"]),
        SText(x + 9, y + 4, label, SmallStyle)
    >>, NoAttrs)

LeaderDot(x, y, hasLeader) ==
    SCircle(x, y, 7, [fill |-> IF hasLeader THEN "#eda100" ELSE "#f0efec",
                      stroke |-> "#898781"])

KeyChip(x, y, label, fill, textFill) ==
    SGroup(<<
        SRect(x, y, 34, 18, [fill |-> fill, stroke |-> "#898781", rx |-> 4]),
        SText(x + 6, y + 13, label,
              ("font-size" :> "11px" @@ "font-family" :> "sans-serif" @@ "fill" :> textFill))
    >>, NoAttrs)

PhaseFill ==
    IF splitPhase = "none" THEN "#f0efec"
    ELSE IF splitPhase = "prepare" THEN "#fab219"
    ELSE "#eb6834"  \* splitting

\* --- Top strip: split phase + metadata flags ---

topStrip ==
    SGroup(<<
        SRect(20, 14, 110, 22, [fill |-> PhaseFill, stroke |-> "#898781", rx |-> 11]),
        SText(30, 29, "phase: " \o splitPhase, LabelStyle),
        Badge(170, 25, "routing updated", routingUpdated),
        Badge(300, 25, "finalize fence", splitFenceSet),
        Badge(430, 25, "archive", archiveCreated)
    >>, NoAttrs)

\* --- Parent shard card ---

\* Owned range: a chip per key, filled when the parent byteRange contains it.
parentRangeChips ==
    [i \in KeyIdx |->
        KeyChip(30 + (i - 1) * 40, 105, ToString(KeySeq[i]),
                IF KeySeq[i] \in parentRange THEN "#2a78d6" ELSE "#f0efec",
                IF KeySeq[i] \in parentRange THEN "#ffffff" ELSE "#898781")]

\* Delta queue: pending (in parentDeltaKeys, not yet replayed) vs replayed.
DeltaFill(k) ==
    IF k \in parentDeltaKeys /\ k \notin childReplayedKeys THEN "#eb6834"
    ELSE IF k \in childReplayedKeys THEN "#1baf7a"
    ELSE "#f0efec"

deltaChips ==
    [i \in KeyIdx |->
        IF KeySeq[i] \in ChildKeys
        THEN KeyChip(30 + (i - 1) * 40, 150, ToString(KeySeq[i]),
                     DeltaFill(KeySeq[i]),
                     IF DeltaFill(KeySeq[i]) = "#f0efec" THEN "#898781" ELSE "#ffffff")
        ELSE SGroup(<<>>, NoAttrs)]

parentCard ==
    SGroup(<<
        SRect(20, 50, 200, 140, [fill |-> "#fcfcfb", stroke |-> "#0b0b0b", rx |-> 8]),
        SText(34, 72, "parent shard", TitleStyle),
        LeaderDot(200, 66, parentHasLeader),
        SText(30, 95, "owned range", SmallStyle),
        SGroup(parentRangeChips, NoAttrs),
        SText(30, 140, "split deltas (pending / replayed)", SmallStyle),
        SGroup(deltaChips, NoAttrs)
    >>, NoAttrs)

\* --- Child shard card ---

replayChips ==
    [i \in KeyIdx |->
        IF KeySeq[i] \in ChildKeys
        THEN KeyChip(290 + (i - 1) * 40, 150, ToString(KeySeq[i]),
                     IF KeySeq[i] \in childReplayedKeys THEN "#1baf7a" ELSE "#f0efec",
                     IF KeySeq[i] \in childReplayedKeys THEN "#ffffff" ELSE "#898781")
        ELSE SGroup(<<>>, NoAttrs)]

childCard ==
    SGroup(<<
        SRect(280, 50, 200, 140, [fill |-> IF newShardState = "none" THEN "#f0efec" ELSE "#fcfcfb",
                                  stroke |-> IF newShardState = "none" THEN "#c3c2b7" ELSE "#0b0b0b",
                                  rx |-> 8]),
        SText(294, 72, "child shard: " \o newShardState, TitleStyle),
        LeaderDot(460, 66, newShardHasLeader),
        Badge(300, 95, "snapshot", newShardHasSnapshot),
        Badge(380, 95, "initializing", newShardInitializing),
        Badge(300, 115, "cutover ready", splitCutoverReady),
        SText(290, 140, "replayed deltas", SmallStyle),
        SGroup(replayChips, NoAttrs)
    >>, NoAttrs)

\* --- Bottom strip: where each key's data lives ---

DataFill(k) ==
    IF dataStore[k] = "parent" THEN "#2a78d6"
    ELSE IF dataStore[k] = "child" THEN "#eb6834"
    ELSE "#4a3aa7"  \* both

dataChips ==
    [i \in KeyIdx |->
        SGroup(<<
            KeyChip(30 + (i - 1) * 110, 225, ToString(KeySeq[i]),
                    DataFill(KeySeq[i]), "#ffffff"),
            SText(70 + (i - 1) * 110, 238, dataStore[KeySeq[i]], SmallStyle)
        >>, NoAttrs)]

dataStrip ==
    SGroup(<<
        SText(30, 215, "data placement (NoDataLoss: every key on some shard)", SmallStyle),
        SGroup(dataChips, NoAttrs)
    >>, NoAttrs)

AnimView ==
    SGroup(<<
        topStrip,
        parentCard,
        childCard,
        dataStrip
    >>, ("transform" :> "scale(1.5) translate(10 10)"))

=============================================================================
