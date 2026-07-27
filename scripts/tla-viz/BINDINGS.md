# Trace Visualization Bindings

How to make `render_trace.py` display any Antfly NDJSON trace sensibly —
including trace tags that did not exist when the visualizer was written —
**without modifying the visualizer**.

## The two-command workflow

```bash
# 1. Reproduce the bug with tracing compiled in (a build flag, not a code change):
ANTFLY_TRACE_FILE=/tmp/repro.ndjson zig build -Dwith_tla=true <reproducer-test>

# 2. Render the artifact (from zig/):
make tla-viz-trace JSON=/tmp/repro.ndjson OUT=/tmp/repro.html [BINDING=/tmp/my-binding.json] [TLC=1]
```

The trace's `tag` field selects a binding from `specs/tla/traces/viz.json`.
Known tags render fully-featured out of the box. A new tag renders with a
generic fallback — or richly, if you pass a **binding overlay**:

```bash
python3 scripts/tla-viz/render_trace.py /tmp/repro.ndjson \
    --binding /tmp/my-binding.json --tlc -o /tmp/repro.html
```

`--binding` files use the same schema as `viz.json` and merge over it per tag
(top-level keys replace). `--tlc` additionally replays the trace through the
model with TLC and bakes the verdict — *which invariant failed at which step*
— into the artifact; if TLC/java is unavailable the render still succeeds.

## Trace format expected

One JSON object per line: `{"tag": "<family>-trace", "event": {"name": ...,
<fields>}}`. Non-JSON line prefixes (logger timestamps) are stripped. Nested
objects are flattened with dotted keys; an `after.` envelope is unwrapped
(`after.status` → `status`). Fields referenced below always use the flattened
names.

## Binding schema

```jsonc
{
  "my-new-trace": {                       // the tag, minus nothing: exact match
    "label": "my subsystem",              // sidebar/legend vocabulary
    "archetype": "narrative",             // consensus | dialogue | narrative

    // ---- Lanes (who acts) ----
    "laneField": "nid",                   // field holding the actor id, OR:
    "laneRules": [["^Primary", "primary"],// ordered [regex-on-event-name, lane]
                  [".*", "cluster"]],
    "lane": "session",                    // single static lane (narrative)
    "laneOrder": ["primary", "cluster"],  // display order
    "lanePrefix": "node ",                // header prefix for laneField values

    // ---- Tenure bands (what phase) ----
    "bandField": "status",                // small-string-domain field
    "bandGlobal": true,                   // global protocol state: full-width strip
    "bandQuiet": ["idle"],                // values rendered untinted
    "model": "storage/db/AntflyTransactionSession",  // link band colors (and the
    "phaseVar": "status",                 //   model-diagram link) to the spec's
                                          //   phase domain via phasecolors.py
    "bandColors": {"X": ["#lite", "#dark"]},  // explicit override

    // ---- Causal arrows ----
    "msgArrows": true,                    // consensus: raft msg envelope matching
    "pairs": [{"from": "PrimaryAppend", "to": "StandbyReceive",
               "keys": ["timeline", "lsn"]},          // same field both sides
              {"from": "ParentRightWrite", "to": "ReplayDelta",
               "fromKeys": ["dbDeltaSeq"], "toKeys": ["dbReplaySeq"]}],

    // ---- Legend categories (≤3; Other and Faults are automatic) ----
    "categories": [{"label": "Writes", "patterns": ["Write", "Stage"]}],
    "faultPatterns": ["Failure"],         // extra fault regexes (crash/corrupt/
                                          // panic/orphan/stale are built in)
    "milestones": ["^Become"],            // dense-timeline direct labels
    "segmented": true,                    // raft-style multi-run splitting

    // ---- Replay scene (the "watch the write" view) ----
    "scene": {
      "stores": [
        {"id": "intents", "label": "intent store", "countField": "intentCount",
         "style": "durable"},             // styles: buffer|durable|public|record
        {"id": "prep", "label": "prepared", "listField": "prepared"},   // chips
        {"id": "rec", "label": "txn record", "valueField": "status"}    // stamp
      ],
      "actors": [{"id": "local", "label": "local shard",
                  "memberOf": {"prepared": "prepared",   // dot per ledger:
                               "resolved": "resolved"}}],// lit when id ∈ field
      "lamps": [{"field": "importRecovered", "label": "import recovered"}],
      "flows": [                          // caption + box pulses on match
        {"on": "^StageWrite$", "from": "staged", "to": "intents",
         "label": "write intent: durable, not visible"},
        {"on": "^Recover", "from": "intents", "to": "visible",
         "when": {"status": "committed"}},               // post-state condition
        {"on": "^Resolve", "from": "rec", "toField": "participant"}]  // dynamic
    },

    // ---- Display invariants (legibility aids; TLC is the authority) ----
    "invariants": [
      {"id": "IdentityLockstep", "expr": "identityRows == visible"},
      {"id": "RepairFirst", "expr": "!restoreIntentCleared || runtimeRepairComplete"}
    ],
    // expr grammar: fields, ints, 'strings', true/false,
    //               == != < <= > >= + - ! && || ( )
    // a missing field makes that step's verdict "n/a", never an error

    // ---- TLC verdict overlay ----
    "traceFamily": "txn-session"          // scripts/tla-check.sh trace family
  }
}
```

## Choosing an archetype

- **consensus** — many symmetric actors with an id field and a message
  envelope (raft). Lanes per node, role bands, send→receive arrows.
- **dialogue** — a small named cast encoded in event names (primary/standby,
  parent/child). Lanes from `laneRules`, arrows from `pairs`.
- **narrative** — one progressing actor. Gets the **Replay** view: an event
  rail plus a scene of stores/actors/lamps stepped frame by frame, with
  per-step state diffs and invariant badges. This is the default for unknown
  tags, so even an overlay with only `label` + `invariants` is useful.

## Reading the artifact

Views: **Replay** (scene + state card, ← → keys, play), **Timeline**
(swimlanes, tenure bands, arrows), **Table** (exact values). With `--tlc`, a
banner reports the verdict and the offending step is flagged in the rail —
`TraceMatched` failures mark the implementation step the model refused;
invariant failures mark the step whose post-state broke the invariant. The
`model:` link opens the spec's generated state diagrams
(`specs/tla/diagrams/…`), whose phase colors match the bands and pills here.
