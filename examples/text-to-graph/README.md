# Text to graph

Paste or type sentences, one per line, and watch a knowledge graph assemble
beside them. Each line is a document; Antfly Inference reads it with
GLiNER2.5 (`fastino/gliner2.5-base-v1`) and returns entities and relations
for the entity and relation types you list. Nothing is stored; the text and
schema live in the URL, so a copied link reproduces the graph.

## Run it

You need an Antfly built from this repository (`make build` at the root);
the Homebrew 0.2.5 release loads the GLiNER2.5 checkpoint but cannot run it
(`MissingWeight`). The workarounds in steps 1 and 2 were needed as of
October 2026.

```bash
# 1. Pull the reviewed GLiNER2.5 revision. An unpinned pull fails
#    (UnsupportedGlinerBoundaryConfiguration): the upstream repo has moved on
#    from the revision Antfly qualified.
./antfly inference pull "fastino/gliner2.5-base-v1:safetensors@72ac19b486cd4557424c8d61114e7530c243e9b0"

# 2. Start Antfly. On a 24 GB Mac the default budgets returned
#    MEMORY_BUDGET_EXCEEDED for GLiNER2.5; these values work.
./antfly standalone \
  --inference-host-budget-mb 6144 \
  --inference-backend-budget-mb 6144 \
  --inference-combined-budget-mb 10240 \
  --inference-scratch-budget-mb 4096

# 3. Start the app (proxies /ai to Antfly on 127.0.0.1:8080).
cd examples/text-to-graph
pnpm install
pnpm dev   # http://localhost:5317
```

`ANTFLY_URL` points the dev proxy elsewhere; `VITE_ANTFLY_MODEL` swaps the
extractor (for example `antflydb/gliner2-base-v1`, which Homebrew 0.2.5 runs,
with noticeably worse relations).

## How it works

- **One request per line.** `POST /ai/v1/extract` with the line as the only
  input. Results are cached by (schema, line text), so editing one line
  re-reads only that line. Requests run one at a time (about 30–40 ms per
  line on Metal) because the model admits one request at a time; a
  `MODEL_RESOURCE_BUSY` reply is retried.
- **Merging across lines** happens in the browser
  ([`src/graph.ts`](src/graph.ts)): mentions with the same normalized text
  become one node, and a capitalized single name ("Priya") folds into the
  one longer name of the same type that starts or ends with it
  ("Priya Raman"). Spans nested inside a longer span are dropped.
- **Relation selection.** GLiNER2.5 scores every relation type for every
  pair, so a pair often gets a correct type and a weaker wrong one
  ("acted_in" 0.86, "directed" 0.69). The graph keeps each pair's best type,
  plus near-ties within 0.03, above a 0.78 floor. These numbers were tuned
  on the bundled examples.
- **Hovering** a node highlights the lines it came from; hovering a line
  highlights the nodes and edges it produced. New nodes enter from the left
  edge at the height of the line that introduced them.

## Limits

- Each line is read on its own, so pronouns don't resolve: "He likes pears"
  won't link to Johnny.
- Relation types are plain names. The API also accepts typed relations
  (`{"type": "targets", "source": "drug", "target": "protein"}`), which
  would help domains like biomedicine where the base model confuses types.
