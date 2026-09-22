# Portland Civic Lab + Antfly demos

A permit explorer built with Antfly, React, and Leaflet. Search real Portland
building projects, filter the records, explore a map, and open the original city
sources. Portland Civic Lab housing trends provide separate, citywide context.
The **Oregon Decision Explorer** at `/governance` adds a source-grounded HB 2017
legislative case study. Both apps share this server and use separate Antfly tables.

## Run the demo

Requires Node 24+, pnpm 11, and an Antfly server built from this checkout. This
example links the repository TypeScript SDK because the published 0.0.14 SDK
predates the current API routes. `pnpm install` also builds that SDK.

Start an isolated Antfly instance from the repository root (build with `make build`
if needed):

```sh
./antfly standalone --port 8088 --health-port 4208 \
  --data-dir ./examples/portland-civic/.antfly
```

In another terminal:

```sh
cd examples/portland-civic
pnpm install
cp .env.example .env
pnpm ingest
pnpm governance:ingest
pnpm dev
```

Open **http://127.0.0.1:3007**. The checked-in `data/sample.json` contains 300
actual records fetched on September 18, 2026, plus dated housing observations.
It is a bounded sample, not synthetic data or a representative citywide sample.
The source had 43,297 matching building-permit records at fetch time. Its complete
selection rule and provenance are embedded in the snapshot and displayed in the UI.

Keyword search, filters, map, permit details, and the source-linked evidence brief
work without an LLM or API key. Try **garage**, **apartments**, or **tenant
improvement**. The map displays the current page of up to 40 results; records
without valid coordinates remain in the list. Search and filter state is in the
URL, so links can be shared and browser back/forward works.

For a production build served locally:

```sh
pnpm build
pnpm start
```

The server binds to loopback. This is a local demo, not an authenticated public
deployment. Credentials and model configuration stay on the server; the browser
can call only the example's bounded application endpoints.

## Oregon Decision Explorer

Open **http://127.0.0.1:3007/governance** after `pnpm governance:ingest`.
The checked-in pilot contains eight preserved primary PDFs, 20 procedural events
attributed to the [Oregon Governance Atlas HB 2017 case](https://oregon.portlandciviclab.org/decisions/dc-2017-hb2017-transportation),
2,173 passages, four prepared findings and four research gaps. No network download
is needed to replay these sources. It is one bounded case, not an import of the
Atlas's complete corpus or evidence-needs queue.

The interface includes:

- A procedural timeline with attributed votes and source inspection.
- Antfly keyword and Qwen3 hybrid retrieval, filtered to a historical source.
- A source inspector with full page text, highlighted passage, original URL,
  preserved PDF page, source SHA-256, acquisition time and explicit review state.
  Passage links use `/governance?passage=…` and can be shared.
- Literal page comparisons for introduced, A-engrossed, enrolled and chapter
  versions. Pages are selected explicitly; this is not a whole-act semantic diff.
- Real Antfly graph traversal over citation, contrary-evidence, institutional
  involvement and version relationships. The UI shows up to 30 nodes in two hops.
- Generated evidence briefs through Antfly's retrieval agent, using local Gemma 4
  or a configured OpenAI generator. The agent searches passages across versions;
  maintained findings are evaluation fixtures, never injected as answers.
  Every generated claim links to retrieved passage IDs. Malformed or invented
  citations suppress the answer. Citation validity does not prove entailment;
  generated claims remain unreviewed.
- A research queue and acquisition ledger that keep extracted text separate from
  human review. None of the documents are represented as human-reviewed.

The search form defaults to **Ask a question**: typing a question and pressing
Enter builds its evidence brief, just like clicking a prepared question. Choose
**Find passages** for raw, source-filtered retrieval. Question answering uses
hybrid retrieval once Qwen enrichment is complete, and keyword retrieval while
it is pending. Pipeline status exposes each model and its actual coverage.

The maintained effective-date finding exposes both the October 6
date on chapter 750's PDF page 98 and the conflicting August 6 date on the annual
summary's PDF page 435. It distinguishes the general effective date from specific
operative dates and from present-day law. Generated question answering is
experimental: the local Gemma regression still sometimes misses this discrepancy
or confuses a relative date formula with a conflicting date. Citation checks and
an answerability audit reject invalid outputs, but do not establish research
quality. Use the maintained finding and source inspector to verify the record.

The governance pipeline is Python-free. Install native models and start a
dedicated inference service (these memory budgets target this 36 GiB Mac Studio):

```sh
antfly inference pull Qwen/Qwen3-Embedding-0.6B-GGUF:gguf:Q8_0 --tasks embed
antfly inference pull antflydb/Florence-2-base --tasks read
antfly inference pull antflydb/mxbai-rerank-base-v1 --tasks rerank
antfly inference pull ggml-org/gemma-4-E4B-it-GGUF:gguf:Q4_K_M --tasks generate
antfly inference pull antflydb/gliner2-base-v1 --tasks extract
antfly inference run --port 8091 --process-memory-budget-mb 24576 \
  --host-budget-mb 12288 --backend-budget-mb 16384 \
  --combined-budget-mb 20480 --max-concurrent-requests 16 --max-loaded-models 2
```

Extraction currently uses `antflydb/gliner2-base-v1` through Antfly's native
entity/relation API. No Python service or conversion is needed. To upgrade later,
set `GOVERNANCE_EXTRACTION_MODEL=fastino/gliner2.5-base-v1` and
`GOVERNANCE_EXTRACTION_SCHEMA_VERSION=2`, install that model and rerun extraction.
The inspected `origin/main` revision `8b87fb96d` still has an empty GLiNER2.5
runtime qualification table, so it rejects serving that model. The optional
`pnpm governance:gliner-model` command installs its pinned native bundle for older
downloaders that omit `encoder_config/config.json`; it does not bypass the gate.

`GOVERNANCE_INFERENCE_URL` defaults to `http://127.0.0.1:8091`. Governance uses a
separate 1024-dimensional `qwen3_evidence_v1` index; it never mixes CLIP vectors.
The model's native query embedding profile supplies its Qwen query instruction.
Before generation, the native Mixedbread reranker scores up to 32 candidates per
query, including source-diverse and exact-phrase retrieval. Only the selected
passages and the closing passages of their source documents enter the
bounded answer context. These boundaries retain short legal clauses that chunk
ranking can overlook. Both retrieval and generation run through Antfly's retrieval
agent; no question-to-answer lookup supplies generated claims.
Run `pnpm governance:ingest` and wait for complete enrichment. The database
should have at least a 4096 MiB process budget for this corpus.

Gemma 4 is the default generator. To use Luna, set this in `.env` and restart
the app:

```dotenv
GOVERNANCE_GENERATOR={"provider":"openai","model":"gpt-5.6-luna","max_tokens":1500}
```

Set `OPENAI_API_KEY` in the Antfly **database server's environment**, because
that process runs the retrieval agent's generator. Keys stay off the browser.
This selection sends retrieved passages to OpenAI. No hosted fallback occurs
automatically. The hosted path requires credentials and has not been validated
on this machine.

To refresh originals and rebuild extraction:

```sh
# Requires curl and network access; downloads are cached in data/governance/cache.
pnpm governance:snapshot
pnpm governance:curate
pnpm governance:ingest
pnpm governance:extract
# Restart pnpm dev if it was already running.
```

Remove the relevant ignored cache file explicitly before requesting a fresh
upstream download. PDFs are preserved under their complete SHA-256 filename.
Native PDF text is quality checked. Missing or corrupted text triggers Florence
OCR on a rendered page. If OCR is insufficient, a caption is retained separately
and the page is marked unreadable; captions never become verbatim citations.
Model outputs are cached by model, request and source content for resumable work.
Extraction produces page text and overlapping passages of at most 900 characters;
the ID contains a source-hash prefix, PDF page and character offset. Ingestion
verifies full PDF checksums, citation slices, unique IDs and graph targets before
writing. Curation fails if its expected source phrases no longer resolve.
Review refreshed findings before treating a new corpus as reliable. Timeline
structure changes also stop extraction for review.

`oregon_governance_evidence` stores 2,218 source, passage, event, institution,
finding and gap nodes. `decision_graph` indexes explicit relation fields;
`qwen3_evidence_v1` indexes passage search text. `oregon_governance_meta` publishes the current
snapshot only after every node is visible. Content-derived generation prefixes
prevent queries and graph edges from mixing snapshots; old generations remain.
The server rejects a mismatch between its local snapshot and the ingested one.

Coverage limits are intentional and visible: the annual summary is preserved in
full, but only its seven pages mentioning HB 2017 / House Bill 2017 are indexed.
Atlas procedural data is a secondary extraction, not independently verified OLIS
roll calls; its original HTML hash is retained, while only the structured event
data is included in the distributable snapshot. All eight primary PDFs are
preserved in full. Six OLIS PDF checksums match those published by the Atlas.
The native GLiNER2 stage uses schema version 1 and converts its UTF-8 byte offsets
to UTF-16 citation offsets. Schema version 2 requests UTF-16 offsets directly.
Both validate mention slices against the preserved passages and persist extraction artifacts
in `oregon_governance_mentions_v1`. Antfly Autograph resolves session-scoped
canonical entities in `oregon_governance_entities_v1`. Relationships retain the
source version, page, offsets, model, schema hash and review status. Relations
scoring at least 0.6 are stored as source-linked assertion records with subject
and object mentions. Autograph connects these to canonical entities; source-local
IDs never become global graph targets. Mention labels and relation directions
are machine suggestions, not verified institutional facts. The
`/api/governance/autograph?passage=…` endpoint exposes bounded native traversal;
`/api/governance/pipeline` distinguishes extraction from graph readiness.

The pilot does not acquire audio, litigation, audits, later rules or project
delivery records. It does not infer motive, influence, actual spending, statutory
compliance or current law. The research queue ranks candidates only within the
acquired pilot corpus.

If viewing from another machine, forward port 3007 through your existing SSH
connection. The app remains bound to the Mac's loopback interface:

```sh
ssh -N -L 3007:127.0.0.1:3007 USER@TAILSCALE_IP
# If using the tailscale wrapper, put the destination before SSH flags:
tailscale ssh USER@stinkbug.tailfe40d7.ts.net -N -L 3007:127.0.0.1:3007
```

Then open `http://localhost:3007/governance` on the viewing machine.

## Refresh or expand the data

```sh
# Download up to 2,000 records, with explicitly labeled sample coverage.
pnpm snapshot --since 2023-01-01 --limit 2000 --out data/snapshot.json
pnpm ingest --file data/snapshot.json

# Retrieve every matching building-permit ID (larger download and index).
pnpm snapshot --since 2023-01-01 --limit 0 --out data/full.json
pnpm ingest --file data/full.json
```

The exporter queries the city's ArcGIS `All Permits` layer for **Residential 1 &
2 Family**, **Commercial Building**, and **Facility** permits created since the
requested date. It captures IDs first, fetches bounded batches in WGS84, rejects
incomplete pages, validates dates and coordinates, and writes the snapshot
atomically. A limited export selects the highest source OBJECTIDs; that is not a
random sample or a guarantee of the newest creation dates. Separate trade permits,
complaints, tree permits, and enforcement cases are excluded.

Civic Lab housing responses are cached in `data/housing-cache.json` for at least
one hour, honoring its polling request. A failed housing refresh retains cached
context with its original fetch date; a first-time failure leaves it unavailable.
Both endpoints must be reachable for a complete fresh export. Snapshot replay
needs only Antfly; map tiles and optional web fonts need an internet connection.

## Enable semantic search and generated explanations

Set `ANTFLY_EMBEDDER` in `.env` to a valid Antfly embedder configuration, then run
`pnpm ingest` again. For example, with Ollama already running and the model pulled:

```dotenv
ANTFLY_EMBEDDER={"provider":"ollama","model":"embeddinggemma","url":"http://localhost:11434"}
```

Or use a model installed in Antfly's built-in inference runtime:

```dotenv
ANTFLY_EMBEDDER={"provider":"antfly","model":"antflydb/clipclap","api_url":"http://127.0.0.1:8088"}
```

Ingestion creates `permit_embeddings` if it does not exist. Antfly generates
embeddings from `search_text`. Wait for index enrichment to finish before trying
the **Hybrid** toggle. It combines BM25 and semantic retrieval. Semantic results
are labeled ranked candidates, and do not claim an exhaustive matching count or
citywide statistics. Existing embedding indexes are reused; changing a model
requires explicitly recreating that index with a compatible configuration.

For generated, cited summaries, also set a generator and restart the app:

```dotenv
ANTFLY_GENERATOR={"provider":"ollama","model":"gemma3:4b","url":"http://localhost:11434"}
```

`Explain these results` uses Antfly's retrieval agent in pipeline mode with the
same snapshot, search, and filter predicates. The answer is labeled as generated,
and citations open the corresponding original records. Without a generator, the
button produces a labeled evidence brief using direct record excerpts and query
counts; it does not pretend to be an AI answer. Provider errors are shown instead
of silently substituting an answer.

## Data model and limitations

- `portland_permits`: one document per source `FOLDERKEY`, with original
  description, address, permit type, neighborhood, status, dates, reported units,
  valuation, coordinates, and source URL. `search_text` combines the fields useful
  for discovery. Exact filters are structured term/date clauses, not interpolated
  query strings.
- `portland_civic_metrics`: individual dated housing observations, plus a `current`
  manifest containing snapshot coverage, filter choices, and chart context.
- Keys have a content-derived snapshot prefix. Reimporting the same snapshot
  upserts the same keys. Ingestion publishes the `current` manifest only after
  loading records and observations; queries always filter to that snapshot.
  Old generations are retained. A local demo can be reset by deleting its two
  dedicated tables before reimporting; no automatic deletion touches other data.
- Permit counts are not dwelling-unit counts. Reported unit counts may overlap
  across applications, and issued permits do not establish completed construction.
- Neighborhood strings are preserved as the city reports them, including
  multi-neighborhood values. Some records have no valid address or coordinates.
- Housing charts preserve upstream dates, data-status flags, and missing values.
  Their geography and coverage may differ from the permit sample; filters do not
  affect them. The ZORI series is a rent index, not a computed median of permits.
- Completed/issued-permit processing times exclude applications still waiting.
  Partial current months should not be compared directly with complete months.

## Verification

```sh
pnpm test
pnpm build
pnpm exec playwright install chromium
# With Antfly running and both ingest commands complete:
pnpm test:e2e
pnpm governance:eval
```

Unit tests cover normalization, missing data, date validation, literal filters,
snapshot scoping, and the source-linked sample. Browser tests exercise actual
Antfly queries on desktop and mobile: pagination, filters, record details,
citations, empty results, and an unavailable-backend state. Screenshots go to
`test-results/portland-desktop.png` and `test-results/portland-mobile.png`.
Legislative tests also check source integrity, graph traversal, source filters,
historical comparisons, contrary evidence, unknown-question abstention and
shareable citations on desktop/mobile. `governance:eval` checks generated date
conflicts, a paraphrase, spending abstention, exact source slices, hybrid filters
and graph traversal. It requires complete Qwen enrichment. Quotes must match a
retrieved source passage; invented citations or quotes are rejected. A separate
model audit checks whether an answer addresses the question and whether an
asserted conflict is a real incompatibility, beyond citation validity.
Run `pnpm exec tsx scripts/governance-reader-probe.ts` for a native Florence OCR
and caption-isolation test using synthetic images excluded from the source corpus. These
fixtures are regression checks, not independent expert evaluation
of civic research quality. Governance screenshots use `governance-*.png`.
`pnpm exec tsx scripts/governance-graph-eval.ts` checks native relation records,
exact mention spans, entity promotion and canonical graph traversal after extraction.

## Sources and attribution

- [City of Portland / PortlandMaps permit layer](https://www.portlandmaps.com/arcgis/rest/services/Public/BDS_Permit/FeatureServer/22)
- [Portland Civic Lab open data](https://www.portlandciviclab.org/open-data) —
  curated datasets under CC BY; original sources include Portland Permitting &
  Development and Zillow Research. The snapshot contains data, not their essays.
- [Civic Lab source methodology and caveats](https://www.portlandciviclab.org/methodology)
- [OpenStreetMap contributors](https://www.openstreetmap.org/copyright) — map tiles.
- [Oregon Governance Atlas / Portland Civic Lab](https://oregon.portlandciviclab.org/decisions/dc-2017-hb2017-transportation)
  — attributed structured procedural data and the starting source inventory.
  This pilot does not reproduce the Atlas's authored case narrative or imply its
  endorsement of our findings. The local research gaps and prepared findings are
  demo annotations, not entries imported from the Atlas research ledger.
- [Oregon Legislature / OLIS](https://olis.oregonlegislature.gov/liz/2017R1/Measures/Overview/HB2017)
  — original bill versions and analyses; the source ledger lists each exact URL.

This example is independent and is not affiliated with the City of Portland or
Portland Civic Lab. No Civic Lab application code is incorporated.
