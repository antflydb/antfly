# Antfly Model Explorer

A static, interactive companion to Antfly Inference. The model walkthroughs explain selected Gemma4, GLiNER2, GLiNER2.5, Qwen3 Embedding and Qwen3-VL paths. A Training section covers Gemma4 DPO/GRPO and the GLiNER2 / GLiNER2.5 finetuning stacks. They are curated teaching diagrams, not traces of a running model or a complete compatibility catalog.

## Run and share

Use the Node and pnpm versions pinned in `ts/package.json`, and install workspace dependencies from `ts/` with `pnpm install --frozen-lockfile`. From this directory:

```sh
pnpm dev
pnpm generate
pnpm gen:check
pnpm test
pnpm typecheck
pnpm lint
pnpm build
pnpm preview
```

`build` verifies the generated snapshot and runs the data/preview regression tests before exporting the Next app into `out/`. `preview` (also `start`) serves that export at `http://127.0.0.1:3000`; set `PORT` to change the port. The preview server needs Node and no third-party runtime dependencies. It is a local inspection tool; publish the contents of `out/` with a static host for teammates. The host must serve extensionless paths such as `/models/gliner2` from `/models/gliner2.html`, serve the `_next/` assets and route `.txt` payloads, and return `404.html` with HTTP 404 for missing pages. Do not configure a catch-all rewrite to `index.html`.

The app has no live inference connection and requires no model weights, API keys or inference server. GitHub source links require access to the linked repository and revision. Publication is separate from building; these commands do not commit, push or deploy.

## Evidence and maintenance

- `generator/` extracts op enum members, Metal kernel declarations and the Metal production schedule table from the local Zig checkout. Kernel families are inferred labels. It also scans `TERMITE_`/`ANTFLY_` names, but only to verify that a flag a page cites really exists; that scan is no longer published as a browsable index.
- `data/curated/*.model.json` supplies representative graphs and model-specific explanations. A variant can override `stats`, `repeatOverrides`, and `stageOverrides` (stage fields, with a merged `repeat` object). Shapes, fusion routes and configuration examples still need review against the applicable model artifact and runtime path.
- `data/curated/links.json` supplies named source anchors. Generation verifies files, line ranges and anchors; unique moved anchors resolve to their current line, while missing or ambiguous anchors fail.
- `data/curated/frames/` contains representative planned scenarios. They are validated before export. Historical measurements and schematic scopes must be labelled accordingly; only captured frames can claim capture evidence, and those require a source and machine.
- `content/` and the systems pages contain authored explanations and historical performance discussion. Generation cannot establish numerical correctness, current benchmark parity, or production qualification for those claims. Review the linked implementation and dated evidence when updating them.

Generated files in `data/generated/` are checked in so readers do not need to run Zig. After an inference-source update or curated-data edit, run `pnpm generate`, review the JSON diff, then run the checks above. The manifest records the exact source revision and generation time. `gen:check` reuses that revision and requires every source file read by the generator to match it, so an unrelated app commit does not invalidate the snapshot. Changed or uncommitted inference source cannot silently produce links to different bytes in GitHub. Keep source changes and snapshot generation coordinated; no automatic anchor update should substitute for rereading the surrounding implementation.

Generation defaults to the local `HEAD`. Before sharing from an unpublished app branch, pin an already published source revision with `pnpm generate --source-ref origin/main` (or an explicit published SHA). The same source-byte verification applies, so a different implementation cannot be substituted just to obtain a public URL. Confirm the selected revision is accessible on GitHub; local Git history alone does not prove publication.

Review scope, 9 September 2026: generated diagrams, inventories and source snippets are pinned to `26e332ed8c335d75eecd875548c6c36bcb47d183`, matching this checkout's inference implementation. The later published commit `aa44bddd1dd8befb5d0aec8bdb6c304054b89149` adds [qualified cross-request scheduling and tensor-forward fusion](https://github.com/antflydb/antfly/blob/aa44bddd1dd8befb5d0aec8bdb6c304054b89149/zig/pkg/inference/BATCHING.md#L95) and [shared Metal provider leases through execution and teardown](https://github.com/antflydb/antfly/blob/aa44bddd1dd8befb5d0aec8bdb6c304054b89149/zig/pkg/inference/src/ops/metal_compute.zig#L4319). These newer serving contracts are covered by a dated runtime note, not the generated snapshot. The reviewed delta preserves the selected Gemma/Qwen/GLiNER architecture math and KV/kernel explanations; its tokenizer change adds allocation-failure cleanup. This review does not qualify the new batching implementation or its performance.

Update, 17 September 2026: the GLiNER2.5 walkthrough and the Training section (`/training` plus three per-model pages) postdate the 9 September review. The snapshot is pinned to published `origin/main` at `20fb9f572acc083e92fbad2bc8a0b6b2e9bf3956` and currently indexes 72 op kinds, 445 Metal kernels, 25 routes and 209 snippets, exporting 25 HTML files (23 routes plus the 404 pages) across 6 model walkthroughs, 4 training pages and the systems pages. The `/dev/kitchen-sink` component fixture is reachable under `pnpm dev` but stripped from the export. The 9 September review covers neither the GLiNER2.5 content nor the Training section.

The regression tests cover invalid graph references, Sankey cycles, frame/barrier integrity, source bounds, enum anchor accuracy, incomplete schedule extraction, and static route/path handling. They do not execute inference or qualify model performance. Before sharing, also inspect navigation, deep links, keyboard interaction, dark mode and narrow layouts in the exported site.
