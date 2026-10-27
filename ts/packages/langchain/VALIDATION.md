# LangChain.js first-release validation

Executed locally on 2026-10-27, macOS ARM64, Node 24.19.0, pnpm 11.10.0,
TypeScript 5.9.3, LangChain core 1.3.0, classic 1.0.56, and OpenAI integration
1.7.1. Implementation lives on `feat/langchain-js`, based on Antfly
`a507489e93`. This report describes executed checks, not registry publication.

## Results

| Check | Result |
| --- | --- |
| SDK and integration ESM/CJS builds and declarations | Passed |
| SDK and integration TypeScript checks | Passed |
| Integration formatting/lint | Passed |
| Existing SDK plus foundation regression tests | 518 passed, 1 existing conditional skip |
| Integration deterministic contracts and answer-chain tests | 36 passed |
| Complete integration profile with real server/models | 41 passed, zero skips |
| Independent npm-tarball consumer | ESM, CommonJS, both TypeScript module modes, live retrieval/deletion passed |
| Three-document quickstart | Returned `owner`: Mira Chen owns operations and handles shipment escalation |
| Persistent knowledge example after real server restart | Verified persisted records and retrieval without re-ingestion |
| Hybrid retrieval plus real local reranking after restart | Owner evidence ranked first; source IDs and per-stage scores retained |
| Workflow YAML parsing and `git diff --check` | Passed |

Ordinary integration tests intentionally skip the five live cases. They were
enabled and passed in the separate complete 41-test run. SDK test counts are
separate; the SDK's existing skip is not represented as a live pass.

## What the checks exercise

- Real SDK transport in contract tests: authentication propagation, typed missing
  resources, factory provisioning/reuse, incompatible-index refusal, and no
  resurrection of resources deleted after initial setup.
- Stable IDs and explicit ID precedence, upserts/deletes, missing-record reads,
  dense distance preservation, fixed/caller filter intersection, and MMR using
  stored candidate vectors.
- Missing/partial/repair-required write outcomes, explicit deferred visibility,
  partial-batch committed IDs, cancellation, and no automatic mutation replay.
- Embedding batching and separate query/document task types, unchanged upstream
  `FakeEmbeddings`, native managed-index retrieval, and hybrid RRF retrieval.
- Standard retriever callbacks and concurrent per-call isolation, standard
  contextual compression, reranking index validation/tie ordering, and runnable
  composition.
- Persistent cited Q&A composition using a deterministic upstream chat model:
  valid citation IDs, rejected invented/missing citations, and explicit abstention.
  Real paid-provider generation was not run.

## Real-server baseline

The live tests use the existing server binary retained by the Python suite,
with a separate data directory and dedicated ports. Its SHA-256 is:

```text
b2734f6e7c0477666940b4d38844f94506f4e59fc327293f86f5247db12a2158
```

The Python suite records that binary against SDK/server foundation
`f9ce40c3e47a4dba48af5532a21b19f4cd428bff`. No claim is made that the current
server checkout was rebuilt. Local models were the installed
`BAAI/bge-small-en-v1.5` (catalog identity `BAAI/bge-small-en-v1.5:onnx`) and
`antflydb/mxbai-rerank-base-v1`. The process had an explicit 4 GiB memory budget.

The Q&A restart check stopped the task-owned server and launched a new process
on the same data directory. The query-only `verify` command then checked the
three persisted IDs and expected owner result. It did not re-ingest records.

## Evidence and reproduction

Small command logs are retained in [validation/](validation/). The tarball
verifier writes its dependency version, hashes, consumer location, and outcomes
to `.state/langchain/consumer-*/receipt.json`. Consumer directories contain the
actual installed packages, ESM/CJS import probes, TypeScript contracts and a
real-server lifecycle probe. They are ignored local artifacts, not source files.

From `ts/`:

```sh
pnpm --filter @antfly/langchain... build
pnpm --filter @antfly/langchain... typecheck
pnpm --filter @antfly/langchain... test
pnpm --filter @antfly/langchain lint
ANTFLY_LIVE=1 ANTFLY_URL=http://localhost:8080 pnpm --filter @antfly/langchain test
ANTFLY_URL=http://localhost:8080 ANTFLY_EMBEDDING_MODEL=your-model pnpm --filter @antfly/langchain test:package
```

Set `ANTFLY_RERANKER_MODEL` when using another installed reranker. The live suite
defaults to the two named models above. An isolated configured server is required.

## Limits and release steps

- A diagnostic corpus containing an additional billing document ranked billing
  above the owner for this small embedding model. The parity quickstart retains
  the Python suite's unchanged three-document corpus. No broad ranking-quality
  claim follows from that fixture; the diagnostic was not an adapter transport bug.
- No browser/edge/Bun/embedded or local Linux execution is claimed. Linux and
  latest-compatible CI are configured but have not run on the hosted service here.
- npm publication and upstream integration listing have not been performed.
  The registry's observed SDK version was 0.0.14; this integration requires the
  new SDK foundations declared as 0.2.0. Release those first, then validate a
  registry-only installation before releasing the integration.
- Published-package ownership/Trusted Publishing and the initial npm bootstrap
  are external release setup. No existing release tag was changed.
- JSON/document/byte stores, LangGraph memory, graph adapters, and multimodal
  workflows remain the later milestones in the agreed scope.
