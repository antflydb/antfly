# LangChain.js validation

Validation uses the Python LangChain suite as the behavioral reference. The
branch packages are `@antfly/langchain` and `@antfly/langgraph-store` 0.1.0.

On 2026-10-29, after rebasing onto main with SDK 0.2.2, the integration builds,
type checks, and lint passed. Workspace tests passed: 532 SDK tests with one
existing conditional skip, 62 LangChain tests, and 7 LangGraph-store tests with
zero integration skips. An isolated consumer installed published
`@antfly/sdk@0.2.2` alongside packed integration packages. ESM, CommonJS,
strict `.mts`/`.cts` types, live smoke, and all 69 integration contracts passed
using the pinned peers below. The same isolated checks passed with core 1.3.1,
classic 1.0.57, and openai 1.8.0. The LangChain registry-only release gate also
passed with published SDK 0.2.2. Raw evidence is under
`.state/langchain/validation-20261029-*` and the consumer receipt records its
exact artifact hashes.

The 2026-10-28 cross-platform baseline below used published SDK 0.2.1, not the
workspace SDK:

| Environment | Checks | Result |
| --- | --- | --- |
| macOS, Node 24.19.0, workspace source | Builds, type checks, live integration suites | 62 LangChain + 7 LangGraph tests passed; zero skips |
| macOS, Node 24.19.0, packed packages, pinned peers | ESM, CommonJS, strict `.mts`/`.cts` types, live smoke, all integration contracts | 62 + 7 passed; zero skips |
| macOS, Node 24.19.0, packed packages, newer peers | Same checks | 62 + 7 passed; zero skips |
| macOS, Node 25.9.0, packed packages, pinned peers | Live integration contracts | 62 + 7 passed; zero skips |
| Linux ARM64, Node 24 container, packed packages, pinned peers | ESM, CommonJS, strict types, live integration contracts | 62 + 7 passed; zero skips |
| Linux AMD64, Node 24.22.2 container, packed packages, pinned peers | ESM, CommonJS, strict types, live integration contracts | 62 + 7 passed; zero skips |
| TypeScript SDK, workspace | SDK tests | 529 passed; 1 existing conditional skip |

Pinned peers: `@langchain/core@1.3.0`, `@langchain/classic@1.0.56`,
`@langchain/openai@1.7.1`, `@langchain/langgraph@1.5.3`. The newer profile used
core 1.3.1, classic 1.0.57, openai 1.8.0, and LangGraph 1.5.3. All package
consumers installed compiled tarballs into isolated directories. Their copied
test suites imported the installed packages through a one-line export shim, so
the contract runs did not substitute workspace implementation source. Package
inspection allowed only `dist`, README, LICENSE, and package metadata.
Integration builds and type checks passed. Lint passed with warnings for dynamic
graph and test-backend types.

The real-server checks exercised retrieval, reranking, codecs, graph scopes,
ownership, paths, counts, memory isolation, native single-field semantic search,
exact multi-field semantic pooling, and multimodal byte integrity. A separate
process restarted Antfly between memory/graph writes and reads; native and
multi-field recall, user namespace isolation, and graph traversal all survived.
The multimodal example ingested one page and two figures, then verified all
three retrieved originals after restart without reading its source fixtures.
That test used fixed summaries and local text embeddings; it did not evaluate
vision or generation quality.

The LangChain registry-only release gate passed with published SDK 0.2.1.
LangGraph store's registry-only gate stopped at npm's missing
`@antfly/langchain@^0.1.0`, which is the expected publication dependency; its
staged-package consumer and live contracts passed. No package was published as
part of validation. Logs and consumer receipts are in
`.state/langchain/validation-20261028/` and `.state/langchain/consumer-*` inside
this worktree. They are excluded from package contents.

The live server was a prebuilt local Antfly qualification binary from the Python
suite, with BGE small English embeddings and MXBAI reranking. This does not
qualify a fresh build of current Antfly main or hosted CI. There was no native
vision model in the isolated model directory, and no paid generation call.

Review suggestions to change `[id]` citations, replace exact multi-field
pooling, or change `not_submitted` commitment semantics were declined to retain
Python compatibility. The exact abstention phrase remains in the generation
contract. Creation/update timestamps use the same atomic transform strategy as
Python.
