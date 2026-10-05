# Apple native providers for Antfly on macOS

This investigation recommends an explicit `apple` provider for OCR, transcription,
and generation, backed by a small native bridge. These are three independent
capabilities: Vision performs OCR, Speech performs transcription, and Foundation
Models provides Apple Intelligence generation. Availability of one must not imply
availability of the others. Start with OCR, validate generation and transcription
on macOS 26, and treat macOS 27 additions as a separate capability tier.

Status: OCR, text generation, and file transcription are implemented in the Zig
runtime behind the single `-Dapple-providers=true` flag. Vision uses the existing
Objective-C bridge; Foundation Models and SpeechAnalyzer share an in-process
Swift bridge in `zig/lib/apple_native`. The public provider name is `apple`, with
`vision-text`, `system`, and `speech-transcriber` aliases. SDKs and the generator
selector have been regenerated/updated.

See [the native provider guide](../guides/apple-native-ocr.mdx) for current
configuration, bounds, and verification. On macOS 27.0.1 / SDK 27, real OCR and
file transcription work, including phrase/word timestamps. Foundation Models
initially reported `modelNotReady`, then became available without an Antfly
settings change. Real text generation, history recall, and byte bounds passed.

The combined opt-in build targets macOS 26+, uses the SDK 27 Swift toolchain, and
strongly links its frameworks. Older macOS combined-binary deployment, Lite host
qualification, packaging/signing, structured generation, tool calling, live
transcription, image prompting, and PCC remain future work. There is no separate
`apple-intelligence` build flag. Disabled/Linux builds do not link Swift.

The remainder records the original investigation and proposed broader roadmap,
including contracts beyond the current synchronous bounded implementation.
Investigated October 5, 2026 against `origin/main` at
`a77d2a7aed129f95ed2ec2d3a2280e4f79d8ab8b`, in
`.worktrees/apple-intelligence-provider`, branch `research/apple-intelligence-provider`.

## Apple API choices

| Task | Recommended initial API | Requirements and limits |
| --- | --- | --- |
| OCR | `VNRecognizeTextRequest` with accurate recognition | Older macOS baseline; tested here on 15.6.1. Independent of Apple Intelligence settings. Returns text, confidence, and normalized rectangles. |
| Structured document OCR | `RecognizeDocumentsRequest` | Newer Vision API for document structure, including tables and lists. Gate separately; verify deployment availability with the selected SDK. |
| Transcription | `SpeechAnalyzer` with `SpeechTranscriber` | macOS 26+. Check device support and locale support, then installed assets. Does not use a Foundation Models session. |
| Text generation | `LanguageModelSession` with `SystemLanguageModel.default` | macOS 26+, an eligible Mac with Apple Intelligence enabled and its model ready. Check availability on each invocation. |
| Image understanding | Foundation Models image attachments | macOS 27 tier; current Apple documentation describes image prompting. Gate separately from the macOS 26 text generation baseline. |
| Cloud generation | `PrivateCloudComputeLanguageModel` | Separate opt-in future work; entitlement and distribution eligibility apply. Do not assume the CLI or Homebrew distribution qualifies. |

Vision's [text recognition request](https://developer.apple.com/documentation/vision/vnrecognizetextrequest)
supports a conventional OCR adapter, while its
[document request](https://developer.apple.com/documentation/vision/recognizedocumentsrequest)
can expose document structure. OCR should preserve the recognized words; model
summarization or repair belongs in a subsequent generator enrichment.

Apple documents macOS 26 availability for
[SpeechTranscriber](https://developer.apple.com/documentation/speech/speechtranscriber)
and [SpeechAnalyzer](https://developer.apple.com/documentation/speech/speechanalyzer).
[AssetInventory](https://developer.apple.com/documentation/speech/assetinventory)
manages system speech assets. Installed and downloadable locales are different
states. Local inference can still require a setup download; expose that state
without silently downloading in a document enrichment request.

[SystemLanguageModel](https://developer.apple.com/documentation/foundationmodels/systemlanguagemodel)
exposes availability reasons and is updated with the OS. Apple documents a
4,096-token on-device session budget in its
[context guidance](https://developer.apple.com/documentation/foundationmodels/managing-the-context-window).
Use runtime context size and token counting where supported by the SDK, rather
than treating that number as permanent. Prompts, history, schemas, tools, and
outputs consume the same budget.

The [2026 updates](https://developer.apple.com/documentation/updates/foundationmodels)
include macOS 27 APIs and changed model behavior, so a 2025-only assessment would
miss important options. Current
[image prompting guidance](https://developer.apple.com/documentation/foundationmodels/analyzing-images-with-multimodal-prompting)
describes attachments and Vision tools. Verify those APIs against the shipping
SDK before enabling a macOS 27 adapter.

[PCC access requirements](https://developer.apple.com/private-cloud-compute/)
include developer eligibility, an entitlement, and App Store distribution, with
specified testing distribution options. This makes PCC a separate product and
packaging decision for Antfly. An on-device `apple` provider should never switch
to PCC implicitly.

## Fit with the current repository

The server and inference runtime are Zig. The Go tree primarily supplies SDKs,
Lite bindings, the operator, and supporting libraries. A Go-only provider would
not wire up the current server runtime.

| Code | Required change |
| --- | --- |
| `zig/lib/generating/src/mod.zig` | Add `apple` to `Provider`, parsing/conversion, default URL and validation rules. Permit an empty URL and define tool support conservatively. |
| `zig/pkg/antfly/src/generating/mod.zig` | Add native dispatch to `BackendFactory` and `BackendState`; avoid HTTP authentication and HTTP quota handling for native calls. Carry the existing execution context. |
| `zig/pkg/antfly/src/common/provider_registry.zig` | Exercise registration of named Apple generators and explicit chains. The generic config registry already provides the basic structure. |
| `zig/lib/readers/src/config.zig` | Add Apple reader configuration and validation, including language and recognition options; update cloning and cleanup in readers. |
| `zig/lib/readers/src/mod.zig` | Add native reader dispatch, map results to `Result`, and extend the encoded-image entry point currently restricted to `.antfly`. |
| `zig/lib/transcribing/src/mod.zig` | Add native transcriber dispatch and configuration lifecycle; return the existing `STTResponse` contract. |
| `zig/pkg/antfly/src/asset_producer_runtime.zig` | Update local-provider classification, capability selection, borrowed media dispatch, admission, batch reports, and generation locality checks. Several branches explicitly assume only `.antfly` is local. |
| `zig/pkg/antfly/build/runtime.zig`, `zig/build.zig` | Introduce an independent Apple-provider build option and wire it through every relevant executable/shared-library unit. |
| `specs/openapi/shared/generating.yaml` | Add `apple` to `GeneratorProvider` and add `AppleGeneratorConfig` to the `oneOf`, rather than modifying only the enum. |
| `specs/openapi/antfly/audio.yaml`, `specs/openapi/antfly/indexes.yaml` | Add Apple STT selection and options, including the inline `TranscriberEnrichmentConfig` fields. |
| Runtime config schemas, inference schemas, SDK generation and UI provider selectors | Audit and update exposed reader/transcriber config paths, capability discovery, generated clients, and provider forms. Run `make generate` after schema changes. |

`Reader.VTable`, `Transcriber.VTable`, and `Generator.VTable` already provide
interfaces suitable for native implementations. The reader contract has text,
`fields_json`, `regions_json`, and page/source identity. The transcriber reuses
the audio library's request/response types. Generation has synchronous complete
responses at this layer; streaming needs separate wiring and cancellation tests.

The macOS PDF renderer in `zig/lib/pdf/src/darwin_render.zig` already uses
CoreGraphics. Reuse the existing bounded rendered-page pipeline. Do not introduce
a second whole-document PDF rasterizer or route PDFs through generation just to
obtain OCR. The first implementation adds Apple-specific borrowed raster
dispatch and capability checks alongside the embedded Antfly provider. The
scanned PDF qualification exercises this path with real Vision recognition.

## Native bridge design

Proposed shared module: `zig/lib/apple_native/`, exposing an OS-independent Zig
interface and a macOS implementation. Non-macOS and disabled builds return a
specific unavailable error without importing or linking Apple frameworks.

Vision can be bridged through Objective-C, following the existing native bridge
pattern in `zig/pkg/inference/src/backends/metal_kernels.m`. Foundation Models
and SpeechAnalyzer expose Swift APIs, so use a Swift bridge for those capabilities.
The bridge should be shared by server and Lite consumers rather than duplicated
in each language SDK.

Recommended sequence: validate a small Swift command-line helper on macOS 26,
then implement a versioned C ABI for production. A helper is useful for proving
headless API access and installation behavior, but making it a permanent
subprocess adds process supervision and media-copy costs. An in-process bridge
fits the existing vtables and borrowed media contracts. Keep a supervised helper
as an alternative if CLI service access or Swift runtime isolation requires it.

Proposed ABI responsibilities:

- Opaque runtime and request handles, ABI version, per-operation capability query,
  submit, cancel, terminal completion, release, and shutdown/drain.
- Explicit byte lengths, bounded output sizes, copied control metadata, and
  encoded or raster media descriptors. No Swift objects cross the boundary.
- A documented allocator/free pair for bridge-owned output. Zig copies output
  into the caller's allocator before releasing it.
- Swift tasks perform async work. Bridge completion must hand results to the
  Zig worker context rather than running orchestration or allocating from a
  request arena on an arbitrary Swift callback thread.
- Request cancellation and deadlines must reach the underlying task/analyzer.
  Hold input buffers alive until the bridge has finished reading them; timeout
  does not authorize freeing memory still used by a callback.
- Shutdown stops admission, cancels pending tasks, drains callbacks, and then
  releases runtime handles. Include Lite handle destruction in that contract.

Avoid waiting on the Swift main actor or blocking Antfly's event loop. A simple
worker-thread wait may fit the synchronous vtables, provided it cooperates with
the runtime's cancellation and budget model. Validate the actual executor and
callback lifetime behavior before choosing that implementation.

Apple publishes a [Foundation Models Python SDK](https://github.com/apple/python-apple-fm-sdk)
with Swift-backed native bindings. Review its bridge and ownership conventions
as a reference. It is useful for generation experiments, but embedding a Python
runtime is unnecessary for the Antfly server and does not supply OCR or Speech
integration.

## Provider behavior

Use one public provider name, `apple`, with task-specific model aliases. These
aliases select an API/use case, not downloadable Antfly model weights. Proposed
aliases are `vision-text`, `speech-transcriber`, and `system`. These aliases are now supported in the config schema. Keep `provider: antfly` behavior unchanged and do not
change existing defaults merely because a Mac is eligible.

Current configurations (advanced options below remain roadmap items):

```yaml
# Reader config
provider: apple
model: vision-text
recognition_languages: [en-US]
recognition_level: accurate
uses_language_correction: false
```

```yaml
# Transcriber enrichment config
provider: apple
model: speech-transcriber
language_code: en-US
timestamps: true
diarization: false
```

```yaml
# Generator config
provider: apple
model: system
temperature: 0.2
max_tokens: 256
```

Reject API keys, remote endpoints, and unsupported sampling/model options on
native Apple configs. Apple's temperature and sampling modes need explicit
mapping; accepting all OpenAI options would imply unsupported parity. Native
concurrency admission should have its own bounded queue rather than borrowing
HTTP token-bucket defaults. Begin with one active generation and one active
speech request per bridge, then qualify higher limits on target hardware.

### OCR

Honor image orientation and bounded pixel dimensions. Convert Vision's
bottom-left normalized rectangles to Antfly reader pixel boxes:
`[x * W, (1 - y - h) * H, (x + w) * W, (1 - y) * H]`.
Check downstream grounding's origin convention and apply page rotation consistently.
Preserve page number, source fingerprint, and item identity across batches.
Use layout-aware reading order for columns; descending vertical position alone
is only adequate for the synthetic probe.

Plain Vision OCR has no general prompt-following semantics. Accept the canonical
OCR request path, but reject arbitrary captioning or document question prompts
and token controls rather than silently dropping them. Map document structure to
`fields_json` only when the selected API actually produces it. A loop over images
is serial execution, even if submitted through a batch endpoint; report it accurately.

### Transcription

Resolve the requested locale through supported locale APIs. Require an explicit
locale initially; do not promise automatic language detection. Check installed
assets and expose a setup operation for downloads and reservations. Decode or
convert audio to an analyzer-compatible format, consume final results, and finish
the analyzer so its result stream terminates. Ensure timestamp offsets remain
relative to the original recording, including conversion/chunk boundaries.

Support timestamps only as supplied by the API and validated against Antfly's
segment/word contract. Do not fabricate confidence or speaker IDs. Reject
diarization until it has a qualified implementation. Existing live transcription
session endpoints need their own adapter; file transcription can ship first.
Do not silently fall back to legacy `SFSpeechRecognizer`, whose behavior differs.

### Generation

Create a fresh session per independent enrichment/request. Replay supported chat
history using transcript APIs with preserved roles; never concatenate messages
and pretend system, user, and tool boundaries are equivalent. Reject unsupported
history structures in the first version. Keep the macOS 26 implementation text
only, and add macOS 27 attachments through separately advertised capabilities.

Small summaries, tags, rewrites, and entity extraction are promising uses.
Long-document RAG and unbounded agents need explicit context planning and quality
evaluation. Use existing chunking and small retrieval contexts; do not silently
truncate documents. Model revisions follow OS updates, so record OS/model API
metadata and qualify prompts per release instead of promising pinned weights.

Foundation Models has [guided structured generation](https://developer.apple.com/documentation/foundationmodels/generating-swift-data-structures-with-guided-generation)
and dynamic schemas. Antfly's current structured/tool paths still require an
adapter: translating JSON Schema into `DynamicGenerationSchema` needs a supported
subset and validation. Framework-managed `Tool.call` execution also differs from
Antfly returning tool calls for its orchestrator to execute. Ship with
`supportsTools == false` until both the call and result/history mapping work;
this means some extractor and agent flows will be unavailable initially.

### Availability and failure handling

Advertise OCR, speech, and generation separately, including OS minimum,
supported locales, asset state, and tool/image support. Distinguish build
disabled, unsupported OS/device, Apple Intelligence disabled, model not ready,
missing speech assets, unsupported locale, context overflow, refusal, cancelled,
deadline exceeded, and transient resource exhaustion. These are proposed error
categories, not existing Antfly errors.

Do not fail the entire server because an optional Apple generator is unavailable.
Configured calls should fail with an actionable reason, and status checks should
not install assets. Classify errors before extending retries: the generation
chain can retry failures and fall through to another provider. Configuration and
context errors cannot improve with backoff. Review cancellation propagation and
refusal policy explicitly before allowing chain fallback to a remote provider.

Distributed enrichments run where their worker is scheduled, not necessarily on
the Mac that submitted a request. Start with local standalone and Lite usage.
For clusters, capability placement must ensure every eligible execution node
can run the configured provider, or use an explicitly configured Mac inference
service. Sharing that service over HTTP is a separate deployment option; it
does not make Apple native APIs available on Linux workers.

## Build and release plan

Propose `-Dapple-providers=true`, independent of `-Dmetal`: this provider invokes
OS services and is not another backend for Antfly's downloaded tensor models.
Keep it opt-in until native signing, runtime loading, and CI coverage are settled.
macOS build jobs need a suitable SDK/Swift toolchain. SDK presence, OS version,
and actual model availability are distinct checks.

Preserve the existing macOS deployment floor with availability guards and weak
linking or a conditionally loaded bridge. Do not let a strongly linked newer
framework stop the binary launching on older macOS. A separately loaded bridge
may need distinct binaries for older OCR and newer Foundation Models/Speech.
Validate the chosen arrangement with `otool` and a real older-OS launch; Swift
runtime library dependencies also need an explicit packaging check.

Native archives include the CLI and `libantfly`, and installers repack those
archives (`docs/cli-packaging.md`). Update archive creation, dependency bundling,
signing/notarization where used, and Lite linkage together. Verify Homebrew and
Python/npm installation layouts, not only `zig build`. Maintain Linux/Windows
builds without `swiftc` or Apple frameworks. An arm64 generation target is the
first release candidate; do not infer Intel support from OCR availability.

Validate CLI, embedded Lite host processes, and a headless launch agent separately.
Speech file analysis, microphone capture, model setup, and sandboxed app hosts
can have different privacy/service access requirements. This investigation has
not established those permissions for macOS 26/27. Do not add microphone
permissions to the file-transcription path without evidence.

## Implementation and validation sequence

The OCR bridge and image/PDF integration below have now been delivered through
`zig/lib/readers/src/apple.zig` and `apple_vision.m`. They are gated by an opt-in
macOS build flag and use an OS-independent unavailable error on disabled builds.
The shared Swift bridge, text generation, file speech, and schema additions have
also been implemented. The original sequence below remains a qualification
roadmap, especially for real generation and advanced features.

1. On a macOS 26 development host, prove Foundation Models generation/availability,
   SpeechTranscriber file analysis, setup downloads, cancellation, and CLI/headless
   service access. Repeat relevant API probes with the macOS 27 SDK. This is the
   prerequisite for committing to a production bridge.
2. Add bridge ownership/cancellation primitives and Apple config/schema support.
   Test disabled and non-macOS builds with stubs and regenerate clients.
3. Deliver OCR end to end: image and PDF enrichments, borrowed encoded/raster
   input, source identity, rotated pages, bounds, and accurate execution reports.
4. Add text generation with fresh sessions, explicit context failure, bounded
   output, cancellation, and named registry/chain integration. Evaluate a real
   summary/tag workload and test disabled Intelligence/model-not-ready states.
5. Add file transcription with locale/assets checks, final text and timestamp
   mapping. Compare an existing audio fixture against the established STT
   response contract, including long input, cancellation, and missing assets.
6. Add guided schemas/tools, live transcription, and macOS 27 image attachments
   only after their contracts are qualified. Consider PCC separately.

Cross-platform CI should cover config round trips, factory dispatch, capabilities,
malformed media, output budgets, cancellation lifetime, and shutdown with in-flight
callbacks using a bridge stub. Real macOS tests cover framework behavior and
runtime loading. A mocked model response cannot establish availability, accuracy,
or performance. Benchmark latency, memory, OCR errors and speech word error rate
against current local providers before changing any default.

## Local probe and observed results

`scripts/apple-provider-probe.swift` creates a single-column image in memory,
runs accurate Vision OCR, and emits JSON with confidence and converted boxes.
It also accepts a local image path (upright orientation is assumed). It performs
no transcription or generation and requests no model downloads.

```sh
xcrun swiftc -module-cache-path /tmp/antfly-apple-module-cache \
  scripts/apple-provider-probe.swift -o /tmp/antfly-apple-provider-probe
/tmp/antfly-apple-provider-probe
```

Observed on an arm64 Mac with macOS 15.6.1 and Swift 6.1.2:

- Compilation succeeded; the linker reported an unrelated inherited ONNX library
  search-path warning.
- The command sandbox run failed with `nilError`. Running the same binary with
  access to macOS services outside that sandbox passed (exit 0).
- Vision revision 3 recognized `Antfly local OCR`, `Invoice 12345`, and
  `Total USD 42.00` exactly, with confidence 1 for each line, and returned boxes.
- The installed SDK cannot import Foundation Models. Generation and new Speech
  APIs were not compiled or exercised. No end-to-end Antfly provider was tested.

This establishes basic Vision OCR from a CLI on this machine. It does not measure
accuracy on scanned documents, prove other sandbox/daemon environments, or verify
the future Swift bridge. The main remaining uncertainty is newer framework
behavior in Antfly's CLI and Lite host environments, not the existence of provider
extension points.
