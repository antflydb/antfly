# Chat Playground: Remove Semantic Index Gate, Add Agentic Mode & Streaming Tool Calls

## Context

The chat playground in Antfarm currently requires a semantic (embedding) index to function. The backend retrieval agent already supports full-text search, agentic tool use, and mixed index types, but the frontend:
1. Filters indexes to only `aknn`/`embedding` types, hiding full-text indexes
2. Disables chat input when no embedding index exists
3. Hardcodes `semantic_search` as the only retrieval strategy
4. Never sets `max_iterations` (agentic mode) or configures tools
5. Ignores `reasoning_chain` and `onReasoning` data from the backend

The backend already handles `full_text_index` names in the `indexes` array by converting `semantic_search` text to a boosted BM25 query (`api_query.go:586-593`). In agentic mode (`max_iterations > 0`), the LLM autonomously picks tools based on available indexes.

Additionally, reasoning steps are only available in the final `done` event — not streamed incrementally. Aligned with Anthropic's API approach (where `tool_use` and `thinking` blocks stream as first-class content blocks), we'll add a `step` SSE event emitted as each step completes. We use `step` rather than `tool_call` because: (a) Antfly's agent executes tools server-side — `tool_call` implies client-side execution like Claude/Gemini, (b) it covers non-tool actions (classification, sufficiency checks), and (c) it aligns with the existing `RetrievalReasoningStep` naming.

Each `step` event carries a `RetrievalReasoningStep` enhanced with `id` (unique step ID for frontend correlation), `status` (success/error/skipped), and `duration_ms` (server-side execution timing — unique observability data only a server-side executor can provide).

## Backward Compatibility

The **answer agent** (`POST /agents/answer`) is the public-facing deprecated endpoint. It is a thin shim (`answer_agent_compat.go`) that remaps field names (`generation` → `answer`, `followup` → `follow_up_question`) and always runs pipeline mode (`MaxIterations=0`). It does NOT expose `state`, `reasoning_chain`, or `tool_calls_made`. **No changes to its contract.**

The **retrieval agent** (`POST /agents/retrieval`) is internal. Breaking changes (field renames, new enums) are acceptable here.

New SSE events (`step_started`, `step_completed`) are additive — the answer agent's `mapAnswerAgentEventType()` (line 147) passes unknown events through unchanged. The `done` event payload is mapped through `mapRetrievalToAnswerResult()` which only extracts known fields, so new fields (`status`, `usage`, `id`, `model`, `created_at`) on `RetrievalAgentResult` are silently dropped from the answer agent's `done` payload. No breakage.

## Changes

### 1a. Backend: Emit `step_started`/`step_completed` SSE events (lifecycle pattern)

Following OpenAI's Responses API pattern for server-side tools (e.g., `web_search_call.in_progress` → `.completed`), each tool execution emits paired lifecycle events with matching `id`:

- **`step_started`**: Emitted when a tool begins execution (for UI "searching..." indicators)
- **`step_completed`**: Emitted when a tool finishes, with `status`, `duration_ms`, and `details`

**`src/metadata/retrieval_agent.go`**

Each executor method already appends to `e.reasoningChain`. Wrap execution in lifecycle events. Each step must include `id` (generate via `fmt.Sprintf("step_%s", xid.New())`).

Add a **step lifecycle helper** on `retrievalToolExecutor` to avoid duplicating lifecycle boilerplate in all 7 executor methods:

```go
// emitStep wraps a tool execution with step_started/step_completed lifecycle events.
// Returns the completed step (for callers that need details like hit count).
func (e *retrievalToolExecutor) emitStep(ctx context.Context, stepName, action string, fn func() (map[string]any, error)) (RetrievalReasoningStep, error) {
    stepID := fmt.Sprintf("step_%s", xid.New())
    if e.streamCallback != nil {
        _ = e.streamCallback(ctx, "step_started", map[string]any{
            "id": stepID, "step": stepName, "action": action,
        })
    }
    start := time.Now()
    details, err := fn()
    status := "success"
    var errMsg string
    if err != nil {
        status = "error"
        errMsg = err.Error()
    }
    step := RetrievalReasoningStep{
        ID: stepID, Step: stepName, Action: action,
        Status: status, ErrorMessage: errMsg,
        DurationMs: int(time.Since(start).Milliseconds()),
        Details: details,
    }
    e.reasoningChain = append(e.reasoningChain, step)
    if e.streamCallback != nil {
        _ = e.streamCallback(ctx, "step_completed", step)
    }
    return step, err
}
```

**Usage in each executor** (replaces ~20 lines of boilerplate with ~5):
```go
func (e *retrievalToolExecutor) ExecuteSemanticSearch(ctx context.Context, query string, ...) error {
    _, err := e.emitStep(ctx, "semantic_search", query, func() (map[string]any, error) {
        // ... existing search logic ...
        return map[string]any{"hits": len(hits)}, err
    })
    return err
}
```

Note: `xid` is already an indirect dependency (`go.mod:420`). Using it directly will promote it to direct on next `go mod tidy`.

Apply at these locations:
- `ExecuteSemanticSearch` — after line 91 (step: `semantic_search`)
- `ExecuteFullTextSearch` — after line 144 (step: `full_text_search`)
- `ExecuteTreeSearch` — after line 190 (step: `tree_search`)
- `ExecuteGraphSearch` — after line 284 (step: `graph_search`)

Also add missing reasoning chain entries + SSE for:
- `ExecuteWebSearch` — after line 330, add `reasoningChain` append + stream (step: `websearch`)
- `ExecuteFetch` — after line 357, add `reasoningChain` append + stream (step: `fetch`)
- `AddFilter` — after line 367, add `reasoningChain` append + stream (step: `add_filter`)

---

### 1b. Backend: Expose token usage from GenKit responses

GenKit's `prompt.Execute()` returns `*ModelResponse` which has `Usage *GenerationUsage` with `InputTokens`, `OutputTokens`, `TotalTokens`, `CachedContentTokens`, `ThoughtsTokens`. Antfly currently calls only `resp.Text()` and discards usage info.

**`lib/ai/genkit.go`** — In `RAG()` (line 382) and `Generate()` calls: capture `resp.Usage` and return it alongside the text/generation output.

**`lib/ai/generation.go`** — Add `Usage *ai.GenerationUsage` to `GenerationResult` (returned by `Generate()`) and `GenerationOutput` (returned by `GenerateQueryResponse()`). Populate from the GenKit response.

**`src/metadata/api.yaml`** — Add `RetrievalAgentUsage` schema and `usage` field on `RetrievalAgentResult`:
```yaml
RetrievalAgentUsage:
  type: object
  properties:
    input_tokens:
      type: integer
      description: Total input tokens across all LLM calls
    output_tokens:
      type: integer
      description: Total output tokens across all LLM calls
    total_tokens:
      type: integer
      description: Sum of input + output tokens
    cached_input_tokens:
      type: integer
      description: Input tokens served from cache
    llm_calls:
      type: integer
      description: Number of LLM invocations made
    resources_retrieved:
      type: integer
      description: Total resources found across all search queries
    prune_stats:
      $ref: '#/components/schemas/PruneStats'
```

Also add a `PruneStats` schema (replaces internal `TokenPruneStats` from `lib/ai/tokenpruner.go:35-45`):
```yaml
PruneStats:
  type: object
  properties:
    resources_kept:
      type: integer
      description: Number of resources kept
    resources_pruned:
      type: integer
      description: Number of resources pruned
    tokens_kept:
      type: integer
      description: Estimated tokens in kept resources
    tokens_pruned:
      type: integer
      description: Estimated tokens in pruned resources
```

Also add new fields to the existing `RetrievalReasoningStep` schema in `api.yaml`:
```yaml
RetrievalReasoningStep:
  # Add to existing properties:
  id:
    type: string
    description: Unique step ID for correlation and tracing
    example: "step_01abc123"
  status:
    type: string
    enum: [success, error, skipped]
    description: Outcome of this step
  error_message:
    type: string
    description: Error details when status is "error"
  duration_ms:
    type: integer
    description: Server-side execution time in milliseconds
```

A single agent request makes multiple LLM calls (classification + N agentic iterations + generation). Usage must be **accumulated** across all calls — add a helper method on the executor or result struct to merge `GenerationUsage` from each GenKit response.

**`lib/ai/genkit.go`** — In `RAG()` (line 382): return `resp.Usage` alongside text. Change return to `(string, *ai.GenerationUsage, error)`.

**`lib/ai/generator.go`** — Update the `RetrievalAugmentedGenerator` interface (line 230-232) to match the new return signature: `RAG(...) (string, *ai.GenerationUsage, error)`.

**`lib/ai/generation.go`** — Update `GenerateQueryResponse()` which calls `RAG()` at lines 433 and 509. Add `Usage *ai.GenerationUsage` to `GenerationResult` and `GenerationOutput`. Propagate usage from inner `RAG()` call.

**`lib/ai/genkit_test.go`** — Update all 18 `RAG()` call sites to handle the new 3-value return.

**`src/metadata/retrieval_agent.go`** — Add `usage RetrievalAgentUsage` accumulator. At each LLM call site (classification at line 448, agentic RAG at line 1158, generation at lines 520/557), capture returned usage and accumulate into `result.Usage`. Set `resources_retrieved` from `len(result.Hits)` and `resources_used`/`resources_pruned` from `PruneStats`.

Run `make generate` after API spec changes to regenerate Go types and SDK types.

---

### 1c. Backend: Return pruning stats in result

`applyTokenPruning` (`api_ai.go:70-108`) returns `TokenPruneStats` but only logs them. These feed into `usage.prune_stats`.

**`lib/ai/tokenpruner.go` → `lib/ai/pruner.go`** (rename file) — Rename types:
- `TokenPruner` → `Pruner`
- `NewTokenPruner` → `NewPruner`
- `TokenPruneStats` → `PruneStats`
- Field renames:
```go
type PruneStats struct {
    ResourcesKept    int `json:"resources_kept"`
    ResourcesPruned  int `json:"resources_pruned"`
    TokensKept       int `json:"tokens_kept"`
    TokensPruned     int `json:"tokens_pruned"`
}
```
Update references in `api_ai.go:87` (`ai.NewTokenPruner` → `ai.NewPruner`).

**`src/metadata/api_ai.go`** — Change `applyTokenPruning` to return `([]schema.Document, *ai.PruneStats)` instead of just `[]schema.Document`.

**`src/metadata/retrieval_agent.go`** — At the `applyTokenPruning` call site (line 481), capture stats and set `result.Usage.PruneStats`.

---

### 1d. Backend: Replace `state` with `status` + `incomplete_details` (OpenAI-aligned)

Currently `RetrievalAgentState` has `complete`, `tool_calling`, `awaiting_clarification` and the plan proposed a separate `stop_reason`. These overlap. Consolidate into a single `status` field following OpenAI's Responses API pattern.

**`src/metadata/api.yaml`** — Replace `state` enum with `status` + `incomplete_details`:
```yaml
# Replace RetrievalAgentState enum with:
status:
  type: string
  enum:
    - completed       # Agent finished successfully
    - in_progress     # Agent is still executing (streaming context)
    - incomplete      # Agent stopped before completion
    - failed          # Error occurred
  description: Current status of the retrieval agent execution

incomplete_details:
  type: object
  description: Present when status is "incomplete" — explains why
  properties:
    reason:
      type: string
      enum:
        - max_iterations          # Hit max_iterations limit
        - max_tokens              # LLM output truncated
        - no_tools                # No tools available for agentic mode
        - clarification_needed    # Agent needs user input
```

Also add response metadata fields to `RetrievalAgentResult`:
```yaml
id:
  type: string
  description: Unique response ID for logging and tracing
  example: "ragr_01abc123"
model:
  type: string
  description: LLM model used for generation
  example: "gemini-2.0-flash"
created_at:
  type: integer
  format: int64
  description: Unix timestamp (seconds) when the response was created
```

**`src/metadata/retrieval_agent.go`** — Set `result.Status` at each termination point:
- `runAgenticWithStructuredOutput`:
  - Line 1166 (LLM error) → `failed`
  - Line 1173 (no actions parsed) → `completed`
  - Line 1235 (clarification) → `incomplete` with reason `clarification_needed`
  - Loop exhaustion (iteration >= MaxIterations) → `incomplete` with reason `max_iterations`
- `runAgenticWithTools`:
  - Line 1052 (no tools) → `incomplete` with reason `no_tools`
  - Line 1098 (GenKit error) → `failed`
  - Line 1115 (clarification) → `incomplete` with reason `clarification_needed`
  - Normal completion → `completed`
- Pipeline mode (non-agentic) → `completed`
- Generate `result.ID` as `fmt.Sprintf("ragr_%s", xid.New())`
- Set `result.Model` from the GenKit model name
- Set `result.CreatedAt` to `time.Now().Unix()` at the start of execution

**Also update these files for `State` → `Status` rename:**
- **`src/metadata/a2a_adapters.go:136`** — reads `string(result.State)`, must change to `result.Status`
- **`src/metadata/answer_agent_compat_test.go`** — sets `State:` in expected results at lines 241, 285, 340
- **`antfly-go/antfly/types.go:136`** — manual type alias `RetrievalAgentState`, must update to new type name after codegen
- **`e2e/retrieval_generation_test.go:488`** — asserts `resp.State`

---

### 2. SDK: Add `onStepStarted`/`onStepCompleted` callbacks and export type

**`ts/packages/sdk/src/types.ts`**:
- Export `RetrievalReasoningStep`:
  ```typescript
  export type RetrievalReasoningStep = components["schemas"]["RetrievalReasoningStep"];
  ```
- Add to `RetrievalAgentStreamCallbacks` (after `onSearchExecuted`, line 249):
  ```typescript
  onStepStarted?: (step: { id: string; step: string; action: string }) => void;
  onStepCompleted?: (step: RetrievalReasoningStep) => void;
  ```

**`ts/packages/sdk/src/client.ts`** — Add cases in SSE switch (after `search_executed` case, around line 293):
```typescript
case "step_started":
  if (callbacks.onStepStarted) {
    callbacks.onStepStarted(JSON.parse(data));
  }
  break;
case "step_completed":
  if (callbacks.onStepCompleted) {
    callbacks.onStepCompleted(JSON.parse(data));
  }
  break;
```

---

### 3. Components: Wire `onStepStarted`/`onStepCompleted` through streaming utilities

**`ts/packages/components/src/utils.ts`**:
- Add to `AnswerCallbacks` interface (line 161):
  ```typescript
  onStepStarted?: (step: { id: string; step: string; action: string }) => void;
  onStepCompleted?: (step: RetrievalReasoningStep) => void;
  ```
- Wire through to `sdkCallbacks` (line 212):
  ```typescript
  onStepStarted: callbacks.onStepStarted,
  onStepCompleted: callbacks.onStepCompleted,
  ```

---

### 4. Extend `ChatTurn` and `ChatConfig` in useChatStream

**`ts/packages/components/src/hooks/useChatStream.ts`**

- Add to `ChatTurn`: `reasoningText: string`, `reasoningChain: RetrievalReasoningStep[]`, `toolCallsMade: number`
- Add to `ChatConfig`: `tools?: ChatToolsConfig`
- Wire `onReasoning` callback to accumulate `reasoningText` (currently a no-op at line 157)
- Wire `onStepStarted` to show "in progress" indicators per step during streaming
- Wire `onStepCompleted` to accumulate `reasoningChain` incrementally during streaming
- Extract `reasoning_chain` and `tool_calls_made` from `onRetrievalAgentResult` (line 178) as fallback for non-streaming
- Pass `config.tools` into the `steps.tools` field of the request (line 134)

---

### 5. Plumb `tools` through ChatBar

**`ts/packages/components/src/ChatBar.tsx`** — Add `tools?: ChatToolsConfig` to props, destructure it, include in the `config` useMemo.

---

### 6. Broaden index fetching in TableProvider

**`ts/apps/antfarm/src/contexts/table-context.ts`** — Add `chatIndexes: string[]` to `TableContextType`.

**`ts/apps/antfarm/src/components/table-provider.tsx`** — In the `fetchIndexes` effect (line 106), alongside computing `embeddingIdxs`, also compute `chatIdxs` that includes both `aknn`/`embedding` AND `full_text` index types. Add `chatIndexes` state and pass it through the Provider value. Existing `embeddingIndexes` stays unchanged for other consumers.

---

### 7. ChatPlaygroundPage changes (3 parts)

**`ts/apps/antfarm/src/pages/ChatPlaygroundPage.tsx`**

**7a. Remove index gate:**
- Get `chatIndexes` from `useTable()`
- Change disabled condition from `!selectedIndex` to `chatIndexes.length === 0`
- Update warning text to "No searchable index found"
- Pass `chatIndexes` instead of `[selectedIndex]` as `semanticIndexes` prop

**7b. Add agentic mode settings:**
- New state: `agenticEnabled`, `maxIterations` (default 5), `enabledTools: ChatToolName[]`
- New settings section after "Pipeline Steps" with:
  - Toggle for agentic mode (with `Bot` icon)
  - Max iterations input (when enabled)
  - Tool checkboxes (when enabled, with descriptions)
- Pass `maxIterations` and `tools` props to `ChatBar`
- Reset agentic state in `handleReset`

**7c. Show reasoning chain per turn:**
- In `renderAssistantMessage`, use the existing third `turn: ChatTurn` parameter (already passed by `ChatMessages.tsx:176`)
- Render `ReasoningChainCollapsible` below each assistant message

---

### 8. Fix naming inconsistency: summary/generation/answer → `generation`

The same concept has three different names across layers:
- SSE event: `"summary"` (genkit.go:371)
- SDK callback: `onAnswer` (types.ts:243)
- API result field: `generation` (api.yaml)

Standardize on `generation` (already the API result field name):

**Backend (both files must change together):**
- **Rename** SSE event from `"summary"` to `"generation"` in `genkit.go:371`
- **Update** `generation.go:664` — the `markdownSectionParser` checks `if eventType != "summary"` and translates to `"generation"`. After renaming in genkit.go, this check must match the new name.
- **Update** `generation_test.go` — 5 references to `"summary"` event type at lines 728, 760, 787, 821, 2131

**SDK callback rename (`onAnswer` → `onGeneration`) — all affected files:**
- `ts/packages/sdk/src/types.ts:243` — interface definition
- `ts/packages/sdk/src/client.ts:300,301,412,414` — SSE handler + chatAgent wrapper
- `ts/packages/sdk/test/client.test.ts:528,596` — SDK tests
- `ts/packages/components/src/utils.ts:165,200,217` — interface + usage
- `ts/packages/components/src/hooks/useChatStream.ts:160` — chat hook
- `ts/packages/components/src/hooks/useAnswerStream.ts:104` — answer hook
- `ts/packages/components/src/hooks/useAnswerStream.test.tsx` — 5 test references
- `ts/packages/components/src/AnswerResults.tsx:230,232` — component (`onAnswerChunk` prop at line 63 is a derivative name, also rename to `onGenerationChunk`)
- `ts/packages/components/src/AnswerResults.test.tsx` — 9 test references
- `ts/apps/antfarm/src/pages/RagPlaygroundPage.tsx:507` — `onAnswerChunk` prop usage

---

### 9. New component: ReasoningChainCollapsible

**`ts/apps/antfarm/src/components/chat/ReasoningChainCollapsible.tsx`** (new file)

Follows the `SourcesCollapsible` pattern from ChatPlaygroundPage (lines 100-137). Accepts `chain: RetrievalReasoningStep[]`, `toolCallsMade: number`, `reasoningText?: string`, `isStreaming?: boolean`.

- **During streaming**: Shows animated "Reasoning..." panel with streaming `reasoningText`, "in progress" indicators from `onStepStarted`, and completed step details from `onStepCompleted`
- **After completion**: Collapsible list of reasoning steps, each showing tool name (badge), action description, and expandable details JSON. Uses connector lines and tool-specific icons (reusing `Search`, `Filter`, `Globe`, etc. from lucide-react).

---

## Files Modified

| File | Change |
|------|--------|
| **Backend** | |
| `src/metadata/api.yaml` | Replace `state` with `status`/`incomplete_details`, add `usage`/`id`/`model`/`created_at`, enhance `RetrievalReasoningStep` |
| `src/metadata/retrieval_agent.go` | Add `emitStep()` helper, emit lifecycle SSE events, set `status`, capture usage + pruning stats |
| `src/metadata/api_ai.go` | Return `PruneStats` from `applyTokenPruning` |
| `src/metadata/a2a_adapters.go` | Update `result.State` → `result.Status` at line 136 |
| `src/metadata/answer_agent_compat_test.go` | Update `State:` → `Status:` in expected results |
| `lib/ai/genkit.go` | Capture `resp.Usage`, rename internal SSE event `"summary"` → `"generation"` |
| `lib/ai/genkit_test.go` | Update 18 `RAG()` call sites for new 3-value return |
| `lib/ai/generator.go` | Update `RetrievalAugmentedGenerator` interface for new `RAG()` return signature |
| `lib/ai/generation.go` | Add `Usage` field to `GenerationResult`/`GenerationOutput`, update `markdownSectionParser` `"summary"` check |
| `lib/ai/generation_test.go` | Update 5 `"summary"` event type references |
| `lib/ai/tokenpruner.go` → `lib/ai/pruner.go` | Rename file + `TokenPruner` → `Pruner`, `TokenPruneStats` → `PruneStats` |
| `lib/ai/tokenpruner_test.go` → `lib/ai/pruner_test.go` | Rename file + update 14 references |
| `e2e/retrieval_generation_test.go` | Update `resp.State` → `resp.Status` at line 488 |
| **SDK** | |
| `ts/packages/sdk/src/types.ts` | Export `RetrievalReasoningStep`, add `onStepStarted`/`onStepCompleted`, rename `onAnswer` → `onGeneration` |
| `ts/packages/sdk/src/client.ts` | Handle `step_started`/`step_completed` SSE events, rename `onAnswer` → `onGeneration` |
| `ts/packages/sdk/test/client.test.ts` | Update 2 `onAnswer` references |
| `antfly-go/antfly/types.go` | Update manual type alias `RetrievalAgentState` after codegen |
| **Components** | |
| `ts/packages/components/src/utils.ts` | Wire `onStepStarted`/`onStepCompleted`, rename `onAnswer` → `onGeneration` |
| `ts/packages/components/src/hooks/useChatStream.ts` | Extend `ChatTurn`/`ChatConfig`, accumulate reasoning steps, rename `onAnswer` → `onGeneration` |
| `ts/packages/components/src/hooks/useAnswerStream.ts` | Rename `onAnswer` → `onGeneration` |
| `ts/packages/components/src/hooks/useAnswerStream.test.tsx` | Update 5 `onAnswer` references |
| `ts/packages/components/src/AnswerResults.tsx` | Rename `onAnswer`/`onAnswerChunk` → `onGeneration`/`onGenerationChunk` |
| `ts/packages/components/src/AnswerResults.test.tsx` | Update 9 `onAnswer` references |
| `ts/packages/components/src/ChatBar.tsx` | Add `tools` prop passthrough |
| **Frontend (Antfarm)** | |
| `ts/apps/antfarm/src/contexts/table-context.ts` | Add `chatIndexes` to context type |
| `ts/apps/antfarm/src/components/table-provider.tsx` | Compute + provide `chatIndexes` |
| `ts/apps/antfarm/src/pages/ChatPlaygroundPage.tsx` | Remove gate, add agentic UI, render reasoning |
| `ts/apps/antfarm/src/pages/RagPlaygroundPage.tsx` | Rename `onAnswerChunk` prop |
| `ts/apps/antfarm/src/components/chat/ReasoningChainCollapsible.tsx` | **New** — reasoning chain visualization |

**After API spec changes**: Run `make generate` to regenerate Go types (`api.gen.go`) and SDK types.

## Verification

1. **Go build**: `GOEXPERIMENT=simd go build ./...` — ensure backend compiles after codegen
2. **Frontend build**: `cd ts && npm run build` — ensure no type errors
3. **Manual test (no embedding index)**: Select a table with only a full-text index. Confirm chat input is enabled and messages send successfully.
4. **Manual test (streaming steps)**: With agentic mode enabled, send a query. Confirm `step_started` and `step_completed` SSE event pairs appear in the browser's Network tab as each tool executes (not just in the final `done`). Verify matching `id` fields.
5. **Manual test (usage + status)**: Send a chat message. Inspect the `done` SSE event data — confirm `usage` object has `input_tokens`/`output_tokens`, `status` is `completed`, `id` and `model` are present, and `prune_stats` is populated when `max_context_tokens` is configured.
6. **Manual test (agentic mode UI)**: Enable agentic mode, set max_iterations=3, check some tools. Send a message. Confirm the `ReasoningChainCollapsible` shows steps arriving incrementally during streaming.
7. **Manual test (max_iterations stop)**: Set max_iterations=1 with agentic mode. Confirm `status` is `incomplete` with `incomplete_details.reason` = `max_iterations` in the response.
8. **Regression**: Confirm existing behavior with embedding indexes still works (semantic search path unchanged).
