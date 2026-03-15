# AI Elements Adapter

Adapter layer for using [Vercel AI Elements](https://elements.ai-sdk.dev/) components with Antfly's `ChatBar`.

## Overview

This adapter bridges Antfly's chat system (`ChatBar`, `useChatStream`) with Vercel AI Elements' presentational components — **without any runtime dependency** on AI Elements or the Vercel AI SDK in `@antfly/components`.

It provides two layers:

1. **Data transforms** — Pure functions that map Antfly types (`QueryHit`, `ChatTurn`) to props shapes AI Elements components expect. Use these for full control over rendering.
2. **Render props factory** — `createAIElementsRenderers()` takes your AI Elements components as parameters and returns a complete set of render props you can spread directly onto `<ChatBar>`.

## Prerequisites

Install AI Elements in your app via the shadcn CLI:

```bash
npx ai-elements@latest add message sources suggestion prompt-input
```

This copies the component source files into your project (e.g., `src/components/ai/message.tsx`). You also need:

```bash
npm install motion streamdown
```

## Quick Start (Factory)

```tsx
import { Antfly, ChatBar, createAIElementsRenderers } from "@antfly/components";

// AI Elements — installed locally via shadcn CLI
import { Message, MessageContent, MessageResponse } from "@/components/ai/message";
import { Sources, SourcesTrigger, SourcesContent, Source } from "@/components/ai/sources";
import { Suggestions, Suggestion } from "@/components/ai/suggestion";
import {
  PromptInput,
  PromptInputTextarea,
  PromptInputSubmit,
  PromptInputFooter,
} from "@/components/ai/prompt-input";

const renderers = createAIElementsRenderers({
  Message,
  MessageContent,
  MessageResponse,
  Sources,
  SourcesTrigger,
  SourcesContent,
  Source,
  Suggestions,
  Suggestion,
  PromptInput,
  PromptInputTextarea,
  PromptInputSubmit,
  PromptInputFooter,
});

function ChatPage() {
  return (
    <Antfly url="http://localhost:8080" table="my-table">
      <ChatBar
        id="chat"
        generator={{ provider: "openai", model: "gpt-4o-mini" }}
        semanticIndexes={["my-index"]}
        showHits
        showFollowUpQuestions
        {...renderers}
      />
    </Antfly>
  );
}
```

That's it — the factory returns `renderUserMessage`, `renderAssistantMessage`, `renderHits`, `renderFollowUpQuestions`, `renderStreamingIndicator`, and `renderInput`, all wired to AI Elements components.

## Overriding Individual Renderers

The factory doesn't cover every render prop (e.g., `renderClarification`, `renderConfidence`, `renderError`). ChatBar falls back to its built-in defaults for those. You can override any renderer after spreading:

```tsx
<ChatBar
  {...renderers}
  renderError={(error) => (
    <div className="text-red-500 p-3 rounded border">{error}</div>
  )}
  renderConfidence={(confidence) => (
    <p className="text-sm text-muted-foreground">
      {confidenceLabel(confidence)}
    </p>
  )}
/>
```

## Custom Class Names

Pass `classNames` to the factory to style the AI Elements wrappers:

```tsx
const renderers = createAIElementsRenderers(components, {
  classNames: {
    userMessage: "max-w-[80%] ml-auto",
    assistantMessage: "max-w-[90%]",
    sources: "mt-2",
    suggestions: "mt-4",
    promptInput: "border-t",
  },
});
```

## Data Transforms (Low-Level)

For full control, use the individual transform functions instead of the factory:

```tsx
import { hitToSourceProps, turnToStatus, confidenceLabel } from "@antfly/components";

// Map a QueryHit → { key, href, title } for the Source component
const sourceProps = hitToSourceProps(hit, index);
// → { key: "doc-1", href: "https://...", title: "My Document" }

// Map streaming state → PromptInputSubmit status
const status = turnToStatus(isStreaming);
// → "streaming" | "awaiting-message"

// Format confidence as a human-readable label
const label = confidenceLabel(confidence);
// → "87% confident"
```

### `hitToSourceProps(hit, index)`

Extracts a display title and URL from a `QueryHit`:
- **title**: `_source.title` → `_source.name` → `_id` → `"Result N"`
- **href**: `_source.url` → `_source.link` → `undefined`
- **key**: `_id` or the index as a string

### `turnToStatus(isStreaming)`

Returns `"streaming"` when active, `"awaiting-message"` when idle.

### `confidenceLabel(confidence)`

Formats `generation_confidence` as a percentage string.

## How It Works

The adapter uses **dependency injection** to avoid any import coupling between `@antfly/components` and AI Elements:

1. `@antfly/components` defines structural interface types (`AIElementsComponents`) that describe the props each AI Elements component accepts.
2. Your app passes the actual component references to `createAIElementsRenderers()`.
3. The factory closes over those components and returns render prop functions that compose them with Antfly data.

This means:
- `@antfly/components` has **zero dependency** on AI Elements or the `ai` package
- TypeScript checks structural compatibility at the call site
- Tree-shaking eliminates the adapter entirely if you don't import it

## Exports

```tsx
// Factory
import { createAIElementsRenderers } from "@antfly/components";

// Data transforms
import { hitToSourceProps, turnToStatus, confidenceLabel } from "@antfly/components";

// Types
import type {
  AIElementsComponents,
  AIElementsRenderers,
  AIElementsRenderersOptions,
  SourceItemProps,
  PromptInputStatus,
} from "@antfly/components";
```
