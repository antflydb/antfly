# Chat Agent Frontend Implementation Spec

This document outlines the frontend changes needed to support the new Chat Agent (`/agents/chat`) endpoint.

## Overview

The Chat Agent adds conversational RAG capabilities with:
- **Message history** - Multi-turn conversations
- **Tool calling** - Filter, clarification, and search tools
- **Streaming** - Real-time SSE events for progressive UI updates

## TypeScript SDK Changes (`ts/`)

After running `make generate`, the SDK will include these new types:

### New Types

```typescript
// Message roles
type ChatMessageRole = "user" | "assistant" | "system" | "tool";

// Chat message structure
interface ChatMessage {
  role: ChatMessageRole;
  content: string;
  tool_calls?: ChatToolCall[];
  tool_results?: ChatToolResult[];
}

// Tool call made by assistant
interface ChatToolCall {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
}

// Result from tool execution
interface ChatToolResult {
  tool_call_id: string;
  result: Record<string, unknown>;
  error?: string;
}

// Filter specification
interface FilterSpec {
  field: string;
  operator: "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "contains" | "prefix" | "range" | "in";
  value: unknown;
}

// Clarification request from model
interface ClarificationRequest {
  question: string;
  options?: string[];
  required?: boolean;
}

// Tool names
type ChatToolName = "add_filter" | "ask_clarification" | "search" | "websearch" | "fetch";

// Tools configuration
interface ChatToolsConfig {
  enabled_tools?: ChatToolName[];       // Default: ["add_filter", "ask_clarification", "search"]
  websearch_config?: WebSearchConfig;   // Required when "websearch" tool is enabled
  fetch_config?: FetchConfig;           // Security config for "fetch" tool
  max_tool_iterations?: number;         // Default: 5
}

// Web search provider config (from lib/websearch)
interface WebSearchConfig {
  provider: "google" | "bing" | "serper" | "tavily" | "brave" | "duckduckgo";
  max_results?: number;
  timeout_ms?: number;
  safe_search?: boolean;
  language?: string;
  region?: string;
  // Provider-specific fields (api_key, etc.) - see lib/websearch/openapi.yaml
}

// Fetch config (from lib/websearch, uses lib/scraping)
interface FetchConfig {
  max_content_length?: number;      // Max chars to return (default: 50000)
  allowed_hosts?: string[];         // Whitelist of allowed hostnames
  block_private_ips?: boolean;      // SSRF prevention (default: true)
  max_download_size_bytes?: number; // Max download size (default: 100MB)
  timeout_seconds?: number;         // Download timeout (default: 30s)
}

// Request structure
interface ChatAgentRequest {
  messages: ChatMessage[];
  generator: GeneratorConfig;
  queries: QueryRequest[];
  steps?: ChatAgentSteps;
  with_streaming?: boolean;
  accumulated_filters?: FilterSpec[];
  system_prompt?: string;
  max_context_tokens?: number;
}

// Response structure
interface ChatAgentResult {
  messages: ChatMessage[];
  pending_clarification?: ClarificationRequest;
  applied_filters?: FilterSpec[];
  query_results?: Record<string, unknown>[];
  answer?: string;
  answer_confidence?: number;
  tool_calls_made?: number;
  classification_transformation?: ClassificationTransformationResult;
}
```

### SDK Method

```typescript
// New method on AntflyClient
chatAgent(request: ChatAgentRequest): Promise<ChatAgentResult>;

// Streaming version
chatAgentStream(request: ChatAgentRequest): AsyncIterable<ChatAgentEvent>;
```

## SSE Event Types

When streaming is enabled, the client receives these event types:

| Event Type | Data | Description |
|------------|------|-------------|
| `classification` | `ClassificationTransformationResult` | Query analysis result |
| `clarification_required` | `ClarificationRequest` | Model needs user input |
| `filter_applied` | `FilterSpec` | Filter tool was executed |
| `search_executed` | `{ query: string }` | Internal search tool was executed |
| `websearch_executed` | `{ query: string, results: WebSearchResult[] }` | Web search completed |
| `fetch_executed` | `{ url: string, content: string }` | URL content fetched |
| `hit` | Document hit object | Individual search result |
| `answer` | `string` | Answer text chunk |
| `done` | `{ applied_filters: FilterSpec[] }` | Turn complete |
| `error` | `{ error: string }` | Error occurred |

### Web Search Result Type

```typescript
interface WebSearchResult {
  title: string;
  url: string;
  snippet: string;
  source?: string;  // Domain name
}
```

## React Components (`antfarm`)

### 1. ChatPage Component

Main chat interface page.

**Location**: `antfarm/src/pages/ChatPage.tsx`

```tsx
interface ChatPageProps {
  tableId?: string; // Optional - pre-select table for queries
}

// State management
const [messages, setMessages] = useState<ChatMessage[]>([]);
const [filters, setFilters] = useState<FilterSpec[]>([]);
const [pendingClarification, setPendingClarification] = useState<ClarificationRequest | null>(null);
const [isLoading, setIsLoading] = useState(false);

// Key features:
// - Message list with user/assistant bubbles
// - Input box at bottom (disabled during clarification)
// - Inline clarification cards
// - Filter indicator bar showing active filters
// - Settings drawer for generator config
```

### 2. ChatMessageList Component

Renders conversation history.

**Location**: `antfarm/src/components/chat/ChatMessageList.tsx`

```tsx
interface ChatMessageListProps {
  messages: ChatMessage[];
  onRetry?: (messageIndex: number) => void;
}

// Features:
// - User messages aligned right (blue background)
// - Assistant messages aligned left (gray background)
// - Markdown rendering for assistant messages
// - Citation links clickable [resource_id doc1]
// - Copy button on assistant messages
// - Retry button on failed messages
```

### 3. ChatInput Component

Message input with send button.

**Location**: `antfarm/src/components/chat/ChatInput.tsx`

```tsx
interface ChatInputProps {
  onSend: (message: string) => void;
  disabled?: boolean;
  placeholder?: string;
}

// Features:
// - Auto-resize textarea
// - Send on Enter (Shift+Enter for newline)
// - Disabled state during loading/clarification
// - Character count (optional)
```

### 4. ClarificationCard Component

Inline clarification prompt.

**Location**: `antfarm/src/components/chat/ClarificationCard.tsx`

```tsx
interface ClarificationCardProps {
  question: string;
  options?: string[];
  onSelect: (answer: string) => void;
  onCustomAnswer: (answer: string) => void;
}

// Features:
// - Question text prominent
// - Option buttons if provided
// - Text input for custom answer
// - Distinct styling (highlight border, background)
```

### 5. FilterIndicator Component

Shows active filters with remove capability.

**Location**: `antfarm/src/components/chat/FilterIndicator.tsx`

```tsx
interface FilterIndicatorProps {
  filters: FilterSpec[];
  onRemove: (index: number) => void;
  onClear: () => void;
}

// Features:
// - Horizontal scrollable list of filter badges
// - Each badge shows: field operator value
// - X button to remove individual filter
// - "Clear all" button when multiple filters
```

### 6. ChatSearchResults Component

Displays search results during streaming.

**Location**: `antfarm/src/components/chat/ChatSearchResults.tsx`

```tsx
interface ChatSearchResultsProps {
  hits: DocumentHit[];
  isLoading: boolean;
  onSelectDocument: (doc: DocumentHit) => void;
}

// Features:
// - Collapsible results panel
// - Show document titles/snippets
// - Click to view full document
// - Loading skeleton during search
```

## Routing Changes

Add route in `antfarm/src/App.tsx`:

```tsx
<Route path="/chat" element={<ChatPage />} />
<Route path="/tables/:tableId/chat" element={<ChatPage />} />
```

## Sidebar Navigation

Add chat link in `antfarm/src/components/sidebar.tsx`:

```tsx
{
  title: "Chat",
  url: "/chat",
  icon: MessageSquare, // from lucide-react
}
```

## State Management

Consider using React Context or Zustand for:

```typescript
interface ChatState {
  // Conversation state
  messages: ChatMessage[];
  filters: FilterSpec[];

  // UI state
  isLoading: boolean;
  pendingClarification: ClarificationRequest | null;
  error: string | null;

  // Configuration
  generatorConfig: GeneratorConfig;
  selectedTable: string | null;
  toolsConfig: ChatToolsConfig;

  // Actions
  sendMessage: (content: string) => Promise<void>;
  respondToClarification: (answer: string) => Promise<void>;
  removeFilter: (index: number) => void;
  clearFilters: () => void;
  reset: () => void;
}
```

## SSE Event Handling

```typescript
async function handleChatStream(request: ChatAgentRequest) {
  const response = await fetch('/api/v1/agents/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...request, with_streaming: true }),
  });

  const reader = response.body?.getReader();
  const decoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const text = decoder.decode(value);
    const lines = text.split('\n');

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const data = JSON.parse(line.slice(6));

        switch (data.type) {
          case 'classification':
            // Update UI with query analysis
            break;
          case 'clarification_required':
            setPendingClarification(data.data);
            break;
          case 'filter_applied':
            setFilters(prev => [...prev, data.data]);
            break;
          case 'hit':
            // Add to search results
            break;
          case 'answer':
            // Append to current answer
            break;
          case 'done':
            setIsLoading(false);
            break;
          case 'error':
            setError(data.data.error);
            break;
        }
      }
    }
  }
}
```

## UI/UX Considerations

### Clarification Flow
1. User sends message
2. Model returns `clarification_required` event
3. UI shows clarification card inline (not modal)
4. Input is disabled until user responds
5. User clicks option or types custom answer
6. New message sent with answer appended
7. Process continues

### Filter Accumulation
- Filters persist across conversation turns
- Show filter bar when filters active
- Allow removing individual filters
- Clear all resets to unfiltered state
- Removing filter should re-run query (optional)

### Error Handling
- Network errors: Show retry button
- Invalid response: Show error message with details
- Timeout: Allow cancellation and retry
- Rate limiting: Show cooldown indicator

### Loading States
- Skeleton for message being generated
- Spinner in input area during send
- Progress indicator for long operations
- Typing indicator for streaming responses

## Testing

### Unit Tests
- Message list rendering
- Clarification card interactions
- Filter badge removal
- SSE event parsing

### Integration Tests
- Full conversation flow
- Clarification round-trip
- Filter application
- Error recovery

### E2E Tests
- Complete chat session
- Multi-turn conversation
- Filter accumulation across turns

## Implementation Priority

1. **Phase 1: Basic Chat**
   - ChatPage layout
   - ChatMessageList
   - ChatInput
   - Basic send/receive

2. **Phase 2: Streaming**
   - SSE event handling
   - Progressive answer display
   - Search results panel

3. **Phase 3: Clarifications**
   - ClarificationCard
   - Flow interruption handling
   - Option selection

4. **Phase 4: Filters**
   - FilterIndicator
   - Filter accumulation
   - Filter removal

5. **Phase 5: Polish**
   - Animations
   - Accessibility
   - Mobile responsiveness
   - Keyboard shortcuts
