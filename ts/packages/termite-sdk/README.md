# @antfly/termite-sdk

TypeScript SDK for the [Termite](https://antfly.io/docs/termite) ML inference API.

Termite is an Ollama-like local inference server for ONNX-based ML models, providing:
- **Embedding Generation**: Text and multimodal (CLIP) embedding models
- **Text Chunking**: Semantic chunking with ONNX models or fixed-size fallback
- **Reranking**: Relevance re-scoring for search results

## Installation

```bash
npm install @antfly/termite-sdk
# or
pnpm add @antfly/termite-sdk
# or
yarn add @antfly/termite-sdk
```

## Quick Start

```typescript
import { TermiteClient } from '@antfly/termite-sdk';

const client = new TermiteClient({
  baseUrl: 'http://localhost:8080/api'
});

// Generate embeddings
const embedResult = await client.embed('bge-small-en-v1.5', ['hello world', 'machine learning']);
console.log(embedResult.embeddings); // [[0.01, -0.02, ...], [0.03, -0.04, ...]]

// Chunk text
const chunkResult = await client.chunk('Long document text...', {
  model: 'fixed',
  target_tokens: 500,
  overlap_tokens: 50
});

// Rerank by relevance
const rerankResult = await client.rerank(
  'bge-reranker-v2-m3',
  'machine learning applications',
  ['Introduction to ML...', 'Deep learning...', 'Cooking recipes...']
);
console.log(rerankResult.scores); // [0.85, 0.92, 0.12]

// List available models
const models = await client.listModels();
console.log(models.embedders); // ['bge-small-en-v1.5', ...]
```

## API Reference

### TermiteClient

#### Constructor

```typescript
new TermiteClient(config: TermiteConfig)
```

| Option | Type | Description |
|--------|------|-------------|
| `baseUrl` | `string` | Base URL of the Termite API (e.g., `http://localhost:8080/api`) |
| `headers` | `Record<string, string>` | Optional additional headers |

#### Methods

##### `embed(model, input, options?)`

Generate embeddings for text or multimodal content.

```typescript
// Single text
await client.embed('bge-small-en-v1.5', 'hello world');

// Multiple texts
await client.embed('bge-small-en-v1.5', ['hello', 'world']);

// Multimodal (CLIP)
await client.embed('clip-vit-base-patch32', [
  { type: 'text', text: 'a photo of a cat' },
  { type: 'image_url', image_url: { url: 'data:image/png;base64,...' } }
]);
```

##### `chunk(text, config?)`

Split text into smaller chunks.

```typescript
// Fixed-size chunking
await client.chunk('Long document...', {
  model: 'fixed',
  target_tokens: 500,
  overlap_tokens: 50
});

// Semantic chunking
await client.chunk('Long document...', {
  model: 'chonky-mmbert-small-multilingual-1',
  threshold: 0.5
});
```

##### `rerank(model, query, prompts)`

Rerank prompts by relevance to a query.

```typescript
const result = await client.rerank(
  'bge-reranker-v2-m3',
  'machine learning',
  ['ML introduction...', 'Deep learning...', 'Cooking...']
);
// result.scores = [0.85, 0.92, 0.12]
```

##### `listModels()`

List available models.

```typescript
const models = await client.listModels();
// { embedders: [...], chunkers: [...], rerankers: [...] }
```

##### `getVersion()`

Get version information.

```typescript
const version = await client.getVersion();
// { version: 'v1.0.0', git_commit: 'abc123', ... }
```

## Types

The SDK exports all types from the OpenAPI spec:

```typescript
import type {
  EmbedRequest,
  EmbedResponse,
  ChunkConfig,
  ChunkResponse,
  Chunk,
  RerankRequest,
  RerankResponse,
  ModelsResponse,
  VersionResponse,
  ContentPart,
  TermiteConfig,
  TermiteError,
} from '@antfly/termite-sdk';
```

## License

Apache-2.0
