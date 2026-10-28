# Antfly TypeScript SDK

## LangChain integration foundations

The companion `@antfly/langchain` package uses this SDK for all database and
inference operations. Table/index/document lookup failures expose
`AntflyHTTPError.status` and `.body`; missing records can be distinguished from
authentication or server failures. Empty batch responses throw
`AntflyResponseError` instead of manufacturing mutation counts. Callers must still
inspect the returned batch `status` to distinguish committed, pending, and
repair-required outcomes. Do not blindly retry a mutation after a missing response.

`InferenceClient.embed()` and `embedDense()` accept retrieval task type,
dimensions, instruction, item error policy, and cancellation options. `rerank()`
also accepts an optional `AbortSignal`. These additions preserve the existing
method forms.

`embedDense()` throws if a per-item response contains any failures, preserving
the positional mapping between inputs and returned vectors. Use `embed()` to
inspect successful input indexes and per-item errors when handling partial results.

A TypeScript SDK for interacting with the Antfly API, suitable for both frontend and backend applications.

## Installation

```bash
# For this development API, build from the pinned source using:
# https://antfly.io/docs/reference/sdks#installation
npm install /tmp/antfly-sdk-0.2.0.tgz
```

The source examples target revision `ddba6584b38b59c62e261ef63e0374dc1f711617`.
The inspected npm `0.0.14` package lacks `readAfterWrite`, `read_requirements`,
and media chunking (checked 2026-10-22). Use the pinned source build above for
these unreleased APIs; an unversioned registry install does not provide them.

## Quick Start

```typescript
import { AntflyClient } from '@antfly/sdk';

// Initialize the client
const client = new AntflyClient({
  baseUrl: 'http://localhost:8080',
  auth: {
    username: 'your-username',
    password: 'your-password'
  }
});

// Query data
const results = await client.query({
  table: 'products',
  limit: 10,
  full_text_search: {
    query: 'laptop'
  }
});

// Create a table
await client.tables.create('products', {
  num_shards: 3,
  schema: {
    key: 'id',
    default_type: 'product'
  }
});
```

## Features

- 🚀 **Full TypeScript Support** - Complete type safety with auto-generated types from OpenAPI spec
- 🎯 **Simple API** - Intuitive methods organized by resource type
- 🔐 **Authentication** - Built-in support for Basic Auth
- 🌐 **Universal** - Works in both Node.js and browser environments
- ⚡ **Lightweight** - Minimal dependencies, tree-shakeable

## API Reference

### Client Initialization

```typescript
const client = new AntflyClient({
  baseUrl: 'https://api.antfly.io',
  auth: {
    username: 'admin',
    password: 'password'
  },
  headers: {
    'X-Custom-Header': 'value'
  }
});
```

### Table Operations

```typescript
// List all tables
const tables = await client.tables.list();

// Get table details
const tableInfo = await client.tables.get('products');

// Create a table
await client.tables.create('products', {
  num_shards: 3,
  schema: { /* ... */ }
});

// Drop a table
await client.tables.drop('products');

// Query a specific table
const results = await client.tables.query('products', {
  limit: 20,
  full_text_search: { query: 'search term' }
});

// Batch operations
await client.tables.batch('products', {
  inserts: {
    'key1': { /* data */ },
    'key2': { /* data */ }
  },
  deletes: ['key3', 'key4']
});

// Lookup a specific key
const record = await client.tables.lookup('products', 'product:123');

// Backup and restore
await client.tables.backup('products', {
  backup_id: 'backup-001',
  location: 's3://bucket/backups/products'
});

await client.tables.restore('products', {
  backup_id: 'backup-001',
  location: 's3://bucket/backups/products'
});
```

### Index Operations

```typescript
// List indexes for a table
const indexes = await client.indexes.list('products');

// Get index details
const indexInfo = await client.indexes.get('products', 'price_index');

// Create an index
await client.indexes.create('products', 'embeddings_index', {
  type: 'embeddings',
  field: 'description',
  dimension: 768,
  embedder: {
    provider: 'openai',
    model: 'text-embedding-ada-002'
  }
});

// Drop an index
await client.indexes.drop('products', 'old_index');
```

### User Management

```typescript
// Get user details
const user = await client.users.get('john_doe');

// Create a user
await client.users.create('john_doe', {
  password: 'secure_password',
  initial_policies: [
    {
      resource: 'products',
      resource_type: 'table',
      type: 'read'
    }
  ]
});

// Update password
await client.users.updatePassword('john_doe', 'new_password');

// Manage permissions
const permissions = await client.users.getPermissions('john_doe');

await client.users.addPermission('john_doe', {
  resource: 'orders',
  resource_type: 'table',
  type: 'write'
});

await client.users.removePermission('john_doe', 'orders', 'table');
```

### Global Query

```typescript
// Execute a query across all tables
const results = await client.query({
  full_text_search: {
    query: 'important document'
  },
  limit: 50,
  aggregations: {
    category: {
      type: 'terms',
      field: 'category',
      size: 10
    }
  }
});
```

### Advanced Usage

#### Custom Headers

```typescript
const client = new AntflyClient({
  baseUrl: 'https://api.antfly.io',
  headers: {
    'X-API-Key': 'your-api-key',
    'X-Request-ID': 'unique-request-id'
  }
});
```

#### Update Authentication

```typescript
client.setAuth('new_username', 'new_password');
```

#### Access Raw OpenAPI Client

For advanced use cases, you can access the underlying OpenAPI client:

```typescript
const rawClient = client.getRawClient();
// Use rawClient for direct OpenAPI operations
```

## TypeScript Types

The SDK exports all types for use in your application:

```typescript
import type {
  QueryRequest,
  QueryResult,
  Table,
  CreateTableRequest,
  User,
  Permission,
  // ... and many more
} from '@antfly/sdk';
```

## Development

### Regenerate Types

When the OpenAPI spec changes, regenerate the types:

```bash
npm run generate
```

### Build

```bash
npm run build
```

### Watch Mode

```bash
npm run dev
```

### Type Checking

```bash
npm run test:ts
```

## Requirements

- Node.js >= 24.0
- TypeScript >= 5.0 (for development)

## License

Apache-2.0 License. See `LICENSE` file for details.

## Examples

### Browser Usage

See `examples/browser-example.html` for a complete browser example. You can serve it locally:

```bash
npx http-server examples
# Open http://localhost:8080/browser-example.html
```

### Node.js Usage

See `examples/node-example.ts` for a complete Node.js example:

```bash
npm run example:node
```

## Error Handling

The SDK throws errors for failed requests. Always wrap API calls in try-catch blocks:

```typescript
try {
  const result = await client.query({ /* ... */ });
} catch (error) {
  if (error instanceof Error) {
    console.error('Query failed:', error.message);
  }
}
```

## Environment Variables

For production use, store credentials in environment variables:

```typescript
const client = new AntflyClient({
  baseUrl: process.env.ANTFLY_URL!,
  auth: {
    username: process.env.ANTFLY_USERNAME!,
    password: process.env.ANTFLY_PASSWORD!
  }
});
```

## Testing

Run the test suite:

```bash
npm test
npm run test:coverage  # With coverage
npm run test:ts       # Type checking only
```

## Development

### Setup

```bash
# Install dependencies
npm install

# Run in watch mode
npm run dev

# Lint and format
npm run lint
npm run format
```

### Publishing

The package is automatically published to npm when changes are pushed to the main branch. To publish manually:

```bash
npm run build
npm publish --access public
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure:
- All tests pass (`npm test`)
- Type checking passes (`npm run test:ts`)
- Code is linted (`npm run lint`)
- Code is formatted (`npm run format`)

## Support

For issues and feature requests, please use the [GitHub issue tracker](https://github.com/antflydb/antfly/issues).

## Agent capacity errors

Retrieval and query-builder HTTP capacity failures reject with `InferenceCapacityError`,
which exposes `retryable`, `reason`, `code`, and `retryAfterMs`. Once an SSE stream has
started, receive the same typed error through `onErrorDetail`; the existing `onError`
callback still receives a message string. Retry only after the indicated delay.

```typescript
import { AntflyClient, InferenceCapacityError } from '@antfly/sdk';

const client = new AntflyClient({ baseUrl: 'http://localhost:8080' });
const request = { query: 'Find relevant notes', queries: [{ table: 'notes' }] };
await client.streamRetrievalAgent(request, {
  onErrorDetail(error) {
    if (error instanceof InferenceCapacityError) {
      console.log(`Retry after ${error.retryAfterMs} ms: ${error.reason}`);
    }
  },
});
```

## Model-aware media chunks

`InferenceClient.chunkAndEmbed` and `chunkAndEmbedStream` accept text, PDF,
images, GIF, audio and video. Every result carries source coordinates alongside
its embedding. For example, using an inference client configured for your server:

```typescript
const response = await inference.chunkAndEmbed({
  input: "A long document…",
  embedding: { model: "embeddinggemma2", task_type: "RETRIEVAL_DOCUMENT" },
  policy: { target_tokens: 500, overlap_tokens: 50 },
  include_content: true,
});
for (const chunk of response.data) {
  console.log(chunk.descriptor.coordinates, chunk.embedding);
}
```

Check `status`, `gaps` and `next_continuation` before declaring complete coverage.
Pass `next_continuation` as `continuation` with the same source and policy to
resume. `chunkAndEmbedStream` yields start context, descriptor/vector pairs, then
a terminal coverage record; EOF without that record throws. Retain the context's
model identity with each vector. Use `embedding.execute: false` for planning only.
Inline media uses `{type: "media", mime_type, data}` with base64 data; immutable
HTTP/S3 objects use a version-pinned `ChunkSourceReference`. See the
[shared contract](../../../docs/plans/model-aware-media-chunking.md) for source
security, rendering policies, limits and native database integration.

## Native memory integer precision

Use `client.memory` or the typed `client.memoryApi` for `/memory/v1` endpoints.
Safe JSON integers remain JavaScript numbers; larger native resource IDs,
revisions, and timestamps decode as `bigint` and are sent back as exact numeric
JSON. Use bigint literals for full-width inputs, such as `9007199254740993n`.
Unsafe integer numbers are rejected because their original value is already lost.
`MemorySchemas` exposes the corresponding `number | bigint` numeric types.
`parseMemoryJSON` and `stringifyMemoryJSON` provide the same codec for external
persistence or logging. The ordinary `client.api` surface retains its existing
number types; native memory endpoints live on `client.memoryApi`.
