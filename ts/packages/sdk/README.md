# Antfly TypeScript SDK

A TypeScript SDK for interacting with the Antfly API, suitable for both frontend and backend applications.

## Installation

```bash
npm install @antfly/sdk
# or
yarn add @antfly/sdk
# or
pnpm add @antfly/sdk
```

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

- Node.js >= 18
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

For issues and feature requests, please use the [GitHub issue tracker](https://github.com/antfly/antfly-sdk-ts/issues).
