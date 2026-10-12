# Antfly TypeScript

A monorepo containing TypeScript/JavaScript packages for Antfly.

## Packages

| Package | Description | npm |
|---------|-------------|-----|
| [@antfly/sdk](packages/sdk) | TypeScript SDK for the Antfly and Antfly inference APIs | [![npm](https://img.shields.io/npm/v/@antfly/sdk.svg)](https://www.npmjs.com/package/@antfly/sdk) |
| [@antfly/components](packages/components) | React components for building search interfaces | [![npm](https://img.shields.io/npm/v/@antfly/components.svg)](https://www.npmjs.com/package/@antfly/components) |

## Quick Start

### SDK

```bash
pnpm add @antfly/sdk
# or
npm install @antfly/sdk
```

```typescript
import { AntflyClient } from '@antfly/sdk';

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
```

### React Components

```bash
pnpm add @antfly/components
# or
npm install @antfly/components
```

```jsx
import { Antfly, QueryBox, Facet, Results } from '@antfly/components';

const MySearchApp = () => (
  <Antfly url="http://localhost:8080" table="movies">
    <QueryBox id="mainSearch" mode="live" placeholder="Search movies..." />
    <Facet id="actors" fields={["actors"]} />
    <Results
      id="results"
      items={data =>
        data.map(item => (
          <div key={item._id}>
            <h3>{item._source.title}</h3>
          </div>
        ))
      }
    />
  </Antfly>
);
```

## Development

This monorepo uses [pnpm](https://pnpm.io/) and [Turborepo](https://turbo.build/) for package management and builds.

### Prerequisites

- Node.js >= 24.0
- pnpm >= 10

### Setup

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test

# Run in development mode
pnpm dev

# Type checking
pnpm typecheck

# Lint and format
pnpm lint
pnpm format
```

### Package-specific commands

```bash
# Run storybook for React components
pnpm storybook

# Build storybook
pnpm build-storybook

# Regenerate SDK types from OpenAPI spec
pnpm --filter @antfly/sdk generate
```

### Project Structure

```
packages/sdk/            → @antfly/sdk (TS client for the joined public API)
packages/components/     → @antfly/components (unstyled search primitives:
                           QueryBox, Autosuggest, Facet, Results, AnswerResults,
                           ChatBar, plus streaming hooks)
packages/design-system/  → @antfly/design-system (shadcn/ui-based shared
                           component library: primitives, compound patterns,
                           brand/marketing blocks, OKLCH tokens, Aeonik)
packages/graph/          → @antfly/graph (force-directed graph component)
packages/web/            → @antfly/web (local browser inference via Antfly WASM
                           and WebGPU; UI in Colony)
packages/embedded/       → @antfly/embedded (Node binding for local Antfly
                           databases and inference over the libantfly C ABI)
                           with platform packages embedded-darwin-arm64,
                           embedded-linux-arm64, embedded-linux-x64
packages/cli/            → @antfly/cli (npm wrapper that installs the antfly
                           binary) with platform packages cli-darwin-arm64,
                           cli-linux-arm64, cli-linux-x64
apps/antfarm/            → dashboard (consumes sdk + components)
apps/model-explorer/     → model registry explorer (unpublished)
apps/playground/         → design-system gallery (unpublished; used by
                           library authors to dogfood new primitives)
```

## Requirements

- Node.js >= 24.0
- TypeScript >= 5.0 (for development)

## License

Apache-2.0 License. See `LICENSE` file for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure:
- All tests pass (`pnpm test`)
- Type checking passes (`pnpm typecheck`)
- Code is linted (`pnpm lint`)

## Support

For issues and feature requests, please use the [GitHub issue tracker](https://github.com/antflydb/antfly/issues).
