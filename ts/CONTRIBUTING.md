# Contributing to Antfly TypeScript SDK

First off, thank you for considering contributing to the Antfly TypeScript SDK! It's people like you that make this SDK better for everyone.

## Code of Conduct

By participating in this project, you agree to abide by our code of conduct: be respectful, constructive, and professional.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When you create a bug report, please include as many details as possible using our bug report template.

**Great Bug Reports** tend to have:
- A quick summary and/or background
- Steps to reproduce (be specific!)
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please use the feature request template and include:
- Use case for the feature
- Proposed API design
- Alternative solutions you've considered

### Pull Requests

1. Fork the repo and create your branch from `main`
2. If you've added code that should be tested, add tests
3. If you've changed APIs, update the documentation
4. Ensure the test suite passes
5. Make sure your code lints
6. Issue that pull request!

## Development Setup

### Prerequisites

- Node.js >= 18
- npm or yarn or pnpm
- Git

### Setting Up Your Development Environment

1. Fork and clone the repository:
```bash
git clone https://github.com/YOUR-USERNAME/antfly-sdk-ts.git
cd antfly-sdk-ts
```

2. Install dependencies:
```bash
npm install
```

3. Run the development build watcher:
```bash
npm run dev
```

### Running Tests

```bash
# Run all tests
npm test

# Run tests with coverage
npm run test:coverage

# Type checking only
npm run test:ts
```

### Code Style

We use Prettier and ESLint to maintain code quality:

```bash
# Format code
npm run format

# Lint code
npm run lint

# Fix lint issues
npm run lint:fix
```

### Building

```bash
# Build the SDK
npm run build

# Generate types from OpenAPI spec
npm run generate
```

### Testing Your Changes

1. **Unit Tests**: Write tests for your changes in the `test/` directory
2. **Type Tests**: Ensure TypeScript types are correct with `npm run test:ts`
3. **Integration Testing**: Test with the examples:
   ```bash
   # Test Node.js example
   npm run example:node
   
   # Test browser example
   npx http-server examples
   # Open http://localhost:8080/browser-example.html
   ```

### Regenerating Types

If the Antfly OpenAPI specification changes:

1. Update the OpenAPI spec file location in `package.json` if needed
2. Run: `npm run generate`
3. Review the changes in `src/antfly-api.d.ts`
4. Update `src/types.ts` if new type exports are needed

## Project Structure

```
antfly-sdk-ts/
├── src/                 # Source code
│   ├── client.ts       # Main SDK client
│   ├── types.ts        # Type definitions and exports
│   ├── index.ts        # Main entry point
│   └── antfly-api.d.ts # Auto-generated OpenAPI types
├── test/               # Test files
├── examples/           # Usage examples
├── dist/               # Built files (generated)
└── docs/               # Documentation (generated)
```

## Commit Messages

We follow conventional commits:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Test changes
- `refactor:` Code refactoring
- `style:` Code style changes
- `perf:` Performance improvements
- `chore:` Build process or auxiliary tool changes

Examples:
```
feat: add retry logic for failed requests
fix: handle null responses in query method
docs: update README with new examples
```

## Releasing

Releases are automated through GitHub Actions when changes are pushed to the main branch. To prepare a release:

1. Update version in `package.json`
2. Update `CHANGELOG.md`
3. Create a pull request
4. After merge, the CI will automatically publish to npm

## Questions?

Feel free to open an issue with your question or reach out in GitHub Discussions.

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License.
