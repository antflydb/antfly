# @antfly/cli

npm package for installing the native Antfly CLI.

```bash
npm install -g @antfly/cli
antfly --version
```

This package is separate from `@antfly/sdk`, which contains the TypeScript SDK.
The CLI package depends on a platform-specific package that carries the native
`antfly` executable, Antfarm dashboard assets, and the Antfly C ABI
header/library.
