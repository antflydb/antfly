# @antfly/cli

npm package for installing the native Antfly CLI.

```bash
npm install -g @antfly/cli
antfly --version
```

This package is separate from `@antfly/sdk`, which contains the TypeScript SDK.
The CLI package selects a platform-specific package by OS and CPU. Linux npm
installations require glibc 2.28 or newer, matching the supported Node.js 24
runtime. On musl systems, install the portable archive with
`https://releases.antfly.io/antfly/latest/install.sh` instead. The platform
package carries the native `antfly` executable, Antfarm dashboard assets, and
the Antfly C ABI header/library.
