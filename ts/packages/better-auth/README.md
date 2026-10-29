# @antfly/better-auth

Better Auth persistence and owner-scoped document search for Antfly. Embedded and remote entry points share the custom adapter, schema checks, and restricted content API.

Initial release candidate (Node 24+):

```sh
npm install @antfly/better-auth@0.1.0-rc.1 @antfly/embedded@0.3.0-rc.6 better-auth@~1.7.14
```

For remote apps, omit the embedded package and install `@antfly/sdk@^0.2.2`. Use Antfly server 0.3.0-rc.6 or a later compatible release. Explicit embedded versions avoid the npm `latest` setup placeholder.

```ts
import { betterAuth } from "better-auth";
import { getDatabase, antflyAdapter, prepareAuthSchema, authDefaults } from "@antfly/better-auth/embedded";
const db = await getDatabase("./data/app.aflite");
const options = { ...authDefaults, secret: process.env.BETTER_AUTH_SECRET, baseURL: "http://localhost:3000", database: antflyAdapter(db) };
await prepareAuthSchema(db, options);
const auth = betterAuth(options);
```

[Complete guide](https://github.com/antflydb/antfly/blob/main/docs/guides/app-login.mdx) · [Express example](https://github.com/antflydb/antfly/tree/main/ts/examples/express-auth) · [Next.js remote example](https://github.com/antflydb/antfly/tree/main/ts/examples/nextjs-auth)

Requires Better Auth 1.7.14+ within the 1.7 minor. Embedded requires Node 24+ and a persistent macOS arm64 or glibc Linux arm64/x64 host. Use remote Antfly for Node serverless applications. Keep native files on one persistent process, and use a stable API owner for remote SQL connections.

`pnpm test` runs transport/fault tests. `pnpm test:integration` requires a current `ANTFLY_LIBRARY` and runs native auth flows plus the upstream adapter suite. Build dependencies (`@antfly/sdk`, `@antfly/embedded`) first. Run `ANTFLY_AUTH_TEST_BINARY=/absolute/path/to/antfly pnpm test:remote` to exercise the remote transport and the same upstream suite; the fixture starts isolated disposable standalone servers. `AUTH_APP_URL=http://localhost:3000 node test/http-smoke.mjs` checks either running example over HTTP. No test run publishes packages.

Before the first npm release, configure the package trusted publisher for `.github/workflows/ts-npm-publish.yml`. Zig E2E runs the supported-version native, remote, installed-package, and container checks using its shared base/full server and library artifacts. The `ts/antfly/better-auth/v*` tag requires a successful main Zig E2E qualification for that exact commit before publishing; it does not rebuild native artifacts. Publish SDK 0.2.2 and embedded/server 0.3.0-rc.6 before this integration. Auth prerelease tags publish to npm `next`; stable versions publish to `latest`.
