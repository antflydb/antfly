# @antfly/better-auth

Better Auth persistence and owner-scoped document search for Antfly. Embedded and remote entry points share the custom adapter, schema checks, and restricted content API.

```ts
import { betterAuth } from "better-auth";
import { getDatabase, antflyAdapter, prepareAuthSchema, authDefaults } from "@antfly/better-auth/embedded";
const db = await getDatabase("./data/app.aflite");
const options = { ...authDefaults, secret: process.env.BETTER_AUTH_SECRET, baseURL: "http://localhost:3000", database: antflyAdapter(db) };
await prepareAuthSchema(db, options);
const auth = betterAuth(options);
```

[Complete guide](https://github.com/antflydb/antfly/blob/main/docs/guides/app-login.mdx) · [Express example](https://github.com/antflydb/antfly/tree/main/examples/express-auth) · [Next.js remote example](https://github.com/antflydb/antfly/tree/main/examples/nextjs-auth)

Requires Better Auth 1.7.14+ within the 1.7 minor. Embedded requires Node 24+ and a persistent macOS arm64 or glibc Linux arm64/x64 host. Use remote Antfly for Node serverless applications. Keep native files on one persistent process, and use a stable API owner for remote SQL connections.

`pnpm test` runs transport/fault tests. `pnpm test:integration` requires a current `ANTFLY_LIBRARY` and runs native auth flows plus the upstream adapter suite. Build dependencies (`@antfly/sdk`, `@antfly/embedded`) first. Run `ANTFLY_AUTH_TEST_BINARY=/absolute/path/to/antfly pnpm test:remote` to exercise the remote transport and the same upstream suite; the fixture starts isolated disposable standalone servers. `AUTH_APP_URL=http://localhost:3000 node test/http-smoke.mjs` checks either running example over HTTP. No test run publishes packages.

Before the first npm release, configure the package trusted publisher for `.github/workflows/ts-npm-publish.yml`. The `ts/antfly/better-auth/v*` tag runs supported-version native and remote checks before publishing. Release the updated SDK with transaction-outcome reads and the embedded binding with SQL write-status metadata alongside this integration. Remote deployment also requires the server fix for concurrent SQL connection-directory mutations included in this change.
