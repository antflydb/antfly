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

## Cloudflare Workers

The `@antfly/better-auth/workers` entry supplies `workersAdapter`,
`createWorkersAuth`, and explicit `prepareWorkersAuth` provisioning over a
server-only `AuthKeyValueStore`. Its transactions must be fenced, serializable,
atomic and durable; callbacks execute once and unknown commits are not replayed.
Cloudflare KV does not satisfy that contract. The hosted mail example implements
it with Antfly's native metadata engine and R2, with no SQLite auth database or
remote SQL server. Request-time schema checks reject configuration changes until
explicit migration. Session/identity equality queries use hashed value indexes;
unsupported compound declarations and scans beyond 4,096 candidates fail closed.

See [Antfly Mail](../../../examples/cloudflare-serverless/MAIL.md) for the two
Workers, configuration, local capture tests and live-domain requirements.
`pnpm test:workers` runs real Better Auth flows and the upstream adapter suite;
the example's `npm run test:mail-workerd` covers native persistence and ownership
in actual workerd. Workers use `nodejs_compat` for Better Auth. Node/remote SQL
entry points retain their existing APIs. Additional auth plugins still need
workflow qualification even when their schema declarations are accepted.
