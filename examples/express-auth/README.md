# Login and personal document search

Node 24+, ESM, Express 5. Run on one persistent process on macOS arm64 or glibc Linux arm64/x64. Install/build from the repository's `ts/` workspace:

```sh
pnpm install
pnpm --filter @antfly/sdk --filter @antfly/embedded --filter @antfly/better-auth build
pnpm --filter @antfly/example-express-auth build
test -f ../examples/express-auth/.env || printf 'BETTER_AUTH_SECRET=%s\n' "$(openssl rand -hex 32)" > ../examples/express-auth/.env
pnpm --filter @antfly/example-express-auth start
```

Open http://localhost:3000, create an account, save a document, and search it. Set `ANTFLY_LIBRARY` to a built libantfly if platform packages have not been populated. The start command loads the local `.env` file; keep that secret and the database on restart.

For deployment, build the Dockerfile from the repository root, using populated Linux native platform packages. Mount `/data`, set a durable random `BETTER_AUTH_SECRET`, and set `APP_ORIGIN` to the external HTTPS origin. Terminate TLS at your reverse proxy. If using a proxy, set `TRUSTED_PROXY_CIDRS` to its exact trusted addresses/networks so per-client rate limiting uses the real client IP. The example overwrites its internal IP header, preventing browser-supplied values from bypassing the limiter. Use `node:24-slim`; Alpine/musl and Windows are unsupported. Embedded files require one persistent Node host; use the remote recipe for serverless hosts.

This first recipe allows unverified email. Before public registration, use `consumerAuthOptions` with your mail delivery callback to require verification and enable password recovery. No email address grants content ownership: ownership uses the authenticated stable user ID.
