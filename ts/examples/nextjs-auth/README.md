# Next.js with remote Antfly

This recipe uses the Node runtime and a remote Antfly backend. No native library or persistent local file is used by Next.js. Set `ANTFLY_URL` to one stable SQL API owner endpoint, `ANTFLY_TOKEN`, `BETTER_AUTH_SECRET` (32+ random characters), and `APP_ORIGIN` to your HTTPS application origin. Initialize schemas once before scaling app instances. Do not point SQL connections at a round-robin endpoint.

From `ts/`, install dependencies, build `@antfly/sdk` and `@antfly/better-auth`, then run `pnpm --filter @antfly/example-nextjs-auth dev`. The example uses Better Auth's standard React client and database-backed rate limiting. Public registrations should replace `authDefaults` with `consumerAuthOptions` and a production mail callback. Google credentials belong only in the server configuration.
