# SDK Design

Antfly SDKs are generated from one public OpenAPI contract and expose product
surfaces from one package per language.

## OpenAPI Contracts

- Source specs live under `specs/openapi/` and stay split by implementation
  owner:
  - `specs/openapi/antfly/metadata.yaml`
  - `specs/openapi/antfly/usermgr.yaml`
  - `specs/openapi/antfly/public.yaml`
  - `specs/openapi/termite/api.yaml`
- The public SDK contract is `specs/openapi/public.yaml`.
- `scripts/join_openapi.py` builds the Antfly public source contract.
- `scripts/join_public_openapi.py` joins Antfly and Termite into the public SDK
  contract.
- Public route prefixes are part of the joined contract:
  - Antfly data API: `/api/v1`
  - Auth API: `/auth/v1`
  - Termite ML API: `/ml/v1`

Zig remains the owner of the source OpenAPI contracts and generated server/client
code. Public SDKs must generate from `specs/openapi/public.yaml`, not from the
split Antfly or Termite source specs.

## Package Layout

- Go SDK: `go/pkg/sdk`
- TypeScript SDK: `ts/packages/sdk`
- Python SDK: `py/packages/sdk`
- Rust SDK crate: `rs/crates/sdk`
- Rust `pgaf` crate: `rs/crates/pgaf`

Shared repository scripts live in top-level `scripts/`. Zig-only build code stays
under `zig/`; there is no `zig/scripts/` directory.

## SDK Shape

Each language exposes one SDK package. Antfly and Termite are product surfaces
inside that package, not separate packages.

Go:

```go
client, err := sdk.NewClient(sdk.Config{
    BaseURL: "http://localhost:8080",
})
antfly := client.Antfly()
termite := client.Termite()
```

TypeScript:

```ts
const client = new Client({ baseUrl: "http://localhost:8080" });
const antfly = client.Antfly();
const termite = client.Termite();
```

Configuration has one required base URL and an optional Termite override:

- `BaseURL` / `baseUrl` points at the joined public server.
- `TermiteBaseURL` / `termiteBaseUrl` points ML operations at hosted Termite
  when needed.
- When the Termite override is empty, Termite uses the same base URL.

Hosted Termite is the exception: it may serve only `/ml/v1`. SDK constructors
accept legacy base URLs ending in `/api/v1` or `/ml/v1` and normalize them to the
server root before issuing requests to the joined paths.

## Generation Rule

All public SDK generated clients and types must be regenerated from
`specs/openapi/public.yaml`:

- Go: `go/pkg/sdk/oapi`
- TypeScript: `ts/packages/sdk/src/public-api.d.ts`
- Python: `py/packages/sdk/src/antfly/client_generated`
- Rust: `rs/crates/sdk`

The split source specs are still useful for ownership and Zig server generation,
but they are not SDK contracts.
