# antfly-ts — Agent Notes

## What this repo is

TypeScript monorepo for every Antfly frontend surface: the public SDK, unstyled
search primitives, the shared design system, and the Antfarm dashboard. All
published packages ship through this repo's CI and tag-triggered npm releases.

## Layout

```
packages/sdk/           → @antfly/sdk (TS client for the joined public API)
packages/components/     → @antfly/components (unstyled search primitives:
                           Autosuggest, AnswerBar, SearchBar)
packages/design-system/  → @antfly/design-system (shadcn/ui-based shared
                           component library: primitives, compound patterns,
                           brand/marketing blocks, OKLCH tokens, Aeonik)
apps/antfarm/            → dashboard (consumes sdk + components)
apps/playground/         → design-system gallery (unpublished; used by
                           library authors to dogfood new primitives)
```

## Conventions

- **Tag-based releases**: `ts/antfly/<package>/v<semver>` on the antfly
  monorepo — e.g. `ts/antfly/sdk/v0.0.12`, `ts/antfly/design-system/v0.2.0`.
  See `.github/workflows/ts-npm-publish.yml` at the repo root. npm Trusted
  Publishing via OIDC; no secrets in CI. (The old `<package>-v<semver>`
  convention belonged to the retired standalone antfly-ts repo.)
- **Versioning**: bump the package's `version` field before tagging — the
  workflow publishes whatever version package.json declares (it does not
  cross-check the tag), and npm rejects re-publishing an existing version.
  CI runs `pnpm install --frozen-lockfile`, `pnpm build --force`, and
  `pnpm test` before `pnpm --filter <pkg> publish --provenance`.
- **Commands** (from repo root):
  - `pnpm install` — install all workspaces
  - `pnpm build` / `dev` / `test` / `typecheck` / `lint` — turbo-delegated
  - `pnpm --filter <pkg> <task>` — scope to one workspace
- **React target**: 19 for dev. peerDeps accept React 18 to keep the door
  open for the SearchAF desktop app (Wails + React 18).
- **Lint**: biome (`biome check .` via `pnpm lint`).

## @antfly/design-system specifics

- Built with Vite lib mode + `vite-plugin-dts`. `tailwindcss` CLI emits
  `dist/index.css` (a token bundle consumers import).
- **Fonts** (Aeonik) are bundled. `tokens/typography.css` declares
  `@font-face` using `./fonts/aeonik/...` relative paths; the build emits
  those fonts into `dist/fonts/` so URLs resolve when consumers import
  `@antfly/design-system/styles.css`.
- **Dark mode** is class-based via `@custom-variant dark (&:is(.dark *))`.
  Apply `<html class="dark">` to opt in. **Consumer apps must re-declare
  this variant in their own Tailwind entry** — the variant is a compile-time
  directive and can't be exported through the compiled stylesheet. Without
  it, `dark:*` utilities fall back to `@media (prefers-color-scheme: dark)`
  and ignore the class toggle. See `apps/playground/app/globals.css` for the
  canonical pattern.
- **Density**: scoped via `data-density="compact"` / `"comfortable"` on any
  ancestor. Overrides Tailwind v4's `--spacing` variable, rescaling every
  spacing utility in the subtree without component changes.
- **Tokens** are the single source of truth for color/spacing/typography.
  Never hardcode hex in a component — route through a CSS var.

## shadcn primitives workflow

Add primitives with `pnpm dlx shadcn@latest add <name>` from inside
`packages/design-system/` (new-york style). Then re-export from `src/index.ts`.

## Related packages in the org

- `@antfly/components` (this repo, `packages/components/`) — unstyled search
  primitives. Not merged with `@antfly/design-system`.
- `@searchaf/ui` — shadcn-based UI package used by dashboard surfaces; future
  work will migrate the dashboard onto `@antfly/design-system`.
- `@antfly/www-design` — predecessor for tokens + Aeonik; still used by
  marketing sites.

## What does NOT belong here

- Antfly backend code — that's in the parent `antfly/` Go module
- App-specific widgets (Shopify, dashboard-specific chrome) — they live with
  their app
- shadcn CLI registry distribution — v1 of the design system is
  compiled-library-only
