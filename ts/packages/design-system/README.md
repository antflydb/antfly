# @antfly/design-system

The official Antfly component library. shadcn/ui primitives (new-york style), compound components, and brand blocks, built on Tailwind v4 with the Aeonik typeface and OKLch design tokens.

## Install

```bash
pnpm add @antfly/design-system
```

Peer deps (install in your app): `react ^18 || ^19`, `react-dom ^18 || ^19`, `tailwindcss ^4`.

## Wire up Tailwind (Next.js / Tailwind v4)

```css
/* app/globals.css */
@import "@antfly/design-system/styles.css";

/* scan library classes so they survive production purge */
@source "../../node_modules/@antfly/design-system/dist/**/*.js";
```

No PostCSS config needed beyond Tailwind v4 defaults. Dark mode is class-based (`<html class="dark">`).

If your app CSS uses `@apply`, import the raw Tailwind contract instead of the
compiled stylesheet:

```css
@import "tailwindcss";
@import "@antfly/design-system/tokens";
@import "@antfly/design-system/theme";
@import "@antfly/design-system/charts.css";
@import "@antfly/design-system/dashboard.css";

@custom-variant dark (&:is(.dark *));
```

## Use

```tsx
import { Button, Card, CardContent } from "@antfly/design-system";

export function SignInCard() {
  return (
    <Card>
      <CardContent className="flex items-center justify-between p-6">
        <p className="font-display text-xl">Welcome back</p>
        <Button>Sign in</Button>
      </CardContent>
    </Card>
  );
}
```

## Exports

- `.` — all components and `cn()`
- `./styles.css` — tokens, fonts, Tailwind theme
- `./tokens` — raw CSS variables only (no Tailwind directives)
- `./theme` — Tailwind v4 theme mappings for canonical tokens
- `./primitives` — shadcn/ui-style low-level React primitives
- `./brand` — Antfly/SearchAF brand primitives and lightweight brand blocks
- `./components` — compound product components built from primitives
- `./charts` — chart helpers and Recharts wrappers
- `./charts.css` — chart CSS variables for custom renderers
- `./dashboard.css` — light dashboard/status CSS contract for shared layout hooks
- `./templates` — reserved for documented composition templates
- `./examples` — reserved for non-canonical examples
- `./fonts/*` — raw Aeonik TTFs if you need to serve them yourself

## Typography

The library ships three type registers: **Aeonik** (display / brand moments), **Mono** (labels, IDs, instrument readouts), and **Inter** (headings, body, paragraphs). The decision rule:

> *Is this content read as a label/identifier, or as a phrase? Mono if label. Inter if phrase. Aeonik only when it's a brand moment.*

See [`TYPOGRAPHY.md`](./TYPOGRAPHY.md) for the full register breakdown, when to use each, tracking and weight conventions, and a per-component reference table.

## Dashboard Shell

Dashboards should import the raw Tailwind contract plus `dashboard.css`, then
scope product UI under `.af-dashboard`.

The dashboard contract should stay quiet. Components like `DashboardPage`,
`DashboardToolbar`, `AuthShell`, `StatusScreen`, and `StatusCard` exist to
standardize structure, spacing, and composition points; they should not create a
new dashboard-only visual language. Prefer the existing primitives (`Card`,
`Button`, `Badge`, form controls, table primitives) for visible surfaces, and
let app/product context decide any stronger composition.

When a shared dashboard component starts adding custom gradients, shadows,
typography scales, sidebar/table/input overrides, or status-card styling, treat
that as a signal to pause and validate the aesthetic with a user before making it
canonical. Keep global dashboard CSS minimal and avoid broad descendant styling
that changes existing primitives by virtue of living under `.af-dashboard`.

```tsx
import {
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DashboardToolbar,
} from "@antfly/design-system/components";

export function TablesPage() {
  return (
    <div className="af-dashboard">
      <DashboardPage>
        <DashboardPageHeader>
          <div>
            <DashboardPageTitle>Tables</DashboardPageTitle>
            <DashboardPageDescription>Manage table schemas and indexes.</DashboardPageDescription>
          </div>
          <DashboardPageActions>{/* buttons */}</DashboardPageActions>
        </DashboardPageHeader>

        <DashboardToolbar>{/* filters or status chips */}</DashboardToolbar>
      </DashboardPage>
    </div>
  );
}
```

Use `AuthShell`, `StatusScreen`, and `StatusCard` for login, loading, access
denied, and app-error states that live outside the sidebar/dashboard route.

```tsx
import { StatusCard, StatusScreen } from "@antfly/design-system/components";
import { Card, CardDescription, CardHeader, CardTitle } from "@antfly/design-system";

export function AccessDenied() {
  return (
    <StatusScreen>
      <StatusCard>
        <Card>
          <CardHeader className="text-center">
            <CardTitle>Access Denied</CardTitle>
            <CardDescription>
              You do not have permission to access this page.
            </CardDescription>
          </CardHeader>
        </Card>
      </StatusCard>
    </StatusScreen>
  );
}
```

## Migration Guidance

`@antfly/design-system` is the canonical source for Antfly and SearchAF visual
decisions. Shared marketing and dashboard work should start here when it defines
tokens, typography, primitive UI, brand assets, chart styling, or repeated layout
patterns.

Keep app-specific page composition in the app until a pattern is reused across
multiple product surfaces. Prefer examples/templates for marketing sections
before promoting them to stable component exports.

For dashboard and status surfaces, preserve the same bias: centralize the parts
that remove duplicated layout decisions, but keep aesthetics conservative until
they have been reviewed in a real app. The shared package should make the
obvious composition easy without making every consumer inherit a new look.

## Density

Set `data-density="compact"` or `data-density="comfortable"` on any ancestor (html, body, or a container) to rescale every spacing utility underneath it:

```html
<html data-density="comfortable">
  ...
</html>
```

Tailwind v4 compiles `h-*`, `p-*`, `m-*`, `gap-*`, etc. to `calc(var(--spacing) * N)`. The library overrides `--spacing` inside the `[data-density]` scope, so Button heights, Card padding, Hero/CTA section padding, and every other spacing utility scale together. No prop drilling, no component forks.

- **compact** — 0.22rem (~12% tighter), for dense dashboards
- **default** — 0.25rem (Tailwind's baseline), attribute unset
- **comfortable** — 0.3rem (~20% airier), for marketing pages with breathing room

Scope it globally (on `<html>`) or locally (on a container for a single section). Fine-grained one-offs can still use `className` overrides on individual components.
