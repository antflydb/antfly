# @antfly/design-system

The official Antfly component library. shadcn/ui primitives (radix-vega style, on the `radix-ui` package), compound components, and brand blocks, built on Tailwind v4 with the Aeonik typeface and OKLch design tokens.

## Install

```bash
pnpm add @antfly/design-system
```

Peer deps (install in your app): `react ^19`, `react-dom ^19`, `tailwindcss ^4`.

## Wire up Tailwind (Next.js / Tailwind v4)

```css
/* app/globals.css */
@import "@antfly/design-system/styles.css";

/* scan library classes so they survive production purge */
@source "../../node_modules/@antfly/design-system/dist/**/*.js";
```

No PostCSS config needed beyond Tailwind v4 defaults. Dark mode is class-based (`<html class="dark">`).

`theme` also defines the state variants the shadcn components use (`data-open:`,
`data-checked:`, `data-disabled:`, and the rest) and the `scroll-fade-*` and
`shimmer` utilities, so an app that imports it can use them too.

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

- `.` — all components, `cn()`, and the code highlighting helpers
  (`useHighlightedCode`, `highlightCode`)
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

## Borders

Every component chassis routes its border width through `--border-width`
(default `1px`, declared in `tokens/borders.css`), referenced in classes as
`border-(length:--border-width)`. Color follows one doctrine:

- **In-flow surfaces** (Card, table rails, Tabs rail, toolbar, Sidebar) use
  `--border` — quiet separators that recede into the page.
- **Floating surfaces** (Dialog, Popover, menus, Sheet, Select content) use
  `--border-strong` plus one soft shadow, the only shadows in the system.
  Everything in the page flow stays flat.
- **Interactive controls** (Input, Textarea, Select trigger, Checkbox,
  outline Buttons) use `--input`, a step firmer than `--border`, so controls
  read as affordances while chrome melts away.

## Corners

The page is square; the things you hold are rounded. Page structure
(sections, rules, rails, full-bleed bands, in-flow callouts) stays square.
Controls use `rounded-sm`/`rounded-md` (4px/6px), cards and floating
surfaces `rounded-lg` (10px), dialogs and the product frame `rounded-xl`
(12px). Radii live in `tokens/radii.css`.

## Buttons and badges

The default Button is an ink fill that contrasts the theme (dark on light,
light on dark) — the everyday primary. The amber fill lives on
`variant="brand"` (Button and Badge alike) and is reserved for special,
infrequent actions — creating a table, not applying a filter — at most once
per screen. The default Badge is a quiet hairline chip.

## Building blocks

Reach for these before hand-rolling the same thing in an app.

- **Spinner** for anything loading. It carries `role="status"` and a label,
  so screen readers announce it. Put it inside a disabled Button while an
  action runs.
- **Kbd** and **KbdGroup** for keyboard shortcuts. Keys are data, so they
  render in mono.
- **Item** for lists of things: tables, models, connections. Stack Items in
  an `ItemGroup` as border-separated rows instead of laying records out as
  card grids. Pass `asChild` with a link to make a whole row clickable.
- **Field** for forms. `Field`, `FieldLabel`, `FieldDescription`, and
  `FieldError` line up label, help text, and error the same way everywhere.
  Set `data-invalid` on the Field and `aria-invalid` on the control when a
  value is wrong. `FieldSet` and `FieldLegend` group related fields, and a
  `FieldLabel` wrapping a horizontal `Field` makes a selectable choice card.
  Field works with any form library. The `Form*` components remain for
  existing react-hook-form screens.
- **Empty** for empty states built from parts. `EmptyState` and
  `AntyEmptyState` are the ready-made versions and are built on it.
- **CodeBlock** for code, JSON, logs, and diffs, with optional line
  numbers, highlighted lines, `added`/`removed` diff lines, wrapping, and a
  copy button. `useHighlightedCode(code, language)` returns highlighted
  lines and tokenizes only the new lines while a response streams.
  `highlightCode` does the same on the server. Each language's grammar
  loads the first time it is used.
- **Message**, **MessageScroller**, and **Trace** for AI conversations.
  Message lays out one turn. MessageScroller keeps the newest turn in view
  while it streams and offers a jump-to-latest button. Trace lists an
  agent's steps (searches, tool calls, reasoning) with a status per line
  (`running` shimmers, `error` turns red, `pending` dims), and a step can
  expand to show its details.

Selection is shown with a check mark in foreground ink, never an amber
dot: Select, Switcher, MultiSelect, and radio items in menus all follow this.

## Layout

`Stack`, `Inline`, and `Section` encode the system's whitespace rhythm so
pages don't hand-roll margins: `Stack`/`Inline` take `gap="sm" | "md" | "lg"`
(0.75rem / 1.5rem / 2.5rem), and `Section` is a page band (`py-10 md:py-12`;
`size="lg"` gives the `py-16 md:py-24` marketing scale).

## Typography

The library ships three type registers: **Aeonik** (display / brand moments), **Inter** (all interface text: controls, labels, headings, body), and **Mono** (data: IDs, values, code). The decision rule:

> *Is this text data, or is it interface? Mono if it's data. Inter for everything else. Aeonik only when it's a brand moment.*

See [`TYPOGRAPHY.md`](TYPOGRAPHY.md) for the full register breakdown, when to use each, tracking and weight conventions, and a per-component reference table.

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
