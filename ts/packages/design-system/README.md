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
@import "tailwindcss";
@import "@antfly/design-system/styles.css";

/* scan library classes so they survive production purge */
@source "../../node_modules/@antfly/design-system/dist/**/*.js";
```

No PostCSS config needed beyond Tailwind v4 defaults. Dark mode is class-based (`<html class="dark">`).

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
- `./fonts/*` — raw Aeonik TTFs if you need to serve them yourself

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
