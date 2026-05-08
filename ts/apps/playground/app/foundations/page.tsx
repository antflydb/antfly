import {
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  GraphPaperBg,
  Lockup,
  Logo,
  MonoLabel,
  Separator,
  Wordmark,
} from "@antfly/design-system";

export const metadata = {
  title: "Foundations · @antfly/design-system",
  description:
    "Design tokens: colors, fonts, radii, shadows. Source files live under packages/design-system/src/tokens/.",
};

/**
 * Semantic color tokens. Both light and dark values are listed explicitly so
 * every swatch can render both modes side-by-side regardless of which theme
 * the playground is currently in. Values here must stay in sync with
 * packages/design-system/src/tokens/colors.css — there isn't a programmatic
 * way to read them at build time, so this is the one place we duplicate.
 */
interface TokenDef {
  name: string;
  varName: string;
  light: string;
  dark: string;
  description?: string;
}

const SEMANTIC_COLORS: TokenDef[] = [
  {
    name: "Background",
    varName: "--background",
    light: "oklch(0.98 0.003 265)",
    dark: "oklch(0.135 0.005 275)",
  },
  {
    name: "Background secondary",
    varName: "--background-secondary",
    light: "oklch(0.965 0.003 265)",
    dark: "oklch(0.11 0.005 275)",
    description: "Optional app-shell background used by dashboard surfaces.",
  },
  {
    name: "Foreground",
    varName: "--foreground",
    light: "oklch(0.185 0.02 275)",
    dark: "oklch(0.91 0.005 265)",
  },
  {
    name: "Card",
    varName: "--card",
    light: "oklch(0.995 0.001 265)",
    dark: "oklch(0.185 0.005 275)",
  },
  {
    name: "Card foreground",
    varName: "--card-foreground",
    light: "oklch(0.185 0.02 275)",
    dark: "oklch(0.91 0.005 265)",
  },
  {
    name: "Popover",
    varName: "--popover",
    light: "oklch(0.995 0.001 265)",
    dark: "oklch(0.185 0.005 275)",
  },
  {
    name: "Popover foreground",
    varName: "--popover-foreground",
    light: "oklch(0.185 0.02 275)",
    dark: "oklch(0.91 0.005 265)",
  },
  {
    name: "Primary",
    varName: "--primary",
    light: "oklch(0.7 0.15 285)",
    dark: "oklch(0.7 0.15 285)",
    description: "Purple accent — identical in light and dark, used sparingly.",
  },
  {
    name: "Primary foreground",
    varName: "--primary-foreground",
    light: "oklch(1 0 0)",
    dark: "oklch(0.135 0.005 275)",
  },
  {
    name: "Secondary",
    varName: "--secondary",
    light: "oklch(0.955 0.003 265)",
    dark: "oklch(0.22 0.005 275)",
  },
  {
    name: "Secondary foreground",
    varName: "--secondary-foreground",
    light: "oklch(0.185 0.02 275)",
    dark: "oklch(0.91 0.005 265)",
  },
  {
    name: "Muted",
    varName: "--muted",
    light: "oklch(0.955 0.003 265)",
    dark: "oklch(0.22 0.005 275)",
  },
  {
    name: "Muted foreground",
    varName: "--muted-foreground",
    light: "oklch(0.55 0.01 265)",
    dark: "oklch(0.65 0.005 265)",
  },
  {
    name: "Accent",
    varName: "--accent",
    light: "oklch(0.955 0.003 265)",
    dark: "oklch(0.22 0.005 275)",
  },
  {
    name: "Accent foreground",
    varName: "--accent-foreground",
    light: "oklch(0.185 0.02 275)",
    dark: "oklch(0.91 0.005 265)",
  },
  {
    name: "Destructive",
    varName: "--destructive",
    light: "oklch(0.577 0.245 27.325)",
    dark: "oklch(0.704 0.191 22.216)",
  },
  {
    name: "Destructive foreground",
    varName: "--destructive-foreground",
    light: "oklch(0.985 0 0)",
    dark: "oklch(0.985 0 0)",
  },
  {
    name: "Success",
    varName: "--success",
    light: "oklch(0.7 0.17 150)",
    dark: "oklch(0.72 0.17 150)",
  },
  {
    name: "Warning",
    varName: "--warning",
    light: "oklch(0.78 0.16 75)",
    dark: "oklch(0.82 0.16 75)",
  },
  {
    name: "Info",
    varName: "--info",
    light: "oklch(0.72 0.13 230)",
    dark: "oklch(0.72 0.13 230)",
  },
  {
    name: "Border",
    varName: "--border",
    light: "oklch(0.91 0.005 265)",
    dark: "oklch(1 0 0 / 8%)",
  },
  {
    name: "Input",
    varName: "--input",
    light: "oklch(0.91 0.005 265)",
    dark: "oklch(1 0 0 / 12%)",
  },
  {
    name: "Ring",
    varName: "--ring",
    light: "oklch(0.65 0.005 265)",
    dark: "oklch(0.5 0.005 265)",
  },
];

const CHART_COLORS: TokenDef[] = [
  {
    name: "Chart 1",
    varName: "--chart-1",
    light: "oklch(0.7 0.15 285)",
    dark: "oklch(0.7 0.15 285)",
    description: "var(--primary) — purple accent.",
  },
  {
    name: "Chart 2",
    varName: "--chart-2",
    light: "oklch(0.55 0.01 265)",
    dark: "oklch(0.65 0.005 265)",
    description: "var(--muted-foreground) — cool gray.",
  },
  {
    name: "Chart 3",
    varName: "--chart-3",
    light: "oklch(0.78 0.15 92)",
    dark: "#FADF5A",
  },
  {
    name: "Chart 4",
    varName: "--chart-4",
    light: "oklch(0.58 0.12 145)",
    dark: "#477F4F",
  },
  {
    name: "Chart 5",
    varName: "--chart-5",
    light: "oklch(0.70 0.13 28)",
    dark: "#F7978D",
  },
  {
    name: "Chart 6",
    varName: "--chart-6",
    light: "oklch(0.64 0.13 225)",
    dark: "oklch(0.64 0.13 225)",
  },
];

interface TokenScale {
  name: string;
  description: string;
  tokens: { varName: string; light: string; dark: string }[];
}

const STATUS_SCALES: TokenScale[] = [
  {
    name: "Danger",
    description: "Compatibility scale for destructive health and failure states.",
    tokens: [
      { varName: "--danger-50", light: "#fef2f2", dark: "#1a0808" },
      { varName: "--danger-100", light: "#fee2e2", dark: "#2a0f0f" },
      { varName: "--danger-200", light: "#fecaca", dark: "#3d1515" },
      { varName: "--danger-300", light: "#fca5a5", dark: "#5c1e1e" },
      { varName: "--danger-400", light: "#f87171", dark: "#7c2626" },
      { varName: "--danger-500", light: "#ef4444", dark: "#ef4444" },
      { varName: "--danger-600", light: "#dc2626", dark: "#f87171" },
      { varName: "--danger-700", light: "#b91c1c", dark: "#fca5a5" },
      { varName: "--danger-800", light: "#991b1b", dark: "#fecaca" },
      { varName: "--danger-900", light: "#7f1d1d", dark: "#fee2e2" },
    ],
  },
  {
    name: "Success",
    description: "Compatibility scale for healthy, complete, and positive states.",
    tokens: [
      { varName: "--success-50", light: "#f0fdf4", dark: "#0a1a0c" },
      { varName: "--success-100", light: "#dcfce7", dark: "#0f2a12" },
      { varName: "--success-200", light: "#bbf7d0", dark: "#153d19" },
      { varName: "--success-300", light: "#86efac", dark: "#1e5c24" },
      { varName: "--success-400", light: "#4ade80", dark: "#267c32" },
      { varName: "--success-500", light: "#22c55e", dark: "#22c55e" },
      { varName: "--success-600", light: "#16a34a", dark: "#4ade80" },
      { varName: "--success-700", light: "#15803d", dark: "#86efac" },
      { varName: "--success-800", light: "#166534", dark: "#bbf7d0" },
      { varName: "--success-900", light: "#14532d", dark: "#dcfce7" },
    ],
  },
  {
    name: "Info",
    description: "Compatibility scale for informational and leader/active states.",
    tokens: [
      { varName: "--info-50", light: "#eff6ff", dark: "#081a2a" },
      { varName: "--info-100", light: "#dbeafe", dark: "#0f2544" },
      { varName: "--info-200", light: "#bfdbfe", dark: "#1e3a5f" },
      { varName: "--info-300", light: "#93c5fd", dark: "#2c5282" },
      { varName: "--info-400", light: "#60a5fa", dark: "#3c6cb4" },
      { varName: "--info-500", light: "#3b82f6", dark: "#3b82f6" },
      { varName: "--info-600", light: "#2563eb", dark: "#60a5fa" },
      { varName: "--info-700", light: "#1d4ed8", dark: "#93c5fd" },
      { varName: "--info-800", light: "#1e40af", dark: "#bfdbfe" },
      { varName: "--info-900", light: "#1e3a8a", dark: "#dbeafe" },
    ],
  },
  {
    name: "Warning",
    description: "Compatibility scale for degraded, pending, and warning states.",
    tokens: [
      { varName: "--warning-50", light: "#fffbeb", dark: "#1a1508" },
      { varName: "--warning-100", light: "#fef3c7", dark: "#2a220f" },
      { varName: "--warning-200", light: "#fde68a", dark: "#3d3015" },
      { varName: "--warning-300", light: "#fcd34d", dark: "#5c4a1e" },
      { varName: "--warning-400", light: "#fbbf24", dark: "#7c6426" },
      { varName: "--warning-500", light: "#f59e0b", dark: "#f59e0b" },
      { varName: "--warning-600", light: "#d97706", dark: "#fbbf24" },
      { varName: "--warning-700", light: "#b45309", dark: "#fcd34d" },
      { varName: "--warning-800", light: "#92400e", dark: "#fde68a" },
      { varName: "--warning-900", light: "#78350f", dark: "#fef3c7" },
    ],
  },
];

const COMPATIBILITY_SCALES: TokenScale[] = [
  {
    name: "SearchAF neutral",
    description: "Temporary compatibility scale for surfaces migrating from existing SearchAF UI.",
    tokens: [
      { varName: "--searchaf-1", light: "#ffffff", dark: "#0a0a0a" },
      { varName: "--searchaf-2", light: "#fafafa", dark: "#0f0f0f" },
      { varName: "--searchaf-3", light: "#f5f5f5", dark: "#131313" },
      { varName: "--searchaf-4", light: "#e5e5e5", dark: "#262626" },
      { varName: "--searchaf-5", light: "#d4d4d4", dark: "#3a3a3a" },
      { varName: "--searchaf-6", light: "#a3a3a3", dark: "#525252" },
      { varName: "--searchaf-7", light: "#737373", dark: "#737373" },
      { varName: "--searchaf-8", light: "#525252", dark: "#a1a1a1" },
      { varName: "--searchaf-9", light: "#1c2024", dark: "#e5e5e5" },
      { varName: "--searchaf-10", light: "#18181b", dark: "#f5f5f5" },
      { varName: "--searchaf-11", light: "#09090b", dark: "#fafafa" },
      { varName: "--searchaf-12", light: "#000000", dark: "#ffffff" },
    ],
  },
  {
    name: "Gray",
    description: "Compatibility gray scale used by dashboard health and fallback states.",
    tokens: [
      { varName: "--gray-1", light: "#fcfcfc", dark: "#0a0a0a" },
      { varName: "--gray-2", light: "#f9f9f9", dark: "#0f0f0f" },
      { varName: "--gray-3", light: "#f0f0f0", dark: "#131313" },
      { varName: "--gray-4", light: "#e4e4e7", dark: "#262626" },
      { varName: "--gray-5", light: "#d4d4d8", dark: "#3a3a3a" },
      { varName: "--gray-6", light: "#a1a1aa", dark: "#525252" },
      { varName: "--gray-7", light: "#71717a", dark: "#737373" },
      { varName: "--gray-8", light: "#52525b", dark: "#a1a1a1" },
      { varName: "--gray-9", light: "#3f3f46", dark: "#d4d4d4" },
      { varName: "--gray-10", light: "#27272a", dark: "#e5e5e5" },
      { varName: "--gray-11", light: "#18181b", dark: "#f5f5f5" },
      { varName: "--gray-12", light: "#09090b", dark: "#ffffff" },
    ],
  },
];

const RADII = [
  { name: "sm", varName: "--radius-sm" },
  { name: "md", varName: "--radius-md" },
  { name: "lg (base)", varName: "--radius-lg" },
  { name: "xl", varName: "--radius-xl" },
];

const SHADOWS = [
  { name: "xs", varName: "--shadow-xs" },
  { name: "sm", varName: "--shadow-sm" },
  { name: "md", varName: "--shadow-md" },
  { name: "lg", varName: "--shadow-lg" },
  { name: "xl", varName: "--shadow-xl" },
];

export default function FoundationsPage() {
  return (
    <div className="mx-auto max-w-5xl space-y-16">
      <header className="space-y-3">
        <MonoLabel>Design tokens</MonoLabel>
        <h1 className="font-display text-4xl font-bold tracking-tight">Foundations</h1>
        <p className="max-w-2xl text-muted-foreground">
          Live specimens for every token shipped by{" "}
          <code className="font-mono text-sm">@antfly/design-system</code>. Authoritative sources:{" "}
          <code className="font-mono text-sm">packages/design-system/src/tokens/</code> and{" "}
          <code className="font-mono text-sm">src/styles.css</code>. Colors show both light and dark
          values per swatch.
        </p>
      </header>

      {/* Colors ---------------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading eyebrow="Color" title="Semantic palette" file="src/tokens/colors.css" />
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          {SEMANTIC_COLORS.map((c) => (
            <ColorSwatch key={c.varName} {...c} />
          ))}
        </div>
      </section>

      <section className="space-y-6">
        <SectionHeading eyebrow="Color" title="Chart palette" />
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          {CHART_COLORS.map((c) => (
            <ColorSwatch key={c.varName} {...c} />
          ))}
        </div>
      </section>

      <section className="space-y-6">
        <SectionHeading eyebrow="Color" title="Status compatibility scales" />
        <div className="grid gap-4 md:grid-cols-2">
          {STATUS_SCALES.map((scale) => (
            <TokenScaleSwatch key={scale.name} scale={scale} />
          ))}
        </div>
      </section>

      <section className="space-y-6">
        <SectionHeading eyebrow="Color" title="Migration compatibility scales" />
        <div className="grid gap-4 md:grid-cols-2">
          {COMPATIBILITY_SCALES.map((scale) => (
            <TokenScaleSwatch key={scale.name} scale={scale} />
          ))}
        </div>
      </section>

      <Separator />

      {/* Typography ------------------------------------------------------ */}
      <section className="space-y-8">
        <SectionHeading
          eyebrow="Typography"
          title="Font families"
          file="src/tokens/typography.css"
        />

        <FontSpecimen
          label="Display"
          varName="--font-display"
          sample="Built for the data your other databases can't touch."
          style={{ fontFamily: "var(--font-display)", fontWeight: 700, letterSpacing: "-0.02em" }}
          className="text-5xl md:text-7xl"
          caption="Aeonik Bold — used by all heading elements automatically."
        />

        <FontSpecimen
          label="Sans"
          varName="--font-sans"
          sample="Hybrid search. Local ML inference. Multimodal documents."
          style={{ fontFamily: "var(--font-sans)" }}
          className="text-xl"
          caption="System sans stack — body text, descriptions, UI copy."
        />

        <FontSpecimen
          label="Mono"
          varName="--font-mono"
          sample="curl -sSL https://antfly.io/install | sh"
          style={{ fontFamily: "var(--font-mono)" }}
          className="text-base"
          caption="Roboto Mono (consumer-loaded). Used for code + technical annotations."
        />

        <div className="rounded-lg border border-border p-6">
          <MonoLabel className="mb-3 block">.mono-label utility</MonoLabel>
          <p className="text-body-sm text-muted-foreground">
            Uppercase Roboto Mono with <code className="font-mono">letter-spacing: 0.1em</code>,
            size <code className="font-mono">--label-size</code>, color{" "}
            <code className="font-mono">--label-color</code>. The &ldquo;technical voice&rdquo;.
          </p>
        </div>
      </section>

      <section className="space-y-4">
        <SectionHeading eyebrow="Typography" title="Heading elements" />
        <div className="rounded-lg border border-border bg-muted/30 p-4 text-sm text-muted-foreground">
          Tailwind&apos;s preflight resets <code className="font-mono text-xs">h1</code>&ndash;
          <code className="font-mono text-xs">h6</code> to inherit font-size. Our base styles apply
          Aeonik / bold / tight tracking to every heading automatically, but{" "}
          <strong>size is applied per-usage</strong> with a Tailwind utility. Raw headings therefore
          all render at body size.
        </div>

        <div className="space-y-3">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Raw (no size utility — body-sized)
          </p>
          {(["h1", "h2", "h3", "h4", "h5", "h6"] as const).map((tag) => {
            const Tag = tag;
            return (
              <div key={tag} className="flex items-baseline gap-6 border-b border-border pb-2">
                <span className="w-10 shrink-0 font-mono text-xs text-muted-foreground">{tag}</span>
                <Tag className="flex-1">The ants are marching.</Tag>
              </div>
            );
          })}
        </div>
      </section>

      <section className="space-y-4">
        <SectionHeading eyebrow="Typography" title="Recommended scale" />
        <p className="max-w-2xl text-sm text-muted-foreground">
          The sizes the library&apos;s own <code className="font-mono text-xs">Hero</code>,{" "}
          <code className="font-mono text-xs">CTA</code>, and{" "}
          <code className="font-mono text-xs">PageHeader</code> components apply. Copy these classes
          onto your own headings for a consistent feel.
        </p>

        <div className="space-y-4">
          <ScaleRow
            name="Hero headline"
            classes="text-5xl md:text-7xl lg:text-8xl"
            sample="Built for the data your other databases can't touch."
          />
          <ScaleRow
            name="CTA headline"
            classes="text-3xl md:text-4xl"
            sample="Ready to ship your answer engine?"
          />
          <ScaleRow
            name="Section heading"
            classes="text-2xl md:text-3xl"
            sample="One database for everything."
          />
          <ScaleRow name="Card title" classes="text-lg" sample="Hybrid Search" />
        </div>
      </section>

      <Separator />

      {/* Radii ----------------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading eyebrow="Shape" title="Radii" file="src/tokens/radii.css" />
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
          {RADII.map((r) => (
            <div key={r.varName} className="flex flex-col items-center gap-3">
              <div
                className="size-24 border border-border bg-muted"
                style={{ borderRadius: `var(${r.varName})` }}
              />
              <div className="text-center">
                <p className="font-medium">{r.name}</p>
                <code className="font-mono text-xs text-muted-foreground">{r.varName}</code>
              </div>
            </div>
          ))}
        </div>
      </section>

      <Separator />

      {/* Shadows --------------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading eyebrow="Elevation" title="Shadows" file="src/tokens/shadows.css" />
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-5">
          {SHADOWS.map((s) => (
            <div key={s.varName} className="flex flex-col items-center gap-3">
              <div
                className="flex size-28 items-center justify-center rounded-lg border border-border bg-card"
                style={{ boxShadow: `var(${s.varName})` }}
              >
                <span className="font-mono text-xs text-muted-foreground">{s.name}</span>
              </div>
              <code className="font-mono text-xs text-muted-foreground">{s.varName}</code>
            </div>
          ))}
        </div>
      </section>

      <Separator />

      {/* Graph paper ----------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Textures"
          title="Graph paper"
          file="src/styles.css :: .grid-paper"
        />
        <GraphPaperBg className="rounded-lg border border-border">
          <div className="p-12 text-center">
            <MonoLabel className="mb-3 block">--grid-color / --grid-size</MonoLabel>
            <p className="font-display text-xl font-bold">
              Subtle hex honeycomb behind hero content.
            </p>
            <p className="mt-2 text-sm text-muted-foreground">
              Stroke color swaps automatically between light and dark modes.
            </p>
          </div>
        </GraphPaperBg>
      </section>

      <Separator />

      {/* Logos ----------------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Brand"
          title="Logos"
          file="components/brand/{logo,wordmark,lockup}.tsx"
        />
        <p className="max-w-2xl text-sm text-muted-foreground">
          The library ships the <em>pattern</em> (sizing, dark-mode flipping, hover coordination) —
          not the SVG assets themselves. Consumers pass their own mark via{" "}
          <code className="font-mono text-xs">src</code> (URL) or{" "}
          <code className="font-mono text-xs">children</code> (inline SVG). Examples below use the
          canonical Antfly mark served from the playground&apos;s{" "}
          <code className="font-mono text-xs">public/af-logo.svg</code>.
        </p>

        <div className="space-y-3">
          <MonoLabel>Size scale</MonoLabel>
          <div className="flex items-end gap-8 rounded-lg border border-border p-6">
            {(
              [
                { size: "sm" as const, px: "24px", use: "inline, badges" },
                { size: "md" as const, px: "32px", use: "header, footer" },
                { size: "lg" as const, px: "48px", use: "hero lockup" },
                { size: "xl" as const, px: "64px", use: "large marketing" },
              ] as const
            ).map(({ size, px, use }) => (
              <div key={size} className="flex flex-col items-center gap-2">
                <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfly" size={size} />
                <code className="font-mono text-xs text-muted-foreground">
                  {size} · {px}
                </code>
                <span className="text-xs text-muted-foreground">{use}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="space-y-3">
          <MonoLabel>Lockup</MonoLabel>
          <div className="flex flex-wrap items-center gap-10 rounded-lg border border-border p-6">
            <Lockup>
              <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfly" />
              <Wordmark className="text-lg">Antfly</Wordmark>
            </Lockup>
            <Lockup>
              <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="SearchAF" size="lg" />
              <Wordmark className="text-2xl">SearchAF</Wordmark>
            </Lockup>
            <Lockup>
              <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Termite" size="lg" />
              <Wordmark className="text-2xl">Termite</Wordmark>
            </Lockup>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="rounded-lg border border-border p-5">
            <h3 className="mb-2 text-sm font-semibold">Minimum size</h3>
            <p className="text-sm text-muted-foreground">
              Don&apos;t render below <code className="font-mono text-xs">size="sm"</code> (24px).
              Below that, legibility and the dark-mode invert both start to fail. For inline
              contexts smaller than 24px, use a text wordmark instead.
            </p>
          </div>
          <div className="rounded-lg border border-border p-5">
            <h3 className="mb-2 text-sm font-semibold">Clear space</h3>
            <p className="text-sm text-muted-foreground">
              Keep padding equal to <strong>half the logo&apos;s height</strong> clear of other
              content on every side. Lockup&apos;s default{" "}
              <code className="font-mono text-xs">gap-2.5</code> already respects this between mark
              and wordmark.
            </p>
          </div>
          <div className="rounded-lg border border-border p-5">
            <h3 className="mb-2 text-sm font-semibold">Dark mode</h3>
            <p className="text-sm text-muted-foreground">
              Pair assets with <code className="font-mono text-xs">src</code> +{" "}
              <code className="font-mono text-xs">srcDark</code> — the component renders both and
              toggles visibility via the <code className="font-mono text-xs">.dark</code> class.
              Falls back to <code className="font-mono text-xs">dark:brightness-0 dark:invert</code>{" "}
              for single-asset monochrome marks.
            </p>
          </div>
          <div className="rounded-lg border border-border p-5">
            <h3 className="mb-2 text-sm font-semibold">Mark-only vs lockup</h3>
            <p className="text-sm text-muted-foreground">
              Use <strong>mark-only</strong> in dense UI (nav icons, avatars, cramped footers). Use
              the full <strong>lockup</strong> (Logo + Wordmark) anywhere the brand needs to read at
              a glance — headers, marketing heroes, email signatures.
            </p>
          </div>
        </div>
      </section>

      <Separator />

      {/* Density --------------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Density"
          title="data-density scope"
          file="src/styles.css :: [data-density]"
        />
        <p className="max-w-2xl text-sm text-muted-foreground">
          Apply <code className="font-mono text-xs">data-density="compact"</code> or{" "}
          <code className="font-mono text-xs">data-density="comfortable"</code> to any ancestor to
          rescale every spacing utility underneath it. Tailwind v4 compiles{" "}
          <code className="font-mono text-xs">h-*</code>,{" "}
          <code className="font-mono text-xs">p-*</code>,{" "}
          <code className="font-mono text-xs">gap-*</code> to{" "}
          <code className="font-mono text-xs">calc(var(--spacing) * N)</code>, so overriding{" "}
          <code className="font-mono text-xs">--spacing</code> cascades everywhere without any
          component changes. Toggle the top-right density button to apply globally, or scope it on a
          container.
        </p>

        <div className="rounded-lg border border-border bg-muted/30 p-4 text-xs text-muted-foreground">
          <p className="font-mono">
            compact = 0.22rem · default = 0.25rem (Tailwind) · comfortable = 0.3rem
          </p>
        </div>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <DensitySample label="compact" density="compact" />
          <DensitySample label="default" density={undefined} />
          <DensitySample label="comfortable" density="comfortable" />
        </div>
      </section>
    </div>
  );
}

function DensitySample({
  label,
  density,
}: {
  label: string;
  density: "compact" | "comfortable" | undefined;
}) {
  return (
    <div
      data-density={density}
      className="flex h-full flex-col gap-4 rounded-lg border border-border bg-card p-6"
    >
      <MonoLabel>{label}</MonoLabel>
      <Card>
        <CardHeader>
          <CardTitle>Cluster prod-east</CardTitle>
          <CardDescription>us-east-1 · 3 shards</CardDescription>
        </CardHeader>
        <CardContent className="flex gap-2">
          <Button size="sm">Details</Button>
          <Button size="sm" variant="outline">
            Logs
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function SectionHeading({
  eyebrow,
  title,
  file,
}: {
  eyebrow: string;
  title: string;
  file?: string;
}) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-2 border-b border-border pb-2">
      <div>
        <MonoLabel className="mb-2 block">{eyebrow}</MonoLabel>
        <h2 className="font-display text-2xl font-bold tracking-tight">{title}</h2>
      </div>
      {file ? <code className="font-mono text-xs text-muted-foreground">{file}</code> : null}
    </div>
  );
}

/**
 * Side-by-side light + dark color chips. Each chip uses an inline
 * background color rather than CSS vars so both modes always render
 * regardless of the playground's current theme.
 */
function ColorSwatch({ name, varName, light, dark, description }: TokenDef) {
  return (
    <div className="overflow-hidden rounded-lg border border-border">
      <div className="grid grid-cols-2">
        <Chip mode="light" color={light} />
        <Chip mode="dark" color={dark} />
      </div>
      <div className="space-y-1 p-3">
        <p className="text-sm font-medium">{name}</p>
        <code className="block font-mono text-xs text-muted-foreground">{varName}</code>
        <div className="grid grid-cols-2 gap-2 pt-1 text-[11px] text-muted-foreground">
          <code className="font-mono leading-tight">{light}</code>
          <code className="font-mono leading-tight">{dark}</code>
        </div>
        {description ? <p className="pt-1 text-xs text-muted-foreground">{description}</p> : null}
      </div>
    </div>
  );
}

function Chip({ mode, color }: { mode: "light" | "dark"; color: string }) {
  return (
    <div
      className="relative h-20"
      style={{
        backgroundColor: color,
        // Use a paired neutral base so transparent / low-alpha colors
        // read correctly against a surface resembling their intended use.
        backgroundImage:
          mode === "light"
            ? "linear-gradient(oklch(0.98 0.003 265),oklch(0.98 0.003 265))"
            : "linear-gradient(oklch(0.135 0.005 275),oklch(0.135 0.005 275))",
        backgroundBlendMode: "normal",
      }}
    >
      {/* Foreground color layer renders over the paired base so alpha tokens are readable */}
      <div className="absolute inset-0" style={{ backgroundColor: color }} />
      <span
        className="absolute bottom-1 left-2 font-mono text-[10px] uppercase tracking-wider"
        style={{
          color: mode === "light" ? "oklch(0.185 0.02 275)" : "oklch(0.91 0.005 265)",
          opacity: 0.7,
        }}
      >
        {mode}
      </span>
    </div>
  );
}

function TokenScaleSwatch({ scale }: { scale: TokenScale }) {
  return (
    <div className="rounded-lg border border-border p-4">
      <div className="mb-3">
        <p className="text-sm font-medium">{scale.name}</p>
        <p className="text-xs text-muted-foreground">{scale.description}</p>
      </div>
      <div className="space-y-3">
        {(["light", "dark"] as const).map((mode) => (
          <div key={mode}>
            <p className="mb-1 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
              {mode}
            </p>
            <div className="grid grid-cols-5 overflow-hidden rounded-md border border-border">
              {scale.tokens.map((token) => (
                <div
                  key={`${mode}-${token.varName}`}
                  className="h-10"
                  title={`${token.varName}: ${mode === "light" ? token.light : token.dark}`}
                  style={{ backgroundColor: mode === "light" ? token.light : token.dark }}
                />
              ))}
            </div>
          </div>
        ))}
      </div>
      <div className="mt-3 flex flex-wrap gap-1">
        {scale.tokens.map((token) => (
          <code key={token.varName} className="font-mono text-[10px] text-muted-foreground">
            {token.varName}
          </code>
        ))}
      </div>
    </div>
  );
}

function ScaleRow({ name, classes, sample }: { name: string; classes: string; sample: string }) {
  return (
    <div className="space-y-3 border-b border-border pb-6">
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
        <p className="text-sm font-medium">{name}</p>
        <code className="font-mono text-xs text-muted-foreground">{classes}</code>
      </div>
      <h3 className={`${classes} font-display font-bold tracking-tight`}>{sample}</h3>
    </div>
  );
}

function FontSpecimen({
  label,
  varName,
  sample,
  style,
  className,
  caption,
}: {
  label: string;
  varName: string;
  sample: string;
  style?: React.CSSProperties;
  className?: string;
  caption?: string;
}) {
  return (
    <div className="space-y-3 rounded-lg border border-border p-6">
      <div className="flex items-baseline justify-between gap-4">
        <MonoLabel>{label}</MonoLabel>
        <code className="font-mono text-xs text-muted-foreground">{varName}</code>
      </div>
      <p className={className} style={style}>
        {sample}
      </p>
      {caption ? <p className="text-sm text-muted-foreground">{caption}</p> : null}
    </div>
  );
}
