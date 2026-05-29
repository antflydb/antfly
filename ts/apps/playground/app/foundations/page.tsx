import {
  AntyPixel,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  GraphPaperBg,
  Highlight,
  Kicker,
  Lockup,
  Logo,
  Separator,
  TypeOn,
  Wordmark,
} from "@antfly/design-system";

export const metadata = {
  title: "Foundations · @antfly/design-system",
  description:
    "Design tokens: colors, fonts, radii, shadows. Source files live under packages/design-system/src/tokens/.",
};

/**
 * Token specimens. Both light and dark hex values are listed explicitly so
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

const AMBER_RAMP: TokenDef[] = [
  { name: "Amber 50", varName: "--amber-50", light: "#fdf6e3", dark: "#fdf6e3" },
  { name: "Amber 100", varName: "--amber-100", light: "#f9e9bf", dark: "#f9e9bf" },
  { name: "Amber 200", varName: "--amber-200", light: "#f3d488", dark: "#f3d488" },
  { name: "Amber 300", varName: "--amber-300", light: "#ecbc4a", dark: "#ecbc4a" },
  {
    name: "Amber 400",
    varName: "--amber-400",
    light: "#e3a112",
    dark: "#ecb52e",
    description: "Primary fill. Slightly brighter on dark for legibility.",
  },
  {
    name: "Amber 500",
    varName: "--amber-500",
    light: "#c4850b",
    dark: "#c4850b",
    description: "Default border on primary controls — Button, Badge solid.",
  },
  { name: "Amber 600", varName: "--amber-600", light: "#9c6907", dark: "#9c6907" },
  { name: "Amber 700", varName: "--amber-700", light: "#6f4a05", dark: "#6f4a05" },
];

const SEMANTIC_COLORS: TokenDef[] = [
  { name: "Background", varName: "--background", light: "#fafafa", dark: "#121212" },
  {
    name: "Background secondary",
    varName: "--background-secondary",
    light: "#f2f2f1",
    dark: "#1a1a1a",
    description: "Optional app-shell background used by dashboard surfaces.",
  },
  { name: "Foreground", varName: "--foreground", light: "#1b1b1a", dark: "#ededec" },
  { name: "Card", varName: "--card", light: "#ffffff", dark: "#1c1c1c" },
  { name: "Card foreground", varName: "--card-foreground", light: "#1b1b1a", dark: "#ededec" },
  { name: "Popover", varName: "--popover", light: "#ffffff", dark: "#1c1c1c" },
  { name: "Popover foreground", varName: "--popover-foreground", light: "#1b1b1a", dark: "#ededec" },
  {
    name: "Primary",
    varName: "--primary",
    light: "#e3a112",
    dark: "#ecb52e",
    description: "var(--amber-400) — the single accent hue.",
  },
  {
    name: "Primary foreground",
    varName: "--primary-foreground",
    light: "#221a06",
    dark: "#1c1407",
    description: "Ink on amber fills. Never use white on amber.",
  },
  { name: "Secondary", varName: "--secondary", light: "#f2f2f1", dark: "#1a1a1a" },
  { name: "Secondary foreground", varName: "--secondary-foreground", light: "#1b1b1a", dark: "#ededec" },
  { name: "Muted", varName: "--muted", light: "#f2f2f1", dark: "#1a1a1a" },
  { name: "Muted foreground", varName: "--muted-foreground", light: "#54534f", dark: "#b2b1ad" },
  { name: "Accent", varName: "--accent", light: "#f2f2f1", dark: "#1a1a1a" },
  { name: "Accent foreground", varName: "--accent-foreground", light: "#1b1b1a", dark: "#ededec" },
  {
    name: "Destructive",
    varName: "--destructive",
    light: "#b23c34",
    dark: "#d96a61",
    description: "Restrained red — express via border/text, not loud fills.",
  },
  { name: "Success", varName: "--success", light: "#4d7a45", dark: "#6fa362" },
  {
    name: "Warning",
    varName: "--warning",
    light: "#d3641a",
    dark: "#e8854a",
    description: "HOT-ORANGE — deliberately distinct from brand amber.",
  },
  { name: "Info", varName: "--info", light: "#3f6c98", dark: "#6fa0cf" },
  {
    name: "Border",
    varName: "--border",
    light: "#e4e3e0",
    dark: "#2c2c2c",
    description: "Subtle separators — table rows, card-head divider.",
  },
  {
    name: "Border strong",
    varName: "--border-strong",
    light: "#cbcac6",
    dark: "#444443",
    description: "Chassis border — card outer, dialog, badge, button-outline.",
  },
  { name: "Input", varName: "--input", light: "#e4e3e0", dark: "#2c2c2c" },
  { name: "Ring", varName: "--ring", light: "#1b1b1a", dark: "#ededec" },
];

const CHART_COLORS: TokenDef[] = [
  {
    name: "Chart 1",
    varName: "--chart-1",
    light: "#e3a112",
    dark: "#ecb52e",
    description: "var(--primary) — amber accent.",
  },
  {
    name: "Chart 2",
    varName: "--chart-2",
    light: "#54534f",
    dark: "#b2b1ad",
    description: "var(--muted-foreground) — neutral.",
  },
  {
    name: "Chart 3",
    varName: "--chart-3",
    light: "#f3d488",
    dark: "#f3d488",
    description: "var(--amber-200) — soft amber wash.",
  },
  {
    name: "Chart 4",
    varName: "--chart-4",
    light: "#4d7a45",
    dark: "#6fa362",
    description: "var(--success).",
  },
  {
    name: "Chart 5",
    varName: "--chart-5",
    light: "#d3641a",
    dark: "#e8854a",
    description: "var(--warning).",
  },
  {
    name: "Chart 6",
    varName: "--chart-6",
    light: "#3f6c98",
    dark: "#6fa0cf",
    description: "var(--info).",
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
      { varName: "--searchaf-3", light: "#f4f4f4", dark: "#131313" },
      { varName: "--searchaf-4", light: "#e4e4e3", dark: "#262626" },
      { varName: "--searchaf-5", light: "#cbcac6", dark: "#3a3a3a" },
      { varName: "--searchaf-6", light: "#a3a29e", dark: "#525252" },
      { varName: "--searchaf-7", light: "#777572", dark: "#737373" },
      { varName: "--searchaf-8", light: "#555350", dark: "#a1a1a1" },
      { varName: "--searchaf-9", light: "#1f1e1c", dark: "#e5e5e5" },
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
        <Kicker>Design tokens</Kicker>
        <h1 className="font-display text-4xl font-bold tracking-tight">Foundations</h1>
        <p className="max-w-2xl text-muted-foreground">
          Live specimens for every token shipped by{" "}
          <code className="font-mono text-sm">@antfly/design-system</code>. Authoritative sources:{" "}
          <code className="font-mono text-sm">packages/design-system/src/tokens/</code> and{" "}
          <code className="font-mono text-sm">src/styles.css</code>. Colors show both light and dark
          values per swatch.
        </p>
        <p className="max-w-2xl text-sm text-muted-foreground">
          The visual language is <Highlight>square, flat, amber</Highlight> — borders carry
          hierarchy, shadows are neutralized, and a single honey-amber hue is the only accent on a
          neutral paper/ink base.
        </p>
      </header>

      {/* Color: Amber ramp ----------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Color"
          title="Amber ramp — the single accent"
          file="src/tokens/colors.css"
        />
        <p className="max-w-2xl text-sm text-muted-foreground">
          One hue, eight stops. Amber 400 is the primary fill; 500 is the default border on primary
          controls (Button, Badge solid). The ramp is exposed as Tailwind utilities —{" "}
          <code className="font-mono text-xs">bg-amber-300</code>,{" "}
          <code className="font-mono text-xs">border-amber-500</code>,{" "}
          <code className="font-mono text-xs">text-amber-600</code> all work directly.
        </p>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          {AMBER_RAMP.map((c) => (
            <ColorSwatch key={c.varName} {...c} />
          ))}
        </div>
      </section>

      {/* Color: Semantic palette ----------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading eyebrow="Color" title="Semantic palette" file="src/tokens/colors.css" />
        <p className="max-w-2xl text-sm text-muted-foreground">
          Neutral paper/ink base. Note the distinction between{" "}
          <code className="font-mono text-xs">--border</code> (subtle row separators) and{" "}
          <code className="font-mono text-xs">--border-strong</code> (chassis borders — card outer,
          dialog, badge, button-outline).
        </p>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          {SEMANTIC_COLORS.map((c) => (
            <ColorSwatch key={c.varName} {...c} />
          ))}
        </div>
      </section>

      <section className="space-y-6">
        <SectionHeading eyebrow="Color" title="Chart palette" />
        <p className="max-w-2xl text-sm text-muted-foreground">
          Amber leads, then the semantic family. <code className="font-mono text-xs">--chart-2</code>{" "}
          is muted neutral so single-series charts default to ink rather than a competing hue.
        </p>
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
        <p className="max-w-2xl text-sm text-muted-foreground">
          <code className="font-mono text-xs">--searchaf-*</code> and{" "}
          <code className="font-mono text-xs">--gray-*</code> are retained for surfaces still
          migrating. Prefer the semantic tokens (<code className="font-mono text-xs">--foreground</code>
          , <code className="font-mono text-xs">--muted-foreground</code>,{" "}
          <code className="font-mono text-xs">--border-strong</code>) for new work.
        </p>
        <div className="grid gap-4 md:grid-cols-2">
          {COMPATIBILITY_SCALES.map((scale) => (
            <TokenScaleSwatch key={scale.name} scale={scale} />
          ))}
        </div>
      </section>

      <Separator />

      {/* Typography: registers ------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Typography"
          title="Three registers + decision rule"
          file="TYPOGRAPHY.md"
        />
        <Card>
          <CardContent className="space-y-4">
            <p className="text-sm">
              <Highlight>The rule.</Highlight> Is this content read as a label/identifier or as a
              phrase?
            </p>
            <ul className="space-y-2 text-sm text-foreground">
              <li>
                <strong className="font-medium">Mono</strong> if it's a label, ID, value, kind, or
                anything that's part of the instrument chassis (menu item, accordion trigger,
                button, badge, tab).
              </li>
              <li>
                <strong className="font-medium">Inter</strong> if it's a heading or sentence the user
                reads as a phrase (Card/Dialog/Sheet titles, page H1/H2, paragraphs, descriptions).
              </li>
              <li>
                <strong className="font-medium">Aeonik</strong> only when it's a brand moment
                (marketing hero, wordmark). Using it elsewhere dilutes the moment.
              </li>
            </ul>
            <p className="text-sm text-muted-foreground">
              <Highlight>Chrome beats content shape.</Highlight> Menu items and accordion triggers
              stay mono even when their text reads as a sentence — the chassis voice wins. See{" "}
              <code className="font-mono text-xs">TYPOGRAPHY.md</code> for the full breakdown,
              including tracking conventions and the per-component reference table.
            </p>
          </CardContent>
        </Card>
      </section>

      {/* Typography: font families --------------------------------------- */}
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
          caption="Aeonik Bold — reserved for marketing hero and wordmarks. Not for in-product headings."
        />

        <FontSpecimen
          label="Sans (body)"
          varName="--font-sans"
          sample="Hybrid search. Local ML inference. Multimodal documents."
          style={{ fontFamily: "var(--font-sans)" }}
          className="text-xl"
          caption="Inter — body text, descriptions, Card/Dialog/Sheet titles. Consumer-loaded webfont."
        />

        <FontSpecimen
          label="Mono (instrument)"
          varName="--font-mono"
          sample="shard-0a1f · healthy · p99 ms 4.21"
          style={{ fontFamily: "var(--font-mono)" }}
          className="text-base"
          caption="Roboto Mono — buttons, menus, badges, table heads, IDs, values, anything chassis."
        />

        <FontSpecimen
          label="Pixel (loud brand)"
          varName="--font-pixel"
          sample="BUILT FOR ANSWER ENGINES"
          style={{ fontFamily: "var(--font-pixel)", letterSpacing: "0.04em" }}
          className="text-base"
          caption="Silkscreen — pixel-font kicker for loud brand moments only. Used by <Kicker>."
        />
      </section>

      {/* Typography: tracking / weight ----------------------------------- */}
      <section className="space-y-6">
        <SectionHeading eyebrow="Typography" title="Tracking and weight" />
        <Card>
          <CardContent className="space-y-3 text-sm">
            <p>
              <strong className="font-medium">Buttons get zero letter-spacing.</strong> Earlier
              tracked specs felt exaggerated — keep buttons tight.
            </p>
            <p>
              Mono UPPERCASE kickers (form labels, table headers, card-head labels, dropdown labels,
              tab labels): <code className="font-mono text-xs">tracking-[0.1em]</code>.
            </p>
            <p>
              Mono UPPERCASE callouts (badges, alert titles — shorter and prouder):{" "}
              <code className="font-mono text-xs">tracking-[0.05em]</code>.
            </p>
            <p>
              Mono readouts (sentence-case — menu items, button text, IDs, values):{" "}
              <code className="font-mono text-xs">tracking-[0]</code>.
            </p>
            <p>
              Inter headings stay <code className="font-mono text-xs">font-medium</code>, not
              semibold. Restraint is part of the voice.
            </p>
          </CardContent>
        </Card>
      </section>

      <section className="space-y-4">
        <SectionHeading eyebrow="Typography" title="Heading elements" />
        <div className="border-[1.5px] border-border-strong bg-muted/30 p-4 text-sm text-muted-foreground">
          Tailwind's preflight resets <code className="font-mono text-xs">h1</code>&ndash;
          <code className="font-mono text-xs">h6</code> to inherit font-size. Our base styles apply
          Aeonik / bold / tight tracking to every heading automatically, but{" "}
          <strong>size is applied per-usage</strong> with a Tailwind utility. Raw headings therefore
          all render at body size.
        </div>

        <div className="space-y-3">
          <Kicker>Raw — no size utility</Kicker>
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
          The sizes the library's own <code className="font-mono text-xs">Hero</code>,{" "}
          <code className="font-mono text-xs">CTA</code>, and{" "}
          <code className="font-mono text-xs">PageHeader</code> components apply.
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
          <ScaleRow
            name="Card title"
            classes="text-base font-medium"
            sample="Hybrid Search"
            font="sans"
          />
        </div>
      </section>

      <Separator />

      {/* Radii ------------------------------------------------------------ */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Shape"
          title="Radii — zero everywhere"
          file="src/tokens/radii.css"
        />
        <div className="border-l-4 border-l-foreground bg-card p-4 text-sm">
          <strong className="block font-mono text-[12px] uppercase tracking-[0.06em]">
            By design
          </strong>
          <p className="mt-1 text-muted-foreground">
            All <code className="font-mono text-xs">--radius-*</code> tokens are{" "}
            <code className="font-mono text-xs">0</code>. The design language is square — borders
            carry hierarchy instead of rounded corners. The tokens remain for backward compatibility
            (consumers using <code className="font-mono text-xs">rounded-[var(--radius)]</code> get
            square corners automatically). Don't introduce new rounded utilities in components.
          </p>
        </div>
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
          {RADII.map((r) => (
            <div key={r.varName} className="flex flex-col items-center gap-3">
              <div
                className="size-24 border-[1.5px] border-border-strong bg-muted"
                style={{ borderRadius: `var(${r.varName})` }}
              />
              <div className="text-center">
                <p className="font-medium">{r.name}</p>
                <code className="font-mono text-xs text-muted-foreground">{r.varName}</code>
                <code className="block font-mono text-[10px] text-muted-foreground">= 0</code>
              </div>
            </div>
          ))}
        </div>
      </section>

      <Separator />

      {/* Shadows --------------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Elevation"
          title="Shadows — neutralized"
          file="src/tokens/shadows.css"
        />
        <div className="border-l-4 border-l-foreground bg-card p-4 text-sm">
          <strong className="block font-mono text-[12px] uppercase tracking-[0.06em]">
            By design
          </strong>
          <p className="mt-1 text-muted-foreground">
            All <code className="font-mono text-xs">--shadow-*</code> tokens resolve to{" "}
            <code className="font-mono text-xs">0 0 #0000</code>. The chassis is flat — borders
            (especially <code className="font-mono text-xs">--border-strong</code>) carry the
            visual hierarchy. Existing consumers using{" "}
            <code className="font-mono text-xs">shadow-md</code> classes will get no shadow rather
            than a broken cascade. Don't add new shadow utilities.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-5">
          {SHADOWS.map((s) => (
            <div key={s.varName} className="flex flex-col items-center gap-3">
              <div
                className="flex size-28 items-center justify-center border-[1.5px] border-border-strong bg-card"
                style={{ boxShadow: `var(${s.varName})` }}
              >
                <span className="font-mono text-xs text-muted-foreground">{s.name}</span>
              </div>
              <code className="font-mono text-xs text-muted-foreground">{s.varName}</code>
              <code className="font-mono text-[10px] text-muted-foreground">= 0 0 #0000</code>
            </div>
          ))}
        </div>
      </section>

      <Separator />

      {/* Brand register: Kicker / Highlight / TypeOn --------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Brand"
          title="Brand register"
          file="components/brand/{kicker,highlight,type-on}.tsx"
        />
        <p className="max-w-2xl text-sm text-muted-foreground">
          Three primitives for brand moments. Use sparingly — these are loud, and overuse drains
          their effect. Reserved for hero / marketing surfaces.
        </p>

        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Kicker</CardTitle>
              <CardDescription>
                Silkscreen pixel overline for the loudest brand kickers. Amber 600 on light, amber
                400 on dark.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Kicker>Built for answer engines</Kicker>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Highlight</CardTitle>
              <CardDescription>
                Amber-fill inline marker for accent words inside headlines. Amber is a fill with
                ink-on-amber text, not amber text on paper.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-xl font-display font-bold tracking-tight">
                Hybrid <Highlight>search</Highlight> meets vector recall.
              </p>
            </CardContent>
          </Card>

          <Card className="md:col-span-2">
            <CardHeader>
              <CardTitle>TypeOn</CardTitle>
              <CardDescription>
                Mono steps() typewriter for brand taglines. The reveal target is computed from text
                length in <code className="font-mono text-xs">ch</code> units; the amber caret
                blinks until the line finishes typing.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <TypeOn text="curl -sSL https://antfly.io/install | sh" />
            </CardContent>
          </Card>
        </div>
      </section>

      <Separator />

      {/* Brand: AntyPixel ------------------------------------------------- */}
      <section className="space-y-6">
        <SectionHeading
          eyebrow="Brand"
          title="AntyPixel"
          file="components/brand/anty/anty-pixel.tsx"
        />
        <p className="max-w-2xl text-sm text-muted-foreground">
          Pixel-sprite Anty mark for loud brand chrome. Two variants: <strong>square</strong>{" "}
          (12×12 resolution, sharp notches at top-right and bottom-left) and{" "}
          <strong>diagonal</strong> (24×24 with stair-step notches). Amber halo glow, stepped eye
          blink. Pair with Kicker; never use inside in-product chrome.
        </p>

        <div className="grid gap-6 md:grid-cols-2">
          <div className="space-y-4 border-[1.5px] border-border-strong bg-card p-6">
            <Kicker>Square — 12×12</Kicker>
            <div className="flex items-end gap-6">
              {(["sm", "md", "lg", "xl"] as const).map((s) => (
                <div key={s} className="flex flex-col items-center gap-2">
                  <AntyPixel variant="square" size={s} />
                  <code className="font-mono text-[10px] text-muted-foreground">{s}</code>
                </div>
              ))}
            </div>
          </div>

          <div className="space-y-4 border-[1.5px] border-border-strong bg-card p-6">
            <Kicker>Diagonal — 24×24 stair-step</Kicker>
            <div className="flex items-end gap-6">
              {(["sm", "md", "lg", "xl"] as const).map((s) => (
                <div key={s} className="flex flex-col items-center gap-2">
                  <AntyPixel variant="diagonal" size={s} />
                  <code className="font-mono text-[10px] text-muted-foreground">{s}</code>
                </div>
              ))}
            </div>
          </div>
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
        <GraphPaperBg className="border-[1.5px] border-border-strong">
          <div className="p-12 text-center">
            <Kicker>--grid-color · --grid-size</Kicker>
            <p className="mt-3 font-display text-xl font-bold">
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
          canonical Antfly mark served from the playground's{" "}
          <code className="font-mono text-xs">public/af-logo.svg</code>.
        </p>

        <div className="space-y-3">
          <Kicker>Size scale</Kicker>
          <div className="flex items-end gap-8 border-[1.5px] border-border-strong p-6">
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
          <Kicker>Lockup</Kicker>
          <div className="flex flex-wrap items-center gap-10 border-[1.5px] border-border-strong p-6">
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
          <Card>
            <CardHeader>
              <CardTitle>Minimum size</CardTitle>
              <CardDescription>
                Don't render below <code className="font-mono text-xs">size="sm"</code> (24px).
                Below that, legibility and the dark-mode invert both start to fail. For inline
                contexts smaller than 24px, use a text wordmark instead.
              </CardDescription>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Clear space</CardTitle>
              <CardDescription>
                Keep padding equal to <strong>half the logo's height</strong> clear of other content
                on every side. Lockup's default <code className="font-mono text-xs">gap-2.5</code>{" "}
                already respects this between mark and wordmark.
              </CardDescription>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Dark mode</CardTitle>
              <CardDescription>
                Pair assets with <code className="font-mono text-xs">src</code> +{" "}
                <code className="font-mono text-xs">srcDark</code> — the component renders both and
                toggles visibility via the <code className="font-mono text-xs">.dark</code> class.
                Falls back to{" "}
                <code className="font-mono text-xs">dark:brightness-0 dark:invert</code> for
                single-asset monochrome marks.
              </CardDescription>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Mark-only vs lockup</CardTitle>
              <CardDescription>
                Use <strong>mark-only</strong> in dense UI (nav icons, avatars, cramped footers).
                Use the full <strong>lockup</strong> (Logo + Wordmark) anywhere the brand needs to
                read at a glance — headers, marketing heroes, email signatures.
              </CardDescription>
            </CardHeader>
          </Card>
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

        <div className="border-[1.5px] border-border-strong bg-muted/30 p-4 text-xs text-muted-foreground">
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
      className="flex h-full flex-col gap-4 border-[1.5px] border-border-strong bg-card p-6"
    >
      <Kicker>{label}</Kicker>
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
    <div className="flex flex-wrap items-end justify-between gap-2 border-b-[1.5px] border-border-strong pb-2">
      <div>
        <Kicker>{eyebrow}</Kicker>
        <h2 className="mt-2 font-display text-2xl font-bold tracking-tight">{title}</h2>
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
    <div className="overflow-hidden border-[1.5px] border-border-strong">
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
        backgroundImage:
          mode === "light"
            ? "linear-gradient(#fafafa,#fafafa)"
            : "linear-gradient(#121212,#121212)",
        backgroundBlendMode: "normal",
      }}
    >
      {/* Foreground color layer renders over the paired base so alpha tokens are readable */}
      <div className="absolute inset-0" style={{ backgroundColor: color }} />
      <span
        className="absolute bottom-1 left-2 font-mono text-[10px] uppercase tracking-wider"
        style={{
          color: mode === "light" ? "#1b1b1a" : "#ededec",
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
    <div className="border-[1.5px] border-border-strong p-4">
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
            <div className="grid grid-cols-5 overflow-hidden border border-border">
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

function ScaleRow({
  name,
  classes,
  sample,
  font = "display",
}: {
  name: string;
  classes: string;
  sample: string;
  font?: "display" | "sans";
}) {
  return (
    <div className="space-y-3 border-b border-border pb-6">
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
        <p className="text-sm font-medium">{name}</p>
        <code className="font-mono text-xs text-muted-foreground">{classes}</code>
      </div>
      <h3
        className={`${classes} ${
          font === "display" ? "font-display font-bold tracking-tight" : "text-foreground"
        }`}
      >
        {sample}
      </h3>
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
    <div className="space-y-3 border-[1.5px] border-border-strong p-6">
      <div className="flex items-baseline justify-between gap-4">
        <Kicker>{label}</Kicker>
        <code className="font-mono text-xs text-muted-foreground">{varName}</code>
      </div>
      <p className={className} style={style}>
        {sample}
      </p>
      {caption ? <p className="text-sm text-muted-foreground">{caption}</p> : null}
    </div>
  );
}
