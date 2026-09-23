import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@antfly/design-system";
import { ArrowRight } from "lucide-react";
import Link from "next/link";
import { SpineStrip } from "@/components/spine-strip";
import { manifest } from "@/lib/data";

const MODEL_CARDS = [
  {
    slug: "gemma4-e4b",
    name: "Gemma4 E2B / E4B",
    kind: "Generative LLM · deep dive",
    hook: "Per-layer embeddings, variant-specific sliding-window attention, shared KV, and Metal decode frames. Related family chapters explain MoE and optional MTP.",
    badges: ["PLE", "iSWA", "shared KV", "Q4_0"],
    accent: "var(--kfam-matvec)",
  },
  {
    slug: "gliner2",
    name: "GLiNER2",
    kind: "Schema-driven extraction",
    hook: "A DeBERTa-v3 encoder with disentangled relative attention and a schema-conditioned head that scores candidate entity spans.",
    badges: ["DeBERTa", "C2P/P2C", "span head"],
    accent: "var(--kfam-attention)",
  },
  {
    slug: "gliner25",
    name: "GLiNER2.5",
    kind: "Schema-driven extraction · boundary head",
    hook: "The same DeBERTa-v3 encoder with a new head: boundary proposals, a shared FiLM-scored candidate pool, abstention and count calibration — recognized by the runtime, serving gated behind qualification.",
    badges: ["boundary head", "shared pool 192", "windowed long docs", "not yet servable"],
    accent: "var(--kfam-fusion)",
  },
  {
    slug: "qwen3-embedding",
    name: "Qwen3 Embedding",
    kind: "Text embeddings",
    hook: "A causal transformer used as an encoder: query instructions, last-token pooling, and normalized vectors. Follow the 0.6B graph and its configured context limit.",
    badges: ["last-token pool", "32k ctx (8k qualified)", "Q8_0"],
    accent: "var(--kfam-sampling)",
  },
  {
    slug: "qwen3-vl",
    name: "Qwen3-VL",
    kind: "Vision-language",
    hook: "Pixels become tokens: Conv3D patch embedding, a 2×2 merger, DeepStack feature taps, and three-axis m-RoPE feeding the same decoder runtime.",
    badges: ["vision tower", "m-RoPE", "DeepStack"],
    accent: "var(--kfam-moe)",
  },
];

const SECTION_LINKS = [
  {
    href: "/runtime",
    name: "The runtime spine",
    hook: "One request, end to end: the server, session, tokenizer, graph, frames, kernels, KV and sampler every model shares.",
  },
  {
    href: "/explore/gemma4-e4b",
    name: "Operation explorer",
    hook: "Every model's graph as an interactive DAG — click any node for its shapes, kernels and source.",
  },
  {
    href: "/legend",
    name: "Visual legend",
    hook: "The shapes, colours and chips these diagrams use, and what each one is claiming.",
  },
];

const INDEX_TILES = [
  { label: "graph op kinds", value: "opKinds", href: "/runtime#graph" },
  { label: "Metal kernels", value: "kernels", href: "/systems/kernels" },
  { label: "compiled kernel routes", value: "routes", href: "/systems/kernels" },
] as const;

export default function HomePage() {
  return (
    <div className="mx-auto max-w-7xl px-4 py-14">
      <section className="mx-auto max-w-3xl text-center">
        <h1 className="text-4xl font-bold tracking-tight sm:text-5xl">
          How Antfly runs models<span className="text-primary">.</span>
        </h1>
        <p className="mt-4 text-lg text-muted-foreground">
          An interactive tour of the Zig inference runtime — from an HTTP request to a sampled
          token, kernel by kernel. Every diagram links back to the exact line of source it
          describes.
        </p>
        <div className="mt-6 flex flex-wrap items-center justify-center gap-2 font-mono text-xs text-muted-foreground">
          <span className="rounded-full border bg-muted/30 px-3 py-1 tabular-nums">
            {manifest.counts.kernels} Metal kernels
          </span>
          <span className="rounded-full border bg-muted/30 px-3 py-1 tabular-nums">
            {manifest.counts.opKinds} graph ops
          </span>
          <span className="rounded-full border bg-muted/30 px-3 py-1 tabular-nums">
            {manifest.counts.routes} quant routes
          </span>
          <span className="rounded-full border bg-muted/30 px-3 py-1">
            pinned to <code>{manifest.gitCommit.slice(0, 10)}</code>
          </span>
        </div>
        <div className="mt-8 flex justify-center">
          <SpineStrip />
        </div>
      </section>

      <section className="mt-14">
        <div className="mb-4 flex items-baseline justify-between">
          <h2 className="font-mono text-xs uppercase tracking-wider text-muted-foreground">
            The models
          </h2>
          <span className="font-mono text-xs text-muted-foreground">
            {MODEL_CARDS.length} families · 6 pages
          </span>
        </div>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {MODEL_CARDS.map((m) => (
            <Link key={m.slug} href={`/models/${m.slug}`} className="group">
              <Card className="relative h-full overflow-hidden transition-all group-hover:-translate-y-0.5 group-hover:border-primary/40 group-hover:shadow-md">
                <div
                  className="absolute inset-x-0 top-0 h-1"
                  style={{ background: m.accent, opacity: 0.75 }}
                />
                <CardHeader>
                  <CardDescription className="flex items-center gap-1.5 font-mono text-[11px] uppercase tracking-wider">
                    <span
                      className="inline-block size-1.5 rounded-full"
                      style={{ background: m.accent }}
                    />
                    {m.kind}
                  </CardDescription>
                  <CardTitle className="flex items-center gap-2">
                    {m.name}
                    <ArrowRight className="size-4 opacity-0 transition-all group-hover:translate-x-0.5 group-hover:opacity-100" />
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm text-muted-foreground">{m.hook}</p>
                  <div className="flex flex-wrap gap-1.5">
                    {m.badges.map((b) => (
                      <Badge key={b} className="font-mono text-xs font-normal">
                        {b}
                      </Badge>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>
      </section>

      <section className="mt-14">
        <h2 className="mb-4 font-mono text-xs uppercase tracking-wider text-muted-foreground">
          Training
        </h2>
        <Link href="/training" className="group block">
          <Card className="relative overflow-hidden transition-all group-hover:-translate-y-0.5 group-hover:border-primary/40 group-hover:shadow-md">
            <div
              className="absolute inset-x-0 top-0 h-1"
              style={{ background: "var(--kfam-sampling)", opacity: 0.75 }}
            />
            <CardHeader>
              <CardDescription className="font-mono text-[11px] uppercase tracking-wider">
                how these models learn
              </CardDescription>
              <CardTitle className="flex items-center gap-2">
                Finetuning &amp; preference optimization
                <ArrowRight className="size-4 opacity-0 transition-all group-hover:translate-x-0.5 group-hover:opacity-100" />
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <p className="text-sm text-muted-foreground">
                The same graph autodiff that runs inference also differentiates it. Follow DPO and
                GRPO through Gemma4's CPU recipe trainer, GLiNER2's graph-first flag CLI, and
                GLiNER2.5's contract-first one-shot job — seeds, receipts, and honest boundaries
                included.
              </p>
              <div className="flex flex-wrap gap-1.5">
                {["DPO · GRPO", "custom VJPs", "LoRA + DoRA", "deterministic"].map((b) => (
                  <Badge key={b} className="font-mono text-xs font-normal">
                    {b}
                  </Badge>
                ))}
              </div>
            </CardContent>
          </Card>
        </Link>
      </section>

      <section className="mt-14">
        <h2 className="mb-4 font-mono text-xs uppercase tracking-wider text-muted-foreground">
          The source index
        </h2>
        <div className="grid gap-4 text-center sm:grid-cols-2 lg:grid-cols-4">
          {INDEX_TILES.map((s) => (
            <Link
              key={s.label}
              href={s.href}
              className="group rounded-lg border p-6 transition-all hover:-translate-y-0.5 hover:border-primary/40 hover:shadow-md"
            >
              <div className="font-mono text-3xl font-bold tabular-nums">
                {manifest.counts[s.value].toLocaleString("en-US")}
              </div>
              <div className="mt-1 flex items-center justify-center gap-1 text-sm text-muted-foreground">
                {s.label}
                <ArrowRight className="size-3 opacity-0 transition-opacity group-hover:opacity-100" />
              </div>
            </Link>
          ))}
        </div>
        <p className="mt-4 text-center font-mono text-[11px] text-muted-foreground">
          Curated teaching diagrams, not traces of a running model — every source link is pinned to
          commit <code>{manifest.gitCommit.slice(0, 10)}</code>.
        </p>
      </section>

      <section className="mt-14">
        <h2 className="mb-4 font-mono text-xs uppercase tracking-wider text-muted-foreground">
          Also on this site
        </h2>
        <div className="grid gap-4 sm:grid-cols-3">
          {SECTION_LINKS.map((s) => (
            <Link
              key={s.href}
              href={s.href}
              className="group rounded-lg border p-4 transition-all hover:-translate-y-0.5 hover:border-primary/40 hover:shadow-md"
            >
              <div className="flex items-center gap-1.5 font-medium">
                {s.name}
                <ArrowRight className="size-3.5 opacity-0 transition-all group-hover:translate-x-0.5 group-hover:opacity-100" />
              </div>
              <p className="mt-1 text-sm text-muted-foreground">{s.hook}</p>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}
