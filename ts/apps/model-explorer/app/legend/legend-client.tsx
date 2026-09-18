"use client";

import {
  EnvFlagChip,
  FusionChip,
  OpKindBadge,
  QuantChip,
  TensorShapeBadge,
} from "@/components/primitives/chips";
import { Divergence } from "@/components/scrollytelling/scrolly";
import {
  ActivationGlyph,
  AttentionGlyph,
  ElementwiseGlyph,
  EmbeddingGlyph,
  Figure,
  ForkGlyph,
  KvBlockGlyph,
  MatmulGlyph,
  NormGlyph,
  SamplerGlyph,
  WeightGlyph,
} from "@/components/viz/glyphs";

const DTYPE_RAMP = [
  ["f32", "var(--dtype-f32)", "full precision"],
  ["f16 / bf16", "var(--dtype-f16)", "half precision"],
  ["q8", "var(--dtype-q8)", "8-bit blocks"],
  ["q4–q6 / iq4 / mxfp4 / polar4", "var(--dtype-q4)", "4–6-bit payloads; metadata adds overhead"],
  ["sub-4-bit (tl1/tl2, turbo3)", "var(--dtype-sub4)", "ternary / 3-bit key payloads"],
  [
    "unknown / integer / route-dependent",
    "var(--muted-foreground)",
    "no floating precision implied",
  ],
] as const;

const KERNEL_FAMILIES = [
  ["matvec", "var(--kfam-matvec)", "matrix-vector routes, often used for decode"],
  ["mm_sg", "var(--kfam-mmsg)", "simdgroup matrix multiply; thresholds vary by route"],
  ["attention", "var(--kfam-attention)", "flash / paged / disentangled attention"],
  ["fusion", "var(--kfam-fusion)", "pair-fusion, norm⋄rope, fused epilogues"],
  ["moe", "var(--kfam-moe)", "expert routing, scatter, slot arena"],
  ["sampling", "var(--kfam-sampling)", "token selection and candidate reduction routes"],
  ["kv", "var(--kfam-kv)", "KV seed / compress (polar4, turbo3)"],
  [
    "gliner",
    "var(--kfam-attention)",
    "DeBERTa / GLiNER span and boundary-head kernels (shares the attention hue)",
  ],
  [
    "vision",
    "var(--kfam-mmsg)",
    "vision tower: patchify, window pack, conv (shares the mm_sg hue)",
  ],
  ["norm_rope", "var(--kfam-fusion)", "standalone norm / RoPE kernels (shares the fusion hue)"],
  [
    "training",
    "var(--muted-foreground)",
    "backward passes, optimizers, gradient utilities — explained in the Training section",
  ],
  [
    "data_movement",
    "var(--muted-foreground)",
    "copies, transposes, dtype conversion, embedding gathers",
  ],
  ["other", "var(--muted-foreground)", "everything the family rules don't claim"],
] as const;

function Swatch({ color, label, note }: { color: string; label: string; note: string }) {
  return (
    <div className="flex items-center gap-3">
      <span className="size-5 shrink-0 rounded" style={{ background: color }} />
      <span className="w-56 font-mono text-sm">{label}</span>
      <span className="text-sm text-muted-foreground">{note}</span>
    </div>
  );
}

export function LegendClient() {
  return (
    <div className="mx-auto max-w-4xl space-y-12 px-4 py-10">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Visual legend</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          A guide to the explorer's schematic shapes and palettes. The operation explorer can color
          nodes by activation dtype, illustrated backend, or operation family. Chapter illustrations
          also use colors to distinguish functional stages; their captions define the context.
          Decoder models flow through embedding → decoder → KV → sampling stages; encoder models
          (GLiNER2, GLiNER2.5, Qwen3 Embedding) end in head and pooling stages instead — no KV
          cache, no sampler.
        </p>
      </header>

      <section id="shapes">
        <h2 className="mb-4 text-xl font-semibold">Shapes</h2>
        <div className="rounded-lg border bg-card p-4">
          <Figure viewBox="0 0 760 260">
            <ActivationGlyph
              x={20}
              y={30}
              label="activation tensor"
              sublabel="schematic dimensions"
              dtype="f16"
            />
            <WeightGlyph x={170} y={30} label="stored weight tensor" quant="q4_0" />
            <MatmulGlyph x={320} y={28} label="matmul / linear" dtype="f16" />
            <AttentionGlyph x={490} y={24} label="attention" dtype="f16" />
            <AttentionGlyph x={620} y={24} label="+ sliding window" dtype="f16" shutter />

            <NormGlyph x={20} y={120} label="norm (small on purpose)" dtype="f32" />
            <ElementwiseGlyph x={170} y={108} label="elementwise" dtype="f16" />
            <EmbeddingGlyph x={260} y={100} label="embedding lookup" dtype="f16" />
            <ForkGlyph x={420} y={100} label="routing (MoE / MTP)" dtype="q4_0" />
            <SamplerGlyph x={560} y={100} label="sampling" dtype="f32" />
            <MatmulGlyph
              x={620}
              y={96}
              w={120}
              label="fused (zipper)"
              fused={["gate", "up", "silu"]}
              dtype="q4_0"
            />

            <g transform="translate(20, 190)">
              <KvBlockGlyph x={0} y={0} state="filled" />
              <KvBlockGlyph x={20} y={0} state="evicted" />
              <KvBlockGlyph x={40} y={0} state="shared" />
              <KvBlockGlyph x={60} y={0} state="empty" />
              <text x={85} y={11} fontSize={10} className="fill-muted-foreground font-mono">
                KV blocks: filled · evicted (hatched) · shared (split) · free
              </text>
            </g>
          </Figure>
        </div>
        <p className="mt-2 text-sm text-muted-foreground">
          In the operation explorer, a dashed node outline marks the illustrated native CPU path; a
          solid outline is used for the other backend labels. In chapter drawings, dashed lines may
          instead mark boundaries, reuse, or optional paths. A thick lower edge identifies a weight
          tensor and does not imply a disk read. Shapes and widths are schematic unless a caption
          gives a measurement.
        </p>
      </section>

      <section id="colors">
        <h2 className="mb-4 text-xl font-semibold">Color axes</h2>
        <div className="grid gap-8 md:grid-cols-2">
          <div>
            <h3 className="mb-3 font-mono text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              colorBy: dtype (default)
            </h3>
            <div className="space-y-2">
              {DTYPE_RAMP.map(([label, color, note]) => (
                <Swatch key={label} color={color} label={label} note={note} />
              ))}
            </div>
          </div>
          <div>
            <h3 className="mb-3 font-mono text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              colorBy: kernel family
            </h3>
            <div className="space-y-2">
              {KERNEL_FAMILIES.map(([label, color, note]) => (
                <Swatch key={label} color={color} label={label} note={note} />
              ))}
            </div>
          </div>
        </div>
      </section>

      <section id="chips">
        <h2 className="mb-4 text-xl font-semibold">Chips</h2>
        <div className="flex flex-wrap items-center gap-3 rounded-lg border bg-card p-4">
          <QuantChip format="q4_k" />
          <QuantChip format="q8_0" />
          <QuantChip format="iq4_xs" />
          <TensorShapeBadge shape={{ dims: ["B", "T", 2048], dtype: "f16" }} />
          <OpKindBadge opKind="dot_general" group="primitive" />
          <OpKindBadge opKind="gqa_paged_attention" group="fused" />
          <FusionChip ops={["head_rms", "rope"]} />
          <EnvFlagChip name="TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME" defaultOn />
          <EnvFlagChip name="TERMITE_METAL_DISABLE_A4B_ZERO_BIAS_ELISION" defaultOn={false} />
        </div>
        <p className="mt-2 text-sm text-muted-foreground">
          Quant chips group storage formats by payload precision; scale and block metadata add
          overhead. Polar4 specifically stores 4-bit keys with INT8 values plus scales. Activation
          dtype is separate from weight format. Where shown, an environment flag's dot is a curated
          default annotation; device, shape, and policy checks can still constrain eligibility. File
          chips like <code className="font-mono text-xs">gpt.zig:1039</code> open a commit-pinned
          GitHub permalink and show a code peek on hover or keyboard focus.
        </p>
      </section>

      <section id="divergences">
        <h2 className="mb-4 text-xl font-semibold">Implementation details</h2>
        <p className="mb-3 max-w-2xl text-sm text-muted-foreground">
          These callouts compare a conceptual baseline with the Antfly route being explained. They
          do not establish how every other runtime behaves or imply that a capability applies to
          every backend and request. For example:
        </p>
        <Divergence
          others={<p>A host sampler reads logits and applies request constraints on the CPU.</p>}
          antfly={
            <p>
              Eligible device routes select a token without full-logit readback; unsupported cases
              use the host sampler. Greedy Q4_K/Q6_K candidate refinement is a separate opt-in path.
            </p>
          }
          link={
            <a className="text-primary underline" href="/models/gemma4-e4b#ch-10">
              Gemma sampling routes and implementation links
            </a>
          }
        />
      </section>
    </div>
  );
}
