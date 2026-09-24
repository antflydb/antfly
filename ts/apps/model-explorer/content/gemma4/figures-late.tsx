"use client";

/**
 * Bespoke figures for Gemma4 chapters 8–11 (kernel compiler, LM head,
 * device-resident sampling, MTP).
 */
import {
  ActivationGlyph,
  Figure,
  FlowArrow,
  KvBlockGlyph,
  MatmulGlyph,
  SamplerGlyph,
  WeightGlyph,
} from "@/components/viz/glyphs";
import type { KernelCensus } from "@/content/registry";
import type { KernelRoute } from "@/lib/schema";

/* ------------------------------------------------------------------ */
/* Ch 8 — kernels are compiled, not written                            */
/* ------------------------------------------------------------------ */

export function CompilerPipelineFigure({ routedSourceFiles }: { routedSourceFiles: number }) {
  const stages = [
    { label: "schedule table", sub: "format × row_bucket × epilogue", color: "var(--kfam-matvec)" },
    { label: "renderer", sub: "one MSL skeleton", color: "var(--kfam-fusion)" },
    {
      label: "generated .metal",
      sub: `${routedSourceFiles} routed source files`,
      color: "var(--kfam-mmsg)",
    },
  ];
  return (
    <Figure
      viewBox="0 0 480 230"
      title="the build-time kernel pipeline"
      caption="Static generation: zig build quant-kernel-codegen -- --check detects source drift. The optional runtime JIT is a separate subsystem."
    >
      {stages.map((s, i) => {
        const x = 20 + i * 160;
        return (
          <g key={s.label}>
            <rect
              x={x}
              y={70}
              width={140}
              height={54}
              rx={6}
              fill={`color-mix(in oklch, ${s.color} 14%, transparent)`}
              stroke={s.color}
              strokeWidth={1.5}
            />
            <text
              x={x + 70}
              y={91}
              textAnchor="middle"
              fontSize={10}
              className="fill-foreground font-mono"
            >
              {s.label}
            </text>
            <text
              x={x + 70}
              y={107}
              textAnchor="middle"
              fontSize={8}
              className="fill-muted-foreground font-mono"
            >
              {s.sub}
            </text>
            {i < stages.length - 1 && <FlowArrow x1={x + 142} y1={97} x2={x + 158} y2={97} />}
          </g>
        );
      })}
      {/* stack of generated files behind the last box */}
      {[1, 2].map((d) => (
        <rect
          key={d}
          x={340 + d * 4}
          y={70 - d * 4}
          width={140}
          height={54}
          rx={6}
          fill="none"
          stroke="var(--kfam-mmsg)"
          strokeWidth={0.75}
          opacity={0.4}
        />
      ))}
      <text x={240} y={170} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        zig build quant-kernel-codegen -- --check
      </text>
      <text x={240} y={188} textAnchor="middle" fontSize={9} className="fill-primary font-mono">
        byte-identical on regen, enforced
      </text>
    </Figure>
  );
}

export function ScheduleRowFigure({ route }: { route?: KernelRoute }) {
  const s = route?.schedule;
  const fields = [
    { k: "threads/threadgroup", v: s?.threadsPerThreadgroup ?? 128 },
    { k: "cols/threadgroup", v: s?.colsPerThreadgroup ?? 16 },
    { k: "rows/threadgroup", v: s?.rowsPerThreadgroup ?? 2 },
    { k: "reduction", v: s?.reduction ?? "simdgroup_tiled" },
  ];
  return (
    <Figure
      viewBox="0 0 480 220"
      title={`one schedule row: ${route?.id ?? "q4_k/rows_2_8/none"}`}
      caption="Every generated matvec kernel is exactly this: a format, a row bucket, an epilogue, and four tuning knobs. Re-tuning is a table edit plus regenerate."
    >
      <rect
        x={30}
        y={30}
        width={420}
        height={44}
        rx={6}
        fill="color-mix(in oklch, var(--kfam-matvec) 10%, transparent)"
        stroke="var(--kfam-matvec)"
        strokeWidth={1.5}
      />
      <text
        x={240}
        y={52}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={12}
        className="fill-foreground font-mono"
      >
        {route?.format ?? "q4_k"} · {route?.rowBucket ?? "rows_2_8"} · {route?.epilogue ?? "none"}
      </text>
      {fields.map((f, i) => {
        const x = 30 + (i % 2) * 215;
        const y = 100 + Math.floor(i / 2) * 52;
        return (
          <g key={f.k}>
            <rect
              x={x}
              y={y}
              width={200}
              height={40}
              rx={4}
              fill="none"
              stroke="var(--border)"
              strokeWidth={1}
            />
            <text x={x + 10} y={y + 15} fontSize={8.5} className="fill-muted-foreground font-mono">
              {f.k}
            </text>
            <text x={x + 10} y={y + 31} fontSize={11} className="fill-foreground font-mono">
              {String(f.v)}
            </text>
          </g>
        );
      })}
    </Figure>
  );
}

const FAMILY_COLORS: Record<string, string> = {
  matvec: "var(--kfam-matvec)",
  attention: "var(--kfam-attention)",
  fusion: "var(--kfam-fusion)",
  sampling: "var(--kfam-sampling)",
  moe: "var(--kfam-moe)",
  kv: "var(--kfam-kv)",
  mm_sg: "var(--kfam-mmsg)",
};

export function KernelCensusFigure({ census }: { census: KernelCensus }) {
  const CENSUS = Object.entries(census.byFamily)
    .map(([family, count]) => ({
      family,
      count,
      color: FAMILY_COLORS[family] ?? "var(--muted-foreground)",
    }))
    .sort((a, b) => b.count - a.count);
  const max = CENSUS[0]?.count ?? 1;
  const rowH = 22;
  const H = CENSUS.length * rowH + 30;
  const barX = 118;
  const barMax = 300;
  return (
    <Figure
      viewBox={`0 0 480 ${H}`}
      title={`${census.total} extracted Metal entry points, by family`}
      caption="Families with a page-wide color keep it; the long tail stays neutral. Counts from the generated kernel inventory."
    >
      {CENSUS.map((c, i) => {
        const y = 12 + i * rowH;
        const w = Math.max(3, (c.count / max) * barMax);
        return (
          <g key={c.family}>
            <text
              x={barX - 8}
              y={y + 8}
              textAnchor="end"
              dominantBaseline="central"
              fontSize={9.5}
              className="fill-foreground font-mono"
            >
              {c.family}
            </text>
            <rect
              x={barX}
              y={y}
              width={w}
              height={14}
              rx={4}
              fill={c.color}
              opacity={c.color === "var(--muted-foreground)" ? 0.35 : 0.8}
            />
            <text
              x={barX + w + 6}
              y={y + 8}
              dominantBaseline="central"
              fontSize={9.5}
              className="fill-muted-foreground font-mono"
            >
              {c.count}
            </text>
          </g>
        );
      })}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 9 — the LM head problem                                          */
/* ------------------------------------------------------------------ */

const BYTES_SEGMENTS = [
  { label: "FFN", mb: 1858, color: "var(--kfam-matvec)" },
  { label: "LM head", mb: 550, color: "var(--kfam-sampling)" },
  { label: "attention", mb: 330, color: "var(--kfam-attention)" },
  { label: "PLE", mb: 86, color: "var(--kfam-fusion)" },
  { label: "norms/KV", mb: 7, color: "var(--kfam-kv)" },
];

export function LmHeadZoomFigure({ isE4b }: { isE4b: boolean }) {
  const total = BYTES_SEGMENTS.reduce((n, s) => n + s.mb, 0);
  const barW = 440;
  let x = 20;
  const placed = BYTES_SEGMENTS.map((s) => {
    const w = (s.mb / total) * barW;
    const seg = { ...s, x, w };
    x += w;
    return seg;
  });
  const lm = placed[1];
  return (
    <Figure
      viewBox="0 0 480 240"
      title="historical E4B weight-size estimate, zoomed on the tail"
      caption={`One matvec against all 262,144 vocab rows: [${isE4b ? 2560 : 1536} × 262144]. On E4B that is 550 MB per token — about 19.5% of the historical weight-size estimate (not measured traffic).`}
    >
      {placed.map((s) => (
        <g key={s.label}>
          <rect
            x={s.x}
            y={40}
            width={Math.max(1.5, s.w - 2)}
            height={30}
            rx={3}
            fill={s.color}
            opacity={s.label === "LM head" ? 0.9 : 0.35}
          />
          {s.w > 40 && (
            <text
              x={s.x + s.w / 2}
              y={28}
              textAnchor="middle"
              fontSize={9}
              className="fill-muted-foreground font-mono"
            >
              {s.label}
            </text>
          )}
        </g>
      ))}
      {/* zoom lines */}
      <line
        x1={lm.x}
        y1={72}
        x2={60}
        y2={130}
        stroke="var(--muted-foreground)"
        strokeWidth={1}
        strokeDasharray="3 3"
      />
      <line
        x1={lm.x + lm.w}
        y1={72}
        x2={420}
        y2={130}
        stroke="var(--muted-foreground)"
        strokeWidth={1}
        strokeDasharray="3 3"
      />
      <rect
        x={60}
        y={132}
        width={360}
        height={44}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-sampling) 16%, transparent)"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.5}
      />
      <text x={240} y={150} textAnchor="middle" fontSize={10} className="fill-foreground font-mono">
        LM head · Q6_K · 550 MB/token · 19.5%
      </text>
      <text
        x={240}
        y={166}
        textAnchor="middle"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        the un-tuned straggler: +7–12 tok/s on the table
      </text>
    </Figure>
  );
}

export function LmHeadPathsFigure() {
  return (
    <Figure
      viewBox="0 0 480 270"
      title="opt-in candidate repacking and a rejected experiment"
      caption="Opt-in Q4_K weights nominate greedy candidates; retained Q6_K weights rescore them (not drawn). Historical Air probes improved about 4–5%; sampled/full-logit paths retain Q6_K. The Q4_0 experiment produced instant end-of-turn."
    >
      <WeightGlyph
        x={20}
        y={60}
        w={110}
        h={36}
        label="lm_head"
        sublabel="[hidden × 262144]"
        quant="q6_k"
      />
      {/* shipped path */}
      <FlowArrow x1={134} y1={70} x2={220} y2={50} label="streaming repack" />
      <WeightGlyph x={224} y={34} w={100} h={32} label="" quant="q4_k" highlight />
      <FlowArrow x1={328} y1={50} x2={370} y2={50} />
      <MatmulGlyph x={352} y={34} w={116} h={32} label="" sublabel="" dtype="q4_k" />
      <text
        x={410}
        y={50}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={7.5}
        className="fill-foreground font-mono"
      >
        q4_k_linear_1x_reduce_v2
      </text>
      <text x={410} y={82} textAnchor="middle" fontSize={9} className="fill-primary font-mono">
        opt-in · historical +4–5%
      </text>
      {/* ghost path */}
      <FlowArrow x1={134} y1={90} x2={220} y2={170} ghost label="re-quantize harder?" />
      <WeightGlyph x={224} y={156} w={100} h={32} label="" quant="q4_0" dim />
      <line x1={218} y1={192} x2={330} y2={152} stroke="var(--destructive)" strokeWidth={1.5} />
      <text x={280} y={216} textAnchor="middle" fontSize={9} className="fill-destructive font-mono">
        refuted: instant-EOT quality collapse
      </text>
      <text
        x={240}
        y={252}
        textAnchor="middle"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        the head is where quantization error meets every vocabulary word at once
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 10 — sampling never leaves the GPU                               */
/* ------------------------------------------------------------------ */

export function GpuSamplingFigure() {
  return (
    <Figure
      viewBox="0 0 480 280"
      title="the sampler lives inside the frame"
      caption="Schematic eligible device path: apply supported constraints, choose argmax for greedy or Gumbel sampling for temperature, then reduce to a token id. Unsupported cases read logits back for host sampling."
    >
      {/* GPU boundary */}
      <rect
        x={12}
        y={24}
        width={456}
        height={170}
        rx={10}
        fill="none"
        stroke="var(--primary)"
        strokeWidth={1.5}
        strokeDasharray="8 4"
      />
      <text x={26} y={44} fontSize={9} className="fill-primary font-mono">
        GPU
      </text>
      <ActivationGlyph
        x={26}
        y={100}
        w={78}
        h={30}
        label="logits"
        sublabel="[262144] f32"
        dtype="f32"
      />
      <FlowArrow x1={108} y1={115} x2={136} y2={115} />
      <MatmulGlyph x={140} y={99} w={92} h={32} label="constraints" dtype="f32" />
      <FlowArrow x1={236} y1={115} x2={264} y2={115} />
      <MatmulGlyph
        x={268}
        y={99}
        w={86}
        h={32}
        label="selection"
        sublabel="greedy / sample"
        dtype="f32"
      />
      <FlowArrow x1={358} y1={115} x2={386} y2={115} />
      <SamplerGlyph x={390} y={98} w={34} label="reduce" dtype="f32" />
      <text x={407} y={160} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        token id
      </text>
      <text
        x={407}
        y={174}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        stays on device
      </text>
      {/* ghost host path */}
      <FlowArrow x1={65} y1={134} x2={65} y2={230} dashed label="fallback: logits readback" />
      <rect
        x={20}
        y={234}
        width={110}
        height={30}
        rx={4}
        fill="none"
        stroke="var(--muted-foreground)"
        strokeWidth={1}
        strokeDasharray="4 3"
        opacity={0.5}
      />
      <text
        x={75}
        y={249}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        host sampler
      </text>
    </Figure>
  );
}

export function TokenHandoffFigure() {
  return (
    <Figure
      viewBox="0 0 480 220"
      title="the device-resident handoff"
      caption="Because the sampled id is already a GPU buffer, frame N+1's embedding lookup can be encoded before frame N finishes — the handoff that makes chapter 7's pipelined frame legal."
    >
      {/* frame N */}
      <rect
        x={20}
        y={50}
        width={190}
        height={60}
        rx={6}
        fill="color-mix(in oklch, var(--kfam-sampling) 12%, transparent)"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.25}
      />
      <text x={115} y={72} textAnchor="middle" fontSize={10} className="fill-foreground font-mono">
        frame N
      </text>
      <text
        x={115}
        y={90}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        …ends in argmax → token buffer
      </text>
      {/* token buffer */}
      <rect
        x={222}
        y={66}
        width={36}
        height={28}
        rx={4}
        fill="color-mix(in oklch, var(--primary) 25%, transparent)"
        stroke="var(--primary)"
        strokeWidth={1.5}
      />
      <text
        x={240}
        y={80}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={8}
        className="fill-foreground font-mono"
      >
        id
      </text>
      <FlowArrow x1={212} y1={80} x2={220} y2={80} />
      <FlowArrow x1={260} y1={80} x2={268} y2={80} />
      {/* frame N+1 */}
      <rect
        x={270}
        y={50}
        width={190}
        height={60}
        rx={6}
        fill="color-mix(in oklch, var(--dtype-f16) 12%, transparent)"
        stroke="var(--dtype-f16)"
        strokeWidth={1.25}
      />
      <text x={365} y={72} textAnchor="middle" fontSize={10} className="fill-foreground font-mono">
        frame N+1
      </text>
      <text
        x={365}
        y={90}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        embed lookup reads the buffer
      </text>
      <text
        x={240}
        y={150}
        textAnchor="middle"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        the CPU encodes N+1 from a token that does not exist on the host yet
      </text>
      <text x={240} y={168} textAnchor="middle" fontSize={9} className="fill-primary font-mono">
        no readback → no bubble (see chapter 07)
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 11 — MTP                                                         */
/* ------------------------------------------------------------------ */

export function MtpSideBySideFigure({ isE4b }: { isE4b: boolean }) {
  const donors: [number, number] = isE4b ? [22, 23] : [13, 14];
  const mainLayers = 8; // compressed drawing of the target stack
  return (
    <Figure
      viewBox="0 0 480 300"
      title="two models, one KV cache"
      caption={`The 4-layer, hidden-256 drafter owns no K/V projections at all — its layers cross-attend the main model's KV banks (sliding donor layer ${donors[0]}, full-attention donor ${donors[1]}).`}
    >
      {/* main model stack */}
      <text x={90} y={30} textAnchor="middle" fontSize={10} className="fill-foreground font-mono">
        main model
      </text>
      {Array.from({ length: mainLayers }, (_, i) => {
        const y = 44 + i * 26;
        const isDonor = i === mainLayers - 2 || i === mainLayers - 1;
        return (
          <g key={y}>
            <rect
              x={30}
              y={y}
              width={120}
              height={20}
              rx={3}
              fill="var(--kfam-attention)"
              opacity={isDonor ? 0.7 : 0.25}
            />
            {isDonor && (
              <>
                <KvBlockGlyph x={156} y={y + 3} size={14} state="filled" />
                <text
                  x={24}
                  y={y + 10}
                  textAnchor="end"
                  dominantBaseline="central"
                  fontSize={8}
                  className="fill-foreground font-mono"
                >
                  L{donors[i - (mainLayers - 2)]}
                </text>
              </>
            )}
          </g>
        );
      })}
      <text
        x={90}
        y={44 + mainLayers * 26 + 14}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        (…{isE4b ? 42 : 35} layers, drawn compressed)
      </text>
      {/* draft stack */}
      <text x={370} y={30} textAnchor="middle" fontSize={10} className="fill-foreground font-mono">
        drafter (4 layers, d=256)
      </text>
      {Array.from({ length: 4 }, (_, i) => {
        const y = 60 + i * 40;
        const full = i === 3;
        const donorY = full ? 44 + (mainLayers - 1) * 26 + 10 : 44 + (mainLayers - 2) * 26 + 10;
        return (
          <g key={y}>
            <rect
              x={320}
              y={y}
              width={100}
              height={26}
              rx={3}
              fill="color-mix(in oklch, var(--kfam-moe) 14%, transparent)"
              stroke="var(--kfam-moe)"
              strokeWidth={1}
            />
            <text
              x={370}
              y={y + 13}
              textAnchor="middle"
              dominantBaseline="central"
              fontSize={8}
              className="fill-foreground font-mono"
            >
              {full ? "full attn" : "sliding"}
            </text>
            {/* cross-attention arrow into the donor KV */}
            <FlowArrow x1={318} y1={y + 13} x2={176} y2={donorY} dashed />
          </g>
        );
      })}
      <text
        x={370}
        y={240}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        query-only: no K/V projections,
      </text>
      <text
        x={370}
        y={253}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        reads the target&apos;s banks directly
      </text>
    </Figure>
  );
}

export function ProposeVerifyFigure() {
  const drafts = [
    { tok: "▁the", ok: true },
    { tok: "▁ant", ok: true },
    { tok: "▁colony", ok: false },
    { tok: "▁sleeps", ok: false },
  ];
  return (
    <Figure
      viewBox="0 0 480 240"
      title="propose → verify → accept"
      caption="Illustrative greedy propose/verify round on a batched verifier route. A mismatch truncates the draft tail and uses a target replacement; verification and KV rollback also cost work."
    >
      <text x={20} y={50} fontSize={9} className="fill-muted-foreground font-mono">
        draft proposes
      </text>
      {drafts.map((d, i) => {
        const x = 130 + i * 82;
        return (
          <g key={d.tok}>
            <rect
              x={x}
              y={34}
              width={72}
              height={26}
              rx={4}
              fill="color-mix(in oklch, var(--kfam-moe) 16%, transparent)"
              stroke="var(--kfam-moe)"
              strokeWidth={1.25}
            />
            <text
              x={x + 36}
              y={47}
              textAnchor="middle"
              dominantBaseline="central"
              fontSize={9}
              className="fill-foreground font-mono"
            >
              {d.tok}
            </text>
          </g>
        );
      })}
      <FlowArrow x1={240} y1={68} x2={240} y2={104} label="one batched verify pass" />
      <text x={20} y={135} fontSize={9} className="fill-muted-foreground font-mono">
        target verifies
      </text>
      {drafts.map((d, i) => {
        const x = 130 + i * 82;
        const rejectedTail = !d.ok && i > 2;
        return (
          <g key={d.tok}>
            <rect
              x={x}
              y={120}
              width={72}
              height={26}
              rx={4}
              fill={d.ok ? "color-mix(in oklch, var(--kfam-attention) 14%, transparent)" : "none"}
              opacity={rejectedTail ? 0.5 : 1}
              stroke={d.ok ? "var(--kfam-attention)" : "var(--destructive)"}
              strokeWidth={1.25}
              strokeDasharray={d.ok ? undefined : "4 3"}
            />
            <text
              x={x + 36}
              y={133}
              textAnchor="middle"
              dominantBaseline="central"
              fontSize={9}
              className="fill-foreground font-mono"
            >
              {d.ok ? "accept" : i === 2 ? "reject" : "dropped"}
            </text>
          </g>
        );
      })}
      <text
        x={240}
        y={190}
        textAnchor="middle"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        2 accepted + the verifier&apos;s replacement = 3 tokens for one main-model pass
      </text>
    </Figure>
  );
}

const MTP_BOARD = [
  { system: "E2B target only", factor: 1, note: "75.9 tok/s" },
  { system: "E2B BF16 MTP", factor: 63.7 / 75.9, note: "63.7 tok/s; 64% accepted", self: true },
];

export function MtpScoreboardFigure() {
  const barX = 150;
  const scale = 130; // px per 1×
  return (
    <Figure
      viewBox="0 0 480 250"
      title="historical E2B Metal pilot: acceptance is not speedup"
      caption="Directional CLI pilot from the performance plan; binary identity was not recorded. The draft lane fell below the target-only baseline and disabled itself after its cost probe."
    >
      {/* 1x reference line */}
      <line
        x1={barX + scale}
        y1={16}
        x2={barX + scale}
        y2={190}
        stroke="var(--muted-foreground)"
        strokeWidth={1}
        strokeDasharray="3 3"
      />
      <text
        x={barX + scale}
        y={206}
        textAnchor="middle"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        1× (no MTP)
      </text>
      {MTP_BOARD.map((b, i) => {
        const y = 28 + i * 42;
        const w = b.factor * scale;
        return (
          <g key={b.system}>
            <text
              x={barX - 8}
              y={y + 9}
              textAnchor="end"
              dominantBaseline="central"
              fontSize={9}
              className="fill-foreground font-mono"
            >
              {b.system}
            </text>
            <rect
              x={barX}
              y={y}
              width={w}
              height={16}
              rx={4}
              fill={b.self ? "var(--destructive)" : "var(--kfam-attention)"}
              opacity={b.self ? 0.65 : 0.7}
            />
            <text
              x={barX + w + 6}
              y={y + 9}
              dominantBaseline="central"
              fontSize={8.5}
              className="fill-muted-foreground font-mono"
            >
              {b.factor.toFixed(2)}× · {b.note}
            </text>
          </g>
        );
      })}
      <text
        x={240}
        y={236}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        historical pilot only — not a current-head benchmark
      </text>
    </Figure>
  );
}
