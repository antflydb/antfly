"use client";

import { cn } from "@antfly/design-system";
import { Figure } from "@/components/viz/glyphs";

/* ------------------------------------------------------------------ */
/* Ch 1 — schema → ten-marker prompt, enum prefix, normalizer          */
/* ------------------------------------------------------------------ */

const MARKER_COLOR = "var(--kfam-fusion)";
const TEXT_COLOR = "var(--kfam-attention)";
const ENUM_COLOR = "var(--kfam-sampling)";

function Marker({ children }: { children: string }) {
  return (
    <span
      className="rounded px-0.5 font-semibold"
      style={{
        background: `color-mix(in oklch, ${MARKER_COLOR} 20%, transparent)`,
        color: "var(--kfam-text-fusion)",
      }}
    >
      {children}
    </span>
  );
}

export function PromptAssemblyFigure({ step }: { step: 0 | 1 | 2 }) {
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      {step === 0 && (
        <>
          <div className="rounded-lg border bg-muted/30 p-3">
            <div className="mb-1.5 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
              illustrative schema: two task groups
            </div>
            <pre className="whitespace-pre-wrap font-mono text-xs leading-relaxed">
              {"entities: company, person\n"}
              {"structure invoice: vendor, total, currency ∈ (USD | EUR)"}
            </pre>
          </div>
          <div className="text-center text-muted-foreground">↓ serialized ahead of the text</div>
          <div className="rounded-lg border p-3 font-mono text-[11px] leading-loose">
            ( <Marker>[P]</Marker> entities ( <Marker>[E]</Marker> company <Marker>[E]</Marker>{" "}
            person ) ) <Marker>[SEP_STRUCT]</Marker> ( <Marker>[P]</Marker> invoice ({" "}
            <Marker>[C]</Marker> vendor <Marker>[C]</Marker> total <Marker>[C]</Marker> currency ) ){" "}
            <Marker>[DESCRIPTION]</Marker> total: amount due <Marker>[SEP_TEXT]</Marker>{" "}
            <span style={{ color: "var(--kfam-text-attention)" }}>…body words…</span>
          </div>
          <div className="text-center font-mono text-[11px] text-muted-foreground">
            illustrative serialization, not a captured prompt · ten marker types vs GLiNER2's five
          </div>
        </>
      )}
      {step === 1 && (
        <>
          <div className="rounded-lg border p-3 font-mono text-[11px] leading-loose">
            <span
              className="rounded px-0.5"
              style={{ background: `color-mix(in oklch, ${ENUM_COLOR} 18%, transparent)` }}
            >
              ( invoice: currency ( USD | EUR ) )
            </span>{" "}
            <span style={{ color: "var(--kfam-text-attention)" }}>
              Invoice from Acme for 1,200 EUR…
            </span>
          </div>
          <div className="grid grid-cols-2 gap-2 text-center font-mono text-[10px]">
            <div className="rounded-md border border-dashed p-2 text-muted-foreground">
              prefix words — scored as spans, never mapped to source offsets
            </div>
            <div className="rounded-md border p-2" style={{ borderColor: TEXT_COLOR }}>
              body words — offsets survive to the response
            </div>
          </div>
          <div className="text-center font-mono text-[11px] text-muted-foreground">
            enum choices become synthetic prefix words the boundary head can "extract" — max_len
            budgets body words only
          </div>
        </>
      )}
      {step === 2 && (
        <>
          <div className="rounded-lg border bg-muted/30 p-3 font-mono text-xs leading-relaxed">
            <div className="text-muted-foreground">tokenizer.json normalizer chain (strict):</div>
            <div className="mt-1">
              NFC (Unicode 15) → Replace(whitespace, ▁-safe) → Strip — every step implemented, or
              the load fails
            </div>
          </div>
          <div className="rounded-lg border p-3">
            <div className="mb-1 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
              one span, three offset units
            </div>
            <div className="font-mono text-xs">café&nbsp;société&nbsp;→&nbsp;“société”</div>
            <div className="mt-1.5 grid grid-cols-3 gap-1.5 text-center font-mono text-[10px]">
              <div className="rounded border p-1.5">utf8_bytes: 6..14</div>
              <div className="rounded border p-1.5">codepoints: 5..12</div>
              <div className="rounded border p-1.5">utf16: 5..12</div>
            </div>
          </div>
          <div className="text-center font-mono text-[11px] text-muted-foreground">
            illustrative offsets · normalization can reorder codepoints, so the model owns its
            word→byte map
          </div>
        </>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 2 — encoder contract + long documents                            */
/* ------------------------------------------------------------------ */

export function EncoderContractFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="same equations, different dispatch"
      caption="Schematic, not a profiler trace. GLiNER2 dispatches a dedicated fused attention kernel that comes in several variants.
        GLiNER2.5 computes the same disentangled attention in a single dispatch of its combined
        boundary kernel, and its optimized path keeps the per-layer relative Q and K resident on
        the GPU."
    >
      <text
        x={110}
        y={26}
        textAnchor="middle"
        fontSize={10}
        className="fill-muted-foreground font-mono"
      >
        GLiNER2 Metal path
      </text>
      <rect
        x={30}
        y={40}
        width={160}
        height={44}
        rx={5}
        fill="none"
        stroke="var(--muted-foreground)"
        strokeWidth={0.9}
        opacity={0.7}
      />
      <text
        x={110}
        y={58}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        termite_disentangled_relative
      </text>
      <text
        x={110}
        y={70}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        _attention_f32 (+_flash4)
      </text>
      <text x={330} y={26} textAnchor="middle" fontSize={10} className="fill-primary font-mono">
        GLiNER2.5 Metal path
      </text>
      <rect
        x={250}
        y={40}
        width={160}
        height={44}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
        stroke="var(--kfam-fusion)"
        strokeWidth={1.5}
      />
      <text x={330} y={58} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        termite_gliner_boundary_f32
      </text>
      <text x={330} y={70} textAnchor="middle" fontSize={8} className="fill-primary font-mono">
        Kind.deberta_attention
      </text>
      {["MPS", "threadgroup", "scalar"].map((v, i) => (
        <g key={v}>
          <path
            d={`M 110 84 L ${46 + i * 64} 112`}
            stroke="var(--muted-foreground)"
            strokeWidth={0.9}
          />
          <rect
            x={18 + i * 64}
            y={114}
            width={56}
            height={18}
            rx={3}
            fill="none"
            stroke="var(--muted-foreground)"
            strokeWidth={0.75}
            opacity={0.7}
          />
          <text
            x={46 + i * 64}
            y={126}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            {v}
          </text>
        </g>
      ))}
      <text
        x={110}
        y={152}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        variant picked by size and flags
      </text>
      <path d="M 330 84 v 22" stroke="var(--muted-foreground)" strokeWidth={0.9} />
      <rect
        x={250}
        y={108}
        width={160}
        height={20}
        rx={3}
        fill="none"
        stroke="var(--kfam-attention)"
        strokeWidth={0.9}
      />
      <text
        x={330}
        y={121}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        one simdgroup dispatch, always
      </text>
      <rect
        x={250}
        y={144}
        width={160}
        height={20}
        rx={3}
        fill="color-mix(in oklch, var(--kfam-kv) 14%, transparent)"
        stroke="var(--kfam-kv)"
        strokeWidth={0.9}
      />
      <text
        x={330}
        y={157}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-foreground font-mono"
      >
        resident rel. Q_r/K_r (optimized path)
      </text>
    </Figure>
  );
}

export function LongDocumentFigure() {
  const winW = 96;
  const overlap = 18;
  return (
    <Figure
      viewBox="0 0 440 200"
      title="one pass to 4,096 words — then windows"
      caption="Schematic windowing for documents too long for one pass. Each 4,096-word window overlaps
        its neighbour by 128 words and re-encodes the same schema header before its own slice of
        the body. Mentions found in an overlap belong to whichever window holds their midpoint,
        which is what keeps them from being counted twice. Window counts and overlaps are illustrative."
    >
      <rect
        x={30}
        y={34}
        width={380}
        height={18}
        rx={3}
        fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
        stroke="var(--kfam-attention)"
        strokeWidth={1}
      />
      <text
        x={220}
        y={47}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        long document — beyond 4,096 words, up to 131,072
      </text>
      {[0, 1, 2, 3].map((i) => {
        const x = 30 + i * (winW - overlap);
        return (
          <g key={i}>
            <rect
              x={x}
              y={72}
              width={winW}
              height={30}
              rx={4}
              fill="color-mix(in oklch, var(--kfam-fusion) 14%, transparent)"
              stroke="var(--kfam-fusion)"
              strokeWidth={1}
            />
            <rect
              x={x + 4}
              y={77}
              width={26}
              height={20}
              rx={2}
              fill="color-mix(in oklch, var(--kfam-sampling) 20%, transparent)"
            />
            <text
              x={x + 17}
              y={90}
              textAnchor="middle"
              fontSize={6.5}
              className="fill-foreground font-mono"
            >
              schema
            </text>
            <text
              x={x + (winW + 30) / 2}
              y={90}
              textAnchor="middle"
              fontSize={7}
              className="fill-muted-foreground font-mono"
            >
              window {i}
            </text>
            <path
              d={`M ${x + winW / 2} 104 v 22`}
              stroke="var(--muted-foreground)"
              strokeWidth={0.9}
            />
          </g>
        );
      })}
      <rect
        x={110}
        y={130}
        width={220}
        height={24}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-kv) 14%, transparent)"
        stroke="var(--kfam-kv)"
        strokeWidth={1.25}
      />
      <text
        x={220}
        y={146}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-foreground font-mono"
      >
        global merge — midpoint word ownership
      </text>
      <text
        x={220}
        y={176}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        mentions dedupe by max score · classifications: owned-word-weighted logit mean · records by
        identity
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 3 — span grid vs boundaries; boundary encoder + proposer         */
/* ------------------------------------------------------------------ */

const BOUNDARY_DOTS = Array.from({ length: 13 }, (_, i) => ({
  id: `bdot-${244 + i * 14.5}`,
  cx: 244 + i * 14.5,
  hot: i === 2 || i === 9,
}));

export function SpanGridVsBoundaryFigure() {
  const cs = 11;
  // Rows are tighter than columns so all 12 widths clear the title.
  const rs = 9;
  const words = 12;
  const cap = 12; // GLiNER2's default max_width
  const cells = [];
  for (let s = 0; s < words; s++) {
    for (let w = 0; w < Math.min(cap, words - s); w++) {
      cells.push(
        <rect
          key={`${s}-${w}`}
          x={30 + s * cs}
          y={149 - w * rs}
          width={cs - 1.5}
          height={rs - 1.5}
          rx={1.5}
          fill="var(--kfam-attention)"
          opacity={0.2 + w * 0.05}
        />
      );
    }
  }
  return (
    <Figure
      viewBox="0 0 440 220"
      title="span grid vs boundary proposals"
      caption="Left: GLiNER2 scores every (start, width ≤ max_width) span — 12 by default, and the cap is structural. Right: GLiNER2.5 scores W+1 boundaries between words and proposes start/end pairs, so span width is unbounded. Counts are illustrative."
    >
      <text
        x={95}
        y={24}
        textAnchor="middle"
        fontSize={10}
        className="fill-muted-foreground font-mono"
      >
        GLiNER2: start × width grid
      </text>
      {cells}
      <text
        x={95}
        y={168}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        width capped at max_width (12)
      </text>
      <text x={330} y={24} textAnchor="middle" fontSize={10} className="fill-primary font-mono">
        GLiNER2.5: W+1 boundaries
      </text>
      <line x1={240} y1={100} x2={420} y2={100} stroke="var(--border)" strokeWidth={1} />
      {BOUNDARY_DOTS.map((d) => (
        <circle
          key={d.id}
          cx={d.cx}
          cy={100}
          r={3.4}
          fill="var(--kfam-fusion)"
          opacity={d.hot ? 1 : 0.35}
        />
      ))}
      <path
        d="M 273 96 Q 330 60 374.5 96"
        fill="none"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.5}
      />
      <text x={330} y={62} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        start 2 → end 9: any width
      </text>
      <text
        x={330}
        y={130}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        learned BOS/EOS states cover the edges
      </text>
      <text
        x={330}
        y={168}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        boundaries, not spans, are the scored unit
      </text>
    </Figure>
  );
}

export function BoundaryEncoderFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 230"
      title={
        step === 0 ? "words → 128-d boundary states" : "per-query marginals → document proposal"
      }
      caption={
        step === 0
          ? "Schematic: each boundary sees its left and right word through separate 768→128 projections, then two windowed attention blocks (window 128, 4 heads) and one SwiGLU refinement block."
          : "Schematic marginals: every query scores every boundary as a start and as an end; the union over queries keeps the document's top-32 starts and ends, and all pairings are scored."
      }
    >
      <rect
        x={30}
        y={30}
        width={380}
        height={20}
        rx={4}
        fill="color-mix(in oklch, var(--dtype-f16) 18%, transparent)"
        stroke="var(--dtype-f16)"
        strokeWidth={1}
      />
      <text
        x={220}
        y={44}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        word states [W, 768] — first sub-token per word
      </text>
      {step === 0 && (
        <>
          <path
            d="M 150 50 L 190 84 M 290 50 L 250 84"
            stroke="var(--muted-foreground)"
            strokeWidth={1}
          />
          <text
            x={122}
            y={72}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            left_projection
          </text>
          <text
            x={320}
            y={72}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            right_projection
          </text>
          <rect
            x={140}
            y={88}
            width={160}
            height={22}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
            stroke="var(--kfam-fusion)"
            strokeWidth={1.25}
          />
          <text
            x={220}
            y={103}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            boundary states [W+1, 128]
          </text>
          <path d="M 220 110 v 16" stroke="var(--muted-foreground)" strokeWidth={1} />
          <rect
            x={120}
            y={128}
            width={200}
            height={22}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
            stroke="var(--kfam-attention)"
            strokeWidth={1}
          />
          <text
            x={220}
            y={143}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            windowed attention ×2 · window 128 · 4 heads
          </text>
          <path d="M 220 150 v 16" stroke="var(--muted-foreground)" strokeWidth={1} />
          <rect
            x={140}
            y={168}
            width={160}
            height={22}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-mmsg) 16%, transparent)"
            stroke="var(--kfam-mmsg)"
            strokeWidth={1}
          />
          <text
            x={220}
            y={183}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            gated FFN (SwiGLU) ×1
          </text>
          <text
            x={220}
            y={212}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            BOS/EOS are learned vectors, not tokens
          </text>
        </>
      )}
      {step === 1 && (
        <>
          <rect
            x={40}
            y={76}
            width={120}
            height={22}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-sampling) 16%, transparent)"
            stroke="var(--kfam-sampling)"
            strokeWidth={1.25}
          />
          <text
            x={100}
            y={91}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            query states [Q, 768]
          </text>
          <path d="M 160 87 h 30" stroke="var(--muted-foreground)" strokeWidth={1} />
          <rect
            x={194}
            y={64}
            width={216}
            height={48}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-fusion) 12%, transparent)"
            stroke="var(--kfam-fusion)"
            strokeWidth={1}
          />
          <text
            x={302}
            y={82}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            start marginals [Q, W+1]
          </text>
          <text
            x={302}
            y={98}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            end marginals [Q, W+1]
          </text>
          <path d="M 302 112 v 18" stroke="var(--muted-foreground)" strokeWidth={1} />
          <rect
            x={214}
            y={132}
            width={176}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
            stroke="var(--kfam-attention)"
            strokeWidth={1.25}
          />
          <text
            x={302}
            y={148}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            union top-32 starts × top-32 ends
          </text>
          <text
            x={302}
            y={180}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            all pairings scored by compatibility + union marginals
          </text>
        </>
      )}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 4 — shared pool + FiLM, pair scorer                              */
/* ------------------------------------------------------------------ */

const POOL_SLOTS = Array.from({ length: 16 }, (_, i) => ({
  id: `slot-${190 + i * 13.5}`,
  x: 190 + i * 13.5,
  opacity: 0.25 + ((i * 5) % 7) * 0.09,
}));

export function SharedPoolFigure({ highlight }: { highlight: "film" | "pair" }) {
  return (
    <Figure
      viewBox="0 0 440 230"
      title={
        highlight === "film" ? "one pool, one FiLM lens per query" : "the explicit-span scorer"
      }
      caption={
        highlight === "film"
          ? "Schematic: the document proposal fills one 192-slot pool (≥8 slots per query). Each query modulates the same pool with FiLM (1+γ)·x+β before scoring — these slot logits are what decode consumes for entities and fields."
          : "Schematic explicit-span scoring — the path for attributes and constrained/enum fields: rotary-encoded endpoints, an 8-head compatibility mix, endpoint differences, inside evidence and length features sum into one logit per (query, span)."
      }
    >
      <g opacity={highlight === "film" ? 1 : 0.7} className="transition-opacity duration-300">
        {["company", "person", "invoice.total"].map((q, i) => (
          <g key={q}>
            <rect
              x={30}
              y={34 + i * 34}
              width={104}
              height={22}
              rx={4}
              fill="color-mix(in oklch, var(--kfam-sampling) 16%, transparent)"
              stroke="var(--kfam-sampling)"
              strokeWidth={1}
            />
            <text
              x={82}
              y={49 + i * 34}
              textAnchor="middle"
              fontSize={7.5}
              className="fill-foreground font-mono"
            >
              {q}
            </text>
            <path
              d={`M 134 ${45 + i * 34} L 178 ${72}`}
              stroke="var(--kfam-fusion)"
              strokeWidth={1}
            />
            <text
              x={158}
              y={40 + i * 34}
              textAnchor="middle"
              fontSize={7}
              className="fill-muted-foreground font-mono"
            >
              γ,β
            </text>
          </g>
        ))}
        <rect
          x={182}
          y={54}
          width={228}
          height={38}
          rx={5}
          fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
          stroke="var(--kfam-attention)"
          strokeWidth={1.5}
        />
        {POOL_SLOTS.map((s) => (
          <rect
            key={s.id}
            x={s.x}
            y={62}
            width={10}
            height={22}
            rx={2}
            fill="var(--kfam-attention)"
            opacity={s.opacity}
          />
        ))}
        <text
          x={296}
          y={108}
          textAnchor="middle"
          fontSize={8}
          className="fill-muted-foreground font-mono"
        >
          shared candidate pool — 192 slots, ≥8 per query
        </text>
      </g>
      <g opacity={highlight === "pair" ? 1 : 0.7} className="transition-opacity duration-300">
        <rect
          x={40}
          y={138}
          width={110}
          height={20}
          rx={3}
          fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
          stroke="var(--kfam-fusion)"
          strokeWidth={1}
        />
        <text
          x={95}
          y={151}
          textAnchor="middle"
          fontSize={7.5}
          className="fill-foreground font-mono"
        >
          rotary endpoints θ=10⁴
        </text>
        <rect
          x={166}
          y={138}
          width={110}
          height={20}
          rx={3}
          fill="color-mix(in oklch, var(--kfam-mmsg) 16%, transparent)"
          stroke="var(--kfam-mmsg)"
          strokeWidth={1}
        />
        <text
          x={221}
          y={151}
          textAnchor="middle"
          fontSize={7.5}
          className="fill-foreground font-mono"
        >
          8-head compat mix
        </text>
        <rect
          x={292}
          y={138}
          width={110}
          height={20}
          rx={3}
          fill="color-mix(in oklch, var(--kfam-kv) 16%, transparent)"
          stroke="var(--kfam-kv)"
          strokeWidth={1}
        />
        <text
          x={347}
          y={151}
          textAnchor="middle"
          fontSize={7.5}
          className="fill-foreground font-mono"
        >
          inside + length + content
        </text>
        {[95, 221, 347].map((x) => (
          <path
            key={x}
            d={`M ${x} 158 L 221 186`}
            stroke="var(--muted-foreground)"
            strokeWidth={0.9}
          />
        ))}
        <rect
          x={156}
          y={188}
          width={130}
          height={22}
          rx={4}
          fill="color-mix(in oklch, var(--kfam-sampling) 18%, transparent)"
          stroke="var(--kfam-sampling)"
          strokeWidth={1.5}
        />
        <text
          x={221}
          y={203}
          textAnchor="middle"
          fontSize={8}
          className="fill-foreground font-mono"
        >
          explicit-span logit
        </text>
      </g>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 5 — decode cascade                                               */
/* ------------------------------------------------------------------ */

export function DecodeCascadeFigure({ step }: { step: 0 | 1 | 2 }) {
  const stages = [
    {
      id: 0,
      title: "abstain?",
      body: "sigmoid(null) > 0.5 → emit nothing",
      color: "var(--kfam-kv)",
    },
    {
      id: 1,
      title: "calibrate (+ rescue)",
      body: "sigmoid(logit/τ) · count rescue if enabled",
      color: "var(--kfam-fusion)",
    },
    {
      id: 2,
      title: "resolve overlaps",
      body: "flat = exact weighted interval scheduling",
      color: "var(--kfam-attention)",
    },
  ];
  return (
    <Figure
      viewBox="0 0 440 210"
      title="the decode cascade"
      caption="Schematic host-side cascade per query. Count guidance only adds candidates by rank — never removing threshold hits — and only when adaptive_threshold is enabled (off in the released bundles). Flat overlap resolution is exact, not greedy; nested, longest and allow are alternatives."
    >
      {stages.map((s, i) => (
        <g key={s.id} opacity={step === s.id ? 1 : 0.7} className="transition-opacity duration-300">
          <rect
            x={30 + i * 136}
            y={64}
            width={124}
            height={64}
            rx={6}
            fill={`color-mix(in oklch, ${s.color} 14%, transparent)`}
            stroke={s.color}
            strokeWidth={step === s.id ? 1.75 : 1}
          />
          <text
            x={92 + i * 136}
            y={86}
            textAnchor="middle"
            fontSize={9}
            className="fill-foreground font-mono font-semibold"
          >
            {s.title}
          </text>
          <foreignObject x={36 + i * 136} y={92} width={112} height={34}>
            <div className="text-center font-mono text-[7.5px] leading-tight text-foreground">
              {s.body}
            </div>
          </foreignObject>
          {i < 2 && (
            <path
              d={`M ${154 + i * 136} 96 h 12`}
              stroke="var(--muted-foreground)"
              strokeWidth={1.25}
            />
          )}
        </g>
      ))}
      <text
        x={220}
        y={160}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        eligible = prob ≥ threshold OR rank &lt; round(exp(count_rate))
      </text>
      <text
        x={220}
        y={178}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        then offsets convert to utf8 bytes / codepoints / utf16 units
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 6 — task heads                                                   */
/* ------------------------------------------------------------------ */

const INSTANCE_QUERY_XS = Array.from({ length: 6 }, (_, i) => 50 + i * 22);

export function TaskHeadsFigure({ task }: { task: "cls" | "rel" | "rec" }) {
  return (
    <Figure
      viewBox="0 0 440 210"
      title={
        task === "cls"
          ? "classification: the [L] choice states decide"
          : task === "rel"
            ? "relations: typed head → tail pairs"
            : "records: 32 instance queries assemble fields"
      }
      caption={
        task === "cls"
          ? "Schematic: a 768→1536→1 MLP on each classification marker state; single, multi and ordinal modes plus the logical-constraint solver run downstream."
          : task === "rel"
            ? "Schematic: directional head/tail role states and a biaffine content gate score each candidate pair; at most 64 pairs per relation type."
            : "Schematic: instances cross-attend over candidates to claim fields. Instance seeds depend on the mode — the anchor field's candidates (natural), all candidates (latent), or 32 learned queries (anchorless)."
      }
    >
      {task === "cls" && (
        <>
          <rect
            x={60}
            y={50}
            width={130}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-sampling) 16%, transparent)"
            stroke="var(--kfam-sampling)"
            strokeWidth={1.25}
          />
          <text
            x={125}
            y={66}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            [L] choice state [768]
          </text>
          <path d="M 190 62 h 34" stroke="var(--muted-foreground)" strokeWidth={1} />
          <rect
            x={228}
            y={50}
            width={150}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-matvec) 16%, transparent)"
            stroke="var(--kfam-matvec)"
            strokeWidth={1.25}
          />
          <text
            x={303}
            y={66}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            768 → 1536 → ReLU → 1
          </text>
          <text
            x={220}
            y={116}
            textAnchor="middle"
            fontSize={8}
            className="fill-muted-foreground font-mono"
          >
            modes: single · multi · ordinal — plus min/max label counts
          </text>
          <rect
            x={90}
            y={130}
            width={260}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-kv) 12%, transparent)"
            stroke="var(--kfam-kv)"
            strokeWidth={1}
            strokeDasharray="4 2"
          />
          <text
            x={220}
            y={146}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            constraint solver: Implies(a, b), counts — exact or beam
          </text>
        </>
      )}
      {task === "rel" && (
        <>
          <rect
            x={44}
            y={54}
            width={110}
            height={22}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-attention) 16%, transparent)"
            stroke="var(--kfam-attention)"
            strokeWidth={1.25}
          />
          <text
            x={99}
            y={69}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            head candidates
          </text>
          <rect
            x={286}
            y={54}
            width={110}
            height={22}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
            stroke="var(--kfam-fusion)"
            strokeWidth={1.25}
          />
          <text
            x={341}
            y={69}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            tail candidates
          </text>
          <path
            d="M 154 65 L 200 104 M 286 65 L 240 104"
            stroke="var(--muted-foreground)"
            strokeWidth={1}
          />
          <rect
            x={140}
            y={108}
            width={160}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-mmsg) 16%, transparent)"
            stroke="var(--kfam-mmsg)"
            strokeWidth={1.25}
          />
          <text
            x={220}
            y={124}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            MLP + biaffine content gate
          </text>
          <text
            x={220}
            y={162}
            textAnchor="middle"
            fontSize={8}
            className="fill-muted-foreground font-mono"
          >
            directional states: (a → b) ≠ (b → a) · pair cap 64 per type
          </text>
        </>
      )}
      {task === "rec" && (
        <>
          {INSTANCE_QUERY_XS.map((x) => (
            <rect
              key={`iq-${x}`}
              x={x}
              y={48}
              width={17}
              height={17}
              rx={3}
              fill="color-mix(in oklch, var(--kfam-sampling) 22%, transparent)"
              stroke="var(--kfam-sampling)"
              strokeWidth={0.9}
            />
          ))}
          <text
            x={116}
            y={84}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            instance seeds (6 drawn)
          </text>
          <rect
            x={250}
            y={44}
            width={150}
            height={26}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
            stroke="var(--kfam-attention)"
            strokeWidth={1.25}
          />
          <text
            x={325}
            y={61}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            pooled candidates [192]
          </text>
          <path
            d="M 116 92 L 200 116 M 325 70 L 244 116"
            stroke="var(--muted-foreground)"
            strokeWidth={1}
          />
          <rect
            x={130}
            y={120}
            width={180}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
            stroke="var(--kfam-fusion)"
            strokeWidth={1.25}
          />
          <text
            x={220}
            y={136}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            cross-attention → field assignment
          </text>
          <text
            x={220}
            y={170}
            textAnchor="middle"
            fontSize={8}
            className="fill-muted-foreground font-mono"
          >
            modes: natural (anchored) · latent · anchorless
          </text>
        </>
      )}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 7 — mega-kernel + precision policy                               */
/* ------------------------------------------------------------------ */

const KIND_SAMPLE = [
  "deberta_attention",
  "banded_attention",
  "swiglu",
  "marginals",
  "film",
  "rotary",
  "endpoints",
  "interval_means",
  "relation_gated_score",
  "record_attention",
  "norm",
  "sigmoid",
];

export function MegaKernelFigure() {
  return (
    <Figure
      viewBox="0 0 440 240"
      title="one kernel void, 42 dispatch kinds"
      caption="Schematic: termite_gliner_boundary_f32 switches on a Kind descriptor (12 of 42 shown) with Params{kind, dims[8], scalars[4]} — a private Zig↔Metal ABI. GEMMs ride MPS outside the kernel."
    >
      <rect
        x={130}
        y={26}
        width={180}
        height={34}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
        stroke="var(--kfam-fusion)"
        strokeWidth={1.5}
        strokeDasharray="6 2 2 2"
      />
      <text x={220} y={41} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        termite_gliner_boundary_f32
      </text>
      <text x={220} y={54} textAnchor="middle" fontSize={7.5} className="fill-primary font-mono">
        Params{"{"}kind, dims[8], scalars[4]{"}"}
      </text>
      {KIND_SAMPLE.map((k, i) => {
        const col = i % 4;
        const row = Math.floor(i / 4);
        const x = 36 + col * 96;
        const y = 96 + row * 34;
        return (
          <path
            key={`line-${k}`}
            d={`M 220 60 L ${x + 44} ${y}`}
            stroke="var(--border)"
            strokeWidth={0.6}
          />
        );
      })}
      {KIND_SAMPLE.map((k, i) => {
        const col = i % 4;
        const row = Math.floor(i / 4);
        const x = 36 + col * 96;
        const y = 96 + row * 34;
        return (
          <g key={k}>
            <rect
              x={x}
              y={y}
              width={88}
              height={20}
              rx={3}
              fill="color-mix(in oklch, var(--kfam-attention) 12%, var(--background))"
              stroke="var(--kfam-attention)"
              strokeWidth={0.8}
            />
            <text
              x={x + 44}
              y={y + 13}
              textAnchor="middle"
              fontSize={6.8}
              className="fill-foreground font-mono"
            >
              {k}
            </text>
          </g>
        );
      })}
      <text
        x={220}
        y={218}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        + 30 more kinds · unchecked descriptors never reach Metal
      </text>
    </Figure>
  );
}

export function PrecisionPolicyFigure() {
  const rows = [
    {
      role: "encoder matrices (declared)",
      policy: "FP32 · FP16 · Q8_0 · Q4_K/Q4_0 by variant",
      ok: true,
    },
    { role: "task heads (boundary/cls/rel/rec)", policy: "FP32, always", ok: false },
    { role: "biases · norms · rel-position table", policy: "FP32, always", ok: false },
  ];
  return (
    <div className="flex h-full flex-col justify-center gap-3">
      <div className="overflow-hidden rounded-lg border">
        {rows.map((r) => (
          <div
            key={r.role}
            className="grid grid-cols-2 border-b font-mono text-[11px] last:border-b-0"
          >
            <div className="border-r p-2.5 text-muted-foreground">{r.role}</div>
            <div
              className="p-2.5"
              style={{ color: r.ok ? "var(--dtype-text-q8)" : "var(--dtype-text-f32)" }}
            >
              {r.policy}
            </div>
          </div>
        ))}
      </div>
      <p className="text-center font-mono text-[11px] text-muted-foreground">
        per-tensor policy, not per-model: "Q8_0 small" still means FP32 heads · Q4_K is rejected for
        small, Q4_0 for base/multi
      </p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 8 — training + adapters                                          */
/* ------------------------------------------------------------------ */

export function TrainingFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 210"
      title={step === 0 ? "LoRA and DoRA in the graph" : "the native trainer"}
      caption={
        step === 0
          ? "Schematic: adapters inject beside frozen base weights. DoRA recomputes its magnitude norm from live weights each forward and detaches it through the stop_gradient intrinsic, so lowering cannot lose the boundary."
          : "Schematic training step: AdamW with partial-window renormalization; Hungarian assignment matches predicted record instances to gold before the loss."
      }
    >
      {step === 0 && (
        <>
          <rect
            x={40}
            y={60}
            width={130}
            height={26}
            rx={4}
            fill="color-mix(in oklch, var(--dtype-f16) 16%, transparent)"
            stroke="var(--dtype-f16)"
            strokeWidth={1.25}
          />
          <text
            x={105}
            y={77}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            frozen base W
          </text>
          <rect
            x={40}
            y={110}
            width={130}
            height={26}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-sampling) 16%, transparent)"
            stroke="var(--kfam-sampling)"
            strokeWidth={1.25}
          />
          <text
            x={105}
            y={127}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            B·A (rank r, α)
          </text>
          <path
            d="M 170 73 L 240 96 M 170 123 L 240 100"
            stroke="var(--muted-foreground)"
            strokeWidth={1}
          />
          <rect
            x={244}
            y={86}
            width={70}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
            stroke="var(--kfam-fusion)"
            strokeWidth={1.25}
          />
          <text
            x={279}
            y={102}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            + → h
          </text>
          <rect
            x={330}
            y={62}
            width={90}
            height={22}
            rx={4}
            fill="none"
            stroke="var(--kfam-kv)"
            strokeWidth={1}
            strokeDasharray="4 2"
          />
          <text
            x={375}
            y={77}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-foreground font-mono"
          >
            DoRA ‖W‖ norm
          </text>
          <path
            d="M 375 84 L 300 90"
            stroke="var(--kfam-kv)"
            strokeWidth={0.9}
            strokeDasharray="3 2"
          />
          <text
            x={375}
            y={100}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            via stop_gradient
          </text>
          <text
            x={220}
            y={172}
            textAnchor="middle"
            fontSize={8}
            className="fill-muted-foreground font-mono"
          >
            targets: public GLiNER aliases or exact module paths · base weights digest-pinned
          </text>
        </>
      )}
      {step === 1 && (
        <>
          {["forward + losses", "Hungarian match records", "AdamW (renorm partial window)"].map(
            (s, i) => (
              <g key={s}>
                <rect
                  x={40}
                  y={54 + i * 40}
                  width={230}
                  height={26}
                  rx={4}
                  fill="color-mix(in oklch, var(--kfam-attention) 12%, transparent)"
                  stroke="var(--kfam-attention)"
                  strokeWidth={1}
                />
                <text
                  x={155}
                  y={71 + i * 40}
                  textAnchor="middle"
                  fontSize={8.5}
                  className="fill-foreground font-mono"
                >
                  {s}
                </text>
                {i < 2 && (
                  <path
                    d="M 155 80 v 14"
                    stroke="var(--muted-foreground)"
                    strokeWidth={1}
                    transform={`translate(0, ${i * 40})`}
                  />
                )}
              </g>
            )
          )}
          <rect
            x={300}
            y={74}
            width={120}
            height={66}
            rx={5}
            fill="color-mix(in oklch, var(--kfam-kv) 12%, transparent)"
            stroke="var(--kfam-kv)"
            strokeWidth={1}
          />
          <text
            x={360}
            y={94}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-foreground font-mono"
          >
            losses: focal marginals
          </text>
          <text
            x={360}
            y={108}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-foreground font-mono"
          >
            soft-IoU · abstention
          </text>
          <text
            x={360}
            y={122}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-foreground font-mono"
          >
            count · listwise rerank
          </text>
          <text
            x={220}
            y={186}
            textAnchor="middle"
            fontSize={8}
            className="fill-muted-foreground font-mono"
          >
            antfly inference finetune → train run gliner25 · checkpoints + resume are digest-pinned
          </text>
        </>
      )}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 9 — qualification gate + variants                                */
/* ------------------------------------------------------------------ */

export function QualificationGateFigure() {
  const gates = [
    { label: "architecture recognized", detail: "detectArchitecture → boundary", closed: true },
    { label: "runtime_available", detail: "= false (architecture-level)", closed: false },
    { label: "qualification profiles", detail: "production_entries = { }", closed: false },
  ];
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      <div className="flex flex-col gap-2">
        {gates.map((g) => (
          <div
            key={g.label}
            className={cn(
              "flex flex-wrap items-center justify-between gap-x-3 gap-y-1 rounded-md border px-3 py-2.5 font-mono text-[11px]",
              g.closed ? "border-primary/60" : "border-dashed"
            )}
          >
            <span className={g.closed ? "text-foreground" : "text-muted-foreground"}>
              {g.label}
            </span>
            <span className="text-muted-foreground">{g.detail}</span>
            <span
              className="shrink-0 rounded px-1.5 py-0.5 text-[10px] font-semibold"
              style={{
                background: `color-mix(in oklch, ${g.closed ? "var(--kfam-attention)" : "var(--kfam-kv)"} 18%, transparent)`,
              }}
            >
              {g.closed ? "true" : "false"}
            </span>
          </div>
        ))}
      </div>
      <div className="text-center font-mono text-xs">
        serve = recognized <span className="text-muted-foreground">AND</span> available{" "}
        <span className="text-muted-foreground">AND</span> qualified →{" "}
        <span className="font-semibold" style={{ color: "var(--kfam-text-kv)" }}>
          withheld
        </span>
      </div>
      <p className="text-center font-mono text-[11px] text-muted-foreground">
        all three are code facts, not policy prose · adding a qualification row is a reviewed
        release decision
      </p>
    </div>
  );
}

export function VariantsFigure() {
  const variants = [
    { name: "small", backbone: "deberta-v3-xsmall", hidden: 384, params: 74, w: 74 / 287 },
    { name: "base", backbone: "deberta-v3-base", hidden: 768, params: 194, w: 194 / 287 },
    { name: "multi", backbone: "mdeberta-v3-base", hidden: 768, params: 287, w: 1 },
  ];
  return (
    <div className="flex h-full flex-col justify-center gap-3">
      {variants.map((v) => (
        <div key={v.name} className="flex items-center gap-3">
          <div className="w-12 text-right font-mono text-[11px] text-muted-foreground">
            {v.name}
          </div>
          <div className="flex-1">
            <div
              className="h-6 rounded"
              style={{
                width: `${Math.round(v.w * 100)}%`,
                background: `color-mix(in oklch, var(--kfam-attention) ${28 + v.w * 30}%, transparent)`,
                boxShadow: "inset 0 0 0 1px var(--kfam-attention)",
              }}
            />
          </div>
          <div className="w-40 font-mono text-[10px] text-muted-foreground">
            ~{v.params}M · H{v.hidden} · {v.backbone}
          </div>
        </div>
      ))}
      <p className="pt-1 text-center font-mono text-[11px] text-muted-foreground">
        parameter counts are inventory-derived approximations · identical boundary-head config, 334
        tensors each
      </p>
    </div>
  );
}
