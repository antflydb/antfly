"use client";

import { useState } from "react";
import { Figure } from "@/components/viz/glyphs";

/* ------------------------------------------------------------------ */
/* Ch 1 — pixels become tokens                                         */
/* ------------------------------------------------------------------ */

export function PixelsToTokensFigure() {
  const cells: React.ReactNode[] = [];
  for (let r = 0; r < 6; r++) {
    for (let c = 0; c < 6; c++) {
      cells.push(
        <rect
          key={`${r}-${c}`}
          x={30 + c * 20}
          y={30 + r * 20}
          width={17}
          height={17}
          rx={2}
          fill="var(--kfam-attention)"
          opacity={0.25 + ((r + c) % 4) * 0.14}
        />,
      );
    }
  }
  return (
    <Figure
      viewBox="0 0 440 210"
      title="an image enters the token stream"
      caption="16×16-pixel patches (×2 temporal frames) become patch vectors; after the 2×2 merger, four patches make one visual token. A 768×768 resized image is 576 visual tokens. Decoder KV cost is per token; vision processing adds separate work."
    >
      {cells}
      <text x={88} y={22} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono">
        pixels → 16×16 patches
      </text>
      <path d="M 160 90 h 40" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <text x={180} y={82} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">
        tower
      </text>
      {/* token stream */}
      {["The", "chart", "<img1>", "<img2>", "<img3>", "shows", "…"].map((t, i) => {
        const visual = t.startsWith("<img");
        return (
          <g key={t}>
            <rect
              x={210 + i * 30}
              y={78}
              width={27}
              height={22}
              rx={4}
              fill={visual ? "color-mix(in oklch, var(--kfam-attention) 25%, transparent)" : "color-mix(in oklch, var(--dtype-f16) 15%, transparent)"}
              stroke={visual ? "var(--kfam-attention)" : "var(--dtype-f16)"}
              strokeWidth={1}
            />
            <text x={210 + i * 30 + 13.5} y={92} textAnchor="middle" fontSize={6.5} className="fill-foreground font-mono">
              {visual ? "▦" : t}
            </text>
          </g>
        );
      })}
      <text x={315} y={126} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
        one interleaved sequence → 28-layer Qwen3 decoder
      </text>
      <rect x={210} y={140} width={205} height={30} rx={5} fill="none" stroke="var(--kfam-matvec)" strokeWidth={1.25} />
      <text x={312} y={159} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        same gpt.zig spine as Gemma4 / Qwen3
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 2 — the vision tower                                             */
/* ------------------------------------------------------------------ */

export function VisionTowerFigure({ step }: { step: 0 | 1 | 2 }) {
  return (
    <Figure
      viewBox="0 0 440 250"
      title={
        step === 0
          ? "Conv3D patch embed + interpolated positions"
          : step === 1
            ? "the 2×2 merger: a 4-into-1 zipper"
            : "DeepStack: features exit at three depths"
      }
    >
      {step === 0 && (
        <>
          {/* patch cube */}
          <g>
            <rect x={50} y={70} width={54} height={54} rx={3} fill="color-mix(in oklch, var(--kfam-attention) 22%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1.25} />
            <rect x={62} y={58} width={54} height={54} rx={3} fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1.25} />
            <text x={88} y={145} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
              16×16 ×2 frames
            </text>
            <text x={88} y={157} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
              (stills duplicate their frame)
            </text>
          </g>
          <path d="M 130 90 h 46" stroke="var(--muted-foreground)" strokeWidth={1.25} />
          <rect x={180} y={72} width={110} height={36} rx={4} fill="color-mix(in oklch, var(--kfam-matvec) 14%, transparent)" stroke="var(--kfam-matvec)" strokeWidth={1.25} />
          <text x={235} y={88} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">Conv3D →</text>
          <text x={235} y={100} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">[patches, 1024]</text>
          <path d="M 290 90 h 46" stroke="var(--muted-foreground)" strokeWidth={1.25} />
          <rect x={340} y={72} width={86} height={36} rx={4} fill="color-mix(in oklch, var(--kfam-fusion) 14%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.25} />
          <text x={383} y={88} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">+ positions</text>
          <text x={383} y={100} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">48×48 grid, interp.</text>
          <text x={235} y={200} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            the learned position table is 2,304 entries; any actual patch grid is bilinearly interpolated from it
          </text>
        </>
      )}
      {step === 1 && (
        <>
          {[0, 1, 2, 3].map((i) => (
            <rect
              key={i}
              x={60 + (i % 2) * 30}
              y={70 + Math.floor(i / 2) * 30}
              width={26}
              height={26}
              rx={3}
              fill="color-mix(in oklch, var(--kfam-attention) 22%, transparent)"
              stroke="var(--kfam-attention)"
              strokeWidth={1}
            />
          ))}
          <text x={88} y={145} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            2×2 patch block
          </text>
          {/* zipper lines */}
          {[0, 1, 2, 3].map((i) => (
            <path
              key={i}
              d={`M ${86 + (i % 2) * 30} ${83 + Math.floor(i / 2) * 30} C 150 ${83 + Math.floor(i / 2) * 30}, 160 98, 200 98`}
              fill="none"
              stroke="var(--kfam-attention)"
              strokeWidth={1}
              opacity={0.7}
            />
          ))}
          <rect x={200} y={80} width={104} height={36} rx={4} fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.25} strokeDasharray="6 2 2 2" />
          <text x={252} y={96} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">concat [4096]</text>
          <text x={252} y={108} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">→ MLP</text>
          <path d="M 304 98 h 40" stroke="var(--muted-foreground)" strokeWidth={1.25} />
          <rect x={348} y={80} width={78} height={36} rx={4} fill="color-mix(in oklch, var(--dtype-f16) 14%, transparent)" stroke="var(--dtype-f16)" strokeWidth={1.25} />
          <text x={387} y={96} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">1 token</text>
          <text x={387} y={108} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">[2048]</text>
          <text x={240} y={200} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            4× fewer tokens into the decoder; spatial detail survives in the concat
          </text>
        </>
      )}
      {step === 2 && (
        <>
          {/* tower blocks */}
          {Array.from({ length: 12 }, (_, i) => i).map((i) => {
            const tap = i === 2 || i === 5 || i === 8; // blocks 5/11/17 of 24, compressed to 12 rows
            return (
              <g key={i}>
                <rect
                  x={60}
                  y={20 + i * 16}
                  width={120}
                  height={12}
                  rx={2}
                  fill="var(--kfam-attention)"
                  opacity={tap ? 0.8 : 0.3}
                />
                {tap && (
                  <path
                    d={`M 180 ${26 + i * 16} C 260 ${26 + i * 16}, 280 ${60 + i * 10}, 330 ${60 + i * 10}`}
                    fill="none"
                    stroke="var(--kfam-fusion)"
                    strokeWidth={2}
                    opacity={0.85}
                  />
                )}
              </g>
            );
          })}
          <text x={120} y={225} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            24 vision blocks · tap indices 5, 11, 17
          </text>
          <rect x={330} y={70} width={90} height={70} rx={5} fill="color-mix(in oklch, var(--kfam-matvec) 12%, transparent)" stroke="var(--kfam-matvec)" strokeWidth={1.25} />
          <text x={375} y={95} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">early decoder</text>
          <text x={375} y={107} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">layers</text>
          <text x={375} y={126} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">+= deepstack[i]</text>
          <text x={240} y={200} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            mid-tower features ride ribbons into the decoder — not as tokens
          </text>
        </>
      )}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 3 — m-RoPE: three clocks (interactive)                           */
/* ------------------------------------------------------------------ */

/** Token layout: 4 text, then a 4×4 image grid (16 tokens), then 4 text. */
const GRID = 4;
const N_TOKENS = 4 + GRID * GRID + 4;

function mropeAxes(i: number): [number, number, number] {
  if (i < 4) return [i, i, i];
  if (i < 4 + GRID * GRID) {
    const p = i - 4;
    const h = Math.floor(p / GRID);
    const w = p % GRID;
    // all image tokens share the same temporal position (4 = text position of the image)
    return [4, 4 + h, 4 + w];
  }
  // text resumes past max(t, h, w) of the grid
  const back = i - (4 + GRID * GRID);
  return [4 + GRID + back, 4 + GRID + back, 4 + GRID + back];
}

function Dial({ x, label, value, max, color }: { x: number; label: string; value: number; max: number; color: string }) {
  const angle = (value / max) * 2 * Math.PI - Math.PI / 2;
  const cx = x;
  const cy = 52;
  const r = 26;
  return (
    <g>
      <circle cx={cx} cy={cy} r={r} fill="none" stroke="var(--border)" strokeWidth={1.5} />
      <line x1={cx} y1={cy} x2={cx + r * 0.8 * Math.cos(angle)} y2={cy + r * 0.8 * Math.sin(angle)} stroke={color} strokeWidth={2.5} strokeLinecap="round" />
      <circle cx={cx} cy={cy} r={2.5} fill={color} />
      <text x={cx} y={cy + r + 14} textAnchor="middle" fontSize={9} fill={color.replace("--kfam-", "--kfam-text-").replace("--dtype-", "--dtype-text-")} className="font-mono font-semibold">
        {label}
      </text>
      <text x={cx} y={cy + r + 26} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono tabular-nums">
        {value}
      </text>
    </g>
  );
}

export function MRopeClocksFigure() {
  const [pos, setPos] = useState(9);
  const [t, h, w] = mropeAxes(pos);
  const maxAxis = 4 + GRID + 4;
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      <svg viewBox="0 0 420 116" className="w-full" role="img">
        <title>three m-RoPE position dials</title>
        <Dial x={90} label="temporal · t" value={t} max={maxAxis} color="var(--dtype-f16)" />
        <Dial x={210} label="image · h" value={h} max={maxAxis} color="var(--kfam-attention)" />
        <Dial x={330} label="image · w" value={w} max={maxAxis} color="var(--kfam-fusion)" />
      </svg>
      {/* token strip */}
      <svg viewBox="0 0 420 64" className="w-full" role="group" aria-label="Choose a sequence token">
        <title>token sequence: text, image grid, text</title>
        <g role="radiogroup" aria-label="Sequence token">
        {Array.from({ length: N_TOKENS }, (_, i) => i).map((i) => {
          const isImg = i >= 4 && i < 4 + GRID * GRID;
          const x = 10 + i * 16.5;
          return (
            <rect
              key={i}
              x={x}
              y={i === pos ? 14 : 18}
              width={14}
              height={i === pos ? 34 : 26}
              rx={3}
              fill={isImg ? "color-mix(in oklch, var(--kfam-attention) 30%, transparent)" : "color-mix(in oklch, var(--dtype-f16) 20%, transparent)"}
              stroke={i === pos ? "var(--primary)" : "none"}
              strokeWidth={1.5}
              role="radio"
              aria-label={`Token ${i}, ${isImg ? "visual" : "text"}`}
              aria-checked={i === pos}
              data-token={i}
              tabIndex={i === pos ? 0 : -1}
              onClick={() => setPos(i)}
              onFocus={() => setPos(i)}
              onKeyDown={(event) => {
                const next = event.key === "ArrowRight" || event.key === "ArrowDown"
                  ? (i + 1) % N_TOKENS
                  : event.key === "ArrowLeft" || event.key === "ArrowUp"
                    ? (i + N_TOKENS - 1) % N_TOKENS
                    : event.key === "Home" ? 0 : event.key === "End" ? N_TOKENS - 1 : i;
                if (next !== i || event.key === " " || event.key === "Enter") {
                  event.preventDefault();
                  setPos(next);
                  event.currentTarget.ownerSVGElement?.querySelector<SVGRectElement>(`[data-token="${next}"]`)?.focus();
                }
              }}
              className="cursor-pointer focus-visible:outline-2 focus-visible:outline-primary"
            />
          );
        })}
        </g>
        <text x={10} y={62} fontSize={8} className="fill-muted-foreground font-mono">text</text>
        <text x={10 + 4 * 16.5} y={62} fontSize={8} className="fill-muted-foreground font-mono">4×4 image grid</text>
        <text x={10 + (4 + GRID * GRID) * 16.5} y={62} fontSize={8} className="fill-muted-foreground font-mono">text</text>
      </svg>
      <input
        type="range"
        min={0}
        max={N_TOKENS - 1}
        step={1}
        value={pos}
        onChange={(e) => setPos(Number(e.target.value))}
        className="w-full accent-[var(--primary)]"
        aria-label="sequence position"
      />
      <p className="text-center font-mono text-[10px] text-muted-foreground">
        schematic sequence (boundary tokens omitted): text advances all axes; the image advances h/w
        while t stays fixed. Token {pos}: t={t}, h={h}, w={w}.
      </p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 4 — two-axis (tower) vs three-axis (decoder)                     */
/* ------------------------------------------------------------------ */

export function TwoVsThreeAxisFigure() {
  return (
    <Figure
      viewBox="0 0 440 220"
      title="two rotations in the tower, three in the decoder"
      caption="Vision RoPE uses patch row and column within each image. Decoder m-RoPE uses temporal/height/width streams, offset by preceding content. In text all three advance together. This diagram illustrates still-image input, not video support."
    >
      {/* left panel */}
      <rect x={25} y={30} width={180} height={150} rx={6} fill="none" stroke="var(--border)" strokeWidth={1} />
      <text x={115} y={50} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">vision tower</text>
      {[0, 1].map((i) => (
        <g key={i}>
          <circle cx={75 + i * 80} cy={105} r={24} fill="none" stroke={i === 0 ? "var(--kfam-attention)" : "var(--kfam-fusion)"} strokeWidth={1.5} />
          <line
            x1={75 + i * 80}
            y1={105}
            x2={75 + i * 80 + 17 * Math.cos(i === 0 ? -0.9 : 0.4)}
            y2={105 + 17 * Math.sin(i === 0 ? -0.9 : 0.4)}
            stroke={i === 0 ? "var(--kfam-attention)" : "var(--kfam-fusion)"}
            strokeWidth={2}
          />
          <text x={75 + i * 80} y={148} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            {i === 0 ? "row (h)" : "col (w)"}
          </text>
        </g>
      ))}
      <text x={115} y={170} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">
        bidirectional · per image
      </text>
      {/* right panel */}
      <rect x={235} y={30} width={180} height={150} rx={6} fill="none" stroke="var(--primary)" strokeWidth={1.25} />
      <text x={325} y={50} textAnchor="middle" fontSize={9} className="fill-primary font-mono">decoder m-RoPE</text>
      {[0, 1, 2].map((i) => (
        <g key={i}>
          <circle cx={275 + i * 50} cy={105} r={18} fill="none" stroke={["var(--dtype-f16)", "var(--kfam-attention)", "var(--kfam-fusion)"][i]} strokeWidth={1.5} />
          <line
            x1={275 + i * 50}
            y1={105}
            x2={275 + i * 50 + 13 * Math.cos(-1.2 + i * 0.8)}
            y2={105 + 13 * Math.sin(-1.2 + i * 0.8)}
            stroke={["var(--dtype-f16)", "var(--kfam-attention)", "var(--kfam-fusion)"][i]}
            strokeWidth={2}
          />
          <text x={275 + i * 50} y={140} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            {["t", "h", "w"][i]}
          </text>
        </g>
      ))}
      <text x={325} y={170} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">
        causal · whole conversation
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 6 — the reranker variant                                         */
/* ------------------------------------------------------------------ */

export function RerankerFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="reranking = one forward pass, two logits"
      caption='The prompt frames relevance as a yes/no question; the score is sigmoid(logit_yes − logit_no) read from the final hidden state — the model never generates a token.'
    >
      {["query", "document", "image"].map((part, i) => (
        <g key={part}>
          <rect
            x={30}
            y={30 + i * 38}
            width={110}
            height={26}
            rx={4}
            fill={`color-mix(in oklch, ${["var(--dtype-f16)", "var(--kfam-matvec)", "var(--kfam-attention)"][i]} 16%, transparent)`}
            stroke={["var(--dtype-f16)", "var(--kfam-matvec)", "var(--kfam-attention)"][i]}
            strokeWidth={1.25}
          />
          <text x={85} y={47 + i * 38} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
            {part}
          </text>
          <path d={`M 140 ${43 + i * 38} C 180 ${43 + i * 38}, 185 88, 215 88`} fill="none" stroke="var(--muted-foreground)" strokeWidth={1} />
        </g>
      ))}
      <rect x={215} y={70} width={100} height={36} rx={5} fill="color-mix(in oklch, var(--kfam-attention) 12%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1.25} />
      <text x={265} y={86} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">28-layer</text>
      <text x={265} y={98} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">decoder</text>
      <path d="M 315 88 h 30" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect x={349} y={56} width={72} height={26} rx={4} fill="color-mix(in oklch, var(--kfam-sampling) 18%, transparent)" stroke="var(--kfam-sampling)" strokeWidth={1.25} />
      <text x={385} y={73} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">"yes"</text>
      <rect x={349} y={94} width={72} height={26} rx={4} fill="none" stroke="var(--muted-foreground)" strokeWidth={1} strokeDasharray="4 3" />
      <text x={385} y={111} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono">"no"</text>
      <text x={265} y={160} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        score = σ(logit_yes − logit_no)
      </text>
      <text x={265} y={176} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">
        two-row semantic head; no sampling or generation
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 7 — spine notes                                                  */
/* ------------------------------------------------------------------ */

export function VlSpineNotesFigure() {
  const rows = [
    { name: "termite_apply_mrope", note: "3-axis rotary positions" },
    { name: "vision tower + merger", note: "Conv3D path, per-image" },
    { name: "DeepStack injection", note: "adds features in early layers" },
    { name: "paged GQA attention", note: "shared decoder primitive" },
    { name: "q4_k matvec routes", note: "shared, compiler-generated" },
  ];
  return (
    <div className="flex h-full flex-col justify-center gap-2.5">
      <div className="font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
        Qwen3-VL features and shared operations
      </div>
      {rows.map((r, i) => (
        <div key={r.name} className="flex items-center justify-between rounded-md border px-3 py-2">
          <span className="font-mono text-xs">{r.name}</span>
          <span className={i < 3 ? "font-mono text-[10px] text-primary" : "font-mono text-[10px] text-muted-foreground"}>
            {r.note}
          </span>
        </div>
      ))}
    </div>
  );
}
