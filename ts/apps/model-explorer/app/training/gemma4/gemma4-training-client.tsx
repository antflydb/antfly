"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { Figure } from "@/components/viz/glyphs";
import { L } from "@/lib/links";

/* ------------------------------------------------------------------ */
/* Figures                                                             */
/* ------------------------------------------------------------------ */

const LORA_TARGETS = ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"];

function LoraAdapterStackFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 220"
      title={step === 0 ? "a frozen model, a thin adapter" : "the trainer underneath"}
      caption={
        step === 0
          ? "Schematic LoRA: the base weight matrix stays frozen; a low-rank A×B pair (rank 16 by default, 8 for GRPO; alpha 32) learns beside it, across all seven Gemma target modules."
          : "Schematic: RealAutodiffTrainer drives every preference step — constant LR 1e-4, AdamW, global-norm clip at 1.0, seed 42, all weights host-resident."
      }
    >
      <rect
        x={60}
        y={40}
        width={170}
        height={90}
        rx={6}
        fill="color-mix(in oklch, var(--dtype-f16) 14%, transparent)"
        stroke="var(--dtype-f16)"
        strokeWidth={1.25}
      />
      <text
        x={145}
        y={88}
        textAnchor="middle"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        frozen W
      </text>
      <rect
        x={252}
        y={40}
        width={26}
        height={90}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-sampling) 20%, transparent)"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.25}
      />
      <text x={265} y={88} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        A
      </text>
      <rect
        x={290}
        y={72}
        width={90}
        height={26}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-sampling) 20%, transparent)"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.25}
      />
      <text x={335} y={89} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        B (zero-init)
      </text>
      {step === 0 ? (
        <g>
          {LORA_TARGETS.map((t, i) => (
            <g key={t}>
              <rect
                x={30 + i * 55}
                y={158}
                width={50}
                height={18}
                rx={3}
                fill="color-mix(in oklch, var(--kfam-matvec) 14%, var(--background))"
                stroke="var(--kfam-matvec)"
                strokeWidth={0.9}
              />
              <text
                x={55 + i * 55}
                y={170}
                textAnchor="middle"
                fontSize={6.5}
                className="fill-foreground font-mono"
              >
                {t}
              </text>
            </g>
          ))}
          <text
            x={220}
            y={196}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            preset all-linear: every one of the seven modules gets an adapter
          </text>
        </g>
      ) : (
        <g>
          <rect
            x={90}
            y={152}
            width={260}
            height={30}
            rx={5}
            fill="color-mix(in oklch, var(--kfam-matvec) 14%, transparent)"
            stroke="var(--kfam-matvec)"
            strokeWidth={1.25}
          />
          <text
            x={220}
            y={171}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            RealAutodiffTrainer · LR 1e-4 · clip 1.0 · seed 42
          </text>
          <text
            x={220}
            y={200}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            BackendKind = enum {"{ native }"} — the whole loop is host-resident CPU
          </text>
        </g>
      )}
    </Figure>
  );
}

const DPO_LAYERS = [
  {
    id: "loss",
    label: "preference_loss.zig",
    sub: "math only — 6 losses, analytic grads, gradient tests",
    color: "var(--kfam-attention)",
  },
  {
    id: "harness",
    label: "preference_harness.zig",
    sub: "gradients, no optimizer — calls the model 4×",
    color: "var(--kfam-fusion)",
  },
  {
    id: "recipe",
    label: "recipe.zig · runOptimizerBackedGemmaDpo",
    sub: "optimizer + checkpoints — saves a LoRA bundle",
    color: "var(--kfam-matvec)",
  },
];

function DpoThreeLayerFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="DPO in three strict layers"
      caption="Schematic layering: pure loss math on top, a model-agnostic gradient harness in the middle, the optimizer-backed Gemma trainer at the bottom. The trainer calls the loss math directly; the harness powers the scoring-only path."
    >
      {DPO_LAYERS.map((l, i) => (
        <g key={l.id}>
          <rect
            x={50}
            y={26 + i * 58}
            width={340}
            height={40}
            rx={5}
            fill={`color-mix(in oklch, ${l.color} 14%, transparent)`}
            stroke={l.color}
            strokeWidth={1.25}
          />
          <text
            x={220}
            y={43 + i * 58}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            {l.label}
          </text>
          <text
            x={220}
            y={57 + i * 58}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            {l.sub}
          </text>
          {i < 2 && (
            <path
              d={`M 220 ${66 + i * 58} v 18`}
              stroke="var(--muted-foreground)"
              strokeWidth={1.25}
            />
          )}
        </g>
      ))}
    </Figure>
  );
}

const SIBLING_LOSSES = ["ipo", "kto", "simpo", "orpo", "cpo"];

function DpoLossFigure() {
  // Plot the actual loss −log σ(margin): high at negative margin, → 0 as it grows.
  const pts: string[] = [];
  for (let i = 0; i <= 40; i++) {
    const x = -6 + (12 * i) / 40;
    const loss = -Math.log(1 / (1 + Math.exp(-x)));
    pts.push(`${60 + i * 6},${120 - Math.min(70, loss * 12)}`);
  }
  return (
    <Figure
      viewBox="0 0 440 230"
      title="−logSigmoid(β·Δc − β·Δr), β = 0.1"
      caption="Schematic loss curve, not a training trace. Δc/Δr are the chosen/rejected log-probability gaps between policy and reference, each scaled once by β; no label smoothing, no rpo_alpha. Five sibling losses exist in the library but the recipe only ever builds DPO."
    >
      <line x1={60} y1={120} x2={300} y2={120} stroke="var(--muted-foreground)" strokeWidth={1} />
      <line
        x1={180}
        y1={40}
        x2={180}
        y2={135}
        stroke="var(--border)"
        strokeWidth={0.75}
        strokeDasharray="3 3"
      />
      <polyline points={pts.join(" ")} fill="none" stroke="var(--kfam-attention)" strokeWidth={2} />
      <circle cx={135} cy={120} r={4} fill="var(--kfam-kv)" />
      <circle cx={245} cy={120} r={4} fill="var(--kfam-fusion)" />
      <path d="M 139 132 h 102" stroke="var(--muted-foreground)" strokeWidth={1} />
      <text
        x={190}
        y={144}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        reward margin
      </text>
      <text
        x={135}
        y={158}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        rejected
      </text>
      <text
        x={245}
        y={158}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        chosen
      </text>
      <rect
        x={310}
        y={52}
        width={110}
        height={110}
        rx={5}
        fill="none"
        stroke="var(--border)"
        strokeWidth={1}
        strokeDasharray="6 3"
      />
      <text
        x={365}
        y={68}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        library-only
      </text>
      {SIBLING_LOSSES.map((s, i) => (
        <g key={s} opacity={0.7}>
          <rect
            x={325}
            y={76 + i * 17}
            width={80}
            height={13}
            rx={3}
            fill="color-mix(in oklch, var(--muted-foreground) 12%, transparent)"
            stroke="var(--muted-foreground)"
            strokeWidth={0.6}
          />
          <text
            x={365}
            y={86 + i * 17}
            textAnchor="middle"
            fontSize={7}
            className="fill-foreground font-mono"
          >
            {s}
          </text>
        </g>
      ))}
    </Figure>
  );
}

function ReferencePolicyFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="two ways to conjure a frozen reference"
      caption="Schematic: the harness default re-uses the policy with its adapter toggled off; the Gemma recipe path loads a second full model from disk instead. Both live entirely in host RAM."
    >
      <text
        x={115}
        y={30}
        textAnchor="middle"
        fontSize={9}
        className="fill-muted-foreground font-mono"
      >
        adapter-disable trick
      </text>
      <rect
        x={45}
        y={44}
        width={140}
        height={70}
        rx={6}
        fill="color-mix(in oklch, var(--kfam-matvec) 14%, transparent)"
        stroke="var(--kfam-matvec)"
        strokeWidth={1.25}
      />
      <text x={115} y={74} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        policy weights
      </text>
      <rect
        x={150}
        y={50}
        width={44}
        height={16}
        rx={3}
        fill="color-mix(in oklch, var(--kfam-sampling) 22%, transparent)"
        stroke="var(--kfam-sampling)"
        strokeWidth={1}
        transform="rotate(18 172 58)"
      />
      <text x={186} y={44} fontSize={6.5} className="fill-muted-foreground font-mono">
        LoRA off ↷
      </text>
      <text
        x={115}
        y={132}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        same weights, called twice
      </text>
      <text x={330} y={30} textAnchor="middle" fontSize={9} className="fill-primary font-mono">
        second LoadedModel
      </text>
      <rect
        x={250}
        y={44}
        width={72}
        height={70}
        rx={6}
        fill="color-mix(in oklch, var(--kfam-matvec) 14%, transparent)"
        stroke="var(--kfam-matvec)"
        strokeWidth={1.25}
      />
      <text x={286} y={82} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        policy
      </text>
      <rect
        x={338}
        y={44}
        width={72}
        height={70}
        rx={6}
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={1.25}
        strokeDasharray="5 3"
      />
      <rect
        x={362}
        y={58}
        width={24}
        height={16}
        rx={2}
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={1}
      />
      <path
        d="M 368 58 v -5 a 6 6 0 0 1 12 0 v 5"
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={1}
      />
      <text
        x={374}
        y={100}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-foreground font-mono"
      >
        reference
      </text>
      <text
        x={330}
        y={132}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        loaded from disk, frozen
      </text>
      <text
        x={220}
        y={172}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        DPO / IPO / KTO require reference logps — MissingReferenceLogps otherwise;
      </text>
      <text
        x={220}
        y={186}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        SimPO / ORPO / CPO are reference-free by design
      </text>
    </Figure>
  );
}

const CE_CELLS = [0, 1, 2, 3, 4, 5, 6, 7].map((i) => ({
  id: `cell-${i}`,
  x: 60 + i * 36,
  hot: i === 4,
}));

function CeInjectionFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="a DPO gradient in a cross-entropy costume"
      caption="Schematic of a scaling identity, not a data-flow trace: scaling the one-hot CE target row by −grad·seq_len makes the generic CE backward pass reproduce exactly the preference gradient."
    >
      {CE_CELLS.map((c) => (
        <rect
          key={c.id}
          x={c.x}
          y={50}
          width={30}
          height={22}
          rx={3}
          fill={
            c.hot
              ? "color-mix(in oklch, var(--kfam-fusion) 30%, transparent)"
              : "color-mix(in oklch, var(--muted-foreground) 10%, transparent)"
          }
          stroke={c.hot ? "var(--kfam-fusion)" : "var(--border)"}
          strokeWidth={c.hot ? 1.5 : 0.75}
        />
      ))}
      <text
        x={220}
        y={40}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        one-hot CE target row (the sequence's tokens)
      </text>
      <circle
        cx={220}
        cy={110}
        r={20}
        fill="none"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.5}
      />
      <path d="M 220 96 v 10 M 220 110 l 8 -6" stroke="var(--kfam-sampling)" strokeWidth={1.25} />
      <text x={220} y={144} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        token_scale_override = −grad · seq_len
      </text>
      <path d="M 220 72 v 16 M 220 152 v 12" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <text
        x={220}
        y={180}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-foreground font-mono"
      >
        CE backward ⇒ exactly the DPO gradient
      </text>
      <text
        x={220}
        y={196}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        chosen and rejected each take a trainer step — grad-accum runs ×2
      </text>
    </Figure>
  );
}

// y maps reward value to height: y = 82 − v·48 (higher reward drawn higher).
const GROUP_REWARDS = [
  { id: "r0", y: 82, v: "0.0" },
  { id: "r1", y: 58, v: "0.5" },
  { id: "r2", y: 67.6, v: "0.3" },
  { id: "r3", y: 34, v: "1.0" },
];
const GROUP_MEAN_Y = 60.4; // mean reward 0.45 on the same scale

function GrpoGroupFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="advantages, normalized inside the group"
      caption="Schematic: each prompt's completions form a group; rewards are centered on the group mean and divided by the group std (+1e-8). The group mean is the whole baseline — there is no value function."
    >
      <text
        x={110}
        y={26}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        raw rewards (one group)
      </text>
      <line
        x1={60}
        y1={GROUP_MEAN_Y}
        x2={160}
        y2={GROUP_MEAN_Y}
        stroke="var(--muted-foreground)"
        strokeWidth={0.9}
        strokeDasharray="4 3"
      />
      <text x={168} y={GROUP_MEAN_Y + 3} fontSize={7} className="fill-muted-foreground font-mono">
        mean
      </text>
      {GROUP_REWARDS.map((r, i) => (
        <g key={r.id}>
          <circle cx={75 + i * 26} cy={r.y} r={5} fill="var(--kfam-attention)" opacity={0.85} />
          <text
            x={75 + i * 26}
            y={r.y - 10}
            textAnchor="middle"
            fontSize={6.5}
            className="fill-muted-foreground font-mono"
          >
            {r.v}
          </text>
        </g>
      ))}
      <path d="M 200 62 h 40" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <text
        x={220}
        y={54}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        −μ, ÷σ
      </text>
      <text x={330} y={26} textAnchor="middle" fontSize={8.5} className="fill-primary font-mono">
        advantages
      </text>
      <line x1={260} y1={62} x2={410} y2={62} stroke="var(--muted-foreground)" strokeWidth={1} />
      {GROUP_REWARDS.map((r, i) => (
        <circle
          key={`a-${r.id}`}
          cx={285 + i * 34}
          cy={62 + (r.y - GROUP_MEAN_Y) * 1.2}
          r={5}
          fill="var(--kfam-fusion)"
          opacity={0.9}
        />
      ))}
      <text
        x={220}
        y={140}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        group_size: 8 in the loss struct and scalar route — but 2 on every
      </text>
      <text
        x={220}
        y={154}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        model-backed Gemma route; normalize_advantage defaults on
      </text>
    </Figure>
  );
}

function GrpoLossFigure() {
  const clipPts: string[] = [];
  for (let i = 0; i <= 62; i++) {
    const ratio = 0.5 + i / 40;
    const clipped = Math.min(Math.max(ratio, 0.8), 1.2);
    clipPts.push(`${60 + i * 5},${150 - clipped * 60}`);
  }
  return (
    <Figure
      viewBox="0 0 440 220"
      title="clipped surrogate + a k3 KL leash"
      caption="Schematic curves, not measurements: the probability ratio is clamped to [1−0.2, 1+0.2] before entering the policy-gradient term, and a Schulman k3 estimator (kl_coef 0.04) penalizes drift from the reference."
    >
      <line x1={60} y1={150} x2={370} y2={150} stroke="var(--muted-foreground)" strokeWidth={1} />
      <polyline
        points={clipPts.join(" ")}
        fill="none"
        stroke="var(--kfam-attention)"
        strokeWidth={2}
      />
      <rect
        x={60}
        y={150 - 1.2 * 60}
        width={310}
        height={0.4 * 60}
        fill="color-mix(in oklch, var(--kfam-attention) 8%, transparent)"
      />
      <text
        x={215}
        y={64}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        clip band: ratio ∈ [0.8, 1.2]
      </text>
      {/* k3 is a U with its minimum at Δ=0: lowest value at the center. */}
      <path
        d="M 60 162 q 155 26 310 0"
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={1.5}
        strokeDasharray="5 3"
      />
      <text
        x={215}
        y={196}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        k3 KL vs reference: exp(Δ) − Δ − 1, weight 0.04 · clip_fraction is reported
      </text>
    </Figure>
  );
}

const ROLLOUT_RANKS = [
  { id: "rk0", label: "rank 0 (greedy)", w: 96 },
  { id: "rk1", label: "rank 1", w: 74 },
  { id: "rk2", label: "rank 2", w: 55 },
  { id: "rk3", label: "rank 3", w: 40 },
];

function RolloutRankFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 220"
      title={step === 0 ? "rollouts without dice" : "rewards without a model"}
      caption={
        step === 0
          ? "Schematic: group member i takes the rank-i token at every step — deterministic argmax over sorted logits, capped at min(group, 8). Nothing is drawn from an RNG."
          : "Schematic: rewards are string-match rules over the target — exact match scores 1.0, substring containment 0.5, otherwise 0. No learned reward model exists."
      }
    >
      {step === 0 ? (
        <g>
          {ROLLOUT_RANKS.map((r, i) => (
            <g key={r.id}>
              <rect
                x={60}
                y={36 + i * 34}
                width={r.w}
                height={20}
                rx={3}
                fill="color-mix(in oklch, var(--kfam-matvec) 18%, transparent)"
                stroke="var(--kfam-matvec)"
                strokeWidth={1}
              />
              <path
                d={`M ${60 + r.w} ${46 + i * 34} h ${40}`}
                stroke="var(--muted-foreground)"
                strokeWidth={1}
              />
              <rect
                x={100 + r.w}
                y={36 + i * 34}
                width={150}
                height={20}
                rx={3}
                fill="color-mix(in oklch, var(--kfam-fusion) 12%, transparent)"
                stroke="var(--kfam-fusion)"
                strokeWidth={0.9}
              />
              <text
                x={175 + r.w}
                y={49 + i * 34}
                textAnchor="middle"
                fontSize={7}
                className="fill-foreground font-mono"
              >
                {r.label} → rollout lane {i}
              </text>
            </g>
          ))}
          <rect
            x={352}
            y={70}
            width={40}
            height={40}
            rx={6}
            fill="none"
            stroke="var(--muted-foreground)"
            strokeWidth={1.25}
          />
          <circle cx={364} cy={82} r={2.5} fill="var(--muted-foreground)" />
          <circle cx={380} cy={98} r={2.5} fill="var(--muted-foreground)" />
          <path d="M 348 114 L 396 66" stroke="var(--kfam-kv)" strokeWidth={2} />
          <text
            x={372}
            y={132}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            no sampling
          </text>
          <text
            x={220}
            y={196}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            sorted logits on the left; max_completion_tokens defaults to 4
          </text>
        </g>
      ) : (
        <g>
          {[
            {
              id: "em",
              rule: "exact_match",
              detail: "1.0 exact · 0.5 substring · 0.0 else",
              y: 44,
            },
            { id: "emci", rule: "exact_match_ci", detail: "case-insensitive variant", y: 92 },
            { id: "pm", rule: "prefix_match", detail: "completion starts with target", y: 140 },
          ].map((r) => (
            <g key={r.id}>
              <rect
                x={70}
                y={r.y}
                width={300}
                height={34}
                rx={5}
                fill="color-mix(in oklch, var(--kfam-sampling) 12%, transparent)"
                stroke="var(--kfam-sampling)"
                strokeWidth={1}
              />
              <text
                x={220}
                y={r.y + 14}
                textAnchor="middle"
                fontSize={8.5}
                className="fill-foreground font-mono"
              >
                {r.rule}
              </text>
              <text
                x={220}
                y={r.y + 27}
                textAnchor="middle"
                fontSize={7}
                className="fill-muted-foreground font-mono"
              >
                {r.detail}
              </text>
            </g>
          ))}
          <text
            x={220}
            y={198}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            anything else → UnsupportedRewardMode
          </text>
        </g>
      )}
    </Figure>
  );
}

function TwoTrainerFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="multimodal GRPO: two trainers, one guard"
      caption="Schematic. The multimodal variant runs two trainers side by side over the same backend:
        the policy being trained, and a frozen reference. It requires the vision projector, and
        refuses any reference that is not the base model."
    >
      <rect
        x={30}
        y={60}
        width={60}
        height={44}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-moe) 16%, transparent)"
        stroke="var(--kfam-moe)"
        strokeWidth={1.25}
      />
      <text x={60} y={86} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        image
      </text>
      <path d="M 90 82 h 26" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect
        x={118}
        y={62}
        width={80}
        height={40}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-mmsg) 16%, transparent)"
        stroke="var(--kfam-mmsg)"
        strokeWidth={1.25}
      />
      <text x={158} y={86} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        projector
      </text>
      <path
        d="M 198 76 L 250 52 M 198 90 L 250 118"
        stroke="var(--muted-foreground)"
        strokeWidth={1}
      />
      <rect
        x={254}
        y={34}
        width={150}
        height={38}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-matvec) 16%, transparent)"
        stroke="var(--kfam-matvec)"
        strokeWidth={1.25}
      />
      <text x={329} y={57} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        policy trainer (LoRA)
      </text>
      <rect
        x={254}
        y={102}
        width={150}
        height={38}
        rx={5}
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={1.25}
        strokeDasharray="5 3"
      />
      <rect
        x={318}
        y={110}
        width={18}
        height={12}
        rx={2}
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={1}
      />
      <path
        d="M 322 110 v -4 a 5 5 0 0 1 10 0 v 4"
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={1}
      />
      <text
        x={329}
        y={134}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-foreground font-mono"
      >
        reference trainer (frozen)
      </text>
      <path d="M 329 142 v 14 L 240 172" stroke="var(--muted-foreground)" strokeWidth={0.9} />
      <rect
        x={140}
        y={164}
        width={200}
        height={24}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-kv) 10%, transparent)"
        stroke="var(--kfam-kv)"
        strokeWidth={1}
      />
      <text
        x={240}
        y={180}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-foreground font-mono"
      >
        guard: reference must equal the base dir
      </text>
    </Figure>
  );
}

const DPO_FORMATS = [
  { id: "scalar", label: "scalar-logprobs", fields: "policy/ref chosen+rejected logps" },
  { id: "text", label: "text-preference", fields: "prompt · chosen · rejected (templated)" },
  { id: "rendered", label: "rendered-text-preference", fields: "prompt already final" },
];
const GRPO_FORMATS = [
  { id: "token", label: "token-logprobs", fields: "scoring-only: tokens + logps + reward" },
  { id: "textg", label: "text-grpo", fields: "prompt · target" },
  { id: "renderedg", label: "rendered-text-grpo", fields: "+ image_paths for multimodal" },
];

function DatasetFormatsFigure() {
  return (
    <div className="flex h-full flex-col justify-center gap-3">
      <div>
        <div className="mb-1.5 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
          DPO datasets (JSONL)
        </div>
        <div className="grid grid-cols-3 gap-1.5">
          {DPO_FORMATS.map((f) => (
            <div key={f.id} className="rounded-md border p-2">
              <div className="font-mono text-[10px]">{f.label}</div>
              <div className="mt-1 font-mono text-[8.5px] text-muted-foreground">{f.fields}</div>
            </div>
          ))}
        </div>
      </div>
      <div>
        <div className="mb-1.5 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
          GRPO datasets (JSONL)
        </div>
        <div className="grid grid-cols-3 gap-1.5">
          {GRPO_FORMATS.map((f) => (
            <div key={f.id} className="rounded-md border p-2">
              <div className="font-mono text-[10px]">{f.label}</div>
              <div className="mt-1 font-mono text-[8.5px] text-muted-foreground">{f.fields}</div>
            </div>
          ))}
        </div>
      </div>
      <p className="text-center font-mono text-[10px] text-muted-foreground">
        unknown formats reject: UnsupportedDpoFormat / UnsupportedGrpoFormat
      </p>
    </div>
  );
}

const DEFAULTS_ROWS = [
  ["DPO β", "0.1"],
  ["GRPO clip / kl_coef", "0.2 / 0.04"],
  ["group size", "2 model-backed · 8 scalar route"],
  ["max completion tokens", "4"],
  ["learning rate", "1e-4, constant"],
  ["max_grad_norm", "1.0"],
  ["AdamW", "0.9 / 0.999 / 1e-8 · wd 0.01"],
  ["epochs / max examples", "1 / 32 (DPO; GRPO unlimited)"],
  ["max seq len", "512 DPO · 128 GRPO · 2048 DPO scoring-only"],
  ["LoRA rank / alpha", "16 (GRPO 8) / 32"],
  ["seed", "42"],
];

function DefaultsTableFigure({ step }: { step: 0 | 1 }) {
  return (
    <div className="flex h-full flex-col justify-center gap-3">
      {step === 0 ? (
        <>
          <div className="overflow-hidden rounded-lg border">
            {DEFAULTS_ROWS.map(([k, v]) => (
              <div
                key={k}
                className="grid grid-cols-2 border-b font-mono text-[10.5px] last:border-b-0"
              >
                <div className="border-r px-2.5 py-1.5 text-muted-foreground">{k}</div>
                <div className="px-2.5 py-1.5">{v}</div>
              </div>
            ))}
          </div>
          <p className="text-center font-mono text-[10px] text-muted-foreground">
            defaults as pinned in source — not a benchmark or a recommendation
          </p>
        </>
      ) : (
        <>
          <div className="rounded-lg border p-3">
            <div className="font-mono text-[10px] text-muted-foreground">
              antfly_inference_finetune_dpo_report/v1
            </div>
            <div className="mt-1 font-mono text-[11px]">
              {"{ examples, loss, mean_reward_margin, accuracy, beta }"}
            </div>
          </div>
          <div className="rounded-lg border p-3">
            <div className="font-mono text-[10px] text-muted-foreground">
              antfly_inference_finetune_grpo_report/v1
            </div>
            <div className="mt-1 font-mono text-[11px]">
              {"{ completions, tokens, groups, loss, pg_loss, kl_loss, clip_fraction }"}
            </div>
          </div>
          <p className="text-center font-mono text-[10px] text-muted-foreground">
            smoke recipes exercise both against a synthetic 1-layer, hidden-32 checkpoint
          </p>
        </>
      )}
    </div>
  );
}

function SilentDowngradeFigure({ step }: { step: 0 | 1 }) {
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      {step === 0 ? (
        <>
          <div className="mx-auto rounded-lg border px-4 py-2 font-mono text-xs">
            recipe.json (kind: dpo)
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg border p-3" style={{ borderColor: "var(--kfam-attention)" }}>
              <div className="font-mono text-[10px]">with any adapter (block or directory)</div>
              <div className="mt-2 font-mono text-[10px] text-muted-foreground">
                → optimizer-backed trainer
                <br />→ gradients flow, LoRA bundle saved
              </div>
            </div>
            <div className="rounded-lg border border-dashed p-3">
              <div className="font-mono text-[10px] text-muted-foreground">
                no adapter anywhere ⌀
              </div>
              <div className="mt-2 font-mono text-[10px] text-muted-foreground">
                → harness-only scoring
                <br />→ dpo_report.json written,{" "}
                <span style={{ color: "var(--kfam-text-kv)" }}>no gradients ever flow</span>
              </div>
            </div>
          </div>
          <p className="text-center font-mono text-[10px] text-muted-foreground">
            the fork is silent — both paths exit 0 and write a report
          </p>
        </>
      ) : (
        <div className="rounded-lg border border-dashed p-4">
          <ul className="space-y-1.5 font-mono text-[11px] text-muted-foreground">
            <li>· CPU-only: BackendKind = {"{ native }"}, weights fully host-resident</li>
            <li>· the reference model is a second full model in RAM</li>
            <li>· activation checkpointing is off on every decoder path</li>
            <li>· CLI: finetune run &lt;recipe.json&gt; + smoke-fast — no dpo subcommand</li>
            <li>· ipo / kto / simpo / orpo / cpo: library-only, no recipe route</li>
            <li>· no PPO, no RLHF, no reward model, no value head, no GAE</li>
            <li>· no HTTP or OpenAPI training surface</li>
          </ul>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Page                                                                */
/* ------------------------------------------------------------------ */

export function Gemma4TrainingClient({
  snippets,
  gitCommit,
  permalinkBase,
}: {
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}) {
  return (
    <SnippetProvider snippets={snippets} gitCommit={gitCommit} permalinkBase={permalinkBase}>
      <div className="py-8">
        <header className="mx-auto max-w-7xl px-4">
          <p className="font-mono text-xs uppercase tracking-wider text-muted-foreground">
            <Link href="/training" className="hover:text-foreground">
              training
            </Link>{" "}
            / gemma4
          </p>
          <h1 className="mt-1 text-3xl font-bold tracking-tight">Gemma4 preference tuning</h1>
          <p className="mt-2 max-w-3xl text-muted-foreground">
            Real, optimizer-backed preference tuning on the CPU autodiff trainer: a textbook DPO
            loss wearing a cross-entropy costume, and a GRPO loop whose rollouts never touch a
            random number generator. The{" "}
            <Link className="text-primary underline" href="/models/gemma4-e4b">
              Gemma4 model page
            </Link>{" "}
            covers how it runs; this page covers how it learns. Everything below links to the code
            that runs; none of it is a quality claim.
          </p>
        </header>

        {/* ── 1 · SFT / LoRA substrate ─────────────────────────────── */}
        <ScrollyChapter
          id="sft"
          number={1}
          title="SFT and LoRA: the substrate"
          intro="Preference tuning sits on top of an ordinary LoRA-SFT stack — meet it first."
        >
          <Scene id="recipes" graphic={<LoraAdapterStackFigure step={0} />}>
            <p>
              The recipe runner knows <code>sft</code>, <code>lora_sft</code> and{" "}
              <code>qlora_sft</code>; a Gemma4 SFT runs as a multi-step tool plan (prepare →
              bootstrap → train-eval). The adapters are classic LoRA: rank 16 by default (the GRPO
              path drops to 8), alpha 32, the <code>all-linear</code> preset covering all seven
              Gemma target modules, A initialized Kaiming-uniform and B at zero so training starts
              from the base model exactly. <code>use_dora</code> and custom LoRA init are
              Gemma4-only options.
            </p>
            <p>
              <CodeLink link={L("train-recipe-cli")} />
            </p>
          </Scene>
          <Scene id="trainer" graphic={<LoraAdapterStackFigure step={1} />}>
            <p>
              Underneath sits <code>RealAutodiffTrainer</code>: constant learning rate 1e-4, AdamW,
              global-norm clipping at 1.0, seed 42 — and a backend enum with exactly one member,{" "}
              <code>native</code>. Every safetensors tensor is loaded fully resident into host
              memory. That single line is why this whole page happens on the CPU.
            </p>
            <p>
              <CodeLink link={L("train-real-autodiff-trainer")} /> ·{" "}
              <CodeLink link={L("train-max-grad-norm")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 2 · DPO stack ────────────────────────────────────────── */}
        <ScrollyChapter
          id="dpo-stack"
          number={2}
          title="DPO in three layers"
          intro="A loss library, a model-agnostic harness, and a recipe trainer — strictly layered."
        >
          <Scene id="layers" graphic={<DpoThreeLayerFigure />}>
            <p>
              The bottom of the stack is pure math: six preference losses with analytic gradients
              and finite-difference tests living next to the formulas. Above it, a harness that
              knows how to call <em>any</em> model's log-probability function four times (policy and
              reference, chosen and rejected) and hand back gradients — it owns no optimizer by
              contract. At the top, the optimizer-backed Gemma path calls the same loss math
              directly, one example at a time, and wires its analytic gradients into the trainer —
              the harness itself serves the scoring-only fallthrough. A LoRA bundle is saved when
              it's done.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-dpo-loss")} /> ·{" "}
              <CodeLink link={L("gemma4-train-harness-step")} /> ·{" "}
              <CodeLink link={L("gemma4-train-dpo-recipe")} />
            </p>
          </Scene>
          <Scene id="loss" graphic={<DpoLossFigure />}>
            <p>
              The loss is the textbook one:{" "}
              <code>
                −logSigmoid(β · ((π_chosen − π_ref_chosen) − (π_rejected − π_ref_rejected)))
              </code>{" "}
              with β defaulting to 0.1 — no label smoothing, no conservative-DPO epsilon. The enum
              next to it names five siblings — IPO, KTO, SimPO, ORPO, CPO — all implemented and
              tested (five of the six carry finite-difference gradient checks; IPO's test pins its
              optimum instead), and all <em>library-only</em>: the recipe runner only ever
              constructs DPO.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-loss-kinds")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 3 · Reference model ──────────────────────────────────── */}
        <ScrollyChapter
          id="reference"
          number={3}
          title="Where the reference model comes from"
          intro="DPO needs a frozen reference — this runtime has two ways to conjure one."
        >
          <Scene id="two-ways" graphic={<ReferencePolicyFigure />}>
            <p>
              The harness's default is elegant: <code>reference_from_disabled_adapter</code> — call
              the <em>policy</em> model a second time with its LoRA adapters toggled off. Since the
              adapters start at zero, base-plus-nothing <em>is</em> the reference. The Gemma recipe
              path takes the sturdier road instead and loads a second full model from disk, scoring
              reference log-probabilities through a frozen forward pass. Both shipped DPO routes in
              fact take the second road — the adapter-disable default is exercised only by the
              harness's own tests. Either way a reference is a hard requirement for DPO, IPO and KTO
              — missing reference logps are a validation error, not a silent default.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-ref-disabled")} /> ·{" "}
              <CodeLink link={L("gemma4-train-seq-logprob")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 4 · CE injection ─────────────────────────────────────── */}
        <ScrollyChapter
          id="injection"
          number={4}
          title="The CE-target gradient trick"
          intro="The trainer only knows cross-entropy — DPO's gradient gets in by disguising itself as a scaled CE target."
        >
          <Scene id="trick" graphic={<CeInjectionFigure />}>
            <p>
              The recipe computes ∂loss/∂logprob analytically from the DPO formula, then builds a
              trainer input whose cross-entropy target row is scaled by{" "}
              <code>token_scale_override = −grad · seq_len</code>. Run the generic CE backward pass
              over that input and what comes out is <em>exactly</em> the preference gradient — a
              weighted log-likelihood in a cross-entropy costume. Chosen and rejected each take
              their own trainer step, which is why the DPO path doubles gradient accumulation.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-ce-inject")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 5 · GRPO objective ───────────────────────────────────── */}
        <ScrollyChapter
          id="grpo-objective"
          number={5}
          title="GRPO: the objective"
          intro="Group-relative policy optimization — advantages normalized within a sampled group, a clipped surrogate, and a k3 KL leash."
        >
          <Scene id="advantages" graphic={<GrpoGroupFigure />}>
            <p>
              Each prompt's completions form a group. <code>computeAdvantages</code> subtracts the
              group mean and divides by the group standard deviation (+1e-8) — and that group
              statistic is the <em>entire</em> baseline. There is no value function and no GAE; the
              group does the critic's job. The loss struct defaults to groups of 8, but every
              model-backed Gemma route runs groups of 2.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-grpo-adv")} />
            </p>
          </Scene>
          <Scene id="surrogate" graphic={<GrpoLossFigure />}>
            <p>
              The per-token loss is a PPO-style clipped surrogate: the new/old probability ratio is
              clamped to [0.8, 1.2] (clip 0.2), and the gradient goes to zero wherever the clip
              binds. A Schulman k3 estimator — <code>exp(Δ) − Δ − 1</code> against the reference —
              adds a KL leash at weight 0.04, and the run reports its <code>clip_fraction</code> — a
              record of how often the clamp fired. A finite-difference gradient test lives beside
              the loss.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-grpo-loss")} /> ·{" "}
              <CodeLink link={L("gemma4-train-grpo-recipe")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 6 · Rollouts ─────────────────────────────────────────── */}
        <ScrollyChapter
          id="grpo-rollouts"
          number={6}
          title="Rollouts without dice"
          intro="The 'sampling' inside GRPO is deterministic — rank-k argmax, not temperature."
        >
          <Scene id="ranked" graphic={<RolloutRankFigure step={0} />}>
            <p>
              Group diversity comes from rank, not randomness: member <em>i</em> takes the rank-
              <em>i</em> token at each decode step — member 0 is pure greedy, member 1 always takes
              the runner-up, and so on, capped at min(group, 8). Completions run to at most 4 tokens
              by default. There is no temperature, no top-p, no seeded multinomial — the entire
              rollout is reproducible by construction.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-rank-rollout")} /> ·{" "}
              <CodeLink link={L("gemma4-train-grpo-sampler")} />
            </p>
          </Scene>
          <Scene id="rewards" graphic={<RolloutRankFigure step={1} />}>
            <p>
              Rewards are equally unmysterious: string-match rules against the target.{" "}
              <code>exact_match</code> is graded — 1.0 for equality, 0.5 for substring containment,
              0 otherwise — with case-insensitive and prefix variants beside it, and anything else
              rejected as an unsupported mode. Rewards can also arrive precomputed in the dataset.
              What does <em>not</em> exist is a learned reward model.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-reward-mode")} />
            </p>
          </Scene>
          <Scene id="multimodal" graphic={<TwoTrainerFigure />}>
            <p>
              GRPO even runs multimodal: with a vision projector configured, the recipe instantiates{" "}
              <em>two</em> trainers over the same backend — the LoRA policy and a frozen reference —
              and hard-refuses any reference path that isn't the base model, so the KL leash always
              measures drift from the true starting point.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-mm-grpo")} /> ·{" "}
              <CodeLink link={L("gemma4-train-ref-path-guard")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 7 · Data / defaults / reports ────────────────────────── */}
        <ScrollyChapter
          id="data"
          number={7}
          title="Datasets, defaults, reports"
          intro="Three DPO formats, three GRPO formats, and two small JSON receipts."
        >
          <Scene id="formats" graphic={<DatasetFormatsFigure />}>
            <p>
              DPO reads <code>scalar-logprobs</code> (log-probabilities precomputed elsewhere),{" "}
              <code>text-preference</code> (prompt/chosen/rejected run through the chat template),
              or <code>rendered-text-preference</code> (the prompt is final as-is). GRPO reads{" "}
              <code>token-logprobs</code> — a scoring-only path that consumes supplied rewards
              without sampling — or <code>text-grpo</code> / <code>rendered-text-grpo</code> rows of
              prompt and target, optionally with image paths for the multimodal variant.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-dpo-gate")} />
            </p>
          </Scene>
          <Scene id="defaults" graphic={<DefaultsTableFigure step={0} />}>
            <p>
              The numbers worth knowing, exactly as pinned in source. Two asymmetries are real and
              deliberate. The first is sequence length: DPO trains at 512, GRPO at 128, and only
              DPO's scoring-only path stretches to 2048. The second is group size: the model-backed
              GRPO routes run groups of 2, while the scalar route keeps the struct default of 8.
            </p>
          </Scene>
          <Scene id="reports" graphic={<DefaultsTableFigure step={1} />}>
            <p>
              Every run ends in a versioned report. DPO writes the loss, the mean reward margin, the
              accuracy — the fraction of pairs where that margin came out positive — and β. GRPO
              writes its policy-gradient loss, its KL loss, the clip fraction and the group count.
              The smoke harness exercises both end-to-end against a synthetic one-layer, hidden-32
              Gemma checkpoint — real code, toy weights.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-dpo-report")} /> ·{" "}
              <CodeLink link={L("gemma4-train-grpo-report")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 8 · Boundaries ───────────────────────────────────────── */}
        <ScrollyChapter
          id="boundaries"
          number={8}
          title="Honest boundaries"
          intro="What this stack refuses to be — and one trap worth knowing."
        >
          <Scene id="downgrade" graphic={<SilentDowngradeFigure step={0} />}>
            <p>
              <strong>The silent downgrade.</strong> The optimizer-backed trainer engages only when
              the recipe names (or its model path implies) the Gemma family <em>and</em> requests
              any adapter — a LoRA block or an adapter directory. Omit every adapter reference and
              the run falls through to harness-only scoring: it still exits cleanly and still writes{" "}
              <code>dpo_report.json</code> — but no gradient ever flows. For a run that was meant to
              train, the report looking plausible is exactly the trap.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-dpo-gate")} />
            </p>
          </Scene>
          <Scene id="scope" graphic={<SilentDowngradeFigure step={1} />}>
            <p>
              The rest of the fence, plainly: decoder training is CPU-only by construction, with the
              reference model doubling host memory and activation checkpointing off. The five
              sibling losses have no recipe route. Several finetune modules in the tree are orphaned
              and deliberately undocumented here. And nothing in this runtime does PPO, RLHF, or
              learned rewards — GRPO's group mean plus string-match rules is the entire story. The
              GLiNER pages tell a very different one:{" "}
              <Link className="text-primary underline" href="/training/gliner2">
                GLiNER2
              </Link>{" "}
              and{" "}
              <Link className="text-primary underline" href="/training/gliner25">
                GLiNER2.5
              </Link>
              .
            </p>
          </Scene>
        </ScrollyChapter>
      </div>
    </SnippetProvider>
  );
}
