"use client";

/**
 * Gemma4 chapters 8–11: the kernel compiler, the LM-head case study,
 * device-resident sampling, and MTP.
 */
import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { QuantChip } from "@/components/primitives/chips";
import { Divergence, Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { L } from "@/lib/links";
import type { ChaptersProps } from "../registry";
import {
  CompilerPipelineFigure,
  GpuSamplingFigure,
  KernelCensusFigure,
  LmHeadPathsFigure,
  LmHeadZoomFigure,
  MtpScoreboardFigure,
  MtpSideBySideFigure,
  ProposeVerifyFigure,
  ScheduleRowFigure,
  TokenHandoffFigure,
} from "./figures-late";

function KernelChip({ name }: { name: string }) {
  return (
    <span className="inline-flex items-center rounded-sm border bg-muted/50 px-1.5 py-px font-mono text-[10px]">
      {name}
    </span>
  );
}

export function Gemma4LateChapters({ spec, routes, kernelCensus }: ChaptersProps) {
  const isE4b = spec.id === "gemma4-e4b";
  const q4kRoute = routes.find((r) => r.id === "q4_k/rows_2_8/none");

  return (
    <div>
      {/* ── Ch 8 · Kernels are compiled, not written ─────────────── */}
      <ScrollyChapter
        id="ch-8"
        number={8}
        title="Kernels are compiled, not written (mostly)"
        intro={
          <p>
            The matvec kernels that move most of the bytes are not hand-typed Metal. They are
            generated from schedule tables. A dedicated regeneration check detects drift between the
            tables and checked-in Metal sources.
          </p>
        }
      >
        <Scene
          id="pipeline"
          graphic={<CompilerPipelineFigure routedSourceFiles={kernelCensus.routedSourceFiles} />}
        >
          <p>
            One schedule table describes every route as <code>format × row_bucket × epilogue</code>{" "}
            plus tuning knobs; one renderer expands each row through a shared MSL skeleton into
            checked-in <code>.metal</code> files. The command{" "}
            <code>zig build quant-kernel-codegen -- --check</code> checks regeneration consistency.
            This describes the static code-generation path; the repository also has an optional
            runtime kernel-JIT subsystem with separate mode and qualification controls.
          </p>
          <p>
            <CodeLink link={L("compiler-schedules")} /> · <CodeLink link={L("renderer")} />
          </p>
        </Scene>
        <Scene id="row" graphic={<ScheduleRowFigure route={q4kRoute} />}>
          <p>
            Here is a small-batch schedule example: <QuantChip format="q4_k" />, the 2–8-row bucket,
            no epilogue — 128 threads per threadgroup, 16 columns, 2 rows, simdgroup-tiled
            reduction. When a sweep finds a better configuration, the fix is an edit to this row and
            a regenerate, and the diff shows exactly what changed in the emitted Metal. The
            single-token LM-head MMV in the next chapter is a different route; this 2–8-row schedule
            is not its launch configuration.
          </p>
        </Scene>
        <Scene id="census" graphic={<KernelCensusFigure census={kernelCensus} />}>
          <p>
            The kernel inventory contains {kernelCensus.total} extracted Metal entry points. This
            inventory includes generated and hand-written kernels across inference, training, and
            supporting operations; it is not the dispatch count for one model.
          </p>
          <Divergence
            others={
              <p>hand-written kernels express format-specific behavior directly in Metal source.</p>
            }
            antfly={
              <p>
                generated routes keep tuning parameters in a schedule table, while other routes and
                JIT specializations have their own implementation and qualification paths.
              </p>
            }
            link={<CodeLink link={L("compiler-schedules")} />}
          />
          <p className="text-xs">
            Browse the current generated inventory:{" "}
            <Link className="text-primary underline" href="/systems/kernels">
              the kernel inventory →
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 9 · The LM head problem ───────────────────────────── */}
      <ScrollyChapter
        id="ch-9"
        number={9}
        title="The LM head problem (a worked example)"
        intro={
          <p>
            The vocabulary projection is large enough to be a useful case study in bandwidth,
            quantization error, and the difference between an experiment and a default.
          </p>
        }
      >
        <Scene id="zoom" graphic={<LmHeadZoomFigure isE4b={isE4b} />}>
          <p>
            Every decode step ends by multiplying the hidden state against all 262,144 vocabulary
            rows — a <code>[{isE4b ? "2560" : "1536"} × 262144]</code> matvec. On E4B that segment
            is about 550 MB of <QuantChip format="q6_k" /> weights in the recorded artifact,
            approximately 19.5% of the historical weight-read estimate. That byte calculation does
            not establish how much latency a different kernel would save.
          </p>
        </Scene>
        <Scene id="paths" graphic={<LmHeadPathsFigure />}>
          <p>
            <strong>An opt-in greedy optimization:</strong> keep the original Q6_K head and create a{" "}
            <QuantChip format="q4_k" /> copy in a streaming load-time pass. The tuned{" "}
            <KernelChip name="termite_q4_k_linear_1x_reduce_v2" /> MMV nominates candidates;{" "}
            <KernelChip name="termite_lm_head_q6_k_rescore_top8" /> rescores them with original
            weights. Full-logit and sampling callers retain Q6_K. The extra copy uses more resident
            memory, so <code>TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK=q4_k</code> remains opt-in.
            Historical Air probes improved about 4–5% with matching greedy tokens; candidate
            truncation is not a proof of equivalence for every prompt.
          </p>
          <p>
            <CodeLink link={L("gemma-lm-head-repack")} />
          </p>
          <p>
            <strong>The branch that didn't:</strong> a <QuantChip format="q4_0" /> head looked like
            free bytes and collapsed generation to an instant end-of-turn token. Measured, struck
            through, and kept — the ghost path is part of the engineering record, not an
            embarrassment to hide.
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 10 · Sampling never leaves the GPU ────────────────── */}
      <ScrollyChapter
        id="ch-10"
        number={10}
        title="Device-resident sampling, when eligible"
        intro={
          <p>
            A supported resident route can select a token without copying the full vocabulary logits
            to the CPU. The canonical host sampler remains available for unsupported cases.
          </p>
        }
      >
        <Scene id="onchip" graphic={<GpuSamplingFigure />}>
          <p>
            Greedy selection reduces logits to an argmax. Temperature sampling can use{" "}
            <KernelChip name="termite_sample_gumbel_partials" /> followed by a reduction that writes
            a token id to a device buffer. Bounded top-k/top-p routes have their own eligibility
            checks. The Q4_K head's top-8 Q6_K rescore is a separate greedy optimization, not a
            universal stage before Gumbel sampling.
          </p>
          <p>
            <CodeLink link={L("kernel-gumbel")} />
          </p>
          <Divergence
            others={
              <p>
                host sampling reads logits and applies the canonical request constraints on the CPU.
              </p>
            }
            antfly={
              <p>
                eligible requests select on device; token ids still reach the host for decoding and
                streaming.
              </p>
            }
            link={<CodeLink link={L("kernel-gumbel")} />}
          />
          <p>
            <CodeLink link={L("gemma-sampling-fallback")} /> Unsupported settings or grammar
            constraints can require a host path.
          </p>
        </Scene>
        <Scene id="handoff" graphic={<TokenHandoffFigure />}>
          <p>
            The payoff is bigger than saved copies: because the sampled id lives in a GPU buffer,
            the next frame's embedding lookup can consume it directly. That device-resident handoff
            is what makes{" "}
            <Link className="text-primary underline" href="#ch-7">
              chapter 7
            </Link>
            &apos;s pipelined decode frame legal — the CPU encodes frame N+1 against a token the
            host has never seen.
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 11 · MTP ──────────────────────────────────────────── */}
      <ScrollyChapter
        id="ch-11"
        number={11}
        title="MTP: a draft model that reads the main model's mind"
        intro={
          <p>
            Gemma4 ships an official speculative drafter — a 4-layer, hidden-256 stack that does not
            build an independent target-style KV cache. It borrows the main model's.
          </p>
        }
      >
        <Scene id="donor" graphic={<MtpSideBySideFigure isE4b={isE4b} />}>
          <p>
            The drafter is query-only: it has Q/O projections and MLPs but no K/V projections at
            all. Its layers cross-attend the main model's KV banks through donor layers —{" "}
            {isE4b
              ? "layers 22 and 23 on E4B"
              : "sliding donor layer 13 and full-attention donor layer 14 on E2B"}{" "}
            (zero-based indices, in the shared-type donor mode) — with a projection pair bridging
            its 256-wide stack to the {isE4b ? "2560" : "1536"}
            -wide backbone. A draft model that literally reads the target&apos;s working memory.
          </p>
          <p>
            <CodeLink link={L("mtp-kv-donor")} /> · <CodeLink link={L("mtp-draft-request")} />
          </p>
        </Scene>
        <Scene id="verify" graphic={<ProposeVerifyFigure />}>
          <p>
            The loop: the drafter proposes k tokens cheaply, then the main model verifies all k
            positions together on eligible verifier routes. Accepted tokens amortize a target pass;
            a mismatch truncates the tail and requires replacement and KV-state handling. Greedy
            verification aims to preserve the target token sequence. Drafting, verification, and
            rejected work all cost time, and batched numerical behavior still needs parity checks.
          </p>
        </Scene>
        <Scene id="board" graphic={<MtpScoreboardFigure />}>
          <p>
            <strong>Acceptance alone is not speedup.</strong> An earlier E2B Metal CLI pilot
            recorded 75.9 tok/s target-only versus 63.7 with the BF16 draft at 64% acceptance. Its
            output did not record a binary identity, so the performance plan labels this directional
            evidence. Later server probes exercised the slow-path auto-disable and fallback
            behavior. Metal automatic MTP requires its explicit enable gate; a forced policy is a
            separate choice. These records do not predict current performance on other devices or
            runtimes.
          </p>
          <p>
            <CodeLink link={L("gemma-mtp-ledger")} />
          </p>
        </Scene>
      </ScrollyChapter>
    </div>
  );
}
