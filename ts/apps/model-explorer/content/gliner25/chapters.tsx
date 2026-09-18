"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { EnvFlagChip } from "@/components/primitives/chips";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { L } from "@/lib/links";
import type { ChaptersProps } from "../registry";
import {
  BoundaryEncoderFigure,
  DecodeCascadeFigure,
  EncoderContractFigure,
  LongDocumentFigure,
  MegaKernelFigure,
  PrecisionPolicyFigure,
  PromptAssemblyFigure,
  QualificationGateFigure,
  SharedPoolFigure,
  SpanGridVsBoundaryFigure,
  TaskHeadsFigure,
  TrainingFigure,
  VariantsFigure,
} from "./figures";

export function Gliner25Chapters({ spec }: ChaptersProps) {
  return (
    <div>
      {/* ── Ch 1 · One prompt, many tasks ───────────────────────────── */}
      <ScrollyChapter
        id="ch-1"
        number={1}
        title="One prompt, many tasks"
        intro="Entities, classifications, relations and records compile into one marker-structured prompt ahead of the text."
      >
        <Scene id="schema" graphic={<PromptAssemblyFigure step={0} />}>
          <p>
            GLiNER2 puts one entity list in front of the text. GLiNER2.5 serializes a whole{" "}
            <em>schema</em>: multiple task groups joined by <code>[SEP_STRUCT]</code>, each opened by{" "}
            <code>[P]</code> and listing its fields under a task-specific marker — <code>[E]</code>{" "}
            entity labels, <code>[C]</code> structure fields, <code>[R]</code> relations,{" "}
            <code>[L]</code> classification choices. Field descriptions ride along after{" "}
            <code>[DESCRIPTION]</code>, and few-shot examples as <code>[EXAMPLE]</code>…
            <code>[OUTPUT]</code> pairs — {spec.stats.promptMarkers} marker <em>types</em> in all (one
            marker token per field in the prompt), versus GLiNER2's five. Everything is still one
            encoder pass; each field marker's hidden state becomes that field's <em>query</em>{" "}
            downstream.
          </p>
          <p>
            <CodeLink link={L("gliner25-markers")} /> · <CodeLink link={L("gliner25-prompt-build")} /> ·{" "}
            <CodeLink link={L("gliner25-extraction-parse")} />
          </p>
        </Scene>
        <Scene id="enums" graphic={<PromptAssemblyFigure step={1} />}>
          <p>
            <strong>Enums become words.</strong> A structure field constrained to{" "}
            <code>(USD | EUR)</code> can't point at text that isn't there — so the processor injects the
            choices as synthetic <em>prefix words</em> before the body. The boundary head then "extracts"
            the right choice as a span over the prefix. Prefix words are scored like any others but never
            map back to source offsets, and <code>max_len</code> budgets body words only.
          </p>
          <p>
            <CodeLink link={L("gliner25-enum-prefix")} />
          </p>
        </Scene>
        <Scene id="normalize" graphic={<PromptAssemblyFigure step={2} />}>
          <p>
            <strong>The tokenizer got stricter.</strong> The released GLiNER2.5 tokenizers carry a real Unigram normalizer chain: NFC
              canonical composition under Unicode 15, whitespace replacement, then stripping. The
              loader implements every step or refuses the model outright. Approximating quietly
              would shift token boundaries, and with them the character offsets every answer is
              reported in. Normalization can reorder codepoints, so the tokenizer's own offsets would no
                longer point where they claim; they are suppressed on this path. The pipeline
                keeps its own word-to-byte map instead, and can answer in UTF-8 bytes, Unicode
                codepoints or UTF-16 units.
          </p>
          <p>
            <CodeLink link={L("gliner25-normalizer")} /> · <CodeLink link={L("gliner25-word-ranges")} /> ·{" "}
            <CodeLink link={L("gliner25-offset-units")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 2 · Same encoder, a new contract ─────────────────────── */}
      <ScrollyChapter
        id="ch-2"
        number={2}
        title="Same encoder, a new contract"
        intro={
          <p>
            The {spec.stats.layers}-layer, hidden-{spec.stats.hidden} DeBERTa-v3 encoder is the same
            disentangled-attention design GLiNER2 uses — the{" "}
            <Link className="text-primary underline" href="/models/gliner2#ch-3">
              GLiNER2 walkthrough
            </Link>{" "}
            covers the C2C/C2P/P2C math. What changed is everything around it.
          </p>
        }
      >
        <Scene id="reuse" graphic={<EncoderContractFigure />}>
          <p>
            <strong>Pinned geometry, different dispatch.</strong> The loader hard-pins the encoder shape —
            exactly 12 layers, hidden 384 or 768, 256 position buckets, exact GELU — and rejects anything
            else. On Metal, attention does not go through GLiNER2's fused kernel or any of its
              variants. The boundary path runs it as a single dispatch of its own combined kernel
              instead — chapter 7 takes that apart. On the optimized resident path, each layer's relative Q/K projections
            are also computed once per session and kept on the device; the reference path recomputes them
            per request.
          </p>
          <p>
            <CodeLink link={L("gliner25-engine-device")} /> ·{" "}
            <CodeLink link={L("gliner25-resident-constants")} />
          </p>
        </Scene>
        <Scene id="longdoc" graphic={<LongDocumentFigure />}>
          <p>
            <strong>One pass to 4,096 words — then windows.</strong> GLiNER2 stops at one 512-token
            pass, schema included. A single GLiNER2.5 pass takes up to {spec.stats.maxBodyWords} body
            words (admitted against a 16,384-token sequence budget — 512 is only the relative-position
            table height). Longer documents, up to 131,072 words, are what the windowing planner is for:
            it tiles the text into overlapping {spec.stats.maxBodyWords}-word windows, re-encodes the
            same schema header in front of each, then merges globally — overlapping words belong to the
            window on their side of the overlap's midpoint, mentions dedupe by max score, classifications
            merge by an owned-word-weighted mean of logits, and records resolve by an explicit identity
            policy. The windowing semantics are versioned so results stay reproducible.
          </p>
          <p>
            <CodeLink link={L("gliner25-longdoc-plan")} /> · <CodeLink link={L("gliner25-longdoc-merge")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 3 · Boundaries, not spans ────────────────────────────── */}
      <ScrollyChapter
        id="ch-3"
        number={3}
        title="Boundaries, not spans"
        intro="GLiNER2 scored every (start, width ≤ max_width) span. GLiNER2.5 scores the boundaries between words and proposes."
      >
        <Scene id="why" graphic={<SpanGridVsBoundaryFigure />}>
          <p>
            GLiNER2's span grid has a structural ceiling: enumerate every start × width pair and anything wider
            than the cap simply cannot be extracted. GLiNER2.5 replaces the grid with{" "}
            <strong>boundary scoring</strong>: W words define W+1 boundaries, every span is a (start
            boundary, end boundary) pair, and width stops being a hyperparameter. The marker query states
            from chapter 1 condition all of it — the same machinery serves entities, structure fields and
            relation arguments.
          </p>
        </Scene>
        <Scene id="bencoder" graphic={<BoundaryEncoderFigure step={0} />}>
          <p>
            <strong>A small transformer over boundaries.</strong> Each boundary sees the word to its left
            and right through separate {spec.stats.hidden}→{spec.stats.boundaryDim} projections; learned
            BOS/EOS states cover the document edges. Two windowed self-attention blocks (window 128, 4 heads — the released
            bundles enable them; the code default is none) let boundaries coordinate locally, and one gated-FFN (SwiGLU) block refines the result —
            all in the {spec.stats.boundaryDim}-d boundary space, far cheaper than the encoder above it.
          </p>
          <p>
            <CodeLink link={L("gliner25-boundary-encoder")} />
          </p>
        </Scene>
        <Scene id="proposer" graphic={<BoundaryEncoderFigure step={1} />}>
          <p>
            <strong>Then propose — once per document.</strong> For every query, every boundary gets a
            start score and an end score. The proposer then takes the <em>union</em> over queries: the
            top-32 boundary starts and top-32 ends by any query's marginal (pool_boundary_top_k, 32 in
            the released bundles vs a code default of 64), and scores all 32×32 pairings by endpoint
            compatibility plus the union marginals. The config also carries per-query top-24 /
            bidirectional-pairing knobs, but this runtime's inference path doesn't consume them — the
            document-level union is what executes.
          </p>
          <p>
            <CodeLink link={L("gliner25-proposer-marginals")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 4 · A shared pool, conditioned per query ─────────────── */}
      <ScrollyChapter
        id="ch-4"
        number={4}
        title="A shared pool, conditioned per query"
        intro="Candidates are pooled once per document; every query looks at the same pool through its own lens."
      >
        <Scene id="pool" graphic={<SharedPoolFigure highlight="film" />}>
          <p>
            In GLiNER2, more labels mean more span-label scores <em>and</em> a longer input. GLiNER2.5
            decouples that: proposals from all queries merge into one document-level{" "}
            <strong>shared candidate pool</strong> of {spec.stats.poolSize} slots in the released bundles (the code default is 384), with at least 8 slots
            reserved per query so rare fields aren't crowded out. Each query then modulates the same pool
            with FiLM — <code>(1+γ)·x + β</code> from its marker state — before a small scorer with
            length and prior features ranks the slots. Adding a label adds one lens, not a new
            enumeration.
          </p>
          <p>
            <CodeLink link={L("gliner25-shared-pool-film")} />
          </p>
        </Scene>
        <Scene id="pair" graphic={<SharedPoolFigure highlight="pair" />}>
          <p>
            <strong>Two scorers, two jobs.</strong> For entities and ordinary fields, the FiLM scorer's
            slot logits <em>are</em> the pair logits decode consumes — there is no second rescoring pass.
            The heavier <em>explicit-span</em> scorer handles attributes and constrained or enum fields,
            where the span is already pinned: start/end boundary states are lifted through endpoint
            projections and rotated by position (rotary, θ=10⁴), an 8-head compatibility mix dots them
            against the query, endpoint-difference features capture shape, and a query-conditioned
            inside-evidence mean plus length features and a content bias join the sum.
          </p>
          <p>
            <CodeLink link={L("gliner25-head-forward")} /> ·{" "}
            <CodeLink link={L("gliner25-rotary-endpoints")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 5 · Deciding what to emit ────────────────────────────── */}
      <ScrollyChapter
        id="ch-5"
        number={5}
        title="Deciding what to emit"
        intro="Decoding is a cascade: abstain, calibrate, rescue, then resolve overlaps — exactly."
      >
        <Scene id="abstain" graphic={<DecodeCascadeFigure step={0} />}>
          <p>
            <strong>First, the query may say nothing.</strong> A dedicated null projection gives every query one extra score standing for "no
              answer". Passed through a sigmoid, a value strictly above 0.5 means the query returns
                nothing at all. That silence is a learned decision, then — not an
              accident of every candidate scoring low. Pair logits that survive are calibrated by a temperature sigmoid.
          </p>
          <p>
            <CodeLink link={L("gliner25-decode-query")} />
          </p>
        </Scene>
        <Scene id="count" graphic={<DecodeCascadeFigure step={1} />}>
          <p>
            <strong>Count conditioning only rescues.</strong> The count head predicts a log-rate for how
            many spans a query should yield; with <code>adaptive_threshold</code> enabled, decode rounds{" "}
            <code>exp(rate)</code> (banker's rounding, overflow-guarded) and admits that many top-ranked
            candidates even if they fell below the threshold. The rule is asymmetric by design: count
            guidance adds candidates, it never removes a threshold hit. One honest footnote: the released
            bundles ship <code>adaptive_threshold: false</code>, so today the head computes its rate but
            the rescue stays dormant.
          </p>
        </Scene>
        <Scene id="overlap" graphic={<DecodeCascadeFigure step={2} />}>
          <p>
            <strong>Overlaps resolve exactly.</strong> Four policies: <code>allow</code>,{" "}
            <code>nested</code>, <code>longest</code>, and the default <code>flat</code> — which is exact
            weighted interval scheduling with deterministic tie-breaks, not GLiNER2's greedy
            score-order sweep. Offsets then convert to the requested unit, refusing to split surrogate
            pairs or UTF-8 sequences.
          </p>
          <p>
            <CodeLink link={L("gliner25-overlap-policy")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 6 · Classification, relations, records ───────────────── */}
      <ScrollyChapter
        id="ch-6"
        number={6}
        title="Classification, relations, records"
        intro="Three more heads read the same encoded sequence — no extra encoder passes."
      >
        <Scene id="cls" graphic={<TaskHeadsFigure task="cls" />}>
          <p>
            <strong>Classification</strong> is an MLP on each <code>[L]</code> choice-marker state —{" "}
            {spec.stats.hidden}→1536→ReLU→1 per label — with single, multi and ordinal modes, label
            count bounds, and a logical-constraint solver (<code>Implies</code>, label counts) that picks
            the best consistent assignment exactly or by beam search when the space is large.
          </p>
          <p>
            <CodeLink link={L("gliner25-task-classify")} />
          </p>
        </Scene>
        <Scene id="rel" graphic={<TaskHeadsFigure task="rel" />}>
          <p>
            <strong>Relations</strong> are learned, not heuristic: typed head and tail role queries pick
            argument candidates, a scorer MLP over the pair joins a biaffine content gate, and directional
            states keep <em>(a → b)</em> distinct from <em>(b → a)</em>. At most 64 pairs per relation type are scored in the released
            bundles; the code default and wire ceiling are 128. GLiNER2's label-list workaround is gone.
          </p>
          <p>
            <CodeLink link={L("gliner25-task-relations")} />
          </p>
        </Scene>
        <Scene id="rec" graphic={<TaskHeadsFigure task="rec" />}>
          <p>
            <strong>Records</strong> assemble multi-field structures: instances cross-attend over the
            candidates and claim fields, so two invoices in one document come out as two records, not a
            field soup. Where the instances come from depends on the mode: anchored{" "}
            <code>natural</code> seeds them from the anchor field's candidates, <code>latent</code> from
            all candidates, and <code>anchorless</code> from 32 learned instance queries. A separate
            joint-IE task optimizes entities and relations globally.
          </p>
          <p>
            <CodeLink link={L("gliner25-task-records")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 7 · One kernel for the whole head ────────────────────── */}
      <ScrollyChapter
        id="ch-7"
        number={7}
        title="One kernel for the whole head"
        intro="On Metal, the boundary stack is one kernel dispatched 42 ways — plus MPS for the matmuls."
      >
        <Scene id="megakernel" graphic={<MegaKernelFigure />}>
          <p>
            Instead of GLiNER2's family of purpose-built kernels, the boundary head ships{" "}
            <code>termite_gliner_boundary_f32</code>: one kernel switching on a 42-entry{" "}
            <code>Kind</code> descriptor — banded attention, SwiGLU, FiLM, rotary, marginals, interval
            means, relation gating, record attention and more. The descriptor struct is a private
            Zig↔Metal ABI with pinned numeric values; descriptors are validated before anything reaches
            the GPU, and a scope accountant caps pending device bytes and dispatches per request. GEMMs
            ride MPS outside the kernel.
          </p>
          <p>
            <CodeLink link={L("gliner25-device-kinds")} /> · <CodeLink link={L("gliner25-mega-kernel")} />
          </p>
          <p>
            <EnvFlagChip name="TERMITE_METAL_DISABLE_DOT_GENERAL_2D_MPS" defaultOn={false} />
          </p>
        </Scene>
        <Scene id="precision" graphic={<PrecisionPolicyFigure />}>
          <p>
            <strong>Precision is a per-tensor contract.</strong> Only declared encoder matrices may be
            reduced — FP16 or Q8_0 everywhere, plus Q4_K for base/multi and Q4_0 for small (each
            variant's other 4-bit format is banned). Every task-head tensor, bias, norm and the
            relative-position table stays FP32,
            whatever the bundle advertises. The native CPU path runs the whole stack in FP32 and serves as the reference
              implementation. Metal is the only GPU backend. The weights and all four sidecar
              files are SHA-256-pinned end to end.
          </p>
          <p>
            <CodeLink link={L("gliner25-precision-policy")} /> ·{" "}
            <CodeLink link={L("gliner25-engine-native")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 8 · Trainable, with adapters ─────────────────────────── */}
      <ScrollyChapter
        id="ch-8"
        number={8}
        title="Trainable, with adapters"
        intro="The boundary stack ships with a native trainer, LoRA and DoRA — no Python in the loop."
      >
        <Scene id="peft" graphic={<TrainingFigure step={0} />}>
          <p>
            Adapters inject into the differentiable graph next to frozen, digest-pinned base weights.
            LoRA is the familiar low-rank pair; <strong>DoRA</strong> adds a learned magnitude vector,
            and every forward recomputes the weight <em>norm</em> of <code>W + scale·BA</code> from the
            live weights, detaching it through a <code>stop_gradient</code> intrinsic — kept as a graph
            node precisely so lowering can never lose the detach boundary. Exported adapters carry a receipt binding the source bundle
            identity, schema digest and frozen-weight digest.
          </p>
          <p>
            <CodeLink link={L("gliner25-peft-inject")} /> · <CodeLink link={L("gliner25-peft-kinds")} /> ·{" "}
            <CodeLink link={L("gliner25-stop-gradient")} />
          </p>
        </Scene>
        <Scene id="train" graphic={<TrainingFigure step={1} />}>
          <p>
            The native trainer (<code>train run gliner25</code>) runs under a supervised one-shot
            process with checkpoints, resume probes and memory envelopes. Two details are contractual
            rather than incidental: AdamW renormalizes a <em>partial</em> final gradient-accumulation
            window to the actual microbatch count, matching upstream exactly; and record supervision uses
            Hungarian assignment to match predicted instances to gold before the loss — over valid
            hypotheses only. Losses span focal boundary marginals, soft-IoU, abstention, count and
            listwise reranking.
          </p>
          <p>
            <CodeLink link={L("gliner25-train-entry")} /> · <CodeLink link={L("gliner25-adamw-renorm")} />{" "}
            · <CodeLink link={L("gliner25-hungarian-match")} />
          </p>
          <p className="text-xs">
            The full training story — job contract, gold schedule, eleven losses:{" "}
            <Link className="text-primary underline" href="/training/gliner25">
              Training → GLiNER2.5
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 9 · Recognized, not yet served ───────────────────────── */}
      <ScrollyChapter
        id="ch-9"
        number={9}
        title="Recognized, not yet served"
        intro="The runtime can parse, run, train and benchmark GLiNER2.5 — and still refuses to advertise it."
      >
        <Scene id="gate" graphic={<QualificationGateFigure />}>
          <p>
            Three code facts gate serving. <code>detectArchitecture</code> recognizes a boundary bundle
            from <code>architecture: "boundary"</code> (and fails closed on ambiguity). The architecture
            module declares <code>runtime_available = false</code>. And the qualification table —
            which must pin an exact model identity, backend, feature set and length limit per row — is
            deliberately empty, with no environment override. The manifest ANDs all three, so listing,
            capability and compatibility APIs report the model as unsupported until a reviewed release
            decision adds a row. Nothing on this page is an availability or performance claim.
          </p>
          <p>
            <CodeLink link={L("gliner25-detect-arch")} /> ·{" "}
            <CodeLink link={L("gliner25-runtime-withheld")} /> ·{" "}
            <CodeLink link={L("gliner25-qualification-empty")} /> ·{" "}
            <CodeLink link={L("gliner25-manifest-gate")} /> ·{" "}
            <CodeLink link={L("gliner25-executor-preflight")} />
          </p>
        </Scene>
        <Scene id="variants" graphic={<VariantsFigure />}>
          <p>
            Three published variants share an identical boundary-head configuration and 334-tensor
            layout: <strong>small</strong> (~74M, hidden 384, deberta-v3-xsmall),{" "}
            <strong>base</strong> (~194M, hidden 768, deberta-v3-base — the numbers on this page), and{" "}
            <strong>multi</strong> (~287M, mdeberta-v3-base, 250k multilingual vocab). Everything else on
            this page — pool of {spec.stats.poolSize}, top-32 pool proposals, ten marker types — is the
            same across all three.
          </p>
          <p className="text-xs">
            The shared runtime spine in full:{" "}
            <Link className="text-primary underline" href="/runtime">
              runtime walkthrough →
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>
    </div>
  );
}
