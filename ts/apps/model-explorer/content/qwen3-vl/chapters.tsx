"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { EnvFlagChip, QuantChip } from "@/components/primitives/chips";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { L } from "@/lib/links";
import type { ChaptersProps } from "../registry";
import {
  MRopeClocksFigure,
  PixelsToTokensFigure,
  RerankerFigure,
  TwoVsThreeAxisFigure,
  VisionTowerFigure,
  VlSpineNotesFigure,
} from "./figures";

export function Qwen3VlChapters({ spec }: ChaptersProps) {
  return (
    <div>
      {/* ── Ch 1 · Pixels are tokens too ────────────────────────────── */}
      <ScrollyChapter
        id="ch-1"
        number={1}
        title="Pixels are tokens too"
        intro="The 2B Instruct example connects an image encoder to a causal text decoder."
      >
        <Scene id="pixels" graphic={<PixelsToTokensFigure />}>
          <p>
            An image is chopped into 16×16-pixel patches (doubled across a 2-frame temporal window), pushed
            through a 24-block vision tower, and merged 4-into-1 — a 768×768 image after resizing becomes 576 visual tokens
            of width {spec.stats.hidden}. From the decoder's perspective they are just embeddings in the
            sequence: attended to causally and cached in the same KV pools. Each visual token has the same
            decoder KV footprint as a text token, but images also require preprocessing, the vision tower,
            projector and DeepStack work.
          </p>
          <p>
            <CodeLink link={L("qwen3vl-projector")} /> <QuantChip format="q8_0" />
            <span className="ml-1 text-xs text-muted-foreground">(mmproj) +</span> <QuantChip format="q4_k" />
            <span className="ml-1 text-xs text-muted-foreground">(decoder)</span>
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 2 · The vision tower ─────────────────────────────────── */}
      <ScrollyChapter
        id="ch-2"
        number={2}
        title="The vision tower"
        intro={
          <p>
            Not a CLIP pooling head: a temporal-split Conv3D patch embed, learned-position interpolation,
            two-axis RoPE, {spec.stats.visionBlocks} attention blocks, and feature taps partway up.
          </p>
        }
      >
        <Scene id="patch" graphic={<VisionTowerFigure step={0} />}>
          <p>
            <strong>Patches first.</strong> The architecture uses a temporal patch of 16×16 pixels × 2
            frames. Antfly currently accepts still images and duplicates their frame to fill that patch;
            video input is rejected. Images are resized to a merge-aligned grid within the request budget.
            A learned 48×48 position table (2,304 entries) is <em>bilinearly interpolated</em> to that grid;
            interpolation alone does not guarantee accuracy at arbitrary resolutions.
          </p>
          <p>
            <CodeLink link={L("qwen3vl-patchify")} /> · <CodeLink link={L("qwen3vl-pos-interp")} />
          </p>
        </Scene>
        <Scene id="merge" graphic={<VisionTowerFigure step={1} />}>
          <p>
            <strong>The 2×2 merger</strong> zips each block of four neighboring patch vectors into one:
            concat to 4,096 dims, MLP down to the decoder's {spec.stats.hidden}. Four times fewer tokens hit
            the expensive causal stack, and the spatial relationships survive inside the concatenation rather
            than being averaged away.
          </p>
          <p>
            <CodeLink link={L("qwen3vl-merger")} />
          </p>
        </Scene>
        <Scene id="deepstack" graphic={<VisionTowerFigure step={2} />}>
          <p>
            <strong>DeepStack.</strong> After zero-based vision blocks 5, 11 and 17, features exit sideways through
            dedicated merger heads and are <em>added after decoder layers 0, 1 and 2</em>{" "}
            at visual-token positions during prefill — mid-level texture and layout information that the top of the tower
            would have abstracted away. If Gemma4's PLE ribbon looked familiar, it should: this is the same
            visual grammar — a side lane feeding the main stack — carrying different physics.
          </p>
          <p>
            Antfly carries DeepStack features separately from the main visual-token embeddings and checks
            their layer count, shape and visual mask. These features add information without extending the
            token sequence. <CodeLink link={L("qwen3vl-deepstack-tap")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 3 · m-RoPE: three clocks ─────────────────────────────── */}
      <ScrollyChapter
        id="ch-3"
        number={3}
        title="m-RoPE: three clocks"
        intro="One rotary embedding, three position streams. Drag the slider."
      >
        <Scene id="clocks" graphic={<MRopeClocksFigure />}>
          <p>
            Every token carries <em>three</em> positions — temporal, height, width — and the
            head dimension is partitioned into interleaved sections rotating on each stream. In plain text the
            three clocks tick in lockstep, which collapses m-RoPE back to ordinary RoPE. Inside an image, the
            temporal hand freezes while h and w advance with the patch grid: two patches in the same row share
            an h phase, two in the same column share w.
          </p>
          <p>
            The position plan is built once per request; decode continues at{" "}
            <code>tokenCount + mrope_position_delta</code>, because a 2D grid consumes fewer sequential
            positions than it has tokens.
          </p>
          <p>
            <CodeLink link={L("qwen3vl-plan")} /> · <CodeLink link={L("qwen3vl-mrope-kernel")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 4 · Two-axis vs three-axis ───────────────────────────── */}
      <ScrollyChapter
        id="ch-4"
        number={4}
        title="Two-axis vs three-axis"
        intro="The tower and the decoder both rotate positions — but they're solving different problems."
      >
        <Scene id="axes" graphic={<TwoVsThreeAxisFigure />}>
          <p>
            The vision tower's RoPE is <strong>two-axis</strong>: patch row and patch column, bidirectional,
            scoped to one image — it only ever needs to know where a patch sits in <em>its</em> grid. The
            decoder's m-RoPE is <strong>three-axis</strong> because its problem is disambiguation across an
            interleaved sequence. All axes advance together through text; image grids have their own
            spatial coordinates and start offsets derived from preceding content. Later text resumes
            after the largest coordinate used. The temporal axis also belongs to the upstream video
            architecture, although Antfly's current input path rejects video.
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 5 · Same decoder, new inputs ─────────────────────────── */}
      <ScrollyChapter
        id="ch-5"
        number={5}
        title="Same decoder, new inputs"
        intro={
          <p>
            The text stack is a stock {spec.stats.layers}-layer Qwen3: GQA {spec.stats.queryHeads}/
            {spec.stats.kvHeads}, Q/K head norm, SwiGLU. It runs in the same <code>gpt.zig</code> that serves
            Gemma4 and Qwen3 Embedding.
          </p>
        }
      >
        <Scene id="interleave" graphic={<VlSpineNotesFigure />}>
          <p>
            Visual tokens are spliced into the embedding stream at the <code>&lt;|image_pad|&gt;</code>{" "}
            markers before layer 0; from there the runtime is family-gated, not forked —{" "}
            <code>family == .qwen3_vl</code> switches on m-RoPE positions and DeepStack injection, alongside Qwen-specific geometry and validation. Much of execution is shared: planned Metal
            frames, paged KV and quantized linear operations.
          </p>
          <p>
            The projector has its own architecture implementation, while the decoder reuses shared GPT
            execution. Admission validates the actual artifact route and backend capabilities.{" "}
            <CodeLink link={L("qwen3vl-family-gate")} />
          </p>
          <p>
            <CodeLink link={L("qwen3vl-splice")} /> ·{" "}
            <Link className="text-primary underline" href="/explore/qwen3-vl">
              walk the full DAG →
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 6 · The reranker variant ─────────────────────────────── */}
      <ScrollyChapter
        id="ch-6"
        number={6}
        title="The reranker variant"
        intro="Take the same model, ask it one question, never let it answer in words."
      >
        <Scene id="rerank" graphic={<RerankerFigure />}>
          <p>
            Qwen3-VL-Reranker wraps query, document and image into a prompt whose system message pins the
            answer space to "yes" or "no". The relevance score is{" "}
            <code>sigmoid(logit_yes − logit_no)</code> — computed from the last active hidden state using the difference between the yes/no head rows.
            The converted serving bundle stores a two-row semantic classifier head in F16. Each
            query/candidate pair is scored pointwise; eligible text-only requests can batch candidates.
            There is no generation or sampling. A score in [0,1] is not automatically a calibrated
            probability of relevance for a given dataset.
          </p>
          <p>
            <CodeLink link={L("qwen3vl-reranker-score")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 7 · Spine notes ──────────────────────────────────────── */}
      <ScrollyChapter
        id="ch-7"
        number={7}
        title="Spine notes"
        intro="What this model added to the kernel inventory, and what it borrowed."
      >
        <Scene id="notes" graphic={<VlSpineNotesFigure />}>
          <p>
            Qwen3-VL-specific integration: <code>termite_apply_mrope</code>, the Conv3D/merger vision path, and DeepStack
            injection. Attention, quantized linear primitives and frame planning are shared, while
            the VL-specific fast paths ship behind their own flags:
          </p>
          <p>
            <EnvFlagChip name="TERMITE_METAL_DISABLE_QWEN3VL_DECODE_FRAME" defaultOn={false} />{" "}
            <EnvFlagChip name="TERMITE_METAL_DISABLE_QWEN3VL_PREPARED_FFN" defaultOn={false} />{" "}
            <EnvFlagChip name="TERMITE_QWEN3VL_PROFILE" defaultOn={false} />
          </p>
          <p>
            Current admission checks required artifacts, tensors, serving role and backend support; exact
            qualification receipts are not a serving allowlist. The split decoder/projector route
            requires Metal; integrated safetensors generation also supports CUDA. Historical qualification
            reports cover specific artifacts and hardware, and do not certify every accepted model.
            Architectural context ({Number(spec.stats.context).toLocaleString("en-US")} tokens for this example)
            is distinct from the smaller request and resource limits used in serving.
          </p>
          <p>
            <CodeLink link={L("model-compatibility-doc")} /> · <CodeLink link={L("qwen3vl-support-doc")} /> ·{" "}
            <Link className="text-primary underline" href="/systems/kernels">
              kernel inventory →
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>
    </div>
  );
}
