import { L, manifest, snippetsFor } from "@/lib/data";
import { Gliner2TrainingClient } from "./gliner2-training-client";

const LINK_IDS = [
  "gliner2-ft-objectives",
  "gliner2-ft-build-loss",
  "gliner2-ft-masked-bce",
  "gliner2-ft-attn-vjp",
  "gliner2-ft-bwd-kernel",
  "gliner2-ft-rank1-gate",
  "gliner2-ft-cli-main",
  "gliner2-ft-compiled-required",
  "gliner2-ft-cli-registry",
  "gliner2-ft-recipe-steps",
  "gliner2-ft-tensors-doc",
  "gliner2-ft-gates-doc",
  "train-executor-flag",
  "train-real-autodiff-trainer",
  "train-graph-backward",
];

export const metadata = { title: "Training · GLiNER2 finetuning" };

export default function Gliner2TrainingPage() {
  return (
    <Gliner2TrainingClient
      snippets={snippetsFor(LINK_IDS.map(L))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
