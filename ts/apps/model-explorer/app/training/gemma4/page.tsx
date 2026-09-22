import { L, manifest, snippetsFor } from "@/lib/data";
import { Gemma4TrainingClient } from "./gemma4-training-client";

const LINK_IDS = [
  "gemma4-train-dpo-loss",
  "gemma4-train-loss-kinds",
  "gemma4-train-harness-step",
  "gemma4-train-ref-disabled",
  "gemma4-train-dpo-recipe",
  "gemma4-train-dpo-gate",
  "gemma4-train-ce-inject",
  "gemma4-train-seq-logprob",
  "gemma4-train-grpo-adv",
  "gemma4-train-grpo-loss",
  "gemma4-train-grpo-recipe",
  "gemma4-train-grpo-sampler",
  "gemma4-train-rank-rollout",
  "gemma4-train-mm-grpo",
  "gemma4-train-ref-path-guard",
  "gemma4-train-reward-mode",
  "gemma4-train-dpo-report",
  "gemma4-train-grpo-report",
  "train-recipe-cli",
  "train-real-autodiff-trainer",
  "train-max-grad-norm",
];

export const metadata = { title: "Training · Gemma4 preference tuning" };

export default function Gemma4TrainingPage() {
  return (
    <Gemma4TrainingClient
      snippets={snippetsFor(LINK_IDS.map(L))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
