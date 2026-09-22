import { L, manifest, snippetsFor } from "@/lib/data";
import { TrainingClient } from "./training-client";

const LINK_IDS = [
  "train-recipe-cli",
  "train-graph-backward",
  "train-real-autodiff-trainer",
  "train-max-grad-norm",
  "train-adamw-config",
  "train-adamw-step",
  "train-executor-flag",
  "gliner25-adamw-renorm",
  "gliner25-train-entry",
  "gliner2-ft-cli-registry",
  "gemma4-train-dpo-report",
  "gliner25-ft-export",
];

export const metadata = { title: "Training" };

export default function TrainingPage() {
  return (
    <TrainingClient
      snippets={snippetsFor(LINK_IDS.map(L))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
