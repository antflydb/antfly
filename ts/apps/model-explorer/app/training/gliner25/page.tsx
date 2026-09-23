import { L, manifest, snippetsFor } from "@/lib/data";
import { Gliner25TrainingClient } from "./gliner25-training-client";

const LINK_IDS = [
  "gliner25-ft-one-shot-parent",
  "gliner25-ft-job-validate",
  "gliner25-ft-job-execute",
  "gliner25-ft-gold-schedule",
  "gliner25-ft-objectives",
  "gliner25-ft-focal",
  "gliner25-ft-listwise",
  "gliner25-ft-consistency",
  "gliner25-ft-hard-negatives",
  "gliner25-ft-costs-f32",
  "gliner25-ft-attn-profiles",
  "gliner25-ft-activation-profiles",
  "gliner25-ft-attn-vjp",
  "gliner25-ft-dt-kernel",
  "gliner25-ft-streams",
  "gliner25-ft-transaction",
  "gliner25-ft-export",
  "gliner25-ft-merge-policy",
  "gliner25-ft-recompute",
  "gliner25-ft-dataset-row",
  "gliner25-ft-targets-compile",
  "gliner25-train-entry",
  "gliner25-peft-inject",
  "gliner25-peft-kinds",
  "gliner25-stop-gradient",
  "gliner25-adamw-renorm",
  "gliner25-hungarian-match",
  "gliner2-ft-objectives",
];

export const metadata = { title: "Training · GLiNER2.5 finetuning" };

export default function Gliner25TrainingPage() {
  return (
    <Gliner25TrainingClient
      snippets={snippetsFor(LINK_IDS.map(L))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
