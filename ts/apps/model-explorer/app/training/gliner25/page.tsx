import { manifest } from "@/lib/data";
import { Gliner25TrainingClient } from "./gliner25-training-client";

export const metadata = { title: "Training · GLiNER2.5 finetuning" };

export default function Gliner25TrainingPage() {
  return <Gliner25TrainingClient permalinkBase={manifest.permalinkBase} />;
}
