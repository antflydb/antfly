import { manifest } from "@/lib/data";
import { Gemma4TrainingClient } from "./gemma4-training-client";

export const metadata = { title: "Training · Gemma4 preference tuning" };

export default function Gemma4TrainingPage() {
  return <Gemma4TrainingClient permalinkBase={manifest.permalinkBase} />;
}
