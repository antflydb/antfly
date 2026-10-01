import { manifest } from "@/lib/data";
import { Gliner2TrainingClient } from "./gliner2-training-client";

export const metadata = { title: "Training · GLiNER2 finetuning" };

export default function Gliner2TrainingPage() {
  return <Gliner2TrainingClient permalinkBase={manifest.permalinkBase} />;
}
