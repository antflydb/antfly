import { manifest } from "@/lib/data";
import { TrainingClient } from "./training-client";

export const metadata = { title: "Training" };

export default function TrainingPage() {
  return <TrainingClient permalinkBase={manifest.permalinkBase} />;
}
