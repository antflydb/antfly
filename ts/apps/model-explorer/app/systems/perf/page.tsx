import { manifest } from "@/lib/data";
import { PerfClient } from "./perf-client";

export const metadata = { title: "Performance & roofline" };

export default function PerfPage() {
  return <PerfClient permalinkBase={manifest.permalinkBase} />;
}
