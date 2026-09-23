import { collectSourceLinks, manifest, snippetsFor } from "@/lib/data";
import { comparisonSamples } from "@/content/perf";
import { PerfClient } from "./perf-client";

export const metadata = { title: "Performance & roofline" };

export default function PerfPage() {
  return (
    <PerfClient
      snippets={snippetsFor(collectSourceLinks(comparisonSamples))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
