import { envFlags, manifest } from "@/lib/data";
import { FlagsClient } from "./flags-client";

export const metadata = { title: "Env flags" };

export default function FlagsPage() {
  // Trim to what the table renders — keeps the client payload lean.
  const flags = envFlags.flags.map((f) => ({
    name: f.name,
    kind: f.kind,
    occurrences: f.occurrences,
    source: f.sources[0],
  }));
  return <FlagsClient flags={flags} gitCommit={manifest.gitCommit} permalinkBase={manifest.permalinkBase} />;
}
