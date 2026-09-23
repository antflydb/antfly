import { L, manifest, snippetsFor } from "@/lib/data";
import { frameQ40, frameQ80Anchor } from "@/lib/frames";
import { TimelineClient } from "./timeline-client";

const LINK_IDS = ["planner-encoder-scope", "runtime-begin-frame", "executor-pipelined-decode"];

export const metadata = { title: "Frame timeline" };

export default function TimelinePage() {
  return (
    <TimelineClient
      frames={{ q40: frameQ40, q80: frameQ80Anchor }}
      snippets={snippetsFor(LINK_IDS.map(L))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
