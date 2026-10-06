import { manifest } from "@/lib/data";
import { frameQ40, frameQ80Anchor } from "@/lib/frames";
import { TimelineClient } from "./timeline-client";

export const metadata = { title: "Frame timeline" };

export default function TimelinePage() {
  return (
    <TimelineClient
      frames={{ q40: frameQ40, q80: frameQ80Anchor }}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
