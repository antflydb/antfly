import { manifest } from "@/lib/data";
import { RuntimeClient } from "./runtime-client";

export const metadata = { title: "Runtime" };

export default function RuntimePage() {
  return <RuntimeClient permalinkBase={manifest.permalinkBase} />;
}
