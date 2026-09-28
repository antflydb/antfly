import { manifest } from "@/lib/data";
import { KvClient } from "./kv-client";

export const metadata = { title: "KV cache" };

export default function KvPage() {
  return <KvClient permalinkBase={manifest.permalinkBase} />;
}
