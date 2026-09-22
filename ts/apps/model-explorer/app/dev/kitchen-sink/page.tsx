import { kernels, manifest, snippetsFor } from "@/lib/data";
import { KitchenSinkClient } from "./sink-client";

export const metadata = {
  title: "Kitchen sink (dev)",
  robots: { index: false, follow: false },
};

export default function KitchenSinkPage() {
  const routes = kernels.routes.slice(0, 3);
  const snippets = snippetsFor(routes.map((r) => r.source));
  return (
    <KitchenSinkClient
      routes={routes}
      snippets={snippets}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
