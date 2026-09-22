import { collectSourceLinks, kernels, manifest, snippetsFor } from "@/lib/data";
import { KernelsClient } from "./kernels-client";

export const metadata = { title: "Kernel routing" };

export default function KernelsPage() {
  return (
    <KernelsClient
      routes={kernels.routes}
      inventory={kernels.inventory}
      snippets={snippetsFor(collectSourceLinks(kernels.routes))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
