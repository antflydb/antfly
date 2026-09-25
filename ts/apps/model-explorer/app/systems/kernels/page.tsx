import { kernels, manifest } from "@/lib/data";
import { KernelsClient } from "./kernels-client";

export const metadata = { title: "Kernel routing" };

export default function KernelsPage() {
  return (
    <KernelsClient
      routes={kernels.routes}
      inventory={kernels.inventory}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
