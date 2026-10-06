import { kernels, manifest } from "@/lib/data";
import { KitchenSinkClient } from "./sink-client";

export const metadata = {
  title: "Kitchen sink (dev)",
  robots: { index: false, follow: false },
};

export default function KitchenSinkPage() {
  const routes = kernels.routes.slice(0, 3);
  return <KitchenSinkClient routes={routes} permalinkBase={manifest.permalinkBase} />;
}
