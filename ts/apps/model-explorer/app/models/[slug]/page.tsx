import { notFound } from "next/navigation";
import type { KernelCensus } from "@/content/registry";
import { kernels, manifest } from "@/lib/data";
import { frameQ40, frameQ80Anchor } from "@/lib/frames";
import { getModelSpec, modelSlugs } from "@/lib/models";
import { ModelPageClient } from "./model-page-client";

export function generateStaticParams() {
  return modelSlugs().map((slug) => ({ slug }));
}

export async function generateMetadata({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const spec = getModelSpec(slug);
  return { title: spec ? `${spec.displayName}` : "Model not found" };
}

function buildKernelCensus(): KernelCensus {
  const byFamily: Record<string, number> = {};
  for (const kernel of kernels.inventory) {
    byFamily[kernel.family] = (byFamily[kernel.family] ?? 0) + 1;
  }
  return {
    total: kernels.inventory.length,
    byFamily,
    routedSourceFiles: new Set(kernels.routes.map((r) => r.generatedFile).filter(Boolean)).size,
  };
}

export default async function ModelPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const spec = getModelSpec(slug);
  if (!spec) notFound();
  return (
    <ModelPageClient
      spec={spec}
      routes={kernels.routes}
      frames={{ q40: frameQ40, q80Anchor: frameQ80Anchor }}
      kernelCensus={buildKernelCensus()}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
