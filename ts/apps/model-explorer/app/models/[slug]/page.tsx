import { notFound } from "next/navigation";
import { contentDirFor, MODEL_LINK_IDS } from "@/content/link-ids";
import type { KernelCensus } from "@/content/registry";
import { collectSourceLinks, kernels, manifest, namedLinks, snippetsFor } from "@/lib/data";
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
  // Ship snippets only for the links this model's page can actually render:
  // the curated spec's own source links plus the chapter prose's named links.
  const linkIds = MODEL_LINK_IDS[contentDirFor(slug)] ?? [];
  const chapterLinks = linkIds.map((id) => namedLinks[id]).filter(Boolean);
  const snippets = snippetsFor([...collectSourceLinks(spec), ...chapterLinks]);
  return (
    <ModelPageClient
      spec={spec}
      routes={kernels.routes}
      frames={{ q40: frameQ40, q80Anchor: frameQ80Anchor }}
      kernelCensus={buildKernelCensus()}
      snippets={snippets}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
