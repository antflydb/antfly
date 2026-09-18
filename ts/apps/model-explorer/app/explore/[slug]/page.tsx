import { notFound } from "next/navigation";
import { ExploreClient } from "@/app/explore/[slug]/explore-client";
import { collectSourceLinks, kernels, manifest, snippetsFor } from "@/lib/data";
import { getModelSpec, modelSlugs } from "@/lib/models";

export function generateStaticParams() {
  return modelSlugs().map((slug) => ({ slug }));
}

export async function generateMetadata({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const spec = getModelSpec(slug);
  return { title: spec ? `Operation explorer · ${spec.displayName}` : "Model not found" };
}

export default async function ExplorePage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const spec = getModelSpec(slug);
  if (!spec) notFound();
  const snippets = snippetsFor([...collectSourceLinks(spec), ...kernels.routes.map((r) => r.source)]);
  return (
    <ExploreClient
      spec={spec}
      routes={kernels.routes}
      snippets={snippets}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
      allSlugs={modelSlugs()}
    />
  );
}
