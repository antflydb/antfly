"use client";

import { useRouter } from "next/navigation";
import { SourceLinkProvider } from "@/components/code/source-link-context";
import { ChoiceGroup } from "@/components/primitives/choice-group";
import { OpDagExplorer } from "@/components/viz/op-dag-explorer";
import type { KernelRoute, ModelSpec } from "@/lib/schema";

export function ExploreClient({
  spec,
  routes,
  permalinkBase,
  allSlugs,
}: {
  spec: ModelSpec;
  routes: KernelRoute[];
  permalinkBase?: string;
  allSlugs: string[];
}) {
  const router = useRouter();
  return (
    <SourceLinkProvider permalinkBase={permalinkBase}>
      <div className="relative">
        <header className="border-b px-4 py-3">
          <h1 className="text-lg font-semibold">Operation explorer · {spec.displayName}</h1>
          <p className="mb-3 max-w-3xl text-xs text-muted-foreground">
            A curated map of the architecture, not a recording of a running model. Layers that
            repeat are drawn once, and the shapes and backend labels describe the documented path.
            Selecting a node opens the tensors it takes in and hands on, with a link to the code.
          </p>
          <ChoiceGroup
            label="Model operation explorer"
            value={spec.id}
            onValueChange={(value) => router.push(`/explore/${value}`)}
            options={allSlugs.map((slug) => ({ value: slug, label: slug }))}
            buttonClassName="h-7 px-3"
          />
        </header>
        <OpDagExplorer spec={spec} routes={routes} />
      </div>
    </SourceLinkProvider>
  );
}
