"use client";

import {
  Badge,
  cn,
  Input,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@antfly/design-system";
import { parseAsString, parseAsStringLiteral, useQueryState } from "nuqs";
import { Suspense, useMemo } from "react";
import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { QuantChip } from "@/components/primitives/chips";
import { ChoiceGroup } from "@/components/primitives/choice-group";

import type { KernelInventoryEntry, KernelRoute } from "@/lib/schema";

const FAMILY_COLOR: Record<string, string> = {
  matvec: "var(--kfam-matvec)",
  mm_sg: "var(--kfam-mmsg)",
  attention: "var(--kfam-attention)",
  fusion: "var(--kfam-fusion)",
  moe: "var(--kfam-moe)",
  sampling: "var(--kfam-sampling)",
  kv: "var(--kfam-kv)",
  norm_rope: "var(--kfam-fusion)",
  vision: "var(--kfam-mmsg)",
  gliner: "var(--kfam-attention)",
  training: "var(--muted-foreground)",
  data_movement: "var(--muted-foreground)",
  other: "var(--muted-foreground)",
};

export function KernelsClient(props: {
  routes: KernelRoute[];
  inventory: KernelInventoryEntry[];
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}) {
  return (
    <Suspense fallback={<div className="min-h-screen" />}>
      <KernelsInner {...props} />
    </Suspense>
  );
}

function KernelsInner({
  routes,
  inventory,
  snippets,
  gitCommit,
  permalinkBase,
}: {
  routes: KernelRoute[];
  inventory: KernelInventoryEntry[];
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}) {
  const [q, setQ] = useQueryState("q", parseAsString.withDefault(""));
  const [family, setFamily] = useQueryState("family", parseAsString.withDefault("all"));
  const [batch, setBatch] = useQueryState(
    "batch",
    parseAsStringLiteral(["1", "32"] as const).withDefault("1")
  );

  const familyCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const k of inventory) counts.set(k.family, (counts.get(k.family) ?? 0) + 1);
    return [...counts.entries()].sort((a, b) => b[1] - a[1]);
  }, [inventory]);

  const filtered = useMemo(() => {
    const needle = q.toLowerCase();
    return inventory.filter(
      (k) => (family === "all" || k.family === family) && (needle === "" || k.name.includes(needle))
    );
  }, [inventory, q, family]);

  const smallBatch = batch === "1";

  return (
    <SnippetProvider snippets={snippets} gitCommit={gitCommit} permalinkBase={permalinkBase}>
      <div className="mx-auto max-w-7xl space-y-10 px-4 py-8">
        <header className="max-w-3xl">
          <h1 className="text-3xl font-bold tracking-tight">Kernel routing</h1>
          <p className="mt-2 text-muted-foreground">
            The generated quantized matvec routes come from the{" "}
            <code className="font-mono text-sm">metal_production_schedules</code> table, the source
            of the production schedule rows shown here — one row per format × row-bucket × epilogue,
            rendered into standalone MSL by{" "}
            <code className="font-mono text-sm">quant_kernel_metal_renderer.zig</code> at build time
            (<code className="font-mono text-sm">zig build quant-kernel-codegen</code>). This is a
            build-time renderer. Handwritten kernels and optional runtime JIT routes also exist;
            this table does not cover every dispatch decision.
          </p>
        </header>

        <section>
          <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
            <h2 className="text-lg font-semibold">
              Generated production schedule table ({routes.length} routes)
            </h2>
            <ChoiceGroup
              label="Kernel routing view"
              value={batch}
              onValueChange={setBatch}
              options={[
                { value: "1", label: "small-row schedules" },
                { value: "32", label: "larger-row paths" },
              ]}
            />
          </div>
          {smallBatch ? (
            <div className="overflow-x-auto rounded-lg border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-muted-foreground">format</TableHead>
                    <TableHead className="text-muted-foreground">rows</TableHead>
                    <TableHead className="text-muted-foreground">epilogue</TableHead>
                    <TableHead className="text-muted-foreground">threads/tg</TableHead>
                    <TableHead className="text-muted-foreground">cols/tg</TableHead>
                    <TableHead className="text-muted-foreground">rows/tg</TableHead>
                    <TableHead className="text-muted-foreground">reduction</TableHead>
                    <TableHead className="text-muted-foreground">generated kernel</TableHead>
                    <TableHead className="text-muted-foreground">schedule row</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {routes.map((r) => (
                    <TableRow
                      key={r.id}
                      className={cn(q && r.id.includes(q.toLowerCase()) && "bg-primary/5")}
                    >
                      <TableCell>
                        <QuantChip format={r.format} />
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {r.rowBucket.replace("rows_", "")}
                      </TableCell>
                      <TableCell className="font-mono text-xs">{r.epilogue}</TableCell>
                      <TableCell className="font-mono text-xs tabular-nums">
                        {r.schedule.threadsPerThreadgroup}
                      </TableCell>
                      <TableCell className="font-mono text-xs tabular-nums">
                        {r.schedule.colsPerThreadgroup}
                      </TableCell>
                      <TableCell className="font-mono text-xs tabular-nums">
                        {r.schedule.rowsPerThreadgroup ?? "—"}
                      </TableCell>
                      <TableCell className="font-mono text-xs">{r.schedule.reduction}</TableCell>
                      <TableCell className="font-mono text-[11px]">
                        {r.generatedFile ? (
                          <CodeLink
                            link={{ path: r.generatedFile }}
                            label={r.generatedFile.split("/").pop()}
                          />
                        ) : (
                          "—"
                        )}
                      </TableCell>
                      <TableCell>
                        <CodeLink link={r.source} label="source" />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          ) : (
            <div className="rounded-lg border bg-muted/20 p-6 text-sm text-muted-foreground">
              <p>
                Larger token-row counts may select handwritten{" "}
                <span className="font-mono text-foreground">*_mm_sg</span> simdgroup matrix-multiply
                kernels. Crossover depends on format, shape, device, and route policy; eight rows is
                not a universal switch. Rows are matrix rows, not necessarily the number of HTTP
                requests. Filter the inventory below by{" "}
                <button
                  type="button"
                  className="font-mono text-primary underline"
                  onClick={() => setFamily("mm_sg")}
                >
                  mm_sg
                </button>{" "}
                to see all {inventory.filter((k) => k.family === "mm_sg").length} of them.
              </p>
            </div>
          )}
        </section>

        <section>
          <h2 className="mb-3 text-lg font-semibold">Kernel inventory ({inventory.length})</h2>
          <p className="mb-3 text-xs text-muted-foreground">
            Source-extracted kernel entry points, grouped by name heuristics. Presence here does not
            imply a kernel is selected for every model or device.
          </p>
          <div className="mb-3 flex flex-wrap gap-1.5">
            <button
              type="button"
              onClick={() => setFamily("all")}
              aria-pressed={family === "all"}
              className={cn(
                "rounded-full border px-2.5 py-0.5 font-mono text-xs transition-colors",
                family === "all"
                  ? "border-primary text-primary"
                  : "text-muted-foreground hover:bg-accent"
              )}
            >
              all {inventory.length}
            </button>
            {familyCounts.map(([f, count]) => (
              <button
                key={f}
                type="button"
                onClick={() => setFamily(f)}
                aria-pressed={family === f}
                className={cn(
                  "rounded-full border px-2.5 py-0.5 font-mono text-xs transition-colors",
                  family === f
                    ? "border-primary text-primary"
                    : "text-muted-foreground hover:bg-accent"
                )}
                style={{ borderLeftColor: FAMILY_COLOR[f], borderLeftWidth: 3 }}
              >
                {f} {count}
              </button>
            ))}
          </div>
          <Input
            aria-label="Filter kernels"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Filter kernels… (e.g. pair_activation, gumbel, disentangled)"
            className="mb-3 max-w-md font-mono text-sm"
          />
          <p role="status" className="mb-2 text-xs text-muted-foreground">
            {filtered.length} matching kernels
          </p>
          <div className="grid grid-cols-1 gap-1 sm:grid-cols-2 lg:grid-cols-3">
            {filtered.slice(0, 120).map((k) => (
              <div
                key={k.name}
                className="flex min-w-0 items-center gap-2 rounded border px-2 py-1"
              >
                <span
                  className="size-2 shrink-0 rounded-full"
                  style={{ background: FAMILY_COLOR[k.family] }}
                />
                <CodeLink
                  link={k.source}
                  label={k.name}
                  className="min-w-0 flex-1 truncate border-0 bg-transparent px-0"
                />
                {k.generated && <Badge className="shrink-0 text-[9px]">gen</Badge>}
              </div>
            ))}
          </div>
          {filtered.length > 120 && (
            <p className="mt-2 text-xs text-muted-foreground">
              Showing 120 of {filtered.length} — narrow the filter.
            </p>
          )}
        </section>
      </div>
    </SnippetProvider>
  );
}
