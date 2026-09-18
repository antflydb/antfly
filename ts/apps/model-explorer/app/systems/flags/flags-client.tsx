"use client";

import { Badge, cn, Input } from "@antfly/design-system";
import { parseAsString, useQueryState } from "nuqs";
import { Suspense, useMemo } from "react";
import { CodeLink } from "@/components/code/code-link";
import { SnippetProvider } from "@/components/code/snippet-context";
import type { SourceLink } from "@/lib/schema";

interface FlagRow {
  name: string;
  kind: string;
  occurrences: number;
  source: SourceLink;
}

const PREFIXES = [
  ["all", "all"],
  ["TERMITE_METAL_", "metal"],
  ["TERMITE_", "termite (other)"],
  ["ANTFLY_GEMMA4_", "gemma4 / MTP"],
  // Substring bucket: GLiNER names span TERMITE_METAL_*GLINER*, *DEBERTA*
  // and ANTFLY_GLINER25_*, so a prefix can't collect them.
  ["*GLINER", "gliner / deberta"],
  ["ANTFLY_CUDA_", "cuda"],
  ["ANTFLY_", "antfly (other)"],
] as const;

const GLINER_RE = /GLINER|DEBERTA|DISENTANGLED/;

export function FlagsClient(props: { flags: FlagRow[]; gitCommit: string; permalinkBase?: string }) {
  return (
    <Suspense fallback={<div className="min-h-screen" />}>
      <FlagsInner {...props} />
    </Suspense>
  );
}

function FlagsInner({ flags, gitCommit, permalinkBase }: { flags: FlagRow[]; gitCommit: string; permalinkBase?: string }) {
  const [q, setQ] = useQueryState("q", parseAsString.withDefault(""));
  const [prefix, setPrefix] = useQueryState("prefix", parseAsString.withDefault("all"));

  const filtered = useMemo(() => {
    const needle = q.toUpperCase();
    return flags.filter((f) => {
      if (prefix === "*GLINER") {
        if (!GLINER_RE.test(f.name)) return false;
      } else if (prefix !== "all") {
        if (!f.name.startsWith(prefix)) return false;
        // Keep the prefix buckets disjoint: "TERMITE_" excludes "TERMITE_METAL_", etc.
        if (prefix === "TERMITE_" && f.name.startsWith("TERMITE_METAL_")) return false;
        if (prefix === "ANTFLY_" && (f.name.startsWith("ANTFLY_GEMMA4_") || f.name.startsWith("ANTFLY_CUDA_"))) return false;
      }
      return needle === "" || f.name.includes(needle);
    });
  }, [flags, q, prefix]);

  return (
    <SnippetProvider snippets={{}} gitCommit={gitCommit} permalinkBase={permalinkBase}>
      <div className="mx-auto max-w-5xl space-y-6 px-4 py-8">
        <header className="max-w-3xl">
          <h1 className="text-3xl font-bold tracking-tight">Env flags</h1>
          <p className="mt-2 text-muted-foreground">
            {flags.length} environment-style names found in the Zig source, including comments, tests, and
            diagnostics. Categories are inferred from names; occurrence counts are textual references.
            Open the source to check accepted values, defaults, and whether a name is read on a given execution path.
          </p>
        </header>

        <div className="flex flex-wrap gap-1.5">
          {PREFIXES.map(([p, label]) => (
            <button
              key={p}
              type="button"
              onClick={() => setPrefix(p)}
              aria-pressed={prefix === p}
              className={cn(
                "rounded-full border px-2.5 py-0.5 font-mono text-xs transition-colors",
                prefix === p ? "border-primary text-primary" : "text-muted-foreground hover:bg-accent",
              )}
            >
              {label}
            </button>
          ))}
        </div>
        <Input
          aria-label="Filter environment names"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Filter flags… (e.g. PIPELINED, GQA_SPLIT, MTP, GLINER)"
          className="max-w-md font-mono text-sm"
        />

        <p role="status" className="text-xs text-muted-foreground">{filtered.length} matching names</p>
        <div className="space-y-0.5">
          {filtered.slice(0, 200).map((f) => (
            <div key={f.name} className="flex flex-wrap items-center gap-2 rounded border-b px-2 py-1.5 text-sm last:border-0">
              <span className="min-w-0 basis-full break-all font-mono text-xs sm:flex-1 sm:basis-auto">{f.name}</span>
              <Badge className="shrink-0 text-[9px]">{f.kind}</Badge>
              <span className="w-14 shrink-0 text-right font-mono text-[10px] text-muted-foreground">
                ×{f.occurrences}
              </span>
              <CodeLink link={f.source} className="shrink-0" />
            </div>
          ))}
        </div>
        {filtered.length > 200 && (
          <p className="text-xs text-muted-foreground">Showing 200 of {filtered.length} — narrow the filter.</p>
        )}
      </div>
    </SnippetProvider>
  );
}
