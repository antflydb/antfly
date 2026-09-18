"use client";

import { createContext, useContext, useMemo } from "react";
import type { SourceLink } from "@/lib/schema";

export interface ClientSnippet {
  path: string;
  line: number;
  startLine: number;
  html: string;
  lang: string;
}

interface SnippetContextValue {
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}

const SnippetContext = createContext<SnippetContextValue>({ snippets: {}, gitCommit: "main" });

/**
 * Server components pass down only the snippets their page actually
 * references, keeping the 470 KB snippet cache out of client bundles.
 */
export function SnippetProvider({
  snippets,
  gitCommit,
  permalinkBase,
  children,
}: SnippetContextValue & { children: React.ReactNode }) {
  const value = useMemo(
    () => ({ snippets, gitCommit, permalinkBase }),
    [snippets, gitCommit, permalinkBase],
  );
  return <SnippetContext.Provider value={value}>{children}</SnippetContext.Provider>;
}

export function useSnippets() {
  const ctx = useContext(SnippetContext);
  return {
    getSnippet: (link: SourceLink): ClientSnippet | undefined =>
      link.line !== undefined ? ctx.snippets[`${link.path}:${link.line}`] : undefined,
    permalinkFor: (link: SourceLink): string | undefined => {
      if (!ctx.permalinkBase) return undefined;
      const anchor = link.line !== undefined ? `#L${link.line}` : "";
      return `${ctx.permalinkBase}/${ctx.gitCommit}/${link.path}${anchor}`;
    },
  };
}
