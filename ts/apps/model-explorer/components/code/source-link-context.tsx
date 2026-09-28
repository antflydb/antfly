"use client";

import { createContext, useContext, useMemo } from "react";
import type { SourceLink } from "@/lib/schema";

interface SourceLinkContextValue {
  permalinkBase?: string;
}

const SourceLinkContext = createContext<SourceLinkContextValue>({});

/** Server components pass down the GitHub base URL that source pills open. */
export function SourceLinkProvider({
  permalinkBase,
  children,
}: SourceLinkContextValue & { children: React.ReactNode }) {
  const value = useMemo(() => ({ permalinkBase }), [permalinkBase]);
  return <SourceLinkContext.Provider value={value}>{children}</SourceLinkContext.Provider>;
}

export function useSourceLinks() {
  const ctx = useContext(SourceLinkContext);
  return {
    permalinkFor: (link: SourceLink): string | undefined =>
      ctx.permalinkBase ? `${ctx.permalinkBase}/${link.path}` : undefined,
  };
}
