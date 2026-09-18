"use client";

import { cn, HoverCard, HoverCardContent, HoverCardTrigger } from "@antfly/design-system";
import { ExternalLink } from "lucide-react";
import type { SourceLink } from "@/lib/schema";
import { useSnippets } from "./snippet-context";

function basename(path: string): string {
  return path.split("/").pop() ?? path;
}

/**
 * A `file.zig:line` pill. Opens the commit-pinned GitHub permalink; hovering
 * shows a pre-highlighted 11-line code peek when the snippet cache has one.
 */
export function CodeLink({ link, label, className }: { link: SourceLink; label?: string; className?: string }) {
  const { getSnippet, permalinkFor } = useSnippets();
  const snippet = getSnippet(link);
  const href = permalinkFor(link);
  const text = label ?? `${basename(link.path)}${link.line !== undefined ? `:${link.line}` : ""}`;

  const pillClass = cn(
    "inline-flex items-center gap-1 rounded-sm border bg-muted/40 px-1.5 py-px font-mono text-[11px] text-foreground/80 transition-colors hover:border-primary/60 hover:text-foreground",
    className,
  );
  // Without a permalink base there is nothing to open: render a span, not a
  // dead <a href={undefined}> that looks clickable but does nothing.
  const pill = href ? (
    <a href={href} target="_blank" rel="noreferrer" className={pillClass} title={`${link.path}:${link.line ?? ""}`}>
      {text}
      <ExternalLink className="size-2.5 opacity-60" />
    </a>
  ) : (
    <span className={pillClass} title={`${link.path}:${link.line ?? ""}`}>
      {text}
    </span>
  );

  if (!snippet) return pill;
  return (
    <HoverCard openDelay={200}>
      <HoverCardTrigger asChild>{pill}</HoverCardTrigger>
      <HoverCardContent side="top" className="w-[36rem] max-w-[90vw] overflow-hidden p-0">
        <div className="border-b bg-muted/50 px-3 py-1.5 font-mono text-[11px] text-muted-foreground">
          {link.path}:{link.line}
        </div>
        <div
          className="overflow-x-auto p-0 text-[11px] leading-snug [&_pre]:m-0 [&_pre]:bg-transparent! [&_pre]:p-3"
          // biome-ignore lint/security/noDangerouslySetInnerHtml: shiki output generated at build time from repo source
          dangerouslySetInnerHTML={{ __html: snippet.html }}
        />
      </HoverCardContent>
    </HoverCard>
  );
}
