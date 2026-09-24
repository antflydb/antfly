"use client";

import { cn } from "@antfly/design-system";
import { ExternalLink } from "lucide-react";
import type { SourceLink } from "@/lib/schema";
import { useSourceLinks } from "./source-link-context";

function basename(path: string): string {
  return path.split("/").pop() ?? path;
}

/**
 * A `file.zig` pill that opens the source file on GitHub. Links point at the
 * default branch by path only; the explorer deliberately carries no line
 * numbers or snippets, so ordinary edits to the runtime never stale it.
 */
export function CodeLink({
  link,
  label,
  className,
}: {
  link: SourceLink;
  label?: string;
  className?: string;
}) {
  const { permalinkFor } = useSourceLinks();
  const href = permalinkFor(link);
  const text = label ?? basename(link.path);

  const pillClass = cn(
    "inline-flex items-center gap-1 rounded-sm border bg-muted/40 px-1.5 py-px font-mono text-[11px] text-foreground/80 transition-colors hover:border-primary/60 hover:text-foreground",
    className
  );
  // Without a permalink base there is nothing to open: render a span, not a
  // dead <a href={undefined}> that looks clickable but does nothing.
  if (!href) {
    return (
      <span className={pillClass} title={link.path}>
        {text}
      </span>
    );
  }
  return (
    <a href={href} target="_blank" rel="noreferrer" className={pillClass} title={link.path}>
      {text}
      <ExternalLink className="size-2.5 opacity-60" />
    </a>
  );
}
