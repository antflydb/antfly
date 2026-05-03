"use client";

import { cn, Input } from "@antfly/design-system";
import { Search, X } from "lucide-react";
import type { MouseEvent } from "react";
import { useCallback, useMemo, useRef, useState } from "react";
import type { GraphSearchProps } from "./types";

export function GraphSearch({ nodes, onSelect, className }: GraphSearchProps) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const results = useMemo(() => {
    if (!query.trim()) return [];
    const q = query.toLowerCase();
    return nodes
      .filter((n) => n.label.toLowerCase().includes(q) || n.type.toLowerCase().includes(q))
      .slice(0, 10);
  }, [nodes, query]);

  const handleSelect = useCallback(
    (nodeId: string) => {
      onSelect(nodeId);
      setQuery("");
      setOpen(false);
      inputRef.current?.blur();
    },
    [onSelect]
  );

  return (
    <div className={cn("absolute top-3 left-3 z-[10] w-64", className)}>
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground pointer-events-none" />
        <Input
          ref={inputRef}
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          placeholder="Search nodes…"
          className="h-8 pl-8 pr-8 text-xs bg-background/90 backdrop-blur-sm"
        />
        {query && (
          <button
            type="button"
            onClick={() => {
              setQuery("");
              inputRef.current?.focus();
            }}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
          >
            <X className="size-3.5" />
          </button>
        )}
      </div>
      {open && results.length > 0 && (
        <div className="mt-1 rounded-md border border-border bg-background/95 backdrop-blur-sm shadow-md overflow-hidden">
          {results.map((node) => (
            <button
              type="button"
              key={node.id}
              onMouseDown={(e: MouseEvent<HTMLButtonElement>) => e.preventDefault()}
              onClick={() => handleSelect(node.id)}
              className="flex w-full items-center gap-2 px-3 py-1.5 text-xs hover:bg-accent text-left"
            >
              <span className="truncate font-medium">{node.label}</span>
              <span className="ml-auto shrink-0 capitalize text-muted-foreground">{node.type}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
