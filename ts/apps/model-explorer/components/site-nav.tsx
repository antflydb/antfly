"use client";

import {
  Button,
  cn,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@antfly/design-system";
import { ChevronDown, Menu, Moon, Sun } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { useEffect, useState } from "react";

const MODELS = [
  { slug: "gemma4-e4b", name: "Gemma4 E4B", hook: "PLE + variant-specific iSWA + shared KV" },
  {
    slug: "gemma4-e2b",
    name: "Gemma4 E2B",
    hook: "the compact variant: 4:1 iSWA, 20 shared-KV layers",
  },
  { slug: "gliner2", name: "GLiNER2", hook: "disentangled attention + span head" },
  { slug: "gliner25", name: "GLiNER2.5", hook: "boundary proposals + shared candidate pool" },
  { slug: "qwen3-embedding", name: "Qwen3 Embedding", hook: "last-token pooling, 8k-qualified" },
  { slug: "qwen3-vl", name: "Qwen3-VL", hook: "pixels → m-RoPE" },
];

const TRAINING = [
  { href: "/training", name: "Overview", hook: "one autodiff library, two philosophies" },
  {
    href: "/training/gemma4",
    name: "Gemma4 · DPO & GRPO",
    hook: "preference tuning on the CPU trainer",
  },
  {
    href: "/training/gliner2",
    name: "GLiNER2 finetuning",
    hook: "graph-first: the same graph learns",
  },
  {
    href: "/training/gliner25",
    name: "GLiNER2.5 finetuning",
    hook: "contract-first: a one-shot supervised job",
  },
];

const SYSTEMS = [
  { href: "/systems/perf", name: "Performance & roofline" },
  { href: "/systems/kernels", name: "Kernel routing" },
  { href: "/systems/timeline", name: "Frame timeline" },
  { href: "/systems/kv", name: "KV cache" },
];

function ThemeToggle() {
  const { resolvedTheme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  if (!mounted) return <div className="size-8" />;
  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label="Toggle theme"
      onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
    >
      {resolvedTheme === "dark" ? <Sun className="size-4" /> : <Moon className="size-4" />}
    </Button>
  );
}

export function SiteNav() {
  const pathname = usePathname();
  const active = (prefix: string) => pathname?.startsWith(prefix);

  return (
    <header className="sticky top-0 z-50 border-b bg-background/90 backdrop-blur">
      <nav aria-label="Site" className="mx-auto flex h-14 max-w-7xl items-center gap-1 px-4">
        <Link href="/" className="mr-4 font-semibold tracking-tight">
          Antfly <span className="text-muted-foreground">Model Explorer</span>
        </Link>
        <div className="site-desktop-nav items-center gap-1">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant={active("/models") ? "outline" : "ghost"} size="sm">
                Models <ChevronDown className="size-3" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-72">
              {MODELS.map((m) => (
                <DropdownMenuItem key={m.slug} asChild>
                  <Link href={`/models/${m.slug}`} className="flex flex-col items-start gap-0.5">
                    <span className="font-medium">{m.name}</span>
                    <span className="text-xs text-muted-foreground">{m.hook}</span>
                  </Link>
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
          <Button variant={active("/runtime") ? "outline" : "ghost"} size="sm" asChild>
            <Link href="/runtime" aria-current={active("/runtime") ? "page" : undefined}>
              Runtime
            </Link>
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant={active("/training") ? "outline" : "ghost"} size="sm">
                Training <ChevronDown className="size-3" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-72">
              {TRAINING.map((t) => (
                <DropdownMenuItem key={t.href} asChild>
                  <Link href={t.href} className="flex flex-col items-start gap-0.5">
                    <span className="font-medium">{t.name}</span>
                    <span className="text-xs text-muted-foreground">{t.hook}</span>
                  </Link>
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
          <Button variant={active("/explore") ? "outline" : "ghost"} size="sm" asChild>
            <Link href="/explore/gemma4-e4b" aria-current={active("/explore") ? "page" : undefined}>
              Operation explorer
            </Link>
          </Button>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant={active("/systems") ? "outline" : "ghost"} size="sm">
                Systems <ChevronDown className="size-3" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start">
              {SYSTEMS.map((s) => (
                <DropdownMenuItem key={s.href} asChild>
                  <Link href={s.href}>{s.name}</Link>
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
          <Button variant={active("/legend") ? "outline" : "ghost"} size="sm" asChild>
            <Link href="/legend" aria-current={active("/legend") ? "page" : undefined}>
              Legend
            </Link>
          </Button>
        </div>
        <div className="site-mobile-nav ml-auto">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon" aria-label="Open navigation">
                <Menu className="size-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="max-h-[80vh] overflow-auto">
              {[
                ...MODELS.map((m) => ({ href: `/models/${m.slug}`, name: m.name })),
                { href: "/runtime", name: "Runtime" },
                ...TRAINING.map((t) => ({
                  href: t.href,
                  name: t.name === "Overview" ? "Training" : t.name,
                })),
                { href: "/explore/gemma4-e4b", name: "Operation explorer" },
                ...SYSTEMS,
                { href: "/legend", name: "Legend" },
              ].map((item) => (
                <DropdownMenuItem key={item.href} asChild>
                  <Link href={item.href}>{item.name}</Link>
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
        <div className={cn("ml-1 md:ml-auto")}>
          <ThemeToggle />
        </div>
      </nav>
    </header>
  );
}
