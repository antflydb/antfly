import Link from "next/link";
import { type DemoCategoryKey, demoCategories } from "@/lib/demos";
import { DensityToggle } from "./density-toggle";
import { ThemeToggle } from "./theme-toggle";

export function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <aside className="sticky top-0 hidden h-screen w-64 shrink-0 overflow-y-auto border-r border-border bg-background/60 px-4 py-6 md:block">
        <Link href="/" className="block px-2 pb-6">
          <span className="font-display text-lg tracking-tight">@antfly/design-system</span>
          <span className="mt-1 block text-xs text-muted-foreground">Component gallery</span>
        </Link>
        <div className="mb-6 border-b border-border pb-4">
          <Link
            href="/foundations"
            className="block rounded-md px-2 py-1.5 text-sm font-medium text-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            Foundations
          </Link>
        </div>
        {(Object.keys(demoCategories) as DemoCategoryKey[]).map((key) => (
          <div key={key} className="mb-6">
            <h3 className="px-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              {demoCategories[key].label}
            </h3>
            <ul className="mt-2 space-y-0.5">
              {demoCategories[key].demos.map((d) => (
                <li key={d.slug}>
                  <Link
                    href={`/components/${d.slug}`}
                    className="block rounded-md px-2 py-1.5 text-sm text-foreground/80 transition-colors hover:bg-accent hover:text-accent-foreground"
                  >
                    {d.name}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </aside>
      <div className="flex flex-1 flex-col">
        <header className="sticky top-0 z-10 flex h-14 items-center justify-between border-b border-border bg-background/80 px-6 backdrop-blur">
          <Link href="/" className="font-display text-base tracking-tight md:hidden">
            @antfly/design-system
          </Link>
          <span className="hidden text-sm text-muted-foreground md:inline">
            Local playground · not published
          </span>
          <div className="flex items-center gap-1">
            <DensityToggle />
            <ThemeToggle />
          </div>
        </header>
        <main className="flex-1 px-6 py-10">{children}</main>
      </div>
    </div>
  );
}
