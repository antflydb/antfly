import { Badge } from "@antfly/design-system";
import Link from "next/link";
import { type DemoCategoryKey, demoCategories } from "@/lib/demos";

export default function HomePage() {
  const totalCount = Object.values(demoCategories).reduce((n, c) => n + c.demos.length, 0);

  return (
    <div className="mx-auto max-w-5xl space-y-16">
      <section className="space-y-4">
        <Badge variant="secondary">{totalCount} components</Badge>
        <h1 className="font-display text-5xl tracking-tight text-foreground">
          @antfly/design-system
        </h1>
        <p className="max-w-2xl text-lg text-muted-foreground">
          A local gallery for every component shipped in the library. Toggle light / dark from the
          top-right to check every token-driven color path. Click into a component for the isolated
          demo.
        </p>
        <Link
          href="/foundations"
          className="inline-flex items-center gap-2 text-sm font-medium text-primary hover:underline"
        >
          View design tokens →
        </Link>
      </section>

      {(Object.keys(demoCategories) as DemoCategoryKey[]).map((key) => {
        const category = demoCategories[key];
        return (
          <section key={key} className="space-y-6">
            <div className="flex items-end justify-between gap-4 border-b border-border pb-2">
              <h2 className="font-display text-2xl tracking-tight">{category.label}</h2>
              <span className="text-sm text-muted-foreground">
                {category.demos.length} components
              </span>
            </div>
            <ul className="grid gap-3 sm:grid-cols-2 md:grid-cols-3">
              {category.demos.map((d) => (
                <li key={d.slug}>
                  <Link
                    href={`/components/${d.slug}`}
                    className="block h-full rounded-xl border border-border bg-card p-4 transition-all hover:border-primary/40 hover:shadow-sm"
                  >
                    <p className="font-display text-base text-card-foreground">{d.name}</p>
                    <p className="mt-1 text-sm text-muted-foreground">{d.description}</p>
                  </Link>
                </li>
              ))}
            </ul>
          </section>
        );
      })}
    </div>
  );
}
