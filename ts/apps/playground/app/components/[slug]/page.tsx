import { Badge, Separator } from "@antfly/design-system";
import { notFound } from "next/navigation";
import { allDemos, findDemo } from "@/lib/demos";

export function generateStaticParams() {
  return allDemos().map((d) => ({ slug: d.slug }));
}

interface PageProps {
  params: Promise<{ slug: string }>;
}

export default async function ComponentPage({ params }: PageProps) {
  const { slug } = await params;
  const found = findDemo(slug);
  if (!found) notFound();
  const { demo, category } = found;

  return (
    <div className="mx-auto max-w-5xl space-y-10">
      <div className="space-y-3">
        <Badge variant="outline">{category}</Badge>
        <h1 className="font-display text-4xl tracking-tight text-foreground">{demo.name}</h1>
        <p className="max-w-2xl text-base text-muted-foreground">{demo.description}</p>
      </div>

      <Separator />

      <section className="rounded-xl border border-border bg-card p-8">
        <p className="mb-6 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Preview
        </p>
        <div className="flex flex-wrap items-start gap-6">{demo.render()}</div>
      </section>
    </div>
  );
}
