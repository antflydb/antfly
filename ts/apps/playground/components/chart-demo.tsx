"use client";

import {
  type ChartConfig,
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from "@antfly/design-system";
import { chartSeries } from "@antfly/design-system/charts";
import { Bar, BarChart, CartesianGrid, XAxis } from "recharts";

const data = [
  { month: "Jan", queries: 86, indexing: 42 },
  { month: "Feb", queries: 112, indexing: 58 },
  { month: "Mar", queries: 148, indexing: 61 },
  { month: "Apr", queries: 190, indexing: 73 },
  { month: "May", queries: 210, indexing: 89 },
  { month: "Jun", queries: 245, indexing: 94 },
];

const config = {
  queries: { label: "Queries (k)", color: chartSeries[0] },
  indexing: { label: "Indexing (k)", color: chartSeries[1] },
} satisfies ChartConfig;

export function ChartDemo() {
  return (
    <ChartContainer config={config} className="h-64 w-full max-w-lg">
      <BarChart data={data}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="month" tickLine={false} axisLine={false} tickMargin={8} />
        <ChartTooltip content={<ChartTooltipContent />} />
        <ChartLegend content={<ChartLegendContent />} />
        <Bar dataKey="queries" fill="var(--color-queries)" radius={4} />
        <Bar dataKey="indexing" fill="var(--color-indexing)" radius={4} />
      </BarChart>
    </ChartContainer>
  );
}
