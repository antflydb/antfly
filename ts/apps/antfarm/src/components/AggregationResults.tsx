import type React from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

interface AggregationResultData {
  value?: number;
  count?: number;
  min?: number;
  max?: number;
  sum?: number;
  avg?: number;
  std_deviation?: number;
  variance?: number;
  sum_of_squares?: number;
  buckets?: {
    key: string;
    key_as_string?: string;
    doc_count: number;
    from?: number;
    to?: number;
    from_as_string?: string;
    to_as_string?: string;
    score?: number;
    bg_count?: number;
  }[];
}

interface AggregationResultsProps {
  aggregations: Record<string, AggregationResultData>;
  className?: string;
}

const StatCard: React.FC<{ label: string; value: string | number }> = ({ label, value }) => (
  <div className="flex flex-col items-center justify-center p-3 bg-muted/30 rounded-lg border">
    <span className="text-2xl font-bold tabular-nums">
      {typeof value === "number" ? formatNumber(value) : value}
    </span>
    <span className="text-xs text-muted-foreground mt-0.5">{label}</span>
  </div>
);

function formatNumber(n: number): string {
  if (Number.isInteger(n)) return n.toLocaleString();
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

const BarChart: React.FC<{
  buckets: NonNullable<AggregationResultData["buckets"]>;
  name: string;
}> = ({ buckets, name }) => {
  if (buckets.length === 0) {
    return <p className="text-xs text-muted-foreground">No buckets returned</p>;
  }

  const maxCount = Math.max(...buckets.map((b) => b.doc_count ?? 0));

  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-2 mb-2">
        <span className="text-sm font-medium">{name}</span>
        <Badge variant="secondary" className="text-[10px]">
          {buckets.length} buckets
        </Badge>
      </div>
      {buckets.map((bucket) => {
        const count = bucket.doc_count ?? 0;
        const pct = maxCount > 0 ? (count / maxCount) * 100 : 0;
        const label = bucket.key_as_string || bucket.key;
        return (
          <div key={bucket.key} className="flex items-center gap-2 text-xs">
            <span className="w-28 truncate text-right text-muted-foreground shrink-0" title={label}>
              {label}
            </span>
            <div className="flex-1 h-5 bg-muted rounded-sm overflow-hidden relative">
              <div
                className={cn("h-full rounded-sm transition-all", "bg-primary/70")}
                style={{ width: `${pct}%` }}
              />
            </div>
            <span className="w-12 text-right tabular-nums text-muted-foreground shrink-0">
              {count.toLocaleString()}
            </span>
          </div>
        );
      })}
    </div>
  );
};

const AggregationResults: React.FC<AggregationResultsProps> = ({ aggregations, className }) => {
  if (!aggregations || Object.keys(aggregations).length === 0) return null;

  return (
    <Card className={className}>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Aggregation Results</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {Object.entries(aggregations).map(([name, result]) => {
          // Bucket aggregation (terms, range, histogram, etc.)
          if (result.buckets) {
            return <BarChart key={name} buckets={result.buckets} name={name} />;
          }

          // Stats aggregation (has count, min, max, sum, avg)
          if (result.count !== undefined && result.min !== undefined) {
            return (
              <div key={name} className="space-y-2">
                <span className="text-sm font-medium">{name}</span>
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-2">
                  <StatCard label="Count" value={result.count} />
                  {result.min !== undefined && <StatCard label="Min" value={result.min} />}
                  {result.max !== undefined && <StatCard label="Max" value={result.max} />}
                  {result.sum !== undefined && <StatCard label="Sum" value={result.sum} />}
                  {result.avg !== undefined && <StatCard label="Avg" value={result.avg} />}
                </div>
              </div>
            );
          }

          // Single metric (sum, avg, min, max, count, cardinality)
          if (result.value !== undefined) {
            return (
              <div key={name} className="space-y-2">
                <span className="text-sm font-medium">{name}</span>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                  <StatCard label={name} value={result.value} />
                </div>
              </div>
            );
          }

          return null;
        })}
      </CardContent>
    </Card>
  );
};

export default AggregationResults;
