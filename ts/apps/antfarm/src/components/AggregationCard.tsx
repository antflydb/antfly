import { Pencil1Icon, TrashIcon } from "@radix-ui/react-icons";
import type React from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

interface AggregationCardProps {
  name: string;
  aggregation: {
    type?: string;
    field?: string;
    size?: number;
    interval?: number;
    calendar_interval?: string;
    ranges?: { name: string; from?: unknown; to?: unknown }[];
    date_ranges?: { name: string; from?: string; to?: string }[];
    [key: string]: unknown;
  };
  onDelete: () => void;
  onEdit?: () => void;
}

const TYPE_COLORS: Record<string, string> = {
  terms: "bg-blue-500/10 text-blue-600 border-blue-500/20",
  range: "bg-orange-500/10 text-orange-600 border-orange-500/20",
  date_range: "bg-orange-500/10 text-orange-600 border-orange-500/20",
  histogram: "bg-purple-500/10 text-purple-600 border-purple-500/20",
  date_histogram: "bg-purple-500/10 text-purple-600 border-purple-500/20",
  sum: "bg-green-500/10 text-green-600 border-green-500/20",
  avg: "bg-green-500/10 text-green-600 border-green-500/20",
  min: "bg-green-500/10 text-green-600 border-green-500/20",
  max: "bg-green-500/10 text-green-600 border-green-500/20",
  count: "bg-green-500/10 text-green-600 border-green-500/20",
  stats: "bg-green-500/10 text-green-600 border-green-500/20",
  cardinality: "bg-green-500/10 text-green-600 border-green-500/20",
};

const AggregationCard: React.FC<AggregationCardProps> = ({
  name,
  aggregation,
  onDelete,
  onEdit,
}) => {
  const type = aggregation.type || "terms";
  const colorClass = TYPE_COLORS[type] || "bg-muted text-muted-foreground";

  const details = (() => {
    const parts: string[] = [];
    if (aggregation.size) parts.push(`size: ${aggregation.size}`);
    if (aggregation.interval) parts.push(`interval: ${aggregation.interval}`);
    if (aggregation.calendar_interval) parts.push(`interval: ${aggregation.calendar_interval}`);
    if (aggregation.ranges?.length) parts.push(`${aggregation.ranges.length} ranges`);
    if (aggregation.date_ranges?.length) parts.push(`${aggregation.date_ranges.length} ranges`);
    return parts.join(" · ");
  })();

  return (
    <div className="flex items-start justify-between p-2.5 bg-muted/30 rounded-lg border group">
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-medium text-sm truncate">{name}</span>
          <Badge variant="outline" className={`text-[10px] border ${colorClass}`}>
            {type}
          </Badge>
        </div>
        <div className="text-xs text-muted-foreground flex items-center gap-1.5">
          <code className="bg-muted px-1 py-0.5 rounded">{aggregation.field || "none"}</code>
          {details && <span>· {details}</span>}
        </div>
      </div>
      <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        {onEdit && (
          <Button variant="ghost" size="sm" onClick={onEdit} className="h-7 w-7 p-0">
            <Pencil1Icon className="h-3 w-3" />
          </Button>
        )}
        <Button variant="ghost" size="sm" onClick={onDelete} className="h-7 w-7 p-0">
          <TrashIcon className="h-3 w-3" />
        </Button>
      </div>
    </div>
  );
};

export default AggregationCard;
