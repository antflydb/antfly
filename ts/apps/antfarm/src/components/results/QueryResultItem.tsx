import type { QueryHit } from "@antfly/sdk";
import { ChevronDown, ChevronRight } from "lucide-react";
import type React from "react";
import { useMemo } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import FieldValueDisplay from "./FieldValueDisplay";

interface QueryResultItemProps {
  hit: QueryHit;
  index: number;
  isExpanded: boolean;
  onToggle: () => void;
  visibleFields?: Set<string>;
  previewFields?: string[];
}

const QueryResultItem: React.FC<QueryResultItemProps> = ({
  hit,
  index,
  isExpanded,
  onToggle,
  visibleFields,
  previewFields = ["title", "name", "description", "text", "content"],
}) => {
  const { _id, _source, _score } = hit;

  // Calculate score display
  const scoreDisplay = useMemo(() => {
    if (typeof _score !== "number") return null;

    const percentage = Math.min(100, Math.max(0, _score * 100));

    return {
      percentage,
      value: _score.toFixed(4),
    };
  }, [_score]);

  // Get preview field values
  const previewData = useMemo(() => {
    if (!_source) return [];

    const fields = previewFields
      .filter((field) => _source[field] !== undefined && _source[field] !== null)
      .slice(0, 3);

    return fields.map((field) => ({
      name: field,
      value: _source[field],
    }));
  }, [_source, previewFields]);

  // Get all fields for expanded view
  const allFields = useMemo(() => {
    if (!_source) return [];

    return Object.entries(_source)
      .filter(([key]) => !visibleFields || visibleFields.has(key))
      .sort(([a], [b]) => {
        // Sort special fields first
        const aSpecial = ["_id", "title", "name", "description"].includes(a);
        const bSpecial = ["_id", "title", "name", "description"].includes(b);
        if (aSpecial && !bSpecial) return -1;
        if (!aSpecial && bSpecial) return 1;
        return a.localeCompare(b);
      });
  }, [_source, visibleFields]);

  // Get preview text
  const previewText = useMemo(() => {
    if (previewData.length === 0) return "No preview available";

    const firstField = previewData[0];
    const value = firstField.value;

    if (typeof value === "string") {
      return value.length > 100 ? `${value.substring(0, 100)}...` : value;
    }

    if (Array.isArray(value)) {
      return `[${value.length} items]`;
    }

    if (typeof value === "object" && value !== null) {
      return `{${Object.keys(value).length} fields}`;
    }

    return String(value);
  }, [previewData]);

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={onToggle}
      className="border rounded-lg bg-card hover:bg-accent/50 transition-colors"
    >
      <CollapsibleTrigger asChild>
        <Button variant="ghost" className="w-full h-auto p-4 hover:bg-transparent justify-start">
          <div className="flex items-start gap-3 w-full text-left">
            {/* Expand/Collapse Icon */}
            <div className="shrink-0 mt-0.5">
              {isExpanded ? (
                <ChevronDown className="h-4 w-4 text-muted-foreground" />
              ) : (
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
              )}
            </div>

            {/* Content */}
            <div className="flex-1 min-w-0 space-y-1">
              {/* Header: ID and Score */}
              <div className="flex items-center gap-2 flex-wrap">
                <code className="text-sm font-mono bg-muted px-2 py-0.5 rounded">
                  {_id || `Result #${index + 1}`}
                </code>

                {scoreDisplay && (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Badge variant="outline" className="gap-1 cursor-help">
                        <span className="text-muted-foreground">Score:</span>
                        {scoreDisplay.value}
                      </Badge>
                    </TooltipTrigger>
                    <TooltipContent className="max-w-xs">
                      <p>
                        Search ranking score. For vector search: L2 squared distance (lower = more
                        similar). For hybrid search: RRF fusion score.
                      </p>
                    </TooltipContent>
                  </Tooltip>
                )}
              </div>

              {/* Preview Text */}
              {!isExpanded && (
                <p className="text-sm text-muted-foreground line-clamp-2">{previewText}</p>
              )}
            </div>
          </div>
        </Button>
      </CollapsibleTrigger>

      <CollapsibleContent className="px-4 pb-4">
        <div className="pl-7 space-y-3 mt-2">
          {/* Score Details */}
          {scoreDisplay && (
            <div className="flex items-center gap-2">
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="text-xs font-medium text-muted-foreground min-w-24 cursor-help underline decoration-dotted">
                    Score:
                  </span>
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p>
                    For vector search: L2 squared distance (lower = more similar). For hybrid
                    search: RRF fusion score.
                  </p>
                </TooltipContent>
              </Tooltip>
              <span className="text-sm font-mono">{scoreDisplay.value}</span>
            </div>
          )}

          {/* Divider */}
          <div className="border-t" />

          {/* All Fields */}
          <div className="space-y-3">
            {allFields.length === 0 ? (
              <p className="text-sm text-muted-foreground italic">No fields to display</p>
            ) : (
              allFields.map(([fieldName, fieldValue]) => (
                <div key={fieldName} className="grid grid-cols-12 gap-3 items-start">
                  <div className="col-span-12 sm:col-span-3">
                    <span className="text-xs font-medium text-muted-foreground break-words">
                      {fieldName}
                    </span>
                  </div>
                  <div className="col-span-12 sm:col-span-9">
                    <FieldValueDisplay value={fieldValue} fieldName={fieldName} />
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
};

export default QueryResultItem;
