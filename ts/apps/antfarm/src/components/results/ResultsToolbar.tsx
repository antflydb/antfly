import { ChevronDown, ChevronUp, Code2, Download, LayoutGrid, Table2 } from "lucide-react";
import type React from "react";
import { useMemo } from "react";
import { Badge } from "@/components/ui/badge";

/**
 * Formats query time from nanoseconds to a human-readable string.
 * Go's time.Duration serializes to JSON as nanoseconds.
 */
export function formatQueryTime(nanoseconds: number): string {
  const ms = nanoseconds / 1_000_000;
  if (ms < 1) return "< 1ms";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

export type ViewMode = "cards" | "table" | "json";
export type SortMode = "relevance" | "id" | "none";

interface ResultsToolbarProps {
  viewMode: ViewMode;
  onViewModeChange: (mode: ViewMode) => void;
  sortMode: SortMode;
  onSortModeChange: (mode: SortMode) => void;
  availableFields: string[];
  visibleFields: Set<string>;
  onVisibleFieldsChange: (fields: Set<string>) => void;
  expandAll: boolean;
  onExpandAllChange: (expand: boolean) => void;
  onExport?: (format: "json" | "csv") => void;
  totalHits?: number;
  queryTime?: number;
}

const ResultsToolbar: React.FC<ResultsToolbarProps> = ({
  viewMode,
  onViewModeChange,
  sortMode,
  onSortModeChange,
  availableFields,
  visibleFields,
  onVisibleFieldsChange,
  expandAll,
  onExpandAllChange,
  onExport,
  totalHits,
  queryTime,
}) => {
  const allFieldsSelected = useMemo(
    () => availableFields.every((field) => visibleFields.has(field)),
    [availableFields, visibleFields]
  );

  const handleToggleAllFields = () => {
    if (allFieldsSelected) {
      onVisibleFieldsChange(new Set());
    } else {
      onVisibleFieldsChange(new Set(availableFields));
    }
  };

  const handleToggleField = (field: string) => {
    const newFields = new Set(visibleFields);
    if (newFields.has(field)) {
      newFields.delete(field);
    } else {
      newFields.add(field);
    }
    onVisibleFieldsChange(newFields);
  };

  return (
    <div className="flex flex-col gap-3 p-4 bg-muted/30 rounded-lg border">
      {/* Top Row: Stats and View Mode */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        {/* Stats */}
        <div className="flex items-center gap-2 flex-wrap">
          {totalHits !== undefined && (
            <Badge variant="secondary" className="font-mono">
              {totalHits.toLocaleString()} {totalHits === 1 ? "hit" : "hits"}
            </Badge>
          )}
          {queryTime !== undefined && (
            <Badge variant="outline" className="font-mono text-xs">
              {formatQueryTime(queryTime)}
            </Badge>
          )}
        </div>

        {/* View Mode Toggle */}
        <div className="flex items-center gap-1 bg-background rounded-md border p-1">
          <Button
            variant={viewMode === "cards" ? "secondary" : "ghost"}
            size="sm"
            onClick={() => onViewModeChange("cards")}
            className="h-7 px-2"
            title="Cards View"
          >
            <LayoutGrid className="h-4 w-4" />
          </Button>
          <Button
            variant={viewMode === "table" ? "secondary" : "ghost"}
            size="sm"
            onClick={() => onViewModeChange("table")}
            className="h-7 px-2"
            title="Table View"
          >
            <Table2 className="h-4 w-4" />
          </Button>
          <Button
            variant={viewMode === "json" ? "secondary" : "ghost"}
            size="sm"
            onClick={() => onViewModeChange("json")}
            className="h-7 px-2"
            title="JSON View"
          >
            <Code2 className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Bottom Row: Controls (only visible in cards/table mode) */}
      {viewMode !== "json" && (
        <div className="flex items-center justify-between flex-wrap gap-2">
          <div className="flex items-center gap-2 flex-wrap">
            {/* Field Filter */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="h-8">
                  Fields
                  <Badge variant="secondary" className="ml-2 h-5 px-1.5 text-xs">
                    {visibleFields.size}/{availableFields.length}
                  </Badge>
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" className="w-64 max-h-96 overflow-y-auto">
                <DropdownMenuLabel>Visible Fields</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuCheckboxItem
                  checked={allFieldsSelected}
                  onCheckedChange={handleToggleAllFields}
                  className="font-medium"
                >
                  All Fields
                </DropdownMenuCheckboxItem>
                <DropdownMenuSeparator />
                {availableFields.map((field) => (
                  <DropdownMenuCheckboxItem
                    key={field}
                    checked={visibleFields.has(field)}
                    onCheckedChange={() => handleToggleField(field)}
                  >
                    <code className="text-xs">{field}</code>
                  </DropdownMenuCheckboxItem>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>

            {/* Sort */}
            <Select value={sortMode} onValueChange={(value) => onSortModeChange(value as SortMode)}>
              <SelectTrigger className="w-32 h-8">
                <SelectValue placeholder="Sort by" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="relevance">Relevance</SelectItem>
                <SelectItem value="id">ID</SelectItem>
                <SelectItem value="none">None</SelectItem>
              </SelectContent>
            </Select>

            {/* Expand/Collapse All (cards mode only) */}
            {viewMode === "cards" && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => onExpandAllChange(!expandAll)}
                className="h-8"
              >
                {expandAll ? (
                  <>
                    <ChevronUp className="h-3 w-3 mr-1" />
                    Collapse All
                  </>
                ) : (
                  <>
                    <ChevronDown className="h-3 w-3 mr-1" />
                    Expand All
                  </>
                )}
              </Button>
            )}
          </div>

          {/* Export */}
          {onExport && (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="h-8">
                  <Download className="h-3 w-3 mr-1" />
                  Export
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => onExport("json")}>Export as JSON</DropdownMenuItem>
                <DropdownMenuItem onClick={() => onExport("csv")}>Export as CSV</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </div>
      )}
    </div>
  );
};

export default ResultsToolbar;
