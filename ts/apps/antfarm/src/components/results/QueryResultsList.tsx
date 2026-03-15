import type { QueryResult } from "@antfly/sdk";
import React, { useCallback, useMemo, useState } from "react";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import JsonViewer from "../JsonViewer";
import FieldValueDisplay from "./FieldValueDisplay";
import QueryResultItem from "./QueryResultItem";
import ResultsToolbar, { type SortMode, type ViewMode } from "./ResultsToolbar";

interface QueryResultsListProps {
  result: QueryResult;
  className?: string;
}

const QueryResultsList: React.FC<QueryResultsListProps> = ({ result, className }) => {
  const [viewMode, setViewMode] = useState<ViewMode>("cards");
  const [sortMode, setSortMode] = useState<SortMode>("relevance");
  const [expandedItems, setExpandedItems] = useState<Set<number>>(new Set());
  const [expandAll, setExpandAll] = useState(false);

  // Extract all unique field names from hits
  const availableFields = useMemo(() => {
    if (!result.hits?.hits || result.hits.hits.length === 0) return [];

    const fieldsSet = new Set<string>();
    for (const hit of result.hits.hits) {
      if (hit._source) {
        for (const key of Object.keys(hit._source)) {
          fieldsSet.add(key);
        }
      }
    }

    return Array.from(fieldsSet).sort();
  }, [result.hits]);

  const [visibleFields, setVisibleFields] = useState<Set<string>>(() => new Set(availableFields));

  // Update visible fields when available fields change
  React.useEffect(() => {
    setVisibleFields(new Set(availableFields));
  }, [availableFields]);

  // Sort hits based on selected mode
  const sortedHits = useMemo(() => {
    if (!result.hits?.hits) return [];

    const hits = [...result.hits.hits];

    switch (sortMode) {
      case "relevance":
        return hits.sort((a, b) => {
          const scoreA = a._score ?? 0;
          const scoreB = b._score ?? 0;
          return scoreB - scoreA;
        });
      case "id":
        return hits.sort((a, b) => {
          const idA = a._id ?? "";
          const idB = b._id ?? "";
          return idA.localeCompare(idB);
        });
      default:
        return hits;
    }
  }, [result.hits, sortMode]);

  // Handle expand/collapse
  const handleToggleItem = useCallback((index: number) => {
    setExpandedItems((prev) => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  }, []);

  // Handle expand all
  const handleExpandAllChange = useCallback(
    (expand: boolean) => {
      setExpandAll(expand);
      if (expand) {
        setExpandedItems(new Set(sortedHits.map((_, idx) => idx)));
      } else {
        setExpandedItems(new Set());
      }
    },
    [sortedHits]
  );

  // Export functionality
  const handleExport = useCallback(
    (format: "json" | "csv") => {
      if (format === "json") {
        const dataStr = JSON.stringify(result, null, 2);
        const dataBlob = new Blob([dataStr], { type: "application/json" });
        const url = URL.createObjectURL(dataBlob);
        const link = document.createElement("a");
        link.href = url;
        link.download = `query-results-${Date.now()}.json`;
        link.click();
        URL.revokeObjectURL(url);
      } else if (format === "csv") {
        if (!result.hits?.hits || result.hits.hits.length === 0) return;

        // Get all fields from all hits
        const allFields = Array.from(
          new Set(result.hits.hits.flatMap((hit) => (hit._source ? Object.keys(hit._source) : [])))
        ).sort();

        // Add metadata fields
        const headers = ["_id", "_score", ...allFields];

        // Build CSV
        const rows = result.hits.hits.map((hit) => {
          const row: string[] = [];
          row.push(hit._id ? `"${hit._id}"` : "");
          row.push(hit._score !== undefined ? String(hit._score) : "");

          allFields.forEach((field) => {
            const value = hit._source?.[field];
            if (value === undefined || value === null) {
              row.push("");
            } else if (typeof value === "object") {
              row.push(`"${JSON.stringify(value).replace(/"/g, '""')}"`);
            } else {
              row.push(`"${String(value).replace(/"/g, '""')}"`);
            }
          });

          return row.join(",");
        });

        const csv = [headers.join(","), ...rows].join("\n");
        const csvBlob = new Blob([csv], { type: "text/csv" });
        const url = URL.createObjectURL(csvBlob);
        const link = document.createElement("a");
        link.href = url;
        link.download = `query-results-${Date.now()}.csv`;
        link.click();
        URL.revokeObjectURL(url);
      }
    },
    [result]
  );

  // Table view columns (visible fields only)
  const tableColumns = useMemo(() => {
    return ["_id", "_score", ...Array.from(visibleFields)].filter(
      (field, index, arr) => arr.indexOf(field) === index
    );
  }, [visibleFields]);

  if (!result.hits?.hits || result.hits.hits.length === 0) {
    return (
      <div className={className}>
        <div className="text-center py-12 text-muted-foreground">
          <p>No results found</p>
        </div>
      </div>
    );
  }

  return (
    <div className={className}>
      {/* Toolbar */}
      <ResultsToolbar
        viewMode={viewMode}
        onViewModeChange={setViewMode}
        sortMode={sortMode}
        onSortModeChange={setSortMode}
        availableFields={availableFields}
        visibleFields={visibleFields}
        onVisibleFieldsChange={setVisibleFields}
        expandAll={expandAll}
        onExpandAllChange={handleExpandAllChange}
        onExport={handleExport}
        totalHits={result.hits?.total}
        queryTime={result.took}
      />

      {/* Content */}
      <div className="mt-4">
        {/* Cards View */}
        {viewMode === "cards" && (
          <div className="space-y-2">
            {sortedHits.map((hit, index) => (
              <QueryResultItem
                key={hit._id || index}
                hit={hit}
                index={index}
                isExpanded={expandedItems.has(index)}
                onToggle={() => handleToggleItem(index)}
                visibleFields={visibleFields}
              />
            ))}
          </div>
        )}

        {/* Table View */}
        {viewMode === "table" && (
          <div className="border rounded-lg overflow-auto max-h-[600px]">
            <Table>
              <TableHeader>
                <TableRow>
                  {tableColumns.map((column) => (
                    <TableHead key={column} className="font-mono text-xs">
                      {column}
                    </TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedHits.map((hit, index) => (
                  <TableRow key={hit._id || index}>
                    <TableCell className="font-mono text-xs">
                      {hit._id || `Result #${index + 1}`}
                    </TableCell>
                    <TableCell>
                      {hit._score !== undefined ? (
                        <Badge variant="outline" className="font-mono text-xs">
                          {hit._score.toFixed(4)}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground text-xs">-</span>
                      )}
                    </TableCell>
                    {Array.from(visibleFields).map((field) => (
                      <TableCell key={field} className="max-w-md">
                        <div className="truncate">
                          <FieldValueDisplay
                            value={hit._source?.[field]}
                            fieldName={field}
                            compact={true}
                          />
                        </div>
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}

        {/* JSON View */}
        {viewMode === "json" && <JsonViewer json={result} />}
      </div>
    </div>
  );
};

export default QueryResultsList;
