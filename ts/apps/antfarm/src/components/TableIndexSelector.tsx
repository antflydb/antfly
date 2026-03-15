import type { TableStatus } from "@antfly/sdk";
import { Database } from "lucide-react";
import type React from "react";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface TableIndexSelectorProps {
  tables: TableStatus[];
  selectedTable: string;
  onTableChange: (table: string) => void;
  embeddingIndexes: string[];
  selectedIndex: string;
  onIndexChange: (index: string) => void;
  compact?: boolean;
}

export const TableIndexSelector: React.FC<TableIndexSelectorProps> = ({
  tables,
  selectedTable,
  onTableChange,
  embeddingIndexes,
  selectedIndex,
  onIndexChange,
  compact,
}) => {
  if (compact) {
    return (
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2">
          <Database className="h-4 w-4 text-muted-foreground shrink-0" />
          <Select value={selectedTable} onValueChange={onTableChange}>
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Select table..." />
            </SelectTrigger>
            <SelectContent>
              {tables.map((table) => (
                <SelectItem key={table.name} value={table.name}>
                  {table.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        {embeddingIndexes.length > 0 && (
          <Select
            value={selectedIndex}
            onValueChange={onIndexChange}
            disabled={embeddingIndexes.length === 0}
          >
            <SelectTrigger className="w-[180px]">
              <SelectValue
                placeholder={
                  embeddingIndexes.length === 0 ? "No embedding index" : "Select index..."
                }
              />
            </SelectTrigger>
            <SelectContent>
              {embeddingIndexes.map((idx) => (
                <SelectItem key={idx} value={idx}>
                  {idx}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
        {selectedTable && embeddingIndexes.length === 0 && (
          <span className="text-xs text-amber-600">No embedding index</span>
        )}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 gap-4">
      <div className="space-y-2">
        <Label>Table</Label>
        <Select value={selectedTable} onValueChange={onTableChange}>
          <SelectTrigger>
            <SelectValue placeholder="Select table..." />
          </SelectTrigger>
          <SelectContent>
            {tables.map((table) => (
              <SelectItem key={table.name} value={table.name}>
                {table.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-2">
        <Label>Index</Label>
        <Select
          value={selectedIndex}
          onValueChange={onIndexChange}
          disabled={embeddingIndexes.length === 0}
        >
          <SelectTrigger>
            <SelectValue
              placeholder={embeddingIndexes.length === 0 ? "No embedding index" : "Select index..."}
            />
          </SelectTrigger>
          <SelectContent>
            {embeddingIndexes.map((idx) => (
              <SelectItem key={idx} value={idx}>
                {idx}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  );
};
