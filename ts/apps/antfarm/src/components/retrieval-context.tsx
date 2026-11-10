import {
  Field,
  FieldLabel,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@antfly/design-system";
import { useEffect } from "react";
import { Link, useLocation } from "react-router-dom";
import { AIWorkflowContextBar } from "@/components/ai-workflow";
import { useTable } from "@/hooks/use-table";
import { cn } from "@/lib/utils";

export function RetrievalContextBar({ searchable = false }: { searchable?: boolean }) {
  const {
    tables,
    selectedTable,
    setSelectedTable,
    selectedIndex,
    setSelectedIndex,
    embeddingIndexes,
    chatIndexes,
    isLoadingTables,
    isLoadingIndexes,
  } = useTable();
  const indexes = searchable ? chatIndexes : embeddingIndexes;
  useEffect(() => {
    if (!isLoadingIndexes && !indexes.includes(selectedIndex)) {
      setSelectedIndex(indexes[0] ?? "");
    }
  }, [indexes, isLoadingIndexes, selectedIndex, setSelectedIndex]);
  return (
    <AIWorkflowContextBar>
      <div className="grid min-w-0 flex-1 gap-3 sm:grid-cols-2">
        <Field className="min-w-0 gap-1.5">
          <FieldLabel htmlFor="workflow-table">Table</FieldLabel>
          <Select
            value={selectedTable}
            onValueChange={setSelectedTable}
            disabled={isLoadingTables || tables.length === 0}
          >
            <SelectTrigger id="workflow-table" className="w-full">
              <SelectValue
                placeholder={isLoadingTables ? "Loading tables…" : "No tables available"}
              />
            </SelectTrigger>
            <SelectContent>
              {tables.map((table) => (
                <SelectItem key={table.name} value={table.name}>
                  {table.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field className="min-w-0 gap-1.5">
          <FieldLabel htmlFor="workflow-index">
            {searchable ? "Search index" : "Embedding index"}
          </FieldLabel>
          <Select
            value={indexes.includes(selectedIndex) ? selectedIndex : ""}
            onValueChange={setSelectedIndex}
            disabled={isLoadingIndexes || indexes.length === 0}
          >
            <SelectTrigger id="workflow-index" className="w-full">
              <SelectValue
                placeholder={isLoadingIndexes ? "Loading indexes…" : "No compatible index"}
              />
            </SelectTrigger>
            <SelectContent>
              {indexes.map((index) => (
                <SelectItem key={index} value={index}>
                  {index}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
      </div>
      {!selectedTable ? (
        <p className="text-sm text-muted-foreground">
          Create a table and add documents to start retrieving answers.
        </p>
      ) : null}
    </AIWorkflowContextBar>
  );
}

export function AskModeNavigation() {
  const { pathname, search } = useLocation();
  return (
    <nav aria-label="Ask mode" className="flex gap-1 border-b border-border">
      {[
        ["Conversation", "/data/playground/chat"],
        ["Pipeline", "/data/playground/rag"],
      ].map(([label, path]) => (
        <Link
          key={path}
          to={`${path}${search}`}
          aria-current={pathname === path ? "page" : undefined}
          className={cn(
            "border-b-2 border-transparent px-4 py-2 font-mono text-xs transition-colors hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring",
            pathname === path && "border-primary text-foreground"
          )}
        >
          {label}
        </Link>
      ))}
    </nav>
  );
}
