import {
  Alert,
  AlertDescription,
  Badge,
  Button,
  type ColumnDef,
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DataTable,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
} from "@antfly/design-system";
import {
  type Table as AntflyTable,
  type QueryRequest,
  queryResultTotalHits,
  type TableStatus,
} from "@antfly/sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import { Trash2 } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { NoTablesState } from "@/components/branded-empty-state";
import { normalizeTablesResponse } from "@/lib/table-utils";
import { useApi } from "../hooks/use-api-config";

const formatBytes = (bytes: number, decimals = 2) => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / k ** i).toFixed(dm))} ${sizes[i]}`;
};

function indexesForTable(table: TableStatus) {
  return Object.values(table.indexes ?? {}) as Array<{ config?: { type?: string } }>;
}

function indexCoverageLabel(table: TableStatus) {
  const indexes = indexesForTable(table);
  const vectorCount = indexes.filter((index) => index.config?.type === "embeddings").length;
  const fullTextCount = indexes.filter((index) => index.config?.type === "full_text").length;
  const graphCount = indexes.filter((index) => index.config?.type === "graph").length;

  if (indexes.length === 0) return "No indexes";

  const parts = [
    vectorCount > 0 ? `${vectorCount} vector` : null,
    fullTextCount > 0 ? `${fullTextCount} full-text` : null,
    graphCount > 0 ? `${graphCount} graph` : null,
  ].filter(Boolean);
  return parts.length > 0 ? parts.join(", ") : `${indexes.length} custom`;
}

const TablesListPage: React.FC = () => {
  const navigate = useNavigate();
  const api = useApi();
  const [tables, setTables] = useState<TableStatus[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [openDropDialog, setOpenDropDialog] = useState(false);
  const [selectedTable, setSelectedTable] = useState<AntflyTable | null>(null);
  const [isDropping, setIsDropping] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [documentCounts, setDocumentCounts] = useState<Record<string, number | null>>({});

  const fetchTables = useCallback(async () => {
    setIsLoading(true);
    try {
      const response = await api.tables.list();
      const nextTables = normalizeTablesResponse(response);
      setTables(nextTables);
      const counts = await Promise.allSettled(
        nextTables.map(async (table) => {
          if (table.storage_status?.empty) return [table.name, 0] as const;
          const query: QueryRequest = {
            filter_query: { match_all: {} },
            count: true,
            limit: 0,
          };
          const result = await api.tables.query(table.name, query);
          return [table.name, queryResultTotalHits(result?.responses?.[0]) ?? null] as const;
        })
      );
      setDocumentCounts(
        Object.fromEntries(
          counts.map((result, index) =>
            result.status === "fulfilled" ? result.value : [nextTables[index].name, null]
          )
        )
      );
    } catch (e) {
      setError("Failed to fetch tables. Make sure the Antfly server is running.");
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  }, [api]);

  useEffect(() => {
    fetchTables();
  }, [fetchTables]);

  const handleOpenDropDialog = useCallback((table: AntflyTable) => {
    setSelectedTable(table);
    setOpenDropDialog(true);
  }, []);

  const handleCloseDropDialog = () => {
    setSelectedTable(null);
    setOpenDropDialog(false);
  };

  const handleDropTable = async () => {
    if (!selectedTable) return;
    setIsDropping(true);
    try {
      await api.tables.drop(selectedTable.name);
      setTimeout(() => {
        fetchTables();
        handleCloseDropDialog();
        setIsDropping(false);
      }, 1000);
    } catch (e) {
      setError(`Failed to drop table ${selectedTable.name}.`);
      console.error(e);
      setIsDropping(false);
    }
  };

  const tableColumns = useMemo<ColumnDef<TableStatus>[]>(
    () => [
      {
        accessorKey: "name",
        header: "Name",
        cell: ({ row }) => (
          <Link
            to={`/tables/${encodeURIComponent(row.original.name)}?section=overview`}
            className="font-medium hover:underline"
          >
            {row.original.name}
          </Link>
        ),
      },
      {
        accessorKey: "description",
        header: "Description",
        cell: ({ row }) =>
          row.original.description ? (
            <span className="text-muted-foreground">{row.original.description}</span>
          ) : (
            <span className="text-muted-foreground/50 italic">No description</span>
          ),
      },
      {
        id: "documents",
        header: "Documents",
        cell: ({ row }) => {
          const count = documentCounts[row.original.name];
          if (count === undefined) {
            return row.original.storage_status?.empty ? "0" : "Checking...";
          }
          return count === null ? "Unknown" : Intl.NumberFormat().format(count);
        },
      },
      {
        id: "indexes",
        header: "Index Coverage",
        cell: ({ row }) => (
          <div className="flex flex-wrap items-center gap-1.5">
            <span>{indexCoverageLabel(row.original)}</span>
            {indexesForTable(row.original).length === 0 && (
              <Badge className="af-status-badge-warning">Setup needed</Badge>
            )}
          </div>
        ),
      },
      {
        id: "status",
        header: "Status",
        cell: ({ row }) => {
          const table = row.original;
          return (
            <div className="flex items-center gap-2">
              {table.migration && (
                <Badge className="af-status-badge-warning">
                  Rebuilding v{table.migration.read_schema.version} → v
                  {table.schema?.version ?? "?"}
                </Badge>
              )}
              {table.storage_status ? (
                <>
                  {table.storage_status.empty && <span>Empty</span>}
                  {table.storage_status.disk_usage !== undefined && (
                    <span>{formatBytes(table.storage_status.disk_usage)}</span>
                  )}
                </>
              ) : (
                "N/A"
              )}
            </div>
          );
        },
      },
      {
        id: "actions",
        header: "",
        cell: ({ row }) => (
          <Button
            variant="ghost"
            size="icon"
            onClick={() => handleOpenDropDialog(row.original)}
            className="text-muted-foreground hover:text-destructive"
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        ),
      },
    ],
    [documentCounts, handleOpenDropDialog]
  );

  return (
    <DashboardPage>
      <div className="relative isolate">
        <DashboardPageHeader>
          <div>
            <DashboardPageTitle className="font-aeonik">Tables</DashboardPageTitle>
            <DashboardPageDescription>
              Browse tables, inspect index coverage, and manage table lifecycle.
            </DashboardPageDescription>
          </div>
          <DashboardPageActions>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="icon" onClick={fetchTables} disabled={isLoading}>
                <ReloadIcon />
              </Button>
            </div>
          </DashboardPageActions>
        </DashboardPageHeader>
      </div>
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {!isLoading && tables.length === 0 ? (
        <NoTablesState onCreate={() => navigate("/create")} />
      ) : (
        <DataTable
          columns={tableColumns}
          data={tables}
          filterColumn="name"
          filterPlaceholder="Filter tables by name…"
          emptyMessage="No tables found."
          pageSize={20}
        />
      )}
      <Dialog open={openDropDialog} onOpenChange={setOpenDropDialog}>
        <DialogContent className="max-w-[450px]">
          <DialogTitle>Drop Table</DialogTitle>
          <DialogDescription>
            Are you sure you want to drop the table "{selectedTable?.name}"? This action cannot be
            undone.
          </DialogDescription>
          <div className="flex gap-3 mt-4 justify-end">
            <DialogTrigger>
              <Button variant="destructive" color="gray" disabled={isDropping}>
                Cancel
              </Button>
            </DialogTrigger>
            <DialogTrigger>
              <Button color="red" onClick={handleDropTable} disabled={isDropping}>
                {isDropping ? "Dropping..." : "Drop"}
              </Button>
            </DialogTrigger>
          </div>
        </DialogContent>
      </Dialog>
    </DashboardPage>
  );
};

export default TablesListPage;
