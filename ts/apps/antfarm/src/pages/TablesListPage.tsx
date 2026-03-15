import type { Table as AntflyTable, TableStatus } from "@antfly/sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import { Trash2, X } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { api } from "../api";

const formatBytes = (bytes: number, decimals = 2) => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / k ** i).toFixed(dm))} ${sizes[i]}`;
};

const TablesListPage: React.FC = () => {
  const [tables, setTables] = useState<TableStatus[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [openDropDialog, setOpenDropDialog] = useState(false);
  const [selectedTable, setSelectedTable] = useState<AntflyTable | null>(null);
  const [isDropping, setIsDropping] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [filterMode, setFilterMode] = useState<"prefix" | "regex">("prefix");
  const [filterValue, setFilterValue] = useState("");

  const fetchTables = useCallback(async () => {
    setIsLoading(true);
    try {
      const params: { prefix?: string; pattern?: string } = {};
      if (filterValue) {
        if (filterMode === "prefix") {
          params.prefix = filterValue;
        } else {
          params.pattern = filterValue;
        }
      }
      const response = await api.tables.list(params);
      setTables(response as TableStatus[]);
    } catch (e) {
      setError("Failed to fetch tables. Make sure the Antfly server is running.");
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  }, [filterValue, filterMode]);

  useEffect(() => {
    // Only fetch on initial load, not when filter changes
    const initialFetch = async () => {
      setIsLoading(true);
      try {
        const response = await api.tables.list();
        setTables(response as TableStatus[]);
      } catch (e) {
        setError("Failed to fetch tables. Make sure the Antfly server is running.");
        console.error(e);
      } finally {
        setIsLoading(false);
      }
    };
    initialFetch();
  }, []);

  const handleOpenDropDialog = (table: AntflyTable) => {
    setSelectedTable(table);
    setOpenDropDialog(true);
  };

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

  const handleClearFilter = () => {
    setFilterValue("");
    setFilterMode("prefix");
  };

  const handleApplyFilter = () => {
    fetchTables();
  };

  return (
    <div>
      <div className="flex items-center gap-2 mb-4">
        <h2 className="text-2xl font-bold">Tables</h2>
        <Button size="icon" onClick={fetchTables} disabled={isLoading}>
          <ReloadIcon />
        </Button>
      </div>
      {error && <p className="text-red-500">{error}</p>}

      <div className="mb-4 p-4 border rounded-lg bg-muted/50">
        <div className="space-y-4">
          <div>
            <Label className="text-sm font-medium mb-2 block">Filter Mode</Label>
            <RadioGroup
              value={filterMode}
              onValueChange={(value) => setFilterMode(value as "prefix" | "regex")}
              className="flex gap-4"
            >
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="prefix" id="prefix" />
                <Label htmlFor="prefix" className="cursor-pointer">
                  Prefix
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="regex" id="regex" />
                <Label htmlFor="regex" className="cursor-pointer">
                  Regex
                </Label>
              </div>
            </RadioGroup>
          </div>
          <div className="flex gap-2">
            <div className="flex-1">
              <Label htmlFor="filter" className="text-sm font-medium mb-2 block">
                {filterMode === "prefix" ? "Filter by Prefix" : "Filter by Regex Pattern"}
              </Label>
              <Input
                id="filter"
                placeholder={
                  filterMode === "prefix" ? 'e.g., "prod_"' : 'e.g., "^prod_.*_v[0-9]+$"'
                }
                value={filterValue}
                onChange={(e) => setFilterValue(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    handleApplyFilter();
                  }
                }}
              />
            </div>
            <div className="flex items-end gap-2">
              <Button onClick={handleApplyFilter} disabled={isLoading}>
                Apply
              </Button>
              {filterValue && (
                <Button
                  variant="outline"
                  size="icon"
                  onClick={handleClearFilter}
                  disabled={isLoading}
                  title="Clear filter"
                >
                  <X className="h-4 w-4" />
                </Button>
              )}
            </div>
          </div>
        </div>
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
            <TableHead>Description</TableHead>
            <TableHead>Shards</TableHead>
            <TableHead>Indexes</TableHead>
            <TableHead>Status</TableHead>
            <TableHead>Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tables.map((table) => (
            <TableRow key={table.name}>
              <TableCell>
                <Link
                  to={`/tables/${table.name}`}
                  className="hover:underline text-blue-600 dark:text-blue-400"
                >
                  {table.name}
                </Link>
              </TableCell>
              <TableCell>
                {table.description ? (
                  <span className="text-muted-foreground">{table.description}</span>
                ) : (
                  <span className="text-muted-foreground italic">No description</span>
                )}
              </TableCell>
              <TableCell>{Object.keys(table.shards).length}</TableCell>
              <TableCell>{Object.keys(table.indexes).length}</TableCell>
              <TableCell>
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
              </TableCell>
              <TableCell>
                <Button
                  variant="destructive"
                  size="icon"
                  onClick={() => handleOpenDropDialog(table)}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
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
    </div>
  );
};

export default TablesListPage;
