import {
  Alert,
  AlertDescription,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "@antfly/design-system";
import type { IndexConfig } from "@antfly/sdk";
import type React from "react";
import { useEffect, useState } from "react";
import type { TableSchema } from "../api";
import { useApi } from "../hooks/use-api-config";
import { useTable } from "../hooks/use-table";
import { createIndexArguments } from "../lib/create-index";
import { buildCreateTableRequest, createTableErrorMessage } from "../lib/create-table";
import TableSchemaForm from "./schema-builder/TableSchemaForm";

interface CreateTableDialogProps {
  open: boolean;
  onClose: () => void;
  onTableCreated: () => void;
  theme: string;
}

const CreateTableDialog: React.FC<CreateTableDialogProps> = ({
  open,
  onClose,
  onTableCreated,
  theme,
}) => {
  const api = useApi();
  const { refreshTables, setSelectedTable } = useTable();
  const [createError, setCreateError] = useState<string | null>(null);

  useEffect(() => {
    if (open) {
      setCreateError(null);
    }
  }, [open]);

  const handleCreateTable = async (data: {
    name: string;
    schema: Omit<TableSchema, "key">;
    num_shards: number;
    indexes: IndexConfig[];
  }) => {
    setCreateError(null);
    try {
      const requestBody = buildCreateTableRequest(data.num_shards, data.schema);
      await api.tables.create(data.name, requestBody);
      for (const index of data.indexes) {
        const { indexName, request } = createIndexArguments(index);
        await api.indexes.create(data.name, indexName, request);
      }
      setSelectedTable(data.name);
      await refreshTables();
      onTableCreated();
      onClose();
    } catch (error) {
      console.error("Failed to create table:", error);
      setCreateError(createTableErrorMessage(error));
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-[900px]">
        <DialogTitle>Create New Table</DialogTitle>
        <DialogDescription>Define the schema for your new table.</DialogDescription>
        {createError && (
          <Alert variant="destructive">
            <AlertDescription>{createError}</AlertDescription>
          </Alert>
        )}
        <TableSchemaForm onSubmit={handleCreateTable} theme={theme} />
      </DialogContent>
    </Dialog>
  );
};

export default CreateTableDialog;
