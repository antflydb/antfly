import type { IndexConfig } from "@antfly/sdk";
import type React from "react";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import type { TableSchema } from "../api";
import { api } from "../api";
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
  const handleCreateTable = async (data: {
    name: string;
    schema: Omit<TableSchema, "key">;
    num_shards: number;
    indexes: IndexConfig[];
  }) => {
    try {
      const requestBody = {
        num_shards: data.num_shards,
        schema: {
          version: 0, // Default version to 0 if not specified
          ...data.schema,
        },
      };
      await api.tables.create(data.name, requestBody);
      for (const index of data.indexes) {
        await api.indexes.create(data.name, index);
      }
      onTableCreated();
      onClose();
    } catch (error) {
      console.error("Failed to create table:", error);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-[900px]">
        <DialogTitle>Create New Table</DialogTitle>
        <DialogDescription>Define the schema for your new table.</DialogDescription>
        <TableSchemaForm onSubmit={handleCreateTable} theme={theme} />
      </DialogContent>
    </Dialog>
  );
};

export default CreateTableDialog;
