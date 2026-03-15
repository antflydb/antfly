import type { IndexConfig } from "@antfly/sdk";
import type React from "react";
import { useNavigate } from "react-router-dom";
import type { TableSchema } from "../api";
import { api } from "../api";
import TableSchemaForm from "../components/schema-builder/TableSchemaForm";

const CreateTablePage: React.FC = () => {
  const theme = localStorage.getItem("theme") || "light";
  const navigate = useNavigate();

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
          version: 0,
          ...data.schema,
        },
      };
      await api.tables.create(data.name, requestBody);
      for (const index of data.indexes) {
        await api.indexes.create(data.name, index);
      }
      navigate("/");
    } catch (error) {
      console.error("Failed to create table:", error);
    }
  };

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">Create New Table</h2>
      <p className="text-muted-foreground mb-6">Define the schema for your new table.</p>
      <TableSchemaForm onSubmit={handleCreateTable} theme={theme} />
    </div>
  );
};

export default CreateTablePage;
