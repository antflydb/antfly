import {
  Alert,
  AlertDescription,
  DashboardPage,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
} from "@antfly/design-system";
import type { IndexConfig } from "@antfly/sdk";
import type React from "react";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import type { TableSchema } from "../api";
import TableSchemaForm from "../components/schema-builder/TableSchemaForm";
import { useApi } from "../hooks/use-api-config";
import { useTable } from "../hooks/use-table";
import { createIndexArguments } from "../lib/create-index";
import { buildCreateTableRequest, createTableErrorMessage } from "../lib/create-table";

const CreateTablePage: React.FC = () => {
  const theme = localStorage.getItem("theme") || "light";
  const navigate = useNavigate();
  const api = useApi();
  const { refreshTables, setSelectedTable } = useTable();
  const [createError, setCreateError] = useState<string | null>(null);

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
      navigate("/");
    } catch (error) {
      console.error("Failed to create table:", error);
      setCreateError(createTableErrorMessage(error));
    }
  };

  return (
    <DashboardPage>
      <div className="relative isolate">
        <DashboardPageHeader>
          <div>
            <DashboardPageTitle className="font-aeonik">Create New Table</DashboardPageTitle>
            <DashboardPageDescription>
              Define the schema for your new table.
            </DashboardPageDescription>
          </div>
        </DashboardPageHeader>
      </div>
      {createError && (
        <Alert variant="destructive" className="mb-4">
          <AlertDescription>{createError}</AlertDescription>
        </Alert>
      )}
      <TableSchemaForm onSubmit={handleCreateTable} theme={theme} />
    </DashboardPage>
  );
};

export default CreateTablePage;
