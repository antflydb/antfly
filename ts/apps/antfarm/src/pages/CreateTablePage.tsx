import {
  DashboardPage,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  GraphPaperBg,
} from "@antfly/design-system";
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
    <DashboardPage>
      <div className="relative isolate">
        <GraphPaperBg className="absolute inset-0 -z-10 rounded-xl" />
        <DashboardPageHeader>
          <div>
            <DashboardPageTitle className="font-aeonik">Create New Table</DashboardPageTitle>
            <DashboardPageDescription>
              Define the schema for your new table.
            </DashboardPageDescription>
          </div>
        </DashboardPageHeader>
      </div>
      <TableSchemaForm onSubmit={handleCreateTable} theme={theme} />
    </DashboardPage>
  );
};

export default CreateTablePage;
