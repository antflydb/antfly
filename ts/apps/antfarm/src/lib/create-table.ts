import type { TableSchema } from "../api";

export function buildCreateTableRequest(numShards: number, tableSchema: TableSchema) {
  const schema = { ...tableSchema };
  delete schema.version;

  const hasDocumentSchemas = Object.keys(schema.document_schemas ?? {}).length > 0;
  return {
    num_shards: numShards,
    ...(hasDocumentSchemas ? { schema } : {}),
  };
}

export function createTableErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Failed to create table.";
}
