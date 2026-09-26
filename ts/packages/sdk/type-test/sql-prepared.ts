import type {
  AntflyClient,
  SQLPreparedExecutionRequest,
  SQLPreparedResponse,
  SQLPrepareRequest,
  SQLResponse,
} from "../src/index.js";

declare const client: AntflyClient;
const prepare: SQLPrepareRequest = { statement: "SELECT $1", database: "analytics" };
const execute: SQLPreparedExecutionRequest = {
  parameters: ["9223372036854775807"],
  session_id: "session",
};
const resource: Promise<SQLPreparedResponse> = client.prepareSQL(prepare);
const result: Promise<SQLResponse> = client.executePreparedSQL("resource", execute);
const closed: Promise<void> = client.closePreparedSQL("resource");
void resource;
void result;
void closed;

declare const prepared: SQLPreparedResponse;
const exactOwner: string = prepared.owner_node_id;
void exactOwner;

// Stored SQL and namespace are immutable authority of the resource.
// @ts-expect-error execution cannot replace the stored statement.
client.executePreparedSQL("resource", { statement: "DELETE FROM other_table" });
// @ts-expect-error execution cannot replace the stored namespace.
client.executePreparedSQL("resource", { namespace: "other_namespace" });
