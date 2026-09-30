import type {
  AntflyClient,
  SQLConnectionOpenRequest,
  SQLConnectionResponse,
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
const closedForConnection: Promise<void> = client.closePreparedSQL("resource", {
  connectionId: "a".repeat(32),
});
const openConnection: Promise<SQLConnectionResponse> = client.openSQLConnection({
  database: "analytics",
} satisfies SQLConnectionOpenRequest);
const closeConnection: Promise<void> = client.closeSQLConnection("a".repeat(32));
void resource;
void result;
void closed;
void closedForConnection;
void openConnection;
void closeConnection;

declare const prepared: SQLPreparedResponse;
const exactOwner: string = prepared.owner_node_id;
void exactOwner;

// Stored SQL and namespace are immutable authority of the resource.
// @ts-expect-error execution cannot replace the stored statement.
client.executePreparedSQL("resource", { statement: "DELETE FROM other_table" });
// @ts-expect-error execution cannot replace the stored namespace.
client.executePreparedSQL("resource", { namespace: "other_namespace" });
