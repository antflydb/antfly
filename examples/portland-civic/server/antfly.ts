import {
  AntflyClient,
  type QueryRequest,
  type TableQueryRequest,
} from "@antfly/sdk";
export const baseUrl = (
  process.env.ANTFLY_URL || "http://127.0.0.1:8088"
).replace(/\/$/, "");
export const auth = process.env.ANTFLY_USERNAME
  ? {
      username: process.env.ANTFLY_USERNAME,
      password: process.env.ANTFLY_PASSWORD || "",
    }
  : undefined;
export const client = new AntflyClient({ baseUrl, auth });
export async function queryAntfly(
  request: QueryRequest,
  options?: { signal?: AbortSignal },
) {
  const { table, ...body } = request;
  if (!table) throw new Error("Antfly query requires a table");
  const response = await client.tables.query(
    table,
    body as TableQueryRequest,
    options,
  );
  return response?.responses?.[0];
}
export const PERMITS = "portland_permits";
export const METRICS = "portland_civic_metrics";
export const generator = process.env.ANTFLY_GENERATOR
  ? JSON.parse(process.env.ANTFLY_GENERATOR)
  : undefined;
export const embedder = process.env.ANTFLY_EMBEDDER
  ? JSON.parse(process.env.ANTFLY_EMBEDDER)
  : undefined;
export const headers = auth
  ? {
      Authorization: `Basic ${Buffer.from(`${auth.username}:${auth.password}`).toString("base64")}`,
    }
  : undefined;
export async function exists(table: string) {
  const response = await fetch(`${baseUrl}/db/v1/tables/${table}`, {
    headers,
    signal: AbortSignal.timeout(10_000),
  });
  if (response.status === 404) return false;
  if (!response.ok)
    throw new Error(`Antfly returned ${response.status} checking ${table}`);
  return true;
}
