/**
 * Example usage of the Antfly SDK
 * This file demonstrates how to use the SDK
 */

import { AntflyClient } from "./client.js";
import type { BatchRequest } from "./types.js";

// Initialize the client
const client = new AntflyClient({
  baseUrl: "http://localhost:8080",
  auth: {
    username: "admin",
    password: "password",
  },
});

// Example: List all tables
async function listTables() {
  const tables = await client.tables.list();
  console.log("Tables:", tables);
}

// Example: Query data
async function queryData() {
  const result = await client.query({
    table: "products",
    limit: 10,
    full_text_search: {
      query: "laptop",
    },
  });
  console.log("Query results:", result?.hits?.hits);
}

// Example: Create a table with schema
async function createTable() {
  await client.tables.create("products", {
    num_shards: 3,
    schema: {
      version: 0,
      default_type: "product",
      document_schemas: {
        product: {
          schema: {
            fields: {
              name: { type: "string" },
              price: { type: "float" },
              description: { type: "string" },
              tags: { type: "array" },
            },
          },
        },
      },
    },
  });
}

// Example: Batch insert data
async function batchInsert() {
  const request: BatchRequest = {
    inserts: {
      "prod:001": {
        name: "Laptop Pro",
        price: 1299.99,
        description: "High-performance laptop",
        tags: ["electronics", "computers"],
      },
      "prod:002": {
        name: "Wireless Mouse",
        price: 29.99,
        description: "Ergonomic wireless mouse",
        tags: ["electronics", "accessories"],
      },
    },
  };
  await client.tables.batch("products", request);
}

// Export examples for testing
export { listTables, queryData, createTable, batchInsert };
