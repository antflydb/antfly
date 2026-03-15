/**
 * Example usage of Antfly SDK in a Node.js backend application
 */

// When running from source, import from src
// In production, use: import { AntflyClient } from "@antfly/sdk";
import { AntflyClient } from "../src/client.js";
import type { BatchRequest, CreateTableRequest, QueryRequest } from "../src/types.js";

async function main() {
  // Initialize the client with environment variables
  const client = new AntflyClient({
    baseUrl: process.env.ANTFLY_URL || "http://localhost:8080",
    auth: {
      username: process.env.ANTFLY_USER || "admin",
      password: process.env.ANTFLY_PASSWORD || "password",
    },
  });

  console.log("Connecting to Antfly at:", process.env.ANTFLY_URL || "http://localhost:8080");
  console.log("Note: Make sure Antfly server is running before running this example");

  try {
    // List existing tables first
    console.log("\nListing existing tables...");
    const existingTables = await client.tables.list();
    console.log("Existing tables:", existingTables?.map((t) => t.name) || []);

    // Create a products table
    console.log("\nCreating products table...");
    await client.tables.create("products", {
      num_shards: 3,
      schema: {
        key: "sku",
        default_type: "product",
        document_types: {
          product: {
            fields: {
              sku: { type: "keyword" },
              name: { type: "string" },
              description: { type: "string" },
              price: { type: "float" },
              category: { type: "keyword" },
              in_stock: { type: "bool" },
              tags: { type: "array" },
            },
          },
        },
      },
    } as CreateTableRequest);

    // Insert some sample data
    console.log("Inserting sample products...");
    const batchRequest: BatchRequest = {
      inserts: {
        SKU001: {
          sku: "SKU001",
          name: "Laptop Pro 15",
          description: "High-performance laptop with 15-inch display",
          price: 1299.99,
          category: "electronics",
          in_stock: true,
          tags: ["laptop", "computer", "electronics", "premium"],
        },
        SKU002: {
          sku: "SKU002",
          name: "Wireless Mouse",
          description: "Ergonomic wireless mouse with long battery life",
          price: 29.99,
          category: "accessories",
          in_stock: true,
          tags: ["mouse", "wireless", "accessories"],
        },
        SKU003: {
          sku: "SKU003",
          name: "USB-C Hub",
          description: "7-in-1 USB-C hub with HDMI and card readers",
          price: 49.99,
          category: "accessories",
          in_stock: false,
          tags: ["hub", "usb-c", "accessories"],
        },
      },
    };
    await client.tables.batch("products", batchRequest);

    // Query the data
    console.log("\nSearching for products...");
    const searchQuery: QueryRequest = {
      table: "products",
      full_text_search: {
        query: "laptop OR mouse",
      },
      limit: 10,
      fields: ["name", "price", "category"],
    };

    const results = await client.tables.query("products", searchQuery);
    console.log("Search results:", JSON.stringify(results?.responses?.[0].hits?.hits, null, 2));

    // Lookup a specific product
    console.log("\nLooking up SKU001...");
    const product = await client.tables.lookup("products", "SKU001");
    console.log("Product details:", product);

    // Create a semantic search index
    console.log("\nCreating semantic search index...");
    await client.indexes.create("products", {
      name: "description_embeddings",
      type: "aknn_v0",
      field: "description",
      dimension: 768,
      embedder: {
        provider: "ollama",
        model: "nomic-embed-text",
      },
    });

    // Example: Table-specific RAG query
    console.log("\nPerforming RAG query on products table...");
    const ragResult = await client.tables.rag("products", {
      query: {
        semantic_search: "high-performance computing devices",
        limit: 5,
      },
      summarizer: {
        provider: "ollama",
        model: "llama3",
      },
      system_prompt:
        "You are a helpful product assistant. Summarize the products that match the query.",
    });

    if (ragResult && typeof ragResult === "object" && "summary_result" in ragResult) {
      console.log("RAG Summary:", ragResult.summary_result?.summary);
      console.log("Query hits:", ragResult.query_result?.hits?.total);
    }

    // Example: Table-specific RAG query with streaming
    console.log("\nPerforming RAG query with streaming...");
    let _streamedText = "";
    await client.tables.rag(
      "products",
      {
        query: {
          semantic_search: "wireless devices",
          limit: 3,
        },
        summarizer: {
          provider: "ollama",
          model: "llama3",
        },
      },
      (chunk) => {
        _streamedText += chunk;
        process.stdout.write(chunk);
      }
    );
    console.log("\n\nStreamed summary complete!");

    // List all tables
    console.log("\nListing all tables...");
    const tables = await client.tables.list();
    console.log(
      "Tables:",
      tables?.map((t) => t.name)
    );
  } catch (error: unknown) {
    if (
      error instanceof Error &&
      (error.cause as Record<string, unknown>)?.code === "ECONNREFUSED"
    ) {
      console.error("\n❌ Connection refused. Please make sure Antfly server is running.");
      console.error("   You can start it with: docker run -p 8080:8080 antfly/antfly");
    } else {
      console.error("Error:", error);
    }
  }
}

// Run the example
main().catch(console.error);
