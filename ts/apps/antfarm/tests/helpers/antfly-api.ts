import type { Page, Route } from "@playwright/test";

type MockIndex = {
  config: {
    name: string;
    type: string;
    [key: string]: unknown;
  };
  status?: Record<string, unknown>;
};

type MockTable = {
  name: string;
  description?: string;
  shards?: Record<string, unknown>;
  indexes?: Record<string, unknown>;
  storage_status?: {
    empty?: boolean;
    disk_usage?: number;
  };
  schema?: Record<string, unknown>;
  migration?: {
    read_schema: {
      version: number;
    };
  };
  indexesList?: MockIndex[];
};

type MockAntflyApiOptions = {
  tables?: MockTable[];
};

const defaultTables: MockTable[] = [
  {
    name: "books",
    description: "Book catalog",
    shards: { "0": {} },
    indexes: { title_embedding: {}, full_text_index_v0: {} },
    storage_status: { disk_usage: 2048 },
    schema: {
      version: 1,
      document_schemas: {
        default: {
          schema: {
            type: "object",
            properties: {
              title: { type: "string" },
              body: { type: "string" },
            },
          },
        },
      },
    },
    indexesList: [
      {
        config: {
          name: "title_embedding",
          type: "embeddings",
          embedder: { provider: "openai", model: "text-embedding-3-small" },
        },
        status: { total_indexed: 42 },
      },
      {
        config: {
          name: "full_text_index_v0",
          type: "full_text",
        },
        status: { total_indexed: 42, disk_usage: 2048 },
      },
    ],
  },
  {
    name: "authors",
    description: "Author directory",
    shards: { "0": {} },
    indexes: {},
    storage_status: { empty: true },
    schema: { version: 1, document_schemas: {} },
    indexesList: [],
  },
];

function json(route: Route, body: unknown, status = 200) {
  return route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

export async function installAntflyApiMocks(
  page: Page,
  options: MockAntflyApiOptions = {}
): Promise<void> {
  const tables = options.tables ?? defaultTables;

  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;

    if (request.method() === "GET" && path === "/api/v1/status") {
      return json(route, { auth_enabled: false });
    }

    if (request.method() === "GET" && path === "/api/v1/tables") {
      const prefix = url.searchParams.get("prefix");
      const pattern = url.searchParams.get("pattern");
      let filtered = tables;

      if (prefix) {
        filtered = filtered.filter((table) => table.name.startsWith(prefix));
      } else if (pattern) {
        const regex = new RegExp(pattern);
        filtered = filtered.filter((table) => regex.test(table.name));
      }

      return json(route, filtered);
    }

    const tableMatch = path.match(/^\/api\/v1\/tables\/([^/]+)$/);
    if (request.method() === "GET" && tableMatch) {
      const tableName = decodeURIComponent(tableMatch[1]);
      const table = tables.find((entry) => entry.name === tableName);
      if (!table) return json(route, { error: "not found" }, 404);
      return json(route, {
        name: table.name,
        schema: table.schema ?? {},
        migration: table.migration,
      });
    }

    const indexesMatch = path.match(/^\/api\/v1\/tables\/([^/]+)\/indexes$/);
    if (request.method() === "GET" && indexesMatch) {
      const tableName = decodeURIComponent(indexesMatch[1]);
      const table = tables.find((entry) => entry.name === tableName);
      if (!table) return json(route, { error: "not found" }, 404);
      return json(route, table.indexesList ?? []);
    }

    return json(route, { error: `Unhandled mock for ${request.method()} ${path}` }, 404);
  });
}
